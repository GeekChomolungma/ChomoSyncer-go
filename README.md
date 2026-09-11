> **Language:** English | [简体中文](README.zh-CN.md)

# ChomoSyncer-go

A high-performance, low-latency real-time streaming ingestion, synchronization, and multi-tier distribution system for market-wide klines on **Binance USDⓈ-M (USDT-margined) Perpetual Futures**.

The system ingests Binance market data streams concurrently through deterministic WebSocket sharding, paired with a historical backfill mechanism that combines token-bucket rate limiting and window gating. This delivers millisecond-level market data distribution, gap-free time-series persistence, and market-wide cross-sectional alignment — providing a standardized, read-only source-of-truth data foundation for downstream quant research systems and live trading strategy engines.

---

## Core Data Outputs & Multi-Tier Interface Semantics

The system only ingests **1-minute base klines** from Binance; every coarser interval (`5m/15m/1h/4h/1d`, …) is derived from it: on the ClickHouse side, materialized-view rollups persist them into `fapi_kline_<interval>` tables, while the real-time path only derives and forwards the `kline_ready` cross-section signal. As a result, Redis only keeps real-time interfaces for 1m — all coarser intervals should be queried from ClickHouse.

| Data Output / Interface | Storage Medium & Key Structure | Data Shape & Protocol | Core Business Semantics & Use Case |
| :--- | :--- | :--- | :--- |
| **1m Time-Series Fact Archive** | **ClickHouse**<br>`market.fapi_kline_1m` | ReplacingMergeTree physical table<br>deduplicated by `(symbol, start_time)` | **The global authoritative fact ledger.** The only raw table written directly; guarantees absolute zero gaps. |
| **Derived-Interval Archive** | **ClickHouse**<br>`market.fapi_kline_{5m,15m,1h,4h,1d}` | ReplacingMergeTree + refreshable materialized views<br>recomputed and aggregated from `fapi_kline_1m FINAL` | **Multi-timeframe backtest/feature store.** Idempotently recomputed from 1m (`deploy/clickhouse/002_kline_rollups.sql`) — never accumulates, never double-counts. |
| **Real-Time Unclosed Snapshot** | **Redis Hash**<br>`livebar:{SYMBOL}:1m` | Hash structure (11 fields)<br>`t, o, h, l, c, v, qv, tbv, tbqv, n, x` | **Reads the transient shape of the still-forming bar.** 1m only. Refreshes the currently ticking kline at millisecond granularity, with an auto-expiring TTL. |
| **Closed Rolling Window** | **Redis List**<br>`kline:{SYMBOL}:1m` | List (fixed length of 200 bars)<br>9-element compact JSON array (no keys) | **Ultra-fast feature-computation cache for strategies.** 1m only, the most recent 200 closed bars. For coarser intervals, query the ClickHouse rollup tables directly. |
| **Cross-Section Ready Notification** | **Redis Stream**<br>`stream:market:kline_ready` | Stream event<br>`interval, timestamp, symbols_count` | **A cross-symbol section synchronization trigger for strategies.** Published when the 1m cross-section is complete; for each interval configured in `serve_intervals`, a derived event is forwarded once its "bucket-closing 1m section" becomes ready (tagged with that coarser `interval`). |

---

## Quick Start

Below is the full forward sequence for bringing an **empty database online for the first time**. The core idea: **first fill ClickHouse with 1m history offline, then build the rollup layer, and only then start the online service** — because a deep historical backfill during online cold start would exceed `gate_timeout` and get released early, leaving a sticky gap that can never be backfilled again.

> For an **existing database / routine restarts**: skip "offline history fill" and table creation — just start the service directly; gaps that occurred while disconnected are automatically backfilled from `max(start_time)` on startup. See [`docs/OPERATIONS.md` §3](docs/OPERATIONS.md) for detailed steps by scenario.

### Mode 1: Docker Compose (recommended for quick evaluation & integration testing)

`deploy/docker-compose.yml` orchestrates three containers: `clickhouse`, `redis`, and `chomosyncer-go`; the root `Dockerfile` is only used to compile the source into the `chomosyncer-go` image. **YAML config takes priority**: compose mounts the root `config.yaml` into the container, sharing the exact same file used on bare metal (only the redis/clickhouse addresses are overridden by compose to the container names).

```bash
# 1. Generate the config. Set cold_start_date to your desired history start point, e.g. "2024-01-01".
cp config.example.yaml config.yaml

# 2. Bring up the underlying dependencies. On first start, ClickHouse auto-creates the raw table fapi_kline_1m (001).
docker compose -f deploy/docker-compose.yml up -d
docker compose -f deploy/docker-compose.yml ps                 # wait for both to become healthy

# 3. Fill history offline. One-shot pull of the whole market's 1m history into fapi_kline_1m;
#    the container exits on its own when done.
#    (WS/Redis are not started; interruption is fine — progress is persisted, reruns resume from max(start_time).)
docker compose -f deploy/docker-compose.yml run --rm chomosyncer-go -backfill-offline-only

# 4. Once history is filled, build the rollup layer + fold in history
#    (only create the MVs now, so they don't compete for time with the large backfill above).
docker compose -f deploy/docker-compose.yml exec clickhouse \
  clickhouse-client --queries-file /clickhouse/002_kline_rollups.sql
docker compose -f deploy/docker-compose.yml exec clickhouse clickhouse-client \
  --param_m_start='2000-01-01 00:00:00' --param_m_end='2099-01-01 00:00:00' \
  --queries-file /clickhouse/003_rollup_backfill.sql

# 5. (Optional) Verify zero history gaps + reconcile against Binance.
python cmd/test-tools/check_clickhouse_integrity.py --intervals 1m,5m,15m,1h,4h,1d
python cmd/test-tools/check_vs_binance.py --intervals 1m,1h,4h

# 6. Narrow config.yaml's cold_start_date down to the last 7 days, then start the online service.
docker compose -f deploy/docker-compose.yml --profile app up -d --build
docker compose -f deploy/docker-compose.yml logs -f chomosyncer-go
curl -s localhost:9090/readyz

# Shut down (add -v to also remove the ClickHouse data volume)
docker compose -f deploy/docker-compose.yml --profile app down
```

### Mode 2: Regular Binary Bare-Metal Deployment (recommended for production)

```bash
# 1. Build a static binary.
CGO_ENABLED=0 go build -trimpath \
  -ldflags "-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo v1.0.0)" \
  -o bin/chomosyncer-go ./cmd/chomosyncer-go

# 2. Generate the config. Fill in the redis / clickhouse addresses; set cold_start_date to the history
#    start point, e.g. "2024-01-01".
cp config.example.yaml config.yaml

# 3. Create the raw table.
clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < deploy/clickhouse/001_fapi_kline.sql

# 4. Fill history offline. Pull the whole market's 1m history into fapi_kline_1m; it exits when done —
#    safe to rerun after an interruption to resume.
./bin/chomosyncer-go -config config.yaml -backfill-offline-only

# 5. Once history is filled, build the rollup layer + fold in history.
clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < deploy/clickhouse/002_kline_rollups.sql
clickhouse-client --host 127.0.0.1 --port 9000 \
  --param_m_start='2000-01-01 00:00:00' --param_m_end='2099-01-01 00:00:00' \
  --queries-file deploy/clickhouse/003_rollup_backfill.sql

# 6. (Optional) Verify.
python cmd/test-tools/check_clickhouse_integrity.py --intervals 1m,5m,15m,1h,4h,1d
python cmd/test-tools/check_vs_binance.py --intervals 1m,1h,4h

# 7. Narrow config.yaml's cold_start_date down to the last 7 days, then start the online business.
./bin/chomosyncer-go -config config.yaml
curl -i http://localhost:9090/readyz                           # returns 200 once backfill is complete
```

---

## Documentation Map: Architecture & Design Details

For low-level implementation details of each module, data protocols, and operational testing, see the dedicated technical documents under `docs/`:

- 📖 **[Module Architecture & Design Deep Dive (`docs/ARCHITECTURE_MODULES.md`)](docs/ARCHITECTURE_MODULES.md)**
  Explains the design of each core module in detail:
  - `internal/chwriter`: high-performance ClickHouse batch-write queue with retry backoff;
  - `internal/rediswin`: atomic Lua-based monotonic rolling window, an independent LiveBar goroutine pool, and Stream publishing;
  - `internal/collector`: deterministic hash sharding of Binance WebSocket connections, staggered connection setup, and a staleness watchdog;
  - `internal/universe`: dynamic market-wide discovery of USDT perpetual contracts with a daily UTC-aligned refresh;
  - `internal/dispatcher`: four-way event fan-out, monotonicity guarding, and a dual-trigger cross-section aggregator;
  - `internal/backfill` & `internal/windowgate`: the three cold-start/reconnect gap-filling rules, window gating, and REST token-bucket rate limiting.

- 📊 **[End-to-End Data Flow & Storage Structures (`docs/DATA_FLOW_AND_STRUCTURES.md`)](docs/DATA_FLOW_AND_STRUCTURES.md)**
  End-to-end data flow topology diagrams, the concurrency model of each workflow, example query commands against external storage (Redis / ClickHouse), returned JSON formats, and field-level business definitions.

- 📥 **[Data Consumer Guide (`docs/DATA_CONSUMER_GUIDE.md`)](docs/DATA_CONSUMER_GUIDE.md)**
  The short version for a downstream reader (e.g. a Python strategy/feature layer) that just needs to consume the data: what's in ClickHouse vs. Redis (`livebar` / closed-window `kline` / `kline_ready`), exact key/table and field layouts, and copy-paste Python snippets — no producer-side internals required.

- 🛠️ **[Deployment, Operations, Failure Drills & Hands-On Testing Guide (`docs/OPERATIONS.md`)](docs/OPERATIONS.md)**
  A complete hands-on guide from environment requirements and capacity planning, to Prometheus alert-rule configuration, network-outage drills, and service failover/recovery.

- 🧪 **[End-to-End Pre-Flight & Data Validation Toolkit (`cmd/test-tools/README.md`)](cmd/test-tools/README.md)**
  A Python automated diagnostics suite for pre-launch checks:
  - `check_clickhouse_integrity.py`: scans the full table with window functions to detect missing-bar gaps in history and data-quality issues;
  - `check_redis_livebars.py`: inspects the existence, TTL, and latency of real-time live bars;
  - `check_redis_closed_windows.py`: verifies the completeness of the 200-bar rolling window list, head freshness, and internal continuity;
  - `monitor_redis_kline_ready.py`: monitors the cross-section ready notification stream in real time (and retrospectively) for latency;
  - `e2e_reconciliation.py`: bar-by-bar reconciliation between the Redis 1m window and ClickHouse;
  - `check_vs_binance.py`: reconciles ClickHouse (raw 1m table / rollup tables) bar-by-bar against the Binance REST API to catch systematic collection/aggregation deviations;
  - `run_all_checks.py`: a one-shot pre-launch scorecard (add `--vs-binance` to include external reconciliation).

---

## License

This project is licensed under the MIT License.
