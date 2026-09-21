> **Language:** English | [简体中文](OPERATIONS.zh-CN.md)

# ChomoSyncer-go Deployment, Operations & End-to-End Testing Handbook

This document is the official operations, deployment, chaos-drill, load-testing, and go-live pre-flight guide for `ChomoSyncer-go`.

---

## 1. Architecture Topology and Port Matrix

```text
  [Binance Futures WS/REST]
             │ (inbound market data)
             ▼
    ┌─────────────────┐
    │  ChomoSyncer-go │◄─────── :9090 (/metrics, /healthz, /readyz)
    └──┬────────────┬─┘
       │            │
       │ TCP 9000   │ TCP 6379
       ▼            ▼
┌─────────────┐  ┌─────────────┐
│ ClickHouse  │  │    Redis    │
│ (time-series│  │(sliding win/│
│   archive)  │  │    Live)    │
└─────────────┘  └─────────────┘
 (HTTP: 8123)
```

### Port Matrix
| Service Component | Listening Port | Protocol | Access Source | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **ChomoSyncer-go** | `9090` | HTTP | Prometheus / K8s probes | Exposes `/metrics`, `/healthz` (liveness), `/readyz` (readiness) |
| **ClickHouse Native**| `9000` | TCP (Native)| ChomoSyncer-go | High-performance columnar batch write channel |
| **ClickHouse HTTP**  | `8123` | HTTP | Test scripts / operations queries | For operators or the Python verification toolkit to query |
| **Redis**            | `6379` | RESP | ChomoSyncer-go / strategy layer | Stores the Live Bar hash, the 200-bar sliding-window list, and the cross-sectional readiness stream |

---

## 2. Prerequisites

| Dependency | Minimum Version | Recommended Version | Notes |
| :--- | :--- | :--- | :--- |
| **Go** | ≥ 1.25 | 1.25+ | Builds the production binary (`CGO_ENABLED=0`) |
| **ClickHouse** | ≥ 24.8 | 24.8+ | ReplacingMergeTree + refreshable materialized views (for deriving rollup periods, see `002_kline_rollups.sql`). Without refreshable MV support, fall back to the cron approach |
| **Redis** | ≥ 6.2 | 7.0+ (Alpine) | Recommended to disable persistence and set `noeviction` |
| **Python** | ≥ 3.9 | 3.10+ | Runs the `cmd/test-tools` pre-flight toolkit |
| **Network requirements** | Outbound connectivity | Low-latency direct connection | Requires stable access to `fstream.binance.com` and `fapi.binance.com` |

---

## 3. Quick Start Guide

Split into two tracks based on **whether ClickHouse already has historical data**, and each track further splits into Docker (Method A) and bare metal (Method B):

| Scenario | Method A: Docker Compose (local / test) | Method B: Bare-metal binary (production-oriented) |
| :--- | :--- | :--- |
| **Bringing an empty database online for the first time** (create tables → backfill history offline → restart the subscription) | §A.1 | §B.1 |
| **An existing database** (routine startup / restart / maintenance) | §A.2 | §B.2 |

> **Configuration precedence** (low → high): built-in defaults `<` `config.yaml` `<` `CHOMOSYNCER_*` environment variables `<` explicit command-line flags. Both methods take the root-level `config.yaml` as the source of truth (Docker also just mounts the same file into the container). See §3.3 at the end of this section for details.

---

### Method A: Docker Compose Mode

#### A.0 How These Files Relate to Each Other

| File | Purpose |
| :--- | :--- |
| **`Dockerfile`** (repository root) | Solely responsible for **compiling the Go source into a `chomosyncer-go` image** (multi-stage: build with `golang:1.25` → run on `distroless/static`, non-root). The image contains **only the collector binary** — no Redis, no ClickHouse, no config file. It is not used standalone; it is invoked by compose. |
| **`deploy/docker-compose.yml`** | Orchestrates three containers: `clickhouse`, `redis`, `chomosyncer-go`. The `chomosyncer-go` service's `build:` points at the root `Dockerfile`, and `profiles: ["app"]` keeps it from starting by default (requires `--profile app`). |
| **`deploy/clickhouse/001_fapi_kline.sql`** | Mounted into the `clickhouse` container's `/docker-entrypoint-initdb.d/` and **executed automatically on first startup** (creates the `market` database and the raw table `fapi_kline_1m`). |
| **`deploy/clickhouse/002` / `003`** | Mounted read-only into the container via `./clickhouse:/clickhouse:ro`, but **not run automatically** — these are the rollup tables and materialized views, run manually by you at the right time. |
| **Root-level `config.yaml`** | **The single source of configuration.** Compose mounts it read-only into the `chomosyncer-go` container (`../config.yaml → /etc/chomosyncer-go/config.yaml`), exactly the same way as the bare-metal setup. **It must exist beforehand** — otherwise Docker will create the mount point as an empty directory, and the collector will crash with `read config file: ... is a directory`. |
| **`deploy/chomosyncer-go.env.example`** | Reference only — lists all available `CHOMOSYNCER_*` variables. Compose **does not read it by default** (YAML takes precedence). |

In one sentence: **`Dockerfile` = compile and package; `docker-compose.yml` = wire the collector + Redis + ClickHouse into one stack; configuration is that single `config.yaml`, shared by both Docker and bare metal.**

#### A.1: Bringing an Empty Database Online for the First Time (create tables → backfill history offline → restart the subscription)

**Why split this into two steps instead of starting everything at once**: In online mode, cold-start historical backfill is gated by `windowgate` in front of Redis business logic (the `kline:*` sliding windows and the `kline_ready` cross-section), but the gate has a safety-net release governed by `backfill.gate_timeout` (default `10m`). If `cold_start_date` is set to several months or even a year in the past, a market-wide backfill will take far longer than that timeout → the gate **releases early**, live klines start being written to Redis, and the deep-history gap in between "pollutes" ClickHouse's `max(start_time)` — a restart will not automatically re-backfill it (it becomes a sticky gap). So treat deep history as a **separate offline phase**, fill it completely, validate it, and only then bring up the online service.

```bash
# 0. Generate the config (compose will mount it into the container). No need to change the
#    redis/clickhouse addresses — compose overrides them with the container names.
#    Set cold_start_date to the historical starting point you want, e.g. "2024-01-01".
cp config.example.yaml config.yaml

# 1. Bring up the underlying dependencies. On first startup, ClickHouse only auto-runs 001
#    (creating fapi_kline_1m).
docker compose -f deploy/docker-compose.yml up -d
docker compose -f deploy/docker-compose.yml ps            # wait until both are healthy

# 2. Backfill history offline: pull the full market-wide 1m history into fapi_kline_1m in one
#    shot; the container exits on its own when done.
#    - Only wires up the universe→REST→ClickHouse path; does not start WS / dispatcher / Redis;
#    - gate_timeout is automatically inert and will not be forcibly released midway;
#    - exit code 0 = completed; non-zero = interrupted, but progress has been persisted, so
#      rerunning resumes from max(start_time);
#    - watch progress via /metrics: backfill_bars_fetched_total / _written_total / _errors_total.
docker compose -f deploy/docker-compose.yml run --rm chomosyncer-go \
  -backfill-offline-only

# 3. Once history is fully backfilled, build the rollup layer and fold in the history.
#    Only create the MV at this point → the refreshable materialized view will only track newly
#    arriving live 1m data, and won't compete for time with the large backfill from the step above.
docker compose -f deploy/docker-compose.yml exec clickhouse \
  clickhouse-client --queries-file /clickhouse/002_kline_rollups.sql
docker compose -f deploy/docker-compose.yml exec clickhouse clickhouse-client \
  --param_m_start='2000-01-01 00:00:00' --param_m_end='2099-01-01 00:00:00' \
  --queries-file /clickhouse/003_rollup_backfill.sql

# 4. Validate (must be all-green before moving to the next step).
python cmd/test-tools/check_clickhouse_integrity.py --intervals 1m,5m,15m,1h,4h,1d
python cmd/test-tools/check_vs_binance.py --intervals 1m,1h,4h

# 5. Narrow config.yaml's cold_start_date down to the most recent 7 days (enough to fill the
#    200-bar 1m sliding window plus margin), then bring up the online service. At this point the
#    online cold start finishes within seconds, will never hit gate_timeout, and the rollup MV
#    keeps up automatically.
docker compose -f deploy/docker-compose.yml --profile app up -d --build
docker compose -f deploy/docker-compose.yml logs -f chomosyncer-go
curl -s localhost:9090/readyz
```

> ClickHouse **< 24.8** does not support refreshable materialized views: remove every `CREATE MATERIALIZED VIEW` block from `002`, and instead use cron / a systemd timer to run `003` every 1–2 minutes (rolling window; see the "CRON FALLBACK" header comment in the `003` file). Recomputation in `003` is idempotent — running it any number of times will never double-count.

#### A.2: An Existing Database (routine startup / restart / maintenance)

Once `fapi_kline_1m` already has data and the rollup tables have already been created, every subsequent startup is a single command:

```bash
docker compose -f deploy/docker-compose.yml --profile app up -d --build
```

- **No need to run 001/002/003 again.** `001` only executes when the ClickHouse data volume is empty; `002`'s MVs already exist; `003` only needs to be rerun after "a new batch of historical 1m data has been backfilled."
- **Gaps from disconnects/restarts are backfilled automatically**: on startup the collector reads `max(start_time)` from `fapi_kline_1m` and resumes backfilling from there; `/readyz` returns 503 until backfill completes. Keep `config.yaml`'s `cold_start_date` set to "the most recent 7 days" (when history already exists it only serves as a safety net for an empty database and has no effect).
- **Changing configuration**: edit the root-level `config.yaml` directly, then rebuild the container with `--profile app up -d`. No second configuration file is needed; the only container-specific overrides are the two addresses hardcoded by compose (`redis:6379` / `clickhouse:9000`).
- **Added a new `serve_intervals`**: first run `docker compose ... exec clickhouse clickhouse-client --queries-file /clickhouse/002_kline_rollups.sql` (`002` uses `CREATE ... IF NOT EXISTS`, so it only adds the missing tables/views), run `003` as needed if history is required, then restart the collector.
- **Shutting down**: `docker compose -f deploy/docker-compose.yml --profile app down` (adding `-v` also wipes the ClickHouse data volume — use with caution).

---

### Method B: Standard Bare-Metal Binary Deployment (production-oriented)

#### B.0: Build

```bash
CGO_ENABLED=0 go build -trimpath \
  -ldflags "-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo 'v1.0.0')" \
  -o bin/chomosyncer-go ./cmd/chomosyncer-go
```

Provide your own ClickHouse (≥ 24.8) and Redis (recommended `--maxmemory 1gb --maxmemory-policy noeviction --save ''`).

#### B.1: Bringing an Empty Database Online for the First Time (create tables → backfill history offline → restart the online service)

The reasoning for splitting this into two steps is the same as §A.1 (deep history must be fully backfilled offline first, otherwise an early release of the `gate_timeout` gate will leave a sticky gap).

```bash
# 1. Generate the config, fill in the redis / clickhouse addresses, and set cold_start_date to
#    the historical starting point (e.g. "2024-01-01").
cp config.example.yaml config.yaml

# 2. Create the raw table.
clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < deploy/clickhouse/001_fapi_kline.sql

# 3. Backfill history offline: pull the full market-wide 1m history into fapi_kline_1m, exiting
#    once done.
#    Behaves the same as step 2 of A.1: only wires up REST→ClickHouse, does not start WS/Redis;
#    gate_timeout is inert;
#    exit code 0 = completed / non-zero = interrupted but resumable by rerunning; watch progress
#    via /metrics: backfill_bars_*.
./bin/chomosyncer-go -config config.yaml -backfill-offline-only

# 4. Once history is fully backfilled, build the rollup layer and fold in the history.
clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < deploy/clickhouse/002_kline_rollups.sql
clickhouse-client --host 127.0.0.1 --port 9000 \
  --param_m_start='2000-01-01 00:00:00' --param_m_end='2099-01-01 00:00:00' \
  --queries-file deploy/clickhouse/003_rollup_backfill.sql

# 5. Validate.
curl -s 'http://127.0.0.1:8123/?query=SHOW+TABLES+FROM+market'
# should return: fapi_kline_1m / _5m / _15m / _1h / _4h / _1d (+ *_rmv materialized views)
python cmd/test-tools/check_clickhouse_integrity.py --intervals 1m,5m,15m,1h,4h,1d
python cmd/test-tools/check_vs_binance.py --intervals 1m,1h,4h

# 6. Narrow config.yaml's cold_start_date down to the most recent 7 days, then bring up the
#    online service.
./bin/chomosyncer-go -config config.yaml
curl -i http://localhost:9090/readyz     # switches to 200 once backfill is complete
```

> ClickHouse **< 24.8**: same note as A.1 — remove `CREATE MATERIALIZED VIEW` from `002` and run `003` via cron instead.

#### B.2: An Existing Database (restart / ongoing maintenance)

```bash
./bin/chomosyncer-go -config config.yaml            # this is the only command needed
```

- **No need to run 001/002/003 again.** Gaps from disconnects during downtime are automatically backfilled on startup from `fapi_kline_1m`'s `max(start_time)`; `/readyz` returns 503 until backfill completes.
- Recommended to manage it with systemd (`Restart=on-failure`, `ExecStart=/opt/chomosyncer-go/bin/chomosyncer-go -config /opt/chomosyncer-go/config.yaml`); `SIGTERM` triggers a graceful shutdown that drains and persists in-flight data.
- **Changing configuration**: edit `config.yaml` and restart the process; individual items can also be overridden temporarily with a `-flag` or `CHOMOSYNCER_*` (higher precedence).
- **Added a new `serve_intervals`**: re-run `002` (`IF NOT EXISTS`, only fills in what's missing), run `003` as needed if history is required, then restart the process.

---

### 3.3 Configuration Sources and Advanced Parameters

Precedence (low → high): **built-in `DefaultConfig()` < `config.yaml` < `CHOMOSYNCER_*` environment variables < explicit command-line flags**. Every item in `config.example.yaml` has a built-in production-grade default, so it will run even if left unset.

- **Bare metal**: `-config config.yaml` is all you need; to temporarily override a given item, use a `-flag` or `CHOMOSYNCER_*`.
- **Docker**: maintain only the single root-level `config.yaml` (mounted into the container); the two addresses hardcoded as env vars in compose (`CHOMOSYNCER_REDIS_ADDR=redis:6379` / `CHOMOSYNCER_CH_ADDR=clickhouse:9000`) are the only container-specific overrides — because `config.yaml` has `localhost`, which isn't reachable inside the container network. To override another item inside the container, add the corresponding `CHOMOSYNCER_*` to the `chomosyncer-go` service's `environment:` (rule: flag `-redis-pool-size` ↔ env `CHOMOSYNCER_REDIS_POOL_SIZE`; see `chomosyncer-go -h` for the full list).
- **To use environment variables exclusively instead of YAML**: remove the `config.yaml` mount in `docker-compose.yml` and uncomment `env_file:` (pointing at `deploy/chomosyncer-go.env`, copied from `chomosyncer-go.env.example`).
- **Parameters that can only be set via YAML, with no environment-variable counterpart**: the whole `open_interest.*` and `weight_gate.*` sections (§3.4), `redis.dial_timeout` / `read_timeout` / `write_timeout`, `redis.window.stream_maxlen` / `key_prefix` / `atomic`, `redis.live.ttl_multiple` / `default_ttl` / `write_timeout` / `key_prefix`, `clickhouse.table_prefix` / `dial_timeout` / `tls` / `max_retries` / `retry_backoff` / `max_retry_backoff` / `shutdown_timeout`, `collector.connect_stagger` / `connect_timeout` / `reconnect_base` / `reconnect_max` / `watchdog_interval`, `dispatcher.publish_timeout`, `universe.refresh_offset` / `http_timeout` / `contract_type` / `status`, `backfill.queue_size`, `app.shutdown_timeout`.

---

### 3.4 Enabling Open-Interest Sync (optional)

The open-interest (OI) module is **off by default** and independent of the kline pipeline: it neither delays `/readyz` nor touches Redis. It writes `market.fapi_oi_5m`; design and measurements: [`../new_requirements/oi.md`](../new_requirements/oi.md), consumer view: [`DATA_CONSUMER_GUIDE.md`](DATA_CONSUMER_GUIDE.md) §1b.

**Two switches, enabled one after the other** (`config.yaml`, see `open_interest:` in `config.example.yaml`):

| Switch | What it does | Pool it spends |
| :--- | :--- | :--- |
| `open_interest.hist_enabled` | At start-up reads, per symbol, how far the table is already calibrated and backfills only the gap from `openInterestHist`; then calibrates every hour at `hh:05` | `/futures/data` (1000 requests / 5 min / IP), its own counter |
| `open_interest.live_enabled` | Every 5-minute boundary, from 30 s before the kline closes, snapshots each symbol with `/fapi/v1/openInterest` | `/fapi` weight budget (2400/min/IP) through the shared `weight_gate` |

#### Step by step

```bash
# 1. Create the raw table. NOT run by docker compose (only 001 is), and the module never creates it.
clickhouse-client --host localhost --user default --password "<pw>" --multiquery < deploy/clickhouse/004_fapi_oi.sql

#    Docker: docker compose -f deploy/docker-compose.yml exec clickhouse \
#              clickhouse-client --queries-file /clickhouse/004_fapi_oi.sql

# 2. Turn on hist ONLY, restart the service.
#      open_interest:
#        hist_enabled: true
#        live_enabled: false
#    Expected logs:  "open-interest sync started"  ->  "loaded open-interest state from ClickHouse"
#                    -> "open-interest hist pass" reason=startup

# 3. Let it run for a day, then validate (see 5.8):
python cmd/test-tools/check_oi_consistency.py --vs-binance --require-calibration

# 4. Then turn on live (live_enabled: true), restart, validate again after a few 5-minute boundaries.

# 5. Optional rollups (15m / 1h / 4h / 1d / 1mo). Requires ClickHouse >= 24.10 (same as 002); build them
#    only after history is in fapi_oi_5m, then fold the history month by month (006 needs month-aligned ranges):
clickhouse-client --queries-file deploy/clickhouse/005_oi_rollups.sql
clickhouse-client --param_m_start='2026-08-01 00:00:00' --param_m_end='2026-09-01 00:00:00' \
                  --queries-file deploy/clickhouse/006_oi_rollup_backfill.sql
```

#### What the first start does (and what a restart does)

- **Empty table**: every symbol is backfilled 48 hours (`hist_cold_start_window`), 2 requests per symbol. For ~528 symbols that is ~1050 requests at `data_rps: 2`, roughly **9 minutes** (an estimate; the end-to-end test measured 2 s for 3 symbols). During that time this IP spends about 60% of its `/futures/data` budget: do not run other `/futures/data` heavy tools meanwhile.
- **Every later start**: the module re-reads the newest calibrated row per symbol from ClickHouse and decides per symbol — already current → **no request at all**; behind → only the gap (`limit` sized to it; verified: a 3-hour outage costs one request per symbol). If ClickHouse is down at start it retries every 5–60 s and logs `could not read open-interest state; will retry`.
- **Long downtime**: gaps are backfilled up to `hist_max_backfill` (7 days). Older parts are counted in `oi_hist_gap_beyond_cap_total` and need the archive import, which is not part of the module yet. Live snapshots of the downtime are gone for good (the live endpoint has no history) — hist fills those bars instead, with Binance's own values.
- The service does **not** need to be stopped to change the switches, but a restart is required for them to take effect.

#### Watching it (`/metrics`)

```bash
curl -s http://localhost:9090/metrics | grep -E '^(oi_|weightgate_)'
```

| Metric | Healthy | If not |
| :--- | :--- | :--- |
| `oi_hist_last_pass_timestamp_seconds` | younger than ~1.5 h | calibration stalled |
| `oi_cross_section_complete_ratio` | ≈ 1 after each hist pass | some symbols failed or lag |
| `oi_hist_symbols_total{outcome="failed"}` | not increasing | see logs `hist request failed` |
| `oi_data_window_used` | far below 900 (a scheduled pass uses ~130 per 5 min) | throttled by the cap, or another tool shares the IP |
| `oi_live_cycle_complete_ratio` | ≈ 1 | round did not finish in time (see `weightgate_*`) |
| `oi_live_cycle_seconds` | about `symbols / fapi_rps` (~21 s for 528; an estimate) | slow responses or gate waits |
| `oi_live_snapshots_total{result="dropped"}` | 0 | snapshot time far from a boundary: check host clock / NTP |
| `oi_live_vs_hist_rel_diff` | mean ≈ 0.04%, nearly all < 0.5% | alignment or clock problem: run the check in 5.8 |
| `oi_live_gap_bars_total` | ≈ 0 | live missed bars (weight gate, network, restart) |
| `oi_rate_limited_total` | 0 | 429/418 seen: the pool pauses itself; find who else uses the IP |
| `oi_writer_rows_dropped_total` | 0 | ClickHouse outage longer than the flush retries |
| `weightgate_used_weight_1m` | below 1800 | above it, bulk kline backfill waits (live waits above 2300) |

#### Common problems

| Symptom | Likely cause | Action |
| :--- | :--- | :--- |
| Logs repeat `could not read open-interest state; will retry` | `market.fapi_oi_5m` does not exist (or ClickHouse unreachable) | Apply `004_fapi_oi.sql` (or fix connectivity); nothing else is needed |
| `check_oi_consistency.py`: "missing bar(s)" | The module was stopped, or live was off and hist has not caught up | Restart (hist backfills the gap) and rerun the check |
| `check_oi_consistency.py`: "live row(s) never calibrated" | `hist_enabled` is off, or hist has been failing | Enable/fix hist; live rows are otherwise never replaced by Binance's series |
| Live rows exist but the last few bars are missing | Normal: hist publishes a label 1–3 minutes late; the check ignores the newest `--settle-minutes` | None |
| A whole hist pass shows `committed=false` | The writer's flush failed (ClickHouse down) | The next pass refetches the same window automatically |
| Everything is quiet after `live_enabled: true` | Restart missing, or `live_lead` window already passed | Check for `open-interest live round` lines at each 5-minute boundary |

#### Turning it off / rolling back

Set both switches to `false` and restart. **Never `DROP market.fapi_oi_5m`**: it holds live snapshots that cannot be re-created. The rollup tables and views (`005`) are derived and safe to drop and rebuild (`005` does so by design; rerun `006` afterwards).

#### Rate-limit budget (`weight_gate`)

All `/fapi` REST callers (kline gap backfill, universe refresh, OI live) share one gate that splits the per-IP 2400 weight/min into budgets (live 600 / bulk 1200 / misc 100, `weight_gate.*` in YAML) and backs off from Binance's own `X-MBX-USED-WEIGHT-1M`. Normally nothing needs tuning; if `weightgate_backpressure_waits_total{class="live"}` keeps growing, something else on the same IP is consuming weight. `/futures/data` is a separate pool and is not governed by it.

---

## 4. Probes and Readiness Checks

Once the service starts, the built-in HTTP server (default `:9090`) exposes three key endpoints:

1. **Liveness Probe**:
   ```bash
   curl -i http://localhost:9090/healthz
   # expected response: HTTP 200 OK
   ```
2. **Readiness Probe**:
   ```bash
   curl -i http://localhost:9090/readyz
   # returns HTTP 503 Service Unavailable (Backfill in progress) while cold-start historical
   # backfill is incomplete
   # returns HTTP 200 OK once backfill is complete and the full pipeline is ready
   ```
3. **Prometheus Metrics Collection**:
   ```bash
   curl -s http://localhost:9090/metrics | grep -E "ws_shards_active|universe_size|redis_bars_pushed_total|clickhouse_rows_flushed_total"
   # if open-interest sync is enabled (§3.4) and for the shared /fapi rate-limit gate:
   curl -s http://localhost:9090/metrics | grep -E '^(oi_|weightgate_)'
   ```
   (`/readyz` is **not** affected by open-interest catch-up.)

---

## 5. Automated Pre-Flight and Data Validation Framework (Python Test Toolkit)

The project ships an out-of-the-box automated test toolkit under [`cmd/test-tools/`](../cmd/test-tools/) that can perform an exhaustive check of the full pipeline's data before go-live.

### Installing Dependencies
```bash
pip install -r cmd/test-tools/requirements.txt
```

### 5.1 ClickHouse Historical Data Continuity and Field Quality Check
```bash
# Checks whether the 1m raw table plus all rollup tables are contiguous, free of gaps, and have
# fully populated fields (default: 1m,5m,15m,1h,4h,1d)
python cmd/test-tools/check_clickhouse_integrity.py

# In-depth check for a specific major symbol
python cmd/test-tools/check_clickhouse_integrity.py --symbol BTCUSDT --show-all-gaps
```
- **Repairing what it finds**: add `--intervals 1m --gaps-csv gaps.csv`, then `python cmd/test-tools/backfill_missing_1m.py gaps.csv` (dry run) and again with `--apply`. The running service already repairs the last 24 hours itself every 30 minutes (`backfill.sweep_*`); the script is for older holes. Minutes in which a symbol had no trades have no kline at Binance and stay listed — see the tool README (§1b).
- **Core criteria**: automatically uses ClickHouse window functions to scan the timestamp differences between adjacent klines to detect gaps; also validates `open/high/low/close > 0`, `high >= low`, `volume >= 0`, `trades_count >= 0`.

### 5.2 Real-Time Check of Redis Unclosed Live Bars
```bash
python cmd/test-tools/check_redis_livebars.py   # 1m only: only the base period has a livebar
```
- **Core criteria**: scans `livebar:{SYMBOL}:1m` market-wide, verifying key existence, TTL (`2 * interval`), freshness of the update lag (to guard against a half-open/stale connection), and the completeness of the 11 hash fields.

### 5.3 Redis Closed Rolling Sliding-Window Check
```bash
python cmd/test-tools/check_redis_closed_windows.py   # 1m only: coarser periods do not build a Redis window and are instead checked against the ClickHouse rollup tables
```
- **Core criteria**: checks whether the `kline:{SYMBOL}:1m` list length reaches the expected 200 bars, whether the head timestamp closely follows the current close boundary, and that every pair of adjacent bars within the sliding window is strictly contiguous with no missing bars.

### 5.4 Cross-Sectional Notification Stream Monitoring and Backtracking
```bash
# View the latency and symbol-completeness rate of the most recent 20 cross-sectional publish events
python cmd/test-tools/monitor_redis_kline_ready.py --recent 20

# Real-time listening mode (streams the cross-sectional readiness duration and number of arrived symbols for each period)
python cmd/test-tools/monitor_redis_kline_ready.py --tail
```

### 5.5 Redis-and-ClickHouse Dual-Write Consistency Reconciliation
```bash
python cmd/test-tools/e2e_reconciliation.py --limit-symbols 20   # 1m only: Redis only has the 1m window
```
- **Core criteria**: compares the Redis `kline:{SYMBOL}:1m` sliding window bar-by-bar against the latest data persisted in ClickHouse's `fapi_kline_1m`; timestamps and OHLCV must match 100% (tolerance `< 1e-5`).

### 5.6 ClickHouse-vs-Binance External Ground-Truth Reconciliation (catches systemic collection/aggregation bias)
```bash
# 1m raw table vs Binance 1m — verifies the collection field mapping/units are correct
python cmd/test-tools/check_vs_binance.py --intervals 1m

# Rollup tables vs Binance — verifies Phase B's bucket alignment and aggregation functions
python cmd/test-tools/check_vs_binance.py --intervals 1h,4h,1d
```
- **Core criteria**: reconciles bar-by-bar against Binance's `/fapi/v1/klines`. `start_time` must align exactly; OHLC has a relative tolerance of `1e-9` (effectively exact); volume-type fields have a relative tolerance of `1e-6` (due to floating-point summation order differences); `trades_count` must be exactly equal.
- `e2e_reconciliation.py` is "the pipeline compared against itself" — this is the only check that can catch systemic errors in 1m collection or rollup.

### 5.7 One-Click Comprehensive Go-Live Scoring
```bash
# Quick smoke scan
python cmd/test-tools/run_all_checks.py --quick

# Deep full scan (including Binance external reconciliation)
python cmd/test-tools/run_all_checks.py --vs-binance
```
- The console prints a full traffic-light scorecard and gives a final go-live decision: `READY FOR PRODUCTION DEPLOYMENT` or `DEPLOYMENT BLOCKED`.

### 5.8 Open-Interest Table Consistency Check (only when §3.4 is enabled)
```bash
# Whole market, last 24h (ClickHouse only, read-only)
python cmd/test-tools/check_oi_consistency.py

# Also compare a sample with Binance's own openInterestHist (5 requests on the /futures/data pool)
python cmd/test-tools/check_oi_consistency.py --vs-binance

# Right after the first hist-only day: demand that live rows were calibrated
python cmd/test-tools/check_oi_consistency.py --require-calibration --max-uncalibrated-hours 1
```
- It checks gaps, the 5-minute grid, `snap_time` semantics, freshness, calibration by hist, coverage against `fapi_kline_5m`, and per-bar cross-section completeness; exit `0` = no FAIL. It can also be run as part of the scorecard: `python cmd/test-tools/run_all_checks.py --oi`.
- If the table does not exist it says so and points at `004_fapi_oi.sql`.

---

## 6. Chaos and Resilience Drills

To verify high availability, the following drills are recommended in a test environment:

### Drill 1: Sudden WebSocket Network Outage and Automatic Gap-Filling (network-outage drill)
1. Observe that `livebar` and the sliding window are updating normally;
2. Unplug the outbound network connection or use a firewall to block Binance's WebSocket port for 30 seconds;
3. Restore the network;
4. **Expected behavior**:
   - The staleness watchdog triggers a disconnect/reconnect;
   - After reconnecting successfully, a `collector.OnGap` gap-fill event fires;
   - The window gate `windowgate.Hold` suspends closed-bar writes for the affected symbols;
   - The Backfill module automatically fetches the missing data from Binance REST starting at ClickHouse's latest timestamp;
   - Once filled, it reloads the ClickHouse tail to rebuild the Redis sliding window and releases the gate;
   - Run `python cmd/test-tools/check_clickhouse_integrity.py` to verify there are zero gaps.

### Drill 2: Temporary ClickHouse Outage and Write-Buffer Backoff
1. Stop the ClickHouse container: `docker stop chomo-clickhouse`;
2. Observe the ChomoSyncer logs: `BatchWriter` triggers exponential-backoff retries, and rows accumulate in the in-memory queue;
3. Restart ClickHouse within 15 seconds: `docker start chomo-clickhouse`;
4. **Expected behavior**: the connection self-heals, the backlogged kline data is successfully persisted in batch, and `clickhouse_rows_dropped_total` stays at 0.

### Drill 4: The Connector's 23-Hour Reconnect (silent gap) and the Sweep

The Binance connector re-creates each WebSocket connection every 23 hours without telling the application; a few 1m bars can be lost right after a minute boundary. Two independent layers cover it.
1. Look for `shard connection silently reconnected by the connector; requesting gap repair` in the log (one per shard per ~23h) and `ws_silent_reconnects_total` increasing; a `backfill request done reason=shard_reconnect` follows within seconds.
2. Every `sweep_every` the log shows `sweep done ... holes_found=… bars_repaired=… empty_at_exchange=…`; `backfill_sweep_last_success_timestamp_seconds` must stay fresh (alert if older than ~2 × `sweep_every`).
3. **Expected**: `python cmd/test-tools/check_clickhouse_integrity.py --intervals 1m --start-date "<a day ago>"` shows only holes at minutes with genuinely no trades. To rehearse without waiting 23h, delete a few 1m rows of a **scratch** database and point the service at it; the next sweep refills them.

### Drill 3: Open-Interest Module Down for a Few Hours (only when §3.4 is enabled)
1. Set both `open_interest` switches to `false` (or stop the service) for ~3 hours, then enable them again and restart;
2. **Expected behavior**:
   - The log shows `loaded open-interest state from ClickHouse` and a `reason=startup` hist pass with `cold_start=0` and about one request per symbol (a 3-hour gap is a single small page each; the pass is not spread out);
   - The bars of the outage are filled with Binance's own values (`src_rank = 2`); symbols that were already current make **no** request;
   - `oi_cross_section_complete_ratio` returns to ≈ 1;
   - `python cmd/test-tools/check_oi_consistency.py --vs-binance` shows zero missing bars.

---

## 7. Capacity Estimation and System Tuning

### 7.1 Symbol Scale and Throughput
- **Market-wide scale**: approximately 300–400 USDT perpetual contracts.
- **1m period throughput**: 300–400 closed bars produced per minute; approximately 5–10 per second.
- **Live Bar throughput**: approximately 500–2,000 transient updates per second.

### 7.2 Redis Resources and Policy
- **Memory footprint**:
  - Each closed sliding window is approximately 200 bars × 10 elements ≈ 33 KB;
  - 400 symbols × 2 periods (1m, 1h) ≈ 25 MB;
  - Live Bar snapshots for 400 symbols × 2 periods ≈ 2 MB;
  - Total steady-state Redis memory usage is typically under **100 MB**.
- **Key configuration**:
  - Be sure to set `--maxmemory 1gb` and configure `--maxmemory-policy noeviction` — it is preferable for writes to fail with an error than to allow eviction of the sliding windows.

### 7.3 ClickHouse Storage Growth Estimate
- A single compressed kline occupies approximately **30–40 bytes** of storage;
- For 400 symbols:
  - `1m` table monthly growth: `400 * 60 * 24 * 30 * 35 B ≈ 600 MB / month`;
  - `1h` table monthly growth: `400 * 24 * 30 * 35 B ≈ 10 MB / month`;
- Annual storage growth is under **10 GB**, which even low-end disks can sustain long-term.

---

## 8. Go-Live Checklist

- [ ] ClickHouse table creation is complete, and partitioning and ReplacingMergeTree have been confirmed correct;
- [ ] Redis is running with `noeviction` configured, and the connection pool and password are configured correctly;
- [ ] The `config.yaml` file or environment variables have been set to the production environment's addresses;
- [ ] `cmd/chomosyncer-go` has been run and the logs show no ERROR entries;
- [ ] Both the `/healthz` and `/readyz` probes have been checked and return HTTP 200;
- [ ] `python cmd/test-tools/run_all_checks.py` has been run and returns an all-green PASS verdict;
- [ ] Prometheus is scraping `/metrics` normally, with alerting rules configured for `ws_connection_status` and `ws_last_message_age_seconds`;
- [ ] *(only if open-interest sync is enabled)* `deploy/clickhouse/004_fapi_oi.sql` has been applied, `python cmd/test-tools/check_oi_consistency.py --vs-binance` passes, and alerts exist for `oi_hist_last_pass_timestamp_seconds` (stale), `oi_live_cycle_complete_ratio` (< 0.99) and `weightgate_used_weight_1m` (> 1800).
