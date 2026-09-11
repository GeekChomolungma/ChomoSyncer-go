> **Language:** English | [简体中文](ARCHITECTURE_MODULES.zh-CN.md)

# ChomoSyncer-go Module Architecture and Core Design Details

This document provides a detailed analysis of the design principles, internal mechanisms, concurrency model, fault-tolerance strategy, and interface specifications of each core business module in `ChomoSyncer-go`.

---

## Module Overview and Dependency Topology

```text
                  ┌──────────────────────┐
                  │  internal/universe   │ (market-wide dynamic symbol discovery)
                  └───────────┬──────────┘
                              │ OnChange(Symbols)
                              ▼
┌──────────────────────────────────────────────────────────┐
│                    internal/collector                    │ (WebSocket sharded stream ingestion)
│            crc32(symbol) % ShardsPerInterval             │
└─────────────────────────────┬────────────────────────────┘
                              │ KlineEvent (neutral event)
                              ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                              internal/dispatcher                              │ (dispatcher + cross-section aggregation)
│  monotonicity guard + four-way fan-out + timed cross-section-ready alignment  │
└───────┬─────────────────┬────────────────────────┬────────────────────────────┘
        │                 │                        │
        │Live Bar         │Closed Bar              │Closed Bar
        ▼                 ▼                        ▼
┌──────────────┐ ┌────────────────┐ ┌────────────────────────────┐
│ rediswin     │ │ windowgate     │ │ chwriter                   │
│ (LiveBar-    │ │ (window gate)  │ │ (ClickHouse batch writer)  │
│  Writer)     │ └────────┬───────┘ └──────────────┬─────────────┘
└──────────────┘          ▼                        │
                 ┌────────────────┐                │
                 │ rediswin       │                │
                 │ (SlidingWin)   │                │
                 └────────┬───────┘                │
                          │                        │
                          │ kline_ready Stream     │
                          ▼                        ▼
┌────────────────────────────────────────────────────────────────────────┐
│                           internal/backfill                            │ (historical backfill and zero-gap repair)
│  cold start / reconnect / window rebuild / token bucket rate limiting  │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Module 1: `internal/chwriter` (ClickHouse Batch Buffered Writer)

### 1.1 Responsibilities and Positioning
Provides high-throughput, low-overhead batch persistence capability for ClickHouse. This avoids the "part merge storm" (Too many parts) that high-frequency single-row inserts cause on LSM-tree-style engines.

> This build only collects the 1m base kline, so **only a single `BatchWriter` is instantiated**, writing to the sole raw table `market.fapi_kline_1m`. Coarser intervals such as `5m/15m/1h/4h/1d` are not written by Go; instead they are rollup tables idempotently recomputed on the ClickHouse side from `fapi_kline_1m FINAL` (see §8.6 of this document and `deploy/clickhouse/002_kline_rollups.sql`).

### 1.2 Core Mechanisms
- **Columnar batch writes**: Built on the official `clickhouse-go/v2` native TCP driver, with LZ4 data compression enabled.
- **Lock-free channel buffering**: An in-memory `chan Row` queue is maintained (default capacity 20,000).
  - The WebSocket real-time stream goes through `TryPush(row)`: non-blocking write; when the queue is full, rows are dropped and an alert metric is recorded;
  - The historical backfill stream goes through `Push(ctx, row)`: blocking enqueue, ensuring zero loss of backfilled historical data.
- **Dual-trigger flush rule** (whichever comes first):
  1. **Row count reached**: the queue accumulates up to `BatchSize` (default 5,000 rows);
  2. **Time elapsed**: the interval since the last flush reaches `FlushInterval` (default 1,000 ms);
  3. **Shutdown signal**: on process exit, a timed drain (Drain & Flush) is triggered.
- **Retry and connection self-healing**:
  - After a single batch write fails, exponential backoff retry with random jitter is performed (up to 5 times);
  - On connection loss or network exceptions, the underlying layer automatically discards the old connection and re-handshakes before the next write.

---

## Module 2: `internal/rediswin` (Redis Rolling Window, LiveBar, and Cross-Section Notification)

This package covers all core logic for the system's interaction with Redis, divided into three sub-functions:

### 2.1 Closed Rolling Window (`Writer`)
- **Storage structure**: Redis List, with the key named `kline:{SYMBOL}:1m`. This build only writes the 1m window; coarser intervals have no Redis window, and downstream consumers query the ClickHouse rollup tables instead (see §8.6).
- **Atomic monotonic push (`PushBarAndTrim`)**:
  - Uses a built-in Lua script to execute `LPUSH` + `LTRIM 0 199`;
  - **Monotonicity guard**: before pushing, the timestamp at the head of the list (`LINDEX 0`) is read; the push is only executed when the timestamp of the Bar to be written is strictly greater than the head's timestamp, preventing the rolling window's time from moving backward due to network reordering.
- **Batch cross-section push (`PushBarsAndTrim`)**:
  - A single-RTT pipeline batch-writes the closed Bars for hundreds of market-wide symbols, keeping write latency under 5ms.
- **Compact serialization**:
  - Uses a keyless 10-element compact JSON array: `[t, o, h, l, c, v, qv, tbv, tbqv, n]`, greatly reducing memory footprint and parsing/deserialization time on the Python side.

### 2.2 Unclosed Real-Time Snapshot (`LiveBarWriter`)
- **Storage structure**: Redis Hash, with the key named `livebar:{SYMBOL}:{interval}` (e.g., `livebar:BTCUSDT:1m`).
- **Dedicated connection pool and workers**:
  - A dedicated Redis Client is configured (small connection pool by default, a short 200ms timeout, no retries), preventing bursts of real-time market writes from contending with the closed rolling window for connection resources;
  - Internally it has its own `input chan` (capacity 8192) and a worker goroutine pool (2 workers by default).
- **Field contract**:
  - Overwrites 11 Hash fields: `t, o, h, l, c, v, qv, tbv, tbqv, n, x`;
  - Carries a millisecond-level TTL (default `2 * interval`, e.g. 120s for 1m), automatically expiring if not updated.
  - Optionally enabling `publish: true` synchronously PUBLISHes the real-time snapshot to the channel `livebar.<interval>`.

### 2.3 Cross-Section Ready Notification (`PublishKlineReady`)
- **Storage structure**: Redis Stream, with the key `stream:market:kline_ready`.
- **Notification payload**:
  - `interval`: the kline interval ("1m", "1h");
  - `timestamp`: the open-time millisecond timestamp of the closed period (`k.t`);
  - `symbols_count`: the number of symbols actually complete in the current cross-section.
- Uses approximate trimming with `MAXLEN ~ 10000` to prevent the Stream from growing unbounded.

---

## Module 3: `internal/collector` (Binance WebSocket Ingestion Layer)

### 3.1 Responsibilities and Characteristics
The only module that connects directly to the Binance derivatives WebSocket service. It shields downstream consumers from the structural details of the Binance SDK, outputting standardized `dispatcher.KlineEvent` directly.

### 3.2 Core Mechanisms
- **Deterministic sharding architecture (`sharding.go`)**:
  - Binance perpetual contracts do not support a market-wide aggregated stream, so each must be subscribed individually via `<symbol>@kline_<interval>`;
  - Sharding algorithm: `crc32(SYMBOL) % ShardsPerInterval` (4 shard connections per interval by default);
  - A fixed hash modulus guarantees that the mapping from symbol to shard is fully deterministic; when a new symbol lists or an old one delists, only the corresponding shard triggers an incremental `SUBSCRIBE` / `UNSUBSCRIBE`, with no need to rebuild the entire connection.
- **Storm prevention and staggered connection setup**:
  - A `connect_stagger` (default 300ms) is introduced between multiple shard connections for staggered connection setup, avoiding concurrent handshakes that would trigger the exchange's IP rate limiting.
- **Dual keepalive and watchdog (`shard.go`)**:
  - The underlying driver has built-in automatic Ping/Pong and a proactive, smooth 23-hour rebuild;
  - A top-level **staleness watchdog** is configured (default 60s): if a shard has not received any market data frame for longer than this duration (even if the TCP connection remains ESTABLISHED), it is judged a half-open/stale connection and is forcibly disconnected and reconnected.
- **Unlimited backoff reconnection**:
  - After a disconnect, it retries continuously with exponential backoff (1s → 30s) plus random jitter until recovery.

---

## Module 3b: `internal/universe` (Market-Wide Symbol Dynamic Discovery)

### 3.1 Responsibilities and Characteristics
- **Market-wide coverage principle**: manually setting volume, open-interest, or liquidity thresholds is strictly forbidden.
- Pulls all USDT perpetual contracts currently trading from the Binance futures REST API:
  `quoteAsset == "USDT" && contractType == "PERPETUAL" && status == "TRADING"`.

### 3.2 Refresh and Event Broadcasting
- **Daily scheduled refresh**:
  - Refreshes once every 24 hours by default, aligned to **2 minutes after UTC 00:00:00** each day (avoiding the exchange's daily settlement window);
- **Dynamic change broadcasting**:
  - When additions or removals in the symbol list are detected, an `OnChange` callback is triggered, hot-updating the `collector` subscription set and the `dispatcher` cross-section denominator.

---

## Module 4: `internal/dispatcher` (Kline Event Orchestration and Cross-Section Aggregator)

### 4.1 Four-Way Event Fan-Out
Receives the neutral `KlineEvent` (in this build `Interval` is always `1m`), and performs strict routing based on the `IsFinal` state machine:

| Event State | Live Snapshot (Redis) | Closed Rolling Window (Redis) | Historical Archive (ClickHouse) | Cross-Section Aggregator (Aggregator) |
| :--- | :---: | :---: | :---: | :---: |
| **Unclosed** (`IsFinal=false`) | ✅ `LiveSink.TryEnqueue` | ❌ Ignored | ❌ Ignored | ❌ Ignored |
| **Closed** (`IsFinal=true`) | ✅ `LiveSink.TryEnqueue` | ✅ `WindowSink.PushBarAndTrim` | ✅ `ArchiveSink.TryPush` | ✅ `aggregator.mark` |

### 4.1b Derived Cross-Section Signals (`serve_intervals`)

After the 1m cross-section is successfully published, the aggregator checks `serve_intervals` (default `5m,15m,1h,4h,1d`): if this 1m Bar's `openTime` happens to close some coarser bucket (`(openTime + 60000) % D == 0`), it additionally publishes a `kline_ready` with the same `symbols_count`, with `interval` labeled as that coarser interval and `timestamp` as the coarse bucket's open time. Derived signals are also constrained by the window gate — cascading only occurs when the base 1m cross-section is actually emitted (i.e., not suppressed by gapfill). The 200-bar window for coarser intervals does not go into Redis; downstream consumers that receive a derived signal query the ClickHouse rollup table directly.

### 4.2 Monotonicity Guard
Maintains `(symbol, interval) -> lastClosedOpenTime` state. If a closed frame with `<= lastClosedOpenTime` is received, it is immediately dropped as an out-of-order/duplicate frame, protecting downstream time-series monotonicity.

### 4.3 Cross-Section Aggregation Aligner (`Aggregator`)
- **Dual trigger mechanism**:
  1. **All-complete trigger (`onComplete`)**: fires immediately when the number of closed symbols within the period reaches the total number of symbols in the universe;
  2. **Fallback timeout trigger (`onTimeout`)**: a `section_timeout` timer (default 5s) starts after the first closed Bar arrives. If a few obscure symbols remain unclosed for a long time due to sparse trading, the timeout forces publication of a cross-section notification with whatever is currently complete.
- **Cross-section deduplication and retention eviction (`SectionRetention`)**:
  - After a cross-section is published, its state is retained in memory for one more period interval, preventing late-arriving frames from repeatedly triggering redundant `kline_ready` notifications.

---

## Module 5: `internal/metrics` (Observability and Monitoring Probes)

- **Unified private registry**: A dedicated registry is built on `prometheus.NewRegistry()`, without polluting the global default DefaultRegisterer.
- **HTTP probe routes**:
  - `GET /metrics`: the standard Prometheus scrape endpoint, aggregating metrics from each module;
  - `GET /healthz`: liveness probe, returns 200 while the process is alive;
  - `GET /readyz`: readiness probe, returns 503 while cold-start historical backfill is incomplete, and returns 200 once backfill is complete and the entire pipeline is ready.

---

## Module 6: `internal/app` + `cmd/chomosyncer-go` (Assembling and Lifecycle)

- **Module assembling**: dependency injection and network topology construction are performed in `internal/app`.
- **Graceful shutdown order**:
  After the process catches a `SIGINT` / `SIGTERM` signal, it performs a strict reverse-order teardown:
  `collector stops ingestion` → `dispatcher drains and closes` → `livebar writer closes` → `ClickHouse force-flushes to storage` → `backfill stops` → `close Redis clients` → `metrics shuts down`.

---

## Module 8: `internal/backfill` + `internal/windowgate` (Historical Backfill and Gap-Filling Design)

### 8.1 Background and Goals
Incremental WebSocket ingestion is a forward-only stream. When an **empty-database cold start**, a **restart from an existing historical archive**, or a **shard reconnect** occurs, data develops time gaps of varying degrees. The Backfill module is responsible for completing data recovery transparently in the background, ensuring the ClickHouse ledger has zero gaps, and rebuilding the Redis 200-bar short-term rolling window.

### 8.2 Three Gap-Filling Strategies
1. **Empty DB Cold Start**:
   - If the corresponding ClickHouse table has no historical records at all, the configuration parameter `cold_start_date` is read (e.g. `"2024-01-01"` or a specific timestamp);
   - Starting from that anchor time, data is fetched in segments of 1500 bars until the present, completing the historical cold-start sync; if unspecified, only the data needed for the most recent window is fetched.
2. **Existing History Cold Start**:
   - Reads the latest timestamp `max(start_time)` for that symbol from ClickHouse;
   - Syncs strictly starting from `max(start_time) + 1 step`, continuing forward to the current latest moment.
3. **Shard Reconnect Gapfill**:
   - No longer limited by a manually preset short-window cap; it directly queries the latest timestamp recorded in ClickHouse;
   - Using that timestamp as the sync starting point, it streams in all klines missing during the downtime, achieving absolute Zero Gap.

### 8.3 Window Gate Mechanism (`internal/windowgate`)
Backfill and real-time ingestion operate in parallel. To prevent contention between historical backfill and real-time writes:
- **Gate interception (`gate.Hold(keys)`)**:
  During backfill for a given symbol, its Redis closed-rolling-window push (`PushBarAndTrim`) and cross-section ready notification (`PublishKlineReady`) are temporarily suspended;
- **Full-speed unblocked path**:
  Real-time Live Bar snapshots and ClickHouse batch writes are **not subject to the gate** and flow through normally (ClickHouse relies on `ReplacingMergeTree`'s native deduplication);
- **Materialized rebuild and release (`gate.Release(keys)`)**:
  1. All backfilled data is written to ClickHouse and an explicit flush barrier is executed;
  2. The latest 200 bars for that symbol are read back from ClickHouse as the authoritative source;
  3. The Lua script `RebuildWindow` is executed to atomically replace the Redis rolling window;
  4. The gate is released, and the suspended real-time updates resume writing seamlessly.

### 8.4 REST Rate-Limit Protection (`BinanceFetcher`)
- Uses the token bucket rate-limiting algorithm (parameter `rest_rps`, default 20 RPS);
- Fetches with pagination (Binance's maximum of 1500 bars per call), with adaptive retries for network jitter, avoiding triggering the exchange's 429 / 418 IP bans.

### 8.5 Offline Backfill Mode (`backfill.offline_only`)

The gate is released as a fallback/safety-net by `gate_timeout` (default `5m`). When `cold_start_date` is set very far in the past and the market-wide historical backfill takes far longer than that duration, the gate is released prematurely, real-time business starts prematurely, and deep-history gaps turn into sticky gaps because real-time writes pollute `max(start_time)`.

To address this, `backfill.offline_only` is provided (in the config file; or `-backfill-offline-only` / `CHOMOSYNCER_BACKFILL_OFFLINE_ONLY`, default `false`):

- When `true`, only the **universe → REST fetch → ClickHouse persistence** chain is assembled; collector / dispatcher / rediswin / windowgate are **not started**;
- `gate_timeout` is automatically disabled (a negative sentinel value inside `internal/backfill` means "no upper bound"), so the full historical fetch is not forcibly released partway through;
- The process exits after performing one whole-universe cold-start backfill (exit code `0` on normal completion; a non-`0` code if interrupted by a signal, but progress has already been persisted and a rerun resumes from `max(start_time)`).

Combined with the two-phase process of "fill deep history offline → validate → start the online business with a `cold_start_date` covering only a small recent window," this ensures the Redis business is only turned on after historical persistence is complete. See `docs/OPERATIONS.md` §A.1 / §B.1 (first-time launch with an empty database) for details.

### 8.6 ClickHouse Rollups for Coarser Intervals (`deploy/clickhouse/002` + `003`)

The ingestion side only subscribes to 1m, avoiding the connection and bandwidth redundancy of "the same matching event running once each on separate 1m/1h WS links." All coarser intervals are derived by ClickHouse from `fapi_kline_1m`:

- **Target tables** `fapi_kline_{5m,15m,1h,4h,1d}`: `ReplacingMergeTree(rollup_version)`, with a schema identical to `fapi_kline_1m` and the same consumption pattern (`... FINAL`).
- **Refreshable materialized views** `*_rmv`: every 60–300s, **recompute** the buckets for the last few days from `fapi_kline_1m FINAL` and `APPEND` them. Buckets use `toStartOfInterval` (epoch-aligned; 5m/15m/1h/4h/1d boundaries match Binance's).
- **Hard idempotency guarantee**: the aggregation is a **full recomputation** using `sum()/argMin()/argMax()/min()/max()`, with no accumulator of any kind; `FINAL` first collapses duplicate 1m rows; each recomputation writes an updated `rollup_version`, and ReplacingMergeTree keeps the latest. As a result, 1m redelivery, overlapping backfills, and repeated reruns of `003` will **never double the volume**. Changing this to `SummingMergeTree` or a non-refreshable incremental MV is strictly forbidden — that would accumulate per inserted block and drift.
- **Historical folding** `003_rollup_backfill.sql`: the MV only consumes 1m rows arriving after its creation, so after `002` is applied, `003` must be run once (sharded by month, or all at once) to fold existing history into the rollup tables.
- **OHLC is exact; volume has ~1e-8 relative drift** (floating-point summation order vs. Binance accumulating from trades) — `check_vs_binance.py` uses a `1e-9` tolerance for price and `1e-6` for volume.
