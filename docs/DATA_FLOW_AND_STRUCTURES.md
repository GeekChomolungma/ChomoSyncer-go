> **Language:** English | [简体中文](DATA_FLOW_AND_STRUCTURES.zh-CN.md)

# ChomoSyncer-go Data Flow and Data Structures: Business Overview

> **Document Purpose**: This document provides an in-depth analysis of the business workflows, data flow pipelines, core concurrency/threading model, and data structure design of each storage/cache layer in the `ChomoSyncer-go` market-wide market-data collector. It focuses on clarifying **the key naming conventions, query command examples, return formats, and per-field business definitions for querying Redis / ClickHouse content from external callers**.  
> **Applicable Version**: `ChomoSyncer-go` v1.0+  
> **Core Packages Involved**: `cmd/chomosyncer-go`, `internal/app`, `internal/universe`, `internal/collector`, `internal/dispatcher`, `internal/rediswin`, `internal/windowgate`, `internal/chwriter`, `internal/backfill`  
>
> **Important (1m baseline + derived timeframes)**: This build **subscribes only to 1-minute klines**, persists only a single raw table `market.fapi_kline_1m`, and Redis maintains only the 1m `livebar:{SYM}:1m` and `kline:{SYM}:1m`. Wherever the text below refers to a `{interval}` Redis key, a `fapi_kline_<interval>` table, or phrases such as "each configured interval", the actual value is always `1m`. Coarser timeframes (`serve_intervals`, defaulting to `5m/15m/1h/4h/1d`) are derived in two places: (1) on the ClickHouse side, rollup tables `fapi_kline_{5m,15m,1h,4h,1d}` idempotently recomputed from `fapi_kline_1m FINAL` (`deploy/clickhouse/002_kline_rollups.sql`); (2) on `stream:market:kline_ready`, a cross-section signal derived and forwarded from the "end-of-bucket 1m cross-section". Coarser timeframes have no Redis window/livebar — downstream consumers query ClickHouse directly.

---

## Overall Architecture & Data Flow Topology Diagram

```text
                                  ┌───────────────────────────────┐
                                  │      Binance REST API         │
                                  │   GET /fapi/v1/exchangeInfo   │
                                  └───────────────┬───────────────┘
                                                  │ (polled every 24h)
                                                  ▼
                                      ┌───────────────────────┐
                                      │    Universe Monitor   │ (market-wide list of USDT perpetual contract symbols)
                                      └───────────┬───────────┘
                                                  │ OnChange-driven subscription alignment
                         ┌────────────────────────┴────────────────────────┐
                         │                                                 │
                         ▼                                                 ▼
        ┌──────────────────────────────────┐             ┌──────────────────────────────────┐
        │  WebSocket Sharded Ingestion     │             │     Historical Gapfill Flow      │
        │  (sharded by interval + crc32)   │             │  (cold start/reconnect/new list) │
        └────────────────┬─────────────────┘             └────────────────┬─────────────────┘
                         │ raw KlineEvent                                 │ 1. Hold(keys) gate lock
                         ▼                                                 ▼
        ┌──────────────────────────────────┐             ┌──────────────────────────────────┐
        │       Kline Dispatcher           │             │   windowgate.Gate (window gate)  │
        │ (monotonicity guard + closed/live│             │ (intercepts live writes & ready  │
        │  four-way dispatch)              │             │  for gated keys)                 │
        └─┬──────────────┬─────────────────┘             └────────────────┬─────────────────┘
          │              │                                                 │ 2. REST backfill of history
          │ unclosed     │ closed frame (k.x == true)                     ▼
          │ frame (all)  ├───────────────────────────────► ┌────────────────────────────────┐
          │              │                                 │     chwriter (ClickHouse)      │
          │              │                                 │   ReplacingMergeTree idempotent│
          │              │                                 │   append                       │
          │              │                                 └──────────────┬─────────────────┘
          │              │                                                │ 3. wait for flush to persist
          │              │                                                ▼
          │              │                                 ┌────────────────────────────────┐
          │              │                                 │   CHStore.LastBars (read back  │
          │              │                                 │   tail)                        │
          │              │                                 └──────────────┬─────────────────┘
          │              │                                                │ 4. RebuildWindow (Lua)
          │              │                                                ▼
          │              │ filtered by gate                ┌────────────────────────────────┐
          │              ├───────────────────────────────► │   Redis Closed Rolling Window  │
          │              │                                 │   kline:{SYM}:{iv} (List 200)  │
          │              │                                 └──────────────┬─────────────────┘
          │              │                                                │ 5. Release(keys) unlock
          │              │ filtered by gate                               ▼
          │              ├───────────────────────────────► ┌────────────────────────────────┐
          │              │ (full cross-section complete    │   stream:market:kline_ready    │
          │              │  or timeout)                    │   (Redis Stream cross-section  │
          │              │                                 │    ready notification)         │
          │              │                                 └────────────────────────────────┘
          ▼              ▼
 ┌──────────────────────────────────┐
 │      Redis Live Bar Snapshot     │
 │  livebar:{SYM}:{iv} (Hash snapshot) │
 └──────────────────────────────────┘
```

---

## 1. Dynamic Market-Wide Symbol Monitoring Flow (Universe Monitor Flow)

> **External Storage Interaction**: This workflow is a pure in-memory/REST protocol flow and does not directly write to or persist data in Redis or ClickHouse.

### 1.1 Purpose & Goals
This system is a **market-wide market-data collector**, and strictly forbids introducing any liquidity or volume threshold filters. The dynamic Universe is responsible for periodically pulling all tradable USDT perpetual contract symbols from the Binance Futures REST API.
This set serves as:
1. **The complete subscription set for the WebSocket Collector**: determines which streams the Collector should maintain;
2. **The total denominator for cross-section readiness computation (`kline_ready`)**: determines the total number of symbols that should close within a given period market-wide;
3. **The trigger source for cold start and dynamic scale-out**: when a newly listed symbol is discovered, it dynamically schedules backfill and shard onboarding.

### 1.2 Core Caller, Concurrency Model & Code Entry Points
- **Core Caller**: [`universe.Monitor`](../internal/universe/monitor.go)
- **Concurrency & Invocation Model**:
  - **Single-goroutine scheduled-alignment polling**: at startup, the main thread synchronously executes one `Refresh`, after which a dedicated background goroutine runs `loop(ctx)`.
  - **Scheduled alignment mechanism**: aligns every 24 hours to `+2min` (`DefaultRefreshOffset`) after UTC midnight `00:00:00`, triggering after exchange listing/delisting settlement has stabilized.
  - **Broadcast pattern**: after `Refresh` succeeds, it holds a read-write lock to update the internal snapshot, then synchronously invokes the registered callback functions `subs []func(Snapshot)` (i.e., `app.onUniverseChange`) outside the lock.
- **Code Entry Points**:
  - Startup entry point: [`internal/universe/monitor.go:141`](../internal/universe/monitor.go#L141) (`Monitor.Start`)
  - Background loop: [`internal/universe/monitor.go:160`](../internal/universe/monitor.go#L160) (`Monitor.loop`)
  - Refresh logic: [`internal/universe/monitor.go:199`](../internal/universe/monitor.go#L199) (`Monitor.Refresh`)
  - Business consumption and dispatch: [`internal/app/app.go:217`](../internal/app/app.go#L217) (`App.onUniverseChange`)

### 1.3 Data Structures & Schema Design
#### Network Layer (Binance REST API)
- **Endpoint path**: `GET https://fapi.binance.com/fapi/v1/exchangeInfo`
- **Raw JSON filtering rules** (hard filters):
  ```go
  // All three hard conditions must be satisfied simultaneously:
  s.QuoteAsset == "USDT" && s.ContractType == "PERPETUAL" && s.Status == "TRADING"
  ```

#### In-Memory Data Structures
- **Snapshot struct** ([`universe.Snapshot`](../internal/universe/monitor.go#L61)):
  ```go
  type Snapshot struct {
      Symbols     []string  // All qualifying symbols, lexicographically sorted, e.g. ["1000BONKUSDT", "BTCUSDT", ...]
      RefreshedAt time.Time // UTC refresh timestamp
  }
  ```
- **Set index**: internally maintains `set: map[string]struct{}`, providing `O(1)`-complexity `Has(symbol string) bool` and `Size() int` queries.

---

## 2. WebSocket Multi-Shard Persistent-Connection Ingestion Flow (WebSocket Sharded Ingestion Flow)

> **External Storage Interaction**: This workflow is responsible for keeping long-lived connections alive and deserializing messages, then handing them off to the in-memory Dispatcher; it does not directly expose Redis or ClickHouse queries externally.

### 2.1 Purpose & Goals
Because Binance USDⓈ-M perpetual contracts **have no market-wide aggregated kline stream** (`!kline_<interval>@arr` is only available on spot), the system must subscribe to `<symbol>@kline_<interval>` per symbol individually. With nearly 500 market-wide symbols across multiple timeframes, cramming them into a single connection would exceed the hard limit of 200 streams per connection.
Therefore, the Collector employs stable consistent-hash sharding, maintaining multiple highly available WebSocket connections, and provides exponential-backoff reconnection, an anti-stall watchdog, and disconnect gap detection.

### 2.2 Core Caller, Concurrency Model & Code Entry Points
- **Core Caller**: [`collector.Collector`](../internal/collector/collector.go#L162) and [`collector.shard`](../internal/collector/shard.go#L41)
- **Concurrency & Goroutine Model**:
  - **Single-interval hash sharding**: subscribes only to `1m`, bucketed by `crc32(SYMBOL) % ShardsPerInterval` (4 shards by default).
    - **Shard naming and ID**: `kline_1m_<bucket>` (`kline_1m_0` … `kline_1m_3`). Each shard carries roughly 100~130 streams, well below the 200 limit.
    - Reason for no longer opening a separate set of shards per interval: coarser timeframes are stacks of 1m bars, and pushing the same matched trade event redundantly across both a 1m and a 1h pipeline is pure waste. See `docs/ARCHITECTURE_MODULES.md` §8.6.
  - **Goroutine Partitioning & Scheduling**:
    - **Each shard has its own dedicated supervisor goroutine**: started via `go s.run(c.rootCtx)` in [`Collector.SetSymbols`](../internal/collector/collector.go#L233).
    - **Staggered connection establishment**: before a new shard starts, it is forced to sleep for a staggered delay (`startDelay += c.cfg.connectStagger`, 300ms by default) to avoid a burst of simultaneous handshakes being rate-limited by Binance's IP throttling.
    - **Concurrent frame reading**: the underlying official library `binance-connector-go`'s `OnMarket` callback spawns a temporary goroutine calling [`handleRaw`](../internal/collector/binanceclient.go#L116) for each incoming message, so event calls entering the ingestion pipeline are **highly concurrent, multi-goroutine**.
    - **Keep-alive and disconnect self-healing**:
      - The official connector handles server-side pings by automatically replying with pongs, and proactively rebuilds the connection every 23h.
      - The Shard Supervisor internally runs a [`watchdogInterval`](../internal/collector/shard.go#L180) (10s) watchdog: if more than `StaleTimeout` (60s by default) has elapsed since the last received message, it is judged a half-open TCP connection and is proactively torn down and reconnected.
      - Reconnection uses exponential backoff with half-jitter (1s ~ 30s). After a successful reconnect, the disconnection gap `[lastMsgAt, reconnectAt]` is computed and delivered to the backfill module via the `OnGap` callback.
- **Code Entry Points**:
  - Shard computation: [`internal/collector/sharding.go:35`](../internal/collector/sharding.go#L35) (`planStreams`)
  - Shard topology alignment: [`internal/collector/collector.go:211`](../internal/collector/collector.go#L211) (`Collector.SetSymbols`)
  - Shard management loop: [`internal/collector/shard.go:94`](../internal/collector/shard.go#L94) (`shard.run`), [`shard.connectAndPump:135`](../internal/collector/shard.go#L135)
  - Frame reading entry point: [`internal/collector/binanceclient.go:116`](../internal/collector/binanceclient.go#L116) (`binanceStreamClient.handleRaw`)
  - Event handoff: [`internal/collector/shard.go:84`](../internal/collector/shard.go#L84) (`shard.onEvent`)

### 2.3 Data Structures & Schema Design
#### WebSocket Combined Stream Protocol
- **Subscription stream format**: `<symbol_lowercase>@kline_<interval>` (e.g. `btcusdt@kline_1m`)
- **Binance push JSON format**:
  ```json
  {
    "stream": "btcusdt@kline_1m",
    "data": {
      "e": "kline",
      "E": 1719835260000,
      "s": "BTCUSDT",
      "k": {
        "t": 1719835200000,  // kline open time (ms) -> unified primary-key baseline
        "T": 1719835259999,  // kline close time (ms)
        "s": "BTCUSDT",      // symbol
        "i": "1m",           // interval
        "o": "60250.50",     // open price
        "c": "60720.00",     // close price (latest price)
        "h": "60800.00",     // high price
        "l": "60100.20",     // low price
        "v": "12450.85",     // base asset volume
        "q": "75602300.50",  // quote (USDT) volume
        "V": "6120.40",      // taker buy base asset volume
        "Q": "37150000.20",  // taker buy quote asset volume
        "n": 1420,           // number of trades
        "x": true            // key: is this bar officially closed (is_bar_final)
      }
    }
  }
  ```

#### Internal Event Struct
- **Unified internal struct** ([`dispatcher.KlineEvent`](../internal/dispatcher/event.go#L21)):
  ```go
  type KlineEvent struct {
      Symbol              string // "BTCUSDT"
      Interval            string // "1m", "1h"
      OpenTime            int64  // k.t (ms)
      CloseTime           int64  // k.T (ms)
      Open                string // raw string retained; parsed centrally by dispatcher
      High                string
      Low                 string
      Close               string
      Volume              string
      QuoteVolume         string
      TakerBuyVolume      string
      TakerBuyQuoteVolume string
      TradeCount          int64  // k.n
      IsFinal             bool   // k.x
  }
  ```

---

## 3. Event Routing & Monotonicity Guard Flow (Dispatcher & Monotonic Guard Flow)

> **External Storage Interaction**: This workflow is an in-memory buffering and dispatch hub that distributes data via channels to each downstream storage sink (Redis / ClickHouse); it has no independent storage key of its own.

### 3.1 Purpose & Goals
The Dispatcher is the data hub of the entire system, responsible for three core business functions:
1. **Separation of unclosed and closed data**: all events flow into the real-time snapshot, while closed events (`k.x == true`) flow into the rolling window, archive, and aggregation;
2. **Monotonicity guard on close**: network disconnects/reconnects, out-of-order delivery, or retries can cause an older bar to arrive late. The Dispatcher strictly validates the open-time timestamp and discards any duplicate or out-of-order closed frame with `<= the last processed open time`, preventing the timeline from going backward;
3. **Asynchronous decoupling and buffering**: the WS frame-reading thread never performs storage I/O directly; it delivers work to workers via an in-memory channel.

### 3.2 Core Caller, Concurrency Model & Code Entry Points
- **Core Caller**: [`dispatcher.Dispatcher`](../internal/dispatcher/dispatcher.go#L106)
- **Concurrency & Goroutine Model**:
  - **Upstream concurrent entry point**: [`HandleKlineEvent`](../internal/dispatcher/dispatcher.go#L177) is called lock-free and concurrently by multiple Collector WS callback threads.
  - **Monotonicity guard lock**: [`acceptClosed`](../internal/dispatcher/dispatcher.go#L234) uses a mutex `guardMu sync.Mutex` to protect `lastClosed: map[string]int64` (keyed by `symbol|interval`), performing only brief in-memory lookups/updates on closed frames.
  - **Closed queue and worker pool**:
    - Queue: `closedCh: chan closedJob`, with capacity `ClosedQueueSize` (4096 by default). If the queue is full, `ErrBusy` is returned immediately and the item is dropped, protecting the main ingestion stream from blocking deadlock.
    - Consumers: a fixed number of **Closed Workers** (4 goroutines by default) run [`d.worker(ctx)`](../internal/dispatcher/dispatcher.go#L245), concurrently pulling tasks off `closedCh` for processing.
  - **Processing order**: each worker strictly follows this order when processing: **write the Redis rolling window first -> then dispatch to the ClickHouse archive -> finally notify the cross-section aggregator**, ensuring that by the time downstream strategies receive a signal, the Redis data is guaranteed to already be readable.
- **Code Entry Points**:
  - Dispatch routing: [`internal/dispatcher/dispatcher.go:177`](../internal/dispatcher/dispatcher.go#L177) (`HandleKlineEvent`)
  - Monotonicity check: [`internal/dispatcher/dispatcher.go:234`](../internal/dispatcher/dispatcher.go#L234) (`acceptClosed`)
  - Closed worker consumption: [`internal/dispatcher/dispatcher.go:245`](../internal/dispatcher/dispatcher.go#L245) (`worker`), [`handleClosed:273`](../internal/dispatcher/dispatcher.go#L273)

### 3.3 Data Structures & Schema Design
#### Internal Job Struct
- **Internal closed-queue element** ([`dispatcher.closedJob`](../internal/dispatcher/dispatcher.go#L94)):
  ```go
  type closedJob struct {
      symbol   string               // symbol, uppercase
      interval string               // interval
      openTime int64                // millisecond timestamp (k.t)
      bar      rediswin.CompactBar  // compact 9-element array for Redis
      row      chwriter.Row         // columnar row struct for ClickHouse
      hasRow   bool                 // whether a Row was successfully parsed
  }
  ```

---

## 4. Redis Real-Time Snapshot Layer Flow (Redis Live Bar Snapshot Flow - livebar)

### 4.1 External Storage Query Contract (Redis Query Contract)

#### 1. Key Naming Pattern
- **Storage structure**: Redis **`Hash`**
- **Key naming convention**: `livebar:{SYMBOL}:{interval}` (**symbol must be all uppercase**, colon-separated)
  - Examples: `livebar:BTCUSDT:1m`, `livebar:ETHUSDT:1h`, `livebar:SOLUSDT:15m`
- **Lifecycle TTL**: carries an automatically expiring `PEXPIRE` with a duration of `2 × interval` (e.g. TTL is 120 seconds for the 1m interval, 2 hours for the 1h interval).
- **Optional Pub/Sub channel**: `livebar.{interval}` (e.g. `livebar.1m`, `livebar.1h`).

#### 2. External Query Commands & Code Examples
```bash
# CLI: view the current, still-forming instantaneous kline snapshot for BTCUSDT 1m
redis-cli HGETALL livebar:BTCUSDT:1m

# CLI: read several key fields (e.g. open time, current price, volume, closed status)
redis-cli HMGET livebar:BTCUSDT:1m t c v x

# CLI: subscribe to the market-wide 1-minute real-time broadcast (requires live-publish enabled)
redis-cli SUBSCRIBE livebar.1m
```

```python
# Python query example (redis-py)
import redis

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# 1. Batch-read the real-time shape of multiple symbols (pipeline)
pipe = r.pipeline()
symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
for sym in symbols:
    pipe.hgetall(f"livebar:{sym}:1m")
live_bars = pipe.execute()  # returns [{'t': '1719835200000', 'c': '60720.0', ...}, ...]

# 2. Convert to strongly-typed fields
current_price = float(live_bars[0]['c'])
is_final = (live_bars[0]['x'] == '1')
```

#### 3. Return Value Format & Per-Field Structure
- **HGETALL return value format**: a dictionary/map of string key-value pairs.
- **Field structure and business meaning**:

| Hash Field | Data Type | Example Value | Corresponding Binance Raw Field | Business Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `t` | `int64` string | `"1719835200000"` | `k.t` | Kline open timestamp (milliseconds) |
| `o` | `float64` string | `"60250.5"` | `k.o` | Open price of this kline |
| `h` | `float64` string | `"60800.0"` | `k.h` | Current high price of this kline |
| `l` | `float64` string | `"60100.2"` | `k.l` | Current low price of this kline |
| `c` | `float64` string | `"60720.0"` | `k.c` | Latest matched trade price (current instantaneous price) |
| `v` | `float64` string | `"12450.85"` | `k.v` | Current cumulative volume (base asset amount) |
| `qv` | `float64` string | `"75602300.5"` | `k.q` | Current cumulative turnover (quote asset, USDT) |
| `tbv` | `float64` string | `"6120.40"` | `k.V` | Taker buy volume (base) |
| `tbqv` | `float64` string | `"37150000.20"` | `k.Q` | Taker buy turnover (quote, USDT) |
| `n` | `int64` string | `"1420"` | `k.n` | Total number of trades produced so far in this kline |
| `x` | `int` string | `"0"` or `"1"` | `k.x` | Whether this bar has officially closed (`0` = still forming, `1` = just closed) |

- **Pub/Sub payload return format**: an 11-element compact, keyless JSON array string:
  `[1719835200000, 60250.5, 60800.0, 60100.2, 60720.0, 12450.85, 75602300.5, 6120.40, 37150000.20, 1420, 0]`

---

### 4.2 Purpose & Goals
Provides the current instantaneous shape of the forming (or just-closed) kline for **real-time consumers independent of strategy computation**, such as order-book monitoring, real-time alerting, and front-end dashboards.
- **Isolation**: the window depth is always 1 — it does not retain intra-bar historical evolution, does not participate in any backtest alignment, and **never triggers `kline_ready`**.
- **High throughput, non-blocking**: real-time data updates at extremely high frequency (~250ms/frame), using fire-and-forget semantics — dropped when full (dropping a frame is harmless, since the next frame overwrites it immediately).

### 4.3 Core Caller, Concurrency Model & Code Entry Points
- **Core Caller**: [`rediswin.LiveBarWriter`](../internal/rediswin/livebar_writer.go#L187)
- **Concurrency & Goroutine Model**:
  - **Producer enqueue**: internally, `dispatcher.HandleKlineEvent` submits the snapshot into a channel via [`TryEnqueue`](../internal/rediswin/livebar_writer.go#L235). It uses `select default`; when the channel is full it is instantly dropped and `redis_livebar_dropped_total` is incremented, and it never blocks.
  - **Worker pool consumption**:
    - Buffer queue: `input: chan liveJob` (capacity 8192 by default).
    - Consumers: a configured number of worker goroutines (2 by default, `Workers`) run [`w.worker(ctx)`](../internal/rediswin/livebar_writer.go#L299), competing to read tasks from `input`.
  - **Connection pool and physical resource isolation**:
    - Uses a dedicated `*redis.Client` (built via [`LiveClientOptions`](../internal/rediswin/livebar_writer.go#L117): PoolSize 8, MinIdleConns 2, DialTimeout 2s, Read/WriteTimeout 300ms, MaxRetries -1).
    - This avoids high-frequency real-time writes saturating the connection pool and impeding the critical path of the closed rolling window and `kline_ready`.
- **Code Entry Points**:
  - Enqueue entry point: [`internal/rediswin/livebar_writer.go:235`](../internal/rediswin/livebar_writer.go#L235) (`LiveBarWriter.TryEnqueue`)
  - Consumption and Redis write: [`internal/rediswin/livebar_writer.go:299`](../internal/rediswin/livebar_writer.go#L299) (`worker`), [`write:329`](../internal/rediswin/livebar_writer.go#L329)

---

## 5. Redis Closed Fixed-Length Rolling Window Cache Flow (Redis Closed Rolling Window Flow - kline)

### 5.1 External Storage Query Contract (Redis Query Contract)

#### 1. Key Naming Pattern
- **Storage structure**: Redis **`List`** (fixed length of 200 bars)
- **Key naming convention**: `kline:{SYMBOL}:{interval}` (**symbol must be all uppercase**, colon-separated)
  - Examples: `kline:BTCUSDT:1h`, `kline:ETHUSDT:1m`, `kline:SOLUSDT:1h`
- **Ordering direction**: **newest-first (the most recently closed bar sits at leftmost index 0)**.

#### 2. External Query Commands & Code Examples
```bash
# CLI: read the full set of 200 closed klines for BTCUSDT 1h
redis-cli LRANGE kline:BTCUSDT:1h 0 199

# CLI: read the most recently closed kline (index 0)
redis-cli LINDEX kline:BTCUSDT:1h 0

# CLI: query the number of bars currently cached in the window (200 at steady state)
redis-cli LLEN kline:BTCUSDT:1h
```

```python
# Python strategy-layer single-RTT batch read of the market-wide cross-section window (recommended pattern)
import redis, json
import polars as pl

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Obtain the Universe's list of active symbols
universe_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "..."]

# Pull all Lists in a single network round trip via pipeline
pipe = r.pipeline()
for sym in universe_symbols:
    pipe.lrange(f"kline:{sym}:1h", 0, 199)
batch_results = pipe.execute()

# Parse into a tensor or Polars DataFrame
market_matrix = {}
for sym, raw_bars in zip(universe_symbols, batch_results):
    # raw_bars contains up to 200 JSON array strings
    parsed_bars = [json.loads(b) for b in raw_bars]
    market_matrix[sym] = parsed_bars
```

#### 3. Return Value Format & Per-Field Structure
- **LRANGE return value format**: a string list `List[string]`, with at most 200 elements.
  - Index `0`: the **newest** closed kline;
  - Index `199`: the **oldest** closed kline.
- **Single list element value structure**: to save 60%+ memory, **keyed JSON objects are not used**; instead a uniform **9-element compact JSON array** is used:
  ```json
  [1719835200000, 60250.5, 60800.0, 60100.2, 60720.0, 12450.85, 75602300.5, 6120.4, 37150000.2]
  ```
- **Exact field definitions for the 9 array positions**:

| Array Index | Field Name | Data Type | Example Value | Business Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `[0]` | `start_time` | `int64` (ms) | `1719835200000` | Kline open time in milliseconds (`k.t`), the alignment baseline primary key |
| `[1]` | `open` | `float64` | `60250.5` | Open price |
| `[2]` | `high` | `float64` | `60800.0` | High price |
| `[3]` | `low` | `float64` | `60100.2` | Low price |
| `[4]` | `close` | `float64` | `60720.0` | Close price (the key reference price for strategy computation) |
| `[5]` | `volume` | `float64` | `12450.85` | Total base asset traded volume |
| `[6]` | `quote_volume` | `float64` | `75602300.5` | Total traded turnover (quote volume, USDT) |
| `[7]` | `taker_buy_volume` | `float64` | `6120.40` | Taker buy volume (used to compute CVD) |
| `[8]` | `taker_buy_quote_volume` | `float64` | `37150000.20` | Taker buy turnover (USDT) |

---

### 5.2 Purpose & Goals
Provides millisecond-level, lock-free market-wide slice queries dedicated to Python feature engineering and strategy computation.
- **Fixed-length constraint**: strictly locks the window length to **200 bars** (the newest bar sits at List index 0), keeping memory usage constant.
- **Monotonic idempotent writes**: a Redis-internal Lua script performs a timestamp comparison, allowing `LPUSH` only when the new bar's `start_time > the current head's start_time`, guaranteeing strict idempotency during the concurrent handoff between historical backfill and the live stream.

### 5.3 Core Caller, Concurrency Model & Code Entry Points
- **Core Caller**: [`rediswin.Writer`](../internal/rediswin/writer.go#L62)
- **Concurrency & Goroutine Model**:
  - **Caller**: concurrently invoked by the Dispatcher's 4 `ClosedWorkers`.
  - **Gate wrapping**: before entering `rediswin.Writer`, requests are intercepted by [`windowgate.Gate.WrapWindow`](../internal/windowgate/gate.go#L126). If the current key is currently undergoing a gapfill backfill, the live write is silently dropped (the authoritative tail is subsequently read back from ClickHouse, guaranteeing consistency).
  - **Atomic execution**: completed within a single network RTT via the precompiled Lua script [`monotonicPushScript`](../internal/rediswin/writer.go#L27).
- **Code Entry Points**:
  - Gate filtering: [`internal/windowgate/gate.go:140`](../internal/windowgate/gate.go#L140) (`gatedWindow.PushBarAndTrim`)
  - Write entry point: [`internal/rediswin/writer.go:103`](../internal/rediswin/writer.go#L103) (`Writer.PushBarAndTrim`)
  - Lua script definition: [`internal/rediswin/writer.go:27`](../internal/rediswin/writer.go#L27) (`monotonicPushScript`)

---

## 6. Market-Wide Cross-Section State Aggregation & Trigger Flow (Cross-Section Aggregator & Kline Ready Stream Flow)

### 6.1 External Storage Query Contract (Redis Stream Contract)

#### 1. Key Naming Pattern
- **Storage structure**: Redis **`Stream`**
- **Stream Key**: `stream:market:kline_ready` (fixed namespace)
- **Capacity cap**: writes carry `MAXLEN ~ 1000`, retaining roughly the most recent 1000 cross-section trigger records with automatic trimming.

#### 2. External Query & Listening Command Examples
```bash
# CLI: block in real time listening for the next incoming cross-section-ready event (block 0 ms means wait forever)
redis-cli XREAD BLOCK 0 STREAMS stream:market:kline_ready $

# CLI: view the 5 most recently triggered cross-section history events
redis-cli XREVRANGE stream:market:kline_ready + - COUNT 5

# CLI: view the stream's queue length and metadata
redis-cli XLEN stream:market:kline_ready
```

```python
# Python strategy-engine event-driven listening main loop
import redis

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
stream_key = "stream:market:kline_ready"
last_id = "$"  # "$" means only listen for new messages arriving after startup

print("Python Strategy Engine listening for kline_ready...")
while True:
    # Block waiting for the cross-section to become ready
    response = r.xread({stream_key: last_id}, block=0, count=1)
    for stream, entries in response:
        for entry_id, fields in entries:
            last_id = entry_id
            interval = fields['interval']
            ts = int(fields['timestamp'])
            count = int(fields['symbols_count'])
            print(f"[EVENT] cross-section closed and ready: interval={interval}, timestamp={ts}, symbols aligned={count}")
            # Immediately trigger a market-wide pipeline pull of 200 bars and recompute features!
```

#### 3. Return Value Format & Per-Field Structure
- **XREAD / XREVRANGE return value format**:
  Each event carries a unique `Entry ID` (e.g. `1719835201042-0`, composed of a millisecond timestamp and an auto-increment sequence number) plus its associated dictionary of fields.
- **Field key-value structure definitions**:

| Field | Data Type | Example Value | Business Meaning & Consumer Behavior |
| :--- | :--- | :--- | :--- |
| `interval` | `string` | `"1m"` (native) or `"5m"/"1h"/…` (derived) | The closed kline interval. `1m` is published natively by the aggregator; coarser intervals in `serve_intervals` derive and forward one entry when their "end-of-bucket 1m cross-section" becomes ready, with `symbols_count` inherited from the 1m cross-section. Upon receiving a coarser-interval signal, query `market.fapi_kline_<interval> FINAL` directly. |
| `timestamp` | `int64` string | `"1719835200000"` | The baseline open millisecond timestamp (`k.t`) of the just-closed cross-section. Used by the strategy layer to align cross-timeframe cross-section matrices. |
| `symbols_count` | `int` string | `"182"` | The number of symbols actually successfully archived and updated in the rolling window for this cross-section. The strategy layer can use this to verify cross-section coverage. |

---

### 6.2 Purpose & Goals
Responsible for tracking, for the entire market (Universe), the closing progress of all contracts for each interval at the same open timestamp. When all symbols within a cross-section have closed (or a timeout fallback is triggered), a cross-section-ready event is published to the Redis Stream, notifying the Python strategy engine to immediately perform a stateless pull of the matrix and run feature computation.

### 6.3 Core Caller, Concurrency Model & Code Entry Points
- **Core Caller**: [`dispatcher.aggregator`](../internal/dispatcher/aggregator.go#L27)
- **Concurrency & Goroutine Model**:
  - **Trigger entry point**: after a successful write to the Redis and ClickHouse buffers, one of the 4 Dispatcher workers calls [`agg.mark(symbol, interval, openTime)`](../internal/dispatcher/aggregator.go#L67).
  - **Concurrent state mutex**: `mark` uses `mu sync.Mutex` to protect the cross-section state dictionary `sections: map[sectionKey]*sectionState`.
  - **Countdown and dual-trigger mechanism**:
    1. **Full-attendance trigger (`complete`)**: a countdown timer (5s `SectionTimeout` by default) starts when the cross-section receives its first bar. If the number of symbols collected reaches `universe.Size()` before the timeout, the timer is stopped immediately and publication is triggered.
    2. **Timeout fallback trigger (`timeout`)**: if some symbols trade too sparsely to produce a bar on time at period end, when the timer expires a goroutine automatically triggers a timeout publication, carrying the currently received symbol count, preventing the strategy engine from being blocked indefinitely.
  - **Cross-section deduplication and memory eviction (retention)**: after publication, the cross-section state is retained in memory for the duration of that interval (e.g. 1m is retained for 1 minute), absorbing any subsequently late residual frames and preventing a second publication for the same cross-section; after expiry, a timer performs `evict` to free the memory.
  - **Gate suppression**: publication passes through [`windowgate.Gate.WrapReady`](../internal/windowgate/gate.go#L131). If any key for that interval is currently held/locked by an in-progress backfill, this publication is silently dropped, preventing the strategy engine from reading a half-ready, dirty cross-section.
- **Code Entry Points**:
  - Cross-section progression: [`internal/dispatcher/aggregator.go:67`](../internal/dispatcher/aggregator.go#L67) (`aggregator.mark`)
  - Timeout callback: [`internal/dispatcher/aggregator.go:104`](../internal/dispatcher/aggregator.go#L104) (`aggregator.onTimeout`)
  - Publish entry point: [`internal/dispatcher/aggregator.go:118`](../internal/dispatcher/aggregator.go#L118) (`aggregator.publish`)
  - Gate filtering: [`internal/windowgate/gate.go:153`](../internal/windowgate/gate.go#L153) (`gatedReady.PublishKlineReady`)
  - Redis XADD execution: [`internal/rediswin/writer.go:268`](../internal/rediswin/writer.go#L268) (`Writer.PublishKlineReady`)

---

## 7. ClickHouse Batch Time-Series Archive Flow (ClickHouse Batch Ingestion Flow)

### 7.1 External Storage Query Contract (ClickHouse Query Contract)

#### 1. Database & Table Naming Pattern
- **Database name**: `market`
- **Raw table**: `market.fapi_kline_1m` — the only table written directly by the collector. Engine `ReplacingMergeTree(created_at)`, primary/sort key `(symbol, start_time)`, partitioned by `toYYYYMM(start_time)`.
- **Derived rollup tables**: `market.fapi_kline_{5m,15m,1h,4h,1d}` — idempotently recomputed from `fapi_kline_1m FINAL` by a refreshable materialized view (`deploy/clickhouse/002_kline_rollups.sql`). Engine `ReplacingMergeTree(rollup_version)`; the schema and consumption pattern (`... FINAL`) are identical to the 1m table. Buckets use `toStartOfInterval` (epoch/UTC-aligned, consistent with Binance's boundaries).
- **Query convention**: every kline table is queried uniformly as `SELECT ... FROM market.fapi_kline_<iv> FINAL ...`.

#### 2. External Query Commands & Code Examples
```bash
# 1. clickhouse-client: query the 5 most recently closed klines for BTCUSDT
clickhouse-client --query "
SELECT symbol, start_time, open, high, low, close, volume, quote_volume, trades_count
FROM market.fapi_kline_1m
WHERE symbol = 'BTCUSDT'
ORDER BY start_time DESC
LIMIT 5
FORMAT PrettyCompact"

# 2. Query the market-wide cross-section at a specific historical moment (using FINAL to guarantee merge deduplication)
clickhouse-client --query "
SELECT symbol, close, quote_volume, trades_count
FROM market.fapi_kline_1h FINAL
WHERE start_time = '2026-09-07 16:00:00'
ORDER BY quote_volume DESC
FORMAT TabSeparatedWithNames"

# 3. Pull JSON data directly via the HTTP interface (Python / external tool integration)
curl -s 'http://127.0.0.1:8123/?query=SELECT+symbol,start_time,close+FROM+market.fapi_kline_1m+WHERE+symbol=%27BTCUSDT%27+ORDER+BY+start_time+DESC+LIMIT+2+FORMAT+JSON'
```

```python
# Python query example (clickhouse-connect)
import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='', database='market')

# Extract multi-dimensional history for a given symbol, directly yielding a Pandas/Polars DataFrame
df = client.query_df("""
    SELECT symbol, start_time, end_time, open, high, low, close, volume, quote_volume, taker_buy_volume
    FROM market.fapi_kline_1h FINAL
    WHERE symbol = 'BTCUSDT' AND start_time >= now() - INTERVAL 7 DAY
    ORDER BY start_time ASC
""")
```

#### 3. Return Value Format & Per-Column Structure
- **Return format**: a columnar/row-oriented table result set (Table ResultSet).
- **Field structure and business meaning**:

| Column | ClickHouse Physical Type | Example Value | Business Meaning |
| :--- | :--- | :--- | :--- |
| `symbol` | `LowCardinality(String)` | `'BTCUSDT'` | Symbol. Uses low-cardinality encoding, substantially improving filtering and aggregation performance. |
| `start_time` | `DateTime64(3, 'UTC')` | `'2026-09-07 16:00:00.000'` | **Primary key**. Kline open millisecond UTC timestamp, the timeline alignment baseline across the whole pipeline. |
| `end_time` | `DateTime64(3, 'UTC')` | `'2026-09-07 16:59:59.999'` | Kline close millisecond UTC timestamp. |
| `open` | `Float64` | `60250.5` | Open price. |
| `high` | `Float64` | `60800.0` | High price. |
| `low` | `Float64` | `60100.2` | Low price. |
| `close` | `Float64` | `60720.0` | Close price. |
| `volume` | `Float64` | `12450.85` | Cumulative traded base-asset volume. |
| `quote_volume` | `Float64` | `75602300.5` | Cumulative traded quote (USDT) turnover. |
| `taker_buy_volume` | `Float64` | `6120.40` | Taker buy volume (used for long/short-force and microstructure analysis). |
| `taker_buy_quote_volume` | `Float64` | `37150000.20` | Taker buy turnover (USDT). |
| `trades_count` | `UInt32` | `1420` | Total number of matched trades within this bar's period. |
| `created_at` | `DateTime` | `'2026-09-07 17:00:01'` | Local time at which the row was persisted to ClickHouse. Used by ReplacingMergeTree for deduplication. |

---

### 7.2 Purpose & Goals
ClickHouse is the system's **sole authoritative source of record** for underlying persistence (cold storage).
- **Append-only**: no row-level updates are performed; it relies on the `ReplacingMergeTree(created_at)` engine to automatically overwrite and deduplicate by `(symbol, start_time)` with the latest data during background merges.
- **Batched aggregated writes**: single-row inserts are strictly forbidden; rows accumulate in a lock-free in-memory queue and are written in bulk via the native columnar TCP protocol once a batch threshold is reached.

### 7.3 Core Caller, Concurrency Model & Code Entry Points
- **Core Caller**: `archiveRouter` and [`chwriter.BatchWriter`](../internal/chwriter/writer.go#L41)
- **Concurrency & Goroutine Model**:
  - **A single writer instance**:
    - This build only ingests 1m, so only a single `*chwriter.BatchWriter` is instantiated, writing to `market.fapi_kline_1m`. Coarser timeframes are handled by the ClickHouse rollup (§7.1) and never pass through the Go write path.
  - **Producer concurrent enqueue**:
    - Real-time ingestion stream: Dispatcher workers call [`TryPush(row)`](../internal/chwriter/writer.go#L110), a non-blocking push into `input chan Row` (capacity 65536), which drops with an error when full.
    - Historical backfill stream: the Backfiller calls [`Push(ctx, row)`](../internal/chwriter/writer.go#L93), blocking to wait for channel capacity, guaranteeing zero loss of historical data.
  - **A single batch-processing loop goroutine**:
    - Each BatchWriter has **1 dedicated background goroutine** internally running [`w.loop(ctx)`](../internal/chwriter/writer.go#L142).
    - **Dual-trigger flush conditions**:
      1. Count-triggered: buffered row count reaches `BatchSize` (**5,000 rows** by default);
      2. Time-triggered: more than `FlushInterval` (**1,000 ms** by default) has elapsed since the last flush.
  - **Self-healing and retry**:
    - The underlying [`clickHouseFlusher.Flush`](../internal/chwriter/clickhouse.go#L93) automatically pings and reconnects when the connection becomes invalid.
    - On write failure it enters [`flushWithRetry`](../internal/chwriter/writer.go#L217), retrying up to 3 times with half-jitter, and alerts if that is exceeded.
- **Code Entry Points**:
  - Routing and dispatch: [`internal/app/app.go:348`](../internal/app/app.go#L348) (`archiveRouter.TryPush`)
  - Batch loop: [`internal/chwriter/writer.go:142`](../internal/chwriter/writer.go#L142) (`BatchWriter.loop`)
  - Columnar send: [`internal/chwriter/clickhouse.go:93`](../internal/chwriter/clickhouse.go#L93) (`clickHouseFlusher.Flush`)

---

## 8. Historical Kline Gapfill Backfill & Redis Window Rebuild Flow (Historical Gapfill & Window Rebuild Flow)

### 8.1 External Storage Query Contract (ClickHouse & Redis Verification)

#### 1. External Storage & Naming Structures Involved
- **ClickHouse read source table**: `market.fapi_kline_<interval>` (using the `FINAL` forced-merge deduplication view)
- **Redis write target key**: `kline:{SYMBOL}:{interval}` (replaces and overwrites the original corrupted or missing List)

#### 2. External Verification & Observation Query Examples
```bash
# 1. Verify whether ClickHouse has completely backfilled a missing contiguous time range (using the 1m interval as an example, checking for timestamp gaps)
clickhouse-client --query "
SELECT symbol, count(), min(start_time), max(start_time)
FROM market.fapi_kline_1m FINAL
WHERE symbol = 'BTCUSDT' AND start_time >= now() - INTERVAL 4 HOUR
GROUP BY symbol"

# 2. Verify whether the Redis window has been atomically overwritten by RebuildWindow and filled to 200 bars
redis-cli LLEN kline:BTCUSDT:1m
redis-cli LINDEX kline:BTCUSDT:1m 0   # check whether the newest bar's open timestamp is aligned with the latest close moment
redis-cli LINDEX kline:BTCUSDT:1m 199 # check whether the oldest bar is correctly populated
```

#### 3. Return Value Format & Data Structure
- **ClickHouse query return**: executing the SQL `SELECT ... FINAL ORDER BY symbol ASC, start_time DESC LIMIT 200 BY symbol` returns fields entirely consistent with [Section 7.1](#71-external-storage-query-contract-clickhouse-query-contract).
- **Redis List return after rebuild**: returns 200 arranged 9-element compact arrays, entirely consistent with [Section 5.1](#51-external-storage-query-contract-redis-query-contract).

---

### 8.2 Purpose & Goals
Incremental ingestion can only collect real-time pushes that occur while the process is alive. A data gap arises whenever the process cold-starts, a shard briefly disconnects, or a new symbol is listed.
Gapfill runs **in parallel** with incremental ingestion: in the background it pulls missing data back from the Binance REST API and injects it into ClickHouse, then reads the authoritative tail back from ClickHouse to fully reset the Redis window, and finally hands off seamlessly back to the live ingestion stream.

### 8.3 Core Caller, Concurrency Model & Code Entry Points
- **Core Caller**: [`backfill.Backfiller`](../internal/backfill/backfill.go#L182)
- **Three trigger sources**:
  1. **Cold start**: in [`App.Run`](../internal/app/app.go#L268), `SubmitColdStart` is called after the Universe is first fetched. If ClickHouse is an empty database, history is pulled starting from `cold_start_date`; if history already exists, it continues syncing from ClickHouse `max(start_time) + 1 step` through to the current latest moment. During this period the HTTP probe `/readyz` returns 503, switching to 200 once backfill completes.
  2. **Shard reconnect**: after a shard successfully reconnects, it calls back [`collector.OnGap`](../internal/app/app.go#L196) -> [`Backfiller.HandleGap`](../internal/backfill/backfill.go#L328). With 30s debouncing and no bounded window limit, it queries ClickHouse's latest record time directly as the starting point, guaranteeing zero-gap backfill.
  3. **Universe symbol addition**: [`App.onUniverseChange`](../internal/app/app.go#L242) detects an increment in the Universe and submits a full-window backfill for the new symbol to the queue.
- **Offline backfill mode (`backfill.offline_only`)**: when this switch is enabled, `App.Run` takes the `runBackfillOnly` branch — only wiring up universe/fetcher/chwriter/backfiller, skipping collector, dispatcher, rediswin, and windowgate; after submitting a single whole-universe cold start, it blocks via `Backfiller.WaitColdStart` until backfill completes and then exits (`gate_timeout` has no effect, and it will not be forcibly released midway). This is used for a two-phase cold start of "fully backfilling deep history offline first, then bringing real-time business online after verification" — see `docs/OPERATIONS.md` §A.1 / §B.1 (first-time launch of an empty database) for details.
- **Concurrency & Goroutine Model**:
  - **Main request scheduling loop (single goroutine)**:
    - A background goroutine runs [`b.run(ctx)`](../internal/backfill/backfill.go#L358), serially pulling tasks from `reqCh: chan Request` (capacity 256).
    - **Deduplication and gate locking**: when pulling a task, keys not already in `inflight` are marked, and [`gate.Hold(keys)`](../internal/windowgate/gate.go#L60) is immediately called to suspend real-time Redis window writes for these keys.
  - **REST fetch worker pool (multiple concurrent goroutines)**:
    - When processing a single request, [`processInterval`](../internal/backfill/backfill.go#L413) internally limits the maximum concurrent worker count (4 by default) via a semaphore channel `sem := make(chan struct{}, b.cfg.workers)`.
    - **Partitioning approach**: **concurrency is partitioned by symbol**. A temporary goroutine is started per symbol to concurrently execute `fetchAndArchive`.
    - **Rate-limit control**: all workers share a single global token-bucket rate limiter [`rate.Limiter`](../internal/backfill/rest.go#L38) (20 RPS by default), preventing triggering Binance's 429 / 418 IP bans.
  - **Explicit flush synchronization barrier and tail read-back**:
    - Data pulled via REST is written row-by-row into the ClickHouse buffer queue via `chwriter.Push`.
    - After all workers finish, the main loop proactively calls `aw.Flush(ctx)` to trigger an explicit persistence barrier; the BatchWriter immediately drains all rows accumulated in the channel and blocks waiting for the ClickHouse TCP batch physical write to complete (falling back to `FlushWait` if there is no writer).
    - It then obtains a ClickHouse read connection and executes a SQL query with `FINAL`, reading out the most recent 200 bars for all target symbols of that interval in one shot. If the flush errors, the rebuild is skipped to avoid reading dirty data into Redis.
  - **Atomic Redis window rebuild and unlock**:
    - Calls [`Writer.RebuildWindow`](../internal/rediswin/writer.go#L222), using an atomic Lua script to fully replace the List elements.
    - Calls [`gate.Release(keys)`](../internal/windowgate/gate.go#L77) to release the gate, resuming live-stream writes.
- **Code Entry Points**:
  - Startup entry point: [`internal/backfill/backfill.go:245`](../internal/backfill/backfill.go#L245) (`Backfiller.Start`)
  - Request enqueue: [`internal/backfill/backfill.go:283`](../internal/backfill/backfill.go#L283) (`Backfiller.Submit`)
  - Main processing loop: [`internal/backfill/backfill.go:358`](../internal/backfill/backfill.go#L358) (`Backfiller.run`)
  - Per-interval concurrent processing: [`internal/backfill/backfill.go:413`](../internal/backfill/backfill.go#L413) (`processInterval`), [`fetchAndArchive:507`](../internal/backfill/backfill.go#L507)
  - REST fetch logic: [`internal/backfill/rest.go:103`](../internal/backfill/rest.go#L103) (`BinanceFetcher.Fetch`)
  - ClickHouse tail query: [`internal/backfill/clickhouse.go:97`](../internal/backfill/clickhouse.go#L97) (`CHStore.LastBars`)
  - Atomic Redis window rebuild: [`internal/rediswin/writer.go:218`](../internal/rediswin/writer.go#L218) (`Writer.RebuildWindow`)

---

## 9. Window Gate Concurrency Arbitration Flow (Window Gate Concurrency Arbitration Flow)

### 9.1 External Storage & Runtime State Observability (Gate Observability)

#### 1. External Storage Keys Affected by the Gate
- **Redis rolling-window key**: `kline:{SYMBOL}:{interval}` (real-time writes **paused** while gated)
- **Redis event key**: `stream:market:kline_ready` (cross-section event publication for the corresponding interval **suppressed** while gated)

#### 2. External Monitoring & State Observation Methods
The gate itself maintains a reference count in memory; externally, Prometheus monitoring probes can observe in real time which keys are currently intercepted by the gate and the suppression behavior:
```bash
# 1. Observe the total number of symbols currently held/suspended by the gate (steady state should be 0; > 0 during cold start or disconnects)
curl -s localhost:9090/metrics | grep windowgate_held_keys

# 2. Observe the cumulative count of real-time rolling-window writes suppressed by the gate per interval
curl -s localhost:9090/metrics | grep redis_window_gated_pushes_total

# 3. Observe the cumulative count of kline_ready events suppressed per interval due to incomplete backfill
curl -s localhost:9090/metrics | grep dispatcher_kline_ready_suppressed_total
```

---

### 9.2 Purpose & Goals
While the Gapfill backfill pipeline is re-materializing 200 bars from ClickHouse for a given symbol, the live WebSocket may simultaneously be pushing a newly closed bar at high frequency.
- **Consistency risk**: if the live bar is written first and the backfill subsequently completes and executes `RebuildWindow` (`DEL` + `RPUSH`), the live bar would be wiped out; if the backfill completes first and the live bar has no monotonicity protection, timeline corruption occurs.
- **Arbitration scheme**: `windowgate.Gate` acts as a transparent adapter layer, performing **gate interception** for specific keys during backfill:
  1. Intercepts the live closed-rolling-window write for that key (dropping that live push, because that bar has already been persisted via the ClickHouse archive pipeline and `RebuildWindow` is guaranteed to retrieve it);
  2. Intercepts `kline_ready` cross-section events for the interval in question, preventing the strategy layer from reading incomplete data mid-rebuild;
  3. Lifts the interception once the rebuild completes, and combined with the monotonicity-guard Lua script, achieves a **fully seamless and idempotent** state handoff.

### 9.3 Core Caller, Concurrency Model & Code Entry Points
- **Core Caller**: [`windowgate.Gate`](../internal/windowgate/gate.go#L39)
- **Concurrency & Goroutine Model**:
  - **In-memory concurrency control**: internally maintains a `sync.RWMutex`, keeping a **reference count** per `Key`, supporting overlapping Hold/Release (e.g. a cold start and a sudden reconnect acting on the same key simultaneously).
  - **Deadlock-free, non-blocking**: when interception occurs it directly `return nil`s, without blocking the Dispatcher worker.
- **Code Entry Points**:
  - Core state: [`internal/windowgate/gate.go:60`](../internal/windowgate/gate.go#L60) (`Hold`), [`Release:77`](../internal/windowgate/gate.go#L77)
  - Window write adapter: [`internal/windowgate/gate.go:135`](../internal/windowgate/gate.go#L135) (`gatedWindow.PushBarAndTrim`)
  - Cross-section ready adapter: [`internal/windowgate/gate.go:148`](../internal/windowgate/gate.go#L148) (`gatedReady.PublishKlineReady`)

---

## 10. Business Workflow Comparison Matrix

| Workflow Name | Core Call Entry Point | Concurrency/Threading Model | Goroutine Partitioning Strategy | Write Storage Target | Core Data Structure / Key Design | External Query Return Format |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Universe symbol monitoring** | `universe.Monitor.loop` | Single dedicated goroutine | No partitioning (scheduled alignment to 00:00 UTC + 2m) | In-memory snapshot | `Snapshot{Symbols: []string}` | Pure in-memory/HTTP state |
| **WebSocket ingestion** | `collector.shard.run` | Multi-goroutine sharding | 1. by interval; 2. `crc32(SYM) % 4` hash bucketing | In-memory Dispatcher | Combined Stream: `<sym>@kline_<iv>` | Pure network stream/internal event |
| **Dispatcher routing** | `Dispatcher.HandleKlineEvent` | Multiple upstreams + worker pool | 4 `ClosedWorkers` competing on a channel | In-memory sinks | `closedJob` + in-memory monotonic map (`sym\|iv`) | Pure in-memory/internal queue |
| **Redis Live Bar snapshot** | `LiveBarWriter.TryEnqueue` | Worker pool | 2 workers competing on a channel | Redis (dedicated client) | `livebar:{SYM}:{iv}` (Hash, TTL=2x) | Redis Hash dictionary (`t`, `o`, `h`, `l`, `c`, `v`...) |
| **Redis closed rolling window** | `Writer.PushBarAndTrim` | Dispatcher workers | Dispatched by the Dispatcher, arbitrated via the gate | Redis (main client) | `kline:{SYM}:{iv}` (List 200, compact JSON) | Redis List (200 9-element compact arrays, newest at 0) |
| **Market-wide cross-section aggregation** | `aggregator.mark` | Dispatcher workers | In-memory mutex + per-section timer | Redis Stream | `stream:market:kline_ready` | Redis Stream entry (`interval`, `timestamp`, `count`) |
| **ClickHouse batch archiving** | `BatchWriter.loop` | Single goroutine / interval | Each interval has its own dedicated BatchWriter | ClickHouse | `market.fapi_kline_<iv>` (ReplacingMergeTree) | Columnar/row table result (12-column raw fact data) |
| **Historical Gapfill backfill** | `Backfiller.run` | Main loop + worker pool | Main loop serial; worker pool concurrent by symbol (4 workers) | CH + Redis | REST `/fapi/v1/klines` + Lua DEL/RPUSH | CH FINAL table + Redis 200-element List after overwrite |
| **Window gate arbitration** | `Gate.Hold / Release` | Invoked from the main loop and workers | In-memory concurrent RWMutex | In-memory reference count | `Key{Symbol, Interval}` reference counter | Controls freezing of `kline:*` and `stream:market:kline_ready` |
