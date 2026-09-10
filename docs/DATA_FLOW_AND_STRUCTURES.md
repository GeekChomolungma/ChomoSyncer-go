# ChomoSyncer-go 数据流与数据结构业务全景解析

> **文档定位**：本文档深入剖析 `ChomoSyncer-go` 全市场行情数据采集器的业务工作流、数据流转管道、核心并发/线程模型以及各存储/缓存层的数据结构设计。重点阐明**从外部调用 Redis / ClickHouse 查询具体内容时的键命名规范、查询命令示例、返回格式及各字段业务定义**。  
> **适用版本**：`ChomoSyncer-go` v1.0+  
> **涉及核心包**：`cmd/chomosyncer-go`、`internal/app`、`internal/universe`、`internal/collector`、`internal/dispatcher`、`internal/rediswin`、`internal/windowgate`、`internal/chwriter`、`internal/backfill`  
>
> **重要（1m 基准 + 派生）**：本构建**只订阅 1 分钟 K 线**，只落一张原始表 `market.fapi_kline_1m`，Redis 只维护 1m 的 `livebar:{SYM}:1m` 与 `kline:{SYM}:1m`。下文凡出现 `{interval}` 的 Redis 键、`fapi_kline_<interval>` 表、"每个配置的 interval" 等表述，实际取值均为 `1m`。更粗周期（`serve_intervals`，默认 `5m/15m/1h/4h/1d`）由两处派生：① ClickHouse 侧从 `fapi_kline_1m FINAL` 幂等重算的 rollup 表 `fapi_kline_{5m,15m,1h,4h,1d}`（`deploy/clickhouse/002_kline_rollups.sql`）；② `stream:market:kline_ready` 上由"桶末 1m 截面"派生转发的截面信号。粗周期没有 Redis 窗口/livebar，下游直接查 ClickHouse。

---

## 总体架构与数据流拓扑图

```text
                                  ┌───────────────────────────────┐
                                  │      Binance REST API         │
                                  │   GET /fapi/v1/exchangeInfo   │
                                  └───────────────┬───────────────┘
                                                  │ (每 24h 轮询)
                                                  ▼
                                      ┌───────────────────────┐
                                      │    Universe Monitor   │ (全市场 USDT 永续合约标的列表)
                                      └───────────┬───────────┘
                                                  │ OnChange 驱动订阅对齐
                         ┌────────────────────────┴────────────────────────┐
                         │                                                 │
                         ▼                                                 ▼
        ┌──────────────────────────────────┐             ┌──────────────────────────────────┐
        │  WebSocket Sharded Ingestion     │             │     Historical Gapfill 流        │
        │  (按 interval + crc32 哈希分片)   │             │   (冷启动/断线重连/新币上市)      │
        └────────────────┬─────────────────┘             └────────────────┬─────────────────┘
                         │ 原始 KlineEvent                                │ 1. Hold(keys) 门控锁定
                         ▼                                                 ▼
        ┌──────────────────────────────────┐             ┌──────────────────────────────────┐
        │       Kline Dispatcher           │             │   windowgate.Gate (窗口门控)     │
        │   (单调递增防护 + 闭合/实时四路分发) │             │ (拦截门控 Key 的实时写与 ready)  │
        └─┬──────────────┬─────────────────┘             └────────────────┬─────────────────┘
          │              │                                                 │ 2. REST 补录历史
          │ 未闭合帧     │ 闭合帧 (k.x == true)                             ▼
          │ (全量)       ├───────────────────────────────► ┌────────────────────────────────┐
          │              │                                 │     chwriter (ClickHouse)      │
          │              │                                 │   ReplacingMergeTree 幂等追加  │
          │              │                                 └──────────────┬─────────────────┘
          │              │                                                │ 3. 等待 flush 落盘
          │              │                                                ▼
          │              │                                 ┌────────────────────────────────┐
          │              │                                 │   CHStore.LastBars (读回尾部)  │
          │              │                                 └──────────────┬─────────────────┘
          │              │                                                │ 4. RebuildWindow (Lua)
          │              │                                                ▼
          │              │ 经过 gate 过滤                  ┌────────────────────────────────┐
          │              ├───────────────────────────────► │   Redis Closed Rolling Window  │
          │              │                                 │   kline:{SYM}:{iv} (List 200)  │
          │              │                                 └──────────────┬─────────────────┘
          │              │                                                │ 5. Release(keys) 解锁
          │              │ 经过 gate 过滤                                 ▼
          │              ├───────────────────────────────► ┌────────────────────────────────┐
          │              │ (全截面到齐或超时)              │   stream:market:kline_ready    │
          │              │                                 │   (Redis Stream 截面就绪通知)  │
          │              │                                 └────────────────────────────────┘
          ▼              ▼
 ┌──────────────────────────────────┐
 │      Redis Live Bar Snapshot     │
 │  livebar:{SYM}:{iv} (Hash 快照)  │
 └──────────────────────────────────┘
```

---

## 1. 动态全市场标的监控流 (Universe Monitor Flow)

> **外部存储交互**：本工作流属于纯内存/REST 协议流，不直接对 Redis 或 ClickHouse 执行数据写入与持久化。

### 1.1 业务定位与目标
本系统是**全市场行情采集器**，严禁引入流动性或交易量门限过滤。动态 Universe 负责定时从币安合约 REST API 拉取全部处于可交易状态的 USDT 永续合约标的。
该集合是：
1. **WebSocket Collector 的订阅全集**：决定 Collector 应该维护哪些 stream；
2. **截面就绪计算 (`kline_ready`) 的总分母**：确定一个周期内全市场应当闭合的总币种数；
3. **冷启动和动态扩容的触发源**：当发现新上架币种时，动态调度回补并接入分片。

### 1.2 核心调用者、并发模型与代码入口
- **核心调用者**：[`universe.Monitor`](../internal/universe/monitor.go)
- **并发与调用模型**：
  - **单 Goroutine 定时对齐轮询**：在启动时由主线程同步执行一次 `Refresh`，之后常驻一个独立的后台 Goroutine 运行 `loop(ctx)`。
  - **定时对齐机制**：每 24 小时对齐到 UTC 午夜 `00:00:00` 之后的 `+2min`（`DefaultRefreshOffset`），等待交易所上市/下市结算平稳后触发。
  - **广播模式**：`Refresh` 成功后，持有读写锁更新内部快照，并在锁外同步调用注册的回调函数 `subs []func(Snapshot)`（即 `app.onUniverseChange`）。
- **代码入口**：
  - 启动入口：[`internal/universe/monitor.go:141`](../internal/universe/monitor.go#L141) (`Monitor.Start`)
  - 后台循环：[`internal/universe/monitor.go:160`](../internal/universe/monitor.go#L160) (`Monitor.loop`)
  - 刷新逻辑：[`internal/universe/monitor.go:199`](../internal/universe/monitor.go#L199) (`Monitor.Refresh`)
  - 业务消费与分发：[`internal/app/app.go:217`](../internal/app/app.go#L217) (`App.onUniverseChange`)

### 1.3 数据结构与 Schema 设计
#### 网络层 (Binance REST API)
- **接口路径**：`GET https://fapi.binance.com/fapi/v1/exchangeInfo`
- **原始 JSON 过滤规则**（硬过滤）：
  ```go
  // 必须同时满足三个硬性条件：
  s.QuoteAsset == "USDT" && s.ContractType == "PERPETUAL" && s.Status == "TRADING"
  ```

#### 内存数据结构
- **快照结构体** ([`universe.Snapshot`](../internal/universe/monitor.go#L61))：
  ```go
  type Snapshot struct {
      Symbols     []string  // 字典序排序的所有合格交易对，例如 ["1000BONKUSDT", "BTCUSDT", ...]
      RefreshedAt time.Time // UTC 刷新时间戳
  }
  ```
- **集合索引**：内部维护 `set: map[string]struct{}`，提供 `O(1)` 时间复杂度的 `Has(symbol string) bool` 查询与 `Size() int` 查询。

---

## 2. WebSocket 多分片长连接采集流 (WebSocket Sharded Ingestion Flow)

> **外部存储交互**：本工作流负责长连接保活与报文反序列化，下发至内存 Dispatcher，不直接对外暴露 Redis 或 ClickHouse 查询。

### 2.1 业务定位与目标
由于币安 U 本位永续合约**不存在全市场 Kline 聚合流**（`!kline_<interval>@arr` 仅在现货提供），系统必须逐交易对订阅 `<symbol>@kline_<interval>`。全市场近 500 个标的 × 多个周期，若挤在单条连接将超过单连接 200 个 stream 的硬限制。
因此，Collector 采用稳定一致性哈希分片，维护多个高可用 WebSocket 连接，并提供指数退避重连、防假死看门狗及断线 Gap 监测。

### 2.2 核心调用者、并发模型与代码入口
- **核心调用者**：[`collector.Collector`](../internal/collector/collector.go#L162) 与 [`collector.shard`](../internal/collector/shard.go#L41)
- **并发与 Goroutine 模型**：
  - **单周期哈希分片**：只订阅 `1m`，按 `crc32(SYMBOL) % ShardsPerInterval`（默认 4 个 shard）分桶。
    - **分片命名与 ID**：`kline_1m_<bucket>`（`kline_1m_0` … `kline_1m_3`）。每个 shard 承载约 100~130 个 stream，远低于 200 上限。
    - 之所以不再按周期开多套分片：更粗周期是 1m 的堆叠，同一撮合事件在 1m/1h 两条链路重复推送是纯浪费。见 `docs/ARCHITECTURE_MODULES.md` §8.6。
  - **Goroutine 分割与调度**：
    - **每个 Shard 对应一个独立的 Supervisor Goroutine**：在 [`Collector.SetSymbols`](../internal/collector/collector.go#L233) 中通过 `go s.run(c.rootCtx)` 启动。
    - **建连错峰**：新建 shard 启动前强制 Sleep 错峰（`startDelay += c.cfg.connectStagger`，默认 300ms），避免同一时刻突发握手被币安 IP 限流。
    - **读帧并发**：底层官方库 `binance-connector-go` 的 `OnMarket` 回调会为每个到来的消息启动临时 Goroutine 调用 [`handleRaw`](../internal/collector/binanceclient.go#L116)，因此进入采集链路的事件调用是**高并发、多 Goroutine** 的。
    - **保活与断线自愈**：
      - 官方连接器处理服务端 ping 自动回 pong，每 23h 主动重建连接。
      - Shard Supervisor 内部运行 [`watchdogInterval`](../internal/collector/shard.go#L180)（10s）看门狗：若距上次收到消息超过 `StaleTimeout`（默认 60s），判定为半开连接（Half-open TCP），主动切断重连。
      - 重连使用带半抖动的指数退避（1s ~ 30s）。重连成功后计算断线缺口 `[lastMsgAt, reconnectAt]`，回调 `OnGap` 投递至回补模块。
- **代码入口**：
  - 分片计算：[`internal/collector/sharding.go:35`](../internal/collector/sharding.go#L35) (`planStreams`)
  - Shard 拓扑对齐：[`internal/collector/collector.go:211`](../internal/collector/collector.go#L211) (`Collector.SetSymbols`)
  - Shard 管理循环：[`internal/collector/shard.go:94`](../internal/collector/shard.go#L94) (`shard.run`), [`shard.connectAndPump:135`](../internal/collector/shard.go#L135)
  - 读帧入口：[`internal/collector/binanceclient.go:116`](../internal/collector/binanceclient.go#L116) (`binanceStreamClient.handleRaw`)
  - 事件转交：[`internal/collector/shard.go:84`](../internal/collector/shard.go#L84) (`shard.onEvent`)

### 2.3 数据结构与 Schema 设计
#### WebSocket Combined Stream 协议
- **订阅流格式**：`<symbol_lowercase>@kline_<interval>`（如 `btcusdt@kline_1m`）
- **币安推送 JSON 格式**：
  ```json
  {
    "stream": "btcusdt@kline_1m",
    "data": {
      "e": "kline",
      "E": 1719835260000,
      "s": "BTCUSDT",
      "k": {
        "t": 1719835200000,  // K 线开盘时间 (ms) -> 统一的主键基准
        "T": 1719835259999,  // K 线收盘时间 (ms)
        "s": "BTCUSDT",      // 标的符号
        "i": "1m",           // 周期
        "o": "60250.50",     // 开盘价
        "c": "60720.00",     // 收盘价 (最新价)
        "h": "60800.00",     // 最高价
        "l": "60100.20",     // 最低价
        "v": "12450.85",     // Base 成交量
        "q": "75602300.50",  // Quote (USDT) 成交量
        "V": "6120.40",      // Taker 买入 Base 成交量
        "Q": "37150000.20",  // Taker 买入 Quote 成交量
        "n": 1420,           // 成交笔数
        "x": true            // 关键：是否正式收盘闭合 (is_bar_final)
      }
    }
  }
  ```

#### 内部事件结构体
- **内部统一结构** ([`dispatcher.KlineEvent`](../internal/dispatcher/event.go#L21))：
  ```go
  type KlineEvent struct {
      Symbol              string // "BTCUSDT"
      Interval            string // "1m", "1h"
      OpenTime            int64  // k.t (ms)
      CloseTime           int64  // k.T (ms)
      Open                string // 保留原始字符串，由 dispatcher 集中解析
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

## 3. 事件路由分发与单调防护流 (Dispatcher & Monotonic Guard Flow)

> **外部存储交互**：本工作流是内存缓冲与分发枢纽，通过 channel 将数据分发至下游各自的存储 Sink（Redis / ClickHouse），自身无独立存储 Key。

### 3.1 业务定位与目标
Dispatcher 是全系统的数据中枢，承担三大核心业务：
1. **未闭合与闭合数据分离**：所有事件进入实时快照，闭合事件（`k.x == true`）进入滑窗、归档与聚合；
2. **闭合单调递增防护**：网络断线重连、乱序或重试可能导致较旧的 Bar 延迟到达。Dispatcher 严格校验开盘时间戳，丢弃 `<= 上次已处理开盘时间` 的重复或乱序闭合帧，杜绝时间线倒流；
3. **异步解耦与缓冲**：WS 读帧线程绝不直接进行任何存储 I/O，通过内存 Channel 交付 Worker 处理。

### 3.2 核心调用者、并发模型与代码入口
- **核心调用者**：[`dispatcher.Dispatcher`](../internal/dispatcher/dispatcher.go#L106)
- **并发与 Goroutine 模型**：
  - **上游并发入口**：[`HandleKlineEvent`](../internal/dispatcher/dispatcher.go#L177) 由多个 Collector WS 回调线程无锁并发调用。
  - **单调防护锁**：[`acceptClosed`](../internal/dispatcher/dispatcher.go#L234) 使用互斥锁 `guardMu sync.Mutex` 保护 `lastClosed: map[string]int64`（Key 为 `symbol|interval`），仅在闭合帧时进行短暂的内存查改。
  - **闭合队列与 Worker Pool**：
    - 队列：`closedCh: chan closedJob`，容量为 `ClosedQueueSize`（默认 4096）。若队列打满，立即返回 `ErrBusy` 并丢弃，保护主摄取流不发生阻塞死锁。
    - 消费者：固定数量的 **Closed Workers**（默认 4 个 Goroutine），运行 [`d.worker(ctx)`](../internal/dispatcher/dispatcher.go#L245)，并发从 `closedCh` 取出任务进行处理。
  - **处理顺序**：每个 Worker 处理时严格遵循：**先写 Redis 滑窗 -> 再投递 ClickHouse 归档 -> 最后通知截面聚合器**，确保下游策略收到信号时 Redis 数据必然已可读。
- **代码入口**：
  - 分发路由：[`internal/dispatcher/dispatcher.go:177`](../internal/dispatcher/dispatcher.go#L177) (`HandleKlineEvent`)
  - 单调检查：[`internal/dispatcher/dispatcher.go:234`](../internal/dispatcher/dispatcher.go#L234) (`acceptClosed`)
  - 闭合 Worker 消费：[`internal/dispatcher/dispatcher.go:245`](../internal/dispatcher/dispatcher.go#L245) (`worker`), [`handleClosed:273`](../internal/dispatcher/dispatcher.go#L273)

### 3.3 数据结构与 Schema 设计
#### 内部作业结构
- **内部闭合队列元素** ([`dispatcher.closedJob`](../internal/dispatcher/dispatcher.go#L94))：
  ```go
  type closedJob struct {
      symbol   string               // 交易对大写
      interval string               // 周期
      openTime int64                // 毫秒时间戳 (k.t)
      bar      rediswin.CompactBar  // 紧凑 9 元素数组，用于 Redis
      row      chwriter.Row         // 列式行结构，用于 ClickHouse
      hasRow   bool                 // 是否成功解析出 Row
  }
  ```

---

## 4. Redis 实时快照层工作流 (Redis Live Bar Snapshot Flow - livebar)

### 4.1 外部存储查询契约 (Redis Query Contract)

#### 1. 键命名结构 (Key Naming Pattern)
- **存储结构**：Redis **`Hash`**
- **键命名规范**：`livebar:{SYMBOL}:{interval}`（**标的必须全大写**，冒号分隔）
  - 示例：`livebar:BTCUSDT:1m`、`livebar:ETHUSDT:1h`、`livebar:SOLUSDT:15m`
- **生命周期 TTL**：带有 `PEXPIRE` 自动过期，时长为 `2 × interval`（例如 1m 周期 TTL 为 120 秒，1h 周期 TTL 为 2 小时）。
- **可选 Pub/Sub 通道**：`livebar.{interval}`（如 `livebar.1m`、`livebar.1h`）。

#### 2. 外部查询命令与代码示例
```bash
# 命令行：查看 BTCUSDT 1m 周期当前最新正在形成的即时 K 线快照
redis-cli HGETALL livebar:BTCUSDT:1m

# 命令行：读取多个关键字段（如开盘时间、当前价、成交量、闭合状态）
redis-cli HMGET livebar:BTCUSDT:1m t c v x

# 命令行：订阅 1 分钟周期全市场实时行情广播（需开启 live-publish）
redis-cli SUBSCRIBE livebar.1m
```

```python
# Python 查询示例 (redis-py)
import redis

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# 1. 批量读取多个标的的实时形态 (Pipeline)
pipe = r.pipeline()
symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
for sym in symbols:
    pipe.hgetall(f"livebar:{sym}:1m")
live_bars = pipe.execute()  # 返回 [{'t': '1719835200000', 'c': '60720.0', ...}, ...]

# 2. 转换为强类型字段
current_price = float(live_bars[0]['c'])
is_final = (live_bars[0]['x'] == '1')
```

#### 3. 返回值形式与各字段结构说明
- **HGETALL 返回值形式**：字符串键值对字典（Dictionary / Map）。
- **各字段结构及业务含义**：

| Hash 字段名 | 数据类型 | 示例返回值 | 对应 Binance 原始字段 | 业务含义与说明 |
| :--- | :--- | :--- | :--- | :--- |
| `t` | `int64` 字符串 | `"1719835200000"` | `k.t` | K 线开始开盘时间戳（毫秒） |
| `o` | `float64` 字符串 | `"60250.5"` | `k.o` | 本根 K 线开盘价 |
| `h` | `float64` 字符串 | `"60800.0"` | `k.h` | 本根 K 线当前最高价 |
| `l` | `float64` 字符串 | `"60100.2"` | `k.l` | 本根 K 线当前最低价 |
| `c` | `float64` 字符串 | `"60720.0"` | `k.c` | 最新撮合成交价（当前即时价格） |
| `v` | `float64` 字符串 | `"12450.85"` | `k.v` | 当前累计成交量（Base 标的资产量） |
| `qv` | `float64` 字符串 | `"75602300.5"` | `k.q` | 当前累计成交额（Quote 计价资产，USDT） |
| `tbv` | `float64` 字符串 | `"6120.40"` | `k.V` | Taker 主动买入成交量（Base） |
| `tbqv` | `float64` 字符串 | `"37150000.20"` | `k.Q` | Taker 主动买入成交额（Quote，USDT） |
| `n` | `int64` 字符串 | `"1420"` | `k.n` | 本根 K 线已产生的总成交笔数 |
| `x` | `int` 字符串 | `"0"` 或 `"1"` | `k.x` | 是否已正式收盘闭合（`0`=正在形成，`1`=刚刚收盘） |

- **Pub/Sub Payload 返回形式**：11 元素紧凑无 Key JSON Array 字符串：
  `[1719835200000, 60250.5, 60800.0, 60100.2, 60720.0, 12450.85, 75602300.5, 6120.40, 37150000.20, 1420, 0]`

---

### 4.2 业务定位与目标
为盘口监控、实时预警、前端看板等**独立于策略计算的实时消费端**提供当前正在形成的（或刚闭合的）K 线即时形态。
- **隔离性**：窗口深度恒为 1，不保存 intra-bar 历史演变，不参与任何回测对齐，**绝不触发 `kline_ready`**。
- **高吞吐无阻塞**：实时数据更新频率极高（~250ms/帧），采用 Fire-and-Forget 语义，满即丢弃（丢帧无害，因为下一帧会立即覆盖）。

### 4.3 核心调用者、并发模型与代码入口
- **核心调用者**：[`rediswin.LiveBarWriter`](../internal/rediswin/livebar_writer.go#L187)
- **并发与 Goroutine 模型**：
  - **生产者入队**：`dispatcher.HandleKlineEvent` 内部通过 [`TryEnqueue`](../internal/rediswin/livebar_writer.go#L235) 将快照送入 channel。采用 `select default`，通道满时瞬间丢弃并累加 `redis_livebar_dropped_total`，绝不阻塞。
  - **Worker Pool 消费**：
    - 缓冲队列：`input: chan liveJob`（默认容量 8192）。
    - 消费者：启动配置数量的 Worker Goroutine（默认 2 个，`Workers`），运行 [`w.worker(ctx)`](../internal/rediswin/livebar_writer.go#L299)，从 `input` 竞争读取任务。
  - **连接池与资源物理隔离**：
    - 使用专属的 `*redis.Client`（通过 [`LiveClientOptions`](../internal/rediswin/livebar_writer.go#L117) 构建：PoolSize 8, MinIdleConns 2, DialTimeout 2s, Read/WriteTimeout 300ms, MaxRetries -1）。
    - 避免高频的实时写打满连接池，阻碍收盘滑窗和 `kline_ready` 关键路径。
- **代码入口**：
  - 入队入口：[`internal/rediswin/livebar_writer.go:235`](../internal/rediswin/livebar_writer.go#L235) (`LiveBarWriter.TryEnqueue`)
  - 消费与写 Redis：[`internal/rediswin/livebar_writer.go:299`](../internal/rediswin/livebar_writer.go#L299) (`worker`), [`write:329`](../internal/rediswin/livebar_writer.go#L329)

---

## 5. Redis 收盘定长滑窗缓存工作流 (Redis Closed Rolling Window Flow - kline)

### 5.1 外部存储查询契约 (Redis Query Contract)

#### 1. 键命名结构 (Key Naming Pattern)
- **存储结构**：Redis **`List`**（定长 200 根）
- **键命名规范**：`kline:{SYMBOL}:{interval}`（**标的必须全大写**，冒号分隔）
  - 示例：`kline:BTCUSDT:1h`、`kline:ETHUSDT:1m`、`kline:SOLUSDT:1h`
- **排列方向**：**Newest-First（最新收盘的 Bar 在最左侧索引 0）**。

#### 2. 外部查询命令与代码示例
```bash
# 命令行：读取 BTCUSDT 1h 周期完整的 200 根已收盘 K 线
redis-cli LRANGE kline:BTCUSDT:1h 0 199

# 命令行：读取最新闭合的第 1 根 K 线（索引 0）
redis-cli LINDEX kline:BTCUSDT:1h 0

# 命令行：查询当前窗口实际缓存的 Bar 数量（稳态为 200）
redis-cli LLEN kline:BTCUSDT:1h
```

```python
# Python 策略层单次 RTT 批量读取全市场截面滑窗 (推荐模式)
import redis, json
import polars as pl

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# 获取 Universe 活跃交易对列表
universe_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "..."]

# 通过 Pipeline 在单次网络往返拉取全部 List
pipe = r.pipeline()
for sym in universe_symbols:
    pipe.lrange(f"kline:{sym}:1h", 0, 199)
batch_results = pipe.execute()

# 解析为张量或 Polars DataFrame
market_matrix = {}
for sym, raw_bars in zip(universe_symbols, batch_results):
    # raw_bars 包含至多 200 个 JSON array 字符串
    parsed_bars = [json.loads(b) for b in raw_bars]
    market_matrix[sym] = parsed_bars
```

#### 3. 返回值形式与各字段结构说明
- **LRANGE 返回值形式**：字符串列表 `List[string]`，至多 200 个元素。
  - 索引 `0`：**最新**收盘的 K 线；
  - 索引 `199`：**最老**收盘的 K 线。
- **单个 List 元素值结构**：为节省 60%+ 内存，**不使用带 Key 的 JSON 对象**，统一采用 **9 元素紧凑 JSON 数组**：
  ```json
  [1719835200000, 60250.5, 60800.0, 60100.2, 60720.0, 12450.85, 75602300.5, 6120.4, 37150000.2]
  ```
- **9 个数组位置的精确字段定义**：

| 数组索引位置 | 字段名称 | 数据类型 | 示例取值 | 业务含义说明 |
| :--- | :--- | :--- | :--- | :--- |
| `[0]` | `start_time` | `int64` (ms) | `1719835200000` | K 线开盘时间毫秒戳（`k.t`），对齐基准主键 |
| `[1]` | `open` | `float64` | `60250.5` | 开盘价 |
| `[2]` | `high` | `float64` | `60800.0` | 最高价 |
| `[3]` | `low` | `float64` | `60100.2` | 最低价 |
| `[4]` | `close` | `float64` | `60720.0` | 收盘价（策略计算关键基准价） |
| `[5]` | `volume` | `float64` | `12450.85` | 标的资产成交总量（Base Volume） |
| `[6]` | `quote_volume` | `float64` | `75602300.5` | 成交总金额（Quote Volume，USDT） |
| `[7]` | `taker_buy_volume` | `float64` | `6120.40` | Taker 主动买入成交量（用于计算 CVD） |
| `[8]` | `taker_buy_quote_volume` | `float64` | `37150000.20` | Taker 主动买入成交金额（USDT） |

---

### 5.2 业务定位与目标
专为 Python 特征工程与策略计算提供全市场毫秒级无锁切片查询。
- **定长约束**：严格锁死窗口长度为 **200 根 Bar**（最新 Bar 在 List 左侧 index 0），维持内存恒定。
- **单调幂等写入**：通过 Redis 内部 Lua 脚本执行时间戳比对，只有当新 Bar 的 `start_time > 当前头部 start_time` 时才允许 `LPUSH`，保证历史回补与实时流并发交接时的严格幂等性。

### 5.3 核心调用者、并发模型与代码入口
- **核心调用者**：[`rediswin.Writer`](../internal/rediswin/writer.go#L62)
- **并发与 Goroutine 模型**：
  - **调用者**：由 Dispatcher 的 4 个 `ClosedWorkers` 并发调用。
  - **门控包装**：在进入 `rediswin.Writer` 前，由 [`windowgate.Gate.WrapWindow`](../internal/windowgate/gate.go#L126) 拦截。如果当前 Key 正在进行 Gapfill 回补，实时写操作直接静默丢弃（后续由 ClickHouse 读回权威尾部，保证一致性）。
  - **原子执行**：通过预编译的 Lua 脚本 [`monotonicPushScript`](../internal/rediswin/writer.go#L27) 在 1 次网络 RTT 内完成。
- **代码入口**：
  - 门控过滤：[`internal/windowgate/gate.go:140`](../internal/windowgate/gate.go#L140) (`gatedWindow.PushBarAndTrim`)
  - 写入入口：[`internal/rediswin/writer.go:103`](../internal/rediswin/writer.go#L103) (`Writer.PushBarAndTrim`)
  - Lua 脚本定义：[`internal/rediswin/writer.go:27`](../internal/rediswin/writer.go#L27) (`monotonicPushScript`)

---

## 6. 全市场截面状态聚合与触发流 (Cross-Section Aggregator & Kline Ready Stream Flow)

### 6.1 外部存储查询契约 (Redis Stream Contract)

#### 1. 键命名结构 (Key Naming Pattern)
- **存储结构**：Redis **`Stream`**
- **Stream Key**：`stream:market:kline_ready`（固定命名空间）
- **容量上限**：写入时附带 `MAXLEN ~ 1000`，保留最近约 1000 条截面触发记录，自动修剪。

#### 2. 外部查询与监听命令示例
```bash
# 命令行：实时阻塞监听下一条到来的截面就绪事件 (阻塞 0 毫秒表示永久等待)
redis-cli XREAD BLOCK 0 STREAMS stream:market:kline_ready $

# 命令行：查看最近触发的 5 条截面历史事件
redis-cli XREVRANGE stream:market:kline_ready + - COUNT 5

# 命令行：查看 Stream 队列长度与元数据信息
redis-cli XLEN stream:market:kline_ready
```

```python
# Python 策略引擎事件驱动监听主循环
import redis

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
stream_key = "stream:market:kline_ready"
last_id = "$"  # "$" 表示只监听启动之后到来的新消息

print("Python Strategy Engine listening for kline_ready...")
while True:
    # 阻塞等待截面就绪
    response = r.xread({stream_key: last_id}, block=0, count=1)
    for stream, entries in response:
        for entry_id, fields in entries:
            last_id = entry_id
            interval = fields['interval']
            ts = int(fields['timestamp'])
            count = int(fields['symbols_count'])
            print(f"[EVENT] 截面闭合就绪: 周期={interval}, 时间戳={ts}, 到齐币种数={count}")
            # 立即触发全市场 200 根 Bar 的 Pipeline 并行拉取与特征重算！
```

#### 3. 返回值形式与各字段结构说明
- **XREAD / XREVRANGE 返回值形式**：
  每个事件包含唯一的 `Entry ID`（例如 `1719835201042-0`，由毫秒时间戳与自增序号构成）以及关联的字典字段。
- **字段键值结构定义**：

| 字段名 | 数据类型 | 示例返回值 | 业务含义与消费端行为 |
| :--- | :--- | :--- | :--- |
| `interval` | `string` | `"1m"`（原生）或 `"5m"/"1h"/…`（派生） | 闭合的 K 线周期。`1m` 由聚合器原生发布；`serve_intervals` 中的粗周期在其"桶末 1m 截面"就绪时派生转发一条，`symbols_count` 继承 1m 截面。收到粗周期信号后直接查 `market.fapi_kline_<interval> FINAL`。 |
| `timestamp` | `int64` 字符串 | `"1719835200000"` | 刚刚闭合截面的基准开盘毫秒时间戳（`k.t`）。策略层用于对齐跨周期截面矩阵。 |
| `symbols_count` | `int` 字符串 | `"182"` | 本截面实际成功归档并更新滑窗的交易对数量。策略层可据此校验截面覆盖度。 |

---

### 6.2 业务定位与目标
负责追踪全市场（Universe）每个周期、同一开盘时间戳的所有合约闭合进度。当截面内全部币种收盘到达（或触发超时兜底）时，向 Redis Stream 发布一条截面就绪事件，通知 Python 策略引擎立即无状态拉取矩阵执行特征计算。

### 6.3 核心调用者、并发模型与代码入口
- **核心调用者**：[`dispatcher.aggregator`](../internal/dispatcher/aggregator.go#L27)
- **并发与 Goroutine 模型**：
  - **触发入口**：由 4 个 Dispatcher Worker 在成功写完 Redis 和 ClickHouse 缓冲后调用 [`agg.mark(symbol, interval, openTime)`](../internal/dispatcher/aggregator.go#L67)。
  - **并发状态互斥**：`mark` 使用 `mu sync.Mutex` 保护跨截面状态字典 `sections: map[sectionKey]*sectionState`。
  - **倒计时与双触发机制**：
    1. **全员到齐触发 (`complete`)**：截面首次收到 Bar 时启动倒计时定时器（默认 5s `SectionTimeout`）。若在超时前收集到的标的数量达到 `universe.Size()`，立即停止定时器并触发发布。
    2. **超时兜底触发 (`timeout`)**：若部分交易对成交稀疏未在周期末准时出 Bar，定时器到期时 Goroutine 自动触发超时发布，带上当前已收到的标的计数，防止策略引擎被阻塞挂起。
  - **截面防重与内存驱逐 (Retention)**：发布后，截面状态在内存中保留该 interval 的时长（如 1m 保留 1 分钟），吞噬之后迟到的残余帧，防止为同一截面二次发布；过期后由定时器执行 `evict` 释放内存。
  - **门控压制**：发布通过 [`windowgate.Gate.WrapReady`](../internal/windowgate/gate.go#L131)。若该 interval 存在任何正在被回补锁定的 Key，则静默丢弃本次发布，避免策略引擎读到半就绪的脏截面。
- **代码入口**：
  - 截面推进：[`internal/dispatcher/aggregator.go:67`](../internal/dispatcher/aggregator.go#L67) (`aggregator.mark`)
  - 超时回调：[`internal/dispatcher/aggregator.go:104`](../internal/dispatcher/aggregator.go#L104) (`aggregator.onTimeout`)
  - 发布入口：[`internal/dispatcher/aggregator.go:118`](../internal/dispatcher/aggregator.go#L118) (`aggregator.publish`)
  - 门控过滤：[`internal/windowgate/gate.go:153`](../internal/windowgate/gate.go#L153) (`gatedReady.PublishKlineReady`)
  - Redis XADD 执行：[`internal/rediswin/writer.go:268`](../internal/rediswin/writer.go#L268) (`Writer.PublishKlineReady`)

---

## 7. ClickHouse 批量时序归档流 (ClickHouse Batch Ingestion Flow)

### 7.1 外部存储查询契约 (ClickHouse Query Contract)

#### 1. 数据库与表命名结构 (Table Naming Pattern)
- **数据库名**：`market`
- **原始表**：`market.fapi_kline_1m` —— 唯一由采集器直接写入的表。引擎 `ReplacingMergeTree(created_at)`，主键/排序键 `(symbol, start_time)`，分区 `toYYYYMM(start_time)`。
- **派生 rollup 表**：`market.fapi_kline_{5m,15m,1h,4h,1d}` —— 由刷新式物化视图从 `fapi_kline_1m FINAL` 幂等重算（`deploy/clickhouse/002_kline_rollups.sql`）。引擎 `ReplacingMergeTree(rollup_version)`，schema 与消费方式（`... FINAL`）与 1m 表完全一致。桶用 `toStartOfInterval`（epoch/UTC 对齐，与币安边界一致）。
- **查询约定**：所有 kline 表一律 `SELECT ... FROM market.fapi_kline_<iv> FINAL ...`。

#### 2. 外部查询命令与代码示例
```bash
# 1. clickhouse-client 查询 BTCUSDT 最近 5 根收盘 K 线
clickhouse-client --query "
SELECT symbol, start_time, open, high, low, close, volume, quote_volume, trades_count
FROM market.fapi_kline_1m
WHERE symbol = 'BTCUSDT'
ORDER BY start_time DESC
LIMIT 5
FORMAT PrettyCompact"

# 2. 查询全市场在特定历史时刻的截面（使用 FINAL 保证合并去重）
clickhouse-client --query "
SELECT symbol, close, quote_volume, trades_count
FROM market.fapi_kline_1h FINAL
WHERE start_time = '2026-09-07 16:00:00'
ORDER BY quote_volume DESC
FORMAT TabSeparatedWithNames"

# 3. HTTP 接口直接拉取 JSON 数据（Python / 外部工具集成）
curl -s 'http://127.0.0.1:8123/?query=SELECT+symbol,start_time,close+FROM+market.fapi_kline_1m+WHERE+symbol=%27BTCUSDT%27+ORDER+BY+start_time+DESC+LIMIT+2+FORMAT+JSON'
```

```python
# Python 查询示例 (clickhouse-connect)
import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='', database='market')

# 提取指定币种的历史多维度数据，直接生成 Pandas/Polars DataFrame
df = client.query_df("""
    SELECT symbol, start_time, end_time, open, high, low, close, volume, quote_volume, taker_buy_volume
    FROM market.fapi_kline_1h FINAL
    WHERE symbol = 'BTCUSDT' AND start_time >= now() - INTERVAL 7 DAY
    ORDER BY start_time ASC
""")
```

#### 3. 返回值形式与各列字段结构说明
- **返回形式**：列式/行式表格结果集（Table ResultSet）。
- **字段结构及业务含义**：

| 列名 (Column) | ClickHouse 物理类型 | 示例返回值 | 业务含义与说明 |
| :--- | :--- | :--- | :--- |
| `symbol` | `LowCardinality(String)` | `'BTCUSDT'` | 交易对符号。使用低基数编码，大幅提升过滤与聚合性能。 |
| `start_time` | `DateTime64(3, 'UTC')` | `'2026-09-07 16:00:00.000'` | **主键**。K 线开盘毫秒 UTC 时间戳，全链路时序对齐基准。 |
| `end_time` | `DateTime64(3, 'UTC')` | `'2026-09-07 16:59:59.999'` | K 线收盘毫秒 UTC 时间戳。 |
| `open` | `Float64` | `60250.5` | 开盘价。 |
| `high` | `Float64` | `60800.0` | 最高价。 |
| `low` | `Float64` | `60100.2` | 最低价。 |
| `close` | `Float64` | `60720.0` | 收盘价。 |
| `volume` | `Float64` | `12450.85` | 标的 Base 资产累计成交总量。 |
| `quote_volume` | `Float64` | `75602300.5` | 标的 Quote (USDT) 累计成交总金额。 |
| `taker_buy_volume` | `Float64` | `6120.40` | 主动买入成交量（用于多空力量与微观结构分析）。 |
| `taker_buy_quote_volume` | `Float64` | `37150000.20` | 主动买入成交金额（USDT）。 |
| `trades_count` | `UInt32` | `1420` | 本根 Bar 周期内的总撮合成交笔数。 |
| `created_at` | `DateTime` | `'2026-09-07 17:00:01'` | 写入 ClickHouse 时的落盘本地时间。供 ReplacingMergeTree 去重使用。 |

---

### 7.2 业务定位与目标
ClickHouse 是全系统底层持久化的**唯一权威事实库**（Cold Storage）。
- **只追加不更新 (Append-Only)**：不执行行级 Update，依赖 `ReplacingMergeTree(created_at)` 引擎在后台 Merge 时按 `(symbol, start_time)` 自动以最新数据覆盖去重。
- **批量聚合写入**：严禁单条插入，通过内存无锁队列攒批，达标后走原生列式 TCP 协议批量落库。

### 7.3 核心调用者、并发模型与代码入口
- **核心调用者**：`archiveRouter` 与 [`chwriter.BatchWriter`](../internal/chwriter/writer.go#L41)
- **并发与 Goroutine 模型**：
  - **单一 writer 实例**：
    - 本构建只采集 1m，故只实例化一个 `*chwriter.BatchWriter`，写 `market.fapi_kline_1m`。更粗周期由 ClickHouse rollup（§7.1）承担，不经过 Go 写入路径。
  - **生产者并发入队**：
    - 实时采集流：Dispatcher Workers 调用 [`TryPush(row)`](../internal/chwriter/writer.go#L110)，非阻塞推入 `input chan Row`（容量 65536），满则丢弃报错。
    - 历史回补流：Backfiller 调用 [`Push(ctx, row)`](../internal/chwriter/writer.go#L93)，阻塞等待通道容量，确保历史数据零丢失。
  - **单一批处理 Loop Goroutine**：
    - 每个 BatchWriter 内部常驻 **1 个后台 Goroutine** 运行 [`w.loop(ctx)`](../internal/chwriter/writer.go#L142)。
    - **双触发 Flush 条件**：
      1. 条数触发：缓冲行数达到 `BatchSize`（默认 **5,000 行**）；
      2. 定时触发：距上次落盘超过 `FlushInterval`（默认 **1,000 ms**）。
  - **自愈与重试**：
    - 底层 [`clickHouseFlusher.Flush`](../internal/chwriter/clickhouse.go#L93) 在连接失效时自动 Ping 和重连。
    - 写入失败时进入 [`flushWithRetry`](../internal/chwriter/writer.go#L217)，最多带半抖动重试 3 次，超出则报警。
- **代码入口**：
  - 路由分发：[`internal/app/app.go:348`](../internal/app/app.go#L348) (`archiveRouter.TryPush`)
  - 批量循环：[`internal/chwriter/writer.go:142`](../internal/chwriter/writer.go#L142) (`BatchWriter.loop`)
  - 列式发送：[`internal/chwriter/clickhouse.go:93`](../internal/chwriter/clickhouse.go#L93) (`clickHouseFlusher.Flush`)

---

## 8. 历史 K 线 Gapfill 回补与 Redis 窗口重建流 (Historical Gapfill & Window Rebuild Flow)

### 8.1 外部存储查询契约 (ClickHouse & Redis Verification)

#### 1. 涉及的外部存储与命名结构
- **ClickHouse 读取源表**：`market.fapi_kline_<interval>`（使用 `FINAL` 强制合并去重视图）
- **Redis 写入目标键**：`kline:{SYMBOL}:{interval}`（替换覆盖原有损坏或缺失的 List）

#### 2. 外部验证与观测查询示例
```bash
# 1. 验证 ClickHouse 中是否已完整补全缺失的连续时间段（以 1m 周期为例，统计是否有时间戳断层）
clickhouse-client --query "
SELECT symbol, count(), min(start_time), max(start_time)
FROM market.fapi_kline_1m FINAL
WHERE symbol = 'BTCUSDT' AND start_time >= now() - INTERVAL 4 HOUR
GROUP BY symbol"

# 2. 验证 Redis 窗口是否已被 RebuildWindow 原子覆写并拉满 200 根 Bar
redis-cli LLEN kline:BTCUSDT:1m
redis-cli LINDEX kline:BTCUSDT:1m 0   # 检查最新 Bar 开盘时间戳是否已对齐当前最新收盘时刻
redis-cli LINDEX kline:BTCUSDT:1m 199 # 检查最老 Bar 是否正确填充
```

#### 3. 返回值形式与数据结构
- **ClickHouse 查询返回**：执行 SQL `SELECT ... FINAL ORDER BY symbol ASC, start_time DESC LIMIT 200 BY symbol`，返回字段与 [7.1 节](#71-外部存储查询契约-clickhouse-query-contract) 完全一致。
- **Redis 重建后的 List 返回**：返回 200 根排列好的 9 元素紧凑数组，与 [5.1 节](#51-外部存储查询契约-redis-query-contract) 完全一致。

---

### 8.2 业务定位与目标
增量采集仅能收集进程存活期间的实时推送。若发生进程冷启动、Shard 短暂断线、或有新币上市，都会产生数据缺口。
Gapfill 负责与增量采集**并行运作**，在后台将缺失数据从币安 REST API 拉回并注入 ClickHouse，然后从 ClickHouse 读取权威尾部全量重置 Redis 窗口，最后无缝交还给实时采集流。

### 8.3 核心调用者、并发模型与代码入口
- **核心调用者**：[`backfill.Backfiller`](../internal/backfill/backfill.go#L182)
- **三大触发源**：
  1. **冷启动 (Cold Start)**：[`App.Run`](../internal/app/app.go#L268) 中在 Universe 首次获取后调用 `SubmitColdStart`。若 ClickHouse 为空库，以 `cold_start_date` 为起点拉取历史；若已有历史落档，从 ClickHouse `max(start_time) + 1 step` 开始持续接续同步至当下最新时刻。在此期间 HTTP 探针 `/readyz` 返回 503，回补完成后转为 200。
  2. **Shard 断线重连 (Shard Reconnect)**：Shard 重连成功后回调 [`collector.OnGap`](../internal/app/app.go#L196) -> [`Backfiller.HandleGap`](../internal/backfill/backfill.go#L328)。带有 30s 防抖，不再设限窗口，直接查询 ClickHouse 的最新记录时间作为起点，保证零缺口 (Zero Gap) 补齐。
  3. **Universe 新增合约 (Universe Add)**：[`App.onUniverseChange`](../internal/app/app.go#L242) 检测到 Universe 发生增量，向队列提交新币的全窗口回补。
- **离线回补模式 (`backfill.offline_only`)**：当开启此开关时，`App.Run` 走 `runBackfillOnly` 分支——只装配 universe/fetcher/chwriter/backfiller，跳过 collector、dispatcher、rediswin、windowgate；提交一次 whole-universe 冷启动后通过 `Backfiller.WaitColdStart` 阻塞至回补完成即退出（`gate_timeout` 失效，不会中途强制释放）。用于"先离线灌满深历史、校验后再上线实时业务"的两阶段冷启动，详见 `docs/OPERATIONS.md` §A.1 / §B.1（空库首次上线）。
- **并发与 Goroutine 模型**：
  - **主请求调度 Loop（单 Goroutine）**：
    - 后台 Goroutine 运行 [`b.run(ctx)`](../internal/backfill/backfill.go#L358)，串行从 `reqCh: chan Request`（容量 256）提取任务。
    - **去重与门控加锁**：提取时将尚未在 `inflight` 中的 Key 标记，并立刻调用 [`gate.Hold(keys)`](../internal/windowgate/gate.go#L60) 挂起这些 Key 的实时 Redis 窗口写入。
  - **REST 拉取 Worker Pool（多 Goroutine 并发）**：
    - 处理单个请求时，在 [`processInterval`](../internal/backfill/backfill.go#L413) 内部通过信号量 Channel `sem := make(chan struct{}, b.cfg.workers)` 限制最大并发 Worker 数（默认 4 个）。
    - **分割方式**：**按 Symbol 进行并发分割**。每个 Symbol 启动一个临时 Goroutine 并发执行 `fetchAndArchive`。
    - **限流控制**：所有 Worker 共享一个全局 Token Bucket 限流器 [`rate.Limiter`](../internal/backfill/rest.go#L38)（默认 20 RPS），防止触发币安 429 / 418 IP 封禁。
  - **显式 Flush 同步屏障与尾部读回**：
    - REST 拉取的数据逐行调用 `chwriter.Push` 写入 ClickHouse 缓冲队列。
    - Worker 全部完成后，主循环主动调用 `aw.Flush(ctx)` 触发显式落盘屏障，BatchWriter 立即清空 Channel 中堆积的所有行并阻塞等待 ClickHouse TCP 批次物理写入完成（若无 writer 则 fallback 到 `FlushWait`）。
    - 调取 ClickHouse 读连接，执行带有 `FINAL` 的 SQL 查询，一次性读出该 interval 所有目标 symbol 最近的 200 根 Bar。若 Flush 出错则跳过重建，避免脏数据读入 Redis。
  - **Redis 窗口原子重建与解锁**：
    - 调用 [`Writer.RebuildWindow`](../internal/rediswin/writer.go#L222)，利用原子 Lua 脚本全量替换 List 元素。
    - 调用 [`gate.Release(keys)`](../internal/windowgate/gate.go#L77) 释放门控，实时流恢复写入。
- **代码入口**：
  - 启动入口：[`internal/backfill/backfill.go:245`](../internal/backfill/backfill.go#L245) (`Backfiller.Start`)
  - 请求排队：[`internal/backfill/backfill.go:283`](../internal/backfill/backfill.go#L283) (`Backfiller.Submit`)
  - 主处理循环：[`internal/backfill/backfill.go:358`](../internal/backfill/backfill.go#L358) (`Backfiller.run`)
  - 分区间并发处理：[`internal/backfill/backfill.go:413`](../internal/backfill/backfill.go#L413) (`processInterval`), [`fetchAndArchive:507`](../internal/backfill/backfill.go#L507)
  - REST 拉取逻辑：[`internal/backfill/rest.go:103`](../internal/backfill/rest.go#L103) (`BinanceFetcher.Fetch`)
  - ClickHouse 尾部查询：[`internal/backfill/clickhouse.go:97`](../internal/backfill/clickhouse.go#L97) (`CHStore.LastBars`)
  - Redis 窗口原子重构：[`internal/rediswin/writer.go:218`](../internal/rediswin/writer.go#L218) (`Writer.RebuildWindow`)

---

## 9. 窗口门控与并发写入仲裁流 (Window Gate Concurrency Arbitration Flow)

### 9.1 外部存储与运行状态观测 (Gate Observability)

#### 1. 受门控影响的外部存储键
- **Redis 滑窗键**：`kline:{SYMBOL}:{interval}`（门控期间**暂停接收**实时写入）
- **Redis 事件键**：`stream:market:kline_ready`（门控期间**压制发布**对应 interval 的截面事件）

#### 2. 外部监控与状态观测方式
门控本身在内存中维持引用计数，外部可通过 Prometheus 监控探针实时观测当前被门控拦截的 Key 与压制行为：
```bash
# 1. 观测当前处于被门控挂起状态的交易对总数 (稳态应为 0；冷启动或断线时 > 0)
curl -s localhost:9090/metrics | grep windowgate_held_keys

# 2. 观测各周期被门控压制的实时滑窗写入次数累计
curl -s localhost:9090/metrics | grep redis_window_gated_pushes_total

# 3. 观测各周期因回补未完成而被压制的 kline_ready 事件次数累计
curl -s localhost:9090/metrics | grep dispatcher_kline_ready_suppressed_total
```

---

### 9.2 业务定位与目标
当 Gapfill 回补链路正在为某个交易对从 ClickHouse 重新物化 200 根 Bar 时，实时 WebSocket 可能正在以高频推送刚刚收盘的新 Bar。
- **一致性风险**：如果实时 Bar 先写入，随后回补完成执行 `RebuildWindow`（`DEL` + `RPUSH`），会导致实时 Bar 被冲掉；若回补先完成，实时 Bar 又没有递增保护，则会发生时间线错乱。
- **仲裁方案**：`windowgate.Gate` 作为透明适配层，在回补期间对特定 Key 执行**门控拦截**：
  1. 拦截该 Key 的实时收盘滑窗写（丢弃该实时推送，因为该 Bar 已经由 ClickHouse Archive 链路落库，RebuildWindow 必然能捞出）；
  2. 拦截所在 Interval 的 `kline_ready` 截面事件，防止策略层在窗口重建中途读取到残缺数据；
  3. 待 Rebuild 完成后解除拦截，结合单调递增 Lua 脚本，实现**完全无缝且幂等**的状态交接。

### 9.3 核心调用者、并发模型与代码入口
- **核心调用者**：[`windowgate.Gate`](../internal/windowgate/gate.go#L39)
- **并发与 Goroutine 模型**：
  - **内存并发控制**：内部维护 `sync.RWMutex`，对每个 `Key` 维护**引用计数**（Ref-count），支持重叠的 Hold/Release（例如冷启动和突发重连同时作用于同个 Key）。
  - **无死锁非阻塞**：拦截发生时直接 `return nil`，不阻塞 Dispatcher Worker。
- **代码入口**：
  - 核心状态：[`internal/windowgate/gate.go:60`](../internal/windowgate/gate.go#L60) (`Hold`), [`Release:77`](../internal/windowgate/gate.go#L77)
  - 窗口写入适配器：[`internal/windowgate/gate.go:135`](../internal/windowgate/gate.go#L135) (`gatedWindow.PushBarAndTrim`)
  - 截面就绪适配器：[`internal/windowgate/gate.go:148`](../internal/windowgate/gate.go#L148) (`gatedReady.PublishKlineReady`)

---

## 10. 业务工作流对比矩阵

| 工作流名称 | 核心调用入口 | 并发/线程模型 | Goroutine 分割策略 | 写入存储目标 | 核心数据结构 / Key 设计 | 外部查询返回形式 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Universe 标的监控** | `universe.Monitor.loop` | 单常驻 Goroutine | 无分割（定时对齐 00:00 UTC + 2m） | 内存快照 | `Snapshot{Symbols: []string}` | 纯内存/HTTP 状态 |
| **WebSocket 采集** | `collector.shard.run` | 多 Goroutine 分片 | 1. 周期；2. `crc32(SYM) % 4` 哈希分桶 | 内存 Dispatcher | Combined Stream: `<sym>@kline_<iv>` | 纯网络流/内部事件 |
| **Dispatcher 路由分发** | `Dispatcher.HandleKlineEvent` | 多上游 + Worker Pool | 4 个 `ClosedWorkers` 竞争 Channel | 内存 Sinks | `closedJob` + 内存单调 Map (`sym\|iv`) | 纯内存/内部队列 |
| **Redis Live Bar 快照** | `LiveBarWriter.TryEnqueue` | Worker Pool | 2 个 Worker 竞争 Channel | Redis (专用 Client) | `livebar:{SYM}:{iv}` (Hash, TTL=2x) | Redis Hash 字典 (`t`, `o`, `h`, `l`, `c`, `v`...) |
| **Redis 收盘滑窗** | `Writer.PushBarAndTrim` | Dispatcher Workers | 由 Dispatcher 分发，经 Gate 仲裁 | Redis (主 Client) | `kline:{SYM}:{iv}` (List 200, Compact JSON) | Redis List（200 根 9 元素紧凑数组，最新在 0） |
| **全市场截面聚合** | `aggregator.mark` | Dispatcher Workers | 内存 Mutex + Section 定时器 | Redis Stream | `stream:market:kline_ready` | Redis Stream Entry (`interval`, `timestamp`, `count`) |
| **ClickHouse 批量归档** | `BatchWriter.loop` | 单 Goroutine / Interval | 每个 Interval 拥有独立 BatchWriter | ClickHouse | `market.fapi_kline_<iv>` (ReplacingMergeTree) | 列式/行式表格结果（12 列 Raw Fact 数据） |
| **历史 Gapfill 回补** | `Backfiller.run` | 主 Loop + Worker Pool | 主 Loop 串行；Worker Pool 按 Symbol 并发 (4 workers) | CH + Redis | REST `/fapi/v1/klines` + Lua DEL/RPUSH | CH FINAL 表格 + Redis 覆盖后 200 根 List |
| **窗口门控仲裁** | `Gate.Hold / Release` | 调用于主 Loop 与 Workers | 内存并发 RWMutex | 内存引用计数 | `Key{Symbol, Interval}` 引用计数器 | 控制 `kline:*` 与 `stream:market:kline_ready` 冻结 |
