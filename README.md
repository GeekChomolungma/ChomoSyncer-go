# ChomoSyncer-go

Binance U 本位永续合约 K 线行情实时采集器。低延迟流式接入 + 多级落库
(Redis 极热缓存 / ClickHouse 时序归档)，为下游 Python 策略层提供只读事实数据。

设计说明书：[`chomoSyncer-go_design_doc.md`](./chomoSyncer-go_design_doc.md)
任务拆解：[`chomosyncer-go-agent-tasks.md`](./chomosyncer-go-agent-tasks.md)
**部署 / 集成 / 压测手册：[`docs/OPERATIONS.md`](./docs/OPERATIONS.md)**（如何拉起 ClickHouse + Redis、配置并启动、正向集成测试、压力测试）

## 功能模组拆解

| # | 模组 | 包路径 | 职责 | 对应设计文档 | 状态 |
|---|------|--------|------|--------------|------|
| 1 | ClickHouse 批量缓冲写入器 | `internal/chwriter` | channel 缓冲 → 5000 条 / 1000ms 触发原生列式批量写；优雅退出全量 flush；连接重连；context 取消 | §3.1, §6 | ✅ 已实现 + 单测 |
| 2 | Redis 层：收盘滑窗 + Stream 通知 + 实时快照 | `internal/rediswin` | 2a `PushBarAndTrim` = Pipeline `LPUSH`+`LTRIM 0 199` / `kline_ready` Stream；2b `LiveBarWriter` 未闭合 K 线实时快照（专用 client + chan 缓冲 + worker pool） | §3.2, §3.3, §4.1 | ✅ 已实现 + 单测 |
| 4 | Kline Dispatcher（编排层） | `internal/dispatcher` | 中性 `KlineEvent` → 按 `IsFinal` 四路扇出（Live 快照 / 收盘滑窗 / ClickHouse / 截面聚合→`kline_ready`）；单调防护；可插拔 `UniverseProvider` + 超时兜底 | §5.2, §3.3 | ✅ 已实现 + 单测 |
| 3b | 动态 Universe 监控 | `internal/universe` | `net/http`+`sonic` 拉 `exchangeInfo`；**全市场** USDT 永续（无门限）→ 同时作订阅全集与截面分母；每日 UTC 午夜后刷新；实现 `dispatcher.UniverseProvider` | §2.1 | ✅ 已实现 + 单测 |
| 3 | Binance 行情流监听 + 断线重连 | `internal/collector` | 逐 symbol `<sym>@kline_<iv>` 订阅（futures 无 kline 聚合流）；crc32 分片；无限退避重连 + staleness 看门狗；回调直连 dispatcher | §2, §5 | ✅ 已实现 + 单测 |
| 5 | 可观测性 | `internal/metrics` | 唯一 `prometheus.Registry`（+ Go/process/build collector）；`/metrics` + `/healthz` HTTP server；优雅关闭；`Registerer()` 交给各模组 | §6 | ✅ 已实现 + 单测 |
| 6 | 主程序装配 | `internal/app` + `cmd/chomosyncer-go` | 配置加载（flag/env）、全链路装配、信号驱动优雅退出、ClickHouse 按 interval 分表路由 | §5 | ✅ 已实现 + 单测 |
| 7 | Python 无状态特征读取骨架 | `python/` *(待做)* | 订阅 `kline_ready`；pipeline 批量读 200 根 Bar；Polars 全量重算 | §4.2 | ⬜ 待做 |

依赖顺序（本轮调整）：`1 → 2 → 4 → 3b → 3 → 5 → 6 → 7 → Integration`。当前进度到 6；剩 7（Python reader）+ 端到端联调。

## 模块 1：`internal/chwriter`（已完成）

ClickHouse 批量缓冲写入器。

- `chwriter.New(ctx, cfg)` — 真实 ClickHouse 连接（`clickhouse-go/v2` 原生 TCP，LZ4 压缩）。
- `chwriter.NewWithFlusher(ctx, cfg, flusher)` — 注入自定义 `Flusher`（测试 / 备用 sink）。
- `w.Push(ctx, row)` — 阻塞式入队，可被 ctx / 关闭打断（可容忍背压的路径）。
- `w.TryPush(row)` — 非阻塞入队，缓冲满立即返回 `ErrBufferFull`（WebSocket 热路径）。
- `w.Close()` — 优雅关闭：停收 → 排空 channel → 有界最终 flush → 关闭底层连接。

触发 flush 的条件（先到先触发）：
- 缓冲行数达到 `BatchSize`（默认 **5000**）；
- 距上次 flush 超过 `FlushInterval`（默认 **1000ms**）；
- `Close()` 或根 context 取消 → 排空并做一次有界 flush。

失败处理：flush 失败按指数退避 + 抖动重试至 `MaxRetries`，仍失败则丢弃该批并计入
`clickhouse_rows_dropped_total`；底层连接在 ping 失败 / prepare / send 出错时作废并于下次
flush 重连。

Prometheus 指标：`clickhouse_buffer_size`、`clickhouse_flush_latency_seconds{status}`、
`clickhouse_flush_total{status}`、`clickhouse_rows_flushed_total`、
`clickhouse_rows_dropped_total`、`clickhouse_flush_retries_total`。

### 用法示例

```go
w, err := chwriter.New(ctx, chwriter.Config{
    Addrs:      []string{"clickhouse:9000"},
    Database:   "market",
    Username:   "default",
    Table:      "market.fapi_kline_1m",
    Registerer: prometheus.DefaultRegisterer,
})
if err != nil { log.Fatal(err) }
defer w.Close()

row, _ := chwriter.NewRow("BTCUSDT", k.StartTime, k.EndTime,
    k.Open, k.High, k.Low, k.Close, k.Volume, k.QuoteVolume,
    k.TakerBuyBaseVolume, k.TakerBuyQuoteVolume, k.TradeNum)
_ = w.TryPush(row)
```

### 建表

```sh
clickhouse-client --multiquery < deploy/clickhouse/001_fapi_kline.sql
```

## 模块 2：`internal/rediswin`（已完成）

Redis 在线滑窗模型（设计文档 §3.2）+ 截面就绪事件通知（§4.1）。

- `rediswin.New(rdb, cfg)` — 接受任意 go-redis 客户端（`*redis.Client` / `*redis.ClusterClient`），
  `rdb` 生命周期由调用方负责，`Writer` 不会关闭它。
- `w.PushBarAndTrim(ctx, symbol, interval, bar)` — 单个 round-trip 内 Pipeline 执行
  `LPUSH kline:{SYMBOL}:{interval} <compactJSON>` + `LTRIM ... 0 199`。
- `w.PushBarsAndTrim(ctx, []SymbolBar)` — 整个截面一次 Pipeline 写完（150+ 币种 1 个 RTT）。
- `w.PushRawAndTrim(ctx, symbol, interval, payload)` — 调用方已持有编码字节时用（回补/重放）。
- `w.PublishKlineReady(ctx, KlineReadyEvent{Interval, Timestamp, SymbolsCount})` — 截面全部闭合后
  `XADD stream:market:kline_ready`（字段 `interval` / `timestamp` / `symbols_count`），返回 entry ID。

要点：
- **Compact Bar**：`CompactBar.Marshal()` 用 sonic 输出 9 元素**无 key 数组**
  `[start_time, open, high, low, close, volume, quote_volume, taker_buy_volume, taker_buy_quote_volume]`，
  `start_time` 为整数（ms，`k.t`）。`NewCompactBar(...)` 从 Binance 原始 ms/字符串构造。
- **键**：symbol 统一大写（对齐文档命名空间与 Binance `s` 字段），`interval` 原样。
- **原子性**：默认用普通 Pipeline（对齐 §5.2 骨架，低延迟）；`Config.Atomic=true` 改用
  `TxPipeline`（MULTI/EXEC），适合多写者场景。
- **Context 取消**：每个方法入口先查 `ctx.Err()`，取消即返回，不产生 Redis I/O。
- **错误处理**：Redis 端失败包装后返回并计入 `redis_pipeline_errors_total{op}`；序列化失败在任何 I/O 前返回。

Prometheus 指标：`redis_pipeline_latency_seconds{op,status}`（op = `push_trim` / `xadd`）、
`redis_bars_pushed_total`、`redis_kline_ready_published_total`、`redis_pipeline_errors_total{op}`。

### 用法示例

```go
rdb := redis.NewClient(&redis.Options{Addr: "redis:6379"})
defer rdb.Close()

w, err := rediswin.New(rdb, rediswin.Config{Registerer: prometheus.DefaultRegisterer})
if err != nil { log.Fatal(err) }

bar, _ := rediswin.NewCompactBar(k.StartTime,
    k.Open, k.High, k.Low, k.Close, k.Volume, k.QuoteVolume,
    k.TakerBuyBaseVolume, k.TakerBuyQuoteVolume)
_ = w.PushBarAndTrim(ctx, k.Symbol, k.Interval, bar)

// 整点截面全部闭合后
_, _ = w.PublishKlineReady(ctx, rediswin.KlineReadyEvent{
    Interval: "1h", Timestamp: barStartMs, SymbolsCount: 182,
})
```

### 2b：`LiveBarWriter` —— 未闭合 K 线实时快照（设计文档 §3.3）

把**所有** `WsKlineEvent`（含未闭合 `k.x==false`）复制一份写入独立的实时快照层，
供独立的实时消费端读取「当前这根 K 线的即时形态」。**不参与** `kline_ready` 契约。

- `rediswin.NewLiveBarWriter(ctx, rdb, cfg)` — 仿 `chwriter.BatchWriter`：buffered channel + `Workers` 个 worker goroutine，fire-and-forget，不重试。
- `w.TryEnqueue(symbol, interval, bar)` — 非阻塞（WS 热路径），channel 满即丢 + `redis_livebar_dropped_total`。
- `w.Enqueue(ctx, ...)` — 阻塞版（可被 ctx / 关闭打断）。
- `w.Close()` — 停止入队 → drain channel → 等待 worker；**不关闭**注入的 `rdb`。
- `rediswin.LiveClientOptions(addr)` — 生成调优后的 `*redis.Options`（小连接池、短超时、`MaxRetries=-1`），
  用它单独建一个 `*redis.Client`，与收盘写入的 client 物理隔离连接池。

写入：`livebar:{SYMBOL}:{interval}` Hash（字段 `t o h l c v qv tbv tbqv n x`），
单 RTT Pipeline `HSET`（整根覆盖）+ `PEXPIRE`（`TTLMultiple × interval`，默认 2×，未知 interval 回退 `DefaultTTL`）；
**不使用 LPUSH/LTRIM，无定长窗口**。`Config.Publish=true` 时额外 `PUBLISH livebar.{interval} <11元素数组>`。

指标：`redis_livebar_updates_total`、`redis_livebar_dropped_total`、
`redis_livebar_latency_seconds{status}`、`redis_livebar_queue_length`。

```go
liveRDB := redis.NewClient(rediswin.LiveClientOptions("redis:6379")) // 独立 client / 连接池
defer liveRDB.Close()

lw, _ := rediswin.NewLiveBarWriter(ctx, liveRDB, rediswin.LiveBarConfig{
    Workers: 2, Publish: true, Registerer: prometheus.DefaultRegisterer,
})
defer lw.Close()

// collector WS goroutine：无论是否闭合，都非阻塞入队
lb, _ := rediswin.NewLiveBar(k.StartTime,
    k.Open, k.High, k.Low, k.Close, k.Volume, k.QuoteVolume,
    k.TakerBuyBaseVolume, k.TakerBuyQuoteVolume, k.TradeNum, k.IsFinal)
_ = lw.TryEnqueue(k.Symbol, k.Interval, lb)
```

## 模块 4：`internal/dispatcher`（已完成）

编排层（设计文档 §5.2）。消费 collector 解码后的中性 `KlineEvent`（**不依赖 Binance SDK**），
`HandleKlineEvent(ev)` 按 `ev.IsFinal` 分流：

| event | Live 快照 (§3.3) | 收盘滑窗 (§3.2) | ClickHouse (§3.1) | 截面聚合 → `kline_ready` (§4.1) |
|---|:---:|:---:|:---:|:---:|
| 未闭合 | ✅ `LiveSink.TryEnqueue` | — | — | — |
| 闭合 | ✅ | ✅ `WindowSink.PushBarAndTrim` | ✅ `ArchiveSink.TryPush` | ✅ `aggregator.mark` → 集齐/超时发一次 |

- 四个 sink 用接口注入（`LiveSink` / `WindowSink` / `ArchiveSink` / `ReadyPublisher`）；
  `*rediswin.LiveBarWriter` / `*rediswin.Writer` / `*chwriter.BatchWriter` 编译期断言可直接接入，
  单测用 fake 实现。
- `UniverseProvider`（`Size()` / `Has()`）可插拔；传 `nil` = 未知 Universe → 只走超时兜底。
  hourly 刷新实现留给模块 3b（`NewStaticUniverse(...)` 用于 bring-up 和测试）。
- **单调防护**：按 `(symbol, interval)` 记住最后处理的 `openTime`，`<=` 的闭合帧丢弃并计
  `dispatcher_out_of_order_total`（Live 路径不受影响，每帧都是新快照）。
- **截面聚合器**：`(interval, openTime)` → 已闭合 symbol 集合；`len >= universe.Size()` 即发
  （`reason=complete`）或 `SectionTimeout` 兜底（`reason=timeout`）；发布后保留一个 interval
  周期吞掉迟到帧，杜绝对同一截面重复发。
- **热路径不阻塞**：Live 走 `TryEnqueue`（rediswin 内部已 buffer）；闭合走 dispatcher 内
  `ClosedWorkers` 个 worker 排空 buffered `closedCh`（满即丢 + 返回 `ErrBusy`）。
  worker 内先 `PushBarAndTrim` 再 `mark`，保证 `kline_ready` 发出时所有计数币的窗口已写好。
- 优雅退出：`Close()` 停收 → drain `closedCh` → 停聚合器 timer → 等 worker；ctx 取消等价 `Close`。

指标：`dispatcher_events_total{interval,kind}`、`dispatcher_events_dropped_total{sink}`、
`dispatcher_sink_errors_total{sink}`、`dispatcher_out_of_order_total{interval}`、
`dispatcher_parse_errors_total`、`dispatcher_section_published_total{interval,reason}`、
`dispatcher_section_publish_errors_total`、`dispatcher_section_pending`、
`dispatcher_section_symbols{interval}`。

```go
d, _ := dispatcher.New(ctx, dispatcher.Config{
    ClosedWorkers: 4, SectionTimeout: 5 * time.Second,
    Registerer: prometheus.DefaultRegisterer,
}, dispatcher.Sinks{
    Live: liveWriter, Window: winWriter, Archive: chWriter, Ready: winWriter,
}, universeProvider /* 可为 nil */)
defer d.Close()

// collector：每条解码后的事件（闭合与否都传）
_ = d.HandleKlineEvent(dispatcher.KlineEvent{
    Symbol: ev.Symbol, Interval: ev.Interval,
    OpenTime: ev.StartTime, CloseTime: ev.EndTime,
    Open: ev.Open, High: ev.High, Low: ev.Low, Close: ev.Close,
    Volume: ev.Volume, QuoteVolume: ev.QuoteVolume,
    TakerBuyVolume: ev.TakerBuyBaseVolume, TakerBuyQuoteVolume: ev.TakerBuyQuoteVolume,
    TradeCount: ev.TradeNum, IsFinal: ev.IsFinal,
})
```

## 模块 3b：`internal/universe`（已完成）

**全市场**交易对发现（设计文档 §2.1）。`net/http` + `sonic` 拉一个免鉴权 REST（`exchangeInfo`）。
**不做**流动性 / 活跃度门限 —— 本服务是全市场采集器，活跃筛选交给消费者。

- `universe.New(cfg)` → `m.Start(ctx)`（首刷同步，失败即报）+ 后台刷新循环 → `m.Close()`。
- 刷新节奏：`RefreshInterval` 默认 **24h**；`>= 1h` 时循环对齐到 **UTC 00:00 + `RefreshOffset`**（默认 +2min），
  `< 1h` 则纯 ticking（测试用）。启动即先同步刷一次。
- 每次刷新产出 `Snapshot{Symbols []string, RefreshedAt}`：`Symbols` = 排序后的全部
  `quoteAsset=USDT ∧ contractType=PERPETUAL ∧ status=TRADING`。
- `m.OnChange(fn)` 注册回调（刷新后同步触发）——装配时接给 `collector.SetSymbols`。
- `*Monitor` 实现 `dispatcher.UniverseProvider`：`Size()` = 全市场合约数，`Has()` = 是否在册可交易。
- 刷新失败保留上一份快照。指标：`universe_refresh_total{status}`、`universe_refresh_duration_seconds`、
  `universe_size`、`universe_last_refresh_timestamp_seconds`。

## 模块 3：`internal/collector`（已完成）

Binance USDⓈ-M futures kline WebSocket 采集层（设计文档 §2、§5）。**唯一** import 官方连接器
（`binance-connector-go/clients/derivativestradingusdsfutures`）的包；对外只吐 `dispatcher.KlineEvent`，
对 Redis / ClickHouse 无感知。

**为什么逐 symbol 订阅**：futures WebSocket **没有** `!kline_<interval>@arr` 全市场聚合流（那是 Spot 专属），
只能订 `<symbol>@kline_<interval>`（如 `btcusdt@kline_1m`）。

- **分片**（`sharding.go`）：先按 `interval`，再按 `crc32(SYMBOL) % ShardsPerInterval`（默认 4，**固定**）。
  每 shard = 一条独立 SINGLE-mode 连接，≤ ~180 stream；建连错峰 300ms。固定模数保证 symbol→shard 稳定，
  Universe 每日增删（上/下市）只在受影响 shard 上产生增量 SUBSCRIBE/UNSUBSCRIBE。
- **`streamClient` 接口**（`Connect`/`Subscribe`/`Unsubscribe`/`LastMessageAt`/`Errors`/`Close`）：
  生产实现 `binanceclient.go` 包连接器 + `map → dispatcher.KlineEvent`；测试注入 fake。
- **shard supervisor**（`shard.go`）：连接器内置有限重连（≤10）+ 自动 pong + 23h 主动重建之上，
  再包 **无限次**带抖动指数退避（1s→30s 半抖动）+ **staleness 看门狗**（`StaleTimeout` 默认 60s 无帧即强制重连，防半开 TCP）。
- **回调直连**：连接器每消息 spawn 一个 goroutine，里面直接 `sink.HandleKlineEvent(ev)`；
  dispatcher 已非阻塞，collector 与 dispatcher 之间**不加 channel**。闭合帧计 `kline_ingested_total{symbol,interval}`。
- `SetSymbols([]string)`（启动 + 每日 `OnChange` 触发）：重算每 shard 期望 stream 集 → 建新 shard / 应用增量 / 拆空 shard。
- `Close()` / ctx 取消：停所有 shard、幂等收尾，不关 sink。

指标：`ws_connection_status{shard}`、`ws_reconnects_total{shard}`、`ws_messages_total{shard}`、
`ws_last_message_age_seconds{shard}`、`ws_subscription_updates_total{shard}`、`ws_shards_active`、
`kline_ingested_total{symbol,interval}`、`collector_dispatch_errors_total{reason}`。

```go
mon := universe.New(universe.Config{Registerer: reg}) // 全市场，无门限
if err := mon.Start(ctx); err != nil { log.Fatal(err) }
defer mon.Close()

col, _ := collector.New(ctx, collector.Config{Intervals: []string{"1m", "1h"}, Registerer: reg}, disp)
defer col.Close()

mon.OnChange(func(s universe.Snapshot) { col.SetSymbols(s.Symbols) })
col.SetSymbols(mon.Snapshot().Symbols) // 首次
// dispatcher 用 mon 作 UniverseProvider：dispatcher.New(ctx, cfg, sinks, mon)
```

## 模块 5：`internal/metrics`（已完成）

全局唯一 `*prometheus.Registry` + `/metrics` HTTP server（设计文档 §6）。

- `metrics.New(cfg)` 建一个私有 registry（**不用** `prometheus.DefaultRegisterer`），注册
  Go runtime / process collector + `chomosyncer_build_info{version,go_version}`。
- **汇总方式**：`m.Registerer()` 交给每个模组的 `Config.Registerer`，各模组用 `promauto` 自注册；
  `/metrics` 自然是所有模组指标之并集。metrics 模组本身不需要知道任何指标名。
- `m.Start()` —— 同步 `net.Listen`（端口被占立即返回 error），再后台 `Serve`；
  路由 `/metrics`（instrumented promhttp，带 `promhttp_metric_handler_*`）+ `/healthz`。
- `m.Close(ctx)` —— `http.Server.Shutdown(ctx)`（ctx 无 deadline 时套用 `ShutdownTimeout`，默认 5s）
  + 等 serve goroutine 退出。幂等。
- `Addr==""` 关闭 HTTP，但 `m.Registerer()` / `m.Handler()` 仍可用（测试 / 嵌入既有 mux）。

```go
mx, err := metrics.New(metrics.Config{Addr: ":9090", Version: buildVersion})
if err != nil { log.Fatal(err) }
if err := mx.Start(); err != nil { log.Fatal(err) }        // 端口冲突在此报错
defer mx.Close(context.Background())

reg := mx.Registerer()
chw, _ := chwriter.New(ctx, chwriter.Config{Addrs: chAddrs, Registerer: reg})
w,   _ := rediswin.New(rdb, rediswin.Config{Registerer: reg})
lw,  _ := rediswin.NewLiveBarWriter(ctx, liveRDB, rediswin.LiveBarConfig{Registerer: reg})
disp, _ := dispatcher.New(ctx, dispatcher.Config{Registerer: reg}, sinks, mon)
col, _ := collector.New(ctx, collector.Config{Registerer: reg}, disp)
mon := universe.New(universe.Config{Registerer: reg})
// → GET :9090/metrics 一次看到 chwriter / rediswin / dispatcher / collector / universe 全部指标
```

## 模块 6：`internal/app` + `cmd/chomosyncer-go`（已完成）

全链路装配 + 主程序（设计文档 §5）。

- **`internal/app`** —— 可测的装配层。`app.New(ctx, cfg)` 把
  `metrics → (closedRDB, liveRDB) → chwriter×interval → rediswin.Writer + LiveBarWriter → dispatcher → collector`
  串起来，注册 `univ.OnChange → col.SetSymbols`；构造期**无网络 I/O**（除 chwriter 的 best-effort ping），
  任一步失败即回滚已建组件。`app.Run(ctx)` 启动 metrics HTTP + universe 首刷（驱动初始订阅），阻塞至
  ctx 取消后逆序 `Shutdown`（`col → disp → live → chwriter×N → univ → redis×2 → metrics`，`errors.Join`，幂等）。
- **ClickHouse 分表路由**：`dispatcher.ArchiveSink.TryPush(interval, row)`；`app` 内 `archiveRouter`
  按 interval 派发到 `market.fapi_kline_<interval>` 各自独立的 `*chwriter.BatchWriter`
  （每个 registerer 带 `{interval}` label，避免 `clickhouse_*` 指标名冲突）。
- **`cmd/chomosyncer-go`** —— `flag` + `CHOMOSYNCER_*` 环境变量（flag 优先），
  `signal.NotifyContext(SIGINT/SIGTERM)` 驱动优雅退出，非 0 退出码传播。

```sh
go build -o bin/chomosyncer-go ./cmd/chomosyncer-go
./bin/chomosyncer-go \
  -redis-addr localhost:6379 \
  -ch-addr localhost:9000 -ch-database market \
  -intervals 1m,1h -metrics-addr :9090
# 或全部用 CHOMOSYNCER_* 环境变量
```

## 开发

```sh
go build ./...
go test ./...
```

> `go test -race` 需要 CGO / gcc；当前环境未安装，CI 中请启用。

## 部署

```sh
clickhouse-client --multiquery < deploy/clickhouse/001_fapi_kline.sql   # 建 market.fapi_kline_1m / _1h
```
