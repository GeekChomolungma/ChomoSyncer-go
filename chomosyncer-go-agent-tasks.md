# Agent 模块化实现任务拆解

采用 **「模块化分步 Prompting（分治法）」**：把设计文档拆成若干独立积木，逐个交付并各自附带 Unit Test；
最后由人负责 Code Review、接口契约核对、组装与端到端 Integration Test。

------------------------------------------------------------------------

## 功能模组总览

| # | 模组 | 包路径 | 对应文档 | 状态 |
|---|------|--------|----------|------|
| 1 | ClickHouse 批量缓冲写入器 | `internal/chwriter` | §3.1, §6 | ✅ 已完成（含单测） |
| 2 | Redis 层：收盘滑窗 + Stream 通知 + 实时快照 | `internal/rediswin` | §3.2, §3.3, §4.1, §6 | ✅ 已完成（2a + 2b，含单测） |
| 3 | Binance 行情流监听 + 断线重连 | `internal/collector` | §2, §5 | ✅ 已完成（含单测） |
| 3b | 动态 Universe 监控 | `internal/universe` | §2.1 | ✅ 已完成（含单测） |
| 4 | Kline Dispatcher（编排 Redis + CH，发截面事件） | `internal/dispatcher` | §5.2 | ✅ 已完成（含单测） |
| 5 | Prometheus `/metrics` | `internal/metrics` | §6 | ✅ 已完成（含单测） |
| 6 | 主程序装配（配置 / 信号 / 优雅退出） | `internal/app` + `cmd/chomosyncer-go` | §5 | ✅ 已完成（含单测） |
| 7 | Python 无状态特征读取骨架 | `python/` | §4.2 | ⬜ 待做 |

### 推荐实施顺序（本轮调整）

```text
1  chwriter                    ✅
2  rediswin (2a+2b)            ✅
4  dispatcher                  ✅
3b universe                    ✅
3  collector                   ✅
5  metrics                     ✅
6  internal/app + cmd/chomosyncer-go   ✅
7  python                      ← 当前
             ↓
        Integration / E2E
```

> 依据：模组 1 / 2 / 4 是「落库 + 编排」骨架，不依赖真实 WS 即可完整单测；模组 3 把真实
> Binance 数据源插进来放到最后，风险最可控。剩下 7（Python reader），之后端到端联调。

------------------------------------------------------------------------

## 任务 1：ClickHouse 批量缓冲写入器 ✅

**包：** `internal/chwriter` ｜ **文档：** §3.1、§6

基于 `github.com/ClickHouse/clickhouse-go/v2` 的线程安全 `BatchWriter`：channel 缓冲，
**5000 条**或 **1000ms** 触发原生列式批量写；优雅退出全量 flush；context 取消；
连接 ping 失败 / prepare / send 出错即作废重连；失败按指数退避 + 抖动重试，超限丢弃并计数。

交付：`BatchWriter` / `New` / `NewWithFlusher` / `Push` / `TryPush` / `Close` + 6 个 Prometheus 指标 + 单测 + `deploy/clickhouse/001_fapi_kline.sql`。

------------------------------------------------------------------------

## 任务 2：Redis 层（收盘滑窗 + Stream 通知 + 实时快照）

**包：** `internal/rediswin` ｜ **文档：** §3.2、§3.3、§4.1、§6

本任务把「Redis 相关的全部写入职责」收敛到一个包，分两部分。

### 2a. 收盘滑窗 + Stream 事件 ✅

**输入文档：** §3.2（Redis 数据契约与 Pipeline 要求）、§4.1（Stream 事件通知契约）

基于 `github.com/redis/go-redis/v9` + `github.com/bytedance/sonic`：

- `CompactBar` 紧凑序列化：9 元素无 key 数组 `[t,o,h,l,c,v,qv,tbv,tbqv]`，`t` 为整数。
- `PushBarAndTrim(ctx, symbol, interval, bar)`：单 RTT Pipeline 执行 `LPUSH` + `LTRIM 0 199`。
- `PushBarsAndTrim(ctx, []SymbolBar)`：整个截面一次 Pipeline。
- `PublishKlineReady(ctx, KlineReadyEvent{Interval, Timestamp, SymbolsCount})`：
  `XADD stream:market:kline_ready`（字段 `interval` / `timestamp` / `symbols_count`）。
- 键 `kline:{SYMBOL}:{interval}`，symbol 统一大写；错误包装 + `ctx.Err()` 前置检查。
- `Config.Atomic` 可切 `TxPipeline`（MULTI/EXEC）。

指标：`redis_pipeline_latency_seconds{op,status}`、`redis_bars_pushed_total`、
`redis_kline_ready_published_total`、`redis_pipeline_errors_total{op}`。

### 2b. 实时未闭合 K 线快照（Live Bar，A 模式）—— 进行中

**输入文档：** §2.2（未闭合 Bar 新增例外）、§3.3（实时快照层）、§6

> 目标：把**所有** `WsKlineEvent`（含 `k.x == false`）复制一份写入独立的实时快照层，
> 让独立的实时消费端随时读取「当前这根 K 线的即时形态」。与收盘滑窗、`kline_ready` 契约完全隔离。

#### Prompt 重点

> 在 `internal/rediswin` 中新增 `LiveBarWriter`，仿照 `chwriter.BatchWriter` 的
> 「buffered channel + 后台 goroutine」结构，但用于 fire-and-forget 的实时写入：
>
> - 键 `livebar:{SYMBOL}:{interval}`，`Hash` 结构，字段 `t o h l c v qv tbv tbqv n x`。
> - 每条写入 = 单 RTT Pipeline：`HSET`（整根覆盖）+ `PEXPIRE`，**不使用 LPUSH/LTRIM**，无定长窗口。
> - `PEXPIRE = TTLMultiple × interval`（默认 2×），未知 interval 回退 `DefaultTTL`。
> - **专用 `*redis.Client`**（调用方注入；提供 `LiveClientOptions(addr)` 生成调优后的 Options：
>   小连接池、短超时、`MaxRetries = -1`）。
> - 热路径解耦：`TryEnqueue(symbol, interval, bar)` 非阻塞，channel 满即丢 + 计数；
>   `Enqueue(ctx, ...)` 阻塞版（可被 ctx / 关闭打断）；`WORKERS` 个 worker goroutine 消费。
> - 可选 `PUBLISH livebar.{interval} <payload>`（11 元素紧凑数组：9 + `n` + `x`）。
> - 优雅退出：停止入队 → drain channel → 等待 worker；`Close()` 不关闭调用方注入的 client。

#### 交付要求

- `LiveBar` 结构 + `NewLiveBar(...)`（从 ms + 字符串 + `isFinal` 构造）+ `hashFields()` + 11 元素 `marshalCompact()`
- `LiveBarConfig`（`KeyPrefix` / `Workers` / `ChannelSize` / `WriteTimeout` / `TTLMultiple` / `DefaultTTL` / `Publish` / `ChannelPrefix`）+ 默认值
- `NewLiveBarWriter(ctx, rdb, cfg)` / `TryEnqueue` / `Enqueue` / `Key` / `Close`
- `LiveClientOptions(addr)` 辅助函数
- interval → duration 解析（`1m/3m/5m/15m/30m/1h/2h/4h/6h/8h/12h/1d/3d/1w/1M`）
- 单命令 Pipeline：`HSET` + `PEXPIRE`（+ 可选 `PUBLISH`）
- 非阻塞入队 + 满即丢 + `redis_livebar_dropped_total`
- 多 worker goroutine + 优雅退出 drain + context 取消
- 指标：`redis_livebar_updates_total` / `redis_livebar_dropped_total` /
  `redis_livebar_latency_seconds` / `redis_livebar_queue_length`
- **Unit Test**（miniredis）：Hash 字段与 `x` 正确、覆盖写、TTL 来自 interval、key 大写、
  满即丢、优雅退出 drain 全部落库、context 取消、可选 Pub/Sub 收到 11 元素数组、nil client 报错

------------------------------------------------------------------------

## 任务 3：Binance 行情流监听与断线重连

**包：** `internal/collector` ｜ **文档：** §2、§5

基于 `github.com/binance/binance-connector-go/clients/derivativestradingusdsfutures` 的
SINGLE-mode WebSocket Streams 实现。**futures 无全市场 kline 聚合流**（见 §2.1 修订），
逐 symbol 订阅 `<symbol>@kline_<interval>`。

- **分片**：`crc32(SYMBOL) % ShardsPerInterval`（默认 4，固定），每 shard 一条独立连接、
  ≤ ~180 stream，建连错峰 300ms。`sharding.go`。
- **中性输出**：`binanceclient.go` 把连接器回调的 `map` → `dispatcher.KlineEvent`，
  经 `streamClient` 接口（`Connect`/`Subscribe`/`Unsubscribe`/`LastMessageAt`/`Errors`/`Close`）
  暴露给 `shard`；测试注入 fake。
- **supervisor**（`shard.go`）：连接器内置有限重连之上再包无限次带抖动指数退避（1s→30s，半抖动）
  + staleness 看门狗（`StaleTimeout` 默认 60s）；连接器自动 pong + 23h 主动重建。
- **回调直连**：连接器回调（每消息一个 goroutine）里直接 `sink.HandleKlineEvent(ev)`
  —— dispatcher 已非阻塞，collector 与 dispatcher 之间无需再加 channel。
- 闭合帧计 `kline_ingested_total{symbol,interval}`；`HandleKlineEvent` 报错计
  `collector_dispatch_errors_total{reason}`，不阻塞读 goroutine。
- `SetSymbols([]string)`（启动 + 每小时）：`planStreams` 重算每 shard 期望 stream 集，
  新建 shard / 应用增量 SUBSCRIBE-UNSUBSCRIBE / 拆除空 shard。
- Context 取消 / `Close()` 优雅退出。

### 交付要求（已完成）

- `Collector` / `New` / `SetSymbols` / `Close` / `EventSink` / `streamClient` 接口 + 生产 wrapper
- 逐 symbol `<symbol>@kline_<interval>` 订阅、crc32 分片、错峰建连
- 无限次 Exponential Backoff with Jitter + staleness 看门狗 + 自动重连
- `ws_connection_status{shard}` / `ws_reconnects_total` / `ws_messages_total` /
  `ws_last_message_age_seconds` / `ws_subscription_updates_total` / `ws_shards_active` /
  `kline_ingested_total{symbol,interval}` / `collector_dispatch_errors_total{reason}`
- Context Cancellation / Graceful Shutdown
- **Unit Test**（16 项，fake `streamClient`）：分片建立、事件转发、错误重连、staleness 重连、
  订阅增量、空桶拆除、连接失败重试、优雅退出、ctx 取消

## 任务 3b：动态 Universe 监控 ✅

**包：** `internal/universe` ｜ **文档：** §2.1

**全市场采集器**：`net/http` + `sonic` 拉 `GET /fapi/v1/exchangeInfo`（**只此一个**，无 `ticker/24hr`）。
**不做**任何流动性 / 活跃度门限 —— 活跃筛选是消费者的事。

- `Snapshot{Symbols []string, RefreshedAt}`：`Symbols` = 全部 `quoteAsset=USDT ∧ contractType=PERPETUAL ∧ status=TRADING`，排序。
  同一份既作 collector 订阅全集，也作 dispatcher `kline_ready` 截面分母。
- 刷新：`RefreshInterval` 默认 **24h**；`>= 1h` 时对齐 **UTC 00:00 + `RefreshOffset`**（默认 +2min），`< 1h` 纯 ticking（测试）。`Start` 首刷同步。
- `Monitor` 实现 `dispatcher.UniverseProvider`（`Size()` = 全市场合约数 / `Has()` = 是否在册）；`OnChange(fn)` 推快照给 collector。
- 刷新失败保留上一份快照。**Unit Test**（7 项，httptest）：全市场过滤 / Provider 接口 / 订阅通知 / 失败保留 / Start-Close / 每日 UTC 对齐 / 子小时 ticking。

------------------------------------------------------------------------

## 任务 4：Kline Dispatcher（编排层）✅

**包：** `internal/dispatcher` ｜ **文档：** §5.2、§3.3

中性输入 `KlineEvent`（不依赖 Binance SDK），`HandleKlineEvent(ev)` 按 `IsFinal` 分流：

| event | Live 快照 | 收盘滑窗 | ClickHouse | 截面聚合 → `kline_ready` |
|---|:---:|:---:|:---:|:---:|
| 未闭合 | ✅ `TryEnqueue` | — | — | — |
| 闭合 | ✅ | ✅ `PushBarAndTrim` | ✅ `TryPush` | ✅ `mark` → 集齐/超时发一次 |

- 四个 sink 通过接口注入（`LiveSink` / `WindowSink` / `ArchiveSink` / `ReadyPublisher`），
  生产类型 `*rediswin.LiveBarWriter` / `*rediswin.Writer` / `*chwriter.BatchWriter` 编译期断言可直接接入。
- `UniverseProvider` 可插拔（`Size()` / `Has()`）；`nil` = 未知 Universe，只走超时兜底。
  由任务 3b 的 `*universe.Monitor` 实现（每日刷新）。
- **单调防护**：按 `(symbol, interval)` 记住最后处理的 `openTime`，`<=` 的闭合帧丢弃并计
  `dispatcher_out_of_order_total`（不影响 Live 路径）。
- **截面聚合器**：`(interval, openTime)` → 已闭合 symbol 集合；`len >= universe.Size()` 即发
  （reason=complete），或 `SectionTimeout` 兜底（reason=timeout）；发布后保留一个 interval 周期
  吞掉迟到帧，杜绝重复发。
- 热路径不阻塞：Live 走 `TryEnqueue`；闭合走 dispatcher 内 `ClosedWorkers` 个 worker
  排空 buffered `closedCh`（满即丢 + `ErrBusy`）。先 `PushBarAndTrim` 再 `mark`，
  保证 `kline_ready` 发出时窗口已写好。
- 优雅退出：`Close()` 停收 → drain `closedCh` → 停聚合器 timer → 等 worker；ctx 取消等价 Close。

指标：`dispatcher_events_total{interval,kind}`、`dispatcher_events_dropped_total{sink}`、
`dispatcher_sink_errors_total{sink}`、`dispatcher_out_of_order_total{interval}`、
`dispatcher_parse_errors_total`、`dispatcher_section_published_total{interval,reason}`、
`dispatcher_section_publish_errors_total`、`dispatcher_section_pending`、
`dispatcher_section_symbols{interval}`。

单测（19 项，合成 event + fake sink）：未闭合只进 Live、闭合四路扇出、字段转换、单调防护、
收齐即发一次、超时即发、超时后迟到帧不重发、未知 Universe 仅超时、非 Universe 币不计数但仍归档、
Live 满不阻塞闭合、archive 丢弃计数、window 报错仍发截面、解析错误、优雅退出 drain、ctx 取消。

------------------------------------------------------------------------

## 任务 5：Prometheus `/metrics` ✅

**包：** `internal/metrics` ｜ **文档：** §6

- `metrics.New(cfg)` 持有**唯一** `*prometheus.Registry`（含 Go runtime / process / `chomosyncer_build_info` collector），
  `m.Registerer()` 交给各模组 `Config.Registerer` —— 各模组用 `promauto` 自注册，`/metrics` 即所有指标之并集。
- `m.Start()` 同步 bind（端口冲突立即报错）+ 后台 serve；`/metrics`（instrumented promhttp）+ `/healthz`。
- `m.Close(ctx)` = `http.Server.Shutdown(ctx)`（ctx 无 deadline 时用 `ShutdownTimeout`）+ 等 serve goroutine 退出。幂等。
- `Addr==""` 关闭 HTTP 但 `Registerer()` / `Handler()` 仍可用（测试 / 嵌入既有 mux）。
- **Unit Test**（7 项，httptest）：共享 registry 收集模组指标、HTTP 服务 `/metrics`+`/healthz`、
  嵌入式 handler、禁用态、端口冲突快失败、Close 幂等、禁用 runtime collector。

------------------------------------------------------------------------

## 任务 6：主程序装配 ✅

**包：** `internal/app`（可测装配层）+ `cmd/chomosyncer-go`（flag/env + 信号）｜ **文档：** §5

- `app.Config` 全部默认值；`cmd/chomosyncer-go` 用 `flag` + `CHOMOSYNCER_*` env（flag 优先）填充。
- `app.New(ctx, cfg)`：装配 `metrics → (closedRDB, liveRDB) → chwriter×interval → rediswin.Writer + LiveBarWriter → dispatcher → collector`，
  `univ.OnChange → col.SetSymbols`；**无网络 I/O**（除 chwriter 的 best-effort ping），任一步失败即回滚已建组件。
- `app.Run(ctx)`：`metrics.Start()`（端口冲突快失败）→ `univ.Start(ctx)`（首刷驱动初始订阅）→ 阻塞至 ctx 取消 → `Shutdown`。
- `app.Shutdown(ctx)`：逆序 `col → disp → live → chwriter×N → univ → redis×2 → metrics`，`errors.Join` 汇总，幂等，部分构建也安全。
- **ClickHouse 分表路由**：`dispatcher.ArchiveSink.TryPush(interval, row)`；app 内 `archiveRouter` 按 interval 派发到
  `market.fapi_kline_<interval>` 的独立 `*chwriter.BatchWriter`（各自 registerer 带 `{interval}` label 避免指标名冲突）。
- `cmd`：`signal.NotifyContext(SIGINT/SIGTERM)` → `app.Run` → 非 0 退出码传播。
- **Unit Test**：`app` 5 项（无外部依赖 New+Shutdown 不 panic / Run 对不可达 universe 快失败 / archiveRouter 路由 + 未配置 interval 报错 + 错误透传）；`cmd` 5 项（flag 默认 / 覆盖 / env / CSV trim / logger）。

------------------------------------------------------------------------

## 任务 7：Python 无状态特征读取骨架

**目录：** `python/` ｜ **文档：** §4.2

- 订阅 `stream:market:kline_ready` → 解析 `interval` / `timestamp` / `symbols_count`
- `redis.pipeline()` 批量 `LRANGE kline:{symbol}:{interval} 0 199`
- Compact Bar 反序列化 → Polars DataFrame（3D 矩阵）
- 无状态示例：截面 Momentum、RSI
- （可选）实时消费端示例：`HGETALL livebar:*` 或 `SUBSCRIBE livebar.{interval}`
- **Unit Test**

------------------------------------------------------------------------

## 人工负责

1. Code Review；2. 接口契约核对；3. 组装；4. 端到端 Integration Test；
5. 启动 Binance → Redis / ClickHouse → Python 完整链路。
