# ChomoSyncer-go 模块架构与核心设计详解

本文档详细剖析 `ChomoSyncer-go` 各核心业务模块的设计原则、内部机制、并发模型、容错策略与接口规范。

---

## 模块全景与依赖拓扑

```text
                  ┌──────────────────────┐
                  │   internal/universe  │ (全市场动态标的发现)
                  └──────────┬───────────┘
                             │ OnChange(Symbols)
                             ▼
┌────────────────────────────────────────────────────────┐
│                   internal/collector                   │ (WebSocket 分片流接入)
│            crc32(symbol) % ShardsPerInterval           │
└────────────────────────────┬───────────────────────────┘
                             │ KlineEvent (中性事件)
                             ▼
┌────────────────────────────────────────────────────────┐
│                  internal/dispatcher                   │ (分发器 + 截面聚合)
│         单调防护 + 四路扇出 + 截面就绪定时对齐           │
└───────┬──────────────┬──────────────┬──────────────────┘
        │              │              │
        │ Live Bar     │ 闭合 Bar     │ 闭合 Bar
        ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐
│ rediswin     │ │ windowgate   │ │ chwriter             │
│ (LiveBar-    │ │ (窗口门控)   │ │ (ClickHouse 批量写)  │
│  Writer)     │ └──────┬───────┘ └──────────┬───────────┘
└──────────────┘        │                    │
                        ▼                    │
                 ┌──────────────┐            │
                 │ rediswin     │            │
                 │ (SlidingWin) │            │
                 └──────┬───────┘            │
                        │                    │
                        │ kline_ready Stream │
                        ▼                    ▼
┌────────────────────────────────────────────────────────┐
│                   internal/backfill                    │ (历史回补与零缺口修复)
│    冷启动 / 断线重连 / 窗口重建 / Token Bucket 限流    │
└────────────────────────────────────────────────────────┘
```

---

## 模块 1：`internal/chwriter`（ClickHouse 批量缓冲写入器）

### 1.1 职责与定位
提供面向 ClickHouse 的高吞吐、低开销批量落库能力。规避高频单条插入对 LSM-tree 类引擎造成的部件合并风暴（Too many parts）。

> 本构建只采集 1m 基准 K 线，因此**只实例化一个 `BatchWriter`**，写入唯一原始表 `market.fapi_kline_1m`。`5m/15m/1h/4h/1d` 等更粗周期不由 Go 写入，而是 ClickHouse 端从 `fapi_kline_1m FINAL` 幂等重算的 rollup 表（见本文 §8.6 与 `deploy/clickhouse/002_kline_rollups.sql`）。

### 1.2 核心机制
- **列式批量写入**：基于官方 `clickhouse-go/v2` 原生 TCP 驱动，启用 LZ4 数据压缩。
- **无锁通道缓冲**：内存维护 `chan Row` 队列（默认容量 20,000）。
  - WebSocket 实时流走 `TryPush(row)`：非阻塞写入，队列满时丢弃并记录报警指标；
  - 历史回补流走 `Push(ctx, row)`：阻塞入队，确保历史补齐数据零丢失。
- **双触发 Flush 规则**（先到先触发）：
  1. **行数满**：队列累积达到 `BatchSize`（默认 5,000 行）；
  2. **时间到**：距上次落盘间隔达到 `FlushInterval`（默认 1,000 ms）；
  3. **停机信号**：进程退出时触发带超时的排空（Drain & Flush）。
- **重试与连接自愈**：
  - 单批写入失败后执行带随机抖动的指数退避重试（最多 5 次）；
  - 遇到连接断开或网络异常，底层自动废弃旧连接并在下次写入前重新握手。

---

## 模块 2：`internal/rediswin`（Redis 滑窗、LiveBar 与截面通知）

本包涵盖系统与 Redis 交互的所有核心逻辑，细分为三大子功能：

### 2.1 已收盘滚动滑窗 (`Writer`)
- **存储结构**：Redis List，Key 命名为 `kline:{SYMBOL}:1m`。本构建只写 1m 窗口；更粗周期无 Redis 窗口，下游查 ClickHouse rollup 表（见 §8.6）。
- **原子单调推入 (`PushBarAndTrim`)**：
  - 采用内置 Lua 脚本执行 `LPUSH` + `LTRIM 0 199`；
  - **单调性防护**：推入前读取列表头部 `LINDEX 0` 的时间戳，仅当待写入 Bar 时间戳严格大于头部时才执行推入，杜绝网络乱序导致的滑窗时间倒流。
- **批量截面推入 (`PushBarsAndTrim`)**：
  - 单一 RTT Pipeline 批量写入全市场数百个币种的已收盘 Bar，写入延迟控制在 5ms 以内。
- **紧凑序列化**：
  - 采用 9 元素无 key JSON 紧凑数组：`[t, o, h, l, c, v, qv, tbv, tbqv]`，极大降低内存占用与 Python 端解析反序列化耗时。

### 2.2 未收盘实时快照 (`LiveBarWriter`)
- **存储结构**：Redis Hash，Key 命名为 `livebar:{SYMBOL}:{interval}`（例如 `livebar:BTCUSDT:1m`）。
- **独立连接池与 Worker**：
  - 配置独立的 Redis Client（默认小连接池、200ms 短超时、无重试），防止实时行情突发写入争抢已收盘滑窗的连接资源；
  - 内部具备独立的 `input chan`（容量 8192）和工作协程池（默认 2 个 Worker）。
- **字段契约**：
  - 覆盖写入 11 个 Hash 字段：`t, o, h, l, c, v, qv, tbv, tbqv, n, x`；
  - 附带毫秒级 TTL（默认 `2 * interval`，例如 1m 对应 120s），无更新自动消亡。
  - 可选开启 `publish: true`，将实时快照同步 PUBLISH 到频道 `livebar.<interval>`。

### 2.3 截面就绪通知 (`PublishKlineReady`)
- **存储结构**：Redis Stream，Key 为 `stream:market:kline_ready`。
- **通知 Payload**：
  - `interval`：K 线周期（"1m", "1h"）；
  - `timestamp`：收盘周期开盘毫秒时间戳（`k.t`）；
  - `symbols_count`：当前截面实际到齐的标的数量。
- 采用 `MAXLEN ~ 10000` 近似修剪，防止 Stream 无限膨胀。

---

## 模块 3：`internal/collector`（Binance WebSocket 采集层）

### 3.1 职责与特性
唯一与币安衍生品 WebSocket 服务直接对接的模块。对下游屏蔽 Binance SDK 结构细节，直接输出标准化 `dispatcher.KlineEvent`。

### 3.2 核心机制
- **确定性分片架构 (`sharding.go`)**：
  - 币安永续合约不支持全市场聚合流，必须单独订阅 `<symbol>@kline_<interval>`；
  - 分片算法：`crc32(SYMBOL) % ShardsPerInterval`（默认每个周期 4 个分片连接）；
  - 固定哈希模数保证币种到分片的映射完全确定，新币上市或旧币下市仅在对应分片触发增量 `SUBSCRIBE` / `UNSUBSCRIBE`，无需重建整条连接。
- **防风暴与建连错峰**：
  - 多个分片连接之间引入 `connect_stagger`（默认 300ms）错峰建连，避免并发握手触发交易所 IP 频控。
- **双重保活与看门狗 (`shard.go`)**：
  - 底层驱动内置自动 Ping/Pong 与 23 小时主动平滑重建；
  - 顶层配置 **Staleness 看门狗**（默认 60s）：若某分片超过该时间未收到任何行情帧（即使 TCP 保持 ESTABLISHED），判定为假死连接并强制断开重连。
- **无限退避重连**：
  - 断线后按指数退避（1s → 30s）并叠加随机抖动，持续重试直至恢复。

---

## 模块 3b：`internal/universe`（全市场交易对动态发现）

### 3.1 职责与特性
- **全市场覆盖原则**：严禁人为设置成交量、持仓量或流动性门限。
- 从币安合约 REST 接口拉取全部处于交易中的 USDT 永续合约：
  `quoteAsset == "USDT" && contractType == "PERPETUAL" && status == "TRADING"`。

### 3.2 刷新与事件广播
- **每日定时刷新**：
  - 默认每 24 小时刷新一次，对齐至每日 **UTC 00:00:00 之后偏移 2 分钟**（避开交易所每日结算窗口）；
- **动态变更广播**：
  - 探测到币种列表增删时，触发 `OnChange` 回调，热更新 `collector` 订阅集与 `dispatcher` 截面分母。

---

## 模块 4：`internal/dispatcher`（K 线事件编排与截面聚合器）

### 4.1 事件四路分流
接收中性 `KlineEvent`（本构建里 `Interval` 恒为 `1m`），根据 `IsFinal` 状态机执行严格路由：

| 事件状态 | Live 快照 (Redis) | 收盘滑窗 (Redis) | 历史归档 (ClickHouse) | 截面聚合器 (Aggregator) |
| :--- | :---: | :---: | :---: | :---: |
| **未闭合** (`IsFinal=false`) | ✅ `LiveSink.TryEnqueue` | ❌ 忽略 | ❌ 忽略 | ❌ 忽略 |
| **已闭合** (`IsFinal=true`) | ✅ `LiveSink.TryEnqueue` | ✅ `WindowSink.PushBarAndTrim` | ✅ `ArchiveSink.TryPush` | ✅ `aggregator.mark` |

### 4.1b 派生截面信号 (`serve_intervals`)

1m 截面发布成功后，聚合器检查 `serve_intervals`（默认 `5m,15m,1h,4h,1d`）：若这根 1m Bar 的 `openTime` 恰好闭合某个更粗桶（`(openTime + 60000) % D == 0`），就以相同的 `symbols_count` 追加发布一条 `kline_ready`，`interval` 标注为该粗周期、`timestamp` 为粗桶开盘时刻。派生信号也受窗口门控约束——只有基准 1m 截面确实发出（未被 gapfill 压制）时才级联。粗周期的 200 根窗口不进 Redis，下游收到派生信号后直接查 ClickHouse rollup 表。

### 4.2 单调性递增防护
维护 `(symbol, interval) -> lastClosedOpenTime` 状态。若收到 `<= lastClosedOpenTime` 的闭合帧，立即作为乱序/重复帧丢弃，保护下游时序单调性。

### 4.3 截面聚合对齐器 (`Aggregator`)
- **双重触发机制**：
  1. **全员齐备触发 (`onComplete`)**：当周期内已闭合标的数量达到 Universe 标的总数时，立即触发；
  2. **兜底超时触发 (`onTimeout`)**：首根闭合 Bar 到达后启动 `section_timeout` 定时器（默认 5s）。若个别冷门币种因成交稀疏迟迟未收盘，超时强制发布当前已齐备的截面通知。
- **截面防重与留存淘汰 (`SectionRetention`)**：
  - 截面发布后，状态在内存中继续保留一个周期间隔，防止迟到帧重复触发多余的 `kline_ready` 通知。

---

## 模块 5：`internal/metrics`（可观测性与监控探针）

- **统一私有注册表**：基于 `prometheus.NewRegistry()` 构建专属注册表，不侵入全局默认 DefaultRegisterer。
- **HTTP 探针路由**：
  - `GET /metrics`：Prometheus 标准抓取端点，汇总各模块指标；
  - `GET /healthz`：Liveness 探针，进程存活返回 200；
  - `GET /readyz`：Readiness 探针，冷启动历史回补未完成时返回 503，回补完毕且全链路就绪后返回 200。

---

## 模块 6：`internal/app` + `cmd/chomosyncer-cmd`（装配与生命周期）

- **模块装配**：在 `internal/app` 中执行依赖注入与网络拓扑构建。
- **优雅关闭顺序**：
  进程捕获 `SIGINT` / `SIGTERM` 信号后，执行严格的逆序拆解：
  `collector 停止接入` → `dispatcher 排空并关闭` → `livebar 写入器关闭` → `ClickHouse 强制 Flush 落盘` → `backfill 停止` → `关闭 Redis 客户端` → `metrics 停机`。

---

## 模块 8：`internal/backfill` + `internal/windowgate`（历史回补与断线补缺设计）

### 8.1 背景与目标
增量 WebSocket 采集属于 forward-only 流。当发生**空库冷启动**、**历史落档重启**或**分片断线重连**时，数据会出现不同程度的时间缺口。Backfill 模块负责在后台无感完成数据补录，确保 ClickHouse 账本零缺口，并重建 Redis 200 根短期滑窗。

### 8.2 三大补齐策略
1. **空库冷启动 (Empty DB Cold Start)**：
   - 若 ClickHouse 对应表无任何历史记录，读取配置参数 `cold_start_date`（例如 `"2024-01-01"` 或具体时间戳）；
   - 从该锚点时间开始，按 1500 根分段拉取直至当下，完成历史冷启动同步；若未指定则拉取最近窗口所需数据。
2. **有历史冷启动 (Existing History Cold Start)**：
   - 读取 ClickHouse 中该标的的最新时间戳 `max(start_time)`；
   - 严格从 `max(start_time) + 1 step` 作为起始时间，向前持续同步至当下最新时刻。
3. **运行中断线重连 (Shard Reconnect Gapfill)**：
   - 不再受限于人为预设的短窗口上限，直接查询 ClickHouse 记录的最新时间戳；
   - 以该时间戳为同步起点，流式补齐停机期间的所有缺失 K 线，做到绝对无缺口 (Zero Gap)。

### 8.3 窗口门控机制 (`internal/windowgate`)
回补与实时采集是并行运作的。为防止历史回补与实时写入产生竞争：
- **门控拦截 (`gate.Hold(keys)`)**：
  在某个标的回补期间，对其 Redis 收盘滑窗推入 (`PushBarAndTrim`) 和截面就绪通知 (`PublishKlineReady`) 进行临时挂起；
- **全速无阻通道**：
  实时 Live Bar 快照和 ClickHouse 批量写入**不受门控限制**，正常流入（ClickHouse 依赖 `ReplacingMergeTree` 原生去重）；
- **物化重建与释放 (`gate.Release(keys)`)**：
  1. 回补数据全部写入 ClickHouse 并执行显式 Flush 屏障；
  2. 从 ClickHouse 权威读回该标的最新的 200 根 Bar；
  3. 执行 Lua 脚本 `RebuildWindow` 原子替换 Redis 滑窗；
  4. 释放门控，被挂起的实时更新无缝恢复写入。

### 8.4 REST 频控保护 (`BinanceFetcher`)
- 采用 Token Bucket 令牌桶限流算法（参数 `rest_rps`，默认 20 RPS）；
- 分页拉取（币安单次最大 1500 根），自适应重试网络抖动，杜绝触发交易所 429 / 418 IP 封禁。

### 8.5 离线回补模式 (`backfill.offline_only`)

门控受 `gate_timeout`（默认 `5m`）兜底释放，当 `cold_start_date` 设为很久远时间、全市场历史回补远超该时长时，门控会提前释放、实时业务提前开跑，且深历史缺口会因实时写入污染 `max(start_time)` 而变为粘性缺口。

为此提供 `backfill.offline_only`（配置文件；或 `-backfill-offline-only` / `CHOMOSYNCER_BACKFILL_OFFLINE_ONLY`，默认 `false`）：

- `true` 时只装配 **universe → REST 拉取 → ClickHouse 落库** 链路，**不启动** collector / dispatcher / rediswin / windowgate；
- `gate_timeout` 自动失效（`internal/backfill` 内负值哨兵表示"无上限"），历史全量拉取不被中途强制释放；
- 执行一次 whole-universe 冷启动回补后进程退出（正常完成退出码 `0`，被信号中断则非 `0` 但进度已落盘、重跑从 `max(start_time)` 续上）。

配合"深历史离线灌满 → 校验 → 用最近少量窗口的 `cold_start_date` 启动在线业务"的两阶段流程，可确保历史落库完成后再开启 Redis 业务。详见 `docs/OPERATIONS.md` 方式 C。

### 8.6 更粗周期的 ClickHouse rollup（`deploy/clickhouse/002` + `003`）

采集端只订阅 1m，避免"同一撮合事件在 1m/1h 两条 WS 链路各跑一遍"的连接与带宽冗余。更粗周期全部由 ClickHouse 从 `fapi_kline_1m` 派生：

- **目标表** `fapi_kline_{5m,15m,1h,4h,1d}`：`ReplacingMergeTree(rollup_version)`，schema 与 `fapi_kline_1m` 一致，消费方式也一致（`... FINAL`）。
- **刷新式物化视图** `*_rmv`：每 60–300s 从 `fapi_kline_1m FINAL` **重算**最近数天的桶并 `APPEND`。桶用 `toStartOfInterval`（epoch 对齐，5m/15m/1h/4h/1d 与币安边界一致）。
- **幂等硬保证**：聚合是 `sum()/argMin()/argMax()/min()/max()` 的**全量重算**，没有任何累加器；`FINAL` 先折叠 1m 的重复行；每次重算写更新的 `rollup_version`，ReplacingMergeTree 保留最新。因此 1m 重发、回补重叠、`003` 反复重跑都**不会让 volume 翻倍**。严禁改成 `SummingMergeTree` 或非刷新式增量 MV——那会按插入块累加而漂移。
- **历史折叠** `003_rollup_backfill.sql`：MV 只吃创建之后到达的 1m 行，故 `002` 应用后运行一次 `003`（按月分片或一次性）把已有历史折进 rollup 表。
- **OHLC 精确、volume 有 ~1e-8 相对漂移**（浮点求和顺序 vs 币安从 trade 累加）——`check_vs_binance.py` 对价用 `1e-9`、对量用 `1e-6` 容差。
