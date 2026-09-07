# 架构设计说明书：Market Data Ingestion Service (Go)

> **文档状态：** Ready for Implementation  
> **适用范围：** Binance U 本位永续合约行情采集管网、Redis 实时截面缓存、ClickHouse 归档存储及 Python 策略计算接口契约。

---

## 1. 系统定位与核心职责

本服务（**ChomoSyncer-go**，二进制 `chomosyncer-go`）是量化系统的基石接入层，专职负责全市场行情数据的低延迟流式接入与多级落库。

### 1.1 职责边界

- 仅处理原始事实数据（Raw Market Facts），严禁包含任何业务衍生特征的计算逻辑。
- 负责维护全市场 U 本位合约的活跃交易对列表（Dynamic Universe）。
- 保证 WebSocket 长连接的高可用、断线自愈与乱序防护。
- 将收盘 K 线同步分发至极热缓存层（Redis）与时序存储层（ClickHouse）。

### 1.2 非目标（Non-Goals）

- 不接入 Trade 逐笔成交流（初期规避存储雪崩）。
- 不处理任何账户与订单执行逻辑。

### 1.3 总体架构

```text
               ┌────────────────────────────────────────────────────────┐
               │                  Binance WebSocket API                 │
               │    !kline_<interval>@arr  /  !markPrice@arr@1s         │
               └───────────────────────────┬────────────────────────────┘
                                           │ JSON Stream (Go SDK)
                                           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                  ChomoSyncer-go (Go Core Daemon)                            │
│                                                                              │
│  ┌──────────────────────┐   ┌─────────────────────┐   ┌───────────────────┐  │
│  │ Universe Monitor     │   │ Kline Dispatcher    │   │ Memory Buffer     │  │
│  │ (REST Polling / 1h)  │   │ (Filter Closed Bar) │   │ (Sync.Pool / Batch│  │
│  └──────────────────────┘   └──────────┬──────────┘   └─────────┬─────────┘  │
└────────────────────────────────────────┼────────────────────────┼────────────┘
                                         │                        │
                   ┌─────────────────────┴──────┐                 │ Flush (1s/1w rows)
                   │ LPUSH + LTRIM (200 Bars)   │                 │ Native Block Insert
                   ▼                            ▼                 ▼
          ┌─────────────────┐          ┌─────────────────┐ ┌───────────────────┐
          │  Redis Cluster  │          │  Redis Stream   │ │    ClickHouse     │
          │ (Rolling Cache) │          │ (Signal Trigger)│ │ (Raw Time-Series) │
          └────────┬────────┘          └────────┬────────┘ └───────────────────┘
                   │ Pipeline Read              │ Pub/Sub Event
                   └──────────────────┬─────────┘
                                      ▼
                        ┌───────────────────────────┐
                        │ Python Strategy Engine    │
                        │ (Polars / Stateless Calc) │
                        └───────────────────────────┘
```

---

## 2. 数据采集核心约束与技术规范

### 2.1 订阅协议与流控机制

#### 订阅模式

> **修订（futures 无全市场 kline 聚合流）：** `!kline_<interval>@arr` 是 **Spot 专属**流，
> U 本位合约 WebSocket **不提供**全市场 kline 聚合流（官方连接器 `binance-connector-go`
> 的 futures 全市场流只有 `!ticker@arr` / `!miniTicker@arr` / `!markPrice@arr` / `!forceOrder@arr`）。
> 因此本服务**逐交易对订阅** `<symbol>@kline_<interval>`（如 `btcusdt@kline_1m`）。

分片策略（`internal/collector`）：

- **先按 interval，再按 symbol 稳定哈希分桶**：`shardID = kline_<interval>_<crc32(SYMBOL) % ShardsPerInterval>`。
- 每个 shard = 一条独立的 SINGLE-mode WebSocket 连接，承载 ≤ ~180 个 stream（留足 200 SUBSCRIBE 上限余量）。
- `ShardsPerInterval` 启动时固定（默认 4），保证 symbol→shard 映射稳定：每小时 Universe 变动
  只在受影响 shard 上产生少量 SUBSCRIBE / UNSUBSCRIBE 增量，不触发全量重订。
- 各 shard 建连错峰 **300ms**（≥ 500ms 避让语义由错峰 + 连接数上限共同保证）。
- 订阅范围 = **全部可交易 USDT 永续**（= Universe，见下）。同一份 Universe 既是订阅全集，
  也是 §4.1 `kline_ready` 的截面分母。

#### 重连与自愈机制

- Binance 服务器单条 WebSocket 连接存活时限为 **24 小时** —— 官方连接器内置每 **23h** 主动重建。
- 服务端 ping 由连接器自动回 pong；连接器内置有限次（≤10）指数退避重连，耗尽后抛
  `ErrReconnectAttemptsExhausted` 并保持 CLOSED。
- **collector 的 shard supervisor 在其之上再包一层**：
  - **无限次**带抖动指数退避重连（基准 `1s`、上限 `30s`、半抖动）。
  - **staleness 看门狗**：`StaleTimeout`（默认 60s）内无任何帧即判定半开连接，强制重建
    （防 ping/pong 漏检的假死 TCP）。
  - context 取消 / `Close()` → 停止所有 shard、幂等收尾。

#### 动态 Universe 对齐（全市场）

> **修订：本服务是全市场采集器。** Universe = **全部**可交易 U 本位永续合约，
> **不做**流动性 / 活跃度门限（Top N、`quoteVolume` 阈值等一律取消）。活跃品种筛选是
> 消费者 / 策略层基于采集后数据自行完成的事，不属于采集层职责。

后台 `UniverseMonitor` Goroutine：

```http
GET /fapi/v1/exchangeInfo
```

**硬过滤（仅此三条）：**

1. `quoteAsset == USDT`；
2. `contractType == PERPETUAL`（自动排除交割 / 季度合约）；
3. `status == TRADING`。

**刷新节奏：** 每 **1 天**一次，对齐到交易所时间（UTC）**00:00 之后**（默认 +2min，等当日
上 / 下市结算完成）。启动时先同步刷一次，不等午夜。理由：合约列表只在 Binance 上市 / 退市时
变化，日频足够；`crc32` 固定模数分片保证当日增删只在对应 shard 上产生少量
`SUBSCRIBE` / `UNSUBSCRIBE` 增量。

Universe 同时是：collector 的**订阅全集**，以及 dispatcher `kline_ready` 的**截面分母**
（`Size()` = 全市场合约数，`Has()` = 是否在册可交易）。

### 2.2 K 线闭合判定（严禁偷价未来函数）

Binance 推送的 K 线事件格式中，同一根 Bar 会高频推送其最新价格。

#### 核心字段提取约束

必须严格检查：

```text
k.x  // is_bar_final
```

只有当：

```text
k.x == true
```

该 Bar 才被判定为正式收盘闭合。此时方可写入 Redis 与 ClickHouse。

未闭合的 Bar：

```text
k.x == false
```

不进入收盘滑窗、不写 ClickHouse、**严禁触发下游推理**。

> **本版新增例外：** 未闭合 Bar 不再被直接丢弃，而是复制一份写入 Redis 实时快照层（见 §3.3），仅供独立的实时消费端读取「当前这根 K 线的即时形态」。该通道与 `kline_ready` 截面契约完全隔离，不参与任何回测对齐的特征计算。

#### 时间戳统一基准

全链路时间戳必须使用：

```text
k.t  // K 线开始时间戳，毫秒
```

作为主键对齐基准，严禁使用收到报文的机器本地时间作为 Bar 时间戳。

---

## 3. 存储层设计与落库规范

### 3.1 ClickHouse 时序仓（只读事实库）

ClickHouse 只追加原始事实数据，不进行行级 `UPDATE`。

#### 建表 DDL

```sql
CREATE TABLE IF NOT EXISTS market.fapi_kline_1m
(
    symbol                  LowCardinality(String),
    start_time              DateTime64(3, 'UTC'),
    end_time                DateTime64(3, 'UTC'),
    open                    Float64,
    high                    Float64,
    low                     Float64,
    close                   Float64,
    volume                  Float64,
    quote_volume            Float64,
    taker_buy_volume        Float64,
    taker_buy_quote_volume  Float64,
    trades_count            UInt32,
    created_at              DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(start_time)
PRIMARY KEY (symbol, start_time)
ORDER BY (symbol, start_time)
SETTINGS index_granularity = 8192;
```

#### 写入技术要求

**批量缓冲写入（Batch Ingestion）：**

严禁单条插入。Go 采集端内建无锁缓冲队列（Ring Buffer），满足以下任一条件时触发批量落库：

- 缓冲条数达到 **5,000 条**；
- 距上次写入时间超过 **1,000 ms**。

使用 ClickHouse 原生 TCP 驱动 `github.com/ClickHouse/clickhouse-go/v2` 进行列式批量追加。

### 3.2 Redis 缓存层（在线滑窗模型）

Redis 专为 Python 推理服务提供毫秒级无锁切片读取。

#### 数据结构与键规范

**键命名空间：**

```text
kline:{symbol}:{interval}
```

例如：

```text
kline:BTCUSDT:1h
```

**存储结构：** `List`

#### 定长截断逻辑（原子操作）

每当收到闭合 Bar 时，通过 Go 执行 Pipeline：

```text
LPUSH kline:BTCUSDT:1h <compact_json_or_msgpack>
LTRIM kline:BTCUSDT:1h 0 199
```

严格锁死 List 长度为 **200 根 Bar**，维持内存上限恒定。

#### 序列化协议（Payload）

为保证解析吞吐，存入 Redis List 的单一元素使用精简紧凑 JSON 或 MsgPack：

```json
[
  1719835200000,
  60250.5,
  60800.0,
  60100.2,
  60720.0,
  12450.85,
  75602300.5,
  6120.40,
  37150000.2
]
```

字段位置定义：

| Index | 字段 | 含义 |
|---:|---|---|
| 0 | `start_time` | K 线开始时间（ms） |
| 1 | `open` | 开盘价 |
| 2 | `high` | 最高价 |
| 3 | `low` | 最低价 |
| 4 | `close` | 收盘价 |
| 5 | `volume` | 成交量 |
| 6 | `quote_volume` | Quote 成交量 |
| 7 | `taker_buy_volume` | Taker Buy Base Volume |
| 8 | `taker_buy_quote_volume` | Taker Buy Quote Volume |

> 采用紧凑 Array 代替带 Key 的 Object，降低 60% 以上的 Redis 内存占用与 Python 字符串解析开销。

### 3.3 Redis 实时快照层（Live Bar，未闭合 K 线）

为让实时消费端随时掌握「当前这根 K 线的即时形态」，本服务将**所有** `WsKlineEvent`（含 `k.x == false` 的未闭合帧）复制一份写入独立的实时快照层。该层与 §3.2 的收盘滑窗**完全解耦**。

#### 定位与边界

- **目的：** 提供当前未闭合 Bar 的实时快照（last-write-wins），供看盘、监控、实时信号等独立消费端读取。
- **非目标：** 不留存 intra-bar 的反复演化历史。若需要真正的微观结构分析，正解是接入逐笔成交流 `aggTrades` 落 ClickHouse（见 §1.2），而非在 Redis 里堆叠同一根 Bar 的高频快照。
- **不参与 `kline_ready` 截面契约**，从而杜绝 train-serve skew：回测里不存在的未闭合数据，不会渗入在线特征计算。

#### 键与结构

**键命名空间（独立前缀）：**

```text
livebar:{SYMBOL}:{interval}
```

例如 `livebar:BTCUSDT:1h`。独立前缀便于多消费端 `SCAN` / `MGET` 且不污染收盘 keyspace，未来可整体迁往独立 Redis 实例。

**存储结构：** `Hash`

| 字段 | 含义 |
|---|---|
| `t` | K 线开始时间（ms，`k.t`） |
| `o` `h` `l` `c` | 开 / 高 / 低 / 收 |
| `v` | 成交量 |
| `qv` | Quote 成交量 |
| `tbv` | Taker Buy Base Volume |
| `tbqv` | Taker Buy Quote Volume |
| `n` | 成交笔数（`k.n`） |
| `x` | 是否已闭合（`k.x`，`0` / `1`） |

#### 写入规范

- 每条 event 执行 `HSET livebar:{SYMBOL}:{interval} ...`（整根覆盖）+ `PEXPIRE`，单次 pipeline 1 RTT，**不使用 LPUSH / LTRIM**，不维护定长窗口（任意时刻每个 `(symbol, interval)` 只有一根正在形成的 Bar，window 恒等于 1）。
- `PEXPIRE = TTL_MULTIPLE × interval`，默认 `TTL_MULTIPLE = 2`。掉出 Universe 或行情停滞的键自动过期，无需手工清理。
- 闭合帧（`x == 1`）同样写入本层；消费端拼接「收盘 List + livebar」时按 `t` 去重，至多 1 处重叠。

#### 连接与资源隔离

- 使用**专用 `*redis.Client`**：独立连接池、短读写超时、不重试（fire-and-forget）。与收盘写入的连接池物理隔离，避免实时写入的突发占满连接池、拖慢收盘关键路径的客户端排队延迟。
- `Addr` 可配；初期与收盘共用同一 Redis 实例（`SELECT db` 不隔离 CPU / 内存），后续可一行配置切至独立实例获得真正的资源隔离。

#### 热路径解耦

Binance WS 读帧 goroutine 的唯一职责是：解码 → 过滤 → **非阻塞入队**，随即返回。绝不在读帧循环内做 Redis I/O。

```text
WS goroutine ──(非阻塞, 满即丢)──► chan(live) ──► N × writer goroutine ──► HSET + PEXPIRE
```

- 实时 channel 满即丢弃并计 `redis_livebar_dropped_total`（实时快照丢帧无害，下一帧 ~250ms 后即到）。
- writer goroutine 数量 `WORKERS`（默认 2）可配，用于吸收 Redis I/O 往返延迟。
- 优雅退出：停止入队 → drain channel → 等待 worker 收尾。

#### 可选 Pub/Sub 推送

开启后每次写入同时 `PUBLISH livebar.{interval} <payload>`，供推送型消费端订阅。`payload` 为 11 元素紧凑数组（§3.2 的 9 元素 + `n` + `x`）。Pub/Sub 无持久化，恰适合实时流（迟到订阅者等下一帧即可）。

#### 消费端约定

- 实时消费端为**独立进程 / 组件**，与 Python 收盘策略引擎解耦；策略形态多样，消费端可任意扩展。
- 读取方式：轮询 `livebar:{SYMBOL}:{interval}`（`HGETALL` / `HMGET` / pipeline 批量），或订阅 `livebar.{interval}`。

---

## 4. 与 Python 策略层的接口契约与约束

为了杜绝“实盘与回测不一致（Train-Serve Skew）”和“在线状态污染”，明确划分数据与特征的交互边界。

### 4.1 触发与通信协议（Pub/Sub）

当一个周期的全市场 K 线全部闭合后（如整点 `00 分 01 秒`），Go 采集层向 Redis Stream 发出截面就绪事件。

**Stream Key：**

```text
stream:market:kline_ready
```

**Payload：**

```json
{
  "interval": "1h",
  "timestamp": 1719835200000,
  "symbols_count": 182
}
```

Python 策略引擎订阅该 Stream，监听到事件后，立即发起截面计算。

### 4.2 特征计算模式：内存无状态全量重算

Python 端行为规范：

1. 收到 `kline_ready` 信号后，使用 `redis-py` 的 pipeline 在单次 RTT 内批量读取所有活跃币种的 `kline:{symbol}:{interval}` 列表。
2. 将提取的矩阵直接反序列化构造成统一的 3D 张量或 Polars DataFrame。
3. 特征计算一律在内存中无状态重算。
4. 利用传入的 200 根 Bar 全量计算：
   - EMA
   - RSI
   - NATR
   - CVD 比率
   - 截面 Z-Score

#### 严格约束

**禁止中间态持久化**

Python 策略层计算的均值状态、中间因子指标严禁回写到 Redis 或数据库中供下次使用。

**自愈与重置**

- 如果 Python 进程重启，直接重新拉取 Redis 当前的 200 根 Bar 即可无缝继续运算。
- 若 Redis 数据丢失，Python 直接向 ClickHouse 查询最近 200 根 Bar 填充，系统无需停机维护。

---

## 5. 核心代码骨架与技术实现要点（Go）

### 5.1 依赖选型

| 组件 | 依赖 |
|---|---|
| Binance SDK | `github.com/adshao/go-binance/v2` |
| JSON 解析 | `github.com/bytedance/sonic` |
| Redis Client | `github.com/redis/go-redis/v9` |
| ClickHouse 驱动 | `github.com/ClickHouse/clickhouse-go/v2` |

其中 `sonic` 用于替代标准库 JSON 解析，以消除 GC 反射损耗。

### 5.2 核心调度器伪代码实现

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/v9"
)

type CompactBar [9]interface{}

type IngestionWorker struct {
	rdb         *redis.Client
	clickhouse  *ClickHouseBatchWriter
	klineStream string
}

func (w *IngestionWorker) HandleKlineEvent(event *futures.WsKlineEvent) {
	// 1. 严格过滤：非闭合 Bar 立即丢弃
	if !event.Kline.IsFinal {
		return
	}

	// 2. 紧凑数组序列化 (零冗余传输)
	bar := CompactBar{
		event.Kline.StartTime,
		event.Kline.Open,
		event.Kline.High,
		event.Kline.Low,
		event.Kline.Close,
		event.Kline.Volume,
		event.Kline.QuoteVolume,
		event.Kline.TakerBuyBaseVolume,
		event.Kline.TakerBuyQuoteVolume,
	}

	payload, err := sonic.Marshal(bar)
	if err != nil {
		return
	}

	ctx := context.Background()
	key := fmt.Sprintf("kline:%s:%s", event.Symbol, event.Kline.Interval)

	// 3. Redis 滑窗维护原子操作 (保持 200 根)
	pipe := w.rdb.Pipeline()
	pipe.LPush(ctx, key, payload)
	pipe.LTrim(ctx, key, 0, 199)
	_, _ = pipe.Exec(ctx)

	// 4. 异步推送至 ClickHouse 内存缓冲池，等待批量落盘
	w.clickhouse.Push(event)
}

func (w *IngestionWorker) StartUniverseMonitor(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 定期调用 fapi.ticker/24hr 更新活跃交易对列表
			// 过滤 quoteVolume 前 150 名标的并对齐 WebSocket 连接池
		}
	}
}
```

---

## 6. 运维与监控指标（Observability）

Go 服务必须通过 Prometheus（HTTP `/metrics`）暴露以下关键运行指标：

| Metric | 类型 | 含义 |
|---|---|---|
| `ws_connection_status{shard="..."}` | Gauge | shard 连接存活状态，`1` 为正常，`0` 为断连 |
| `ws_reconnects_total{shard="..."}` | Counter | 各 shard 重连累计次数 |
| `ws_messages_total{shard="..."}` | Counter | 各 shard 收到的 kline 消息累计量 |
| `ws_last_message_age_seconds{shard="..."}` | Gauge | 距该 shard 上次收到帧的秒数（staleness 看门狗输入） |
| `ws_subscription_updates_total{shard="..."}` | Counter | 各 shard 因 Universe 变动应用订阅增量的次数 |
| `ws_shards_active` | Gauge | 当前 collector 管理的 shard 连接数 |
| `kline_ingested_total{symbol="...", interval="..."}` | Counter | 各币种闭合 Bar 转发累计量 |
| `collector_dispatch_errors_total{reason="..."}` | Counter | 下游 `HandleKlineEvent` 报错累计（`busy` / `closed` / `other`） |
| `universe_refresh_total{status="..."}` | Counter | Universe 刷新成功 / 失败累计 |
| `universe_size` | Gauge | 全市场可交易 U 本位永续合约数 |
| `universe_refresh_duration_seconds` | Histogram | 单次 `exchangeInfo` 拉取 + 解析耗时 |
| `dispatcher_events_total{interval,kind}` | Counter | dispatcher 处理的事件（`kind` = `live` / `closed`） |
| `dispatcher_events_dropped_total{sink="..."}` | Counter | 下游 sink 缓冲满导致的丢弃（`live` / `closed` / `archive`） |
| `dispatcher_out_of_order_total{interval="..."}` | Counter | 单调防护丢弃的乱序 / 重发闭合 Bar |
| `dispatcher_section_published_total{interval,reason}` | Counter | `kline_ready` 发布次数（`reason` = `complete` / `timeout`） |
| `dispatcher_section_pending` | Gauge | 当前跟踪中的截面数 |
| `clickhouse_buffer_size` | Gauge | 当前内存积压待写行数；超过 `20,000` 触发告警 |
| `clickhouse_flush_latency_seconds` | Histogram | ClickHouse 单批次写入耗时 |
| `redis_pipeline_latency_seconds` | Histogram | Redis 收盘滑窗更新往返延迟 |
| `redis_livebar_updates_total` | Counter | 实时未闭合 Bar 写入累计量 |
| `redis_livebar_dropped_total` | Counter | 实时 Bar 因 channel 溢出被丢弃的累计量；持续增长表示 `WORKERS` / channel 容量不足或 Redis 拥塞 |
| `redis_livebar_latency_seconds` | Histogram | 实时快照单次写入（`HSET` + `PEXPIRE`）耗时 |
| `redis_livebar_queue_length` | Gauge | 实时写入 channel 当前积压条数 |
