# 架构设计说明书：Market Data Ingestion Service (Go)

> **文档状态：** Ready for Implementation  
> **适用范围：** Binance U 本位永续合约行情采集管网、Redis 实时截面缓存、ClickHouse 归档存储及 Python 策略计算接口契约。

---

## 1. 系统定位与核心职责

本服务（`md-collector`）是量化系统的基石接入层，专职负责全市场行情数据的低延迟流式接入与多级落库。

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
│                    md-collector (Go Core Daemon)                             │
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

优先使用全市场聚合流（All-Market Aggregate Streams）：

- `!kline_1h@arr`
- `!kline_1m@arr`

单连接即可拉取全量币种，避免为 300+ 币种开启数百条连接触发 Binance 连接数封禁。

若按交易对细粒度订阅：

- 必须以**每批次最大 200 个 Streams**进行拆包分片。
- 各分片连接建立间隔设置 **500ms 避让窗口**。

#### 重连与自愈机制

- Binance 服务器单条 WebSocket 连接存活时限为 **24 小时**。
- 程序必须监听关闭帧与保活超时。
- Ping-Pong Timeout 设定为 **15 秒**。
- 遇到断连必须启用带抖动的指数退避重试（Exponential Backoff with Jitter）：
  - 基准：`1s`
  - 上限：`30s`

#### 动态 Universe 剔除与对齐

后台启动 `UniverseMonitor` Goroutine，每隔 **1 小时**调用一次：

```http
GET /fapi/v1/ticker/24hr
```

**硬过滤条件：**

1. 计价货币为 `USDT`；
2. 交易状态为 `TRADING`；
3. 过去 24 小时成交额 `quoteVolume` 位于全市场前 N（如 Top 150）。

同时剔除：

- 交割合约；
- 非 USDT 本位合约；
- 流动性黑洞标的。

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

直接在内存中丢弃，**严禁触发下游推理**。

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
| `ws_connection_status{stream="..."}` | Gauge | WebSocket 连接存活状态，`1` 为正常，`0` 为断连 |
| `kline_ingested_total{symbol="...", interval="..."}` | Counter | 各币种闭合 Bar 摄入累计量 |
| `clickhouse_buffer_size` | Gauge | 当前内存积压待写行数；超过 `20,000` 触发告警 |
| `clickhouse_flush_latency_seconds` | Histogram | ClickHouse 单批次写入耗时 |
| `redis_pipeline_latency_seconds` | Histogram | Redis 滑窗更新往返延迟 |
