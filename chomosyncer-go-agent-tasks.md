# Agent 模块化实现任务拆解

建议采用 **"模块化分步 Prompting（分治法）"**，把文档拆解为 4
个任务依次丢给 Agent。

------------------------------------------------------------------------

## 任务 1：ClickHouse 批量缓冲写入器（Batch Ingestion）

### 输入给 Agent

将文档的 **第 3.1 节（DDL 与批量写入要求）** 贴给 Agent。

### Prompt 重点

> 请基于 `github.com/ClickHouse/clickhouse-go/v2` 实现一个线程安全的
> `ClickHouseBatchWriter`。
>
> 内部使用 channel 缓冲，当达到 **5000 条数据**或定时器达到 **1000ms**
> 时触发 flush。
>
> 请保证优雅退出时能将内存剩余数据完全
> flush，并处理好连接重连与上下文取消。

### 交付要求

-   `ClickHouseBatchWriter` 实现
-   5000 条触发批量 Flush
-   1000ms 定时 Flush
-   Graceful Shutdown
-   Context Cancellation
-   ClickHouse 连接异常与重连处理
-   对应 **Unit Test**

------------------------------------------------------------------------

## 任务 2：Redis 200 根滑窗写入与 Stream 事件通知

### 输入给 Agent

将文档的：

-   **第 3.2 节：Redis 数据契约与 Pipeline 要求**
-   **第 4.1 节：Stream 事件通知契约**

贴给 Agent。

### Prompt 重点

> 请基于 `github.com/redis/go-redis/v9` 和 `github.com/bytedance/sonic`
> 实现 Redis 滑窗写入逻辑。
>
> 实现：
>
> ``` go
> PushBarAndTrim(ctx, symbol, interval, compactBar)
> ```
>
> 使用 Pipeline 执行：
>
> ``` text
> LPUSH + LTRIM 0 199
> ```
>
> 并实现一个发事件函数，在截面收盘时写入：
>
> ``` text
> stream:market:kline_ready
> ```

### 交付要求

-   Compact Bar 序列化
-   `PushBarAndTrim(...)`
-   Redis Pipeline
-   `LPUSH`
-   `LTRIM 0 199`
-   `kline_ready` Stream Event Publisher
-   错误处理与 Context Cancellation
-   对应 **Unit Test**

------------------------------------------------------------------------

## 任务 3：Binance 行情流监听与断线重连

### 输入给 Agent

将文档的：

-   **第 2 节：数据采集核心约束与技术规范**
-   **第 5 节：核心代码骨架与技术实现要点**

贴给 Agent。

### Prompt 重点

> 请基于 `github.com/adshao/go-binance/v2/futures` 实现 WebSocket
> 行情监听器。
>
> 严格过滤：
>
> ``` text
> is_final == true
> ```
>
> 的 K 线才允许向下游分发。
>
> 实现带抖动的指数退避重连机制（Exponential Backoff with Jitter）。
>
> 同时启动一个 Goroutine，定时每小时更新一次活跃交易对 Universe。

### 交付要求

-   Binance Futures WebSocket Listener
-   Closed Kline Filter
-   `is_final == true` 强约束
-   WebSocket 异常检测
-   Exponential Backoff with Jitter
-   自动重连
-   `UniverseMonitor`
-   每小时刷新 Active Symbols
-   Context Cancellation / Graceful Shutdown
-   对应 **Unit Test**

------------------------------------------------------------------------

## 任务 4：Python 端无状态特征计算骨架（对齐契约）

### 输入给 Agent

将文档的 **第 4.2 节（Python 接口契约）** 贴给 Agent。

### Prompt 重点

> 请编写 Python 策略端的测试读取脚本。
>
> 监听 Redis Stream：
>
> ``` text
> stream:market:kline_ready
> ```
>
> 收到信号后，通过：
>
> ``` python
> redis.pipeline()
> ```
>
> 批量拉取所有币种的：
>
> ``` text
> kline:{symbol}:1h
> ```
>
> 将数据反序列化后构造成 Polars
> DataFrame，并写一个示例函数，无状态计算截面动量与 RSI。

### 交付要求

-   Redis Stream Consumer
-   `kline_ready` 事件解析
-   Pipeline 批量读取 Kline Window
-   Compact Bar 反序列化
-   Polars DataFrame 构建
-   Stateless Feature Calculation
-   截面 Momentum 示例
-   RSI 示例
-   对应 **Unit Test**

------------------------------------------------------------------------

## 推荐实施顺序

``` text
Task 1
ClickHouseBatchWriter
        │
        ▼
Task 2
Redis Sliding Window + Stream
        │
        ▼
Task 3
Binance WebSocket + Universe
        │
        ▼
Task 4
Python Strategy Reader
        │
        ▼
Integration / Code Review
```

按这个节奏给 Agent 派发任务，并要求**每个模块附带对应的 Unit Test**。

最终你只需要负责：

1.  Code Review；
2.  检查各模块接口契约；
3.  将几个独立积木进行组装；
4.  完成端到端 Integration Test；
5.  启动 Binance → Redis / ClickHouse → Python 的完整数据链路。

按照该拆分方式，目标是在 **1～2 天内稳定跑通整套采集服务**。
