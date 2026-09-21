# Binance U 本位合约持仓量（OI）落盘设计（第一版）

> 状态：**已实现。** 新表 SQL（`deploy/clickhouse/004~006`）与 Go 模块 `internal/openinterest`（live 快照 + hist 冷启动补缺/每小时校准，接入共享权重闸门）均已完成，并在真实币安与本机 ClickHouse 上做过端到端验证。归档导入器与完整性检查脚本尚未做。
> 标注约定：**[实测]** 是我对线上接口/归档做的实际请求；**[官方文档]** 来自币安官方文档（通过网页抓取工具读到的摘要，不是逐字原文）；**[假设]** 是没有证实、为安全起见按保守值设计的。

---

## 0. 结论

1. **不做 WSS。** USDⓈ-M 合约没有 OI、多空比的推送流：官方 connector（`derivativestradingusdsfutures@v1.20.0`）的 `websocketstreams` 与 `websocketapi` 里没有任何相关的流；实测订阅 `<symbol>@openInterest`（`/market`、`/public`、旧路径，单流与混订）均收不到数据，而同一连接上的 `kline_1m` 对照组正常。所以只能走 REST。
2. **REST 里要区分两类端点，各有各的用途：**
   - `GET /fapi/v1/openInterest`：**实时快照**，用来做 live 采集。
   - `GET /futures/data/openInterestHist`：币安统计任务产出的**历史序列**，用来对账和补缺口。
   - 另有**官方归档**（`data.binance.vision`），用于冷启动历史与每日对账。
3. **第一版只做 `sum_open_interest` 一列。** `sum_open_interest_value` 不入库：它等于持仓量乘以标记价格，需要额外一次 `premiumIndex` 调用，而用 `sum_open_interest × K 线收盘价` 就能近似（偏差见 §2）。其余列（三个多空比、taker 比）暂缓。
4. **K 线表和 OI 表是两张独立的表**，消费者按 `start_time` 拼接成 `t, o, h, l, c, v, oi`。
5. **8 列没有任何综合接口**（见 §2），所以不存在“一行宽表”这种数据形态，之前设计里的 `market.fapi_metrics_5m` 宽表整体作废。
6. **对齐规则一句话：** OI 行的 `start_time = t` 表示“K 线 `t` 收盘（`t+5m`）时刻的持仓量”，与该 K 线的收盘价平行。live 在 K 线收盘前约 20 秒快照；hist 与归档返回的 `timestamp = T` 要写到 `T-5m` 这一行。理由与验证见 §3。

---

## 1. 接口与限速

`/fapi/*` 与 `/futures/data/*` 是**两个独立的额度池**。

| 池 | 本设计用到的端点 | 额度 | 备注 |
|---|---|---|---|
| `/fapi/*` 权重池 | `/fapi/v1/openInterest`（权重 1） | **2400 权重 / 分钟 / IP**（`exchangeInfo.rateLimits`）**[实测]** | 响应带 `x-mbx-used-weight-1m`（手写客户端直接读这个头）。**现有 K 线 REST 回补也在这个池里，见本节末尾的说明。** |
| `/futures/data/*` | `/futures/data/openInterestHist` | **1000 次 / 5 分钟 / IP** **[官方文档]** | **不占**上面的 2400：先调 `/fapi/v1/openInterest`（已用权重 2），再连调 6 次 hist，再调一次，已用权重只变成 3 **[实测]**。**响应没有用量头**（且走了 CDN），必须自己计数。多个 `/futures/data` 端点是否共用 1000，文档没写，**按共用设计 [假设]** |

其他事实：

- hist 只保留最近约 30 天（`startTime` 更早返回 `-1130`）**[官方文档 + 实测]**；`limit` 默认 30，文档最大 500，实测 1000 也可用（未文档化，不要依赖）。
- **`limit` 取的是区间里最新的那几根 [实测]：** 区间 `[startTime, endTime]` 里的数据多于 `limit` 时，接口返回的是**最新**的 `limit` 根（`startTime` 只是下界），所以**只传 `startTime` 无法向前翻页**。回补长缺口必须从新往旧翻：先取 `endTime=现在` 的一页，再把 `endTime` 设成该页最早标签减 1 毫秒，直到覆盖 `startTime`。为避免边界行为不一致，代码里**总是同时传 `startTime` 和 `endTime`**。
- hist 没有批量：`symbol`、`period` 必填，一次一个标的。无效 `symbol` 返回 HTTP 200 和空数组；缺 `period` 返回 400（`-1130`）**[实测]**。`/fapi/v1/openInterest` 无效 `symbol` 返回 400（`-1121`）**[实测]**。
- 超限返回 429；持续违规会被封 IP（418），封禁 2 分钟起、最长 3 天 **[官方文档]**。封的是 IP，不是端点。
- 两个 OI 端点是公共接口，**不需要 API Key**。

**本版预算（标的数 528）：**

| 用途 | 池 | 每 5 分钟消耗 | 占比 |
|---|---|---|---|
| live 快照：528 次 `/fapi/v1/openInterest` | `/fapi` | 528 权重 | 约 4.4%（2400 × 5 = 12000） |
| hist 对账：每小时 528 次（`limit` 取 24） | `/futures/data` | 均摊约 44 次 | 约 4.4%（占 1000） |
| 停机/冷启动回补：每页 500 根（约 41.7 小时） | `/futures/data` | 每个标的 1 页 | 528 次 / 页，限速发出 |

**限流规则（实现时必须满足）：**

1. 每个池一个限流器，池内所有调用（live、对账、回补）都必须经过它。
2. live 一轮的 528 个请求要在约 20 秒内发完，限流器上限取 **25 请求/秒**（远低于 2400/分钟 = 40/秒，给 K 线回补留余量）。
3. `/futures/data` 另加**滑动 5 分钟窗口计数器**，达到 900 就停止发出新请求；对账匀速发出，不要突发。
4. 收到 429：立即暂停该池全部请求至少 300 秒并告警；收到 418：停止该池请求直到封禁结束并触发高优先级告警。
5. 重试也计入额度：每个请求最多重试 2 次，指数退避。

**`/fapi` 池由所有 `/fapi` 调用方共用，已统一记账（已实现）：**

改动之前的问题（代码与实测）：

- 令牌桶只有一个（`internal/backfill/rest.go`，参数 `backfill.rest_rps` 默认 20），只限制 `/fapi/v1/klines`，按“请求数”而不是“权重”限流；universe 的 `exchangeInfo` 和 `cmd/test-tools` 里的 Python 脚本不经过任何限流器，但和它们在同一个出口 IP 上。
- **K 线请求的权重由 `limit` 参数决定，与实际返回条数无关 [实测]：** `limit` 100 → 1、500 → 2、1000 → 5、1500 → 10。回补默认 `limit=1500`（权重 10），默认 20 次/秒 = 12000 权重/分钟，是 2400 上限的 5 倍；整个 universe 的冷启动或大面积断线（528 个标的 × 10 = 5280）会超限。旧进程运行约 9 天未见 429，只是因为回补量一直很小。

已完成的两项改动：

1. **`limit` 按缺口长度取值**（`backfill/rest.go` 的 `pageLimitFor`）：`limit = min(页上限, 缺口/周期 + 2)`。对真实币安的验证：3 分钟缺口 `limit=5`、权重 1（原来 10）；1 小时缺口 `limit=62`、权重 1；8 小时缺口 `limit=482`、权重 2。
2. **共享权重闸门 `internal/weightgate`**，所有 `/fapi` 请求都经过它（目前是 K 线回补与 universe 的 `exchangeInfo`，OI live 之后接入）：
   - 令牌按**权重**计，调用方声明权重（K 线按 `limit` 查表，`exchangeInfo` 与 `openInterest` 为 1）。
   - **每类一个每分钟预算：** `live` 600（OI 快照，一轮 528 可以一次发完）、`bulk` 1200（回补）、`misc` 100（universe），合计 1900，其余 500 留给闸门看不到的同 IP 流量。配置段是 `weight_gate`。
   - **响应头反馈：** 每个 `/fapi` 响应把 `X-MBX-USED-WEIGHT-1M` 报给闸门并导出为 Prometheus 指标 `weightgate_used_weight_1m`；最近读数 ≥ `soft_limit`（1800）时 `bulk` 等到这一分钟结束，≥ `hard_limit`（2300）时 `live`/`misc` 也等。
   - **429/418：** 调用 `PauseFor(Retry-After)`，暂停**所有** `/fapi` 调用方。
   - 其他指标：`weightgate_weight_acquired_total{class}`、`weightgate_wait_seconds{class}`、`weightgate_backpressure_waits_total{class}`、`weightgate_pauses_total`、`weightgate_paused_until_timestamp_seconds`。
   - 原来的 `backfill_rest_weight_used` 指标由 `weightgate_used_weight_1m` 取代（后者覆盖所有 `/fapi` 调用方，不只是回补）。

`/futures/data` 是另一个池，**不放进这个闸门**，由 OI 自己的限流器管理。

---

## 2. 归档 8 列矩阵与本版范围

归档 CSV 的 8 列：`create_time`、`symbol` 是键，其余 6 列是指标。**6 个指标来自 5 个不同的端点加本地 K 线，不存在一个能一次返回全部指标的接口**（`/fapi/v1` 下没有多空比或 taker 比的同名接口，连接器路径清单与线上探测均确认 **[实测]**）。

**时间戳列的含义（关键）：** REST `timestamp` 与归档 `create_time` 是**同一个数轴**（BTCUSDT 一天 288 个点 288/288 命中 **[实测]**）。但“标签”对不同列含义不同：

- **快照类（OI、三个多空比）：标签 = 快照时刻**，即前一个 5 分钟窗口的**结束**（连接器源码注释 “End time of the period”）。
- **流量类（taker 比）：标签 = 窗口 `[t, t+5m)` 的开始**（连接器注释 “Start time of the period”；与本地 K 线推导值对比，按“开盘”对齐平均相对误差 0.23%，平移一格则约 70% **[实测]**）。

| 归档列 | 含义 | `/fapi/v1`（实时） | `/futures/data`（hist） | 标签语义与对齐到 K 线 `start_time` | 本版 |
|---|---|---|---|---|---|
| `sum_open_interest` | 全网未平仓合约总张数（标的资产计） | `GET /fapi/v1/openInterest` → `openInterest`、`time`（**快照时刻**） | `GET /futures/data/openInterestHist` → `sumOpenInterest`、`timestamp` | 快照类：`start_time = 标签 − 5m`（§3） | **做** |
| `sum_open_interest_value` | 上述持仓的名义价值（USDT） | 无 | `sumOpenInterestValue`（= `sumOpenInterest × 标记价格`，见 §3.4） | 快照类；**不入库**，需要时用 `sum_open_interest × fapi_kline_5m.close` 近似（§4.4） | **不做** |
| `count_toptrader_long_short_ratio` | 大户账户数多空比 | 无 | `topLongShortAccountRatio` → `longShortRatio` | 快照类 **[推断]** | 待定 |
| `sum_toptrader_long_short_ratio` | 大户持仓量多空比 | 无 | `topLongShortPositionRatio` → `longShortRatio` | 快照类 **[推断]** | 待定 |
| `count_long_short_ratio` | 全网账户数多空比 | 无 | `globalLongShortAccountRatio` → `longShortRatio` | 快照类 **[推断]** | 待定 |
| `sum_taker_long_short_vol_ratio` | 主动买卖量比 | 无 | `takerlongshortRatio` → `buySellRatio` | 流量类：`start_time = 标签`，**不平移**。也可由本地 1m K 线推导：`taker_buy_volume / (volume - taker_buy_volume)` | 待定 |

**关于名义价值不入库：** hist 的 `sumOpenInterestValue` 精确等于 `sumOpenInterest × 标记价格`（§3.4）；要在 live 行里补出它，每轮需要多调一次批量 `premiumIndex`。而 K 线收盘价与标记价格相差很小，`sum_open_interest × fapi_kline_5m.close` 即可近似：把 hist 的 `sumOpenInterestValue` 与 `sumOpenInterest × 同一行 K 线的 close` 逐点对比（`start_time = T-5m` 的 5 分钟 K 线，其 `close` 是 `T` 时刻的价格），平均相对偏差 **BTCUSDT 0.35 bp、ETHUSDT 0.51 bp、SOLUSDT 0.90 bp**（每个标的 500 个点）**[实测]**，即万分之零点几到一。 因此本版不存这一列，也不调用 `premiumIndex`。

关于“宽表”作废，需要指正你的推理：**“没有综合接口”是原因之一，但不是唯一原因。** 宽表还有另外两个问题，即使有综合接口也成立：

1. **不同列的时间语义不同**（快照类要平移 −5m，流量类不平移）。放在一行里，一个时间列无法同时表达两种语义。
2. **可得延迟不同、来源不同。** 宽表要求“所有列齐了才写一行”，会让最快的列被最慢的列拖住；而 OI 我们要在收盘前就写。

归档 CSV 本身是宽的，但那只是文件格式：**导入时按列族拆开，写进各自的表**即可，库里不需要宽表。将来加多空比时，可以为它们（同为快照类、同样只有 hist）另起一张表，不必和 OI 合表。

---

## 3. 时间轴与对齐

### 3.1 约定

- `t` = K 线开盘时间，即 `fapi_kline_5m.start_time`（K 线 `[t, t+5m)`）。
- **OI 表的一行 `start_time = t`，表示“K 线 `t` 收盘那一刻（`t+5m`）的持仓量”**，与该 K 线的 `close` 平行。
- 于是这一行与同 `t` 的 K 线**在同一时刻（`t+5m`）全部可知**，消费者拼接后得到 `t, o, h, l, c, v, oi`，没有前视。

### 3.2 live：在 K 线收盘前快照

```
K 线 t                                                     K 线 t+5m（下一根）
|<----------------------- [t, t+5m) ----------------------->|
                                  ^30s  ^~9s  ^0
                                  开始   结束   收盘
                                  live 轮询    → 用这一行指导 t+5m 开盘后的交易
```

1. 每个 5 分钟边界 `B = t+5m`：在 `B-30s` 启动一轮，限流器 25 请求/秒，528 个标的约 21 秒发完，预计在 `B-9s` 结束。
2. **行的归属由响应里的 `time` 决定，不是由调用时刻决定：**
   ```
   b = round_to_nearest_5m(time)        // 例：time=14:09:41 → 14:10:00
   要求 |time − b| ≤ 60s，否则丢弃这条快照（并计数告警）
   start_time = b − 5m                  // → 14:05:00，即 K 线 14:05 这一行
   ```
   `time = 14:10:04`（略迟）同样归属 `14:05` 这一行；`time = 14:07:00` 距任何边界都超过 60 秒，丢弃。
3. `snap_time` 记录 `time`，用于审计快照实际发生的时刻。**实测响应里的 `time` 比发出请求的时刻早约 2~3 秒**（真实币安上，请求发出于 20:14:30，返回的 `time` 是 20:14:26.978），所以一轮 `B-30s` 起的快照，其 `time` 大约落在 `B-33s ~ B-12s`；这远在 `live_accept_window`（60 秒）以内，但意味着 live 值比“收盘前 20 秒”再早几秒。

### 3.3 hist 与归档：标签 `T` 写到 `T-5m` 这一行

hist 返回的每一项 `timestamp = T`，是 **`T` 时刻的快照**（依据见 §3.4）；`T` 按整 5 分钟切分，正好是 K 线 `T-5m` 的收盘时刻。所以：

```
start_time = T − 5m        // 与 live 的 “b − 5m” 是同一个映射，因为 T 就是那个边界 b
```

即“hist 返回项 `T` 补到数据库里 `T-5m` 这条记录”。你的分析成立：live 在 K 线 `t` 快收盘时快照，写到 `t`；hist 在同一个边界 `T = t+5m` 的快照，也必须写到 `t = T-5m`，两个来源才在同一条时间轴上。

**归档的日边界要特别处理：** 归档文件 `D` 的 `create_time` 从 `D 00:00:00` 到 `D 23:55:00`，平移后对应的 `start_time` 是 `D-1 23:55` 到 `D 23:50`。所以 **`D` 这天最后一根 K 线（23:55）的 OI 来自 `D+1` 那个文件的第一行（`00:00:00`）**。导入必须把相邻两天的文件连续处理，或至少在导入完成后检查每天是否有 288 行。归档 CSV 的行顺序不保证按时间排序，导入时不依赖文件顺序。

### 3.4 为什么 hist 的标签 `T` 是“`T` 时刻的快照” [实测]

- **价格当时钟：** `sumOpenInterestValue / sumOpenInterest` 得到隐含价格。它与 `markPriceKlines`（1m）中“在 `T` 收盘的那根”的收盘价，平均偏差 **BTCUSDT 0.00 bp、ETHUSDT 0.02 bp、SOLUSDT 0.00 bp**（每个标的最近 60 个标签）。与最新成交价 1m 收盘价对比则是 0.44、0.64、1.15 bp。所以 **`sumOpenInterestValue` 就是 `sumOpenInterest × 标记价格(T)`**，快照时刻就是 `T`。
- **可读时间早于 `T+5m`：** 标签 `T` 的 OI 在 `T+56 ~ T+178 秒` 就可读（2 个边界，1 个标的，量级参考）。所以它不是“等 5 分钟窗口收盘才统计出来”的值，只是**发布有 1~3 分钟的延迟**。
- **名义价值的近似：** 既然 hist 的价值 = OI × 标记价格，而标记价格与 K 线收盘价相差很小，就可以用 `sum_open_interest × K 线收盘价` 近似（偏差见 §2），所以不需要额外调用 `premiumIndex`。

### 3.5 多来源的优先级与差异

`src_rank`：**1 = live，2 = hist（REST），3 = 归档**。同一 `(symbol, start_time)` 上 `src_rank` 大者覆盖小者；同级则后写入者胜。

live 与 hist 的数值**不会完全一致**：

- live 快照在 `B-30 ~ B-9 秒`，hist 是恰好 `B`，存在最多约 30 秒的偏移。
- 我测到 REST hist 与归档的差异：OI 中位约 0.02%、最大 0.4%。live 与 hist 之间也应是这个量级（未单独测）。
- 结果：**实盘看到的是 live 值，回测读到的是被 hist/归档覆盖后的值**，二者有微小差异。这是有意接受的，因为 live 快照不可重放，覆盖后的序列才是可复现的“事实”。

### 3.6 边界情况

- **采集器停机：** live 快照不可回补（`/fapi/v1/openInterest` 没有历史）。启动时必须先用 hist 从每个标的的 `max(start_time)` 补到现在（每页 500 根，约 41.7 小时），再启动 live。
- **hist 只保留 30 天：** 更早的缺口只能等归档。
- **新上市标的：** 首个 live 快照即为其第一行；hist 对账时会补出上市以来的历史。
- **下架标的：** universe 中移除后停止采集，历史保留。

---

## 4. ClickHouse 表设计

**只存 `sum_open_interest`，不存名义价值**（见 §2）。一共 **1 张原始表 + 5 张汇总表 + 5 个可刷新物化视图**。所有表建在 `market` 库，与 K 线表同库。

### 4.1 原始表：`market.fapi_oi_5m`

一行 = 一个标的在一根 5 分钟 K 线收盘时刻的持仓量。

| 列 | 类型 | 说明 |
|---|---|---|
| `symbol` | `LowCardinality(String)` | |
| `start_time` | `DateTime64(3, 'UTC')` | 对应 K 线的开盘时间，**与 `fapi_kline_5m.start_time` 同类型同含义**，用来拼接 |
| `sum_open_interest` | `Float64` | 持仓量（张数） |
| `snap_time` | `DateTime64(3, 'UTC')` | 快照实际发生的时刻：live = 响应里的 `time`；hist/归档 = 标签 `T` |
| `src_rank` | `UInt8` | 1 = live，2 = hist，3 = 归档 |
| `created_at` | `DateTime DEFAULT now()` | |

引擎 `ReplacingMergeTree(src_rank)`，`PARTITION BY toYYYYMM(start_time)`，`ORDER BY (symbol, start_time)`。读取一律加 `FINAL`。

### 4.2 汇总表：`market.fapi_oi_{15m,1h,4h,1d}`（4 张）

由 5m 原始表在 ClickHouse 内汇总，思路与 `002_kline_rollups.sql` 一致：**每次从源表 `FINAL` 全量重算、不累加、每档直接读 5m 表不做级联**，目标表 `ReplacingMergeTree(rollup_version)`。

| 列 | 含义 |
|---|---|
| `symbol`、`start_time` | 桶起点（UTC 对齐） |
| `samples` | 桶内 5m 行数。完整桶为 3 / 12 / 48 / 288（15m/1h/4h/1d），使用前应据此过滤 |
| `sum_open_interest_{close,high,low}` | `close` = 桶内最后一行（桶收盘时刻的持仓量）；`high/low` = 桶内各收盘快照的最大/最小 |
| `rollup_version` | `now64(3)` |

不提供 `open`：一个桶“开盘时刻”的 OI 是上一个桶的 `close`，消费者用 `lag` 即可，放在桶内会造成语义混乱。

**回看下界必须对齐桶边界并锚定 UTC**（写法见 `deploy/clickhouse/002_kline_rollups.sql`，原因见 `deploy/clickhouse-fixes/README.md`）：

```sql
WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 3 DAY, INTERVAL 1 HOUR)
```

### 4.3 物化视图：`market.fapi_oi_{15m,1h,4h,1d}_rmv`（4 个）

`REFRESH EVERY N SECOND APPEND TO` 的可刷新物化视图，每档一个，负责把最近若干天的 5m 行重新汇总进对应汇总表。MV 的替换规则（先 `DROP` 再 `CREATE`）见 `deploy/clickhouse-fixes/README.md`。

### 4.4 与 K 线拼接

```sql
SELECT k.symbol, k.start_time, k.open, k.close, k.low, k.high, k.volume,
       o.sum_open_interest,
       o.sum_open_interest * k.close AS sum_open_interest_value_approx   -- 需要名义价值时的近似
FROM market.fapi_kline_5m AS k FINAL
LEFT JOIN market.fapi_oi_5m AS o FINAL
       ON o.symbol = k.symbol AND o.start_time = k.start_time
```

（语法已在 ClickHouse 26.8 上检查通过。）两张表在同一时刻 `t+5m` 全部可知；`k.close` 是 `t+5m` 时刻的价格，正好与 OI 快照同一时刻，所以近似的时点是对齐的。

### 4.5 SQL 文件

均在 `deploy/clickhouse/`，**已实现，并在 ClickHouse 26.8 的临时库里验证过**（见文末的验证说明）：

| 文件 | 作用 |
|---|---|
| `004_fapi_oi.sql` | 原始表 `fapi_oi_5m`。**不含任何 `DROP`：live 快照不可重放，这张表不能删。** |
| `005_oi_rollups.sql` | 5 张汇总表加 5 个物化视图。**“推倒重建”语义**（与 002 相同）：开头的 `REBUILD-DROPS` 块会删除并空表重建这 10 个对象，再由 006 重新灌满；`fapi_oi_5m` 不在删除范围内 |
| `006_oi_rollup_backfill.sql` | 历史折叠：按月把 5m 表重新汇总进各汇总表，可反复执行（幂等） |

应用顺序：`004 → 导入历史到 fapi_oi_5m → 005 → 006`（先灌历史、后建汇总层，与 K 线的两阶段冷启动一致）。compose 只自动执行 001，004 起需手工执行。原先的宽表版本 `004_fapi_metrics.sql`、`005_metrics_rollups.sql`、`006_metrics_rollup_backfill.sql` 已删除（它们从未在生产库执行过）。

---

## 5. 代码设计框架

### 5.1 位置与复用

- 新模块 `internal/openinterest`，独立 goroutine，在 `internal/app` 里与 `universe`、`backfill` 并列装配。**不进 dispatcher，不参与 windowgate，不涉及 Redis。**
- 复用：`internal/universe`（标的列表）、`internal/config`（新增 `open_interest:` 配置段）、`internal/metrics` 风格的 Prometheus 指标（`promauto` + `Registerer`）。
- **REST 客户端手写**，照 `internal/backfill/rest.go` 的写法（`net/http`、`sonic`、`x/time/rate`、429/418 的 `Retry-After` 退避、`X-MBX-USED-WEIGHT-1M` gauge）。仓库里 connector 只用在 WebSocket 上，REST 一律手写；OI 只有两个端点，手写比引入 connector 的 REST 客户端更一致，也更容易用 `httptest` 测试。connector 源码只作为请求/响应格式的规格参考（§2.1）。`/fapi` 池的调用（live）经过 §1 的共享权重闸门（`internal/weightgate`，已实现，用 `weightgate.ClassLive`），`/futures/data` 池（hist）用自己的限流器。
- **落盘不复用 `internal/chwriter`：** 它的 `BatchWriter` 和 `Flusher` 是写死的 K 线 `Row` 类型（且 `insertColumns` 写死）。第一版建议在 `internal/openinterest` 里放一个**小的独立批量写入器**，照抄它的“攒批、超时刷盘、重试退避、优雅停机”逻辑，避免改动线上跑着的 K 线写入路径。等稳定后再考虑把 `BatchWriter` 泛型化。

### 5.2 文件划分

| 文件 | 职责 |
|---|---|
| `config.go` | 配置结构与默认值（见 §5.3） |
| `client.go` | 手写 REST：`GetOpenInterest(ctx, symbol)`、`GetOpenInterestHist(ctx, symbol, limit, start, end)`；解析（数值是字符串）、429/418 识别与 `Retry-After`、`/fapi` 响应头的 `X-MBX-USED-WEIGHT-1M` 回报给闸门 |
| `limiter.go` | `/futures/data` 池的限流器：令牌桶加 5 分钟滑动窗口计数器加全局 `Pause(until)`。`/fapi` 池使用 §1 的共享权重闸门（`internal/weightgate`，已实现，`ClassLive`），本包只负责在 live 里按 25 rps 匀速发出 |
| `align.go` | 纯函数：`liveBarStart(time) (start, ok)` 与 `histBarStart(ts) start`（§3.2、§3.3）；**最需要单元测试** |
| `live.go` | 边界调度：每个 5 分钟边界在 `B-30s` 启动一轮；对 universe 里每个标的取快照，产出 `Row`（`src_rank=1`） |
| `store.go` | 启动时读一次 ClickHouse：每个标的最新的 `src_rank ≥ 2` 的 `start_time`（`maxIf … GROUP BY symbol`），类似 `backfill.CHStore.MaxStartTime` |
| `hist.go` | 启动补缺与每小时校准，**同一段代码**（见 §5.8）；产出 `Row`（`src_rank=2`） |
| `syncer.go` | 门面：`New(deps)` / `Start(ctx)` / `Close()`，供 `internal/app` 装配 |
| `writer.go` | 独立批量写入器，写 `market.fapi_oi_5m` |
| `row.go` | `Row` 结构与校验 |
| `metrics.go` | 指标（见 §5.5） |

`Row`：

```go
type Row struct {
    Symbol          string
    StartTime       time.Time // K 线开盘时间，UTC
    SumOpenInterest float64
    SnapTime        time.Time
    SrcRank         uint8     // 1=live 2=hist 3=archive
}
```

### 5.3 配置（`open_interest:`，已写入 `config.example.yaml`，两项开关默认关）

```yaml
open_interest:
  hist_enabled: false          # 启动时读库中每个标的已校准到哪根，据此补缺；之后每小时校准。建议先只开这个
  live_enabled: false          # 每根 5m K 线收盘前 live_lead 打一轮快照。观察校准指标后再开
  table: "market.fapi_oi_5m"
  live_lead: 30s               # 在 K 线收盘前多久开始一轮
  live_accept_window: 60s      # 快照时间距最近 5m 边界超过此值则丢弃（必须 < 2m30s）
  live_workers: 8
  fapi_rps: 25                 # 一轮的请求节奏；/fapi 池的硬限制由 weight_gate 负责
  hist_reconcile_interval: 1h
  hist_reconcile_offset: 5m    # 每小时 hh:05 开始
  hist_reconcile_spread: 20m   # 一轮的请求在这段时间内匀速铺开（启动补缺不铺开）
  hist_publish_lag: 4m         # 标签 T 预计在 T 之后多久可读，用来判断“已是最新则跳过”
  hist_limit_margin: 3         # 每次请求比缺口多要几根
  hist_max_limit: 500          # 单次 limit 上限；更长的缺口从新往旧分页
  hist_cold_start_window: 48h  # 库里没有任何已校准行的标的，补回多远
  hist_max_backfill: 168h      # 补缺最远回溯多久（币安只保留约 30 天）；更早的缺口需要归档
  hist_workers: 4
  data_rps: 2                  # /futures/data 池的请求速率
  data_window_cap: 900         # 该池 5 分钟滑动窗口的硬上限
```

`config.Validate` 会拒绝：`live_lead ≥ 5m`、`live_accept_window ≥ 2m30s`、`hist_reconcile_offset ≥ interval`、`hist_max_limit > 500`、`hist_max_backfill > 30 天`、`hist_cold_start_window > hist_max_backfill` 等。写入使用与 K 线相同的 ClickHouse 连接与批量参数。

### 5.4 启动与停机顺序

1. `internal/app` 在 universe 首次刷新成功之后启动（`Run()` 里 `univ.Start()` 之后）；未启用任何一项时不构造。
2. **hist 与 live 同时启动。** 之前的设计要求先补缺、后启动 live，理由是“避免额度冲突”；但两者用的是**不同的池**（live 走 `/fapi` 权重闸门，hist 走 `/futures/data` 池），互不占用额度，所以补缺期间 live 照常快照，不会因为补缺跑得久而漏掉快照。同一个 `(symbol, start_time)` 上 hist（`src_rank=2`）总是覆盖 live（`1`），与谁先写入无关。
3. **hist 先做冷启动决策（见 §5.8）**：读库、逐个标的判断是否需要补，再跑一轮不铺开的补缺（启动补缺只受 `/futures/data` 池限制），之后每小时 `hh:05` 校准一次。
4. 停机：取消两个循环并等待，再依次关闭写入器（排空并落盘）与存储连接。

### 5.5 指标（Prometheus）

`oi_live_snapshots_total{result="ok|dropped|error"}`、`oi_live_cycle_seconds`、`oi_hist_requests_total{result}`、`oi_data_window_used`（`/futures/data` 5 分钟窗口已用次数）、`oi_rate_limited_total{code="429|418"}`、`oi_rows_written_total{src_rank}`、`oi_last_start_time_lag_seconds{symbol}`、`oi_live_vs_hist_rel_diff`（校准时 live 与 hist 的相对偏差直方图）、`oi_live_gap_bars_total`（校准时发现 live 缺失的根数）、`oi_cross_section_complete_ratio`（每轮结束后，拿到最近一根已收盘 bar 的标的数 ÷ universe 大小）。

### 5.6 冷启动（归档导入器）

- 一个独立命令（第一版可以放在 `deploy/clickhouse/` 下，风格与 `import_fapi_kline.py` 一致；放 `cmd/` 下的 Go 子命令也可以）。
- 对每个标的每天下载 `SYMBOL-metrics-YYYY-MM-DD.zip`，校验 `.CHECKSUM`，只取 `sum_open_interest` 一列；`start_time = create_time − 5 分钟`，`snap_time = create_time`，`src_rank = 3`。
- **`create_time` 必须按 UTC 解析**：`input()` 里写 `DateTime('UTC')`（写 `DateTime` 会按服务器时区解析，服务器是 `Asia/Shanghai` 时整体偏 8 小时 **[实测]**）。
- 相邻两天的文件要连续处理（§3.3 的日边界）；导入完成后检查每个 `(symbol, 日)` 是否有 288 行（新上市、下架的标的除外）。
- 记录已完成的 `(symbol, date)`，支持断点续传。

### 5.7 测试要点

- `align.go`：`liveBarStart` 覆盖“略早、略迟、恰好在边界、超过 60 秒被丢弃”；`histBarStart` 覆盖日边界。
- 归档对账：把某个标的一天的归档与同时刻 hist 的结果逐行比较，验证平移后 `start_time` 一致。
- 限流器：突发不超过上限、429 后暂停、窗口计数正确。
- `hist.go`：每个标的的动态 `limit`（含 30 小时缺口、超过 500 分页、新上市标的）；一轮的请求按 `spread` 匀速铺开；缺口优先的排序。
- 写入器：沿用 `chwriter` 的测试用例结构（fake flusher）。

### 5.8 冷启动决策与每小时校准（hist.go，已实现）

**冷启动决策：读库里每个标的的最大开盘时间，再决定要不要调 hist。** 启动时 `store.LastStarts` 用一条查询取每个标的的两个值：

```sql
SELECT symbol, max(start_time), maxIf(start_time, src_rank >= 2) FROM market.fapi_oi_5m WHERE symbol IN (?) GROUP BY symbol
```

- `Any`：所有来源里最新的一根（含 live）。
- `Hist`：`src_rank ≥ 2` 里最新的一根，即**校准到了哪里**。之后每个标的的决策以它为准：Hist 之后的 live 行还没被校准，所以也要重新取。只有 live 行、没有任何校准行的标的按“无校准行”处理。
- 库不可用时 `LoadState` 会一直重试（5 秒起翻倍，最长 1 分钟），因为没有它无法决定跳过什么。

设 `target` 为“预计已发布的最新一根”的开盘时间（`floor5(now − hist_publish_lag) − 5m`），逐个标的：

| 情况 | 动作 |
|---|---|
| 已校准到 `≥ target` | **跳过，不发请求** |
| 已校准到 `lh < target` | 从 `lh` 起重取（多取 `lh` 这一根做重叠），一直到现在 |
| 没有任何校准行（新库、新上市） | 取 `hist_cold_start_window`（默认 48 小时） |
| 缺口早于 `hist_max_backfill`（默认 7 天） | 截断到该范围，并计入 `oi_hist_gap_beyond_cap_total`；更早的部分需要归档 |

**每个请求的 `limit`** = `min(缺口根数 + hist_limit_margin, hist_max_limit)`。正常每小时约 17；停机 3 小时约 39；缺口超过 500 根时**从新往旧翻页**（§2.4：先取最新一页，再把 `endTime` 设成该页最早标签减 1 毫秒，直到覆盖起点）。因此启动补缺和每小时校准是**同一段代码**。

**其余的设计点：**

1. **匀速铺开：** 定时校准每小时 `hh:05` 开始，一轮的请求在 `hist_reconcile_spread`（20 分钟）内均匀排开（约 0.44 次/秒），启动补缺不铺开。
2. **缺口大的优先：** 一轮里按缺口从大到小取；中途被 429 暂停，最重要的已经取完。
3. **失败处理：** 单个标的失败，轮末重试一次；仍失败则状态不前进，下一轮的缺口会自动变大补上。`ErrInvalidSymbol`（标的刚下架）和空数组（刚上市）不算失败。
4. **状态只在落盘后才前进：** 一轮结束时先对写入器做一次 `Flush` 屏障，成功才更新内存里的“校准到哪里”。否则一次被丢弃的批次会让内存状态永远领先数据库。
5. **稳态不读 ClickHouse：** 状态放在内存里，每轮约 528 × 13 ≈ 7000 行直接写入，`ReplacingMergeTree` 吸收。
6. **live 与 hist 的对比几乎零成本：** live 行写入时在内存里保留每个标的最近 24 根；校准时直接对比，得到 `oi_live_vs_hist_rel_diff`（偏差直方图）与 `oi_live_gap_bars_total`（live 缺失的根数）；每轮结束后计算 `oi_cross_section_complete_ratio`。
7. **校准之后整个截面才是同一时刻：** live 快照分散在 `B-30s` 到 `B-9s` 之间，hist 是恰好 `B`。

成本与间隔成反比：每小时 528 次请求，每 30 分钟 1056 次（占 1000 的 8.8%），每 4 小时 132 次。

### 5.9 实现状态

| 步骤 | 状态 |
|---|---|
| 配置（`open_interest:`、校验、示例） | 已完成 |
| `align.go`（`liveBarStart` / `histBarStart` / `newestPublishedStart`）与测试 | 已完成 |
| `row.go`、`writer.go`（独立批量写入器）、`clickhouse.go`（写入与 `LastStarts` 读取） | 已完成 |
| `client.go`（手写 REST）、`limiter.go`（`/futures/data` 池） | 已完成 |
| `hist.go`（冷启动决策、分页、校准）、`live.go`（边界调度）、`syncer.go` | 已完成 |
| `internal/app` 接线 | 已完成 |
| 归档导入器与完整性检查脚本 | **未做** |

测试：`internal/openinterest` 下的单元测试全部用假时钟与一个**忠实模拟真实接口语义**的假 API（返回区间内最新的 `limit` 根）；另有两个默认跳过的测试：`TestIntegrationClickHouse`（真实 ClickHouse 的临时库：时间无偏移、hist 覆盖 live、`LastStarts` 语义）与 `TestE2ERealBinance`（真实币安：冷启动、二次运行、重启、3 小时停机、一轮真实 live、被 hist 校准）。

**上线顺序：** 执行 004 → 只开 `hist_enabled` 跑一天 → 看指标（`oi_hist_pass_seconds`、`oi_cross_section_complete_ratio`、`oi_data_window_used`、`weightgate_used_weight_1m`）→ 再开 `live_enabled`。
