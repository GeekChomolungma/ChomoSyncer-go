# Binance U 本位合约持仓量（OI）落盘设计（第一版）

> 状态：设计稿。新表 SQL（`deploy/clickhouse/004~006`）已实现并在 ClickHouse 26.8 的临时库里验证过；Go 代码尚未实现。
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
3. `snap_time` 记录 `time`，用于审计快照实际发生的时刻。

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

### 4.2 汇总表：`market.fapi_oi_{15m,1h,4h,1d,1mo}`（5 张）

由 5m 原始表在 ClickHouse 内汇总，思路与 `002_kline_rollups.sql` 一致：**每次从源表 `FINAL` 全量重算、不累加、每档直接读 5m 表不做级联**，目标表 `ReplacingMergeTree(rollup_version)`。

| 列 | 含义 |
|---|---|
| `symbol`、`start_time` | 桶起点（UTC 对齐；月桶用 `toStartOfMonth`） |
| `samples` | 桶内 5m 行数。完整桶为 3 / 12 / 48 / 288（15m/1h/4h/1d），月为 `当月天数 × 288`，使用前应据此过滤 |
| `sum_open_interest_{close,high,low}` | `close` = 桶内最后一行（桶收盘时刻的持仓量）；`high/low` = 桶内各收盘快照的最大/最小 |
| `rollup_version` | `now64(3)` |

不提供 `open`：一个桶“开盘时刻”的 OI 是上一个桶的 `close`，消费者用 `lag` 即可，放在桶内会造成语义混乱。

**回看下界必须对齐桶边界并锚定 UTC**（写法见 `deploy/clickhouse/002_kline_rollups.sql`，原因见 `deploy/clickhouse-fixes/README.md`）：

```sql
WHERE start_time >= toStartOfInterval(toTimeZone(now(), 'UTC') - INTERVAL 3 DAY, INTERVAL 1 HOUR)
```

### 4.3 物化视图：`market.fapi_oi_{15m,1h,4h,1d,1mo}_rmv`（5 个）

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

### 5.3 配置（`config.example.yaml` 新增）

```yaml
open_interest:
  hist_enabled: false          # 先开这个：启动补缺 + 每小时校准
  live_enabled: false          # 观察一天校准指标（§5.5）后再开
  live_lead: 30s               # 在 K 线收盘前多久开始一轮
  live_accept_window: 60s      # 快照 time 距最近 5m 边界的最大偏差
  fapi_rps: 25                 # live 一轮的发出节奏；同时受共享权重闸门约束（§1）
  hist_reconcile_interval: 1h
  hist_reconcile_offset: 5m    # 每小时 hh:05 开始（标签 hh:00 最晚约 3 分钟可读）
  hist_reconcile_spread: 20m   # 一轮 528 次请求在这段时间内匀速铺开
  hist_limit_margin: 3         # limit = ceil((now - 该标的最新 hist 行) / 5m) + margin
  hist_max_limit: 500          # 需要更多时改用 startTime/endTime 分页
  data_window_cap: 900         # /futures/data 5 分钟滑动窗口硬上限
  data_rps: 2                  # 启动补缺时的上限；每小时校准用 spread 算出的更低速率
```

### 5.4 启动与停机顺序

1. 加载 universe。
2. `hist_enabled` 时**立刻跑一轮 hist**（这就是启动补缺，与每小时校准是同一段代码），等它完成，再启动 live 调度；补缺期间 live 不启动，避免额度冲突。
3. 启动 live 调度、启动 hist 定时对账。
4. 停机：停止调度 → 等在途请求 → `writer.Close()` 排空并落盘。

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

### 5.8 每小时校准的高效设计

hist 没有批量，一轮 = N（528）次请求，这个数省不掉。能优化的是节奏和写入：

1. **匀速铺开，不突发。** 每小时 `hh:05` 开始，在 `hist_reconcile_spread`（20 分钟）内铺开，约 0.44 次/秒；每个 5 分钟窗口约 132 次，占 1000 的 13%。一口气按 2 次/秒发完，一个窗口就占 53%。
2. **每个标的的 `limit` 动态计算：** `limit = ceil((now − 该标的最新 hist 行) / 5m) + hist_limit_margin`，限制在 `[6, hist_max_limit]`。正常每小时约 16；停机 30 小时约 360，一页拿完；超过 500 才用 `startTime/endTime` 分页。因此**启动补缺与每小时校准是同一段代码**；新上市标的没有 hist 行，自然按最大值取；超过 30 天的缺口只能等归档，日志里标明。
3. **稳态不读 ClickHouse：** 启动时用 `store.go` 查一次每个标的最新 hist 行，放在内存里，每次成功写入后更新。稳态每轮约 528 × 13 ≈ 7000 行写入，`ReplacingMergeTree` 直接吸收，不需要先读再比较。
4. **缺口优先：** 一轮里 live 行缺失或只有 live 值的标的排前面；若中途被 429 暂停，最重要的先覆盖。每轮起点轮转，避免总是同一批标的排在最后。
5. **失败处理：** 单个标的失败，轮末重试一次；仍失败就留给下一轮（下一轮的 `limit` 会自动变大补上）。
6. **顺带的监控，几乎零成本：** live 行写入时在内存里保留每个标的最近约 24 根，校准时直接与 hist 对比（不用查库），得到 `oi_live_vs_hist_rel_diff` 与 `oi_live_gap_bars_total`；每轮结束后计算 `oi_cross_section_complete_ratio`，低于 99% 告警。这能提早发现 live 平移错误或系统性偏移。
7. **附带收益：** live 快照的时刻分散在 `B-30s` 到 `B-9s` 之间，hist 是恰好 `B`。校准之后，每个 `t` 的整个截面才是**同一时刻**，做横截面因子时用校准后的数据更干净。

成本与间隔成反比，间隔就是“多久后值才被封存”：每小时 528 次，每 30 分钟 1056 次（占 1000 的 8.8%），每 4 小时 132 次。默认每小时。

### 5.9 实现步骤

每步都可以独立验证：

1. **配置：** `internal/config` 加 `OpenInterestConfig`（默认值、`Validate`）与 `config.example.yaml`；`hist_enabled` 与 `live_enabled` 分开。
2. **`align.go` 加测试：** `liveBarStart`（略早、略迟、恰在边界、超过 60 秒丢弃）、`histBarStart`（含日边界）。
3. **`row.go` + `writer.go`：** 写 `DateTime64` 一律用 `time.Time`，不要用裸 epoch 数字（会被解析成乱值）。用 fake flusher 测试。
4. **`client.go`：** 用已抓到的真实响应做 `httptest` 样例。
5. **限流：** `/futures/data` 的 `limiter.go`。`/fapi` 的共享权重闸门（§1）已完成。
6. **`store.go` + `hist.go`：** 动态 `limit`、内存状态、`spread` 铺开。
7. **`live.go`：** 假时钟 + fake client，验证归属和丢弃规则、一轮耗时。
8. **`syncer.go` + `internal/app` 接线：** `New()` 构造，`Run()` 在 `univ.Start()` 之后启动，`Shutdown()` 里 `closeStep(…, "openinterest", …)` 放在 collector 之后；live 每轮直接取当时的 `univ.Snapshot().Symbols`，不订阅 `OnChange`。
9. **归档导入器**（照 `import_fapi_kline.py` 风格）与完整性检查脚本（每个 `(symbol, 日)` 288 行）。
10. **上线顺序：** 执行 004 → 只开 `hist_enabled` 跑一天 → 看 §5.5 的指标 → 再开 `live_enabled`。
