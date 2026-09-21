> **Language:** [English](DATA_CONSUMER_GUIDE.md) | 简体中文

# ChomoSyncer-go 数据消费指南

**面向对象**：读取 `ChomoSyncer-go` 生产出的行情数据的下游消费方（Python 或任何其它语言）。你**不需要**运行、部署或理解 Go 内部实现——只需要知道数据源里有什么、长什么样、怎么读。

这是一份**面向"怎么消费数据"**的参考文档，不是"数据怎么被生产出来的"。如果要了解生产端的内部机制（goroutine 模型、并发设计、Go 内部结构体、逐行代码入口），去看 [`DATA_FLOW_AND_STRUCTURES.md`](DATA_FLOW_AND_STRUCTURES.zh-CN.md)——本文档是给"我只想正确读到数据"这个诉求准备的精简版。

---

## 两大生产者一览

下游要读的数据，全部来自同一条流水线写入的**两个存储**：

| # | 存储 | 里面是什么 | 结构 | 覆盖的币种 | 历史深度 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | **ClickHouse**（`market` 库） | 1m 原始归档 + 5m/15m/1h/4h/1d rollup | SQL 表 | 全周期、全市场 | 完整历史（以年计） |
| 1b | **ClickHouse** —— `market.fapi_oi_5m` + 汇总表 *(可选)* | 每根 5m bar 收盘时刻的持仓量 | SQL 表 | 全市场 | 从模块启用起算（从币安补回至多约 30 天） |
| 2a | **Redis** —— `livebar:{SYM}:1m` | 还在形成中、未收盘的当前 bar | Hash | 仅 1m | 1 根（每次更新原地覆盖） |
| 2b | **Redis** —— `kline:{SYM}:1m` | 最近 200 根**已收盘**的 1m bar | List | 仅 1m | 200 根（约 3 小时 20 分） |
| 2c | **Redis** —— `stream:market:kline_ready` | "全市场某个截面刚收盘"的通知 | Stream | 全周期（见下方说明） | 最近 `stream_maxlen` 条（默认 10000） |

**该读哪个，简单判断：**
- 要 `1m` 还在实时跳动的最新价/成交量？→ **livebar**。
- 要最近 ~3 小时的已收盘 `1m` 滚动特征窗口，全市场、一个网络往返拿全？→ **已收盘滑窗**。
- 要知道全市场某个周期的截面**恰好**收齐的那一刻，用来触发计算？→ **kline_ready**，拿到通知后再去 Redis（`1m`）或 ClickHouse（更粗周期）取真正的数据。
- 要 3 小时之前的数据，或者 `1m` 以外的任何周期？→ **一律去 ClickHouse**。
- 要**持仓量**？→ **ClickHouse 的 `fapi_oi_5m`**（§1b），按 `(symbol, start_time)` 与 `fapi_kline_5m` 拼接。Redis 里没有 OI。

---

## 连接信息

| | 默认值 | 配置项 |
| :--- | :--- | :--- |
| ClickHouse 原生协议（Go 写入用） | `9000` | `config.yaml` 里的 `clickhouse.addrs` |
| ClickHouse HTTP（Python 客户端用这个） | `8123` | 同一台机器，固定偏移 |
| ClickHouse 库名 | `market` | `clickhouse.database` |
| Redis | `6379`，db `0` | `redis.addr` / `redis.db` |

具体环境的真实地址/账号，找部署方要（看 `config.yaml`）——本文档默认你已经能连上。

```bash
pip install redis clickhouse-connect
```

---

## 1. ClickHouse —— 权威归档

### 表

`market.fapi_kline_1m` 是采集器唯一直接写入的表。更粗的周期都是从它 rollup 出来的物化视图，从不手动改：

| 表 | 周期 |
| :--- | :--- |
| `market.fapi_kline_1m` | 1 分钟 |
| `market.fapi_kline_5m` | 5 分钟 |
| `market.fapi_kline_15m` | 15 分钟 |
| `market.fapi_kline_1h` | 1 小时 |
| `market.fapi_kline_4h` | 4 小时 |
| `market.fapi_kline_1d` | 1 天 |

六张表列结构完全一致：

| 列 | 类型 | 含义 |
| :--- | :--- | :--- |
| `symbol` | `LowCardinality(String)` | 如 `"BTCUSDT"` |
| `start_time` | `DateTime64(3, 'UTC')` | Bar 开盘时间，UTC，毫秒精度。**主键**——所有对齐/join 都靠它。 |
| `end_time` | `DateTime64(3, 'UTC')` | Bar 收盘时间，UTC。 |
| `open` / `high` / `low` / `close` | `Float64` | OHLC。 |
| `volume` | `Float64` | 基础资产成交量。 |
| `quote_volume` | `Float64` | 计价资产（USDT）成交额。 |
| `taker_buy_volume` | `Float64` | 主动买入基础资产量（做订单流/CVD 有用）。 |
| `taker_buy_quote_volume` | `Float64` | 主动买入计价资产额。 |
| `trades_count` | `UInt32` | 该 bar 内成交笔数。 |
| `created_at`（仅 1m 表）/ `rollup_version`（rollup 表） | `DateTime` / `DateTime64(3,'UTC')` | 内部字段，供 ClickHouse 合并去重用。除非在排查问题，否则不用管它。 |

> ⚠️ **查询永远要带 `FINAL`。** 引擎是 `ReplacingMergeTree`，去重是**后台合并**时才发生的——没合并完的表，同一个 `(symbol, start_time)` 短暂地可能有不止一行物理数据。`FINAL` 强制读时去重。下面所有示例都带了，别删掉。

### 怎么读

```python
import clickhouse_connect

client = clickhouse_connect.get_client(
    host="localhost", port=8123, username="default", password="", database="market"
)

# 某个币种最近 7 天的 1h K 线，直接拿成 DataFrame
df = client.query_df("""
    SELECT symbol, start_time, end_time, open, high, low, close,
           volume, quote_volume, taker_buy_volume, taker_buy_quote_volume, trades_count
    FROM market.fapi_kline_1h FINAL
    WHERE symbol = 'BTCUSDT' AND start_time >= now() - INTERVAL 7 DAY
    ORDER BY start_time ASC
""")

# 全市场在某个已收盘时刻的截面
snapshot = client.query_df("""
    SELECT symbol, close, quote_volume, trades_count
    FROM market.fapi_kline_1h FINAL
    WHERE start_time = '2026-09-07 16:00:00'
    ORDER BY quote_volume DESC
""")
```

```bash
# 命令行版本，随手看一眼用
clickhouse-client --query "
SELECT symbol, start_time, close, volume
FROM market.fapi_kline_1m FINAL
WHERE symbol = 'BTCUSDT' ORDER BY start_time DESC LIMIT 5
FORMAT PrettyCompact"
```

---

## 1b. ClickHouse —— 持仓量（`fapi_oi_*`）

持仓量（OI）是某个币种未平仓合约的总张数。本节是**可选数据**：只有生产者以 `open_interest.hist_enabled` / `live_enabled` 运行、且已建好 `deploy/clickhouse/004`–`006` 的表，才会存在。先确认：

```sql
EXISTS TABLE market.fapi_oi_5m
```

背后的设计与实测依据见 [`../new_requirements/oi.md`](../new_requirements/oi.md)。

### 表

| 表 | 周期 |
| :--- | :--- |
| `market.fapi_oi_5m` | 5 分钟 —— 生产者直接写入的原始表 |
| `market.fapi_oi_15m`、`_1h`、`_4h`、`_1d` | 汇总表，由 5m 表重算得到，不要手工改 |

`market.fapi_oi_5m` 的列：

| 列 | 类型 | 含义 |
| :--- | :--- | :--- |
| `symbol` | `LowCardinality(String)` | 例如 `"BTCUSDT"` |
| `start_time` | `DateTime64(3, 'UTC')` | **5 分钟 K 线**的开盘时间——与 `fapi_kline_5m` 的 `start_time` 是同一个。**拼接键。** |
| `sum_open_interest` | `Float64` | 持仓量，单位是合约张数（标的资产计）。 |
| `snap_time` | `DateTime64(3, 'UTC')` | 这个值实际被观测到的时刻（见“一行数据是什么意思”）。 |
| `src_rank` | `UInt8` | 谁写的这一行：`1` live 快照，`2` 币安历史序列（`openInterestHist`），`3` 币安每日归档。合并时最高的胜出。 |
| `created_at` | `DateTime` | 内部字段，除非排查问题否则忽略。 |

> ⚠️ **一律带 `FINAL` 查询**，和 K 线表一样：`ReplacingMergeTree` 在后台按 `(symbol, start_time)` 去重，一行 live 数据和它后来的 hist 替换行在合并之前可能同时存在。

### 一行数据是什么意思（拼接之前务必读一遍）

- **`start_time` 是 bar 的开盘时间，但数值是该 bar 收盘那一刻（`start_time + 5 分钟`）的持仓量**，与这根 K 线的 `close` 平行。所以同一个 `start_time` 的 OI 行和 K 线，是在**同一时刻** `start_time + 5m` 才一起变得可知——在这个时刻或之后用它们做决策，没有前视。
- **没有名义价值列。** 币安自己的名义价值是 `持仓量 × 标记价格`；用同一根 5m K 线的 `sum_open_interest * close` 就能近似（与币安的值实测平均偏差：BTCUSDT 0.35 bp、ETHUSDT 0.51 bp、SOLUSDT 0.90 bp）。
- **一行数据会被修正一次。** 它先以 live 快照（`src_rank = 1`）写入，快照发生在 bar 收盘**之前**约 10–35 秒；通常一小时内会被币安自己在同一时刻的历史点替换（`src_rank = 2`；从每日归档导入的是 `3`）。在真实币安上的测试里两者平均相差 0.04%。所以实盘策略在收盘时看到的是 live 值，回测读到的是修正后的值——这是一个很小的、有意接受的差异。带 `FINAL` 读取时永远返回最高级别的那一版。
- **校准之后整个截面是同一时刻的。** 各个标的的 live 快照分散在约 20 秒之内，而 hist 的值全部恰好落在 bar 收盘那一刻。

### 怎么读

```python
import clickhouse_connect

client = clickhouse_connect.get_client(host="localhost", port=8123, username="default", password="", database="market")

# 把 OI 拼到同一根 5m K 线上 -> t, o, h, l, c, v, oi
df = client.query_df("""
    SELECT k.symbol, k.start_time, k.open, k.high, k.low, k.close, k.volume,
           o.sum_open_interest,
           o.sum_open_interest * k.close AS oi_value_approx
    FROM market.fapi_kline_5m AS k FINAL
    LEFT JOIN market.fapi_oi_5m AS o FINAL
           ON o.symbol = k.symbol AND o.start_time = k.start_time
    WHERE k.symbol = 'BTCUSDT' AND k.start_time >= now() - INTERVAL 1 DAY
    ORDER BY k.start_time
    SETTINGS join_use_nulls = 1
""")
```

> ⚠️ **LEFT JOIN 要加 `SETTINGS join_use_nulls = 1`。** ClickHouse 默认会把右表**缺失**的行补成 `0`，于是没有 OI 行的 bar 会被悄悄读成“持仓量为 0”，而不是 `NULL`。（已验证：默认返回 `0`，加了这个设置返回 `NULL`。）

```sql
-- 每根 bar 的持仓量变化
SELECT start_time, sum_open_interest,
       sum_open_interest - lagInFrame(sum_open_interest) OVER (PARTITION BY symbol ORDER BY start_time) AS d_oi
FROM market.fapi_oi_5m FINAL
WHERE symbol = 'BTCUSDT' AND start_time >= now() - INTERVAL 1 DAY
ORDER BY start_time

-- 全市场在某根已收盘 bar 上的持仓量
SELECT symbol, sum_open_interest FROM market.fapi_oi_5m FINAL
WHERE start_time = '2026-09-21 12:00:00' ORDER BY symbol
```

### 汇总表

`fapi_oi_{15m,1h,4h,1d}` 每个桶存的是**桶收盘时刻**的持仓量及其区间：

| 列 | 含义 |
| :--- | :--- |
| `symbol`、`start_time` | 桶起点（UTC）。 |
| `samples` | 桶内 5m 行数。完整的桶是 3 / 12 / 48 / 288（15m / 1h / 4h / 1d）。**务必按它过滤**——最新的那个桶通常是不完整的。 |
| `sum_open_interest_close` | 桶收盘时刻的持仓量（桶内最后一个 5m 行）。 |
| `sum_open_interest_high` / `_low` | 桶内各收盘快照的最大 / 最小值。 |
| `rollup_version` | 内部字段。 |

没有 `open`：一个桶开盘时刻的持仓量就是上一个桶的 close——用 `lagInFrame(sum_open_interest_close) OVER (PARTITION BY symbol ORDER BY start_time)` 取。

```sql
SELECT start_time, samples, sum_open_interest_close, sum_open_interest_high, sum_open_interest_low
FROM market.fapi_oi_1h FINAL
WHERE symbol = 'BTCUSDT' AND samples = 12
ORDER BY start_time DESC LIMIT 24
```

### 能回溯多远，怎么确认可信

- **历史从模块启用那一刻开始。** 首次启动时，生产者会把库里缺的部分从币安补回来（默认 48 小时，最多 7 天）；币安本身只保留约 30 天。更早的数据需要归档导入，这一块还不在模块里。
- 依赖它之前先验证：[`../cmd/test-tools/check_oi_consistency.py`](../cmd/test-tools/README.zh-CN.md) 会检查缺口、5 分钟网格、`snap_time`、新鲜度、hist 校准、与 `fapi_kline_5m` 的覆盖率，加 `--vs-binance` 还会与币安逐值对账。

---

## 2. Redis —— `livebar:{SYMBOL}:1m`（未收盘实时快照）

某个币种当前正在形成的 bar，收盘前每秒更新多次。**只有 `1m` 有**——不存在 `livebar:*:5m` 这种 key。

- **Key**：`livebar:{SYMBOL}:1m`，币种**大写**（如 `livebar:BTCUSDT:1m`）
- **类型**：Redis `Hash`
- **TTL**：`2 × interval`（1m 就是 120 秒）——如果采集器停止更新某个币种，key 会自己过期；key 不存在不一定是故障，先看 `t` 判断新鲜度。

| Hash 字段 | 含义 | Python 转型 |
| :--- | :--- | :--- |
| `t` | Bar 开盘时间，epoch 毫秒（UTC） | `int(v)` |
| `o` | 开盘价 | `float(v)` |
| `h` | 目前为止的最高价 | `float(v)` |
| `l` | 目前为止的最低价 | `float(v)` |
| `c` | 最新价（当前的"收盘价"） | `float(v)` |
| `v` | 目前为止的累计基础成交量 | `float(v)` |
| `qv` | 目前为止的累计计价（USDT）成交额 | `float(v)` |
| `tbv` | 目前为止的主动买入基础量 | `float(v)` |
| `tbqv` | 目前为止的主动买入计价额 | `float(v)` |
| `n` | 目前为止的成交笔数 | `int(v)` |
| `x` | `"1"` = 刚收盘，`"0"` = 仍在形成 | `v == "1"` |

> ⚠️ **所有值取回来都是字符串**——Redis Hash 没有数值类型，算之前先转型。

```python
import redis

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

pipe = r.pipeline()
symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
for sym in symbols:
    pipe.hgetall(f"livebar:{sym}:1m")
raw = pipe.execute()

live = {
    sym: {"t": int(h["t"]), "close": float(h["c"]), "closed": h["x"] == "1"}
    for sym, h in zip(symbols, raw) if h
}
```

---

## 3. Redis —— `kline:{SYMBOL}:1m`（已收盘滑窗）

最近 200 根**已收盘**的 1m bar，严格有序排列，用来给全市场特征计算做快速拉取。**只有 `1m` 有**——更粗周期的滑窗需求直接查 ClickHouse（那边开销很小，完全够用）。

- **Key**：`kline:{SYMBOL}:1m`，币种大写
- **类型**：Redis `List`，固定长度 200
- **顺序**：**从新到旧**——index `0` 是最新收盘的那根，index `199` 是最老的。
- **元素格式**：紧凑、**不带 key** 的 10 元素 JSON 数组（不是对象——省内存也省 Python 解析开销）：

| 数组下标 | 字段 | 类型 |
| :--- | :--- | :--- |
| `[0]` | `start_time`（epoch 毫秒，UTC） | `int` |
| `[1]` | `open` | `float` |
| `[2]` | `high` | `float` |
| `[3]` | `low` | `float` |
| `[4]` | `close` | `float` |
| `[5]` | `volume` | `float` |
| `[6]` | `quote_volume` | `float` |
| `[7]` | `taker_buy_volume` | `float` |
| `[8]` | `taker_buy_quote_volume` | `float` |
| `[9]` | `trades_count` | `int` |

```python
import redis, json

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# 单个币种
raw_bars = r.lrange("kline:BTCUSDT:1m", 0, 199)      # 从新到旧
bars = [json.loads(b) for b in raw_bars]
latest_close = bars[0][4]
latest_trades = bars[0][9]

# 全市场，一个网络往返拿全（推荐用法）
universe = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]           # 你自己的 universe 来源
pipe = r.pipeline()
for sym in universe:
    pipe.lrange(f"kline:{sym}:1m", 0, 199)
batch = pipe.execute()

market = {sym: [json.loads(b) for b in raw] for sym, raw in zip(universe, batch)}
```

刚冷启动的币种（新上市，或者服务刚从一次长时间断线里恢复）滑窗可能暂时不满 200 根——用之前先 `len(bars)` 确认一下，别默认它是满的。

**这跟 ClickHouse 是怎么对上的**：这个数组、`livebar` 的 Hash、还有 ClickHouse 那一行，全部来自**同一个**解析结果（`dispatcher.KlineEvent.toCompactBar` / `.toLiveBar` / `.toRow` 各转一次，不存在各自重新解析——所以只要某个字段在不止一处出现，值必然完全一致）。但**字段集合不是完全一样的**——ClickHouse 是超集：

| 字段 | ClickHouse 行 | 这个数组 | `livebar` Hash |
| :--- | :---: | :---: | :---: |
| `start_time` | ✅ | ✅ `[0]` | ✅ `t` |
| `end_time` | ✅ | ❌ | ❌ |
| OHLC | ✅ | ✅ `[1..4]` | ✅ `o/h/l/c` |
| `volume` / `quote_volume` | ✅ | ✅ `[5..6]` | ✅ `v/qv` |
| `taker_buy_volume` / `_quote_volume` | ✅ | ✅ `[7..8]` | ✅ `tbv/tbqv` |
| `trades_count` | ✅ | ✅ `[9]` | ✅ `n` |
| 收盘标志位 | 不适用（CH 只存已收盘的） | 不适用 | ✅ `x` |

ClickHouse 有、两边 Redis 结构都没有的列只剩 `end_time` 一个——故意不带，因为对齐/去重全靠 `start_time` 一个键就够，用不上它。真要 `end_time`，去查 ClickHouse。

---

## 4. Redis —— `stream:market:kline_ready`（截面就绪通知）

一个单独的 Redis Stream，用来广播"这个周期全市场的 bar 都收盘了（或者兜底超时触发了）——可以去取数据了"。这是一个**触发信号**，不是数据本身——它从不携带 OHLCV，只告诉你去哪查。

- **Key**：`stream:market:kline_ready`（固定名字，不按币种区分）
- **类型**：Redis `Stream`，`MAXLEN ~ 10000`（近似裁剪，老事件会被挤出去）

| 字段 | 类型 | 含义 |
| :--- | :--- | :--- |
| `interval` | `string` | 刚收盘的周期：`"1m"`（原生发布，每分钟一条）或 `serve_intervals` 里的更粗周期（`"5m"`、`"15m"`、`"1h"`、`"4h"`、`"1d"`——在其对应的 1m 截面就绪时派生转发一条）。 |
| `timestamp` | `int64`（字符串形式） | 该截面的开盘时间，epoch 毫秒 UTC——和该 bar 的 `start_time` / 数组 `[0]` / Hash `t` 是同一个值。 |
| `symbols_count` | `int`（字符串形式） | 这个截面实际到齐了多少个币种。可以拿自己的 universe 大小来对比，评估齐备率。 |

**派生的粗周期事件到底是怎么产生的**——只有你会消费 `"1m"` 以外的事件才需要看这段，但很容易理解错，所以专门讲清楚：

1. 聚合器只按 `1m` 粒度追踪到齐情况。某个 `1m` 截面收齐（或超时）时，先发基准的 `interval="1m"` 事件。
2. **只有这个基准事件真的发出去了**（没被下面讲的 gate 压住），才会去检查每个配置的粗周期：这根 `1m` bar 的收盘时刻，是不是恰好也是那个粗周期自己桶的边界（`(开盘时间 + 60_000) % 该周期毫秒数 == 0`）。命中的每一个，都会单独发一条 `interval` 标成对应粗周期的 `kline_ready`。
3. **粗周期事件上的 `symbols_count` 不是独立算出来的**——是直接沿用触发它的那个 `1m` 截面的数值。一条 `"1h"` 事件的 `symbols_count`，反映的是"这一小时最后一分钟有多少币种到齐"，不是"这一小时里有多少币种凑出了完整的 1h bar"。实际中这两个数通常一样，但要当成一个代理值，不是精确的粗周期覆盖率——真要精确的，自己数:`SELECT count() FROM market.fapi_kline_1h FINAL WHERE start_time = <bucket_start>`。
4. 第 2 步带来一个直接后果：如果某根 `1m` 的基准事件被压住了，**这根 bar 本该触发的所有粗周期事件也会跟着一起被跳过**——被压住的那一分钟如果刚好也是整点，那这一小时的 `kline_ready` 也不会发了，不只是这一分钟的。

```python
import redis

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
last_id = "$"   # "$" = 只要之后发生的事件；用 "0" 可以回放流里现存的全部历史

while True:
    resp = r.xread({"stream:market:kline_ready": last_id}, block=0, count=1)
    for _stream, entries in resp:
        for entry_id, fields in entries:
            last_id = entry_id
            interval = fields["interval"]
            ts = int(fields["timestamp"])
            n = int(fields["symbols_count"])
            print(f"截面就绪: interval={interval} ts={ts} symbols={n}")

            if interval == "1m":
                # 走 Redis 快路径，拿全市场已收盘滑窗
                ...
            else:
                # 更粗周期 -> 查 ClickHouse market.fapi_kline_<interval> FINAL
                # WHERE start_time = <ts 转成 UTC 时间>
                ...
```

> ⚠️ **这是尽力而为的提示，不是有保证的投递日志。** 当采集器正在为某个币种从历史重新同步时（刚重启之后，或某个分片短暂断网重连之后），整个 `1m` 周期的 `kline_ready` 会被压住——正常分片重连也就几秒，但刚重启完整个进程可能要压几分钟。**被压住期间错过的事件不会重试、也不会补发。** 如果你的消费端需要"这个周期是否真的收盘了"这种有保证的信号，而不只是一个低延迟的提醒，直接轮询 ClickHouse 或 Redis 滑窗，别只依赖这个 Stream。想看完整机制，仓库里的 `todo_improvement/cold-start-gate-batch-latency.md` 有详细记录。

---

## 字段与时间戳约定（全局通用）

- **所有时间都是 UTC。** ClickHouse 的 `start_time`/`end_time` 是 `DateTime64(3,'UTC')`；Redis 里所有毫秒级 epoch 值（`t`、数组 `[0]`、Stream 的 `timestamp`）同样以 UTC 为基准（epoch 本身跟时区无关，但换算成日期时按 UTC 处理）。
- **Redis 永远不返回数值类型。** Hash 字段、Stream 字段、紧凑 JSON 数组里的数字，在 Python 里都要显式转型（`float()`/`int()`）——`redis-py` 的 `decode_responses=True` 给你的是字符串，只有数组本身经过 `json.loads` 解析出来的才是真正的 `int`/`float`。
- **Redis 只认识 `1m` 这一个周期。** `livebar:*` 和 `kline:*` 永远只有 `...:1m` 这种 key——不存在 `livebar:BTCUSDT:5m`。更粗的周期一律在 ClickHouse 里。
- **ClickHouse 查询模式永远是 `... FROM market.fapi_kline_<interval> FINAL WHERE ...`。** 忘记带 `FINAL` 是最常见的坑——你偶尔会看到同一个 `(symbol, start_time)` 出现重复行。
- **持仓量打破了“`start_time` 就是这个值所属时间”的习惯。** 在 `fapi_oi_*` 里，`start_time` 是 K 线的**开盘**时间，但数值属于它的**收盘**时刻（`start_time + 5m`）——见 §1b。它仍然按 `start_time` 与 `fapi_kline_5m` 拼接。
- **`start_time` 是全局统一的对齐键**——ClickHouse、Redis List 的 `[0]`、Redis Hash 的 `t`，同一根 bar 在三处的这个值完全一致。用它来对齐全市场截面。

---

## 接下来看哪份文档

| 需求 | 文档 |
| :--- | :--- |
| 这些数据内部到底是怎么生产出来的（goroutine、并发、Go 结构体） | [`DATA_FLOW_AND_STRUCTURES.zh-CN.md`](DATA_FLOW_AND_STRUCTURES.zh-CN.md) |
| 部署 / 运维这个生产者本身 | [`OPERATIONS.md`](OPERATIONS.zh-CN.md) |
| 在信任这些数据源之前，先验证它们健康、完整 | [`../cmd/test-tools/README.zh-CN.md`](../cmd/test-tools/README.zh-CN.md)——尤其是 `check_redis_livebars.py`、`check_redis_closed_windows.py`、`check_vs_binance.py`，验证的正是本文档讲的这几条读取路径；`check_oi_consistency.py` 覆盖 §1b |
| 逐模块的架构设计 | [`ARCHITECTURE_MODULES.md`](ARCHITECTURE_MODULES.zh-CN.md) |
| 持仓量为什么这样对齐、这样修正（实测与设计） | [`../new_requirements/oi.md`](../new_requirements/oi.md) |
