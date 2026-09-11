> **Language:** [English](README.md) | 简体中文

# ChomoSyncer 数据校验与线上预检工具包 (Test & Validation Toolkit)

本目录提供一套完整的 Python 脚本工具库，专门用于 **ChomoSyncer-go** 系统在部署上线前、运行期间或断线恢复后的全链路数据完整性、连续性、时效性及多级存储一致性校验。

每个工具都会打印一张易读的表格，并给出明确结论；除纯监控工具 `monitor_redis_kline_ready.py` 外，其余全部遵循"PASS 退出码 0，FAIL 非 0"，可以直接接进 cron / CI，不用额外包一层判断逻辑。

---

## 推荐执行顺序

下面各小节的排列顺序，按的是**在真实部署流程中每个检查项第一次有意义的时间点**，不是字母序或文件名序。分两个阶段：

| 阶段 | 时机 | 工具（按顺序） |
| :--- | :--- | :--- |
| **A —— 在线服务启动前**（离线灌历史 + `002`/`003` rollup 建完之后） | 只需要 ClickHouse，不需要 Redis、不需要在跑的 daemon | 1) `check_clickhouse_integrity.py` → 2) `check_vs_binance.py` |
| **B —— 在线服务启动后** | 需要 daemon 正在往 Redis 写数据 | 3) `check_redis_livebars.py` → 4) `check_redis_closed_windows.py` → 5) `monitor_redis_kline_ready.py` → 6) `e2e_reconciliation.py` |
| **C —— 任意时刻，一把打包 A2 + 全部 B** | 一条命令，给出 go/no-go 结论 | 7) `run_all_checks.py` |

这和 `docs/OPERATIONS.md` §3（B.1/B.2）的顺序是一致的：先校验 ClickHouse 自身历史的完整性、再对回币安真值确认没有系统性偏差（该文档里两阶段冷启动的设计初衷正是为此），**都做完之后再打开在线链路**；在线链路起来之后，再校验面向 Redis 的实时通路。

---

## 目录结构

| 文件 | 说明 |
| :--- | :--- |
| `check_clickhouse_integrity.py` | **[阶段 A·1] ClickHouse K 线完整性检查器**：检测 1m 原始表 + 各 rollup 表的时间戳连续性（缺口/断档判定）与字段非空/有效性校验 |
| `check_vs_binance.py` | **[阶段 A·2] 外部真值对账**：把 ClickHouse（1m 原始表 / rollup 表）逐根对回币安 `/fapi/v1/klines`，抓采集字段映射、单位、rollup 桶对齐与聚合函数的系统性偏差 |
| `check_redis_livebars.py` | **[阶段 B·1] Redis 未收盘 Live Bar 检查器**：检测各币种 `livebar:{SYM}:1m` Hash 是否存在、TTL 时效、时延与字段完备性 |
| `check_redis_closed_windows.py` | **[阶段 B·2] Redis 已收盘滑窗检查器**：检测 `kline:{SYM}:1m` 200 根滑窗是否存在、单调递减连续性、内部有无漏 Bar、头部是否当下最新 |
| `monitor_redis_kline_ready.py` | **[阶段 B·3] Redis 截面通知 Stream 监控器**：监控 `stream:market:kline_ready`（含派生的粗周期信号），评估就绪时延、标的收盘覆盖率与聚合原因 |
| `e2e_reconciliation.py` | **[阶段 B·4] 缓存与存储对账工具**：对比 Redis `kline:{SYM}:1m` 滑窗与 ClickHouse `fapi_kline_1m`，逐根对比时间戳与 OHLCV 一致性 |
| `run_all_checks.py` | **[阶段 C] 一键全量预检调度器**：一键执行所有检查项，输出可视化红绿灯健康计分卡 (Scorecard) |
| `common.py` | 基础公用库：配置文件加载、ClickHouse/Redis 客户端、交易标的探测、时间工具、表格格式化等。本身不是一项检查。 |
| `test_toolkit.py` | 工具包内置单元测试与 Mock 验证套件（`python -m pytest cmd/test-tools/test_toolkit.py`，或直接运行） |
| `requirements.txt` | Python 依赖包清单 |

---

## 环境准备与依赖安装

在任意安装有 Python 3.9+ 的环境中执行：

```bash
pip install -r cmd/test-tools/requirements.txt
```

> **提示**：
> - 脚本优先连接 `config.yaml` 中的配置地址（找不到则退回 `config.example.yaml`）；用 `--config /path/to/config.yaml` 指向别处，或用各工具自带的 `--ch-*` / `--redis-*` 参数单独覆盖某个字段，不用改配置文件本身。
> - ClickHouse 支持双协议接入：优先通过 `clickhouse-connect`；若未安装 C 扩展，自动无缝降级为 ClickHouse 原生 HTTP 接口（8123 端口），无需担心跨平台编译依赖。
> - 会打币安 REST 的工具（`check_vs_binance.py`）和在线 daemon 自己的回补流量共用同一个 IP 的限流预算——跑全市场之前先看它那一节的限速说明。

---

## 工具详细使用说明

### 1. ClickHouse 历史 K 线完整性检查 (`check_clickhouse_integrity.py`)

**阶段 A·1 —— 离线灌历史 + `002`/`003` rollup 建完之后立刻跑，还不需要起在线服务。** 纯 ClickHouse 读取，不需要 Redis，也不需要 daemon 在跑。

利用 ClickHouse 向量化窗口函数 (`lagInFrame`)，在数秒内扫描全库数亿行 K 线记录，精确找出所有断档时间区间及异常脏数据。

#### 检查项目：
- **时间戳连续性**：计算相邻 K 线的 `diff_ms`，判断是否存在 `> interval_ms` 的断档，并计算累计丢失根数与覆盖率。
- **字段非空与逻辑有效性**：
  - 价格合理性：`open > 0`, `high > 0`, `low > 0`, `close > 0`
  - 价格上下界逻辑：`high >= low`, `high >= max(open, close)`, `low <= min(open, close)`
  - 成交量：`volume >= 0`, `quote_volume >= 0`
  - 成交笔数：`trades_count >= 0`（警告交易笔数为 0 的冷门假数据）
  - 时间戳对齐：`end_time > start_time` 且严格匹配周期间隔。

#### 参数：
| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `--config` | 自动探测 | `config.yaml` 路径 |
| `--intervals` | `1m,5m,15m,1h,4h,1d` | 逗号分隔的周期，每个映射到 `<table-prefix>_<interval>` |
| `--symbol` | *(全部)* | 只查一个币种，而非全库 |
| `--limit-symbols` | *(全部)* | 限制检查的币种数量 |
| `--table-prefix` | `market.fapi_kline` | ClickHouse 表前缀 |
| `--start-date` / `--end-date` | *(全部历史)* | 限定扫描时间范围，如 `"2024-01-01"` 或 `"2024-01-01 00:00:00"` |
| `--max-gaps` | `5` | 每个币种最多打印几个断档，超出截断 |
| `--show-all-gaps` | 关 | 打印全部断档，不截断 |
| `--ch-host` / `--ch-port` / `--ch-db` / `--ch-user` / `--ch-password` | 取自配置 | 逐字段覆盖 ClickHouse 连接信息 |

#### 运行示例：
```bash
# 检查默认配置的所有周期（1m + 全部 rollup）下全库所有币种
python cmd/test-tools/check_clickhouse_integrity.py

# 指定币种与周期检查
python cmd/test-tools/check_clickhouse_integrity.py --symbol BTCUSDT --intervals 1m

# 指定起始时间检查（如某日期之后），并列出全部断档
python cmd/test-tools/check_clickhouse_integrity.py --start-date "2026-09-01" --show-all-gaps

# 抽样快速检查 10 个币种
python cmd/test-tools/check_clickhouse_integrity.py --limit-symbols 10
```

退出码：`0` = 无断档、无非法字段；`1` = 发现完整性问题（或连接失败）。

---

### 2. ClickHouse 与币安外部真值对账 (`check_vs_binance.py`)

**阶段 A·2 —— 紧接第 1 项，同样在起在线服务之前跑。** `e2e_reconciliation.py`（第 6 项）是"管道跟自己比"（Redis 与 ClickHouse 都是同一条链路写入的）；本工具把 ClickHouse 逐根对回**币安官方** `/fapi/v1/klines`，是唯一能发现 1m 采集或 rollup 聚合**系统性偏差**（字段映射错、单位错、rollup 桶对不齐、聚合函数写错）的检查。

#### 检查项目：
- `start_time` 必须精确对齐（桶对齐 / 采集时间基准）。
- OHLC 相对容差 `1e-9`（`argMin/argMax/min/max` → 精确相等）。
- `volume / quote_volume / taker_buy_*` 相对容差 `1e-6`（浮点求和顺序不同：币安按逐笔成交求和，我们按 1m bar 求和）。
- `trades_count` 必须精确相等（整数求和）。
- 对比窗口永远是**"现在往前数的最近 N 根已收盘 bar"**（`--bars`，不是日期范围）——所以天然就是个"最近历史抽查"工具，比如 `--intervals 1m --bars 720` 就是最近 12 小时，不用自己算时间戳。

#### 参数：
| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `--config` | 自动探测 | `config.yaml` 路径 |
| `--intervals` | `1h,4h,1d` | 逗号分隔的周期；加 `1m` 验证原始采集，或用 rollup 周期验证 Phase B 聚合 |
| `--symbol` | *(抽样)* | 只查一个币种，而非从活跃 universe 抽样 |
| `--limit-symbols` | `8` | 不指定 `--symbol` 时抽样的币种数（按字母序）；传一个 ≥ 活跃币种总数的值即等于"全市场" |
| `--bars` | `48` | 每个 (币种, 周期) 组合比对的已收盘 bar 数，从现在往回数——如 `720` = 最近 12h 的 `1m` |
| `--settle-lag` | `1` | 排除窗口里最新的这么多根不参与比对。ClickHouse 的写入延迟（`chwriter` 按 `flush_interval` 攒批，一两秒级）、以及币安自己 WS 收盘帧和 REST 历史 K 线之间的结算差，都可能让**最新那一根**在链路完全健康的情况下短暂显示"缺失"或"不匹配"。528 个币种逐个查（`--symbol-delay`）跑下来要几分钟，"现在"这个概念全程在往前走，不留这道余量的话，全市场一跑就会报一批"轮着换币种"的假阳性。设成 `0` 可以追到最边缘（更吵；被标记的那根通常几秒内自己就结算好了——过一分钟单独重查一下就能确认）。 |
| `--price-tol` | `1e-9` | OHLC 相对容差 |
| `--volume-tol` | `1e-6` | 成交量类字段相对容差 |
| `--table-prefix` | `market.fapi_kline` | ClickHouse 表前缀 |
| `--http-timeout` | `15` | 币安 REST 超时（秒） |
| `--symbol-delay` | `0.35` | 币种之间的睡眠秒数。这里每次 `/fapi/v1/klines` 请求都固定 `limit=1500`（权重 10，跟 `--bars` 要多少无关）；`0.35s` ≈ 2.9 req/s ≈ 1740 权重/分钟，压在币安 2400/分钟 的单 IP 上限之下，还给在线 daemon 自己的 REST 用量留了余量。**跑全市场（`--limit-symbols` 覆盖全部）时千万别设成 0**——同样的权重预算算法见 `docs/OPERATIONS.md` 里 `backfill.rest_rps` 那段说明。 |
| `--show-only-failures` | 关 | 只打印非 PASS 的行 |
| `--ch-host` / `--ch-port` / `--ch-db` / `--ch-user` / `--ch-password` | 取自配置 | 逐字段覆盖 ClickHouse 连接信息 |

#### 运行示例：
```bash
# 1m 原始表 vs 币安 1m —— 验证采集字段映射/单位，抽样 10 个币种
python cmd/test-tools/check_vs_binance.py --intervals 1m --limit-symbols 10

# rollup 表 vs 币安 —— 验证 Phase B 的桶对齐与聚合函数
python cmd/test-tools/check_vs_binance.py --intervals 1h,4h,1d --symbol BTCUSDT --bars 96

# 全市场抽查最近 12 小时的 1m 数据（限速安全跑法；~500 个币种大概 4-6 分钟）
python cmd/test-tools/check_vs_binance.py --intervals 1m --bars 720 --limit-symbols 600 --symbol-delay 0.35 --show-only-failures
```

退出码：`0` = 所有检查的 bar 都在容差内匹配；`1` = 有不匹配或缺失。

---

### 3. Redis 未收盘 Live Bar 检查 (`check_redis_livebars.py`)

**阶段 B·1 —— 在线 daemon 跑起来之后第一个有意义的检查。** WS 采集器收到某币种第一帧行情的几秒内，对应的 `livebar:*` Hash 就应该出现，所以这是判断"服务是不是真的在流式采集"最快的信号。

检查 Redis 中正在形成的实时未收盘 K 线 Hash (`livebar:<SYMBOL>:<interval>`)。

#### 检查项目：
- **Key 存在性**：全市场活跃币种是否全部在 Redis 中落有 Live Bar。
- **TTL 有效性**：TTL 是否大于 0 且处于合理窗口 (`<= ttl_multiple * interval`)。
- **实时时效性**：`now_ms - t` 是否在当前周期内；若超过 `2 * interval` 则判定为假死 STALE。
- **11 个字段完整性**：`t, o, h, l, c, v, qv, tbv, tbqv, n, x`。

#### 参数：
| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `--config` | 自动探测 | `config.yaml` 路径 |
| `--intervals` | `1m` | 逗号分隔的周期；只有基准周期有 Redis live bar |
| `--symbol` | *(全部)* | 只查一个币种，而非整个活跃 universe |
| `--limit-symbols` | *(全部)* | 限制检查的币种数量 |
| `--prefix` | 取自配置 / `livebar` | Key 前缀覆盖 |
| `--show-only-failures` | 关 | 只显示异常/缺失/假死的 bar |
| `--redis-host` / `--redis-port` / `--redis-db` / `--redis-password` | 取自配置 | 逐字段覆盖 Redis 连接信息 |

#### 运行示例：
```bash
# 检查全市场实时行情
python cmd/test-tools/check_redis_livebars.py

# 仅显示异常/缺失的币种
python cmd/test-tools/check_redis_livebars.py --show-only-failures
```

退出码：`0` = 每个币种都有新鲜、完整的 live bar；`1` = 存在缺失/假死/字段不全的情况。

---

### 4. Redis 已收盘短期滑窗检查 (`check_redis_closed_windows.py`)

**阶段 B·2。** 启动后第一根 bar 收盘就开始有意义（冷启动的币种靠自然收盘攒够 `window_size` 根可能要等一阵子；如果冷启动回补时 `RebuildWindow` 已经从 ClickHouse 灌满过，则一开始就是满的）。

检查 Redis 中供量化策略引擎消费的已收盘滚动滑窗 (`kline:<SYMBOL>:<interval>`)。

#### 检查项目：
- **滑窗长度**：是否达到期望长度（如生产默认 200 根）。
- **头部新鲜度 (Head Freshness)**：列表首元素 (index 0) 的起始时间戳是否严格对齐当下刚刚收盘的周期边界，是否滞后。
- **严格单调递减**：验证时间戳是否严格从新到旧排列 (`t[0] > t[1] > ...`)。
- **滑窗内部连续无断档**：逐根验证 `t[i] - t[i+1] == interval_ms`，杜绝滑窗内部出现缺失漏 Bar。
- **紧凑 JSON 格式校验**：9 元素紧凑数组 `[t, o, h, l, c, v, qv, tbv, tbqv]` 解析及数值合法性。

#### 参数：
| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `--config` | 自动探测 | `config.yaml` 路径 |
| `--intervals` | `1m` | 逗号分隔的周期；只有基准周期有 Redis 已收盘滑窗——更粗周期在 ClickHouse rollup 表里 |
| `--symbol` | *(全部)* | 只查一个币种，而非整个活跃 universe |
| `--limit-symbols` | *(全部)* | 限制检查的币种数量 |
| `--window-size` | 取自配置 / `200` | 期望滑窗长度覆盖 |
| `--prefix` | 取自配置 / `kline` | Key 前缀覆盖 |
| `--show-only-failures` | 关 | 只显示异常/过短/滞后的滑窗 |
| `--redis-host` / `--redis-port` / `--redis-db` / `--redis-password` | 取自配置 | 逐字段覆盖 Redis 连接信息 |

#### 运行示例：
```bash
# 检查默认（1m）滑窗
python cmd/test-tools/check_redis_closed_windows.py

# 自定义滑窗长度检查（例如设为 100 根）
python cmd/test-tools/check_redis_closed_windows.py --window-size 100
```

退出码：`0` = 每个滑窗都满长、有序、无内部断档且头部新鲜；`1` = 存在过短/乱序/内部断档/头部滞后的情况。

---

### 5. Redis 截面就绪通知监控 (`monitor_redis_kline_ready.py`)

**阶段 B·3。** 这是一个诊断/可观测性工具，不是通过/失败的门禁——除了连接失败会退出 `1`，它本身不判定 PASS/WARN/FAIL，也没有可脚本化的退出码契约；它的延迟/覆盖率数字是给人看的（或接进仪表盘），不建议拿它的退出码当 CI 门禁用。

监听或回溯 `stream:market:kline_ready` 截面通知流，了解聚合器 (Aggregator) 工作状态。

#### 检查项目：
- **聚合就绪时延**：`发布时间 - (K线收盘时间)`。
  - `< 1.0s`：秒级即时齐备 (onComplete)
  - `~ 5.0s`：兜底超时触发 (onTimeout 保护机制，由 `dispatcher.section_timeout` 驱动)
  - `> 10.0s`：网络拥塞或 Worker 阻塞
- **截面标的齐备率**：接收到的标的数量 vs 全市场目标标的数量比例（如 248/250, 99.2%）。
- **截面连续性**：相邻两个事件的 timestamp 差值是否刚好为一个周期间隔。

> `docs/OPERATIONS.md` 里的提醒：只要还有任意一个 `1m` key 被 `windowgate` 持有（冷启动回补，或一次分片重连补缺），整个 interval 的 `kline_ready` 发布就会被压住，而且**不会补发**——重启后或分片重连后这里安静一阵子是预期行为，不一定是 bug。完整机制见 `todo_improvement/cold-start-gate-batch-latency.md`。

#### 参数：
| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `--config` | 自动探测 | `config.yaml` 路径 |
| `--recent` | `20` | 从 Stream 回溯的最近事件数 |
| `--tail` | 关 | 持续监听实时事件，而不是回溯历史 |
| `--max-events` | *(不限)* | `--tail` 模式下，收满这么多事件就停 |
| `--stream-key` | 取自配置 / `stream:market:kline_ready` | Stream key 覆盖 |
| `--redis-host` / `--redis-port` / `--redis-db` / `--redis-password` | 取自配置 | 逐字段覆盖 Redis 连接信息 |

#### 运行示例：
```bash
# 回溯查看最近 20 条已发布的截面就绪事件
python cmd/test-tools/monitor_redis_kline_ready.py --recent 20

# 实时监听模式（收到截面事件立即打印告警与延迟统计）
python cmd/test-tools/monitor_redis_kline_ready.py --tail

# 只监听 50 个事件就停（适合做一次有边界的冒烟测试）
python cmd/test-tools/monitor_redis_kline_ready.py --tail --max-events 50
```

---

### 6. 端到端双写一致性对账 (`e2e_reconciliation.py`)

**阶段 B·4 —— 在线检查里最后一个跑。** 需要 Redis 和 ClickHouse 都已经有新鲜、匹配的最近数据，所以适合在 #3~#5 都已经看起来健康之后，作为"两个存储层彼此是否一致"的收尾检查。

验证 Redis 1m 滑窗缓存与 ClickHouse `fapi_kline_1m` 之间的数据完全一致性。

#### 检查项目：
- 取出 Redis `kline:SYMBOL:1m` 的最多 `--window-size` 根，与 ClickHouse `market.fapi_kline_1m` 降序取相同根数。
- 逐根对齐时间戳 `t`。
- 逐根比较 `Open, High, Low, Close, Volume, QuoteVolume`，浮点差异须在公差内 (`< 1e-5`)。

> 只对 `1m`：更粗周期没有 Redis 窗口。更粗周期的正确性用 `check_vs_binance.py`（第 2 项）对回币安确认。

#### 参数：
| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `--config` | 自动探测 | `config.yaml` 路径 |
| `--intervals` | `1m` | 逗号分隔的周期；只有基准周期有 Redis 窗口 |
| `--symbol` | *(抽样)* | 只查一个币种，而非从活跃 universe 抽样 |
| `--limit-symbols` | `10` | 不指定 `--symbol` 时抽样的币种数 |
| `--window-size` | 取自配置 / `200` | 每个币种对账的 bar 数 |
| `--prefix` | 取自配置 / `kline` | Redis key 前缀覆盖 |
| `--table-prefix` | `market.fapi_kline` | ClickHouse 表前缀 |
| `--redis-host` / `--redis-port` / `--redis-db` / `--redis-password` | 取自配置 | 逐字段覆盖 Redis 连接信息 |
| `--ch-host` / `--ch-port` / `--ch-db` / `--ch-user` / `--ch-password` | 取自配置 | 逐字段覆盖 ClickHouse 连接信息 |

#### 运行示例：
```bash
python cmd/test-tools/e2e_reconciliation.py --limit-symbols 10
python cmd/test-tools/e2e_reconciliation.py --symbol BTCUSDT --window-size 200
```

退出码：`0` = Redis 与 ClickHouse 在所有比对的 bar 上一致；`1` = 任一方发现不匹配或缺失。

---

### 7. 一键全量预检计分卡 (`run_all_checks.py`)

**阶段 C —— 最终门禁，也是外部用户最常直接运行的一个。** 内部按顺序跑第 1、3、4、5、6 项（第 2 项即 `check_vs_binance`，因为慢、又要打外部 REST，默认不跑，加 `--vs-binance` 才跑），汇总成一张红绿灯计分卡并给出发布结论（`READY FOR PRODUCTION DEPLOYMENT` 或 `DEPLOYMENT BLOCKED`）。

#### 参数：
| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `--config` | 自动探测 | `config.yaml` 路径 |
| `--intervals` | `1m` | 面向 Redis 的周期（只有基准周期有 Redis 窗口/live bar） |
| `--ch-intervals` | `1m,5m,15m,1h,4h,1d` | 完整性检查用的 ClickHouse 周期（原始表 + 全部 rollup） |
| `--symbol` | *(每个子检查各自的抽样/全部)* | 把每个子检查都限定到一个币种 |
| `--quick` | 关 | 快速冒烟检查，只抽样前 5 个币种 |
| `--skip-ch` | 关 | 跳过 ClickHouse 完整性检查 |
| `--skip-redis` | 关 | 跳过 Redis 检查（live bar + 已收盘滑窗） |
| `--skip-e2e` | 关 | 跳过端到端对账 |
| `--vs-binance` | 关 | 附带跑 `check_vs_binance.py` 对回 ClickHouse rollup（慢，会打外部交易所——见第 2 项的限速说明） |
| `--verbose` | 关 | 打印每个子检查的完整输出，而不只是摘要 |

#### 运行示例：
```bash
# 快速冒烟预检 (抽查前 5 个核心币种)
python cmd/test-tools/run_all_checks.py --quick

# 完整深度全量预检（ClickHouse + Redis + e2e，不含币安）
python cmd/test-tools/run_all_checks.py

# 完整深度全量预检，附带外部币安真值对账
python cmd/test-tools/run_all_checks.py --vs-binance

# 详细输出子命令详情
python cmd/test-tools/run_all_checks.py --verbose
```

退出码：`0` = 跑过的子检查全部通过；`1` = 至少一项子检查失败。

---

## 预检结论判定规则

| 状态 | 含义 | 建议动作 |
| :--- | :--- | :--- |
| **PASS** (绿色) | 全部指标完美符合生产标准（无断档、数据饱满、时钟对齐） | 允许直接发布上线 |
| **WARN** (黄色) | 存在非关键轻微偏差（如冷门币种交易笔数为 0、刚冷启动滑窗未填满 200 根） | 人工确认原因后可选择性放行 |
| **FAIL** (红色) | 存在严重数据质量问题（K 线断档漏 Bar、价格或时间戳倒错、Redis 头部滞后） | **严禁发布**，排查回补 (Backfill) 或收集器 (Collector) |
