> **Language:** [English](README.md) | 简体中文

# ChomoSyncer 数据校验与线上预检工具包 (Test & Validation Toolkit)

本目录提供一套完整的 Python 脚本工具库，专门用于 **ChomoSyncer-go** 系统在部署上线前、运行期间或断线恢复后的全链路数据完整性、连续性、时效性及多级存储一致性校验。

---

## 目录结构

| 文件 | 说明 |
| :--- | :--- |
| `common.py` | 基础公用库：配置文件加载、ClickHouse/Redis 客户端、交易标的探测、时间工具、表格格式化等 |
| `check_clickhouse_integrity.py` | **ClickHouse K 线完整性检查器**：检测 1m 原始表 + 各 rollup 表的时间戳连续性（缺口/断档判定）与字段非空/有效性校验 |
| `check_redis_livebars.py` | **Redis 未收盘 Live Bar 检查器**：检测各币种 `livebar:{SYM}:1m` Hash 是否存在、TTL 时效、时延与字段完备性 |
| `check_redis_closed_windows.py` | **Redis 已收盘滑窗检查器**：检测 `kline:{SYM}:1m` 200 根滑窗是否存在、单调递减连续性、内部有无漏 Bar、头部是否当下最新 |
| `monitor_redis_kline_ready.py` | **Redis 截面通知 Stream 监控器**：监控 `stream:market:kline_ready`（含派生的粗周期信号），评估就绪时延、标的收盘覆盖率与聚合原因 |
| `e2e_reconciliation.py` | **缓存与存储对账工具**：对比 Redis `kline:{SYM}:1m` 滑窗与 ClickHouse `fapi_kline_1m`，逐根对比时间戳与 OHLCV 一致性 |
| `check_vs_binance.py` | **外部真值对账**：把 ClickHouse（1m 原始表 / rollup 表）逐根对回币安 `/fapi/v1/klines`，抓采集字段映射、单位、rollup 桶对齐与聚合函数的系统性偏差 |
| `run_all_checks.py` | **一键全量预检调度器**：一键执行所有检查项，输出可视化红绿灯健康计分卡 (Scorecard) |
| `test_toolkit.py` | 工具包内置单元测试与 Mock 验证套件 |
| `requirements.txt` | Python 依赖包清单 |

---

## 环境准备与依赖安装

在任意安装有 Python 3.9+ 的环境中执行：

```bash
pip install -r cmd/test-tools/requirements.txt
```

> **提示**：
> - 脚本优先连接 `config.yaml` 或 `config.example.yaml` 中的配置地址。
> - ClickHouse 支持双协议接入：优先通过 `clickhouse-connect`；若未安装 C 扩展，自动无缝降级为 ClickHouse 原生 HTTP 接口（8123 端口），无需担心跨平台编译依赖。

---

## 工具详细使用说明

### 1. ClickHouse 历史 K 线完整性检查 (`check_clickhouse_integrity.py`)

利用 ClickHouse 向量化窗口函数 (`lagInFrame`)，在数秒内扫描全库数亿行 K 线记录，精确找出所有断档时间区间及异常脏数据。

#### 检查项目：
- **时间戳连续性**：计算相邻 K 线的 `diff_ms`，判断是否存在 `> interval_ms` 的断档，并计算累计丢失根数与覆盖率。
- **字段非空与逻辑有效性**：
  - 价格合理性：`open > 0`, `high > 0`, `low > 0`, `close > 0`
  - 价格上下界逻辑：`high >= low`, `high >= max(open, close)`, `low <= min(open, close)`
  - 成交量：`volume >= 0`, `quote_volume >= 0`
  - 成交笔数：`trades_count >= 0`（警告交易笔数为 0 的冷门假数据）
  - 时间戳对齐：`end_time > start_time` 且严格匹配周期间隔。

#### 运行示例：
```bash
# 检查默认配置的所有周期 (1m, 1h) 下全库所有币种
python cmd/test-tools/check_clickhouse_integrity.py

# 指定币种与周期检查
python cmd/test-tools/check_clickhouse_integrity.py --symbol BTCUSDT --intervals 1m

# 指定时间范围检查（如最近一周）
python cmd/test-tools/check_clickhouse_integrity.py --start-date "2026-09-01" --max-gaps 10

# 抽样快速检查 10 个币种
python cmd/test-tools/check_clickhouse_integrity.py --limit-symbols 10
```

---

### 2. Redis 未收盘 Live Bar 检查 (`check_redis_livebars.py`)

检查 Redis 中正在形成的实时未收盘 K 线 Hash (`livebar:<SYMBOL>:<interval>`)。

#### 检查项目：
- **Key 存在性**：全市场活跃币种是否全部在 Redis 中落有 Live Bar。
- **TTL 有效性**：TTL 是否大于 0 且处于合理窗口 (`<= ttl_multiple * interval`)。
- **实时时效性**：`now_ms - t` 是否在当前周期内；若超过 `2 * interval` 则判定为假死 STALE。
- **11 个字段完整性**：`t, o, h, l, c, v, qv, tbv, tbqv, n, x`。

#### 运行示例：
```bash
# 检查全市场实时行情
python cmd/test-tools/check_redis_livebars.py

# 仅显示异常/缺失的币种
python cmd/test-tools/check_redis_livebars.py --show-only-failures
```

---

### 3. Redis 已收盘短期滑窗检查 (`check_redis_closed_windows.py`)

检查 Redis 中供量化策略引擎消费的已收盘滚动滑窗 (`kline:<SYMBOL>:<interval>`)。

#### 检查项目：
- **滑窗长度**：是否达到期望长度（如生产默认 200 根）。
- **头部新鲜度 (Head Freshness)**：列表首元素 (index 0) 的起始时间戳是否严格对齐当下刚刚收盘的周期边界，是否滞后。
- **严格单调递减**：验证时间戳是否严格从新到旧排列 (`t[0] > t[1] > ...`)。
- **滑窗内部连续无断档**：逐根验证 `t[i] - t[i+1] == interval_ms`，杜绝滑窗内部出现缺失漏 Bar。
- **紧凑 JSON 格式校验**：9 元素紧凑数组 `[t, o, h, l, c, v, qv, tbv, tbqv]` 解析及数值合法性。

#### 运行示例：
```bash
# 检查 1m 和 1h 滑窗
python cmd/test-tools/check_redis_closed_windows.py

# 自定义滑窗长度检查（例如设为 100 根）
python cmd/test-tools/check_redis_closed_windows.py --window-size 100
```

---

### 4. Redis 截面就绪通知监控 (`monitor_redis_kline_ready.py`)

监听或回溯 `stream:market:kline_ready` 截面通知流，了解聚合器 (Aggregator) 工作状态。

#### 检查项目：
- **聚合就绪时延**：`发布时间 - (K线收盘时间)`。
  - `< 1.0s`：秒级即时齐备 (onComplete)
  - `~ 5.0s`：兜底超时触发 (onTimeout 保护机制)
  - `> 10.0s`：网络拥塞或 Worker 阻塞
- **截面标的齐备率**：接收到的标的数量 vs 全市场目标标的数量比例（如 248/250, 99.2%）。
- **截面连续性**：相邻两个事件的 timestamp 差值是否刚好为一个周期间隔。

#### 运行示例：
```bash
# 回溯查看最近 20 条已发布的截面就绪事件
python cmd/test-tools/monitor_redis_kline_ready.py --recent 20

# 实时监听模式（收到截面事件立即打印告警与延迟统计）
python cmd/test-tools/monitor_redis_kline_ready.py --tail
```

---

### 5. 端到端双写一致性对账 (`e2e_reconciliation.py`)

验证 Redis 1m 滑窗缓存与 ClickHouse `fapi_kline_1m` 之间的数据完全一致性。

#### 检查项目：
- 取出 Redis `kline:SYMBOL:1m` 的 200 根与 ClickHouse `market.fapi_kline_1m` 降序前 200 根。
- 逐根对齐时间戳 `t`。
- 逐根比较 `Open, High, Low, Close, Volume, QuoteVolume`，浮点差异须在公差内 (`< 1e-5`)。

> 只对 `1m`：更粗周期没有 Redis 窗口。更粗周期的正确性用 `check_vs_binance.py` 对回币安。

#### 运行示例：
```bash
python cmd/test-tools/e2e_reconciliation.py --limit-symbols 10
python cmd/test-tools/e2e_reconciliation.py --symbol BTCUSDT --window-size 200
```

---

### 5b. ClickHouse 与币安外部真值对账 (`check_vs_binance.py`)

`e2e_reconciliation.py` 是"管道跟自己比"（Redis 与 ClickHouse 都是同一条链路写入的）。本工具把 ClickHouse 逐根对回**币安官方** `/fapi/v1/klines`，是唯一能发现 1m 采集或 rollup 聚合**系统性偏差**的检查。

#### 检查项目：
- `start_time` 必须精确对齐（桶对齐 / 采集时间基准）。
- OHLC 相对容差 `1e-9`（`argMin/argMax/min/max` → 精确相等）。
- `volume / quote_volume / taker_buy_*` 相对容差 `1e-6`（浮点求和顺序差异，约 1e-8）。
- `trades_count` 必须精确相等（整数求和）。

#### 运行示例：
```bash
# 1m 原始表 vs 币安 1m —— 验证采集字段映射/单位
python cmd/test-tools/check_vs_binance.py --intervals 1m --limit-symbols 10

# rollup 表 vs 币安 —— 验证 Phase B 的桶对齐与聚合函数
python cmd/test-tools/check_vs_binance.py --intervals 1h,4h,1d --symbol BTCUSDT --bars 96
```

---

### 6. 一键全量预检计分卡 (`run_all_checks.py`)

在执行正式发布、版本上线前，一键运行上述全部检查，自动汇总生成全景预检表格并给出发布通行结论 (`READY FOR PRODUCTION DEPLOYMENT` 或 `DEPLOYMENT BLOCKED`)。

#### 运行示例：
```bash
# 快速冒烟预检 (抽查前 5 个核心币种)
python cmd/test-tools/run_all_checks.py --quick

# 完整深度全量预检
python cmd/test-tools/run_all_checks.py

# 详细输出子命令详情
python cmd/test-tools/run_all_checks.py --verbose
```

---

## 预检结论判定规则

| 状态 | 含义 | 建议动作 |
| :--- | :--- | :--- |
| **PASS** (绿色) | 全部指标完美符合生产标准（无断档、数据饱满、时钟对齐） | 允许直接发布上线 |
| **WARN** (黄色) | 存在非关键轻微偏差（如冷门币种交易笔数为 0、刚冷启动滑窗未填满 200 根） | 人工确认原因后可选择性放行 |
| **FAIL** (红色) | 存在严重数据质量问题（K 线断档漏 Bar、价格或时间戳倒错、Redis 头部滞后） | **严禁发布**，排查回补 (Backfill) 或收集器 (Collector) |
