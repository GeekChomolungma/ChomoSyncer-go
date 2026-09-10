# ChomoSyncer-go

高性能、低延迟的 **Binance U 本位永续合约 (USDⓈ-M Futures)** 全市场 K 线实时流式采集、同步与多级分发系统。

系统通过确定性 WebSocket 分片并行接入币安行情流，配合带有令牌桶限流与窗口门控的历史回补机制，实现毫秒级行情分发、无缺口时序落库与全市场横截面对齐，为下游量化投研系统与实盘策略引擎提供标准化、只读的事实数据底座。

---

## 核心数据产出与多级接口语义

系统只从币安采集 **1 分钟基准 K 线**，所有更粗周期（`5m/15m/1h/4h/1d`…）都由它派生：ClickHouse 端用物化视图 rollup 落成 `fapi_kline_<interval>` 表，实时链路只把 `kline_ready` 截面信号派生转发。因此 Redis 只保留 1m 的实时接口，更粗周期一律查 ClickHouse。

| 数据出口 / 接口 | 存储介质与键结构 | 数据形态与协议 | 核心业务语义与应用场景 |
| :--- | :--- | :--- | :--- |
| **1m 时序事实归档** | **ClickHouse**<br>`market.fapi_kline_1m` | ReplacingMergeTree 物理表<br>以 `(symbol, start_time)` 去重 | **全局权威事实账本**。唯一直接落库的原始表，保证绝对零缺口。 |
| **派生周期归档** | **ClickHouse**<br>`market.fapi_kline_{5m,15m,1h,4h,1d}` | ReplacingMergeTree + 刷新式物化视图<br>由 `fapi_kline_1m FINAL` 重算聚合 | **多周期回测/特征库**。从 1m 幂等重算（`deploy/clickhouse/002_kline_rollups.sql`），永不累加、不会重复计数。 |
| **实时未收盘快照** | **Redis Hash**<br>`livebar:{SYMBOL}:1m` | Hash 结构（11 字段）<br>`t, o, h, l, c, v, qv, tbv, tbqv, n, x` | **未收盘瞬态形态读取**。仅 1m。毫秒级刷新当前跳动的这根 K 线，带自动过期 TTL。 |
| **已收盘滚动滑窗** | **Redis List**<br>`kline:{SYMBOL}:1m` | List 列表（定长 200 根）<br>9 元素紧凑 JSON 数组 (无 Key) | **策略特征极速计算缓存**。仅 1m，最近 200 根已闭合 Bar。更粗周期请直接查 ClickHouse rollup 表。 |
| **截面就绪通知** | **Redis Stream**<br>`stream:market:kline_ready` | Stream 流事件<br>`interval, timestamp, symbols_count` | **跨币种截面策略同步触发器**。1m 截面到齐时发布；`serve_intervals` 中每个周期在其“桶末 1m 截面”就绪时派生转发一条（`interval` 标注为该粗周期）。 |

---

## 快速启动 (Quick Start)

### 模式 1：Docker Compose 容器化启动（推荐快速体验与联调）

通过根目录的一键编排，可快速拉起 ClickHouse、Redis 并自动构建启动采集服务：

```bash
# 1. 启动底层依赖 (ClickHouse + Redis)，首次启动自动执行建表 DDL
docker compose -f deploy/docker-compose.yml up -d

# 检查依赖健康状态（确认两者均为 healthy）
docker compose -f deploy/docker-compose.yml ps

# 2. 启动采集服务（构建并运行采集器容器）
docker compose -f deploy/docker-compose.yml --profile app up -d --build

# 3. 查看实时采集日志
docker compose -f deploy/docker-compose.yml logs -f chomosyncer-go

# 4. 停机与清理
docker compose -f deploy/docker-compose.yml --profile app down
```

---

### 模式 2：常规二进制裸机部署（生产部署推荐）

#### 1. 编译构建
```bash
# 静态编译无 CGO 依赖的二进制文件
CGO_ENABLED=0 go build -trimpath   -ldflags "-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo 'v1.0.0')"   -o bin/chomosyncer-cmd ./cmd/chomosyncer-cmd
```

#### 2. 初始化 ClickHouse 表结构
```bash
# 001：建库 market 与唯一原始表 fapi_kline_1m
clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < deploy/clickhouse/001_fapi_kline.sql
# 002：建 5m/15m/1h/4h/1d rollup 表 + 刷新式物化视图（需要 ClickHouse ≥ 24.8）
clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < deploy/clickhouse/002_kline_rollups.sql
# 003：仅在“已有历史 1m 数据”时运行一次，把历史折叠进 rollup 表（幂等，详见文件头与两阶段冷启动）
```

#### 3. 配置文件启动
从配置模板生成生产配置并运行：
```bash
# 复制配置文件模板
cp config.example.yaml config.yaml

# 启动服务（系统自动查找 config.yaml）
./bin/chomosyncer-cmd -config config.yaml

# 可通过命令行 Flag 灵活覆盖配置项（例如调试模式启动）
./bin/chomosyncer-cmd -config config.yaml -log-level debug -intervals 1m,1h
```

#### 4. 健康状态校验
```bash
curl -i http://localhost:9090/healthz   # 进程存活检查 (HTTP 200)
curl -i http://localhost:9090/readyz    # 全链路就绪检查 (冷启动回补完成返回 HTTP 200)
```

#### 5.（可选）两阶段冷启动：先离线灌满历史，再上线实时业务

当 `cold_start_date` 设为很久远的时间时，实时链路的冷启动门控存在 `gate_timeout` 兜底释放，无法保证"历史全部落库后才开启 Redis 业务"。推荐把深历史作为独立的离线阶段先跑完：

```bash
# 阶段一：仅离线回补。只把 1m 历史灌入 ClickHouse fapi_kline_1m，跑完即退出（exit 0），不启动任何实时链路。
#         被中断也没关系，进度已落盘，重跑会从 max(start_time) 续上。
./bin/chomosyncer-cmd -config config.yaml -backfill-offline-only -backfill-start-date 2024-01-01

# 建 rollup 表 + 刷新式物化视图（此后 MV 只吃新到的实时 1m 行）
clickhouse-client --multiquery < deploy/clickhouse/002_kline_rollups.sql
# 把阶段一已灌好的 1m 历史一次性折叠进各 rollup 表（幂等，可重跑）
clickhouse-client --param_m_start='2000-01-01 00:00:00' --param_m_end='2099-01-01 00:00:00' \
  --queries-file deploy/clickhouse/003_rollup_backfill.sql

# 校验历史零断档 + 与币安对账
python cmd/test-tools/check_clickhouse_integrity.py
python cmd/test-tools/check_vs_binance.py --intervals 1m,1h,4h

# 阶段二：启动完整在线业务。此时 cold_start_date 只需覆盖最近少量窗口（如 7 天），冷启动秒级完成。
./bin/chomosyncer-cmd -config config.yaml
```

也可在 `config.yaml` 中用 `backfill.offline_only: true` 开启（默认 `false`）。详见 [`docs/OPERATIONS.md`](docs/OPERATIONS.md)。

---

## 架构与设计细节文档导航

关于系统各模块的底层实现细节、数据协议与运维测试，请参阅 `docs/` 下的专项技术文档：

- 📖 **[系统模块架构与设计详解 (`docs/ARCHITECTURE_MODULES.md`)](docs/ARCHITECTURE_MODULES.md)**  
  详细讲解各核心模块的设计：
  - `internal/chwriter`：ClickHouse 高性能批量写入队列与重试退避；
  - `internal/rediswin`：原子 Lua 单调滑窗、LiveBar 独立协程池与 Stream 发布；
  - `internal/collector`：Binance WebSocket 确定性哈希分片、错峰建连与 Staleness 看门狗；
  - `internal/universe`：全市场 USDT 永续合约动态发现与每日 UTC 对齐机制；
  - `internal/dispatcher`：四路事件分发、单调防护与双触发截面聚合器；
  - `internal/backfill` & `internal/windowgate`：冷启动与断线补缺的三大规则、窗口门控与 REST 令牌桶限流。

- 📊 **[数据流转与存储结构业务全景 (`docs/DATA_FLOW_AND_STRUCTURES.md`)](docs/DATA_FLOW_AND_STRUCTURES.md)**  
  端到端数据流拓扑图、各工作流并发模型、外部存储（Redis / ClickHouse）查询命令示例、返回 JSON 格式及各字段业务定义。

- 🛠️ **[部署运维、故障演练与实操测试手册 (`docs/OPERATIONS.md`)](docs/OPERATIONS.md)**  
  从环境要求、容量规划、Prometheus 告警规则配置，到断网演练、服务熔断与恢复的完整实操指南。

- 🧪 **[全链路预检与数据验证工具包 (`cmd/test-tools/README.md`)](cmd/test-tools/README.md)**  
  上线前专用的 Python 自动化排查套件：
  - `check_clickhouse_integrity.py`：基于窗口函数扫描全表检测历史漏 Bar 断档与数据质量；
  - `check_redis_livebars.py`：巡检实时 Live Bar 存在性、TTL 与时延；
  - `check_redis_closed_windows.py`：验证 200 根滑窗列表完整度、头部时效与滑窗内连续性；
  - `monitor_redis_kline_ready.py`：实时监控与回溯截面就绪通知流时延；
  - `e2e_reconciliation.py`：Redis 1m 滑窗与 ClickHouse 逐根对账；
  - `check_vs_binance.py`：把 ClickHouse（1m 原始表 / rollup 表）逐根对回币安 REST，抓采集与聚合的系统性偏差；
  - `run_all_checks.py`：上线前一键综合评分卡（加 `--vs-binance` 纳入外部对账）。

---

## 许可证

本项目遵循 MIT 开源许可证。
