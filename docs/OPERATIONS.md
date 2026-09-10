# ChomoSyncer-go 部署运维与全链路测试实操手册

本文档为 `ChomoSyncer-go` 的官方运维部署、故障演练、性能压测及线上预检实操指南。

---

## 1. 架构拓扑与端口矩阵

```text
  [Binance Futures WS/REST]
             │ (入向行情)
             ▼
    ┌─────────────────┐
    │  ChomoSyncer-go │◄─────── :9090 (/metrics, /healthz, /readyz)
    └──┬────────────┬─┘
       │            │
       │ TCP 9000   │ TCP 6379
       ▼            ▼
┌─────────────┐  ┌─────────────┐
│ ClickHouse  │  │    Redis    │
│  (时序归档) │  │ (滑窗/Live) │
└─────────────┘  └─────────────┘
 (HTTP: 8123)
```

### 端口矩阵
| 服务组件 | 监听端口 | 协议 | 访问来源 | 作用说明 |
| :--- | :--- | :--- | :--- | :--- |
| **ChomoSyncer-go** | `9090` | HTTP | Prometheus / K8s 探针 | 提供 `/metrics`、`/healthz` (存活)、`/readyz` (就绪) |
| **ClickHouse Native**| `9000` | TCP (Native)| ChomoSyncer-go | 高性能列式批量写入通道 |
| **ClickHouse HTTP**  | `8123` | HTTP | 测试脚本 / 运维查询 | 供运维人员或 Python 验证工具查询 |
| **Redis**            | `6379` | RESP | ChomoSyncer-go / 策略层 | 存储 Live Bar Hash、200 根滑窗 List、截面就绪 Stream |

---

## 2. 前置环境要求

| 依赖组件 | 最低版本 | 推荐版本 | 说明 |
| :--- | :--- | :--- | :--- |
| **Go** | ≥ 1.25 | 1.25+ | 编译构建生产二进制 (`CGO_ENABLED=0`) |
| **ClickHouse** | ≥ 24.8 | 24.8+ | ReplacingMergeTree + 刷新式物化视图（rollup 派生周期，见 `002_kline_rollups.sql`）。无刷新式 MV 时可退回 cron 方案 |
| **Redis** | ≥ 6.2 | 7.0+ (Alpine) | 建议禁用持久化，配置 `noeviction` |
| **Python** | ≥ 3.9 | 3.10+ | 运行 `cmd/test-tools` 预检工具包 |
| **网络要求** | 出网连接 | 低延迟直连 | 需稳定访问 `fstream.binance.com` 与 `fapi.binance.com` |

---

## 3. 快速启动指南

按**是否已有 ClickHouse 历史数据**分两条线,每条线又分 Docker(方法 A)和裸机(方法 B):

| 场景 | 方法 A：Docker Compose（本地 / 测试） | 方法 B：二进制裸机（生产向） |
| :--- | :--- | :--- |
| **空库首次上线**（建表 → 离线灌历史 → 重启订阅） | §A.1 | §B.1 |
| **已有库**（日常启动 / 重启 / 维护） | §A.2 | §B.2 |

> **配置优先级**（低 → 高）：内置默认值 `<` `config.yaml` `<` `CHOMOSYNCER_*` 环境变量 `<` 显式命令行 flag。两种方法都以根目录 `config.yaml` 为准(Docker 也是把同一份挂进容器)。详见本节末 §3.3。

---

### 方法 A：Docker Compose 模式

#### A.0 这几个文件是什么关系

| 文件 | 作用 |
| :--- | :--- |
| **`Dockerfile`**（仓库根目录） | 只负责**把 Go 源码编译成一个 `chomosyncer-go` 镜像**（多阶段:`golang:1.25` 编译 → `distroless/static` 运行，非 root）。镜像里**只有采集器二进制**，没有 Redis、没有 ClickHouse、没有配置文件。不单独手动用,由 compose 调用。 |
| **`deploy/docker-compose.yml`** | 编排三个容器:`clickhouse`、`redis`、`chomosyncer-go`。`chomosyncer-go` 服务的 `build:` 指向根 `Dockerfile`,`profiles: ["app"]` 让它默认不启动(要 `--profile app`)。 |
| **`deploy/clickhouse/001_fapi_kline.sql`** | 挂进 `clickhouse` 容器的 `/docker-entrypoint-initdb.d/`,**首次启动自动执行**(建 `market` 库 + 原始表 `fapi_kline_1m`)。 |
| **`deploy/clickhouse/002` / `003`** | 通过 `./clickhouse:/clickhouse:ro` 只读挂进容器,但**不自动跑**——rollup 表 + 物化视图,由你在正确时机手动执行。 |
| **根目录 `config.yaml`** | **配置的唯一来源**。compose 把它只读挂进 `chomosyncer-go` 容器(`../config.yaml → /etc/chomosyncer-go/config.yaml`),和裸机跑法一模一样。**必须先存在**,否则 Docker 会把挂载点建成空目录,采集器崩溃刷 `read config file: ... is a directory`。 |
| **`deploy/chomosyncer-go.env.example`** | 仅作参考——列出所有可用的 `CHOMOSYNCER_*` 变量。compose **默认不读它**(YAML 优先)。 |

一句话:**`Dockerfile` = 编译打包;`docker-compose.yml` = 把采集器 + Redis + ClickHouse 拼成一套栈;配置就是那份 `config.yaml`,Docker 和裸机共用同一份。**

#### A.1：空库首次上线（建表 → 离线灌历史 → 重启订阅）

**为什么要分两步而不是一把启动**：在线模式下,冷启动历史回补由 `windowgate` 门控挡在 Redis 业务(`kline:*` 滑窗 + `kline_ready` 截面)之前,但门控受 `backfill.gate_timeout`(默认 `10m`)兜底释放。如果 `cold_start_date` 设成几个月甚至一年前,全市场回补远超这个时长 → 门控**提前释放**,实时 K 线开始写 Redis,而中间那段深历史缺口会"污染" ClickHouse 的 `max(start_time)`,重启也不会自动重补(成为粘性缺口)。所以把深历史当一个**独立的离线阶段**先灌满,校验后再起在线业务。

```bash
# 0. 生成配置(compose 会挂进容器)。redis/clickhouse 地址不用改,compose 会覆盖成容器名。
#    把 cold_start_date 设成你要的历史起点,例如 "2024-01-01"。
cp config.example.yaml config.yaml

# 1. 起底层依赖。ClickHouse 首次启动只自动跑 001(建 fapi_kline_1m)。
docker compose -f deploy/docker-compose.yml up -d
docker compose -f deploy/docker-compose.yml ps            # 等两者都 healthy

# 2. 离线灌历史:一次性拉全市场 1m 历史进 fapi_kline_1m,跑完容器自己退出。
#    - 只装配 universe→REST→ClickHouse 这条链路,不启 WS / dispatcher / Redis;
#    - gate_timeout 自动失效,不会中途被强制释放;
#    - 退出码 0 = 跑完;非 0 = 被中断,但进度已落盘,重跑会从 max(start_time) 续上;
#    - 进度看 /metrics 的 backfill_bars_fetched_total / _written_total / _errors_total。
docker compose -f deploy/docker-compose.yml run --rm chomosyncer-go \
  -backfill-offline-only

# 3. 历史灌完后,再建 rollup 层 + 折叠历史。
#    此刻才建 MV → 刷新式物化视图只会跟新到的实时 1m,不会和上一步的大回补抢时间。
docker compose -f deploy/docker-compose.yml exec clickhouse \
  clickhouse-client --queries-file /clickhouse/002_kline_rollups.sql
docker compose -f deploy/docker-compose.yml exec clickhouse clickhouse-client \
  --param_m_start='2000-01-01 00:00:00' --param_m_end='2099-01-01 00:00:00' \
  --queries-file /clickhouse/003_rollup_backfill.sql

# 4. 校验(必须全绿再进下一步)。
python cmd/test-tools/check_clickhouse_integrity.py --intervals 1m,5m,15m,1h,4h,1d
python cmd/test-tools/check_vs_binance.py --intervals 1m,1h,4h

# 5. 把 config.yaml 的 cold_start_date 收窄到最近 7 天(够填满 200 根 1m 滑窗 + 余量),
#    起在线服务。此时在线冷启动秒级完成,永远撞不到 gate_timeout,rollup MV 自动跟上。
docker compose -f deploy/docker-compose.yml --profile app up -d --build
docker compose -f deploy/docker-compose.yml logs -f chomosyncer-go
curl -s localhost:9090/readyz
```

> ClickHouse **< 24.8** 不支持刷新式物化视图:把 `002` 里所有 `CREATE MATERIALIZED VIEW` 段删掉,改用 cron / systemd-timer 每 1–2 分钟跑一次 `003`(滚动窗口,见 `003` 文件头 "CRON FALLBACK")。`003` 的重算是幂等的,怎么跑都不会重复累加。

#### A.2：已有库（日常启动 / 重启 / 维护）

库里已有 `fapi_kline_1m` 数据、rollup 表也建过,以后每次启动就是一条命令:

```bash
docker compose -f deploy/docker-compose.yml --profile app up -d --build
```

- **无需再跑 001/002/003**。`001` 只在 ClickHouse 数据卷为空时执行;`002` 的 MV 已存在;`003` 只在"新灌了一批历史 1m"后才需要补跑。
- **断线/重启的缺口自动补**:采集器启动时读 `fapi_kline_1m` 的 `max(start_time)`,从那里续上回补,`/readyz` 在补完前返回 503。`config.yaml` 的 `cold_start_date` 保持"最近 7 天"即可(有历史时它只作为空库兜底,不生效)。
- **改配置**:直接编辑根目录 `config.yaml` → `--profile app up -d` 重建容器。不需要第二份配置;唯一的容器专属项是 compose 写死的两个地址(`redis:6379` / `clickhouse:9000`)。
- **加了新的 `serve_intervals`**:先 `docker compose ... exec clickhouse clickhouse-client --queries-file /clickhouse/002_kline_rollups.sql`(`002` 用 `CREATE ... IF NOT EXISTS`,只新增缺的表/视图),需要历史再按需跑 `003`,然后重启采集器。
- **停机**:`docker compose -f deploy/docker-compose.yml --profile app down`(加 `-v` 连 ClickHouse 数据卷一起清空——慎用)。

---

### 方法 B：常规二进制裸机部署（生产向）

#### B.0：编译

```bash
CGO_ENABLED=0 go build -trimpath \
  -ldflags "-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo 'v1.0.0')" \
  -o bin/chomosyncer-go ./cmd/chomosyncer-go
```

自备 ClickHouse (≥ 24.8) 与 Redis (建议 `--maxmemory 1gb --maxmemory-policy noeviction --save ''`)。

#### B.1：空库首次上线（建表 → 离线灌历史 → 重启在线业务）

分两步的理由同 §A.1（深历史必须先离线灌满,否则 `gate_timeout` 提前释放门控会留下粘性缺口）。

```bash
# 1. 生成配置,填好 redis / clickhouse 地址,cold_start_date 设为历史起点(如 "2024-01-01")。
cp config.example.yaml config.yaml

# 2. 建原始表。
clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < deploy/clickhouse/001_fapi_kline.sql

# 3. 离线灌历史:拉全市场 1m 历史进 fapi_kline_1m,跑完即退出。
#    行为同 A.1 步骤 2:只装配 REST→ClickHouse,不启 WS/Redis;gate_timeout 失效;
#    退出码 0=完成 / 非 0=中断但可重跑续上;进度看 /metrics 的 backfill_bars_*。
./bin/chomosyncer-go -config config.yaml -backfill-offline-only

# 4. 历史灌完后,建 rollup 层 + 折叠历史。
clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < deploy/clickhouse/002_kline_rollups.sql
clickhouse-client --host 127.0.0.1 --port 9000 \
  --param_m_start='2000-01-01 00:00:00' --param_m_end='2099-01-01 00:00:00' \
  --queries-file deploy/clickhouse/003_rollup_backfill.sql

# 5. 校验。
curl -s 'http://127.0.0.1:8123/?query=SHOW+TABLES+FROM+market'
# 应返回: fapi_kline_1m / _5m / _15m / _1h / _4h / _1d (+ *_rmv 物化视图)
python cmd/test-tools/check_clickhouse_integrity.py --intervals 1m,5m,15m,1h,4h,1d
python cmd/test-tools/check_vs_binance.py --intervals 1m,1h,4h

# 6. 把 config.yaml 的 cold_start_date 收窄到最近 7 天,起在线业务。
./bin/chomosyncer-go -config config.yaml
curl -i http://localhost:9090/readyz     # 回补补完后转 200
```

> ClickHouse **< 24.8**:同 A.1 备注,`002` 去掉 `CREATE MATERIALIZED VIEW`,改 cron 跑 `003`。

#### B.2：已有库（重启 / 后续维护）

```bash
./bin/chomosyncer-go -config config.yaml            # 就这一条
```

- **不用再跑 001/002/003**。断线期间的缺口在启动时从 `fapi_kline_1m` 的 `max(start_time)` 自动续补,`/readyz` 在补完前 503。
- 建议用 systemd 托管(`Restart=on-failure`、`ExecStart=/opt/chomosyncer-go/bin/chomosyncer-go -config /opt/chomosyncer-go/config.yaml`),`SIGTERM` 会触发优雅停机排空落盘。
- **改配置**:改 `config.yaml` 后重启进程即可;个别项也可用 `-flag` 或 `CHOMOSYNCER_*` 临时覆盖(优先级更高)。
- **加了新的 `serve_intervals`**:重新执行 `002`(`IF NOT EXISTS`,只补缺的),需要历史再跑 `003`,然后重启进程。

---

### 3.3 配置来源与深度参数

优先级(低 → 高):**内置 `DefaultConfig()`  <  `config.yaml`  <  `CHOMOSYNCER_*` 环境变量  <  显式命令行 flag**。`config.example.yaml` 里每一项都有内置生产级默认值,不写也能跑。

- **裸机**:`-config config.yaml` 就是全部;要临时压某项用 `-flag` 或 `CHOMOSYNCER_*`。
- **Docker**:只维护根目录 `config.yaml` 一份(挂进容器);compose 里写死的两个地址 env(`CHOMOSYNCER_REDIS_ADDR=redis:6379` / `CHOMOSYNCER_CH_ADDR=clickhouse:9000`)是仅有的容器专属覆盖——因为 `config.yaml` 里是 `localhost`,容器网络里不通。想在容器里再压某项,往 `chomosyncer-go` 服务的 `environment:` 加对应 `CHOMOSYNCER_*`(规则:flag `-redis-pool-size` ↔ env `CHOMOSYNCER_REDIS_POOL_SIZE`;全量 `chomosyncer-go -h`)。
- **想彻底用环境变量不用 YAML**:在 `docker-compose.yml` 去掉 `config.yaml` 挂载、取消注释 `env_file:`(指向从 `chomosyncer-go.env.example` 复制的 `deploy/chomosyncer-go.env`)。
- **只能写 YAML、没有环境变量双胞胎的参数**:`redis.dial_timeout` / `read_timeout` / `write_timeout`、`redis.window.stream_maxlen` / `key_prefix` / `atomic`、`redis.live.ttl_multiple` / `default_ttl` / `write_timeout` / `key_prefix`、`clickhouse.table_prefix` / `dial_timeout` / `tls` / `max_retries` / `retry_backoff` / `max_retry_backoff` / `shutdown_timeout`、`collector.connect_stagger` / `connect_timeout` / `reconnect_base` / `reconnect_max` / `watchdog_interval`、`dispatcher.publish_timeout`、`universe.refresh_offset` / `http_timeout` / `contract_type` / `status`、`backfill.queue_size`、`app.shutdown_timeout`。

---

## 4. 探针与就绪检查

服务启动后，内置 HTTP Server（默认 `:9090`）暴露三个关键端点：

1. **存活探针 (Liveness Probe)**：
   ```bash
   curl -i http://localhost:9090/healthz
   # 预期返回: HTTP 200 OK
   ```
2. **就绪探针 (Readiness Probe)**：
   ```bash
   curl -i http://localhost:9090/readyz
   # 冷启动历史回补未完成时返回: HTTP 503 Service Unavailable (Backfill in progress)
   # 回补完成且全链路就绪后返回: HTTP 200 OK
   ```
3. **监控指标采集 (Prometheus Metrics)**：
   ```bash
   curl -s http://localhost:9090/metrics | grep -E "ws_shards_active|universe_size|redis_bars_pushed_total|clickhouse_rows_flushed_total"
   ```

---

## 5. 自动化预检与数据验证框架 (Python Test Toolkit)

项目在 [`cmd/test-tools/`](../cmd/test-tools/) 下内置了一套开箱即用的自动化测试工具包，可在部署上线前对全链路数据进行无死角排查。

### 依赖安装
```bash
pip install -r cmd/test-tools/requirements.txt
```

### 5.1 ClickHouse 历史数据连续性与字段质量检查
```bash
# 检查 1m 原始表 + 全部 rollup 表是否连续、有无断档、字段是否饱满（默认 1m,5m,15m,1h,4h,1d）
python cmd/test-tools/check_clickhouse_integrity.py

# 针对特定主力币种深度检查
python cmd/test-tools/check_clickhouse_integrity.py --symbol BTCUSDT --show-all-gaps
```
- **核心判定**：自动利用 ClickHouse 窗口函数扫描相邻 K 线时间戳差值，排查断档（Gap）；同时校验 `open/high/low/close > 0`、`high >= low`、`volume >= 0`、`trades_count >= 0`。

### 5.2 Redis 未收盘 Live Bar 实时检查
```bash
python cmd/test-tools/check_redis_livebars.py   # 仅 1m：只有基准周期有 livebar
```
- **核心判定**：扫描全市场 `livebar:{SYMBOL}:1m`，验证 Key 存在性、TTL（`2 * interval`）、时延时效（防止假死）以及 11 个 Hash 字段的完备性。

### 5.3 Redis 已收盘滚动滑窗检查
```bash
python cmd/test-tools/check_redis_closed_windows.py   # 仅 1m：更粗周期不建 Redis 窗口，改查 ClickHouse rollup 表
```
- **核心判定**：检查 `kline:{SYMBOL}:1m` 列表长度是否达到期望的 200 根，头部时间戳是否紧跟当前收盘边界，且滑窗内部每两根 Bar 之间严格连续无漏根。

### 5.4 截面通知 Stream 监控与回溯
```bash
# 查看最近 20 个截面发布事件的时延与标的齐备率
python cmd/test-tools/monitor_redis_kline_ready.py --recent 20

# 实时监听模式（流式打印每个周期的截面就绪耗时与到达标的数）
python cmd/test-tools/monitor_redis_kline_ready.py --tail
```

### 5.5 Redis 与 ClickHouse 双写一致性对账
```bash
python cmd/test-tools/e2e_reconciliation.py --limit-symbols 20   # 仅 1m：Redis 只有 1m 窗口
```
- **核心判定**：将 Redis `kline:{SYMBOL}:1m` 滑窗与 ClickHouse `fapi_kline_1m` 最新落盘逐根比对，时间戳与 OHLCV 必须 100% 吻合（容差 `< 1e-5`）。

### 5.6 ClickHouse 与币安外部真值对账（抓采集/聚合的系统性偏差）
```bash
# 1m 原始表 vs 币安 1m —— 验证采集字段映射/单位没错
python cmd/test-tools/check_vs_binance.py --intervals 1m

# rollup 表 vs 币安 —— 验证 Phase B 的桶对齐与聚合函数
python cmd/test-tools/check_vs_binance.py --intervals 1h,4h,1d
```
- **核心判定**：逐根对回币安 `/fapi/v1/klines`。`start_time` 必须精确对齐；OHLC 相对容差 `1e-9`（≈精确）；volume 类字段相对容差 `1e-6`（浮点求和顺序差异）；`trades_count` 必须精确相等。
- `e2e_reconciliation.py` 是“管道跟自己比”，只有这一项能发现 1m 采集或 rollup 的系统性错误。

### 5.7 上线前一键综合评分
```bash
# 快速冒烟扫描
python cmd/test-tools/run_all_checks.py --quick

# 深度全量扫描（含币安外部对账）
python cmd/test-tools/run_all_checks.py --vs-binance
```
- 控制台将打印全红绿灯 Scorecard，并给出最终发布决策：`READY FOR PRODUCTION DEPLOYMENT` 或 `DEPLOYMENT BLOCKED`。

---

## 6. 故障演练与恢复实操 (Chaos & Resilience Testing)

为验证高可用性，建议在测试环境中进行以下演练：

### 演练 1：WebSocket 突发网络中断与自动补缺
1. 观察 `livebar` 与滑窗正常更新；
2. 拔掉外网连接或使用防火墙阻断币安 WebSocket 端口 30 秒；
3. 恢复网络；
4. **预期表现**：
   - Staleness 看门狗触发断线重连；
   - 重连成功后触发 `collector.OnGap` 补缺事件；
   - 窗口门控 `windowgate.Hold` 挂起受影响币种的收盘写；
   - Backfill 模块自动从 ClickHouse 最新时间戳开始向币安 REST 补齐缺失数据；
   - 补全后重新加载 ClickHouse 尾部重建 Redis 滑窗并释放门控；
   - 运行 `python cmd/test-tools/check_clickhouse_integrity.py` 验证零断档。

### 演练 2：ClickHouse 临时停机与写入缓冲退避
1. 停止 ClickHouse 容器：`docker stop chomo-clickhouse`；
2. 观察 ChomoSyncer 日志：`BatchWriter` 触发指数退避重试，行数在内存队列积蓄；
3. 15 秒内重新启动 ClickHouse：`docker start chomo-clickhouse`；
4. **预期表现**：连接自愈，积压的 K 线数据批量成功落盘，`clickhouse_rows_dropped_total` 保持为 0。

---

## 7. 容量估算与系统调优

### 7.1 标的规模与吞吐
- **全市场规模**：约 300–400 个 USDT 永续合约。
- **1m 周期吞吐**：每分钟产生 300–400 根闭合 Bar；每秒约 5–10 根。
- **Live Bar 吞吐**：每秒约 500–2,000 次瞬态更新。

### 7.2 Redis 资源与策略
- **内存占用**：
  - 每个闭合滑窗约 200 根 × 9 元素 ≈ 30 KB；
  - 400 币种 × 2 周期 (1m, 1h) ≈ 25 MB；
  - Live Bar 快照 400 币种 × 2 周期 ≈ 2 MB；
  - 全量 Redis 稳态运行内存通常小于 **100 MB**。
- **关键配置**：
  - 务必设置 `--maxmemory 1gb` 并配置 `--maxmemory-policy noeviction`，宁可写入报错也不允许淘汰滑窗。

### 7.3 ClickHouse 存储增长预估
- 单条 K 线压缩后存储约为 **30–40 Bytes**；
- 400 个币种：
  - `1m` 表每月增长：`400 * 60 * 24 * 30 * 35 B ≈ 600 MB / 月`；
  - `1h` 表每月增长：`400 * 24 * 30 * 35 B ≈ 10 MB / 月`；
- 年存储增量小于 **10 GB**，低配磁盘即可长期稳定承载。

---

## 8. 生产发布检查清单 (Go-Live Checklist)

- [ ] ClickHouse 建表完成，分区与 ReplacingMergeTree 确认无误；
- [ ] Redis 已启动且配置 `noeviction`，连接池与密码配置正确；
- [ ] 配置文件 `config.yaml` 或环境变量已配置生产环境地址；
- [ ] 执行 `cmd/chomosyncer-go`，确认日志无 ERROR 报错；
- [ ] 检查 `/healthz` 与 `/readyz` 探针均返回 HTTP 200；
- [ ] 运行 `python cmd/test-tools/run_all_checks.py` 得到全绿 PASS 结论；
- [ ] Prometheus 正常抓取 `/metrics`，配置了 `ws_connection_status` 和 `ws_last_message_age_seconds` 报警规则。
