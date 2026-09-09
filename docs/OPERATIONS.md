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
| **ClickHouse** | ≥ 23.8 | 24.8+ | 必须启用 ReplacingMergeTree 引擎 |
| **Redis** | ≥ 6.2 | 7.0+ (Alpine) | 建议禁用持久化，配置 `noeviction` |
| **Python** | ≥ 3.9 | 3.10+ | 运行 `cmd/test-tools` 预检工具包 |
| **网络要求** | 出网连接 | 低延迟直连 | 需稳定访问 `fstream.binance.com` 与 `fapi.binance.com` |

---

## 3. 快速启动指南

### 方式 A：Docker Compose 模式（推荐本地与测试环境）

项目根目录提供了开箱即用的容器编排：

```bash
# 1. 仅启动底层存储依赖 (ClickHouse + Redis)，并自动执行建表
docker compose -f deploy/docker-compose.yml up -d

# 检查依赖健康状态（等待两者状态变为 healthy）
docker compose -f deploy/docker-compose.yml ps

# 2. 启动采集器主程序（使用 --profile app 触发构建并拉起）
docker compose -f deploy/docker-compose.yml --profile app up -d --build

# 3. 观察实时运行日志
docker compose -f deploy/docker-compose.yml logs -f chomosyncer-go

# 4. 停机与清理
docker compose -f deploy/docker-compose.yml --profile app down
# 若需彻底清空数据卷：docker compose -f deploy/docker-compose.yml --profile app down -v
```

---

### 方式 B：常规二进制裸机部署（生产推荐）

#### 步骤 1：编译静态二进制
```bash
CGO_ENABLED=0 go build -trimpath   -ldflags "-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo 'v1.0.0')"   -o bin/chomosyncer-cmd ./cmd/chomosyncer-cmd
```

#### 步骤 2：初始化 ClickHouse 数据库与表
执行建表 SQL 脚本：
```bash
# 原生客户端方式：
clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < deploy/clickhouse/001_fapi_kline.sql

# 或直接通过 HTTP 接口执行：
curl -s 'http://127.0.0.1:8123/' --data-binary @deploy/clickhouse/001_fapi_kline.sql

# 验证数据表是否创建成功
curl -s 'http://127.0.0.1:8123/?query=SHOW+TABLES+FROM+market'
# 正常应返回: fapi_kline_1h 和 fapi_kline_1m
```

#### 步骤 3：配置与启动
支持通过配置文件与命令行参数协同启动。优先读取 `config.yaml`（可从 `config.example.yaml` 复制）：

```bash
cp config.example.yaml config.yaml
# 根据实际网络与集群修改 config.yaml 中的 redis / clickhouse 地址

# 启动服务（显式指定配置文件）
./bin/chomosyncer-cmd -config config.yaml

# 或通过命令行参数覆盖部分配置项
./bin/chomosyncer-cmd -config config.yaml -log-level debug -intervals 1m,1h
```

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
# 检查 1m 和 1h 周期全库数据是否连续、有无断档、字段是否饱满
python cmd/test-tools/check_clickhouse_integrity.py --intervals 1m,1h

# 针对特定主力币种深度检查
python cmd/test-tools/check_clickhouse_integrity.py --symbol BTCUSDT --show-all-gaps
```
- **核心判定**：自动利用 ClickHouse 窗口函数扫描相邻 K 线时间戳差值，排查断档（Gap）；同时校验 `open/high/low/close > 0`、`high >= low`、`volume >= 0`、`trades_count >= 0`。

### 5.2 Redis 未收盘 Live Bar 实时检查
```bash
python cmd/test-tools/check_redis_livebars.py --intervals 1m,1h
```
- **核心判定**：扫描全市场 `livebar:{SYMBOL}:{interval}`，验证 Key 存在性、TTL（`2 * interval`）、时延时效（防止假死）以及 11 个 Hash 字段的完备性。

### 5.3 Redis 已收盘滚动滑窗检查
```bash
python cmd/test-tools/check_redis_closed_windows.py --intervals 1m,1h
```
- **核心判定**：检查 `kline:{SYMBOL}:{interval}` 列表长度是否达到期望的 200 根，头部时间戳是否紧跟当前收盘边界，且滑窗内部每两根 Bar 之间严格连续无漏根。

### 5.4 截面通知 Stream 监控与回溯
```bash
# 查看最近 20 个截面发布事件的时延与标的齐备率
python cmd/test-tools/monitor_redis_kline_ready.py --recent 20

# 实时监听模式（流式打印每个周期的截面就绪耗时与到达标的数）
python cmd/test-tools/monitor_redis_kline_ready.py --tail
```

### 5.5 Redis 与 ClickHouse 双写一致性对账
```bash
python cmd/test-tools/e2e_reconciliation.py --limit-symbols 20
```
- **核心判定**：直接将 Redis 滑窗与 ClickHouse 最新落盘数据进行逐根比对，时间戳与 OHLCV 数值必须 100% 吻合（容差 `< 1e-5`）。

### 5.6 上线前一键综合评分
```bash
# 快速冒烟扫描
python cmd/test-tools/run_all_checks.py --quick

# 深度全量扫描
python cmd/test-tools/run_all_checks.py
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
- [ ] 执行 `cmd/chomosyncer-cmd`，确认日志无 ERROR 报错；
- [ ] 检查 `/healthz` 与 `/readyz` 探针均返回 HTTP 200；
- [ ] 运行 `python cmd/test-tools/run_all_checks.py` 得到全绿 PASS 结论；
- [ ] Prometheus 正常抓取 `/metrics`，配置了 `ws_connection_status` 和 `ws_last_message_age_seconds` 报警规则。
