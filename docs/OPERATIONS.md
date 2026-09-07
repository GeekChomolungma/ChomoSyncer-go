# ChomoSyncer-go — 部署、集成与压力测试手册

面向已经 `go test ./...` 全绿的当前实现（模组 1–6）。模组 7（Python reader）尚未研发，不影响
Go 侧全链路运行。

链路：`Binance WS → collector → dispatcher → { Redis 收盘滑窗 + kline_ready 流, Redis 实时快照, ClickHouse 归档 }`。
详见 [`../chomoSyncer-go_design_doc.md`](../chomoSyncer-go_design_doc.md)。

---

## 0. 前置条件

| 依赖 | 版本 | 说明 |
|---|---|---|
| Go | ≥ 1.25（`go.mod` 指定；开发用 1.27） | 编译 `chomosyncer-go` |
| ClickHouse | ≥ 23.8 | 原生 TCP 9000（`clickhouse-go/v2` 用）+ HTTP 8123（调试用） |
| Redis | ≥ 7 | 6379；缓存角色，持久化建议关闭 |
| Docker + Compose | 可选 | 一键起依赖 |
| 出网 | — | 访问 `fstream.binance.com` / `fapi.binance.com`（或 testnet） |

`go test -race` 需要 CGO/gcc；发布用二进制 `CGO_ENABLED=0`，无此依赖。

---

## 1. 快速上手（Docker Compose）

```bash
# 仅依赖（ClickHouse 首次启动自动执行 deploy/clickhouse/001_fapi_kline.sql 建表）
docker compose -f deploy/docker-compose.yml up -d
docker compose -f deploy/docker-compose.yml ps        # 等两者 healthy

# 连上采集器（构建镜像 + 拉起，读 deploy/chomosyncer-go.env.example）
docker compose -f deploy/docker-compose.yml --profile app up -d --build

# 验证
curl -s localhost:9090/healthz                        # ok
curl -s localhost:9090/metrics | grep -E 'ws_shards_active|universe_size'
```

停：`docker compose -f deploy/docker-compose.yml --profile app down`（去掉 `--profile app` 的
话依赖不会停）。删数据卷：`... down -v`。

---

## 2. 手动部署

### 2.1 ClickHouse + 建表

```bash
# 起服务（示例用 docker，裸机同理）
docker run -d --name ch -p 9000:9000 -p 8123:8123 \
  --ulimit nofile=262144:262144 clickhouse/clickhouse-server:24.8

# 建库表（native 9000）
clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < deploy/clickhouse/001_fapi_kline.sql
# 或走 HTTP，无需 client：
curl -s 'http://127.0.0.1:8123/' --data-binary @deploy/clickhouse/001_fapi_kline.sql

# 校验
curl -s 'http://127.0.0.1:8123/?query=SHOW+TABLES+FROM+market'
# 期望：fapi_kline_1h  /  fapi_kline_1m
```

> 表按 interval 分：`market.fapi_kline_1m` / `market.fapi_kline_1h`，`ReplacingMergeTree(created_at)`
> 按 `(symbol, start_time)` 去重，`PARTITION BY toYYYYMM(start_time)`。追加多的 interval 时，
> 复制一段 `CREATE TABLE ... _<iv>` 即可。

### 2.2 Redis

```bash
docker run -d --name redis -p 6379:6379 redis:7-alpine \
  redis-server --save '' --appendonly no --maxmemory 1gb --maxmemory-policy noeviction
```

- 持久化关闭：这是缓存。`livebar:*` 带 TTL，`kline:*` 靠 `LTRIM` 定长 200。
- `noeviction`：内存打满时写入报错（可见）而不是静默淘汰滑窗数据。全市场稳态约 30–100 MB。

### 2.3 编译

```bash
CGO_ENABLED=0 go build -trimpath \
  -ldflags "-s -w -X main.version=$(git describe --tags --always --dirty)" \
  -o bin/chomosyncer-go ./cmd/chomosyncer-go
```

### 2.4 配置

每个 flag 都有 `CHOMOSYNCER_*` 环境变量孪生；**显式 flag 优先于环境变量**。

| flag / env | 默认 | 说明 |
|---|---|---|
| `-metrics-addr` / `CHOMOSYNCER_METRICS_ADDR` | `:9090` | `/metrics` + `/healthz`；`off` 关闭 HTTP |
| `-redis-addr` / `..._REDIS_ADDR` | `localhost:6379` | 收盘滑窗 + `kline_ready` 流 |
| `-redis-db` / `-redis-password` | `0` / 空 | |
| `-live-redis-addr` / `..._LIVE_REDIS_ADDR` | 空→复用 `-redis-addr` | 实时未闭合快照的**专用 client** |
| `-live-redis-db` / `-live-redis-password` | `0` / 空 | |
| `-live-publish` / `..._LIVE_PUBLISH` | `false` | 额外 `PUBLISH livebar.<interval>` |
| `-ch-addr` / `..._CH_ADDR` | `localhost:9000` | 逗号分隔多节点；**原生 TCP 端口** |
| `-ch-database` / `-ch-username` / `-ch-password` | `market` / `default` / 空 | |
| `-intervals` / `..._INTERVALS` | `1m,1h` | 逗号分隔；每个 interval 一张 CH 表 + 一组 WS shard |
| `-shards-per-interval` / `..._SHARDS_PER_INTERVAL` | `4` | 每 interval 的 WS 连接数（固定；改动会触发一次全量重订） |
| `-ws-url` / `..._WS_URL` | `wss://fstream.binance.com` | testnet：`wss://fstream.binancefuture.com` |
| `-rest-url` / `..._REST_URL` | `https://fapi.binance.com` | testnet：`https://testnet.binancefuture.com` |
| `-section-timeout` / `..._SECTION_TIMEOUT` | `5s` | 截面未集齐时 `kline_ready` 的兜底 |
| `-backfill` / `..._BACKFILL` | `true` | 历史回补总开关：冷启动 + shard 重连回补 + CH→Redis 窗口重建（见 [`GAPFILL_DESIGN.md`](GAPFILL_DESIGN.md)）。`false` = forward-only |
| `-backfill-rest-rps` / `..._BACKFILL_REST_RPS` | `20` | `/fapi/v1/klines` 令牌桶速率 |
| `-backfill-workers` / `..._BACKFILL_WORKERS` | `4` | 单请求并行 REST 拉取数 |
| `-backfill-gap-debounce` / `..._BACKFILL_GAP_DEBOUNCE` | `30s` | 合并同一 shard 的重连 gap 事件 |
| `-backfill-gate-timeout` / `..._BACKFILL_GATE_TIMEOUT` | `5m` | 被门控的 key 超时强制放行（forward-only 降级） |
| `-backfill-flush-wait` / `..._BACKFILL_FLUSH_WAIT` | `2s` | 归档后、读 CH 前的等待（覆盖 chwriter flush 间隔） |
| `-log-level` / `-log-format` | `info` / `text` | `debug\|info\|warn\|error` / `text\|json` |

样例：`cp deploy/chomosyncer-go.env.example deploy/chomosyncer-go.env` 后按需改。

### 2.5 启动与验证

```bash
set -a; . deploy/chomosyncer-go.env; set +a
./bin/chomosyncer-go            # 或 ./bin/chomosyncer-go -redis-addr ... -ch-addr ...
```

启动日志应依次出现：`universe refreshed symbols=<数百>` → `symbol set applied shards=8`（1m,1h ×4）
→ 若干 `shard connected`。随后：

```bash
curl -s localhost:9090/healthz
curl -s localhost:9090/metrics | grep -E \
  'ws_connection_status|ws_shards_active|universe_size|kline_ingested_total|redis_bars_pushed_total'
```

- 所有 `ws_connection_status{shard=...} 1`
- `universe_size` 为几百
- 约 1 分钟后 `kline_ingested_total` / `redis_bars_pushed_total` / `clickhouse_rows_flushed_total`
  开始增长，整分钟出现 `dispatcher_section_published_total{interval="1m",reason="complete"}`

启用 `-backfill`（默认）时：启动会先做**冷启动回补**——对全 universe 拉历史进 CH 并从 CH 重建 Redis
收盘窗口，期间 `/readyz` 返回 **503**（`/healthz` 仍 200）。完成后 `/readyz` 转 200，日志出现
`cold-start backfill complete`。此时 `redis-cli LLEN kline:BTCUSDT:1h` 应**立即接近 200**（而非从 0 攒）。

**优雅退出**：`SIGTERM`（`kill` / `docker stop` / Ctrl-C）。日志出现 `shutdown signal received; draining`
→ `clickhouse batch writer stopped` → `shutdown complete`。退出前会把 CH 缓冲全量 flush、drain 各 channel、
取消在途回补并释放门控。

---

## 3. 数据落点速查

| 位置 | Key / 表 | 内容 |
|---|---|---|
| Redis List | `kline:{SYMBOL}:{interval}` | 定长 200 的收盘 Bar，`LPUSH` 队头最新，元素 = 9 元素紧凑数组 `[t,o,h,l,c,v,qv,tbv,tbqv]` |
| Redis Stream | `stream:market:kline_ready` | 截面就绪事件，字段 `interval` / `timestamp` / `symbols_count` |
| Redis Hash | `livebar:{SYMBOL}:{interval}` | 实时快照（含未闭合），字段 `t o h l c v qv tbv tbqv n x`，`PEXPIRE = 2×interval` |
| Redis Pub/Sub | `livebar.{interval}`（需 `-live-publish`） | 11 元素数组 `[...,n,x]` |
| ClickHouse | `market.fapi_kline_{interval}` | 只追加原始 Bar，`ReplacingMergeTree` 去重 |

```bash
redis-cli LRANGE kline:BTCUSDT:1m 0 0
redis-cli HGETALL livebar:BTCUSDT:1m
redis-cli XREVRANGE stream:market:kline_ready + - COUNT 3
curl -s 'http://127.0.0.1:8123/?database=market' --data-binary \
  'SELECT symbol,start_time,close FROM fapi_kline_1m ORDER BY start_time DESC LIMIT 5'
```

---

## 4. 正向集成测试

### 4.1 单元 / 装配层

```bash
go test ./...                    # 全部包
go test -run TestNewAndShutdown ./internal/app/    # 无外部依赖装配 + 优雅退出
```

`internal/app` 的 smoke 测试已覆盖「不连任何外部依赖也能 New + Shutdown 不 panic」和
「universe 不可达时 Run 快失败」。

### 4.2 端到端脚本

栈起好后（`--profile app`），跑：

```bash
bash scripts/integration_check.sh          # 默认采样间隔 90s
SETTLE=150 bash scripts/integration_check.sh
```

脚本做 5 组断言：liveness（healthz / 所有 shard 连接 / universe_size>0）→ 计数器在 90s 内推进
（`kline_ingested_total` / `redis_bars_pushed_total` / `redis_livebar_updates_total` /
`clickhouse_rows_flushed_total` / `dispatcher_section_published_total`）→ 丢弃/错误类保持 0 →
Redis 内容（`LLEN ≤ 200`、`livebar` 存在、`XLEN > 0`）→ ClickHouse 内容（行数 > 0、最新 1m Bar
< 180s、OHLC 不变式 `high ≥ max(o,c)` / `low ≤ min(o,c)`）。

### 4.3 手动交叉核对清单

| 检查 | 命令 | 期望 |
|---|---|---|
| 收盘 List 与 CH 一致 | 取 `LRANGE kline:BTCUSDT:1m 0 0` 的 `t`，查 `SELECT * FROM fapi_kline_1m WHERE symbol='BTCUSDT' AND toUnixTimestamp64Milli(start_time)=<t>` | OHLCV 完全一致 |
| 无重复 Bar | `SELECT symbol,start_time,count() c FROM fapi_kline_1m GROUP BY 1,2 HAVING c>1` | 空（`FINAL` 前也应≈空） |
| 截面事件节奏 | `XREVRANGE stream:market:kline_ready + - COUNT 5` | 每分钟一条 `interval=1m`，`symbols_count ≈ universe_size` |
| livebar 时效 | `HGET livebar:BTCUSDT:1m t` 与当前分钟对齐；`x` 在整分钟边界翻 `1` | 实时更新 |
| 未闭合不进 CH/收盘 | `dispatcher_events_total{kind="live"}` ≫ `{kind="closed"}`，CH 行数只随闭合增长 | ✅ |
| 优雅退出不丢数 | 记录 `SELECT count() FROM fapi_kline_1m`，`SIGTERM`，重启前再查 | 不回退（退出前已 flush） |
| 断线自愈 | `docker network disconnect` WS 或 `iptables` 封 30s 再放开 | `ws_reconnects_total` +1，`ws_connection_status` 回 `1`，无数据缺口（除断网那几秒） |

### 4.4 testnet

改 `-ws-url wss://fstream.binancefuture.com -rest-url https://testnet.binancefuture.com`。
testnet 交易对少、成交稀，适合验证「链路通」，不适合验证吞吐或数据质量。

---

## 5. 压力测试

### 5.1 瓶颈分析

Binance 侧速率是固定的（全市场 ~1–3k msg/s），**跑真实行情压不出上限**。压测要点是用合成负载把
`dispatcher → rediswin/chwriter` 这段推到远超 Binance 的量，观察背压是否体面、指标是否触顶。

链路里几个「可能先饱和」的点：
1. `chwriter` 批缓冲（`ChannelSize=20000`，满则 `TryPush` 丢弃计 `clickhouse_rows_dropped_total`）。
2. `dispatcher` 闭合队列（`ClosedQueueSize=4096`，满则 `ErrBusy` 计 `dispatcher_events_dropped_total{sink="closed"}`）。
3. `rediswin.LiveBarWriter` channel（`ChannelSize=8192`，满则 `redis_livebar_dropped_total`）。
4. Redis 单线程 / go-redis 连接池排队（`redis_pipeline_latency_seconds` p99）。
5. ClickHouse 写入延迟（`clickhouse_flush_latency_seconds`，`clickhouse_buffer_size` 逼近 20000 即告警）。

### 5.2 方法 A —— dispatcher 负载发生器（推荐，需补一个小程序）

尚未实现。约 60 行的 `cmd/loadgen`：构造 `dispatcher.KlineEvent`（随机 symbol/价格，按比例产生
`IsFinal=true`），以 `-rate N/s` 调 `dispatcher.HandleKlineEvent`，sinks 接**真实** Redis + CH。
骨架：

```go
d, _ := dispatcher.New(ctx, dispatcher.Config{Registerer: reg},
    dispatcher.Sinks{Live: liveW, Window: win, Archive: router, Ready: win}, staticUniverse)
tick := time.NewTicker(time.Second / time.Duration(ratePerSec))
for range tick.C { _ = d.HandleKlineEvent(randomEvent()) }   // 观察返回的 ErrBusy 比例
```

跑法：`loadgen -rate 5000 -final-ratio 0.02 -symbols 500`，逐步加 `-rate` 直到某个
`*_dropped_total` 或 `clickhouse_buffer_size` 起飞，记录该 rate 为当前配置的吞吐上限。

### 5.3 方法 B —— 组件级基准

```bash
# Redis 原始能力（LPUSH+LTRIM 近似）
redis-benchmark -h 127.0.0.1 -p 6379 -t lpush -n 1000000 -P 16 -q

# ClickHouse 插入路径
clickhouse-benchmark --host 127.0.0.1 --port 9000 --concurrency 4 <<<'INSERT INTO market.fapi_kline_1m ...'

# Go 侧（需补 Benchmark 函数）：chwriter 的批 flush、rediswin 的 pipeline
go test -bench=. -benchmem ./internal/chwriter/ ./internal/rediswin/
```

### 5.4 压测期间盯的指标

| 指标 | 健康 | 异常信号 |
|---|---|---|
| `dispatcher_events_dropped_total{sink=*}` | `0` | 任意增长 = 对应下游打满 |
| `redis_livebar_dropped_total` | 允许少量 | 持续增长 = `LiveBarWriter.Workers` 不够 / Redis 拥塞 |
| `clickhouse_rows_dropped_total` | `0` | 非 0 = CH 写入彻底跟不上（重试也耗尽） |
| `clickhouse_buffer_size` | < 5000 | 逼近 20000 触发设计文档 §6 告警 |
| `clickhouse_flush_latency_seconds` p99 | < 1s | 飙升 = CH 端瓶颈（磁盘 / merge） |
| `redis_pipeline_latency_seconds` p99 | < 5ms | 飙升 = Redis CPU / 连接池排队 |
| `dispatcher_section_pending` | 个位数 | 持续攀升 = 截面聚合器没在回收（universe 分母不对？） |
| `go_goroutines` | 稳态平线 | 单调上升 = goroutine 泄漏 |
| `go_memstats_heap_inuse_bytes` | 锯齿平稳 | 单调上升 = 内存泄漏 |
| `ws_reconnects_total` 速率 | 接近 0 | 频繁重连 = 读循环被下游阻塞（不该发生，回调直连非阻塞） |

### 5.5 可调旋钮

**已可通过 flag/env 调**：`-shards-per-interval`、`-section-timeout`、`-intervals`、`-backfill*`。

**gapfill 相关指标**（`-backfill` 开启时）：`backfill_ready`（1=冷启动完成）、`backfill_requests_total{reason}`、
`backfill_bars_fetched_total{interval}` / `backfill_bars_written_total{interval}`、
`backfill_windows_rebuilt_total{interval}`、`backfill_errors_total{stage}`、`backfill_duration_seconds{reason}`、
`backfill_rest_weight_used`、`backfill_rest_http_errors_total{code}`、`windowgate_held_keys`、
`redis_window_gated_pushes_total{interval}`、`redis_bars_skipped_total`、
`dispatcher_kline_ready_suppressed_total{interval}`。告警：`backfill_ready == 0` 持续 > 10min、
`rate(backfill_errors_total[10m]) > 0`、`windowgate_held_keys` 长期 > 0。


**目前写死在 `internal/app/app.go` 的 `*.Config{}` 里，压测要调得改代码重编**（后续应提升为 `app.Config` + flag）：

| 组件 | 字段 | 默认 | 压测方向 |
|---|---|---|---|
| `chwriter.Config` | `BatchSize` / `FlushInterval` / `ChannelSize` / `MaxRetries` | 5000 / 1s / 20000 / 5 | 高吞吐可加大 `ChannelSize`、缩短 `FlushInterval` |
| `dispatcher.Config` | `ClosedWorkers` / `ClosedQueueSize` | 4 / 4096 | 整分钟突发大可加 workers / 队列 |
| `rediswin.LiveBarConfig` | `Workers` / `ChannelSize` / `WriteTimeout` | 2 / 8192 / 200ms | live 丢帧多则加 `Workers` |
| `collector.Config` | `StaleTimeout` / `WatchdogInterval` / `ReconnectMax` | 60s / 10s / 30s | 一般不动 |
| `universe.Config` | `RefreshInterval` / `RefreshOffset` | 24h / +2min | 一般不动 |

### 5.6 Soak（24–48h）

真实行情连跑，每小时抓一次 `/metrics`：确认 `go_goroutines` / heap 平稳、`ws_reconnects_total`
速率低、Redis `INFO memory` 稳定、ClickHouse 分区按 `toYYYYMM` 正常增长、每天 00:02 UTC 出现
`universe refreshed`（新上市/退市被吸收，`ws_subscription_updates_total` 可能 +1）。

---

## 6. 生产部署要点

### systemd

```ini
# /etc/systemd/system/chomosyncer-go.service
[Unit]
Description=ChomoSyncer-go market data collector
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
EnvironmentFile=/etc/chomosyncer-go/chomosyncer-go.env
ExecStart=/usr/local/bin/chomosyncer-go
Restart=on-failure
RestartSec=5
TimeoutStopSec=45          # 覆盖 app 的 30s ShutdownTimeout + 余量
LimitNOFILE=65536
User=chomo
Group=chomo

[Install]
WantedBy=multi-user.target
```

`systemctl stop` 发 `SIGTERM`，`TimeoutStopSec` 要 ≥ 采集器的 30s 优雅退出预算。

### Prometheus 抓取

```yaml
scrape_configs:
  - job_name: chomosyncer-go
    scrape_interval: 15s
    static_configs:
      - targets: ["chomo-host:9090"]
```

关键告警：`clickhouse_buffer_size > 20000` (5m)、`rate(clickhouse_rows_dropped_total[5m]) > 0`、
`rate(dispatcher_events_dropped_total[5m]) > 0`、`max_over_time(ws_connection_status[5m]) == 0`（某 shard 长期断）、
`time() - universe_last_refresh_timestamp_seconds > 172800`（>2 天没刷新）。

### 资源估算（全市场 ~500 symbol × {1m,1h}）

- WS 连接：`intervals × shards-per-interval` = 8；消息 ~1–3k/s。
- Redis：~30–100 MB，< 5k cmd/s。
- ClickHouse：每表约 500 行/分钟（1m）、500 行/小时（1h）；批 5000 或 1s 触发一次写。
- 采集器进程：常驻 goroutine ~30 + 每消息瞬时协程；heap 典型 50–150 MB；单核足够。

---

## 7. 故障排查

| 现象 | 可能原因 | 处置 |
|---|---|---|
| 启动即退出，日志 `start universe: ... connection refused` | 拿不到 `exchangeInfo`（出网 / DNS / 被墙） | 检查到 `fapi.binance.com` 的连通；或先用 testnet |
| `/metrics` 有数据但 `kline_ingested_total` 不涨 | WS 没连上 / 全被过滤 | 看 `ws_connection_status`；`-log-level debug` 看是否收到帧 |
| `clickhouse_rows_dropped_total` 增长 | CH 宕 / 写太慢 / 表不存在 | 确认建表 + `-ch-addr` 是**9000**（非 8123）；看 CH 端 `system.errors` |
| `redis ... connection refused` 反复 | Redis 地址错 / 未起 | 收盘与实时是两个 client，检查 `-redis-addr` 和 `-live-redis-addr` |
| `dispatcher_section_published_total` 只有 `reason="timeout"` | universe 分母对不上（`Has()` 恒 false） | 确认 `universe_size` > 0 且与实际订阅集一致 |
| `ws_reconnects_total` 高频增长 | 网络抖 / 被限速 / 下游阻塞 | 看 `collector_dispatch_errors_total`；确认 Redis/CH 没打满 |
| 内存 / goroutine 单调涨 | 泄漏 | 抓 `curl localhost:9090/debug/pprof/...`（当前未挂 pprof，需要时在 `metrics` 加） |
