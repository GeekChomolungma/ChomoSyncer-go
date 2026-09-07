# 历史 K 线 Gapfill / 回补设计

> 状态：**P1–P5 已实现**（`internal/rediswin` 单调 LPUSH + `RebuildWindow`、`internal/windowgate`、
> `internal/backfill`、`internal/app` 装配、`collector.OnGap`）。**P6（周期对账）仍为 TODO**。
> 关联：[`../chomoSyncer-go_design_doc.md`](../chomoSyncer-go_design_doc.md)、[`OPERATIONS.md`](OPERATIONS.md)
>
> 实现与本设计的偏差：`backfill_rest_errors_total{code}` 实际名为 `backfill_rest_http_errors_total`；
> gate 的持有计数指标名为 `windowgate_held_keys`（非 `backfill_keys_gated`）；`backfill_requests_dropped_total`
> 替代设计里的队列满计数；`backfill_errors_total{stage}` 覆盖 rest/archive/ch_max/ch_read/rebuild。

---

## 1. 背景与目标

当前实现是 **forward-only 增量采集**：只从「进程 / WS 连接活着的那一刻」往后接闭合 Bar。
断线重连不补发历史帧（Binance `<symbol>@kline` 流没有 `startTime` 参数），因此存在三类洞：

- **冷启动**：Redis 窗口从 0 攒（1m 要 200 分钟满），ClickHouse 无历史。
- **shard 断线**：停机窗口内闭合的 Bar 永久丢失，CH 出洞，Redis 窗口 `t` 静默跳变。
- **新上市**：universe 每日刷新新增的合约没有历史、没有窗口。

**目标**：引入一条与增量采集**平行、低耦合**的回补链路，做到：

1. 冷启动时对**全部在交易的 U 本位永续合约**回补历史 → CH，并把 CH 尾部 `WindowSize` 根物化进 Redis 收盘窗口。
2. shard 断线重连时对该 shard 的 key 做区间 gapfill，并同样从 CH 重建其 Redis 窗口。
3. 新上市合约等同「无历史 key」，触发一次 full-window 回补。
4. 回补链路 **绕过 dispatcher 的单调防护、不发 `kline_ready`**，与实时业务解耦。
5. 回补幂等：重复运行安全。

**核心不变式**：ClickHouse 是账本（append-only + `ReplacingMergeTree` 去重）；Redis 收盘窗口是
「CH 权威尾部 + 尚未落 CH 的最新 live Bar」的物化缓存。任何 gapfill 之后，Redis 窗口 == CH 尾部。

## 2. 非目标

- **深度历史回补**（数周 / 数月）：本设计冷启动只回补「足够填满窗口 + 少量余量」（默认 ~300 根）。
  深度历史是独立的一次性 backfill 工具，另议。
- **周期性全量对账**：仅留接口（见 §5.4），补丁式接入。
- **补发历史 `kline_ready`**：不做。依赖 Python §4.2 无状态重算在下一周期自愈。
- **逐笔成交 / Trade 流回补**：不在范围。

## 3. 术语

| 术语 | 含义 |
|---|---|
| key | 一个 `(symbol, interval)`，对应 Redis `kline:{SYMBOL}:{interval}` 与 CH `market.fapi_kline_{interval}` |
| WindowSize | Redis 收盘窗口定长，200（设计文档 §3.2，固定） |
| gate / 门控 | 临时挂起某 key 的 Redis 收盘窗口写入（以及所在 interval 的 `kline_ready`），直到该 key 的 CH→Redis 重建完成 |
| BackfillRequest | 回补工作项：`{keys, since, until, reason}` |
| 交接 / handoff | backfiller 完成一个 key 的「REST→CH→CH 读回→Redis 重建」后，`Release` 该 key，恢复实时窗口写 |

## 4. 总体架构

```
                    ┌──────────────── triggers ────────────────┐
  app 冷启动 ───────►│  cold_start (全 universe)                 │
  collector.OnGap ──►│  shard_reconnect (shard keys, [t_down,t_up])│──► BackfillRequest 队列
  universe.OnChange ►│  universe_add   (新 symbol, full window)   │
  (TODO 周期对账) ───►│  reconcile      (补丁接入)                 │
                    └──────────────────────────────────────────┘
                                      │
                                      ▼
                        internal/backfill.Backfiller (worker pool)
             ┌────────────────────────┼─────────────────────────────┐
             ▼                        ▼                             ▼
   1. gate.Hold(keys)      2. REST /fapi/v1/klines        3. chwriter.Push  ──► ClickHouse
      (windowgate)            (rate-limited, 只取闭合)         (每 interval 复用现有 BatchWriter)
                                                              │  flush 落 CH
                                      ┌───────────────────────┘
                                      ▼
                        4. KlineStore.LastBars(interval, keys, N)   ◄── ClickHouse (FINAL 去重)
                                      ▼
                        5. rediswin.Writer.RebuildWindow(key, bars) ──► Redis (原子 Lua)
                                      ▼
                        6. gate.Release(keys)  →  实时窗口写恢复

  实时链路（不变，仅经过 gate 适配器）：
    collector ─► dispatcher ─► gatedWindow ─► rediswin.Writer.PushBarAndTrim (单调 LPUSH)
                            └─► archiveRouter ─► chwriter (不门控，CH 幂等)
                            └─► gatedReady ─► rediswin.Writer.PublishKlineReady
                            └─► rediswin.LiveBarWriter (不门控)
```

**并行原则**：WS 从 t=0 就连、就把闭合 Bar 写 CH 与实时快照；backfiller 与实时采集**同时**写 CH
（`ReplacingMergeTree` 去重，无序无所谓）。唯一被 backfiller 独占的是 **Redis 收盘窗口**（及其 `kline_ready`），
独占期通过 gate 实现，独占结束即交接。

## 5. 触发器

所有触发器统一往 `BackfillRequest` 队列投递；Backfiller 用 worker pool 消费，按 key 去重 / 合并。

### 5.1 冷启动（`reason=cold_start`）

`app.Run` 里，在 `collector` 启动**之后**、对外 `/readyz` 置就绪**之前**：

1. `universe.Start` 已产出全市场 symbol 集。
2. 对每个 interval：`gate.Hold(all keys of interval)`（此刻实时窗口写全部挂起，但 CH archive + live snapshot 照常）。
3. 投递一个 `BackfillRequest{keys=all, since=coldStartSince(key), until=now, reason=cold_start}`。
   - `coldStartSince(key)` = `max(CH.max(start_time)+1step, now - ColdStartLookback)`；CH 无该 key 数据时取 `now - ColdStartLookback`。
   - `ColdStartLookback` 默认 = `WindowSize × interval + 5×interval`（多拉 5 根余量）。
4. Backfiller 处理完**全部** key 后，`/readyz` 才返回 200。

**回答「先回补还是先 WS 还是同时」**：**同时**。WS 立即连（forward Bar 不丢、CH archive 与实时快照立即可用），
Redis 收盘窗口由 backfiller 独占至交接。冷启动期 `kline_ready` 全程压住（`gatedReady` 见 §7.2）。

### 5.2 shard 断线重连（`reason=shard_reconnect`）

`collector` 的 shard supervisor 在**重连成功后**回调 `OnGap(GapEvent{shardID, streams, lastMsgAt, reconnectAt})`：

- `since = lastMsgAt - 1×interval`（保守多回补一根）。
- `until = reconnectAt`。
- **封顶**：`since = max(since, until - MaxGapWindow)`，`MaxGapWindow` 默认 = `WindowSize × interval`。更久的洞留给周期对账。
- **去抖**：同一 shard 在 `GapDebounce`（默认 30s）内的多次 gap 合并为一个（取最早 `since`）。
- **跳过**：`until - since < 1×interval`（不可能有闭合 Bar）→ 不投递。
- keys = 该 shard `streams` 映射出的 `(symbol, interval)` 集合。
- `gate.Hold(keys)` → 投递 `BackfillRequest` → 处理完 `gate.Release(keys)`。

`collector.Config` 新增 `OnGap func(GapEvent)`（可为 nil；nil 时退化为「只重连不回补」，即当前行为）。
collector **不 import backfill**。

### 5.3 universe 新增合约（`reason=universe_add`）

`app` 在 `universe.OnChange` 回调里对 `snapshot` 与上一版做 diff，新增的 symbol：

- `keys = {(sym, iv) for iv in intervals}`，`since = now - ColdStartLookback`，`until = now`。
- 同样 `gate.Hold → 回补 → gate.Release`。

### 5.4 周期对账（`reason=reconcile`）—— TODO / 补丁接入点

**本期不实现。** 预留接口：

```go
// internal/backfill
type ReconcileScanner interface {
    // 返回 CH 里在 [since, until] 缺失的 (key, missing ranges)
    Scan(ctx context.Context, keys []Key, since, until time.Time) ([]BackfillRequest, error)
}
```

补丁时：一个 `go m.reconcileLoop(ctx)` 每 `ReconcileInterval`（如 1h）对全 universe 调 `Scan` → 投递
`reason=reconcile` 的请求（这类请求**只补 CH，不重建 Redis 窗口、不 gate**，因为它填的是更早的历史）。

## 6. 组件设计

### 6.1 `internal/backfill`

```go
package backfill

type Key struct{ Symbol, Interval string }

type BackfillRequest struct {
    Keys   []Key
    Since  time.Time
    Until  time.Time
    Reason string // cold_start | shard_reconnect | universe_add | reconcile
}

type GapEvent struct {
    ShardID     string
    Streams     []string
    LastMsgAt   time.Time
    ReconnectAt time.Time
}

type Config struct {
    RESTBaseURL      string        // https://fapi.binance.com
    ColdStartLookback time.Duration // 默认 WindowSize×interval + 余量（按 interval 计算）
    MaxGapWindow     time.Duration  // shard 回补封顶，默认 WindowSize×interval
    GapDebounce      time.Duration  // 默认 30s
    Workers          int            // 回补 worker 数，默认 4
    RequestLimit     rate.Limit     // REST 令牌桶，默认 ~20 req/s
    WindowSize       int            // 200
    GateTimeout      time.Duration  // CH 一直不可达时的门控降级，默认 5m
    Registerer       prometheus.Registerer
    Logger           *slog.Logger
}

type Backfiller struct { /* ... */ }

func New(cfg Config, rest KlineFetcher, ch KlineStore, win WindowRebuilder, gate *windowgate.Gate) *Backfiller

func (b *Backfiller) Start(ctx context.Context)                 // 起 worker pool
func (b *Backfiller) Close() error
func (b *Backfiller) Submit(req BackfillRequest)                // 三个触发器都调它
func (b *Backfiller) HandleGap(ev GapEvent)                     // collector.OnGap 直接指向它（内部转 Submit）
func (b *Backfiller) Ready() bool                               // 冷启动请求是否全部完成（喂 /readyz）
```

依赖接口（便于测试注入 fake）：

```go
type KlineFetcher interface {
    // 拉 [since, until) 内的闭合 Bar（已按 startTime 升序、已过滤未闭合）
    Fetch(ctx context.Context, k Key, since, until time.Time) ([]chwriter.Row, error)
}
type KlineStore interface {
    // 从 CH 读每个 key 最近 n 根（FINAL 去重，升序）
    LastBars(ctx context.Context, interval string, symbols []string, n int) (map[string][]chwriter.Row, error)
}
type WindowRebuilder interface {
    RebuildWindow(ctx context.Context, symbol, interval string, barsNewestFirst []rediswin.CompactBar) error
}
```

- **CH 写入**：`KlineFetcher.Fetch` 的结果由 Backfiller 逐条 `chwriter.BatchWriter.Push(ctx, row)`（阻塞，
  历史不丢），落哪张表由 interval 决定（复用 `app` 的 per-interval writer map）。
- **worker pool**：`Workers` 个 goroutine 消费内部 channel；每个 `BackfillRequest` 拆成 per-key 子任务，
  同一 key 串行、不同 key 并发。REST 调用过 `rate.Limiter`。
- **单请求处理流程**（per key）：
  1. `rest.Fetch(k, since, until)` → `[]Row`（分页：`/klines limit=1500`，`since` 步进直到 `>= until`）。
  2. 逐条 `chwriter.Push`。
  3. 攒够一个 interval 的所有 key 后（或该请求所有 key 的 Push 都完成后）：`chwriter` 强制 flush 或等一个
     `FlushInterval`，确保落 CH。
  4. `ch.LastBars(interval, symbols, WindowSize)` 一次查回该 interval 全部涉及 symbol 的尾部。
  5. per key：`win.RebuildWindow(sym, iv, bars)`。
  6. `gate.Release(key)`。
- **降级**：某 key 的 REST 或 CH 持续失败超过 `GateTimeout` → `gate.Release(key)` 强制放行（forward-only），
  计 `backfill_gate_timeout_total`，日志 Warn。

### 6.2 `internal/windowgate`

```go
package windowgate

type Key = backfill.Key // 或独立定义，避免 import 环则放公共小包

type Gate struct { /* readySet: map[Key]struct{} of *held* keys + interval 反查 */ }

func New(metrics ...) *Gate
func (g *Gate) Hold(keys ...Key)
func (g *Gate) Release(keys ...Key)
func (g *Gate) HeldWindow(symbol, interval string) bool   // 该 key 是否被门控
func (g *Gate) HeldInterval(interval string) bool         // 该 interval 是否有任一 key 被门控

// 适配器：包住现有 sink，dispatcher 拿到的是这两个
func (g *Gate) WrapWindow(inner dispatcher.WindowSink) dispatcher.WindowSink
func (g *Gate) WrapReady(inner dispatcher.ReadyPublisher) dispatcher.ReadyPublisher
```

- `gatedWindow.PushBarAndTrim(ctx, sym, iv, bar)`：`g.HeldWindow(sym,iv)` → 直接 return nil（该 Bar 已由
  archive 落 CH，重建时会捞回），计 `redis_window_gated_pushes_total`；否则透传 `inner`。
- `gatedReady.PublishKlineReady(ctx, evt)`：`g.HeldInterval(evt.Interval)` → return `"", nil`（跳过），
  计 `dispatcher_kline_ready_suppressed_total{interval}`；否则透传。
- `Hold` / `Release` 并发安全（`sync.RWMutex`），幂等。

> 放在独立包避免 `backfill` ↔ `dispatcher` 的 import 方向纠缠；`app` 同时 import 三者做装配。

### 6.3 `rediswin` 改动（仅新增，不改现有语义）

**a) `PushBarAndTrim` 改为单调 LPUSH（幂等）**

现在：`pipe.LPush + pipe.LTrim 0 199`。
改为一段 Lua（`EVALSHA`）：

```lua
-- KEYS[1]=kline:{SYM}:{iv}  ARGV[1]=compact_json  ARGV[2]=start_time  ARGV[3]=N
local head = redis.call('LINDEX', KEYS[1], 0)
if head then
  local ht = cjson.decode(head)[1]        -- 紧凑数组第 0 位 = start_time
  if tonumber(ARGV[2]) <= tonumber(ht) then return 0 end   -- 不比队头新：跳过
end
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('LTRIM', KEYS[1], 0, tonumber(ARGV[3]) - 1)
return 1
```

意义：重建与实时写的接缝天然免重复 / 免乱序；`PushBarAndTrim` 返回是否真的写入（计
`redis_bars_skipped_total`）。`CompactBar` 已带 `StartTime`，无需额外解析。

**b) 新增 `RebuildWindow`（原子全量替换）**

```go
func (w *Writer) RebuildWindow(ctx context.Context, symbol, interval string, barsNewestFirst []CompactBar) error
```

Lua：

```lua
-- KEYS[1]=key  ARGV[1..M]=compact_json (newest-first)  最后一个不算 bar：ARGV[#ARGV]=N
redis.call('DEL', KEYS[1])
if #ARGV > 1 then redis.call('RPUSH', KEYS[1], unpack(ARGV, 1, #ARGV-1)) end
redis.call('LTRIM', KEYS[1], 0, tonumber(ARGV[#ARGV]) - 1)
```

`RPUSH(newest-first)` → 队头 = 最新，与实时 `LPUSH` 后的布局一致。空 `bars` → 只剩一个空 key（或 `DEL` 后不建）。

**c) `PushBarsAndTrim`（批量）** 同样切到单调 Lua（可选，一致性）。

### 6.4 CH 读回查询

```sql
SELECT symbol, start_time, end_time, open, high, low, close,
       volume, quote_volume, taker_buy_volume, taker_buy_quote_volume, trades_count
FROM market.fapi_kline_1m FINAL
WHERE symbol IN ({symbols})
ORDER BY symbol ASC, start_time DESC
LIMIT {WindowSize} BY symbol
```

- `FINAL` 去 `ReplacingMergeTree` 重复；`LIMIT n BY symbol` 每 symbol 取尾部 n 根。
- ~500 symbol × 200 根 = 10 万行，单查询秒级。shard 重连场景只查该 shard 的 ~125 symbol。
- 备选（避免 `FINAL` 扫描）：子查询 `GROUP BY symbol,start_time` + `argMax(col, created_at)`。
- 实现：`internal/backfill` 里的 `chKlineStore`，持有一个 `driver.Conn`（与 `chwriter` 独立连接，读写分离）。

### 6.5 REST `/fapi/v1/klines` 映射

请求：`GET /fapi/v1/klines?symbol=BTCUSDT&interval=1m&startTime={ms}&endTime={ms}&limit=1500`

响应逐行 `[openTime, o, h, l, c, volume, closeTime, quoteVolume, count, takerBuyVol, takerBuyQuoteVol, _]`：

| REST idx | → `chwriter.NewRow` 参数 |
|---|---|
| 0 `openTime` | `startMs` |
| 6 `closeTime` | `endMs` |
| 1..4 | `open/high/low/close` |
| 5 | `volume` |
| 7 | `quoteVolume` |
| 8 `count` | `tradesCount` |
| 9 | `takerBuyVolume` |
| 10 | `takerBuyQuoteVolume` |

- `symbol` / `interval` 从请求参数带入。
- **只取闭合**：丢弃 `closeTime >= serverNow` 的行（响应最后一根常是当前形成中的 Bar）。
- 分页：`startTime` 推进到 `lastOpenTime + 1step`，直到 `>= until` 或返回空。
- 限流：`rate.Limiter`（默认 ~20 req/s）；解析响应头 `X-MBX-USED-WEIGHT-1M`，接近上限时主动降速；
  `429` → 读 `Retry-After` 退避；`418` → 长退避 + 告警。
- 客户端：`net/http` + `sonic`（与 `internal/universe` 一致，不引连接器生成类型）。

### 6.6 `chwriter` 复用

Backfiller 不新开 CH 写路径：注入 `app` 已建的 `map[interval]*chwriter.BatchWriter`，逐条 `Push(ctx, row)`
（**阻塞**版，历史数据不允许因缓冲满被丢）。`ReplacingMergeTree` 保证与实时写的重叠区间幂等。

## 7. 一致性与幂等性论证

| 情况 | 结论 |
|---|---|
| gapfill 与实时写**同时**写 CH 同一 `(symbol,start_time)` | `ReplacingMergeTree(created_at)` 合并时保留 `created_at` 最大者；查询侧用 `FINAL` / `argMax`。幂等。 |
| 重建 Redis 窗口时有并发 live LPUSH | 该 key 处于 gate.Hold → `gatedWindow` 已挂起 live LPUSH → 无竞态。 |
| gate.Release 之后，第一根 live Bar 与 CH 读回的最后一根**同 `start_time`** | 单调 LPUSH（§6.3a）比较队头，`<=` 直接跳过 → 无重复。 |
| gate.Release 之后，live Bar 比 CH 读回**更旧**（极端乱序） | 单调 LPUSH 跳过；CH 侧该 Bar 若之前已 archive，则 `FINAL` 去重。 |
| gapfill 重复运行（同 key 同区间） | REST 再拉一遍 → `Push` → CH 幂等；`RebuildWindow` 全量替换 → 幂等；`gate` 标志幂等。 |
| 冷启动期间闭合的 Bar | WS 已连 → archive 落 CH；窗口被 gate 挂起 → 由 `LastBars` 读回（含这些 Bar）→ 重建。零丢失。 |
| 停机 > `MaxGapWindow` | 只回补最近 `MaxGapWindow`；更早的洞留给周期对账（TODO）。窗口（200 根）不受影响，Python §4.2 的 CH 兜底也只需最近 200 根。 |

## 8. 失败与降级

| 失败 | 行为 |
|---|---|
| REST 429 / 418 | 退避（读 `Retry-After`），期间 key 保持 gate.Hold；超过 `GateTimeout` → 强制 Release（forward-only）+ `backfill_gate_timeout_total` + Warn。 |
| REST 单 key 报错（非限速） | 重试 3 次退避；仍失败 → 跳过该 key 的 CH 补充，仍尝试 `LastBars`+`RebuildWindow`（用 CH 现有数据），再 Release。 |
| CH 写不可达 | `chwriter` 自身重试 / 缓冲；backfiller 等其恢复；超 `GateTimeout` → forward-only 降级。 |
| CH 读不可达（`LastBars` 失败） | 重试；超时 → 该 key **不重建**、直接 Release（窗口 forward-only，靠 live 攒回）；计 `backfill_window_rebuild_errors_total`。 |
| shard 高频 flapping | `GapDebounce` 合并；`Workers` 限制并发；同 key 请求在队列里去重（保留最早 `since`）。 |
| 冷启动 backfill 一直没完成 | `/readyz` 持续 503；`/healthz`（liveness）仍 200，进程不被杀。 |

## 9. 指标与探针

新增指标（走 `metrics.Registerer`）：

| 指标 | 类型 | 含义 |
|---|---|---|
| `backfill_requests_total{reason}` | Counter | 各触发源的回补请求数 |
| `backfill_keys_total{reason,status}` | Counter | 处理的 key 数（`status`=ok/rest_error/ch_error/gate_timeout） |
| `backfill_bars_fetched_total{interval}` | Counter | REST 拉回的闭合 Bar 数 |
| `backfill_bars_written_total{interval}` | Counter | 写入 CH 的 Bar 数 |
| `backfill_windows_rebuilt_total{interval}` | Counter | Redis 窗口重建次数 |
| `backfill_duration_seconds{reason}` | Histogram | 单个请求端到端耗时 |
| `backfill_rest_weight_used` | Gauge | 最近一次响应头 `X-MBX-USED-WEIGHT-1M` |
| `backfill_rest_errors_total{code}` | Counter | 429 / 418 / 5xx / other |
| `backfill_gate_timeout_total` | Counter | 门控超时强制放行的 key 数 |
| `backfill_keys_gated` | Gauge | 当前被 gate.Hold 的 key 数 |
| `redis_window_gated_pushes_total` | Counter | gated 期间被 `gatedWindow` 挡下的 live LPUSH 数 |
| `redis_bars_skipped_total` | Counter | 单调 LPUSH 因不比队头新而跳过的次数 |
| `dispatcher_kline_ready_suppressed_total{interval}` | Counter | gated 期间被 `gatedReady` 压住的 `kline_ready` 数 |
| `backfill_ready` | Gauge | 冷启动回补完成 = 1 |

探针（`internal/metrics` 增一个可注入的 readiness 判定）：

- `/healthz` — liveness，不变（进程活着即 200）。
- `/readyz` — **新增**：冷启动 `Backfiller.Ready()` 为真才 200，否则 503。

## 10. 配置项（`app.Config` / flag / env 新增）

| flag / env | 默认 | 说明 |
|---|---|---|
| `-backfill` / `CHOMOSYNCER_BACKFILL` | `true` | 总开关；`false` 退化为当前 forward-only 行为 |
| `-backfill-cold-start-lookback` / `..._BACKFILL_COLD_START_LOOKBACK` | `0`（= 自动按 `WindowSize×interval + 余量`） | 冷启动每 key 回补深度 |
| `-backfill-max-gap` / `..._BACKFILL_MAX_GAP` | `0`（= `WindowSize×interval`） | shard 重连回补封顶 |
| `-backfill-workers` / `..._BACKFILL_WORKERS` | `4` | 回补并发 |
| `-backfill-rest-rps` / `..._BACKFILL_REST_RPS` | `20` | REST 令牌桶速率 |
| `-backfill-gate-timeout` / `..._BACKFILL_GATE_TIMEOUT` | `5m` | 门控降级超时 |

## 11. 对现有代码的改动清单（最小侵入）

| 文件 / 包 | 改动 | 侵入度 |
|---|---|---|
| `internal/rediswin/writer.go` | `PushBarAndTrim` / `PushBarsAndTrim` 切单调 LPUSH Lua；新增 `RebuildWindow`；`PushBarAndTrim` 返回值语义（是否写入） | 中（本体逻辑，需回归现有单测 + 加 Lua 测试） |
| `internal/collector/collector.go` `shard.go` | `Config` 加 `OnGap func(GapEvent)`；shard 重连成功后组装并回调；`nil` 时行为不变 | 小 |
| `internal/collector/collector.go` | `SetSymbols` 已有 delta 能力；`app` 在 `OnChange` 侧做 symbol diff，不改 collector | 无 |
| `internal/metrics/metrics.go` | 加可注入的 `ReadinessFunc`；`/readyz` 路由 | 小 |
| `internal/dispatcher/*` | **不改**（经 `gatedWindow` / `gatedReady` 适配器） | 无 |
| `internal/app/app.go` | 装配 `backfill` + `windowgate`；`dispatcher.Sinks.Window/Ready` 换成 gate 包装版；`universe.OnChange` 里做 symbol diff → `Submit`；`collector.Config.OnGap = backfiller.HandleGap`；冷启动流程（§5.1）；`/readyz` 接 `backfiller.Ready` | 中（装配层，无业务逻辑） |
| 新增 `internal/windowgate/` | `Gate` + 两个适配器 | 新增 |
| 新增 `internal/backfill/` | `Backfiller` + `KlineFetcher`(REST) + `KlineStore`(CH 读) + 队列 + 限流 | 新增 |
| `docs/OPERATIONS.md` | 补 backfill 配置、`/readyz`、相关指标与告警 | 文档 |

## 12. 测试计划

**`internal/rediswin`**

- 单调 LPUSH：旧 / 同 `start_time` 的 Bar 被跳过；新的正常前插 + `LTRIM`。
- `RebuildWindow`：空窗口 → 建 N 根；已有窗口 → 全量替换；`bars` 超 N → `LTRIM` 到 N；`bars` 空 → 空 key。
- 并发：`RebuildWindow` 与 `PushBarAndTrim` 并发（真 Lua 原子性，miniredis）。

**`internal/windowgate`**

- `Hold` 后 `gatedWindow.PushBarAndTrim` 不落 Redis、计数；`Release` 后透传。
- `HeldInterval` 任一 key held → `gatedReady` 跳过。
- 幂等：重复 `Hold` / `Release`。

**`internal/backfill`**（fake `KlineFetcher` / `KlineStore` / miniredis / fake `chwriter`）

- 冷启动：CH 空 → REST N 根 → 写 CH → `LastBars` → `RebuildWindow` → `Release` → `Ready()` 变真。
- shard 重连：`HandleGap` → 只回补该 shard keys；`until-since < interval` 跳过；`> MaxGapWindow` 被封顶。
- 去抖：30s 内两次 `HandleGap` 合并成一个请求。
- 只取闭合：REST 响应含一根 `closeTime >= now` → 被过滤。
- 限速：注入 429 → 退避重试；`X-MBX-USED-WEIGHT-1M` 高 → 降速（可用 fake clock 断言间隔）。
- 降级：CH 读一直失败 → `GateTimeout` 后 `Release` + `backfill_gate_timeout_total`。
- 幂等：同一 `BackfillRequest` 跑两次，CH 行数不翻倍、窗口一致。

**`internal/metrics`**

- `/readyz`：`ReadinessFunc` 返回 false → 503；true → 200。

**集成（`scripts/` 增补）**

- 起栈 → 先灌一批历史进 CH（模拟已有数据）→ 启动 app → 断言：`/readyz` 由 503 转 200；
  `redis-cli LLEN kline:BTCUSDT:1h` 启动后**立即** ≈ 200（而非从 0 攒）；窗口首元素 `t` 与
  `SELECT max(start_time) FROM fapi_kline_1h FINAL WHERE symbol='BTCUSDT'` 对齐。
- 用 `iptables` / `docker network disconnect` 封某 shard 90s → 放开 → 断言 `backfill_requests_total{reason="shard_reconnect"}` +1、
  该 shard 某 symbol 的 CH 无洞、Redis 窗口 `t` 连续。

## 13. 分期实现建议

1. **P1**：`rediswin` 单调 LPUSH + `RebuildWindow`（+ 单测）。纯改进，可独立合入。
2. **P2**：`internal/windowgate`（Gate + 两适配器 + 单测）。
3. **P3**：`internal/backfill` 骨架 + `KlineFetcher`(REST + 限流) + `KlineStore`(CH 读) + worker pool + 单测。
4. **P4**：`app` 装配 + 冷启动流程 + `/readyz`。跑集成脚本。
5. **P5**：`collector.OnGap` + shard 重连触发 + universe_add 触发。
6. **P6（补丁）**：周期对账 `ReconcileScanner`。

## 14. 开放问题 / TODO

- [ ] `MaxGapWindow` 之外的深洞：是否需要 P6 之前的临时手段（人工触发全量回补 CLI）？
- [ ] `ColdStartLookback` 是否要支持「N 天」而非「N 根」，用于首次部署想要一段可回测历史的场景。
- [ ] `/readyz` 粒度：是否需要 per-interval readiness（1m 就绪先放行、1h 慢慢来）？
- [ ] 周期对账的「缺失检测」实现：`SELECT` 出连续 `start_time` 的 gap（`neighbor` / 数组 diff）在 500 symbol 规模的成本。
- [ ] `kline_ready` 抑制期是否要在 `Release` 后补发一次「恢复标记」事件（`recovered=true`）供 Python 区分冷启动首轮。
- [ ] REST `serverTime` 漂移：用本地 `now` 还是先 `GET /fapi/v1/time` 校准来判定「闭合」。
