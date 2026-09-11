# 冷启动 / 回补 gate 批量释放:Redis 全局延迟 + kline_ready 永久丢失

- 状态: 待评估，未动手
- 发现日期: 2026-09-11
- 涉及模块: `internal/backfill`、`internal/windowgate`、`internal/dispatcher`
- 触发场景: 任意会调用 `Backfiller.Submit()` 的路径——在线冷启动(`SubmitColdStart`，一次性提交全市场)、断线重连补缺(`HandleGap`，`ReasonShardReconnect`)

---

## 一句话结论

`windowgate` 的 Hold/Release 粒度是**整个 Request**，不是单个币种。一次冷启动把全市场（当前 528 个）打成一个 Request；哪怕某个币种自己的回补第 1 秒就抓完了，也要陪着最慢的那个一起等，直到 `rest_rps` 限速下把整批 REST 请求做完才**一次性**全部释放。这带来两个不对称的后果：

- **ClickHouse**：完全不受影响，实时 WS 收盘 bar 全程直连落库（`aw.Push`/`TryPush` 不经过 gate）。
- **Redis 滑窗 `kline:{SYM}:1m`**：不是永久性 gap，但**全批次统一延迟**——先做完的币种也要等最慢的一起 release，release 那一刻靠 `RebuildWindow` 从 ClickHouse 整体重建，数据补得齐，只是"能在 Redis 里看到最新收盘 bar"这件事被拖后了。
- **`kline_ready` Stream**：不是延迟，是**永久丢失**——gate 持有期间收盘的那几分钟 1m 截面，其 `kline_ready` 事件被静默丢弃、不重试，批次结束后也不会补发。

---

## 代码证据

### 1. Gate 的 Hold/Release 语义（`internal/windowgate/gate.go`）

```go
// WrapWindow returns a WindowSink that drops pushes for held keys.
func (g *Gate) WrapWindow(inner dispatcher.WindowSink) dispatcher.WindowSink {
	return gatedWindow{g: g, inner: inner}
}

func (w gatedWindow) PushBarAndTrim(ctx context.Context, symbol, interval string, bar rediswin.CompactBar) error {
	if w.g.HeldWindow(symbol, interval) {
		w.g.metrics.windowSuppressed.WithLabelValues(interval).Inc()
		return nil   // <- 直接丢弃，不缓冲、不报错
	}
	return w.inner.PushBarAndTrim(ctx, symbol, interval, bar)
}

func (r gatedReady) PublishKlineReady(ctx context.Context, evt rediswin.KlineReadyEvent) (string, error) {
	if r.g.HeldInterval(evt.Interval) {
		r.g.metrics.readySuppressed.WithLabelValues(interval).Inc()
		return "", nil   // <- 对调用方而言"发布成功"，聚合器不会重试
	}
	return r.inner.PublishKlineReady(ctx, evt)
}
```

`HeldInterval` 是"这个 interval 下只要还有一个 key 被 hold 就全局压住"（[gate.go](../internal/windowgate/gate.go)），不是按 symbol 精确匹配：

```go
func (g *Gate) HeldInterval(interval string) bool {
	...
	return g.byInterv[interval] > 0
}
```

包注释自己也写明了不对称设计：

> While a key is held, live closed bars for it still flow to ClickHouse (the archive sink is not gated), so the subsequent RebuildWindow picks them up.

### 2. 释放粒度是整个 Request，不是单个币种（`internal/backfill/backfill.go`）

```go
func (b *Backfiller) process(parent context.Context, req Request) {
	...
	defer b.releaseKeys(req.Keys)   // <- 一次性释放这个 Request 里的全部 key
	...
	for iv, symbols := range byIv {
		b.processInterval(ctx, iv, symbols, req.Since, until)  // 内部 wg.Wait() 等全部 symbol 抓完
	}
	...
}
```

`processInterval` 内部用 `workers` 个 goroutine 并发抓，但共用同一个 `rest_rps` 令牌桶（见 `internal/backfill/rest.go`），所以整批的墙钟耗时 ≈ `len(symbols) / rest_rps`（外加分页数）。

`SubmitColdStart` 把**全市场一次性**打成一个 Request：

```go
func (b *Backfiller) SubmitColdStart(keys []Key) {
	...
	b.Submit(Request{Keys: keys, Reason: ReasonColdStart})
}
```

`run()` 是单 goroutine 串行处理 `reqCh`，同一时刻只处理一个 Request。

### 3. 实测数据（2026-09-11，本机，528 个币种，`rest_rps=3`）

冷启动重启后持续轮询 `/metrics`：

```
t+15s   fetched=2009  windowgate_held_keys=528  readyz=503
t+30s   fetched=2324  windowgate_held_keys=528  readyz=503
...
t+195s  fetched=3570  windowgate_held_keys=0    readyz=200
```

`windowgate_held_keys` 全程钉在 528，直到最后一刻才归零——印证"批量释放，不是抓完一个放一个"。

---

## 影响范围：不只是冷启动这一次性场景

`HeldInterval` 判的是 interval 维度，而 backfill 的 key 恒为 `Interval="1m"`（`collector.Intervals` 固定 `["1m"]`），所以这个"全压"只影响 **1m 的 `kline_ready`**；5m/15m/1h/4h/1d 的截面通知走独立的 `evt.Interval`，不受这个 gate 影响（它们的数据来自 ClickHouse rollup，本身也不经过 gate）。

但哪怕只看 1m：**日常的分片重连补缺（`ReasonShardReconnect`）也会触发同样的机制**——一个分片只占 ~66 个币种，哪怕只有 1 个币种在补几分钟的缺口，`HeldInterval("1m")` 依然为真，**全市场 528 个币种的 1m `kline_ready` 会一起被静默丢弃**，直到那一个币种补完。这是常态化风险，不是只在重启这一次性场景里才发生。

---

## 改进方案（三选一或组合，未实施）

### 方案 1：Release 粒度从"整批"改成"逐币种"
每个 symbol 自己的 `fetchAndArchive` + `RebuildWindow` 一做完就单独 `Release` 那一个 key，不等整批。
- 优点：最坏延迟从"批次总时长"降到"这个币种自己的抓取时间"。
- 改动位置：`processInterval` / `process`（`internal/backfill/backfill.go`），中等大小，涉及并发释放逻辑调整。
- 不能解决 `kline_ready` "丢失不重试"的问题（只要还有任何一个 symbol 没完成，`HeldInterval` 依然为真）。

### 方案 2：`kline_ready` 改成"压住就重试"而不是"压住就丢"
`gatedReady` 返回一个可识别的 sentinel（或在 aggregator 侧缓存被压住的截面事件），gate 释放后补发。
- 优点：根治"永久丢事件"这个更尖锐的问题。
- 改动位置：`internal/windowgate/gate.go` + `internal/dispatcher/aggregator.go`，改动较大，涉及聚合器状态与重放逻辑。

### 方案 3（推荐先做）：`SubmitColdStart` 按分片拆批提交
把一次性提交全市场，改成按 8 个分片分批提交（每批 ~66 个），不用碰 `windowgate` 内部逻辑。
- 优点：风险最低、改动最小（只在 `internal/app` 的调用处拆分批次），最坏延迟天然砍到 1/8。
- 缺点：不是根治，`kline_ready` 丢失问题依然存在，只是窗口更短。

---

## 相关文件索引

- [internal/windowgate/gate.go](../internal/windowgate/gate.go)
- [internal/backfill/backfill.go](../internal/backfill/backfill.go)（`Submit` / `process` / `processInterval` / `SubmitColdStart` / `HandleGap`）
- [internal/dispatcher/aggregator.go](../internal/dispatcher/aggregator.go)（`mark` / `PublishKlineReady` 调用点）
- [internal/app/app.go](../internal/app/app.go)（gate 装配位置 `WrapWindow` / `WrapReady`，`SubmitColdStart` 调用点）
