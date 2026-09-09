// Package backfill fills historical-kline gaps and reconciles the Redis rolling
// window against the ClickHouse ledger, in parallel with (and decoupled from)
// the live incremental collector.
//
// Triggers (all funnel into Submit): cold start (whole universe), shard
// reconnect (that shard's keys, bounded window), universe add (new listings).
// Flow per request: hold the window gate for the keys -> REST /fapi/v1/klines
// -> chwriter.Push (ClickHouse, idempotent via ReplacingMergeTree) -> wait for
// the CH flush -> read the deduped tail back from CH -> RebuildWindow in Redis
// -> release the gate. kline_ready stays suppressed for a held interval; the
// dispatcher and rediswin are untouched (the gate is an app-layer adapter).
package backfill

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/HarvestStars/chomosyncer-go/internal/chwriter"
	"github.com/HarvestStars/chomosyncer-go/internal/rediswin"
)

// Key is a (symbol, interval). Symbol is stored upper-cased.
type Key struct {
	Symbol   string
	Interval string
}

// Reason labels a backfill request's trigger.
type Reason string

const (
	ReasonColdStart      Reason = "cold_start"
	ReasonShardReconnect Reason = "shard_reconnect"
	ReasonUniverseAdd    Reason = "universe_add"
	ReasonReconcile      Reason = "reconcile" // TODO: not emitted yet
)

// Request is one unit of backfill work.
type Request struct {
	Keys   []Key
	Since  time.Time // zero: cold-start semantics (resume from CH max, floored by lookback)
	Until  time.Time // zero: now
	Reason Reason
}

// GapEvent is what a shard reconnect reports (structurally mirrors
// collector.GapEvent; the app converts).
type GapEvent struct {
	ShardID     string
	Streams     []string
	LastMsgAt   time.Time
	ReconnectAt time.Time
}

// --- injected dependencies ---

// KlineFetcher pulls closed bars from the exchange REST API.
type KlineFetcher interface {
	Fetch(ctx context.Context, symbol, interval string, since, until time.Time) ([]chwriter.Row, error)
}

// StreamFetcher is optionally implemented by KlineFetcher to stream batches
// of rows as they are fetched from the exchange, reducing memory usage and
// ensuring partial progress is persisted even if later pages fail or time out.
type StreamFetcher interface {
	FetchStream(ctx context.Context, symbol, interval string, since, until time.Time, onBatch func([]chwriter.Row) error) error
}

// ArchiveWriter is the per-interval ClickHouse writer. *chwriter.BatchWriter satisfies it.
type ArchiveWriter interface {
	Push(ctx context.Context, row chwriter.Row) error
	Flush(ctx context.Context) error
}

// KlineStore reads back from ClickHouse.
type KlineStore interface {
	MaxStartTime(ctx context.Context, interval string, symbols []string) (map[string]time.Time, error)
	LastBars(ctx context.Context, interval string, symbols []string, n int) (map[string][]chwriter.Row, error)
}

// WindowRebuilder replaces a Redis window wholesale. *rediswin.Writer satisfies it.
type WindowRebuilder interface {
	RebuildWindow(ctx context.Context, symbol, interval string, barsNewestFirst []rediswin.CompactBar) error
}

// Gate holds / releases the window suppression for a key. *windowgate.Gate is
// adapted to this by the app.
type Gate interface {
	Hold(symbol, interval string)
	Release(symbol, interval string)
}

// --- config ---

type Config struct {
	// ColdStartTime is the initial historical backfill start time. If set,
	// empty ClickHouse tables backfill from this time up to now.
	ColdStartTime time.Time
	// GapDebounce coalesces repeated gap events from the same shard. Default 30s.
	GapDebounce time.Duration
	// Workers is the per-request parallel REST fetch count. Default 4.
	Workers int
	// QueueSize is the request channel capacity. Default 256.
	QueueSize int
	// WindowSize is the Redis window length. Default 200.
	WindowSize int
	// GateTimeout bounds one request; on expiry keys are force-released
	// (forward-only degrade). Default 5m.
	GateTimeout time.Duration
	// FlushWait is how long to wait after archiving before reading CH back
	// (covers the chwriter flush interval). Default 2s.
	FlushWait time.Duration

	Clock      func() time.Time
	Registerer prometheus.Registerer
	Logger     *slog.Logger
}

type resolved struct {
	coldStartTime time.Time
	gapDebounce   time.Duration
	workers       int
	queueSize     int
	windowSize    int
	gateTimeout   time.Duration
	flushWait     time.Duration
}

func (c Config) resolve() resolved {
	r := resolved{
		coldStartTime: c.ColdStartTime,
		gapDebounce:   c.GapDebounce,
		workers:       c.Workers,
		queueSize:     c.QueueSize,
		windowSize:    c.WindowSize,
		gateTimeout:   c.GateTimeout,
		flushWait:     c.FlushWait,
	}
	if r.gapDebounce <= 0 {
		r.gapDebounce = 30 * time.Second
	}
	if r.workers <= 0 {
		r.workers = 4
	}
	if r.queueSize <= 0 {
		r.queueSize = 256
	}
	if r.windowSize <= 0 {
		r.windowSize = 200
	}
	if r.gateTimeout <= 0 {
		r.gateTimeout = 5 * time.Minute
	}
	if r.flushWait <= 0 {
		r.flushWait = 2 * time.Second
	}
	return r
}

// --- backfiller ---

type Backfiller struct {
	cfg     resolved
	fetch   KlineFetcher
	archive map[string]ArchiveWriter
	store   KlineStore
	rebuild WindowRebuilder
	gate    Gate
	now     func() time.Time
	log     *slog.Logger
	metrics *metrics

	reqCh     chan Request
	stopCh    chan struct{}
	wg        sync.WaitGroup
	runCancel context.CancelFunc

	closeOnce sync.Once
	closed    atomic.Bool

	mu       sync.Mutex
	inflight map[Key]struct{}
	gapSeen  map[string]time.Time

	coldStartSubmitted atomic.Bool
	coldStartDone      atomic.Bool
}

// New builds a Backfiller. store / rebuild / gate may be nil (degraded: no CH
// readback / no Redis rebuild / no gating — useful for tests and reconcile-only
// modes). archive maps interval -> writer.
func New(cfg Config, fetch KlineFetcher, archive map[string]ArchiveWriter, store KlineStore, rebuild WindowRebuilder, gate Gate) (*Backfiller, error) {
	if fetch == nil {
		return nil, errors.New("backfill: KlineFetcher is required")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	nowFn := cfg.Clock
	if nowFn == nil {
		nowFn = time.Now
	}
	r := cfg.resolve()
	b := &Backfiller{
		cfg:      r,
		fetch:    fetch,
		archive:  archive,
		store:    store,
		rebuild:  rebuild,
		gate:     gate,
		now:      nowFn,
		log:      log.With("component", "backfill"),
		reqCh:    make(chan Request, r.queueSize),
		stopCh:   make(chan struct{}),
		inflight: map[Key]struct{}{},
		gapSeen:  map[string]time.Time{},
	}
	b.metrics = newMetrics(cfg.Registerer, b.Ready)
	return b, nil
}

// Start launches the single request runner. Per-request REST fetches fan out to
// Config.Workers goroutines.
func (b *Backfiller) Start(ctx context.Context) {
	rctx, cancel := context.WithCancel(ctx)
	b.runCancel = cancel
	b.wg.Add(1)
	go b.run(rctx)
}

// Close cancels any in-flight request, stops the runner, and waits for it.
func (b *Backfiller) Close() error {
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		if b.runCancel != nil {
			b.runCancel()
		}
		close(b.stopCh)
	})
	b.wg.Wait()
	return nil
}

// Ready reports whether the cold-start backfill has finished (or was never
// needed). Feeds /readyz.
func (b *Backfiller) Ready() bool {
	return !b.coldStartSubmitted.Load() || b.coldStartDone.Load()
}

// SubmitColdStart enqueues the whole-universe cold-start request and arms Ready().
func (b *Backfiller) SubmitColdStart(keys []Key) {
	b.coldStartSubmitted.Store(true)
	if len(keys) == 0 {
		b.coldStartDone.Store(true)
		return
	}
	b.Submit(Request{Keys: keys, Reason: ReasonColdStart})
}

// Submit enqueues a request. Keys already inflight are filtered out; the gate is
// held immediately for the remaining keys.
func (b *Backfiller) Submit(req Request) {
	if b.closed.Load() {
		return
	}
	b.mu.Lock()
	fresh := make([]Key, 0, len(req.Keys))
	for _, k := range req.Keys {
		nk := Key{Symbol: strings.ToUpper(k.Symbol), Interval: k.Interval}
		if nk.Symbol == "" || nk.Interval == "" {
			continue
		}
		if _, dup := b.inflight[nk]; dup {
			continue
		}
		b.inflight[nk] = struct{}{}
		fresh = append(fresh, nk)
	}
	b.mu.Unlock()

	if len(fresh) == 0 {
		if req.Reason == ReasonColdStart {
			b.coldStartDone.Store(true)
		}
		return
	}
	if b.gate != nil {
		for _, k := range fresh {
			b.gate.Hold(k.Symbol, k.Interval)
		}
	}
	req.Keys = fresh
	b.metrics.requests.WithLabelValues(string(req.Reason)).Inc()

	select {
	case b.reqCh <- req:
	default:
		b.releaseKeys(fresh)
		b.metrics.dropped.Inc()
		b.log.Error("backfill queue full; request dropped", "reason", req.Reason, "keys", len(fresh))
		if req.Reason == ReasonColdStart {
			b.coldStartDone.Store(true) // don't wedge readiness
		}
	}
}

// HandleGap turns a shard-reconnect gap into a Submit, with per-shard debounce
// and a "no bar could have closed" skip.
func (b *Backfiller) HandleGap(ev GapEvent) {
	if b.closed.Load() {
		return
	}
	b.mu.Lock()
	if last, ok := b.gapSeen[ev.ShardID]; ok && b.now().Sub(last) < b.cfg.gapDebounce {
		b.mu.Unlock()
		return
	}
	b.gapSeen[ev.ShardID] = b.now()
	b.mu.Unlock()

	keys := make([]Key, 0, len(ev.Streams))
	for _, s := range ev.Streams {
		if k, ok := parseStream(s); ok {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return
	}
	until := ev.ReconnectAt
	if until.IsZero() {
		until = b.now()
	}
	b.Submit(Request{Keys: keys, Since: ev.LastMsgAt, Until: until, Reason: ReasonShardReconnect})
}

func (b *Backfiller) run(ctx context.Context) {
	defer b.wg.Done()
	for {
		select {
		case <-ctx.Done():
			b.drainRelease()
			return
		case <-b.stopCh:
			b.drainRelease()
			return
		case req := <-b.reqCh:
			b.process(ctx, req)
		}
	}
}

// drainRelease releases the gate for anything still queued at shutdown.
func (b *Backfiller) drainRelease() {
	for {
		select {
		case req := <-b.reqCh:
			b.releaseKeys(req.Keys)
		default:
			return
		}
	}
}

func (b *Backfiller) process(parent context.Context, req Request) {
	ctx, cancel := context.WithTimeout(parent, b.cfg.gateTimeout)
	defer cancel()
	defer b.releaseKeys(req.Keys)
	if req.Reason == ReasonColdStart {
		defer b.coldStartDone.Store(true)
	}

	start := b.now()
	until := req.Until
	if until.IsZero() {
		until = b.now()
	}

	byIv := map[string][]string{}
	for _, k := range req.Keys {
		byIv[k.Interval] = append(byIv[k.Interval], k.Symbol)
	}
	for iv, symbols := range byIv {
		b.processInterval(ctx, iv, symbols, req.Since, until)
	}

	b.metrics.duration.WithLabelValues(string(req.Reason)).Observe(b.now().Sub(start).Seconds())
	b.log.Info("backfill request done",
		"reason", req.Reason, "keys", len(req.Keys), "took", b.now().Sub(start).Round(time.Millisecond))
}

func (b *Backfiller) processInterval(ctx context.Context, iv string, symbols []string, since, until time.Time) {
	ivDur, ok := parseIntervalDuration(iv)
	if !ok {
		ivDur = time.Minute
	}
	aw := b.archive[iv]

	var chMax map[string]time.Time
	if b.store != nil {
		m, err := b.store.MaxStartTime(ctx, iv, symbols)
		if err != nil {
			b.metrics.errors.WithLabelValues("ch_max").Inc()
			b.log.Warn("MaxStartTime failed; using full lookback", "interval", iv, "err", err)
		} else {
			chMax = m
		}
	}
	sinceFor := func(sym string) time.Time {
		if m, ok := chMax[sym]; ok && !m.IsZero() {
			// 2 & 3: 只要 ClickHouse 有历史落档（无论冷启动还是中断重连），
			// 直接以 ClickHouse 档案最新时间 + 1 interval 作为同步起点，保证 100% 零 gap
			return m.Add(ivDur)
		}
		// 1: 空库或该交易对在 ClickHouse 中无落档，直接使用 coldStartTime 作为启动锚点
		if !b.cfg.coldStartTime.IsZero() {
			return b.cfg.coldStartTime
		}
		// 降级兜底（未配置 coldStartTime 或单测无 store 场景）：若有 since 则按 since，否则按 windowSize 回溯
		if !since.IsZero() {
			return since.Add(-ivDur)
		}
		return until.Add(-time.Duration(b.cfg.windowSize) * ivDur)
	}

	// 1. fetch + archive, parallel
	sem := make(chan struct{}, b.cfg.workers)
	var wg sync.WaitGroup
	for _, sym := range symbols {
		s := sinceFor(sym)
		if until.Sub(s) < ivDur {
			continue // no closed bar in range
		}
		wg.Add(1)
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Done()
			return
		}
		go func(sym string, s time.Time) {
			defer wg.Done()
			defer func() { <-sem }()
			b.fetchAndArchive(ctx, iv, sym, s, until, aw)
		}(sym, s)
	}
	wg.Wait()

	if b.store == nil || b.rebuild == nil {
		return
	}

	// 2. flush chwriter to guarantee all rows are on ClickHouse before reading back
	if aw != nil {
		if err := aw.Flush(ctx); err != nil {
			b.metrics.errors.WithLabelValues("archive").Inc()
			b.log.Warn("archive flush failed before rebuild", "interval", iv, "err", err)
			return
		}
	} else if b.cfg.flushWait > 0 {
		select {
		case <-time.After(b.cfg.flushWait):
		case <-ctx.Done():
			return
		}
	}

	// 3. read the deduped tail back, rebuild windows
	last, err := b.store.LastBars(ctx, iv, symbols, b.cfg.windowSize)
	if err != nil {
		b.metrics.errors.WithLabelValues("ch_read").Inc()
		b.log.Warn("LastBars failed; windows not rebuilt", "interval", iv, "err", err)
		return
	}
	for _, sym := range symbols {
		rows := last[sym] // ascending
		bars := make([]rediswin.CompactBar, len(rows))
		for i := range rows {
			bars[len(rows)-1-i] = rowToCompact(rows[i]) // newest-first
		}
		if err := b.rebuild.RebuildWindow(ctx, sym, iv, bars); err != nil {
			b.metrics.errors.WithLabelValues("rebuild").Inc()
			b.log.Warn("RebuildWindow failed", "symbol", sym, "interval", iv, "err", err)
			continue
		}
		b.metrics.windowsRebuilt.WithLabelValues(iv).Inc()
	}
}

func (b *Backfiller) fetchAndArchive(ctx context.Context, iv, sym string, since, until time.Time, aw ArchiveWriter) {
	if sf, ok := b.fetch.(StreamFetcher); ok {
		var totalFetched, totalWritten int
		err := sf.FetchStream(ctx, sym, iv, since, until, func(batch []chwriter.Row) error {
			totalFetched += len(batch)
			if aw == nil {
				return nil
			}
			for i := range batch {
				if err := aw.Push(ctx, batch[i]); err != nil {
					b.metrics.errors.WithLabelValues("archive").Inc()
					b.log.Warn("archive push failed mid-backfill", "symbol", sym, "interval", iv, "err", err)
					return err
				}
				totalWritten++
			}
			return nil
		})
		b.metrics.barsFetched.WithLabelValues(iv).Add(float64(totalFetched))
		b.metrics.barsWritten.WithLabelValues(iv).Add(float64(totalWritten))
		if err != nil {
			b.metrics.errors.WithLabelValues("rest").Inc()
			b.log.Warn("REST fetch failed", "symbol", sym, "interval", iv, "err", err)
		}
		return
	}

	rows, err := b.fetch.Fetch(ctx, sym, iv, since, until)
	if err != nil {
		b.metrics.errors.WithLabelValues("rest").Inc()
		b.log.Warn("REST fetch failed", "symbol", sym, "interval", iv, "err", err)
		return
	}
	b.metrics.barsFetched.WithLabelValues(iv).Add(float64(len(rows)))
	if aw == nil || len(rows) == 0 {
		return
	}
	written := 0
	for i := range rows {
		if err := aw.Push(ctx, rows[i]); err != nil {
			b.metrics.errors.WithLabelValues("archive").Inc()
			b.log.Warn("archive push failed mid-backfill", "symbol", sym, "interval", iv, "err", err)
			break
		}
		written++
	}
	b.metrics.barsWritten.WithLabelValues(iv).Add(float64(written))
}

func (b *Backfiller) releaseKeys(keys []Key) {
	if len(keys) == 0 {
		return
	}
	b.mu.Lock()
	for _, k := range keys {
		delete(b.inflight, k)
	}
	b.mu.Unlock()
	if b.gate != nil {
		for _, k := range keys {
			b.gate.Release(k.Symbol, k.Interval)
		}
	}
}

func rowToCompact(r chwriter.Row) rediswin.CompactBar {
	return rediswin.CompactBar{
		StartTime:           r.StartTime.UnixMilli(),
		Open:                r.Open,
		High:                r.High,
		Low:                 r.Low,
		Close:               r.Close,
		Volume:              r.Volume,
		QuoteVolume:         r.QuoteVolume,
		TakerBuyVolume:      r.TakerBuyVolume,
		TakerBuyQuoteVolume: r.TakerBuyQuoteVolume,
	}
}

// parseStream turns "btcusdt@kline_1m" into Key{BTCUSDT, 1m}.
func parseStream(s string) (Key, bool) {
	at := strings.IndexByte(s, '@')
	if at < 1 {
		return Key{}, false
	}
	iv, ok := strings.CutPrefix(s[at+1:], "kline_")
	if !ok || iv == "" {
		return Key{}, false
	}
	return Key{Symbol: strings.ToUpper(s[:at]), Interval: iv}, true
}
