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
	ReasonSweep          Reason = "sweep"     // periodic integrity sweep repairing holes in the archive
)

// Range is a half-open interval [From, To) of bar OPEN times.
type Range struct {
	From, To time.Time
}

// RangeOutcome is what happened to one requested Range in repair mode.
type RangeOutcome struct {
	Key     Key
	Range   Range
	Fetched int   // closed bars the exchange returned for the range
	Err     error // non-nil: the fetch failed (Fetched may still be > 0 for a partial page run)
}

// RepairResult is delivered on Request.Result when a repair request finishes.
type RepairResult struct {
	Outcomes []RangeOutcome
	// SkippedKeys counts keys that were not processed at all (already inflight,
	// queue full, or the backfiller was closing). Their ranges are simply retried
	// by the next sweep.
	SkippedKeys int
}

// Request is one unit of backfill work.
type Request struct {
	Keys   []Key
	Since  time.Time // zero: cold-start semantics (resume from CH max, floored by lookback)
	Until  time.Time // zero: now
	Reason Reason

	// Ranges switches the request to REPAIR mode: exactly these bar-open ranges are
	// fetched per key, instead of resuming from ClickHouse's max(start_time). Resume
	// only fills the tail, so a bar that was lost while later bars kept arriving (an
	// interior hole) is invisible to it; repair mode is how such holes get filled.
	Ranges map[Key][]Range
	// Result, if non-nil, receives the outcome of a repair request. It must be
	// buffered (size >= 1): the send never blocks.
	Result chan<- RepairResult

	skipped int // keys Submit dropped as already inflight; reported back in the result
}

func (r Request) deliver(res RepairResult) {
	if r.Result == nil {
		return
	}
	select {
	case r.Result <- res:
	default:
	}
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
	// (forward-only degrade). Default 5m. A negative value disables the bound
	// entirely: used by offline "backfill-only" runs, where there is no live
	// window to protect and a full historical pull can legitimately take hours.
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
	if r.gateTimeout == 0 {
		r.gateTimeout = 5 * time.Minute
	}
	// A negative gateTimeout is a deliberate sentinel ("no bound") and passes
	// through untouched.
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

// WaitColdStart blocks until the cold-start backfill has finished (Ready reports
// true) or ctx is done. Returns nil on completion, ctx.Err() on cancellation.
// Intended for offline "backfill-only" runs that exit once history is loaded.
func (b *Backfiller) WaitColdStart(ctx context.Context) error {
	if b.Ready() {
		return nil
	}
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if b.Ready() {
				return nil
			}
		}
	}
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
		req.deliver(RepairResult{SkippedKeys: len(req.Keys)})
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

	skipped := len(req.Keys) - len(fresh)
	if len(fresh) == 0 {
		if req.Reason == ReasonColdStart {
			b.coldStartDone.Store(true)
		}
		req.deliver(RepairResult{SkippedKeys: skipped})
		return
	}
	if b.gate != nil {
		for _, k := range fresh {
			b.gate.Hold(k.Symbol, k.Interval)
		}
	}
	req.Keys = fresh
	req.skipped = skipped
	if req.Ranges != nil { // keys were upper-cased above; normalise the range map to match
		nr := make(map[Key][]Range, len(req.Ranges))
		for k, v := range req.Ranges {
			nr[Key{Symbol: strings.ToUpper(k.Symbol), Interval: k.Interval}] = v
		}
		req.Ranges = nr
	}
	b.metrics.requests.WithLabelValues(string(req.Reason)).Inc()

	select {
	case b.reqCh <- req:
	default:
		b.releaseKeys(fresh)
		req.deliver(RepairResult{SkippedKeys: skipped + len(fresh)})
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
			req.deliver(RepairResult{SkippedKeys: len(req.Keys) + req.skipped})
		default:
			return
		}
	}
}

func (b *Backfiller) process(parent context.Context, req Request) {
	ctx := parent
	if b.cfg.gateTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(parent, b.cfg.gateTimeout)
		defer cancel()
	}
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
	var rec *recorder
	if req.Ranges != nil {
		rec = &recorder{}
		defer func() { req.deliver(RepairResult{Outcomes: rec.all(), SkippedKeys: req.skipped}) }()
	}
	for iv, symbols := range byIv {
		var repair map[string][]Range
		if req.Ranges != nil {
			repair = map[string][]Range{}
			for _, sym := range symbols {
				repair[sym] = req.Ranges[Key{Symbol: sym, Interval: iv}]
			}
		}
		b.processInterval(ctx, iv, symbols, req.Since, until, repair, rec)
	}

	b.metrics.duration.WithLabelValues(string(req.Reason)).Observe(b.now().Sub(start).Seconds())
	b.log.Info("backfill request done",
		"reason", req.Reason, "keys", len(req.Keys), "took", b.now().Sub(start).Round(time.Millisecond))
}

// recorder collects per-range outcomes of a repair request from concurrent workers.
type recorder struct {
	mu  sync.Mutex
	out []RangeOutcome
}

func (r *recorder) add(o RangeOutcome) {
	r.mu.Lock()
	r.out = append(r.out, o)
	r.mu.Unlock()
}

func (r *recorder) all() []RangeOutcome {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]RangeOutcome(nil), r.out...)
}

// fetchJob is one REST fetch: bars with open time in [since, until).
type fetchJob struct {
	sym          string
	since, until time.Time
	rng          *Range // non-nil in repair mode
}

func (b *Backfiller) processInterval(ctx context.Context, iv string, symbols []string, since, until time.Time, repair map[string][]Range, rec *recorder) {
	ivDur, ok := parseIntervalDuration(iv)
	if !ok {
		ivDur = time.Minute
	}
	aw := b.archive[iv]

	var chMax map[string]time.Time
	if b.store != nil && repair == nil { // repair mode fetches explicit ranges; no resume point needed
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

	// 1. plan the fetches: one per symbol (resume from the tail) or one per range (repair)
	var jobs []fetchJob
	if repair != nil {
		for _, sym := range symbols {
			for _, r := range repair[sym] {
				r := r
				jobs = append(jobs, fetchJob{sym: sym, since: r.From, until: r.To, rng: &r})
			}
		}
	} else {
		for _, sym := range symbols {
			s := sinceFor(sym)
			if until.Sub(s) < ivDur {
				continue // no closed bar in range
			}
			jobs = append(jobs, fetchJob{sym: sym, since: s, until: until})
		}
	}

	// fetch + archive, parallel
	sem := make(chan struct{}, b.cfg.workers)
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Done()
			wg.Wait()
			return
		}
		go func(j fetchJob) {
			defer wg.Done()
			defer func() { <-sem }()
			n, err := b.fetchAndArchive(ctx, iv, j.sym, j.since, j.until, aw)
			if rec != nil && j.rng != nil {
				rec.add(RangeOutcome{Key: Key{Symbol: j.sym, Interval: iv}, Range: *j.rng, Fetched: n, Err: err})
			}
		}(j)
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

	// 3. read the deduped tail back, rebuild windows. In repair mode only symbols
	//    whose repaired ranges reach into the Redis window need it.
	if repair != nil {
		windowStart := until.Add(-time.Duration(b.cfg.windowSize) * ivDur)
		var touched []string
		for _, sym := range symbols {
			for _, r := range repair[sym] {
				if r.To.After(windowStart) {
					touched = append(touched, sym)
					break
				}
			}
		}
		if symbols = touched; len(symbols) == 0 {
			return
		}
	}
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

// fetchAndArchive pulls [since, until) for one symbol into the archive and returns
// how many bars the exchange returned plus the fetch error, if any.
func (b *Backfiller) fetchAndArchive(ctx context.Context, iv, sym string, since, until time.Time, aw ArchiveWriter) (int, error) {
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
		return totalFetched, err
	}

	rows, err := b.fetch.Fetch(ctx, sym, iv, since, until)
	if err != nil {
		b.metrics.errors.WithLabelValues("rest").Inc()
		b.log.Warn("REST fetch failed", "symbol", sym, "interval", iv, "err", err)
		return 0, err
	}
	b.metrics.barsFetched.WithLabelValues(iv).Add(float64(len(rows)))
	if aw == nil || len(rows) == 0 {
		return len(rows), nil
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
	return len(rows), nil
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
		TradesCount:         int64(r.TradesCount),
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
