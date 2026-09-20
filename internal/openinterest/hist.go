package openinterest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"
)

// HistConfig tunes the hist reconciler. Zero values take the defaults.
type HistConfig struct {
	Interval        time.Duration // period of the scheduled calibration pass (default 1h)
	Offset          time.Duration // pass starts at hh:Offset of each period (default 5m)
	Spread          time.Duration // a scheduled pass spreads its requests evenly over this (default 20m)
	PublishLag      time.Duration // a label T is expected readable this long after T (default 4m)
	LimitMargin     int           // extra bars requested beyond the gap (default 3)
	MaxLimit        int           // largest `limit` per request (default 500, Binance's documented max)
	ColdStartWindow time.Duration // symbols with no calibrated rows are backfilled this far (default 48h)
	MaxBackfill     time.Duration // never reach back further than this; older gaps need the archive (default 168h)
	Workers         int           // concurrent symbol fetches (default 4)
	RequestTimeout  time.Duration // per request (default 15s)
}

func (c HistConfig) withDefaults() HistConfig {
	if c.Interval <= 0 {
		c.Interval = time.Hour
	}
	if c.Offset <= 0 {
		c.Offset = 5 * time.Minute
	}
	if c.Spread <= 0 {
		c.Spread = 20 * time.Minute
	}
	if c.PublishLag <= 0 {
		c.PublishLag = 4 * time.Minute
	}
	if c.LimitMargin <= 0 {
		c.LimitMargin = 3
	}
	if c.MaxLimit <= 0 {
		c.MaxLimit = 500
	}
	if c.ColdStartWindow <= 0 {
		c.ColdStartWindow = 48 * time.Hour
	}
	if c.MaxBackfill <= 0 {
		c.MaxBackfill = 168 * time.Hour
	}
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 15 * time.Second
	}
	return c
}

// PassStats summarises one hist pass.
type PassStats struct {
	Symbols    int // symbols in the universe
	Skipped    int // already up to date, no request made
	Fetched    int // symbols fetched successfully (including those with no data)
	Failed     int // symbols still failing after the retry
	Rows       int // rows pushed
	BeyondCap  int // symbols whose gap reached past MaxBackfill
	Committed  bool
	Duration   time.Duration
	ColdStart  int // symbols that had no calibrated rows at all
	LiveGapBar int // bars hist returned that live had no snapshot for
}

// Reconciler brings each symbol's calibrated (hist) rows up to date. The same
// pass serves as the start-up catch-up and the hourly calibration: for each symbol
// it asks how far the database is calibrated and fetches only what is missing.
//
// State: last[sym] is the start_time of the newest row with src_rank >= 2 in the
// database. It is loaded from ClickHouse once at start-up and kept in memory after
// that, updated only once a pass's rows have been flushed.
type Reconciler struct {
	cfg     HistConfig
	src     HistSource
	store   Store
	sink    RowSink
	symbols func() []string
	live    *liveCache
	m       *metrics
	log     *slog.Logger
	now     func() time.Time
	sleep   func(ctx context.Context, d time.Duration) error

	mu        sync.Mutex
	last      map[string]time.Time
	liveSince time.Time // when live snapshots started; bars before it are not counted as live gaps
}

// NewReconciler builds a reconciler. live may be nil (no live/hist comparison).
func NewReconciler(cfg HistConfig, src HistSource, store Store, sink RowSink, symbols func() []string, live *liveCache, m *metrics, log *slog.Logger) *Reconciler {
	if m == nil {
		m = newMetrics(nil)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Reconciler{
		cfg: cfg.withDefaults(), src: src, store: store, sink: sink, symbols: symbols, live: live, m: m,
		log:   log.With("component", "oi_hist"),
		now:   time.Now,
		sleep: sleepCtx,
		last:  map[string]time.Time{},
	}
}

// SetLiveSince tells the reconciler when live snapshots began, so that bars before
// that are not reported as live gaps.
func (r *Reconciler) SetLiveSince(t time.Time) {
	r.mu.Lock()
	r.liveSince = t
	r.mu.Unlock()
}

// LoadState reads from the database how far each symbol is calibrated. It retries
// until it succeeds or ctx ends, because without it a start-up pass cannot decide
// what to skip.
func (r *Reconciler) LoadState(ctx context.Context) error {
	backoff := 5 * time.Second
	for {
		syms := r.symbols()
		got, err := r.store.LastStarts(ctx, syms)
		if err == nil {
			r.mu.Lock()
			withHist, liveOnly := 0, 0
			for _, s := range syms {
				ls, ok := got[s]
				switch {
				case ok && !ls.Hist.IsZero():
					r.last[s] = ls.Hist
					withHist++
				case ok && !ls.Any.IsZero():
					liveOnly++ // only live rows so far: nothing calibrated yet
				}
			}
			r.mu.Unlock()
			r.log.Info("loaded open-interest state from ClickHouse",
				"symbols", len(syms), "calibrated", withHist, "live_rows_only", liveOnly, "no_rows", len(syms)-withHist-liveOnly)
			return nil
		}
		r.log.Warn("could not read open-interest state; will retry", "err", err, "in", backoff)
		if serr := r.sleep(ctx, backoff); serr != nil {
			return serr
		}
		if backoff *= 2; backoff > time.Minute {
			backoff = time.Minute
		}
	}
}

// Run loads the state, runs the start-up pass immediately (no spreading: it is
// paced only by the /futures/data pool), then a scheduled pass every Interval at
// hh:Offset. It returns when ctx ends.
func (r *Reconciler) Run(ctx context.Context) {
	if err := r.LoadState(ctx); err != nil {
		return
	}
	r.runPass(ctx, 0, "startup")
	for ctx.Err() == nil {
		next := nextReconcileTime(r.now(), r.cfg.Interval, r.cfg.Offset)
		if err := r.sleep(ctx, next.Sub(r.now())); err != nil {
			return
		}
		r.runPass(ctx, r.cfg.Spread, "scheduled")
	}
}

func (r *Reconciler) runPass(ctx context.Context, spread time.Duration, reason string) {
	st, err := r.Pass(ctx, spread)
	if err != nil && ctx.Err() == nil {
		r.log.Error("open-interest hist pass failed", "reason", reason, "err", err)
		return
	}
	r.log.Info("open-interest hist pass",
		"reason", reason, "symbols", st.Symbols, "skipped", st.Skipped, "fetched", st.Fetched, "failed", st.Failed,
		"rows", st.Rows, "cold_start", st.ColdStart, "beyond_cap", st.BeyondCap, "live_gap_bars", st.LiveGapBar,
		"committed", st.Committed, "took", st.Duration.Round(time.Second))
}

// nextReconcileTime is the next hh:offset strictly after now.
func nextReconcileTime(now time.Time, interval, offset time.Duration) time.Time {
	t := now.UTC().Truncate(interval).Add(offset)
	if !t.After(now) {
		t = t.Add(interval)
	}
	return t
}

type histJob struct {
	symbol string
	since  time.Time // start_time of the oldest bar to (re)fetch
	needed int       // labels wanted: since+5m .. floorBar(now)
	fresh  bool      // the symbol had no calibrated rows
}

type jobResult struct {
	rows   int
	newest time.Time // newest bar start seen
	gaps   int
	err    error
}

// plan decides, per symbol, whether hist has to be called and how far back.
//
//	calibrated up to lh, target = newest bar expected published:
//	  lh >= target        -> skip (nothing new to ask for)
//	  otherwise           -> re-fetch from lh (one bar of overlap) to now
//	no calibrated rows    -> fetch the cold-start window
//	either way never earlier than now - MaxBackfill (older needs the archive).
func (r *Reconciler) plan(now time.Time) (jobs []histJob, st PassStats) {
	syms := r.symbols()
	st.Symbols = len(syms)
	floorNow := floorBar(now)
	target := newestPublishedStart(now, r.cfg.PublishLag)
	capStart := floorBar(now.Add(-r.cfg.MaxBackfill))

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range syms {
		lh, ok := r.last[s]
		var since time.Time
		fresh := !ok
		if fresh {
			since = floorNow.Add(-r.cfg.ColdStartWindow)
			st.ColdStart++
		} else {
			if !lh.Before(target) {
				st.Skipped++
				continue
			}
			since = lh
		}
		if since.Before(capStart) {
			since = capStart
			st.BeyondCap++
			r.m.histBeyondCap.Inc()
		}
		needed := int(floorNow.Sub(since) / BarInterval)
		if needed < 1 {
			st.Skipped++
			continue
		}
		jobs = append(jobs, histJob{symbol: s, since: since, needed: needed, fresh: fresh})
	}
	// biggest gaps first, so a rate-limit pause leaves only the least important undone
	sort.SliceStable(jobs, func(i, j int) bool { return jobs[i].needed > jobs[j].needed })
	return jobs, st
}

// Pass runs one pass. spread > 0 spaces the symbols' first requests evenly over
// that duration; 0 sends them as fast as the /futures/data pool allows.
func (r *Reconciler) Pass(ctx context.Context, spread time.Duration) (PassStats, error) {
	start := r.now()
	jobs, st := r.plan(start)
	r.m.histSymbols.WithLabelValues("skipped").Add(float64(st.Skipped))

	results := map[string]jobResult{}
	failed := r.runJobs(ctx, jobs, spread, results)
	if len(failed) > 0 && ctx.Err() == nil {
		r.log.Warn("retrying failed symbols once", "count", len(failed))
		failed = r.runJobs(ctx, failed, 0, results)
	}
	if err := ctx.Err(); err != nil {
		return st, err
	}

	// Commit the in-memory state only once the rows are really flushed: otherwise a
	// dropped batch would leave the state ahead of the database for good.
	committed := true
	if f, ok := r.sink.(interface{ Flush(context.Context) error }); ok {
		if err := f.Flush(ctx); err != nil {
			committed = false
			r.log.Error("flush after hist pass failed; state not advanced, the next pass will refetch", "err", err)
		}
	}
	failedSet := map[string]struct{}{}
	for _, j := range failed {
		failedSet[j.symbol] = struct{}{}
	}
	r.mu.Lock()
	for _, j := range jobs {
		res := results[j.symbol]
		if _, bad := failedSet[j.symbol]; bad {
			st.Failed++
			continue
		}
		st.Fetched++
		st.Rows += res.rows
		st.LiveGapBar += res.gaps
		if committed && !res.newest.IsZero() && res.newest.After(r.last[j.symbol]) {
			r.last[j.symbol] = res.newest
		}
	}
	current := 0
	target := newestPublishedStart(r.now(), r.cfg.PublishLag)
	for _, s := range r.symbols() {
		if lh, ok := r.last[s]; ok && !lh.Before(target) {
			current++
		}
	}
	r.mu.Unlock()
	if st.Symbols > 0 {
		r.m.crossSection.Set(float64(current) / float64(st.Symbols))
	}

	st.Committed = committed
	st.Duration = r.now().Sub(start)
	r.m.histSymbols.WithLabelValues("fetched").Add(float64(st.Fetched))
	r.m.histSymbols.WithLabelValues("failed").Add(float64(st.Failed))
	r.m.histPassSeconds.Observe(st.Duration.Seconds())
	r.m.histLastPass.Set(float64(r.now().Unix()))
	return st, nil
}

// runJobs fetches every job with a small worker pool. Results are stored into out;
// the jobs that failed are returned.
func (r *Reconciler) runJobs(ctx context.Context, jobs []histJob, spread time.Duration, out map[string]jobResult) (failed []histJob) {
	if len(jobs) == 0 {
		return nil
	}
	var step time.Duration
	if spread > 0 {
		step = spread / time.Duration(len(jobs))
	}
	pc := &pacer{step: step, next: r.now(), now: r.now, sleep: r.sleep}

	ch := make(chan histJob)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < r.cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range ch {
				if pc.wait(ctx) != nil {
					return
				}
				res := r.fetchSymbol(ctx, j)
				mu.Lock()
				if res.err != nil {
					failed = append(failed, j)
				} else {
					out[j.symbol] = res
				}
				mu.Unlock()
			}
		}()
	}
feed:
	for _, j := range jobs {
		select {
		case ch <- j:
		case <-ctx.Done():
			break feed
		}
	}
	close(ch)
	wg.Wait()
	return failed
}

// fetchSymbol pages BACKWARDS through the symbol's gap. openInterestHist returns
// the newest `limit` points inside [startTime, endTime], so after a full page the
// next request moves endTime to just before the oldest label received.
func (r *Reconciler) fetchSymbol(ctx context.Context, j histJob) jobResult {
	now := r.now()
	startLabel := j.since.Add(BarInterval) // the label of bar `since`
	end := now
	limit := minInt(j.needed+r.cfg.LimitMargin, r.cfg.MaxLimit)

	var res jobResult
	maxPages := j.needed/maxInt(r.cfg.MaxLimit-r.cfg.LimitMargin, 1) + 3
	for page := 0; page < maxPages; page++ {
		rctx, cancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
		pts, err := r.src.History(rctx, j.symbol, startLabel, end, limit)
		cancel()
		if err != nil {
			if errors.Is(err, ErrInvalidSymbol) { // delisted between the universe refresh and now
				r.m.histRequests.WithLabelValues("invalid_symbol").Inc()
				return res
			}
			r.m.histRequests.WithLabelValues("error").Inc()
			r.log.Warn("hist request failed", "symbol", j.symbol, "err", err)
			res.err = err
			return res
		}
		if len(pts) == 0 {
			r.m.histRequests.WithLabelValues("empty").Inc()
			return res // e.g. a symbol listed a moment ago
		}
		r.m.histRequests.WithLabelValues("ok").Inc()
		if err := r.emit(ctx, j.symbol, pts, now, &res); err != nil {
			res.err = err
			return res
		}
		if len(pts) < limit {
			return res // the range held fewer points than we allowed for: done
		}
		oldest := pts[0].Label
		end = oldest.Add(-time.Millisecond)
		if end.Before(startLabel) {
			return res
		}
		remaining := int(oldest.Sub(startLabel)/BarInterval) + 1
		limit = minInt(remaining+r.cfg.LimitMargin, r.cfg.MaxLimit)
	}
	res.err = fmt.Errorf("hist paging for %s did not terminate after %d pages", j.symbol, maxPages)
	return res
}

func (r *Reconciler) emit(ctx context.Context, symbol string, pts []HistPoint, now time.Time, res *jobResult) error {
	r.mu.Lock()
	liveSince := r.liveSince
	r.mu.Unlock()
	for _, p := range pts {
		if p.Label.After(now) {
			continue
		}
		start, ok := histBarStart(p.Label)
		if !ok {
			r.log.Warn("hist label not on a 5m boundary; skipped", "symbol", symbol, "label", p.Label)
			continue
		}
		row := Row{Symbol: symbol, StartTime: start, SumOpenInterest: p.OpenInterest, SnapTime: p.Label, SrcRank: RankHist}
		if err := r.sink.Push(ctx, row); err != nil {
			return err
		}
		res.rows++
		r.m.histRows.Inc()
		if start.After(res.newest) {
			res.newest = start
		}
		r.compareWithLive(symbol, start, p.OpenInterest, liveSince, res)
	}
	return nil
}

// compareWithLive feeds the live-vs-hist diagnostics.
func (r *Reconciler) compareWithLive(symbol string, start time.Time, hist float64, liveSince time.Time, res *jobResult) {
	if r.live == nil {
		return
	}
	if lv, ok := r.live.get(symbol, start); ok {
		if hist > 0 {
			r.m.liveVsHistRelDiff.Observe(math.Abs(lv-hist) / hist)
		}
		return
	}
	// live never snapshotted this bar. Only a gap if live was running then: the
	// snapshot would have been taken shortly before the bar closed.
	if !liveSince.IsZero() && start.Add(BarInterval).After(liveSince) {
		res.gaps++
		r.m.liveGapBars.Inc()
	}
}

// pacer spaces calls step apart on a shared timeline, however many workers use it.
type pacer struct {
	mu    sync.Mutex
	next  time.Time
	step  time.Duration
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
}

func (p *pacer) wait(ctx context.Context) error {
	if p.step <= 0 {
		return ctx.Err()
	}
	p.mu.Lock()
	n := p.now()
	t := p.next
	if t.Before(n) {
		t = n
	}
	p.next = t.Add(p.step)
	p.mu.Unlock()
	if d := t.Sub(n); d > 0 {
		return p.sleep(ctx, d)
	}
	return ctx.Err()
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
