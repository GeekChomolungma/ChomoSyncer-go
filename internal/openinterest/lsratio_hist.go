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

// LSReconciler keeps fapi_ls_ratio_5m up to date from the three long/short ratio
// endpoints. It is the open-interest Reconciler's twin and shares its scheduling
// (planJobs, runHistJobs, the start-up FULL pass followed by hourly incremental
// passes at hh:Offset spread over Spread) and its /futures/data pool. What differs:
//
//   - a symbol job pages back through all three endpoints before writing anything,
//     and merges them per label into one row: ReplacingMergeTree replaces whole rows,
//     so a row must carry all three columns when it is written;
//   - rows stop at the newest label that EVERY non-empty endpoint has returned. The
//     endpoints publish a label at slightly different moments (globalLongShortAccountRatio
//     usually first); a row written before its slowest column was published would keep
//     that column NULL for good, since the state then moves past it;
//   - there is no live source, hence no live comparison and no src_rank 1.
//
// A column is NULL only where its endpoint returned no point for that bar although it
// did return others (counted in ls_ratio_null_cells_total). An endpoint returning
// nothing at all while the others return points holds the symbol back (see emit).
type LSReconciler struct {
	cfg     HistConfig
	src     RatioHistSource
	store   Store
	sink    LSRowSink
	symbols func() []string
	m       *metrics
	log     *slog.Logger
	now     func() time.Time
	sleep   func(ctx context.Context, d time.Duration) error

	mu   sync.Mutex
	last map[string]time.Time // start_time of the newest row per symbol (all rows are src_rank >= 2)
}

// NewLSReconciler builds a long/short ratio reconciler. cfg should already carry the
// long/short defaults (see Config); zero values take the open-interest ones.
func NewLSReconciler(cfg HistConfig, src RatioHistSource, store Store, sink LSRowSink, symbols func() []string, m *metrics, log *slog.Logger) *LSReconciler {
	if m == nil {
		m = newMetrics(nil)
	}
	if log == nil {
		log = slog.Default()
	}
	return &LSReconciler{
		cfg: cfg.withDefaults(), src: src, store: store, sink: sink, symbols: symbols, m: m,
		log:   log.With("component", "ls_ratio_hist"),
		now:   time.Now,
		sleep: sleepCtx,
		last:  map[string]time.Time{},
	}
}

// LoadState reads from the database how far each symbol is filled. It retries until
// it succeeds or ctx ends.
func (r *LSReconciler) LoadState(ctx context.Context) error {
	backoff := 5 * time.Second
	for {
		syms := r.symbols()
		got, err := r.store.LastStarts(ctx, syms)
		if err == nil {
			r.mu.Lock()
			have := 0
			for _, s := range syms {
				if ls, ok := got[s]; ok && !ls.Hist.IsZero() {
					r.last[s] = ls.Hist
					have++
				}
			}
			r.mu.Unlock()
			r.log.Info("loaded long/short ratio state from ClickHouse", "symbols", len(syms), "with_rows", have, "no_rows", len(syms)-have)
			return nil
		}
		r.log.Warn("could not read long/short ratio state; will retry", "err", err, "in", backoff)
		if serr := r.sleep(ctx, backoff); serr != nil {
			return serr
		}
		if backoff *= 2; backoff > time.Minute {
			backoff = time.Minute
		}
	}
}

// Run loads the state, runs the full start-up pass immediately (paced only by the
// /futures/data pool), then an incremental pass every Interval at hh:Offset. It
// returns when ctx ends.
func (r *LSReconciler) Run(ctx context.Context) {
	if err := r.LoadState(ctx); err != nil {
		return
	}
	r.runPass(ctx, 0, "startup", true)
	for ctx.Err() == nil {
		next := nextReconcileTime(r.now(), r.cfg.Interval, r.cfg.Offset)
		if err := r.sleep(ctx, next.Sub(r.now())); err != nil {
			return
		}
		r.runPass(ctx, r.cfg.Spread, "scheduled", false)
	}
}

func (r *LSReconciler) runPass(ctx context.Context, spread time.Duration, reason string, full bool) {
	st, err := r.pass(ctx, spread, full)
	if err != nil && ctx.Err() == nil {
		r.log.Error("long/short ratio pass failed", "reason", reason, "err", err)
		return
	}
	r.log.Info("long/short ratio pass",
		"reason", reason, "symbols", st.Symbols, "skipped", st.Skipped, "fetched", st.Fetched, "failed", st.Failed,
		"rows", st.Rows, "cold_start", st.ColdStart, "beyond_cap", st.BeyondCap,
		"committed", st.Committed, "took", st.Duration.Round(time.Second))
}

// Pass runs one incremental pass; PassFull one full (start-up) pass.
func (r *LSReconciler) Pass(ctx context.Context, spread time.Duration) (PassStats, error) {
	return r.pass(ctx, spread, false)
}

// PassFull runs a full pass: every symbol is asked for at least one whole page.
func (r *LSReconciler) PassFull(ctx context.Context, spread time.Duration) (PassStats, error) {
	return r.pass(ctx, spread, true)
}

func (r *LSReconciler) pass(ctx context.Context, spread time.Duration, full bool) (PassStats, error) {
	start := r.now()
	syms := r.symbols()
	r.mu.Lock()
	jobs, st := planJobs(r.cfg, syms, r.last, start, full)
	r.mu.Unlock()
	r.m.ls.beyondCap.Add(float64(st.BeyondCap))
	r.m.ls.symbols.WithLabelValues("skipped").Add(float64(st.Skipped))

	results := map[string]jobResult{}
	failed := runHistJobs(ctx, r.cfg.Workers, jobs, spread, r.now, r.sleep, r.fetchSymbol, results)
	if len(failed) > 0 && ctx.Err() == nil {
		r.log.Warn("retrying failed symbols once", "count", len(failed))
		failed = runHistJobs(ctx, r.cfg.Workers, failed, 0, r.now, r.sleep, r.fetchSymbol, results)
	}
	if err := ctx.Err(); err != nil {
		return st, err
	}

	// Advance the in-memory state only once the rows are really flushed.
	committed := true
	if f, ok := r.sink.(interface{ Flush(context.Context) error }); ok {
		if err := f.Flush(ctx); err != nil {
			committed = false
			r.log.Error("flush after long/short ratio pass failed; state not advanced, the next pass will refetch", "err", err)
		}
	}
	failedSet := map[string]struct{}{}
	for _, j := range failed {
		failedSet[j.symbol] = struct{}{}
	}
	r.mu.Lock()
	for _, j := range jobs {
		if _, bad := failedSet[j.symbol]; bad {
			st.Failed++
			continue
		}
		res := results[j.symbol]
		st.Fetched++
		st.Rows += res.rows
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
		r.m.ls.crossSection.Set(float64(current) / float64(st.Symbols))
	}

	st.Committed = committed
	st.Duration = r.now().Sub(start)
	r.m.ls.symbols.WithLabelValues("fetched").Add(float64(st.Fetched))
	r.m.ls.symbols.WithLabelValues("failed").Add(float64(st.Failed))
	r.m.ls.passSeconds.Observe(st.Duration.Seconds())
	r.m.ls.lastPass.Set(float64(r.now().Unix()))
	return st, nil
}

// fetchSymbol pages back through the gap on each of the three endpoints, then
// writes the merged rows. Any endpoint failing fails the whole symbol (nothing is
// written; the retry or the next pass re-fetches it).
func (r *LSReconciler) fetchSymbol(ctx context.Context, j histJob) jobResult {
	now := r.now()
	var res jobResult
	var series [numRatios][]RatioPoint
	for k := RatioKind(0); k < numRatios; k++ {
		pts, err := r.pageBack(ctx, k, j, now)
		if errors.Is(err, ErrInvalidSymbol) { // delisted between the universe refresh and now
			r.m.ls.requests.WithLabelValues(k.String(), "invalid_symbol").Inc()
			return res
		}
		if err != nil {
			r.log.Warn("long/short ratio request failed", "symbol", j.symbol, "endpoint", k.String(), "err", err)
			res.err = err
			return res
		}
		series[k] = pts
	}
	if err := r.emit(ctx, j.symbol, series, now, &res); err != nil {
		res.err = err
	}
	return res
}

// pageBack returns every point of one endpoint with a label in [label(since), now],
// oldest first, paging BACKWARDS like Reconciler.fetchSymbol (the endpoint returns
// the newest `limit` points inside [startTime, endTime]).
func (r *LSReconciler) pageBack(ctx context.Context, kind RatioKind, j histJob, now time.Time) ([]RatioPoint, error) {
	startLabel := j.since.Add(BarInterval) // the label of bar `since`
	end := now
	limit := minInt(j.needed+r.cfg.LimitMargin, r.cfg.MaxLimit)
	maxPages := j.needed/maxInt(r.cfg.MaxLimit-r.cfg.LimitMargin, 1) + 3
	var pages [][]RatioPoint
	for page := 0; page < maxPages; page++ {
		rctx, cancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
		pts, err := r.src.RatioHistory(rctx, kind, j.symbol, startLabel, end, limit)
		cancel()
		if err != nil {
			if !errors.Is(err, ErrInvalidSymbol) {
				r.m.ls.requests.WithLabelValues(kind.String(), "error").Inc()
			}
			return nil, err
		}
		if len(pts) == 0 {
			r.m.ls.requests.WithLabelValues(kind.String(), "empty").Inc()
			break // e.g. a symbol listed a moment ago
		}
		r.m.ls.requests.WithLabelValues(kind.String(), "ok").Inc()
		pages = append(pages, pts)
		if len(pts) < limit {
			break // the range held fewer points than we allowed for: done
		}
		oldest := pts[0].Label
		end = oldest.Add(-time.Millisecond)
		if end.Before(startLabel) {
			break
		}
		remaining := int(oldest.Sub(startLabel)/BarInterval) + 1
		limit = minInt(remaining+r.cfg.LimitMargin, r.cfg.MaxLimit)
		if page == maxPages-1 {
			return nil, fmt.Errorf("%s paging for %s did not terminate after %d pages", kind, j.symbol, maxPages)
		}
	}
	var out []RatioPoint
	for i := len(pages) - 1; i >= 0; i-- { // pages are newest first
		out = append(out, pages[i]...)
	}
	return out, nil
}

// emit merges the three series per label and pushes one row per bar, up to the
// newest label every series has reached.
//
// When some endpoints returned points for the window and another returned none, the
// symbol is NOT written this pass (its state stays put, so the next pass asks again):
// every listed symbol has all three series, so an empty one next to non-empty ones
// is an outage or a publication delay, and writing the rows now would leave that
// column NULL for good. Holes inside a series are written as NULL.
func (r *LSReconciler) emit(ctx context.Context, symbol string, series [numRatios][]RatioPoint, now time.Time, res *jobResult) error {
	var common time.Time // newest label all series share as an upper bound
	var empty []string
	for k, pts := range series {
		if len(pts) == 0 {
			empty = append(empty, RatioKind(k).String())
			continue
		}
		if newest := pts[len(pts)-1].Label; common.IsZero() || newest.Before(common) {
			common = newest
		}
	}
	if common.IsZero() {
		return nil // nothing at all for this window (e.g. a symbol listed a moment ago)
	}
	if len(empty) > 0 {
		r.m.ls.partial.Inc()
		r.log.Warn("long/short ratio endpoint returned nothing while the others did; symbol held back until the next pass",
			"symbol", symbol, "empty", empty)
		return nil
	}
	if common.After(now) {
		common = now
	}

	rows := map[time.Time]*LSRow{}
	for k, pts := range series {
		for _, p := range pts {
			if p.Label.After(common) {
				continue
			}
			start, ok := histBarStart(p.Label)
			if !ok {
				r.log.Warn("long/short ratio label not on a 5m boundary; skipped", "symbol", symbol, "endpoint", RatioKind(k).String(), "label", p.Label)
				continue
			}
			row := rows[start]
			if row == nil {
				nr := newLSRow(symbol, start, RankHist)
				row = &nr
				rows[start] = row
			}
			row.Ratios[k] = p.Ratio
		}
	}
	starts := make([]time.Time, 0, len(rows))
	for s := range rows {
		starts = append(starts, s)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	for _, s := range starts {
		row := *rows[s]
		for k, v := range row.Ratios {
			if math.IsNaN(v) {
				r.m.ls.nullCells.WithLabelValues(ratioSpecs[k].column).Inc()
			}
		}
		if err := r.sink.Push(ctx, row); err != nil {
			return err
		}
		res.rows++
		r.m.ls.rows.Inc()
		if s.After(res.newest) {
			res.newest = s
		}
	}
	return nil
}
