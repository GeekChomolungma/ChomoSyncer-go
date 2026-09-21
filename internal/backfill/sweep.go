package backfill

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// GapFinder reports interior holes in an archive table.
type GapFinder interface {
	FindGaps(ctx context.Context, interval string, from, to time.Time) (map[string][]Range, error)
}

// RepairSubmitter accepts repair-mode backfill requests (implemented by *Backfiller).
type RepairSubmitter interface {
	Submit(Request)
}

// SweepConfig configures the periodic integrity sweep.
type SweepConfig struct {
	Interval string        // bar interval swept; default "1m"
	Every    time.Duration // sweep period; default 30m
	Window   time.Duration // how far back each sweep looks; default 24h
	Settle   time.Duration // newest bars younger than this are not judged; default 3m
	// FirstDelay is the wait before the first sweep after Start; default 2m, so a
	// cold-start backfill and the live streams settle first.
	FirstDelay time.Duration
	// EmptyTTL is how long a range the exchange returned nothing for is remembered
	// and not re-requested. Binance omits klines for minutes with no trades, so such
	// holes are legitimate and would otherwise be re-fetched on every sweep. Default 24h (= the default window: each empty range is asked once while it is inside the window).
	EmptyTTL time.Duration
	// MaxRanges caps the ranges repaired per sweep (the rest wait for the next one). Default 3000.
	MaxRanges int

	Clock      func() time.Time
	Registerer prometheus.Registerer
	Logger     *slog.Logger
}

// Sweeper periodically looks for missing bars in the last Window of the archive
// and repairs them over REST. It is the safety net behind the live stream: any
// loss the reconnect-gap logic misses (a silent reconnect, a dropped frame, a
// crash) is found here within one period.
type Sweeper struct {
	cfg    SweepConfig
	finder GapFinder
	sub    RepairSubmitter
	log    *slog.Logger
	now    func() time.Time
	m      *sweepMetrics

	mu    sync.Mutex
	empty map[emptyKey]time.Time // -> expiry

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

type emptyKey struct {
	sym  string
	from int64
}

// NewSweeper validates the config and applies defaults.
func NewSweeper(cfg SweepConfig, finder GapFinder, sub RepairSubmitter) (*Sweeper, error) {
	if finder == nil || sub == nil {
		return nil, errors.New("backfill: sweeper needs a gap finder and a submitter")
	}
	if cfg.Interval == "" {
		cfg.Interval = "1m"
	}
	if _, ok := parseIntervalDuration(cfg.Interval); !ok {
		return nil, errors.New("backfill: sweeper: unknown interval " + cfg.Interval)
	}
	if cfg.Every <= 0 {
		cfg.Every = 30 * time.Minute
	}
	if cfg.Window <= 0 {
		cfg.Window = 24 * time.Hour
	}
	if cfg.Settle <= 0 {
		cfg.Settle = 3 * time.Minute
	}
	if cfg.FirstDelay <= 0 {
		cfg.FirstDelay = 2 * time.Minute
	}
	if cfg.EmptyTTL <= 0 {
		cfg.EmptyTTL = 24 * time.Hour
	}
	if cfg.MaxRanges <= 0 {
		cfg.MaxRanges = 3000
	}
	now := cfg.Clock
	if now == nil {
		now = time.Now
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Sweeper{
		cfg: cfg, finder: finder, sub: sub, now: now,
		log:   log.With("component", "backfill-sweep"),
		m:     newSweepMetrics(cfg.Registerer),
		empty: map[emptyKey]time.Time{},
	}, nil
}

// Start launches the periodic loop. Close stops it.
func (s *Sweeper) Start(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTimer(s.cfg.FirstDelay)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			s.SweepOnce(ctx)
			t.Reset(s.cfg.Every)
		}
	}()
}

// Close stops the loop and waits for an in-flight sweep to return.
func (s *Sweeper) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	return nil
}

// SweepStats summarises one sweep.
type SweepStats struct {
	Found, Skipped, Submitted int // ranges: holes found / suppressed as known-empty / sent for repair
	Repaired                  int // bars written back
	Empty                     int // ranges the exchange has no bars for
	Failed                    int // ranges whose fetch errored
	SkippedKeys               int // symbols the backfiller could not take this round
}

// SweepOnce runs a single find-and-repair pass.
func (s *Sweeper) SweepOnce(ctx context.Context) (SweepStats, error) {
	start := s.now()
	var st SweepStats
	dur, _ := parseIntervalDuration(s.cfg.Interval)

	to := start.Add(-s.cfg.Settle).Truncate(dur)
	from := to.Add(-s.cfg.Window)

	gaps, err := s.finder.FindGaps(ctx, s.cfg.Interval, from, to)
	if err != nil {
		s.m.runs.WithLabelValues("find_error").Inc()
		s.log.Warn("gap scan failed", "err", err)
		return st, err
	}

	s.pruneEmpty(start)
	ranges := map[Key][]Range{}
	var keys []Key
	syms := make([]string, 0, len(gaps))
	for sym := range gaps {
		syms = append(syms, sym)
	}
	sort.Strings(syms)
	for _, sym := range syms {
		k := Key{Symbol: sym, Interval: s.cfg.Interval}
		for _, r := range gaps[sym] {
			st.Found++
			if s.isEmpty(sym, r.From) {
				st.Skipped++
				continue
			}
			if st.Submitted >= s.cfg.MaxRanges {
				continue
			}
			if len(ranges[k]) == 0 {
				keys = append(keys, k)
			}
			ranges[k] = append(ranges[k], r)
			st.Submitted++
		}
	}
	s.m.found.Set(float64(st.Found))
	if st.Found > st.Submitted+st.Skipped {
		s.log.Warn("sweep capped; remaining holes wait for the next pass",
			"found", st.Found, "submitted", st.Submitted, "max_ranges", s.cfg.MaxRanges)
	}
	if len(keys) == 0 {
		s.finish("ok", start, st)
		return st, nil
	}

	res := make(chan RepairResult, 1)
	s.sub.Submit(Request{Keys: keys, Ranges: ranges, Until: to, Reason: ReasonSweep, Result: res})
	var r RepairResult
	select {
	case r = <-res:
	case <-ctx.Done():
		return st, ctx.Err()
	}

	st.SkippedKeys = r.SkippedKeys
	for _, o := range r.Outcomes {
		switch {
		case o.Err != nil:
			st.Failed++
		case o.Fetched == 0:
			st.Empty++
			s.markEmpty(o.Key.Symbol, o.Range.From, start)
		default:
			st.Repaired += o.Fetched
		}
	}
	s.m.repaired.Add(float64(st.Repaired))
	s.m.empty.Add(float64(st.Empty))
	outcome := "ok"
	if st.Failed > 0 || st.SkippedKeys > 0 {
		outcome = "partial"
	}
	s.finish(outcome, start, st)
	return st, nil
}

func (s *Sweeper) finish(outcome string, start time.Time, st SweepStats) {
	s.m.runs.WithLabelValues(outcome).Inc()
	s.m.duration.Observe(s.now().Sub(start).Seconds())
	if outcome == "ok" {
		s.m.lastOK.Set(float64(s.now().Unix()))
	}
	s.log.Info("sweep done", "outcome", outcome, "holes_found", st.Found, "known_empty", st.Skipped,
		"submitted", st.Submitted, "bars_repaired", st.Repaired, "empty_at_exchange", st.Empty,
		"failed", st.Failed, "skipped_keys", st.SkippedKeys)
}

func (s *Sweeper) isEmpty(sym string, from time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	exp, ok := s.empty[emptyKey{sym, from.UnixMilli()}]
	return ok && s.now().Before(exp)
}

func (s *Sweeper) markEmpty(sym string, from, now time.Time) {
	s.mu.Lock()
	s.empty[emptyKey{sym, from.UnixMilli()}] = now.Add(s.cfg.EmptyTTL)
	s.mu.Unlock()
}

func (s *Sweeper) pruneEmpty(now time.Time) {
	s.mu.Lock()
	for k, exp := range s.empty {
		if !now.Before(exp) {
			delete(s.empty, k)
		}
	}
	s.mu.Unlock()
}

type sweepMetrics struct {
	runs     *prometheus.CounterVec
	found    prometheus.Gauge
	repaired prometheus.Counter
	empty    prometheus.Counter
	duration prometheus.Histogram
	lastOK   prometheus.Gauge
}

func newSweepMetrics(reg prometheus.Registerer) *sweepMetrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)
	return &sweepMetrics{
		runs: f.NewCounterVec(prometheus.CounterOpts{
			Name: "backfill_sweep_runs_total", Help: "Integrity sweeps by outcome (ok|partial|find_error).",
		}, []string{"outcome"}),
		found: f.NewGauge(prometheus.GaugeOpts{
			Name: "backfill_sweep_holes_found", Help: "Interior holes (ranges) found by the last sweep, before known-empty suppression.",
		}),
		repaired: f.NewCounter(prometheus.CounterOpts{
			Name: "backfill_sweep_bars_repaired_total", Help: "Bars written back by the integrity sweep.",
		}),
		empty: f.NewCounter(prometheus.CounterOpts{
			Name: "backfill_sweep_empty_ranges_total", Help: "Ranges the exchange returned no bars for (no trades in that minute).",
		}),
		duration: f.NewHistogram(prometheus.HistogramOpts{
			Name: "backfill_sweep_duration_seconds", Help: "Duration of one integrity sweep.",
			Buckets: []float64{.1, .5, 1, 5, 15, 60, 300, 900},
		}),
		lastOK: f.NewGauge(prometheus.GaugeOpts{
			Name: "backfill_sweep_last_success_timestamp_seconds", Help: "Unix time of the last fully successful sweep; alert if it goes stale.",
		}),
	}
}
