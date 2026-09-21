package openinterest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// LiveConfig tunes the live snapshotter. Zero values take the defaults.
type LiveConfig struct {
	Lead           time.Duration // start each round this long before the kline closes (default 30s)
	AcceptWindow   time.Duration // max distance of a snapshot's time from a 5m boundary (default 60s)
	Workers        int           // concurrent requests (default 8)
	RPS            float64       // request pacing; the shared weight gate is the hard limit (default 25)
	RequestTimeout time.Duration // per request (default 10s)
	// MinRemaining: a round is not started when fewer than this remains before the
	// boundary (default 5s); it waits for the next boundary instead.
	MinRemaining time.Duration
}

func (c LiveConfig) withDefaults() LiveConfig {
	if c.Lead <= 0 {
		c.Lead = 30 * time.Second
	}
	if c.AcceptWindow <= 0 {
		c.AcceptWindow = 60 * time.Second
	}
	if c.Workers <= 0 {
		c.Workers = 8
	}
	if c.RPS <= 0 {
		c.RPS = 25
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 10 * time.Second
	}
	if c.MinRemaining <= 0 {
		c.MinRemaining = 5 * time.Second
	}
	return c
}

// CycleStats summarises one live round.
type CycleStats struct {
	Boundary time.Time
	Symbols  int
	OK       int
	Dropped  int // snapshot time too far from a 5m boundary
	Errors   int
	Took     time.Duration
}

// Live snapshots every symbol's open interest shortly before each 5-minute kline
// closes, so a row is available when the kline closes (oi.md §3.2). Requests go
// through the shared /fapi weight gate as weightgate.ClassLive (inside the
// SnapshotSource).
type Live struct {
	cfg     LiveConfig
	src     SnapshotSource
	sink    RowSink
	symbols func() []string
	cache   *liveCache
	m       *metrics
	log     *slog.Logger
	lim     *rate.Limiter
	now     func() time.Time
	sleep   func(ctx context.Context, d time.Duration) error

	// lastBoundary is the newest boundary Run has already snapshotted. Only Run touches it.
	lastBoundary time.Time
}

// NewLive builds a Live. cache may be nil.
func NewLive(cfg LiveConfig, src SnapshotSource, sink RowSink, symbols func() []string, cache *liveCache, m *metrics, log *slog.Logger) *Live {
	cfg = cfg.withDefaults()
	if m == nil {
		m = newMetrics(nil)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Live{
		cfg: cfg, src: src, sink: sink, symbols: symbols, cache: cache, m: m,
		log:   log.With("component", "oi_live"),
		lim:   rate.NewLimiter(rate.Limit(cfg.RPS), maxInt(cfg.Workers, 1)),
		now:   time.Now,
		sleep: sleepCtx,
	}
}

// nextRound returns the boundary B to snapshot for and when to start.
//
// B is the next 5-minute boundary; the round starts at B-Lead. If that moment has
// already passed the round starts now, unless fewer than MinRemaining is left, in
// which case the following boundary is used. A boundary that Run has already
// snapshotted is never chosen again: a round that finishes with more than MinRemaining
// still left before its own boundary would otherwise start a second round for it.
func (l *Live) nextRound(now time.Time) (boundary, start time.Time) {
	boundary = floorBar(now).Add(BarInterval)
	if boundary.Sub(now) < l.cfg.MinRemaining {
		boundary = boundary.Add(BarInterval)
	}
	if !boundary.After(l.lastBoundary) {
		boundary = l.lastBoundary.Add(BarInterval)
	}
	start = boundary.Add(-l.cfg.Lead)
	if start.Before(now) {
		start = now
	}
	return boundary, start
}

// Run snapshots at every boundary until ctx ends.
func (l *Live) Run(ctx context.Context) {
	for ctx.Err() == nil {
		boundary, start := l.nextRound(l.now())
		if d := start.Sub(l.now()); d > 0 {
			if err := l.sleep(ctx, d); err != nil {
				return
			}
		}
		st := l.Cycle(ctx, boundary)
		if ctx.Err() != nil {
			return
		}
		l.lastBoundary = boundary
		l.log.Info("open-interest live round",
			"boundary", st.Boundary.Format("15:04:05"), "symbols", st.Symbols, "ok", st.OK,
			"dropped", st.Dropped, "errors", st.Errors, "took", st.Took.Round(100*time.Millisecond))
	}
}

// Cycle snapshots every symbol once. Requests still outstanding when the accept
// window after the boundary has passed are abandoned: their snapshots would be
// rejected anyway.
func (l *Live) Cycle(ctx context.Context, boundary time.Time) CycleStats {
	begin := l.now()
	syms := l.symbols()
	st := CycleStats{Boundary: boundary, Symbols: len(syms)}

	// Abandon whatever is still outstanding once the accept window after the
	// boundary is over (the duration is computed from the injected clock).
	remaining := boundary.Add(l.cfg.AcceptWindow).Sub(begin)
	if remaining < 0 {
		remaining = 0
	}
	cctx, cancel := context.WithTimeout(ctx, remaining)
	defer cancel()

	var ok, dropped, errs atomic.Int64
	ch := make(chan string)
	var wg sync.WaitGroup
	for w := 0; w < l.cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sym := range ch {
				switch l.one(cctx, sym) {
				case outcomeOK:
					ok.Add(1)
				case outcomeDropped:
					dropped.Add(1)
				default:
					errs.Add(1)
				}
			}
		}()
	}
feed:
	for _, s := range syms {
		select {
		case ch <- s:
		case <-cctx.Done():
			break feed
		}
	}
	close(ch)
	wg.Wait()

	st.OK, st.Dropped, st.Errors = int(ok.Load()), int(dropped.Load()), int(errs.Load())
	// symbols never reached because the round timed out count as errors
	if missed := st.Symbols - st.OK - st.Dropped - st.Errors; missed > 0 {
		st.Errors += missed
		l.m.liveSnapshots.WithLabelValues("error").Add(float64(missed))
	}
	st.Took = l.now().Sub(begin)
	l.m.liveCycleSeconds.Observe(st.Took.Seconds())
	if st.Symbols > 0 {
		l.m.liveCycleComplete.Set(float64(st.OK) / float64(st.Symbols))
	}
	return st
}

type outcome int

const (
	outcomeOK outcome = iota
	outcomeDropped
	outcomeError
)

func (l *Live) one(ctx context.Context, symbol string) outcome {
	if err := l.lim.Wait(ctx); err != nil {
		l.m.liveSnapshots.WithLabelValues("error").Inc() // the round ended before this symbol got its turn
		return outcomeError
	}
	rctx, cancel := context.WithTimeout(ctx, l.cfg.RequestTimeout)
	snap, err := l.src.Snapshot(rctx, symbol)
	cancel()
	if err != nil {
		if ctx.Err() == nil && !errors.Is(err, ErrInvalidSymbol) {
			l.log.Debug("live snapshot failed", "symbol", symbol, "err", err)
		}
		l.m.liveSnapshots.WithLabelValues("error").Inc()
		return outcomeError
	}
	start, ok := liveBarStart(snap.Time, l.cfg.AcceptWindow)
	if !ok {
		l.m.liveSnapshots.WithLabelValues("dropped").Inc()
		l.log.Debug("live snapshot dropped: time not near a 5m boundary", "symbol", symbol, "time", snap.Time)
		return outcomeDropped
	}
	row := Row{Symbol: symbol, StartTime: start, SumOpenInterest: snap.OpenInterest, SnapTime: snap.Time, SrcRank: RankLive}
	if err := l.sink.Push(ctx, row); err != nil {
		l.m.liveSnapshots.WithLabelValues("error").Inc()
		return outcomeError
	}
	if l.cache != nil {
		l.cache.put(symbol, start, snap.OpenInterest)
	}
	l.m.liveSnapshots.WithLabelValues("ok").Inc()
	return outcomeOK
}
