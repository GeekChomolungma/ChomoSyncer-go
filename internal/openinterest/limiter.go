package openinterest

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// DataPool admits requests to /futures/data/*, which is a different pool from
// /fapi/*: Binance allows 1000 requests per 5 minutes per IP there, counted per
// request (not per weight), with no usage header to read back. It therefore
// counts requests itself, in a sliding window, and adds a token bucket for
// pacing and a pause for 429/418. It is NOT the weightgate.Gate, which governs
// the /fapi weight pool.
type DataPool struct {
	lim      *rate.Limiter
	window   time.Duration
	cap      int
	m        *metrics
	log      *slog.Logger
	now      func() time.Time
	sleep    func(ctx context.Context, d time.Duration) error
	pausedNs atomic.Int64 // unix nanoseconds; 0 = not paused
	mu       sync.Mutex
	times    []time.Time // request instants inside the window, oldest first
}

// DataPoolConfig configures a DataPool. Zero values take the defaults.
type DataPoolConfig struct {
	RPS       float64       // token-bucket rate (default 2)
	Burst     int           // token-bucket burst (default 2)
	WindowCap int           // hard cap of requests per Window (default 900 of the 1000 allowed)
	Window    time.Duration // sliding window (default 5m)

	Metrics *metrics
	Logger  *slog.Logger
	// Clock and Sleep are for tests.
	Clock func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
}

// NewDataPool builds a pool.
func NewDataPool(cfg DataPoolConfig) *DataPool {
	if cfg.RPS <= 0 {
		cfg.RPS = 2
	}
	if cfg.Burst <= 0 {
		cfg.Burst = 2
	}
	if cfg.WindowCap <= 0 {
		cfg.WindowCap = 900
	}
	if cfg.Window <= 0 {
		cfg.Window = 5 * time.Minute
	}
	p := &DataPool{
		lim:    rate.NewLimiter(rate.Limit(cfg.RPS), cfg.Burst),
		window: cfg.Window,
		cap:    cfg.WindowCap,
		m:      cfg.Metrics,
		log:    cfg.Logger,
		now:    time.Now,
		sleep:  sleepCtx,
	}
	if p.m == nil {
		p.m = newMetrics(nil)
	}
	if p.log == nil {
		p.log = slog.Default()
	}
	p.log = p.log.With("component", "oi_data_pool")
	if cfg.Clock != nil {
		p.now = cfg.Clock
	}
	if cfg.Sleep != nil {
		p.sleep = cfg.Sleep
	}
	return p
}

// Wait blocks until one more /futures/data request may be sent, or ctx ends. On a
// nil error the request has been counted.
func (p *DataPool) Wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := p.now()

		if d := p.pauseRemaining(now); d > 0 {
			if err := p.sleep(ctx, d); err != nil {
				return err
			}
			continue
		}
		if d := p.windowWait(now); d > 0 {
			if err := p.sleep(ctx, d); err != nil {
				return err
			}
			continue
		}

		r := p.lim.ReserveN(now, 1)
		if d := r.DelayFrom(now); d > 0 {
			if err := p.sleep(ctx, d); err != nil {
				r.CancelAt(p.now())
				return err
			}
		}
		after := p.now()
		if p.pauseRemaining(after) > 0 {
			r.CancelAt(after)
			continue
		}
		if !p.record(after) { // the window filled up while we waited for a token
			r.CancelAt(after)
			continue
		}
		return nil
	}
}

// PauseFor blocks the pool for d (after a 429/418). A shorter pause never
// shortens one already in force.
func (p *DataPool) PauseFor(d time.Duration) {
	if d <= 0 {
		return
	}
	until := p.now().Add(d).UnixNano()
	for {
		cur := p.pausedNs.Load()
		if until <= cur {
			return
		}
		if p.pausedNs.CompareAndSwap(cur, until) {
			p.log.Warn("pausing /futures/data requests after a rate-limit response", "for", d)
			return
		}
	}
}

// Used is the number of requests counted in the current window.
func (p *DataPool) Used() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.prune(p.now())
	return len(p.times)
}

func (p *DataPool) pauseRemaining(now time.Time) time.Duration {
	until := p.pausedNs.Load()
	if until == 0 {
		return 0
	}
	return time.Unix(0, until).Sub(now)
}

// windowWait is how long until the window has room for one more request (0 = room now).
func (p *DataPool) windowWait(now time.Time) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.prune(now)
	if len(p.times) < p.cap {
		return 0
	}
	d := p.times[0].Add(p.window).Sub(now)
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return d
}

// record counts a request at now; false when the window is already full.
func (p *DataPool) record(now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.prune(now)
	if len(p.times) >= p.cap {
		return false
	}
	p.times = append(p.times, now)
	p.m.dataWindow.Set(float64(len(p.times)))
	return true
}

// prune drops requests older than the window. Caller holds mu.
func (p *DataPool) prune(now time.Time) {
	cutoff := now.Add(-p.window)
	i := 0
	for i < len(p.times) && !p.times[i].After(cutoff) {
		i++
	}
	if i > 0 {
		p.times = append(p.times[:0], p.times[i:]...)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
