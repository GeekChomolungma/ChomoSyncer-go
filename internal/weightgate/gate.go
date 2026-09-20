// Package weightgate is the single admission gate for every Binance USDⓈ-M
// futures REST request that draws on the /fapi request-weight budget
// (REQUEST_WEIGHT, 2400 per minute per IP). Callers declare each request's
// weight and wait at the gate before sending; after the response they report
// the X-MBX-USED-WEIGHT-1M header back.
//
// Why one shared gate: the limit is per IP, so the kline gap backfill, the
// open-interest live snapshots and the universe refresh all spend the same
// pool. A per-caller request-per-second limiter cannot see that: the kline
// endpoint costs 1..10 weight depending on its `limit` parameter, and traffic
// from other processes on the same IP (scripts, another instance) is invisible
// to it.
//
// Two mechanisms work together:
//
//  1. Budgets. Each Class owns a token bucket denominated in weight, refilled
//     at budget/60 per second. A burst-heavy, time-critical caller (Live) is
//     therefore never starved by a bulk caller (Bulk), because they draw from
//     separate buckets whose budgets sum to less than the exchange cap; the
//     remainder is headroom for traffic the gate cannot see.
//
//  2. Feedback. Every response reports the exchange's own count of used weight.
//     While the last observation (valid until the end of the UTC minute it was
//     taken in) is at or above a threshold, callers wait for that minute to
//     end: Bulk at SoftLimit, Live and Misc at the higher HardLimit. This is
//     what covers foreign traffic.
//
// A 429/418 makes the caller report PauseFor(Retry-After), which blocks every
// class, not just the caller that saw the error.
//
// /futures/data/* (open-interest history, ratios) is a different pool
// (1000 requests per 5 minutes per IP, no usage header) and does NOT go
// through this gate.
package weightgate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
)

// Class selects which per-minute weight budget a request draws from and which
// used-weight threshold makes it back off.
type Class int

const (
	// ClassLive is for time-critical, small, bursty work (open-interest live
	// snapshots: a whole-market round must go out within ~20s).
	ClassLive Class = iota
	// ClassBulk is for elastic bulk work that can wait (kline gap backfill).
	ClassBulk
	// ClassMisc is for rare housekeeping (universe exchangeInfo).
	ClassMisc

	numClasses
)

func (c Class) String() string {
	switch c {
	case ClassLive:
		return "live"
	case ClassBulk:
		return "bulk"
	case ClassMisc:
		return "misc"
	default:
		return "unknown"
	}
}

// Defaults. Budgets sum to 1900 of the 2400/min exchange cap; the remaining 500
// is headroom for traffic the gate cannot see (scripts, other processes).
const (
	ExchangeWeightPerMinute = 2400 // Binance REQUEST_WEIGHT, per IP

	DefaultLiveBudget = 600
	DefaultBulkBudget = 1200
	DefaultMiscBudget = 100
	DefaultSoftLimit  = 1800
	DefaultHardLimit  = 2300

	// UsedWeightHeader is the response header carrying the exchange's count of
	// weight used in the current one-minute window.
	UsedWeightHeader = "X-MBX-USED-WEIGHT-1M"
)

var (
	// ErrWeightExceedsBurst is returned when one request's weight is larger than
	// its class's burst, which could never be satisfied.
	ErrWeightExceedsBurst = errors.New("weightgate: request weight exceeds the class burst")
	errUnknownClass       = errors.New("weightgate: unknown class")
)

// Config configures a Gate. Zero values mean "use the default".
type Config struct {
	// Per-minute weight budgets per class.
	LiveBudget int
	BulkBudget int
	MiscBudget int

	// Bucket capacity per class (largest weight that may be spent at once).
	// Defaults: Live = LiveBudget (a full market round in one go),
	// Bulk = max(BulkBudget/6, 10), Misc = MiscBudget.
	LiveBurst int
	BulkBurst int
	MiscBurst int

	// SoftLimit: Bulk callers wait while the last observed used weight is >= it.
	// HardLimit: Live and Misc callers wait while it is >= this.
	SoftLimit int
	HardLimit int

	Registerer prometheus.Registerer
	Logger     *slog.Logger

	// Clock and Sleep are for tests. Defaults: time.Now and a ctx-aware timer.
	Clock func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
}

type resolvedConfig struct {
	budget [numClasses]int
	burst  [numClasses]int
	soft   int
	hard   int
}

func (c Config) resolve() resolvedConfig {
	var r resolvedConfig
	r.budget[ClassLive] = orDefault(c.LiveBudget, DefaultLiveBudget)
	r.budget[ClassBulk] = orDefault(c.BulkBudget, DefaultBulkBudget)
	r.budget[ClassMisc] = orDefault(c.MiscBudget, DefaultMiscBudget)
	r.burst[ClassLive] = orDefault(c.LiveBurst, r.budget[ClassLive])
	r.burst[ClassBulk] = orDefault(c.BulkBurst, max(r.budget[ClassBulk]/6, 10))
	r.burst[ClassMisc] = orDefault(c.MiscBurst, r.budget[ClassMisc])
	r.soft = orDefault(c.SoftLimit, DefaultSoftLimit)
	r.hard = orDefault(c.HardLimit, DefaultHardLimit)
	return r
}

func orDefault(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

// Validate reports a configuration that cannot work. It is meant for user
// supplied settings (YAML); New itself never fails and clamps instead.
func (c Config) Validate() error {
	for _, f := range []struct {
		name string
		v    int
	}{
		{"live_budget", c.LiveBudget}, {"bulk_budget", c.BulkBudget}, {"misc_budget", c.MiscBudget},
		{"live_burst", c.LiveBurst}, {"bulk_burst", c.BulkBurst}, {"misc_burst", c.MiscBurst},
		{"soft_limit", c.SoftLimit}, {"hard_limit", c.HardLimit},
	} {
		if f.v < 0 {
			return fmt.Errorf("weight_gate.%s must be >= 0 (0 = default), got %d", f.name, f.v)
		}
	}
	r := c.resolve()
	if sum := r.budget[ClassLive] + r.budget[ClassBulk] + r.budget[ClassMisc]; sum > ExchangeWeightPerMinute {
		return fmt.Errorf("weight_gate: live+bulk+misc budgets = %d exceed the exchange cap of %d/min", sum, ExchangeWeightPerMinute)
	}
	if r.soft > r.hard {
		return fmt.Errorf("weight_gate.soft_limit (%d) must be <= hard_limit (%d)", r.soft, r.hard)
	}
	if r.hard > ExchangeWeightPerMinute {
		return fmt.Errorf("weight_gate.hard_limit (%d) exceeds the exchange cap of %d/min", r.hard, ExchangeWeightPerMinute)
	}
	return nil
}

type observation struct {
	used int
	at   time.Time
}

// Gate admits requests against the shared /fapi weight pool. Safe for
// concurrent use.
type Gate struct {
	cfg     resolvedConfig
	log     *slog.Logger
	now     func() time.Time
	sleep   func(ctx context.Context, d time.Duration) error
	buckets [numClasses]*rate.Limiter
	m       *metrics

	pausedUntilNs atomic.Int64 // unix nanoseconds; 0 = not paused
	obs           atomic.Pointer[observation]
}

// New builds a Gate. It never fails: non-positive settings take their defaults
// and SoftLimit is clamped to HardLimit. Use Config.Validate to reject bad
// user settings up front.
func New(cfg Config) *Gate {
	rc := cfg.resolve()
	if rc.soft > rc.hard {
		rc.soft = rc.hard
	}
	g := &Gate{
		cfg:   rc,
		log:   slog.Default(),
		now:   time.Now,
		sleep: sleepCtx,
		m:     newMetrics(cfg.Registerer),
	}
	if cfg.Logger != nil {
		g.log = cfg.Logger
	}
	g.log = g.log.With("component", "weightgate")
	if cfg.Clock != nil {
		g.now = cfg.Clock
	}
	if cfg.Sleep != nil {
		g.sleep = cfg.Sleep
	}
	for c := Class(0); c < numClasses; c++ {
		g.buckets[c] = rate.NewLimiter(rate.Limit(float64(rc.budget[c])/60.0), rc.burst[c])
	}
	return g
}

// Wait blocks until a request of the given weight may be sent, or ctx ends.
// On a nil error the weight has been charged to the class's budget.
//
// Order of checks, repeated until all pass: (1) global pause after a 429/418,
// (2) used-weight backpressure, (3) the class's token bucket.
func (g *Gate) Wait(ctx context.Context, c Class, weight int) error {
	if c < 0 || c >= numClasses {
		return errUnknownClass
	}
	if weight <= 0 {
		weight = 1
	}
	if weight > g.cfg.burst[c] {
		return fmt.Errorf("%w: class=%s weight=%d burst=%d", ErrWeightExceedsBurst, c, weight, g.cfg.burst[c])
	}
	start := g.now()
	lim := g.buckets[c]
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := g.now()

		if d := g.pauseRemaining(now); d > 0 {
			if err := g.sleep(ctx, d); err != nil {
				return err
			}
			continue
		}
		if d := g.backpressureDelay(c, now); d > 0 {
			g.m.backpressure.WithLabelValues(c.String()).Inc()
			g.log.Debug("backing off: exchange used weight above threshold", "class", c.String(), "wait", d)
			if err := g.sleep(ctx, d); err != nil {
				return err
			}
			continue
		}

		r := lim.ReserveN(now, weight)
		if !r.OK() {
			return fmt.Errorf("%w: class=%s weight=%d", ErrWeightExceedsBurst, c, weight)
		}
		if d := r.DelayFrom(now); d > 0 {
			if err := g.sleep(ctx, d); err != nil {
				r.CancelAt(g.now())
				return err
			}
		}
		// A pause or backpressure may have started while we slept for tokens.
		// Give the tokens back and start over rather than send into it.
		after := g.now()
		if g.pauseRemaining(after) > 0 || g.backpressureDelay(c, after) > 0 {
			r.CancelAt(after)
			continue
		}
		g.m.acquired.WithLabelValues(c.String()).Add(float64(weight))
		g.m.wait.WithLabelValues(c.String()).Observe(after.Sub(start).Seconds())
		return nil
	}
}

// Observe records the exchange's own count of weight used in the current
// minute (the X-MBX-USED-WEIGHT-1M header value).
func (g *Gate) Observe(used int) {
	if used < 0 {
		return
	}
	g.m.used.Set(float64(used))
	g.obs.Store(&observation{used: used, at: g.now()})
}

// ObserveHeader is Observe for a response header; a missing or malformed
// header is ignored.
func (g *Gate) ObserveHeader(h http.Header) {
	if used, ok := ParseUsedWeight(h); ok {
		g.Observe(used)
	}
}

// PauseFor blocks every class for d (typically Retry-After after a 429/418).
// A shorter pause never shortens one already in force.
func (g *Gate) PauseFor(d time.Duration) {
	if d <= 0 {
		return
	}
	until := g.now().Add(d).UnixNano()
	for {
		cur := g.pausedUntilNs.Load()
		if until <= cur {
			return
		}
		if g.pausedUntilNs.CompareAndSwap(cur, until) {
			break
		}
	}
	g.m.pauses.Inc()
	g.m.pausedUntil.Set(float64(until) / 1e9)
	g.log.Warn("pausing all /fapi requests after a rate-limit response", "for", d)
}

// ParseUsedWeight reads X-MBX-USED-WEIGHT-1M from a response header.
func ParseUsedWeight(h http.Header) (int, bool) {
	v := h.Get(UsedWeightHeader)
	if v == "" {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// RetryAfter parses the Retry-After header (whole seconds) of a 429/418
// response, falling back to fallback when it is absent or malformed.
func RetryAfter(h http.Header, fallback time.Duration) time.Duration {
	if v := h.Get("Retry-After"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return fallback
}

func (g *Gate) pauseRemaining(now time.Time) time.Duration {
	until := g.pausedUntilNs.Load()
	if until == 0 {
		return 0
	}
	return time.Unix(0, until).Sub(now)
}

// backpressureDelay is how long a class must wait because the exchange's last
// reported used weight is at or above its threshold. An observation is only
// trusted until the end of the (UTC) minute it was taken in — the exchange's
// counter is a one-minute window — after which it says nothing and the next
// request refreshes it.
func (g *Gate) backpressureDelay(c Class, now time.Time) time.Duration {
	o := g.obs.Load()
	if o == nil {
		return 0
	}
	end := o.at.Truncate(time.Minute).Add(time.Minute)
	if !now.Before(end) {
		return 0
	}
	limit := g.cfg.hard
	if c == ClassBulk {
		limit = g.cfg.soft
	}
	if o.used < limit {
		return 0
	}
	return end.Sub(now)
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
