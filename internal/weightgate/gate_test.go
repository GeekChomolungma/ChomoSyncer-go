package weightgate

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeClock is a deterministic clock: Sleep advances time instead of blocking.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
	return nil
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// newTestGate returns a Gate on a fake clock that starts at 12:00:20 UTC, i.e.
// 40s before the minute ends.
func newTestGate(t *testing.T, cfg Config) (*Gate, *fakeClock, *prometheus.Registry) {
	t.Helper()
	fc := &fakeClock{t: time.Date(2026, 9, 21, 12, 0, 20, 0, time.UTC)}
	reg := prometheus.NewRegistry()
	cfg.Clock = fc.Now
	cfg.Sleep = fc.Sleep
	cfg.Registerer = reg
	return New(cfg), fc, reg
}

func approx(t *testing.T, got, want, tol time.Duration, what string) {
	t.Helper()
	if d := got - want; d < -tol || d > tol {
		t.Fatalf("%s = %v, want %v ± %v", what, got, want, tol)
	}
}

func TestWaitWithinBurstDoesNotBlock(t *testing.T) {
	g, fc, _ := newTestGate(t, Config{})
	start := fc.Now()
	// a whole market of open-interest snapshots (528 x weight 1) fits the live burst
	if err := g.Wait(context.Background(), ClassLive, 528); err != nil {
		t.Fatal(err)
	}
	if !fc.Now().Equal(start) {
		t.Fatalf("a request within the burst slept %v", fc.Now().Sub(start))
	}
}

func TestWaitBeyondBurstWaitsForRefill(t *testing.T) {
	g, fc, _ := newTestGate(t, Config{}) // live: 600/min = 10 weight/s, burst 600
	start := fc.Now()
	if err := g.Wait(context.Background(), ClassLive, 528); err != nil {
		t.Fatal(err)
	}
	// 72 tokens left; 100 more needs 28 more -> 2.8s at 10/s
	if err := g.Wait(context.Background(), ClassLive, 100); err != nil {
		t.Fatal(err)
	}
	approx(t, fc.Now().Sub(start), 2800*time.Millisecond, 20*time.Millisecond, "refill wait")
}

func TestClassesAreIndependent(t *testing.T) {
	g, fc, _ := newTestGate(t, Config{})
	// drain the bulk bucket completely (default burst = 1200/6 = 200)
	if err := g.Wait(context.Background(), ClassBulk, 200); err != nil {
		t.Fatal(err)
	}
	start := fc.Now()
	// live and misc are untouched by bulk's exhaustion
	if err := g.Wait(context.Background(), ClassLive, 528); err != nil {
		t.Fatal(err)
	}
	if err := g.Wait(context.Background(), ClassMisc, 1); err != nil {
		t.Fatal(err)
	}
	if !fc.Now().Equal(start) {
		t.Fatalf("live/misc waited %v because bulk was exhausted", fc.Now().Sub(start))
	}
}

func TestWeightExceedsBurst(t *testing.T) {
	g, _, _ := newTestGate(t, Config{})
	err := g.Wait(context.Background(), ClassBulk, 201) // burst 200
	if !errors.Is(err, ErrWeightExceedsBurst) {
		t.Fatalf("err = %v, want ErrWeightExceedsBurst", err)
	}
	if err := g.Wait(context.Background(), Class(99), 1); err == nil {
		t.Fatal("unknown class must fail")
	}
}

func TestNonPositiveWeightCountsAsOne(t *testing.T) {
	g, _, reg := newTestGate(t, Config{})
	if err := g.Wait(context.Background(), ClassMisc, 0); err != nil {
		t.Fatal(err)
	}
	_ = reg
	if v := testutil.ToFloat64(g.m.acquired.WithLabelValues("misc")); v != 1 {
		t.Fatalf("acquired{misc} = %v, want 1", v)
	}
}

func TestBulkBacksOffAtSoftLimitLiveAtHardLimit(t *testing.T) {
	g, fc, _ := newTestGate(t, Config{}) // soft 1800, hard 2300
	g.Observe(1900)                      // at 12:00:20; valid until 12:01:00

	// live and misc are below the hard limit: go straight through
	start := fc.Now()
	if err := g.Wait(context.Background(), ClassLive, 1); err != nil {
		t.Fatal(err)
	}
	if err := g.Wait(context.Background(), ClassMisc, 1); err != nil {
		t.Fatal(err)
	}
	if !fc.Now().Equal(start) {
		t.Fatalf("live/misc slept %v at used=1900", fc.Now().Sub(start))
	}

	// bulk is above the soft limit: waits for the minute to end (40s)
	if err := g.Wait(context.Background(), ClassBulk, 1); err != nil {
		t.Fatal(err)
	}
	approx(t, fc.Now().Sub(start), 40*time.Second, time.Millisecond, "bulk backpressure wait")

	// above the hard limit even live waits
	g2, fc2, _ := newTestGate(t, Config{})
	g2.Observe(2350)
	s2 := fc2.Now()
	if err := g2.Wait(context.Background(), ClassLive, 1); err != nil {
		t.Fatal(err)
	}
	approx(t, fc2.Now().Sub(s2), 40*time.Second, time.Millisecond, "live backpressure wait")
	if v := testutil.ToFloat64(g2.m.backpressure.WithLabelValues("live")); v != 1 {
		t.Fatalf("backpressure_waits{live} = %v, want 1", v)
	}
}

func TestObservationExpiresAtMinuteEnd(t *testing.T) {
	g, fc, _ := newTestGate(t, Config{})
	g.Observe(2000) // bulk would wait...
	fc.advance(40 * time.Second)
	start := fc.Now() // ...but it is now 12:01:00, a new minute
	if err := g.Wait(context.Background(), ClassBulk, 1); err != nil {
		t.Fatal(err)
	}
	if !fc.Now().Equal(start) {
		t.Fatalf("a stale observation still blocked for %v", fc.Now().Sub(start))
	}
}

func TestPauseForBlocksEveryClass(t *testing.T) {
	for _, c := range []Class{ClassLive, ClassBulk, ClassMisc} {
		g, fc, _ := newTestGate(t, Config{})
		start := fc.Now()
		g.PauseFor(30 * time.Second)
		if err := g.Wait(context.Background(), c, 1); err != nil {
			t.Fatal(err)
		}
		approx(t, fc.Now().Sub(start), 30*time.Second, time.Millisecond, "pause wait for "+c.String())
	}
}

func TestShorterPauseDoesNotShortenLongerOne(t *testing.T) {
	g, fc, _ := newTestGate(t, Config{})
	start := fc.Now()
	g.PauseFor(60 * time.Second)
	g.PauseFor(5 * time.Second)
	if err := g.Wait(context.Background(), ClassMisc, 1); err != nil {
		t.Fatal(err)
	}
	approx(t, fc.Now().Sub(start), 60*time.Second, time.Millisecond, "pause wait")
	if v := testutil.ToFloat64(g.m.pauses); v != 1 {
		t.Fatalf("pauses_total = %v, want 1 (the shorter pause is a no-op)", v)
	}
}

func TestContextCancelReturnsAndRefundsTokens(t *testing.T) {
	// real clock: bulk 60/min = 1/s, burst 10
	reg := prometheus.NewRegistry()
	g := New(Config{BulkBudget: 60, BulkBurst: 10, Registerer: reg})
	if err := g.Wait(context.Background(), ClassBulk, 10); err != nil { // drains the bucket
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := g.Wait(ctx, ClassBulk, 10) // needs ~10s of refill -> ctx must win
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("Wait ignored ctx for %v", time.Since(start))
	}
	// the cancelled reservation was given back: a 1-weight request needs ~1s, not ~11s
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	if err := g.Wait(ctx2, ClassBulk, 1); err != nil {
		t.Fatalf("tokens were not refunded after cancel: %v", err)
	}
}

func TestParseUsedWeight(t *testing.T) {
	h := http.Header{}
	if _, ok := ParseUsedWeight(h); ok {
		t.Fatal("missing header must not parse")
	}
	h.Set("X-MBX-USED-WEIGHT-1M", "560")
	if n, ok := ParseUsedWeight(h); !ok || n != 560 {
		t.Fatalf("got (%d, %v), want (560, true)", n, ok)
	}
	h.Set("X-MBX-USED-WEIGHT-1M", "abc")
	if _, ok := ParseUsedWeight(h); ok {
		t.Fatal("malformed header must not parse")
	}
	h.Set("X-MBX-USED-WEIGHT-1M", "-3")
	if _, ok := ParseUsedWeight(h); ok {
		t.Fatal("negative value must not parse")
	}
}

func TestObserveHeaderUpdatesGauge(t *testing.T) {
	g, _, _ := newTestGate(t, Config{})
	h := http.Header{}
	h.Set("X-MBX-USED-WEIGHT-1M", "742")
	g.ObserveHeader(h)
	if v := testutil.ToFloat64(g.m.used); v != 742 {
		t.Fatalf("weightgate_used_weight_1m = %v, want 742", v)
	}
	g.ObserveHeader(http.Header{}) // no header: gauge untouched
	if v := testutil.ToFloat64(g.m.used); v != 742 {
		t.Fatalf("gauge changed to %v on a header-less response", v)
	}
}

func TestMetricsRecordAcquiredWeightAndWait(t *testing.T) {
	g, _, reg := newTestGate(t, Config{})
	if err := g.Wait(context.Background(), ClassLive, 528); err != nil {
		t.Fatal(err)
	}
	if v := testutil.ToFloat64(g.m.acquired.WithLabelValues("live")); v != 528 {
		t.Fatalf("acquired{live} = %v, want 528", v)
	}
	n, err := testutil.GatherAndCount(reg, "weightgate_wait_seconds")
	if err != nil || n == 0 {
		t.Fatalf("wait histogram not exported (n=%d err=%v)", n, err)
	}
}

func TestConfigValidate(t *testing.T) {
	if err := (Config{}).Validate(); err != nil {
		t.Fatalf("zero config (all defaults) must be valid: %v", err)
	}
	bad := []Config{
		{LiveBudget: -1},
		{LiveBudget: 1500, BulkBudget: 1000}, // 2600 > 2400
		{SoftLimit: 2000, HardLimit: 1500},   // soft > hard
		{HardLimit: 2500},                    // above the exchange cap
		{SoftLimit: DefaultHardLimit + 1},    // resolved soft > default hard
	}
	for i, c := range bad {
		if err := c.Validate(); err == nil {
			t.Errorf("case %d (%+v): expected an error", i, c)
		}
	}
}

func TestNewClampsSoftToHard(t *testing.T) {
	g := New(Config{SoftLimit: 2000, HardLimit: 1500, Registerer: prometheus.NewRegistry()})
	if g.cfg.soft != 1500 {
		t.Fatalf("soft = %d, want clamped to hard (1500)", g.cfg.soft)
	}
}

func TestRetryAfter(t *testing.T) {
	h := http.Header{}
	if got := RetryAfter(h, 5*time.Second); got != 5*time.Second {
		t.Fatalf("missing header: got %v, want the fallback", got)
	}
	h.Set("Retry-After", "42")
	if got := RetryAfter(h, 5*time.Second); got != 42*time.Second {
		t.Fatalf("got %v, want 42s", got)
	}
	h.Set("Retry-After", "soon")
	if got := RetryAfter(h, 5*time.Second); got != 5*time.Second {
		t.Fatalf("malformed header: got %v, want the fallback", got)
	}
	h.Set("Retry-After", "0")
	if got := RetryAfter(h, 5*time.Second); got != 5*time.Second {
		t.Fatalf("zero: got %v, want the fallback", got)
	}
}
