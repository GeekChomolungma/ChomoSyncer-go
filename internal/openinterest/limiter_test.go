package openinterest

import (
	"context"
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

func newFakeClock(start string) *fakeClock { return &fakeClock{t: ts(start)} }

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

// blockingSleep never returns until ctx ends: any wait becomes a ctx error while
// the fake clock stands still.
func blockingSleep(ctx context.Context, _ time.Duration) error {
	<-ctx.Done()
	return ctx.Err()
}

func newTestPool(cfg DataPoolConfig, fc *fakeClock) (*DataPool, *metrics) {
	m := newMetrics(prometheus.NewRegistry())
	cfg.Metrics = m
	cfg.Clock = fc.Now
	cfg.Sleep = fc.Sleep
	return NewDataPool(cfg), m
}

func within(t *testing.T, got, want, tol time.Duration, what string) {
	t.Helper()
	if d := got - want; d < -tol || d > tol {
		t.Fatalf("%s = %v, want %v ± %v", what, got, want, tol)
	}
}

func TestPoolTokenBucketPacing(t *testing.T) {
	fc := newFakeClock("2026-09-21 12:00:00.000")
	p, _ := newTestPool(DataPoolConfig{RPS: 2, Burst: 2}, fc)
	start := fc.Now()
	for i := 0; i < 2; i++ { // the burst goes through at once
		if err := p.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if !fc.Now().Equal(start) {
		t.Fatalf("burst slept %v", fc.Now().Sub(start))
	}
	if err := p.Wait(context.Background()); err != nil { // third needs a refill: 1/2s
		t.Fatal(err)
	}
	within(t, fc.Now().Sub(start), 500*time.Millisecond, 5*time.Millisecond, "wait for the third token")
}

func TestPoolWindowCapBlocksUntilOldestExpires(t *testing.T) {
	fc := newFakeClock("2026-09-21 12:00:00.000")
	p, m := newTestPool(DataPoolConfig{RPS: 1000, Burst: 1000, WindowCap: 3, Window: 5 * time.Minute}, fc)
	start := fc.Now()
	for i := 0; i < 3; i++ {
		if err := p.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		fc.Sleep(context.Background(), 10*time.Second) // requests at t=0,10,20
	}
	if got := p.Used(); got != 3 {
		t.Fatalf("Used = %d, want 3", got)
	}
	if v := testutil.ToFloat64(m.dataWindow); v != 3 {
		t.Fatalf("oi_data_window_used = %v, want 3", v)
	}
	// the 4th must wait until the first (t=0) leaves the window at t=300s
	if err := p.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	within(t, fc.Now().Sub(start), 300*time.Second, 50*time.Millisecond, "wait for the window")
	if got := p.Used(); got != 3 {
		t.Fatalf("Used after slide = %d, want 3 (one out, one in)", got)
	}
}

func TestPoolPauseFor(t *testing.T) {
	fc := newFakeClock("2026-09-21 12:00:00.000")
	p, _ := newTestPool(DataPoolConfig{}, fc)
	start := fc.Now()
	p.PauseFor(30 * time.Second)
	p.PauseFor(5 * time.Second) // must not shorten
	if err := p.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	within(t, fc.Now().Sub(start), 30*time.Second, time.Millisecond, "pause")
}

func TestPoolContextCancel(t *testing.T) {
	fc := newFakeClock("2026-09-21 12:00:00.000")
	m := newMetrics(prometheus.NewRegistry())
	p := NewDataPool(DataPoolConfig{Metrics: m, Clock: fc.Now, Sleep: blockingSleep, WindowCap: 1})
	if err := p.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := p.Wait(ctx); err == nil { // window full, sleep blocks until ctx ends
		t.Fatal("expected a ctx error")
	}
}

func TestPoolDefaults(t *testing.T) {
	p := NewDataPool(DataPoolConfig{})
	if p.cap != 900 || p.window != 5*time.Minute {
		t.Fatalf("defaults: cap=%d window=%v", p.cap, p.window)
	}
}
