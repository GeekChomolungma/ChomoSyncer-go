package windowgate

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/HarvestStars/chomosyncer-go/internal/rediswin"
)

type fakeWindow struct {
	mu    sync.Mutex
	calls []string // "SYMBOL/interval"
	err   error
}

func (f *fakeWindow) PushBarAndTrim(_ context.Context, s, i string, _ rediswin.CompactBar) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, s+"/"+i)
	return f.err
}
func (f *fakeWindow) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }

type fakeReady struct {
	mu     sync.Mutex
	events []rediswin.KlineReadyEvent
	err    error
}

func (f *fakeReady) PublishKlineReady(_ context.Context, e rediswin.KlineReadyEvent) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
	return "id", f.err
}
func (f *fakeReady) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.events) }

func TestHoldReleaseAndQueries(t *testing.T) {
	g := New(prometheus.NewRegistry())

	if g.HeldWindow("BTCUSDT", "1m") || g.HeldInterval("1m") {
		t.Fatal("nothing should be held initially")
	}

	g.Hold(Key{"btcusdt", "1m"}) // case-insensitive
	if !g.HeldWindow("BTCUSDT", "1m") || !g.HeldInterval("1m") {
		t.Fatal("BTCUSDT/1m should be held")
	}
	if g.HeldWindow("ETHUSDT", "1m") {
		t.Fatal("ETHUSDT/1m not held")
	}
	if g.HeldCount() != 1 {
		t.Fatalf("HeldCount = %d", g.HeldCount())
	}

	g.Hold(Key{"ETHUSDT", "1m"})
	g.Release(Key{"BTCUSDT", "1m"})
	if g.HeldWindow("BTCUSDT", "1m") {
		t.Fatal("BTCUSDT released")
	}
	if !g.HeldInterval("1m") {
		t.Fatal("interval 1m still held via ETHUSDT")
	}
	g.Release(Key{"ethusdt", "1m"})
	if g.HeldInterval("1m") || g.HeldCount() != 0 {
		t.Fatalf("all released; HeldInterval=%v count=%d", g.HeldInterval("1m"), g.HeldCount())
	}
}

func TestRefCounting(t *testing.T) {
	g := New(prometheus.NewRegistry())
	k := Key{"BTCUSDT", "1h"}
	g.Hold(k)
	g.Hold(k) // cold-start + shard-reconnect both hold
	g.Release(k)
	if !g.HeldWindow("BTCUSDT", "1h") {
		t.Fatal("still held after one Release (ref-count 1 left)")
	}
	g.Release(k)
	if g.HeldWindow("BTCUSDT", "1h") {
		t.Fatal("released after balanced Release")
	}
	g.Release(k) // over-release: no panic, no underflow
	if g.HeldCount() != 0 {
		t.Fatalf("HeldCount = %d", g.HeldCount())
	}
}

func TestWrapWindowGates(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := New(reg)
	fw := &fakeWindow{}
	w := g.WrapWindow(fw)
	ctx := context.Background()

	g.Hold(Key{"BTCUSDT", "1m"})
	if err := w.PushBarAndTrim(ctx, "btcusdt", "1m", rediswin.CompactBar{}); err != nil {
		t.Fatalf("gated push should return nil, got %v", err)
	}
	if fw.count() != 0 {
		t.Fatal("inner window must not be called while held")
	}

	// a different key flows through
	if err := w.PushBarAndTrim(ctx, "ETHUSDT", "1m", rediswin.CompactBar{}); err != nil {
		t.Fatal(err)
	}
	if fw.count() != 1 {
		t.Fatalf("inner calls = %d, want 1", fw.count())
	}

	g.Release(Key{"BTCUSDT", "1m"})
	if err := w.PushBarAndTrim(ctx, "BTCUSDT", "1m", rediswin.CompactBar{}); err != nil {
		t.Fatal(err)
	}
	if fw.count() != 2 {
		t.Fatalf("inner calls after release = %d, want 2", fw.count())
	}
	if v := testutil.ToFloat64(g.metrics.windowSuppressed.WithLabelValues("1m")); v != 1 {
		t.Fatalf("windowSuppressed{1m} = %v, want 1", v)
	}
}

func TestWrapReadyGatesByInterval(t *testing.T) {
	reg := prometheus.NewRegistry()
	g := New(reg)
	fr := &fakeReady{}
	r := g.WrapReady(fr)
	ctx := context.Background()

	g.Hold(Key{"BTCUSDT", "1m"})
	id, err := r.PublishKlineReady(ctx, rediswin.KlineReadyEvent{Interval: "1m", Timestamp: 1})
	if err != nil || id != "" {
		t.Fatalf("suppressed publish -> ('',nil); got (%q,%v)", id, err)
	}
	if fr.count() != 0 {
		t.Fatal("inner ready must not be called")
	}

	// other interval passes
	if _, err := r.PublishKlineReady(ctx, rediswin.KlineReadyEvent{Interval: "1h", Timestamp: 1}); err != nil {
		t.Fatal(err)
	}
	if fr.count() != 1 {
		t.Fatalf("inner ready calls = %d, want 1", fr.count())
	}

	g.Release(Key{"BTCUSDT", "1m"})
	if _, err := r.PublishKlineReady(ctx, rediswin.KlineReadyEvent{Interval: "1m", Timestamp: 2}); err != nil {
		t.Fatal(err)
	}
	if fr.count() != 2 {
		t.Fatalf("inner ready calls after release = %d, want 2", fr.count())
	}
	if v := testutil.ToFloat64(g.metrics.readySuppressed.WithLabelValues("1m")); v != 1 {
		t.Fatalf("readySuppressed{1m} = %v, want 1", v)
	}
}

func TestInnerErrorPropagates(t *testing.T) {
	g := New(prometheus.NewRegistry())
	boom := errors.New("redis down")
	w := g.WrapWindow(&fakeWindow{err: boom})
	if err := w.PushBarAndTrim(context.Background(), "BTCUSDT", "1m", rediswin.CompactBar{}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}
