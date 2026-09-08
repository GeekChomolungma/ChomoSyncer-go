package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/HarvestStars/chomosyncer-go/internal/chwriter"
	"github.com/HarvestStars/chomosyncer-go/internal/config"
)

// unreachable endpoints so no test touches a real service.
func smokeConfig() Config {
	cfg := config.DefaultConfig()
	cfg.Collector.WSURL = "ws://127.0.0.1:1"
	cfg.Universe.RESTURL = "http://127.0.0.1:1"
	cfg.Collector.Intervals = []string{"1m", "1h"}
	cfg.Collector.ShardsPerInterval = 2
	cfg.Redis.Addr = "127.0.0.1:1"
	cfg.ClickHouse.Addrs = []string{"127.0.0.1:1"}
	cfg.ClickHouse.DialTimeout = 150 * time.Millisecond
	cfg.App.MetricsAddr = "off"
	cfg.Dispatcher.SectionTimeout = time.Second
	cfg.Backfill.Enabled = false
	return Config{
		Config:  cfg,
		Version: "test",
	}
}

func TestNewAndShutdownNoExternalDeps(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a, err := New(ctx, smokeConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Every interval got a ClickHouse writer.
	if len(a.chWriters) != 2 || a.chWriters["1m"] == nil || a.chWriters["1h"] == nil {
		t.Fatalf("chWriters = %v", a.chWriters)
	}
	if a.win == nil || a.live == nil || a.disp == nil || a.col == nil || a.univ == nil || a.metrics == nil {
		t.Fatal("a component was not wired")
	}

	done := make(chan error, 1)
	go func() { done <- a.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown hung")
	}

	// idempotent
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

func TestRunFailsFastWhenUniverseUnreachable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a, err := New(ctx, smokeConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run should fail: universe REST is unreachable")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not fail fast on unreachable universe")
	}
}

func TestArchiveRouter(t *testing.T) {
	reg := prometheus.NewRegistry()
	f1m, f1h := &fakeArchive{}, &fakeArchive{}
	r := &archiveRouter{
		writers:  map[string]archiveWriter{"1m": f1m, "1h": f1h},
		unrouted: prometheus.NewCounter(prometheus.CounterOpts{Name: "x_unrouted_total", Help: "x"}),
	}
	reg.MustRegister(r.unrouted)

	if err := r.TryPush("1m", chwriter.Row{Symbol: "BTCUSDT"}); err != nil {
		t.Fatalf("TryPush 1m: %v", err)
	}
	if err := r.TryPush("1h", chwriter.Row{Symbol: "ETHUSDT"}); err != nil {
		t.Fatalf("TryPush 1h: %v", err)
	}
	if len(f1m.rows) != 1 || f1m.rows[0].Symbol != "BTCUSDT" {
		t.Fatalf("1m writer rows = %v", f1m.rows)
	}
	if len(f1h.rows) != 1 || f1h.rows[0].Symbol != "ETHUSDT" {
		t.Fatalf("1h writer rows = %v", f1h.rows)
	}

	err := r.TryPush("15m", chwriter.Row{Symbol: "SOLUSDT"})
	if err == nil {
		t.Fatal("TryPush for an unconfigured interval should error")
	}
	if got := testutil.ToFloat64(r.unrouted); got != 1 {
		t.Fatalf("unrouted counter = %v, want 1", got)
	}
}

func TestArchiveRouterForwardsError(t *testing.T) {
	boom := errors.New("clickhouse buffer full")
	r := &archiveRouter{
		writers:  map[string]archiveWriter{"1m": &fakeArchive{err: boom}},
		unrouted: prometheus.NewCounter(prometheus.CounterOpts{Name: "y_unrouted_total", Help: "y"}),
	}
	if err := r.TryPush("1m", chwriter.Row{}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

// --- helpers ---

type fakeArchive struct {
	rows []chwriter.Row
	err  error
}

func (f *fakeArchive) TryPush(row chwriter.Row) error {
	if f.err != nil {
		return f.err
	}
	f.rows = append(f.rows, row)
	return nil
}

func TestBackfillWiring(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := smokeConfig()
	cfg.Backfill.Enabled = true
	cfg.Backfill.FlushWait = time.Millisecond

	a, err := New(ctx, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if a.backfiller == nil || a.gate == nil || a.chStore == nil {
		t.Fatalf("backfill components not wired: bf=%v gate=%v store=%v", a.backfiller != nil, a.gate != nil, a.chStore != nil)
	}
	// readiness closure reflects the backfiller
	if !a.backfiller.Ready() {
		t.Fatal("Ready() should be true before any cold-start submission")
	}

	done := make(chan error, 1)
	go func() { done <- a.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown hung with backfill enabled")
	}
}

func TestBackfillDisabledByDefault(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, err := New(ctx, smokeConfig()) // Backfill: false
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if a.backfiller != nil || a.chStore != nil {
		t.Fatal("backfiller/chStore should be nil when Backfill is off")
	}
	if a.gate == nil {
		t.Fatal("gate is always wired (adapters are cheap)")
	}
	_ = a.Shutdown(context.Background())
}
