package universe

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const exchangeInfoJSON = `{"symbols":[
 {"symbol":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING","quoteAsset":"USDT"},
 {"symbol":"ETHUSDT","contractType":"PERPETUAL","status":"TRADING","quoteAsset":"USDT"},
 {"symbol":"SOLUSDT","contractType":"PERPETUAL","status":"TRADING","quoteAsset":"USDT"},
 {"symbol":"XRPUSDT","contractType":"PERPETUAL","status":"TRADING","quoteAsset":"USDT"},
 {"symbol":"BTCUSDC","contractType":"PERPETUAL","status":"TRADING","quoteAsset":"USDC"},
 {"symbol":"OLDUSDT","contractType":"PERPETUAL","status":"SETTLING","quoteAsset":"USDT"},
 {"symbol":"BTCUSDT_251226","contractType":"CURRENT_QUARTER","status":"TRADING","quoteAsset":"USDT"}
]}`

// wantTradable is the sorted whole-market set implied by exchangeInfoJSON:
// only USDT + PERPETUAL + TRADING, so USDC / SETTLING / quarterly are excluded.
var wantTradable = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"}

func testServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/fapi/v1/exchangeInfo", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(exchangeInfoJSON))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newTestMonitor(t *testing.T, cfg Config) *Monitor {
	t.Helper()
	srv := testServer(t)
	cfg.BaseURL = srv.URL
	cfg.HTTPClient = srv.Client()
	if cfg.Registerer == nil {
		cfg.Registerer = prometheus.NewRegistry()
	}
	return New(cfg)
}

func TestRefreshWholeMarketFilter(t *testing.T) {
	m := newTestMonitor(t, Config{})

	snap, err := m.Refresh(context.Background())
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if !equalStrings(snap.Symbols, wantTradable) {
		t.Fatalf("Symbols = %v, want %v", snap.Symbols, wantTradable)
	}
	if snap.RefreshedAt.IsZero() {
		t.Fatal("RefreshedAt not set")
	}
}

func TestUniverseProviderInterface(t *testing.T) {
	m := newTestMonitor(t, Config{})
	if m.Size() != 0 {
		t.Fatal("Size should be 0 before first refresh")
	}
	if _, err := m.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.Size() != len(wantTradable) {
		t.Fatalf("Size = %d, want %d", m.Size(), len(wantTradable))
	}
	if !m.Has("BTCUSDT") || !m.Has("SOLUSDT") {
		t.Fatal("Has should be true for every tradable symbol")
	}
	if m.Has("BTCUSDC") || m.Has("OLDUSDT") || m.Has("NOPEUSDT") {
		t.Fatal("Has should be false for non-tradable / unknown symbols")
	}
}

func TestOnChangeNotified(t *testing.T) {
	m := newTestMonitor(t, Config{})
	var (
		mu   sync.Mutex
		last Snapshot
		n    int
	)
	m.OnChange(func(s Snapshot) {
		mu.Lock()
		last, n = s, n+1
		mu.Unlock()
	})

	if _, err := m.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if n != 1 || !equalStrings(last.Symbols, wantTradable) {
		t.Fatalf("subscriber n=%d symbols=%v", n, last.Symbols)
	}
}

func TestRefreshErrorKeepsPreviousSnapshot(t *testing.T) {
	var failing atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/fapi/v1/exchangeInfo", func(w http.ResponseWriter, _ *http.Request) {
		if failing.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(exchangeInfoJSON))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	reg := prometheus.NewRegistry()
	m := New(Config{BaseURL: srv.URL, HTTPClient: srv.Client(), Registerer: reg})

	if _, err := m.Refresh(context.Background()); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	good := m.Snapshot()

	failing.Store(true)
	if _, err := m.Refresh(context.Background()); err == nil {
		t.Fatal("expected error on failing refresh")
	}
	if !equalStrings(m.Snapshot().Symbols, good.Symbols) {
		t.Fatal("snapshot changed after a failed refresh")
	}
	if v := testutil.ToFloat64(m.metrics.refreshTotal.WithLabelValues("error")); v != 1 {
		t.Fatalf("refresh_total{error} = %v, want 1", v)
	}
}

func TestStartAndClose(t *testing.T) {
	m := newTestMonitor(t, Config{RefreshInterval: 40 * time.Millisecond}) // sub-hour => plain ticking
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if m.Size() != len(wantTradable) {
		t.Fatalf("Size after Start = %d", m.Size())
	}
	time.Sleep(120 * time.Millisecond) // let the loop tick a few times

	done := make(chan struct{})
	go func() { _ = m.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung")
	}
	if v := testutil.ToFloat64(m.metrics.refreshTotal.WithLabelValues("ok")); v < 2 {
		t.Fatalf("refresh_total{ok} = %v, want >= 2 (initial + loop ticks)", v)
	}
}

func TestNextDelayDailyAlignment(t *testing.T) {
	m := New(Config{RefreshInterval: 24 * time.Hour, RefreshOffset: 2 * time.Minute})

	// 01:30:00 UTC -> next run is tomorrow 00:02:00 UTC = 22h32m away.
	now := time.Date(2026, 9, 7, 1, 30, 0, 0, time.UTC)
	if got := m.nextDelay(now); got != 22*time.Hour+32*time.Minute {
		t.Fatalf("nextDelay(01:30) = %v, want 22h32m", got)
	}

	// 00:00:30 UTC -> today's 00:02:00 is still ahead: 1m30s away.
	now = time.Date(2026, 9, 7, 0, 0, 30, 0, time.UTC)
	if got := m.nextDelay(now); got != 90*time.Second {
		t.Fatalf("nextDelay(00:00:30) = %v, want 1m30s", got)
	}

	// Exactly at 00:02:00 -> not After(now), so roll to tomorrow (24h).
	now = time.Date(2026, 9, 7, 0, 2, 0, 0, time.UTC)
	if got := m.nextDelay(now); got != 24*time.Hour {
		t.Fatalf("nextDelay(00:02:00) = %v, want 24h", got)
	}

	// A non-UTC input is normalized: 03:30 +02:00 == 01:30 UTC -> 22h32m.
	loc := time.FixedZone("UTC+2", 2*3600)
	now = time.Date(2026, 9, 7, 3, 30, 0, 0, loc)
	if got := m.nextDelay(now); got != 22*time.Hour+32*time.Minute {
		t.Fatalf("nextDelay(03:30+02:00) = %v, want 22h32m", got)
	}
}

func TestNextDelaySubHourIsInterval(t *testing.T) {
	m := New(Config{RefreshInterval: 30 * time.Second})
	if got := m.nextDelay(time.Now()); got != 30*time.Second {
		t.Fatalf("nextDelay (sub-hour) = %v, want 30s", got)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
