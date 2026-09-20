package backfill

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/HarvestStars/chomosyncer-go/internal/weightgate"
)

// klineRow returns one Binance /klines array row.
func klineRow(openMs int64, closeMs int64, o, h, l, c string) []any {
	return []any{openMs, o, h, l, c, "100.5", closeMs, "6100000", 42, "60.2", "3600000", "0"}
}

func TestBinanceFetcherParsesAndFilters(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	openA := now.Add(-3 * time.Minute).UnixMilli()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-MBX-USED-WEIGHT-1M", "12")
		payload := [][]any{
			klineRow(openA, openA+59_999, "100", "110", "90", "105"),
			klineRow(openA+60_000, openA+119_999, "105", "112", "104", "108"),
			// forming bar: closeTime in the future -> must be filtered
			klineRow(now.Add(-30*time.Second).UnixMilli(), now.Add(30*time.Second).UnixMilli(), "108", "109", "107", "108"),
		}
		_ = json.NewEncoder(w).Encode(payload)
	}))
	defer srv.Close()

	reg := prometheus.NewRegistry()
	f := NewBinanceFetcher(FetcherConfig{
		BaseURL:    srv.URL,
		HTTPClient: srv.Client(),
		Registerer: reg,
		Clock:      func() time.Time { return now },
	})

	rows, err := f.Fetch(context.Background(), "BTCUSDT", "1m", now.Add(-10*time.Minute), now)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2 (forming bar filtered)", len(rows))
	}
	if rows[0].Symbol != "BTCUSDT" || rows[0].Open != 100 || rows[0].High != 110 || rows[0].Close != 105 {
		t.Fatalf("row0 = %+v", rows[0])
	}
	if !rows[0].StartTime.Equal(time.UnixMilli(openA).UTC()) {
		t.Fatalf("row0 StartTime = %v", rows[0].StartTime)
	}
	if rows[1].Close != 108 {
		t.Fatalf("row1 close = %v", rows[1].Close)
	}
	// the X-MBX-USED-WEIGHT-1M header is reported to the shared gate, which exports it
	if v := gaugeValue(t, reg, "weightgate_used_weight_1m"); v != 12 {
		t.Fatalf("weightgate_used_weight_1m = %v, want 12", v)
	}
}

func TestBinanceFetcherPagination(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	start := now.Add(-1 * time.Hour)
	var reqs atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reqs.Add(1)
		st, _ := strconv.ParseInt(r.URL.Query().Get("startTime"), 10, 64)
		var payload [][]any
		switch n {
		case 1:
			// full page (== limit) so the fetcher pages again
			for i := int64(0); i < 3; i++ {
				o := st + i*60_000
				payload = append(payload, klineRow(o, o+59_999, "1", "2", "0.5", "1"))
			}
		case 2:
			// short page -> stop
			o := st
			payload = append(payload, klineRow(o, o+59_999, "1", "2", "0.5", "1"))
		}
		_ = json.NewEncoder(w).Encode(payload)
	}))
	defer srv.Close()

	f := NewBinanceFetcher(FetcherConfig{
		BaseURL: srv.URL, HTTPClient: srv.Client(), Registerer: prometheus.NewRegistry(),
		Clock: func() time.Time { return now }, MaxPageSpan: 3,
	})

	rows, err := f.Fetch(context.Background(), "ETHUSDT", "1m", start, now)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if reqs.Load() != 2 {
		t.Fatalf("made %d requests, want 2", reqs.Load())
	}
	if len(rows) != 4 {
		t.Fatalf("got %d rows, want 4 (3 + 1)", len(rows))
	}
}

func TestBinanceFetcher429(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"code":-1003,"msg":"Too much request weight"}`))
	}))
	defer srv.Close()

	reg := prometheus.NewRegistry()
	f := NewBinanceFetcher(FetcherConfig{BaseURL: srv.URL, HTTPClient: srv.Client(), Registerer: reg})

	// short ctx so the (60s) backoff returns quickly
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err := f.Fetch(ctx, "BTCUSDT", "1m", time.Now().Add(-time.Hour), time.Now())
	if err == nil {
		t.Fatal("expected error on 429")
	}
	if v := testutil.ToFloat64(f.httpErr.WithLabelValues("429")); v != 1 {
		t.Fatalf("http_errors{429} = %v, want 1", v)
	}
}

func TestBinanceFetcher5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer srv.Close()

	f := NewBinanceFetcher(FetcherConfig{BaseURL: srv.URL, HTTPClient: srv.Client(), Registerer: prometheus.NewRegistry()})
	if _, err := f.Fetch(context.Background(), "BTCUSDT", "1m", time.Now().Add(-time.Hour), time.Now()); err == nil {
		t.Fatal("expected error on 502")
	}
	if v := testutil.ToFloat64(f.httpErr.WithLabelValues("5xx")); v != 1 {
		t.Fatalf("http_errors{5xx} = %v, want 1", v)
	}
}

// gaugeValue returns the value of an unlabelled gauge in reg.
func gaugeValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

// stuckGate returns a gate whose clock never advances and whose sleep blocks
// until the caller's ctx ends, so any wait at the gate is observable as a ctx
// error without real time passing.
func stuckGate(reg prometheus.Registerer) *weightgate.Gate {
	fixed := time.Date(2026, 9, 21, 12, 0, 20, 0, time.UTC)
	return weightgate.New(weightgate.Config{
		Registerer: reg,
		Clock:      func() time.Time { return fixed },
		Sleep: func(ctx context.Context, _ time.Duration) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
}

func TestKlineWeightTable(t *testing.T) {
	for limit, want := range map[int]int{
		1: 1, 5: 1, 99: 1, // < 100
		100: 2, 499: 2, // [100, 500)
		500: 5, 1000: 5, // [500, 1000]
		1001: 10, 1500: 10,
	} {
		if got := klineWeight(limit); got != want {
			t.Errorf("klineWeight(%d) = %d, want %d", limit, got, want)
		}
	}
}

// The request `limit` (which alone decides the weight) must be sized to the gap.
func TestBinanceFetcherSizesLimitToGap(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 30, 0, time.UTC) // deliberately unaligned
	cases := []struct {
		name      string
		gap       time.Duration
		pageSpan  int
		wantLimit int
		wantW     int
	}{
		{"3 minute gap", 3 * time.Minute, 0, 5, 1}, // 3 bars + 2
		{"1 hour gap", time.Hour, 0, 62, 1},        // still weight 1
		{"8 hour gap", 8 * time.Hour, 0, 482, 2},   // 480 + 2
		{"2 day gap hits the page cap", 48 * time.Hour, 0, 1500, 10},
		{"custom page cap", 48 * time.Hour, 200, 200, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var gotLimit atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 64)
				gotLimit.Store(n)
				_, _ = w.Write([]byte("[]")) // no bars -> single page
			}))
			defer srv.Close()
			f := NewBinanceFetcher(FetcherConfig{
				BaseURL: srv.URL, HTTPClient: srv.Client(), Registerer: prometheus.NewRegistry(),
				Clock: func() time.Time { return now }, MaxPageSpan: c.pageSpan,
			})
			if _, err := f.Fetch(context.Background(), "BTCUSDT", "1m", now.Add(-c.gap), now); err != nil {
				t.Fatal(err)
			}
			if int(gotLimit.Load()) != c.wantLimit {
				t.Fatalf("limit sent = %d, want %d", gotLimit.Load(), c.wantLimit)
			}
			if w := klineWeight(int(gotLimit.Load())); w != c.wantW {
				t.Fatalf("weight = %d, want %d", w, c.wantW)
			}
		})
	}
}

// A page shorter than the sized limit ends pagination; a full page continues it.
func TestBinanceFetcherSizedLimitStillPaginates(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	start := now.Add(-10 * time.Minute)
	var reqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		st, _ := strconv.ParseInt(r.URL.Query().Get("startTime"), 10, 64)
		lim, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		var payload [][]any
		for i := 0; i < lim && i < 4; i++ { // the exchange only has 4 bars in range
			o := st + int64(i)*60_000
			payload = append(payload, klineRow(o, o+59_999, "1", "2", "0.5", "1"))
		}
		_ = json.NewEncoder(w).Encode(payload)
	}))
	defer srv.Close()
	f := NewBinanceFetcher(FetcherConfig{
		BaseURL: srv.URL, HTTPClient: srv.Client(), Registerer: prometheus.NewRegistry(),
		Clock: func() time.Time { return now },
	})
	rows, err := f.Fetch(context.Background(), "BTCUSDT", "1m", start, now)
	if err != nil {
		t.Fatal(err)
	}
	if reqs.Load() != 1 || len(rows) != 4 {
		t.Fatalf("requests=%d rows=%d, want 1 request and 4 rows (4 < limit 12 means last page)", reqs.Load(), len(rows))
	}
}

// After a 429 the whole gate pauses: a second request never reaches the server.
func TestBinanceFetcher429PausesTheGate(t *testing.T) {
	var reqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reqs.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	f := NewBinanceFetcher(FetcherConfig{BaseURL: srv.URL, HTTPClient: srv.Client(), Registerer: reg, Gate: stuckGate(reg)})

	if _, err := f.Fetch(context.Background(), "BTCUSDT", "1m", time.Now().Add(-time.Hour), time.Now()); err == nil {
		t.Fatal("expected an error on 429")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err := f.Fetch(ctx, "ETHUSDT", "1m", time.Now().Add(-time.Hour), time.Now())
	if err == nil {
		t.Fatal("second request should have been held at the gate")
	}
	if reqs.Load() != 1 {
		t.Fatalf("server saw %d requests, want 1 (the 429); the pause must stop the next one", reqs.Load())
	}
}

// When Binance reports high used weight, bulk backfill backs off.
func TestBinanceFetcherBacksOffWhenUsedWeightHigh(t *testing.T) {
	var reqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reqs.Add(1)
		w.Header().Set("X-MBX-USED-WEIGHT-1M", "1900") // above the default soft limit (1800)
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()
	reg := prometheus.NewRegistry()
	f := NewBinanceFetcher(FetcherConfig{BaseURL: srv.URL, HTTPClient: srv.Client(), Registerer: reg, Gate: stuckGate(reg)})

	if _, err := f.Fetch(context.Background(), "BTCUSDT", "1m", time.Now().Add(-time.Hour), time.Now()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, err := f.Fetch(ctx, "ETHUSDT", "1m", time.Now().Add(-time.Hour), time.Now()); err == nil {
		t.Fatal("bulk request should back off while used weight is above the soft limit")
	}
	if reqs.Load() != 1 {
		t.Fatalf("server saw %d requests, want 1", reqs.Load())
	}
}
