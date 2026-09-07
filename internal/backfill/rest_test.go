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
	if v := testutil.ToFloat64(f.weight); v != 12 {
		t.Fatalf("weight gauge = %v, want 12", v)
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
