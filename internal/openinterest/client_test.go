package openinterest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/HarvestStars/chomosyncer-go/internal/weightgate"
)

// Real response shapes, captured from fapi.binance.com.
const (
	snapshotBody = `{"symbol":"BTCUSDT","openInterest":"109049.480","time":1789913109480}`
	histBody     = `[{"symbol":"BTCUSDT","sumOpenInterest":"109115.83500000","sumOpenInterestValue":"8785524572.89207600","CMCCirculatingSupply":"20086328.00000000","timestamp":1789912200000},` +
		`{"symbol":"BTCUSDT","sumOpenInterest":"109098.63700000","sumOpenInterestValue":"8785091375.37910000","CMCCirculatingSupply":"20086328.00000000","timestamp":1789912500000}]`
)

// stuckGate never lets a request through once it has to wait (clock frozen, sleep blocks).
func stuckGate(reg prometheus.Registerer) *weightgate.Gate {
	fixed := ts("2026-09-21 12:00:20.000")
	return weightgate.New(weightgate.Config{Registerer: reg, Clock: func() time.Time { return fixed }, Sleep: blockingSleep})
}

func newTestClient(t *testing.T, h http.HandlerFunc, gate *weightgate.Gate, pool *DataPool) (*Client, *metrics) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	m := newMetrics(prometheus.NewRegistry())
	if pool == nil {
		pool = NewDataPool(DataPoolConfig{Metrics: m, RPS: 1000, Burst: 1000})
	}
	return NewClient(ClientConfig{BaseURL: srv.URL, HTTPClient: srv.Client(), Gate: gate, Pool: pool, Metrics: m}), m
}

func gaugeVal(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

func TestSnapshotParsesAndReportsWeight(t *testing.T) {
	var gotPath, gotSymbol string
	gateReg := prometheus.NewRegistry()
	c, m := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotSymbol = r.URL.Path, r.URL.Query().Get("symbol")
		w.Header().Set("X-MBX-USED-WEIGHT-1M", "42")
		_, _ = w.Write([]byte(snapshotBody))
	}, weightgate.New(weightgate.Config{Registerer: gateReg}), nil)

	s, err := c.Snapshot(context.Background(), "BTCUSDT")
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/fapi/v1/openInterest" || gotSymbol != "BTCUSDT" {
		t.Fatalf("request = %s ?symbol=%s", gotPath, gotSymbol)
	}
	if s.OpenInterest != 109049.480 || !s.Time.Equal(time.UnixMilli(1789913109480).UTC()) {
		t.Fatalf("snapshot = %+v", s)
	}
	if v := gaugeVal(t, gateReg, "weightgate_used_weight_1m"); v != 42 {
		t.Fatalf("weightgate_used_weight_1m = %v, want 42 (header reported to the shared gate)", v)
	}
	if v := testutil.ToFloat64(m.httpRequests.WithLabelValues("snapshot", "ok")); v != 1 {
		t.Fatalf("http_requests{snapshot,ok} = %v", v)
	}
}

func TestSnapshotInvalidSymbol(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":-1121,"msg":"Invalid symbol."}`))
	}, nil, nil)
	if _, err := c.Snapshot(context.Background(), "NOTASYMBOL"); !errors.Is(err, ErrInvalidSymbol) {
		t.Fatalf("err = %v, want ErrInvalidSymbol", err)
	}
}

func TestSnapshotMalformed(t *testing.T) {
	for _, body := range []string{`{}`, `{"openInterest":"abc","time":1}`, `{"openInterest":"1","time":0}`, `not json`} {
		c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }, nil, nil)
		if _, err := c.Snapshot(context.Background(), "BTCUSDT"); err == nil {
			t.Errorf("body %q: expected an error", body)
		}
	}
}

func TestSnapshot429PausesTheSharedGate(t *testing.T) {
	var reqs atomic.Int32
	reg := prometheus.NewRegistry()
	gate := stuckGate(reg)
	c, m := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		reqs.Add(1)
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}, gate, nil)

	_, err := c.Snapshot(context.Background(), "BTCUSDT")
	var rl *RateLimitError
	if !errors.As(err, &rl) || rl.Status != 429 || rl.RetryAfter != 30*time.Second {
		t.Fatalf("err = %v, want RateLimitError{429, 30s}", err)
	}
	if v := testutil.ToFloat64(m.rateLimited.WithLabelValues("fapi", "429")); v != 1 {
		t.Fatalf("rate_limited{fapi,429} = %v", v)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, err := c.Snapshot(ctx, "ETHUSDT"); err == nil {
		t.Fatal("second snapshot must be held at the paused gate")
	}
	if reqs.Load() != 1 {
		t.Fatalf("server saw %d requests, want 1", reqs.Load())
	}
}

func TestSnapshot418(t *testing.T) {
	c, m := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }, nil, nil)
	_, err := c.Snapshot(context.Background(), "BTCUSDT")
	var rl *RateLimitError
	if !errors.As(err, &rl) || rl.Status != 418 || rl.RetryAfter != 60*time.Second {
		t.Fatalf("err = %v, want RateLimitError{418, 60s fallback}", err)
	}
	if v := testutil.ToFloat64(m.rateLimited.WithLabelValues("fapi", "418")); v != 1 {
		t.Fatalf("rate_limited{fapi,418} = %v", v)
	}
}

func TestHistoryRequestAndParsing(t *testing.T) {
	var q url.Values
	var path string
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		path, q = r.URL.Path, r.URL.Query()
		// deliberately newest-first and with one malformed item
		_, _ = w.Write([]byte(`[{"sumOpenInterest":"2.5","timestamp":1789912500000},{"sumOpenInterest":"oops","timestamp":1789912400000},{"sumOpenInterest":"1.5","timestamp":1789912200000}]`))
	}, nil, nil)

	start, end := time.UnixMilli(1789912200000), time.UnixMilli(1789912800000)
	pts, err := c.History(context.Background(), "BTCUSDT", start, end, 17)
	if err != nil {
		t.Fatal(err)
	}
	if path != "/futures/data/openInterestHist" {
		t.Fatalf("path = %s", path)
	}
	for k, want := range map[string]string{"symbol": "BTCUSDT", "period": "5m", "startTime": "1789912200000", "endTime": "1789912800000", "limit": "17"} {
		if q.Get(k) != want {
			t.Errorf("query %s = %q, want %q", k, q.Get(k), want)
		}
	}
	if len(pts) != 2 || pts[0].OpenInterest != 1.5 || pts[1].OpenInterest != 2.5 || !pts[0].Label.Before(pts[1].Label) {
		t.Fatalf("points = %+v, want 2 points sorted oldest first (malformed one skipped)", pts)
	}
}

func TestHistoryRealShape(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(histBody)) }, nil, nil)
	pts, err := c.History(context.Background(), "BTCUSDT", time.Now(), time.Now(), 2)
	if err != nil || len(pts) != 2 || pts[0].OpenInterest != 109115.835 {
		t.Fatalf("pts=%+v err=%v", pts, err)
	}
	if !pts[0].Label.Equal(time.UnixMilli(1789912200000).UTC()) {
		t.Fatalf("label = %v", pts[0].Label)
	}
}

func TestHistoryEmptyIsNotAnError(t *testing.T) {
	c, m := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`[]`)) }, nil, nil)
	pts, err := c.History(context.Background(), "NEWCOIN", time.Now(), time.Now(), 5)
	if err != nil || len(pts) != 0 {
		t.Fatalf("pts=%v err=%v", pts, err)
	}
	if v := testutil.ToFloat64(m.httpRequests.WithLabelValues("hist", "ok")); v != 1 {
		t.Fatalf("http_requests{hist,ok} = %v", v)
	}
}

func TestHistory429PausesTheDataPoolNotTheFapiGate(t *testing.T) {
	var reqs atomic.Int32
	m := newMetrics(prometheus.NewRegistry())
	fc := newFakeClock("2026-09-21 12:00:00.000")
	pool := NewDataPool(DataPoolConfig{Metrics: m, Clock: fc.Now, Sleep: blockingSleep})
	gateReg := prometheus.NewRegistry()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reqs.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c := NewClient(ClientConfig{BaseURL: srv.URL, HTTPClient: srv.Client(), Gate: weightgate.New(weightgate.Config{Registerer: gateReg}), Pool: pool, Metrics: m})

	if _, err := c.History(context.Background(), "BTCUSDT", time.Now(), time.Now(), 5); err == nil {
		t.Fatal("expected a rate limit error")
	}
	if v := testutil.ToFloat64(m.rateLimited.WithLabelValues("data", "429")); v != 1 {
		t.Fatalf("rate_limited{data,429} = %v", v)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, err := c.History(ctx, "ETHUSDT", time.Now(), time.Now(), 5); err == nil {
		t.Fatal("second history call must be held at the paused pool")
	}
	if reqs.Load() != 1 {
		t.Fatalf("server saw %d requests, want 1", reqs.Load())
	}
	// the /fapi gate was not paused: a snapshot is not blocked by the data-pool pause
	if err := c.gate.Wait(context.Background(), weightgate.ClassLive, 1); err != nil {
		t.Fatalf("/fapi gate should be unaffected: %v", err)
	}
}
