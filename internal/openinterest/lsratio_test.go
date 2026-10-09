package openinterest

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Real response shape, captured from fapi.binance.com (all three endpoints share it).
const ratioBody = `[{"symbol":"BTCUSDT","longAccount":"0.6670","longShortRatio":"2.0030","shortAccount":"0.3330","timestamp":1791504000000},` +
	`{"symbol":"BTCUSDT","longAccount":"0.6666","longShortRatio":"1.9994","shortAccount":"0.3334","timestamp":1791504300000}]`

// fakeRatioAPI models the real endpoint semantics like fakeAPI: the NEWEST `limit`
// points whose label lies in [start, end], oldest first; one series per endpoint.
type fakeRatioAPI struct {
	mu     sync.Mutex
	series map[RatioKind]map[string][]RatioPoint
	calls  []ratioCall
	errFor map[RatioKind]map[string]error // always fail
	failN  map[RatioKind]map[string]int   // fail the first N calls
}

type ratioCall struct {
	kind       RatioKind
	symbol     string
	start, end time.Time
	limit      int
}

func newFakeRatioAPI() *fakeRatioAPI {
	f := &fakeRatioAPI{series: map[RatioKind]map[string][]RatioPoint{}, errFor: map[RatioKind]map[string]error{}, failN: map[RatioKind]map[string]int{}}
	for k := RatioKind(0); k < numRatios; k++ {
		f.series[k], f.errFor[k], f.failN[k] = map[string][]RatioPoint{}, map[string]error{}, map[string]int{}
	}
	return f
}

// setSeries publishes labels from..to (inclusive) on one endpoint with value base+i/100.
func (f *fakeRatioAPI) setSeries(kind RatioKind, symbol, from, to string, base float64) {
	var pts []RatioPoint
	i := 0
	for l := ts(from); !l.After(ts(to)); l = l.Add(BarInterval) {
		pts = append(pts, RatioPoint{Label: l, Ratio: base + float64(i)/100})
		i++
	}
	f.series[kind][symbol] = pts
}

func (f *fakeRatioAPI) setAll(symbol, from, to string) {
	for k := RatioKind(0); k < numRatios; k++ {
		f.setSeries(k, symbol, from, to, float64(k+1))
	}
}

func (f *fakeRatioAPI) RatioHistory(_ context.Context, kind RatioKind, symbol string, start, end time.Time, limit int) ([]RatioPoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, ratioCall{kind, symbol, start, end, limit})
	if err := f.errFor[kind][symbol]; err != nil {
		return nil, err
	}
	if f.failN[kind][symbol] > 0 {
		f.failN[kind][symbol]--
		return nil, errors.New("transient")
	}
	var sel []RatioPoint
	for _, p := range f.series[kind][symbol] {
		if !p.Label.Before(start) && !p.Label.After(end) {
			sel = append(sel, p)
		}
	}
	if len(sel) > limit {
		sel = sel[len(sel)-limit:]
	}
	return append([]RatioPoint(nil), sel...), nil
}

func (f *fakeRatioAPI) callCount() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }

type fakeLSSink struct {
	mu       sync.Mutex
	rows     []LSRow
	flushErr error
}

func (s *fakeLSSink) Push(_ context.Context, r LSRow) error {
	if err := r.validate(); err != nil {
		return err
	}
	s.mu.Lock()
	s.rows = append(s.rows, r)
	s.mu.Unlock()
	return nil
}

func (s *fakeLSSink) Flush(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushErr
}

func (s *fakeLSSink) forSymbol(sym string) []LSRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []LSRow
	for _, r := range s.rows {
		if r.Symbol == sym {
			out = append(out, r)
		}
	}
	return out
}

type lsh struct {
	fc    *fakeClock
	api   *fakeRatioAPI
	store *fakeStore
	sink  *fakeLSSink
	m     *metrics
	syms  []string
	r     *LSReconciler
}

// newLSHarness: now = 12:07:30 UTC (as newHistHarness), floorNow = 12:05, newest bar
// expected published (lag 4m) starts at 11:55.
func newLSHarness(t *testing.T, cfg HistConfig, syms ...string) *lsh {
	t.Helper()
	fc := newFakeClock("2026-09-21 12:07:30.000")
	h := &lsh{fc: fc, api: newFakeRatioAPI(), store: &fakeStore{data: map[string]LastStarts{}}, sink: &fakeLSSink{}, m: newMetrics(prometheus.NewRegistry()), syms: syms}
	if cfg.Workers == 0 {
		cfg.Workers = 1
	}
	h.r = NewLSReconciler(cfg, h.api, h.store, h.sink, func() []string { return h.syms }, h.m, nil)
	h.r.now, h.r.sleep = fc.Now, fc.Sleep
	return h
}

func (h *lsh) setLast(sym, at string) { h.store.data[sym] = LastStarts{Hist: ts(at), Any: ts(at)} }

func (h *lsh) load(t *testing.T) {
	t.Helper()
	if err := h.r.LoadState(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func (h *lsh) pass(t *testing.T, full bool) PassStats {
	t.Helper()
	st, err := h.r.pass(context.Background(), 0, full)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func (h *lsh) last(sym string) time.Time { h.r.mu.Lock(); defer h.r.mu.Unlock(); return h.r.last[sym] }

func TestLSPassMergesTheThreeEndpointsPerBar(t *testing.T) {
	h := newLSHarness(t, HistConfig{}, "AAA")
	h.setLast("AAA", "2026-09-21 11:00:00.000")
	h.api.setAll("AAA", "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000")
	h.load(t)
	st := h.pass(t, false)

	rows := h.sink.forSymbol("AAA")
	// bars 11:00 .. 12:00 (labels 11:05 .. 12:05): 13 rows, one per bar, all three columns set
	if st.Rows != 13 || len(rows) != 13 {
		t.Fatalf("rows=%d (stats %d), want 13", len(rows), st.Rows)
	}
	for i, r := range rows {
		wantStart := ts("2026-09-21 11:00:00.000").Add(time.Duration(i) * BarInterval)
		if !r.StartTime.Equal(wantStart) || !r.SnapTime.Equal(wantStart.Add(BarInterval)) || r.SrcRank != RankHist {
			t.Fatalf("row %d: start=%v snap=%v rank=%d", i, r.StartTime, r.SnapTime, r.SrcRank)
		}
		for k := RatioKind(0); k < numRatios; k++ {
			if math.IsNaN(r.Ratios[k]) || math.Floor(r.Ratios[k]) != float64(k+1) {
				t.Fatalf("row %d %s = %v: columns mixed up", i, k, r.Ratios[k])
			}
		}
	}
	if h.api.callCount() != 3 {
		t.Fatalf("calls = %d, want one per endpoint", h.api.callCount())
	}
	if !h.last("AAA").Equal(ts("2026-09-21 12:00:00.000")) {
		t.Fatalf("last = %v", h.last("AAA"))
	}
}

func TestLSRowsStopAtTheNewestLabelEveryEndpointHas(t *testing.T) {
	h := newLSHarness(t, HistConfig{}, "AAA")
	h.setLast("AAA", "2026-09-21 11:00:00.000")
	h.api.setAll("AAA", "2026-09-21 10:00:00.000", "2026-09-21 12:00:00.000")
	h.api.setSeries(RatioGlobalAccount, "AAA", "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000", 3) // published one label earlier
	h.load(t)
	h.pass(t, false)

	rows := h.sink.forSymbol("AAA")
	// bars 11:00 .. 11:55; bar 12:00 (label 12:05) waits until all three endpoints have it
	if n := len(rows); n != 12 || !rows[n-1].StartTime.Equal(ts("2026-09-21 11:55:00.000")) {
		t.Fatalf("got %d rows ending %v; the 12:00 bar must wait until all three endpoints have label 12:05", n, rows[len(rows)-1].StartTime)
	}
	// the next pass (an hour later) picks the held-back bar up once the slow endpoints publish it
	h.api.setAll("AAA", "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000")
	_ = h.fc.Sleep(context.Background(), time.Hour)
	h.pass(t, false)
	rows = h.sink.forSymbol("AAA")
	last := rows[len(rows)-1]
	if !last.StartTime.Equal(ts("2026-09-21 12:00:00.000")) || math.IsNaN(last.Ratios[RatioTopAccount]) {
		t.Fatalf("last row %+v", last)
	}
}

func TestLSHoleInOneEndpointIsNullNotDropped(t *testing.T) {
	h := newLSHarness(t, HistConfig{}, "AAA")
	h.setLast("AAA", "2026-09-21 11:00:00.000")
	h.api.setAll("AAA", "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000")
	pts := h.api.series[RatioTopPosition]["AAA"]
	var holed []RatioPoint
	for _, p := range pts {
		if !p.Label.Equal(ts("2026-09-21 11:30:00.000")) {
			holed = append(holed, p)
		}
	}
	h.api.series[RatioTopPosition]["AAA"] = holed
	h.load(t)
	h.pass(t, false)

	var found bool
	for _, r := range h.sink.forSymbol("AAA") {
		if r.StartTime.Equal(ts("2026-09-21 11:25:00.000")) {
			found = true
			if !math.IsNaN(r.Ratios[RatioTopPosition]) || math.IsNaN(r.Ratios[RatioTopAccount]) || math.IsNaN(r.Ratios[RatioGlobalAccount]) {
				t.Fatalf("row %+v: only the holed column may be missing", r)
			}
			if args := r.appendArgs(); args[3].(*float64) != nil || args[2].(*float64) == nil {
				t.Fatalf("appendArgs must map the hole to NULL: %v", args)
			}
		}
	}
	if !found {
		t.Fatal("the bar with one missing column must still be written")
	}
	if v := testutil.ToFloat64(h.m.ls.nullCells.WithLabelValues("sum_toptrader_long_short_ratio")); v != 1 {
		t.Fatalf("null_cells = %v", v)
	}
}

func TestLSOneEmptyEndpointHoldsTheSymbolBack(t *testing.T) {
	h := newLSHarness(t, HistConfig{}, "AAA")
	h.setLast("AAA", "2026-09-21 11:00:00.000")
	h.api.setAll("AAA", "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000")
	delete(h.api.series[RatioTopAccount], "AAA") // this endpoint returns [] for the window
	h.load(t)
	st := h.pass(t, false)
	if st.Failed != 0 || len(h.sink.forSymbol("AAA")) != 0 || !h.last("AAA").Equal(ts("2026-09-21 11:00:00.000")) {
		t.Fatalf("stats %+v rows %d last %v: nothing may be written and the state must stay put", st, len(h.sink.forSymbol("AAA")), h.last("AAA"))
	}
	if v := testutil.ToFloat64(h.m.ls.partial); v != 1 {
		t.Fatalf("partial = %v", v)
	}
	// once the endpoint serves the window again, the next pass writes everything
	h.api.setSeries(RatioTopAccount, "AAA", "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000", 1)
	if st = h.pass(t, false); st.Rows != 13 {
		t.Fatalf("after recovery: stats %+v", st)
	}
}

func TestLSOneFailingEndpointFailsTheSymbolAndWritesNothing(t *testing.T) {
	h := newLSHarness(t, HistConfig{}, "AAA", "BBB")
	for _, s := range h.syms {
		h.setLast(s, "2026-09-21 11:00:00.000")
		h.api.setAll(s, "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000")
	}
	h.api.errFor[RatioTopPosition]["AAA"] = errors.New("boom")
	h.load(t)
	st := h.pass(t, false)

	if st.Failed != 1 || st.Fetched != 1 || len(h.sink.forSymbol("AAA")) != 0 || len(h.sink.forSymbol("BBB")) != 13 {
		t.Fatalf("stats %+v, AAA rows %d, BBB rows %d", st, len(h.sink.forSymbol("AAA")), len(h.sink.forSymbol("BBB")))
	}
	if !h.last("AAA").Equal(ts("2026-09-21 11:00:00.000")) {
		t.Fatalf("a failed symbol's state must not move: %v", h.last("AAA"))
	}
}

func TestLSTransientFailureIsRetriedOnce(t *testing.T) {
	h := newLSHarness(t, HistConfig{}, "AAA")
	h.setLast("AAA", "2026-09-21 11:00:00.000")
	h.api.setAll("AAA", "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000")
	h.api.failN[RatioGlobalAccount]["AAA"] = 1
	h.load(t)
	st := h.pass(t, false)
	if st.Failed != 0 || st.Rows != 13 {
		t.Fatalf("stats %+v: the retry should have fetched the symbol", st)
	}
}

func TestLSEmptyAndInvalidSymbolAreNotFailures(t *testing.T) {
	h := newLSHarness(t, HistConfig{}, "NEW", "GONE")
	h.api.errFor[RatioTopAccount]["GONE"] = ErrInvalidSymbol
	h.load(t)
	st := h.pass(t, true)
	if st.Failed != 0 || st.Fetched != 2 || st.Rows != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestLSColdStartPagesEveryEndpointBackwardsWithoutGaps(t *testing.T) {
	h := newLSHarness(t, HistConfig{MaxLimit: 100, LimitMargin: 3, ColdStartWindow: 48 * time.Hour}, "AAA")
	h.api.setAll("AAA", "2026-09-18 00:00:00.000", "2026-09-21 12:05:00.000")
	h.load(t)
	st := h.pass(t, false)

	rows := h.sink.forSymbol("AAA")
	want := int((48 * time.Hour) / BarInterval) // bars 09-19 12:05 .. 09-21 12:00
	if st.ColdStart != 1 || len(rows) != want {
		t.Fatalf("cold=%d rows=%d, want %d", st.ColdStart, len(rows), want)
	}
	for i := 1; i < len(rows); i++ {
		if rows[i].StartTime.Sub(rows[i-1].StartTime) != BarInterval {
			t.Fatalf("gap or duplicate between %v and %v", rows[i-1].StartTime, rows[i].StartTime)
		}
	}
	if per := h.api.callCount() / 3; per < 5 {
		t.Fatalf("%d calls per endpoint; 576 bars at limit 100 need paging", per)
	}
}

func TestLSIncrementalPassSkipsCurrentSymbolsAndStateWaitsForTheFlush(t *testing.T) {
	h := newLSHarness(t, HistConfig{}, "CUR", "OLD")
	h.setLast("CUR", "2026-09-21 11:55:00.000")
	h.setLast("OLD", "2026-09-21 11:00:00.000")
	for _, s := range h.syms {
		h.api.setAll(s, "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000")
	}
	h.load(t)
	h.sink.flushErr = errors.New("clickhouse down")
	st := h.pass(t, false)
	if st.Skipped != 1 || st.Committed || !h.last("OLD").Equal(ts("2026-09-21 11:00:00.000")) {
		t.Fatalf("stats %+v last(OLD)=%v", st, h.last("OLD"))
	}
	h.sink.flushErr = nil
	if st = h.pass(t, false); !st.Committed || !h.last("OLD").Equal(ts("2026-09-21 12:00:00.000")) {
		t.Fatalf("after recovery: stats %+v last(OLD)=%v", st, h.last("OLD"))
	}
}

func TestLSScheduledPassIsSpread(t *testing.T) {
	h := newLSHarness(t, HistConfig{}, "A", "B", "C", "D")
	for _, s := range h.syms {
		h.setLast(s, "2026-09-21 11:00:00.000")
		h.api.setAll(s, "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000")
	}
	h.load(t)
	start := h.fc.Now()
	if _, err := h.r.Pass(context.Background(), 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	within(t, h.fc.Now().Sub(start), 22*time.Minute+30*time.Second, time.Second, "time until the last symbol's slot")
}

func TestLSRowValidate(t *testing.T) {
	ok := newLSRow("AAA", ts("2026-09-21 11:00:00.000"), RankHist)
	ok.Ratios[RatioTopAccount] = 1.5
	if err := ok.validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*LSRow){
		"no value":     func(r *LSRow) { r.Ratios[RatioTopAccount] = math.NaN() },
		"negative":     func(r *LSRow) { r.Ratios[RatioTopPosition] = -1 },
		"infinite":     func(r *LSRow) { r.Ratios[RatioGlobalAccount] = math.Inf(1) },
		"live rank":    func(r *LSRow) { r.SrcRank = RankLive },
		"unaligned":    func(r *LSRow) { r.StartTime = r.StartTime.Add(time.Minute) },
		"empty symbol": func(r *LSRow) { r.Symbol = "" },
	} {
		r := ok
		mutate(&r)
		if r.validate() == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestRatioHistoryRequestAndParsing(t *testing.T) {
	var gotPath string
	var gotQuery map[string]string
	c, m := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = map[string]string{}
		for k := range r.URL.Query() {
			gotQuery[k] = r.URL.Query().Get(k)
		}
		_, _ = w.Write([]byte(ratioBody))
	}, nil, nil)
	pts, err := c.RatioHistory(context.Background(), RatioTopPosition, "BTCUSDT", ts("2026-10-09 00:00:00.000"), ts("2026-10-09 00:10:00.000"), 5)
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/futures/data/topLongShortPositionRatio" || gotQuery["period"] != "5m" || gotQuery["limit"] != "5" ||
		gotQuery["startTime"] != "1791504000000" || gotQuery["endTime"] != "1791504600000" || gotQuery["symbol"] != "BTCUSDT" {
		t.Fatalf("request %s %v", gotPath, gotQuery)
	}
	if len(pts) != 2 || !pts[0].Label.Equal(ts("2026-10-09 00:00:00.000")) || pts[0].Ratio != 2.003 || pts[1].Ratio != 1.9994 {
		t.Fatalf("parsed %+v", pts)
	}
	if v := testutil.ToFloat64(m.httpRequests.WithLabelValues("topLongShortPositionRatio", "ok")); v != 1 {
		t.Fatalf("http_requests = %v", v)
	}
}

// The ratio endpoints and openInterestHist count against ONE /futures/data budget.
func TestRatioHistoryAndOIHistShareTheDataPool(t *testing.T) {
	m := newMetrics(prometheus.NewRegistry())
	pool := NewDataPool(DataPoolConfig{Metrics: m, RPS: 1000, Burst: 1000, WindowCap: 900})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/futures/data/openInterestHist" {
			_, _ = w.Write([]byte(histBody))
			return
		}
		_, _ = w.Write([]byte(ratioBody))
	}))
	defer srv.Close()
	c := NewClient(ClientConfig{BaseURL: srv.URL, HTTPClient: srv.Client(), Pool: pool, Metrics: m})
	now := time.Now()
	if _, err := c.History(context.Background(), "BTCUSDT", now, now, 5); err != nil {
		t.Fatal(err)
	}
	for k := RatioKind(0); k < numRatios; k++ {
		if _, err := c.RatioHistory(context.Background(), k, "BTCUSDT", now, now, 5); err != nil {
			t.Fatal(err)
		}
	}
	if pool.Used() != 4 {
		t.Fatalf("pool counted %d requests, want 4 (1 OI + 3 ratios)", pool.Used())
	}
}

func TestRatioHistory429PausesTheSharedPool(t *testing.T) {
	m := newMetrics(prometheus.NewRegistry())
	fc := newFakeClock("2026-09-21 12:00:00.000")
	pool := NewDataPool(DataPoolConfig{Metrics: m, Clock: fc.Now, Sleep: blockingSleep})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c := NewClient(ClientConfig{BaseURL: srv.URL, HTTPClient: srv.Client(), Pool: pool, Metrics: m})
	var rl *RateLimitError
	if _, err := c.RatioHistory(context.Background(), RatioTopAccount, "BTCUSDT", time.Now(), time.Now(), 5); !errors.As(err, &rl) {
		t.Fatalf("err = %v, want a RateLimitError", err)
	}
	// openInterestHist is now held at the same paused pool
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, err := c.History(ctx, "BTCUSDT", time.Now(), time.Now(), 5); err == nil {
		t.Fatal("openInterestHist must wait while a ratio 429 pauses the shared pool")
	}
}

func TestBuildWithOnlyLSRatioEnabled(t *testing.T) {
	s, err := Build(Config{LSRatioEnabled: true}, CHConfig{Addrs: []string{"127.0.0.1:1"}}, "http://127.0.0.1:1", nil, func() []string { return nil }, prometheus.NewRegistry(), nil)
	if err != nil || s == nil {
		t.Fatalf("Build: %v, %v", s, err)
	}
	if s.ls == nil || s.hist != nil || s.live != nil {
		t.Fatalf("ls=%v hist=%v live=%v", s.ls != nil, s.hist != nil, s.live != nil)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewRequiresLSCollaborators(t *testing.T) {
	if _, err := New(Config{LSRatioEnabled: true}, Deps{Symbols: func() []string { return nil }}); err == nil {
		t.Fatal("expected an error without RatioHistSource / LSStore / LSSink")
	}
}
