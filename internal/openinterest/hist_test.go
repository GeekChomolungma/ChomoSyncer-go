package openinterest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeAPI models the real openInterestHist semantics (verified against Binance):
// it returns the NEWEST `limit` points whose label lies in [start, end], oldest first.
type fakeAPI struct {
	mu        sync.Mutex
	series    map[string][]HistPoint // symbol -> labels ascending
	calls     []histCall
	errFor    map[string]error // always fail this symbol
	ignoreEnd bool             // return labels past `end` too (to exercise the caller's own future-label guard)
	failN     map[string]int   // fail the first N calls for this symbol
}

type histCall struct {
	symbol     string
	start, end time.Time
	limit      int
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{series: map[string][]HistPoint{}, errFor: map[string]error{}, failN: map[string]int{}}
}

// setSeries publishes labels from..to (inclusive, every 5m) with value base+i.
func (f *fakeAPI) setSeries(symbol, from, to string, base float64) {
	var pts []HistPoint
	i := 0
	for l := ts(from); !l.After(ts(to)); l = l.Add(BarInterval) {
		pts = append(pts, HistPoint{Label: l, OpenInterest: base + float64(i)})
		i++
	}
	f.series[symbol] = pts
}

func (f *fakeAPI) History(_ context.Context, symbol string, start, end time.Time, limit int) ([]HistPoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, histCall{symbol, start, end, limit})
	if err := f.errFor[symbol]; err != nil {
		return nil, err
	}
	if f.failN[symbol] > 0 {
		f.failN[symbol]--
		return nil, errors.New("transient")
	}
	var sel []HistPoint
	for _, p := range f.series[symbol] {
		if !p.Label.Before(start) && (f.ignoreEnd || !p.Label.After(end)) {
			sel = append(sel, p)
		}
	}
	if len(sel) > limit {
		sel = sel[len(sel)-limit:]
	}
	return append([]HistPoint(nil), sel...), nil
}

func (f *fakeAPI) callsFor(symbol string) []histCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []histCall
	for _, c := range f.calls {
		if c.symbol == symbol {
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeAPI) totalCalls() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }

type fakeStore struct {
	mu     sync.Mutex
	data   map[string]LastStarts
	failN  int
	calls  int
	closed bool
}

func (s *fakeStore) LastStarts(_ context.Context, symbols []string) (map[string]LastStarts, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.failN {
		return nil, errors.New("clickhouse down")
	}
	out := map[string]LastStarts{}
	for _, sym := range symbols {
		if v, ok := s.data[sym]; ok {
			out[sym] = v
		}
	}
	return out, nil
}

func (s *fakeStore) Close() error { s.mu.Lock(); s.closed = true; s.mu.Unlock(); return nil }

type fakeSink struct {
	mu       sync.Mutex
	rows     []Row
	flushErr error
	flushes  int
}

func (s *fakeSink) Push(_ context.Context, r Row) error {
	if err := r.validate(); err != nil {
		return err
	}
	s.mu.Lock()
	s.rows = append(s.rows, r)
	s.mu.Unlock()
	return nil
}

func (s *fakeSink) Flush(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushes++
	return s.flushErr
}

func (s *fakeSink) forSymbol(sym string) []Row {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Row
	for _, r := range s.rows {
		if r.Symbol == sym {
			out = append(out, r)
		}
	}
	return out
}

func (s *fakeSink) count() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.rows) }

type hh struct {
	fc    *fakeClock
	api   *fakeAPI
	store *fakeStore
	sink  *fakeSink
	m     *metrics
	reg   *prometheus.Registry
	live  *liveCache
	syms  []string
	r     *Reconciler
}

// newHistHarness: now = 12:07:30 UTC, so floorNow = 12:05 and the newest bar
// expected published (lag 4m) starts at 11:55.
func newHistHarness(t *testing.T, cfg HistConfig, syms ...string) *hh {
	t.Helper()
	fc := newFakeClock("2026-09-21 12:07:30.000")
	reg := prometheus.NewRegistry()
	h := &hh{fc: fc, api: newFakeAPI(), store: &fakeStore{data: map[string]LastStarts{}}, sink: &fakeSink{}, m: newMetrics(reg), reg: reg, live: newLiveCache(24), syms: syms}
	if cfg.Workers == 0 {
		cfg.Workers = 1 // deterministic call order
	}
	h.r = NewReconciler(cfg, h.api, h.store, h.sink, func() []string { return h.syms }, h.live, h.m, nil)
	h.r.now, h.r.sleep = fc.Now, fc.Sleep
	return h
}

func (h *hh) setLast(sym, hist string) { h.store.data[sym] = LastStarts{Hist: ts(hist), Any: ts(hist)} }

func (h *hh) load(t *testing.T) {
	t.Helper()
	if err := h.r.LoadState(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func (h *hh) pass(t *testing.T, spread time.Duration) PassStats {
	t.Helper()
	st, err := h.r.Pass(context.Background(), spread)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func (h *hh) last(sym string) time.Time { h.r.mu.Lock(); defer h.r.mu.Unlock(); return h.r.last[sym] }

func histogramCount(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf.GetMetric()[0].GetHistogram().GetSampleCount()
		}
	}
	return 0
}

func TestNextReconcileTime(t *testing.T) {
	cases := []struct{ now, want string }{
		{"2026-09-21 12:07:30.000", "2026-09-21 13:05:00.000"},
		{"2026-09-21 12:04:59.000", "2026-09-21 12:05:00.000"},
		{"2026-09-21 12:05:00.000", "2026-09-21 13:05:00.000"}, // strictly after now
		{"2026-09-21 23:59:00.000", "2026-09-22 00:05:00.000"},
	}
	for _, c := range cases {
		if got := nextReconcileTime(ts(c.now), time.Hour, 5*time.Minute); !got.Equal(ts(c.want)) {
			t.Errorf("nextReconcileTime(%s) = %v, want %s", c.now, got, c.want)
		}
	}
}

func TestLoadStateReadsCalibratedRowsOnly(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "AAA", "BBB", "CCC")
	h.store.data["AAA"] = LastStarts{Any: ts("2026-09-21 12:00:00.000"), Hist: ts("2026-09-21 11:00:00.000")} // live rows newer than the calibrated ones
	h.store.data["BBB"] = LastStarts{Any: ts("2026-09-21 12:00:00.000")}                                      // live rows only
	h.load(t)                                                                                                 // CCC has no rows at all
	if got := h.last("AAA"); !got.Equal(ts("2026-09-21 11:00:00.000")) {
		t.Fatalf("AAA last = %v, want the HIST max (11:00), not the live max", got)
	}
	if !h.last("BBB").IsZero() || !h.last("CCC").IsZero() {
		t.Fatal("symbols without calibrated rows must have no state")
	}
}

func TestLoadStateRetriesUntilStoreAnswers(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "AAA")
	h.store.failN = 2
	start := h.fc.Now()
	h.setLast("AAA", "2026-09-21 11:00:00.000")
	h.load(t)
	if h.store.calls != 3 {
		t.Fatalf("store calls = %d, want 3", h.store.calls)
	}
	within(t, h.fc.Now().Sub(start), 15*time.Second, time.Millisecond, "backoff 5s + 10s") // fake sleep advances the clock
	if h.last("AAA").IsZero() {
		t.Fatal("state not loaded after the retries")
	}
}

func TestPassSkipsSymbolsAlreadyCurrent(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "AAA")
	h.setLast("AAA", "2026-09-21 11:55:00.000") // == newest expected bar
	h.api.setSeries("AAA", "2026-09-21 11:00:00.000", "2026-09-21 12:05:00.000", 100)
	h.load(t)
	st := h.pass(t, 0)
	if st.Skipped != 1 || st.Fetched != 0 || h.api.totalCalls() != 0 {
		t.Fatalf("stats=%+v calls=%d, want the symbol skipped with no request", st, h.api.totalCalls())
	}
}

func TestPassCatchesUpFromTheLastCalibratedBar(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "AAA")
	h.setLast("AAA", "2026-09-21 11:50:00.000")
	h.api.setSeries("AAA", "2026-09-21 11:00:00.000", "2026-09-21 12:05:00.000", 100)
	h.load(t)
	st := h.pass(t, 0)

	calls := h.api.callsFor("AAA")
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want exactly 1 request", len(calls))
	}
	// since = 11:50 -> first label wanted 11:55; needed = (12:05-11:50)/5m = 3; limit = 3+3
	if !calls[0].start.Equal(ts("2026-09-21 11:55:00.000")) || calls[0].limit != 6 {
		t.Fatalf("call = %+v, want start 11:55 limit 6", calls[0])
	}
	rows := h.sink.forSymbol("AAA")
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3 (labels 11:55, 12:00, 12:05)", len(rows))
	}
	// label T is written to the bar T-5m
	wantStarts := []string{"2026-09-21 11:50:00.000", "2026-09-21 11:55:00.000", "2026-09-21 12:00:00.000"}
	for i, r := range rows {
		if !r.StartTime.Equal(ts(wantStarts[i])) || !r.SnapTime.Equal(r.StartTime.Add(BarInterval)) || r.SrcRank != RankHist {
			t.Errorf("row %d = %+v, want start %s snap=start+5m rank hist", i, r, wantStarts[i])
		}
	}
	if !h.last("AAA").Equal(ts("2026-09-21 12:00:00.000")) || !st.Committed {
		t.Fatalf("state = %v committed=%v, want advanced to 12:00", h.last("AAA"), st.Committed)
	}
	// running again right away has nothing to do
	if st2 := h.pass(t, 0); st2.Skipped != 1 || h.api.totalCalls() != 1 {
		t.Fatalf("second pass stats=%+v calls=%d, want skipped and no new request", st2, h.api.totalCalls())
	}
}

func TestHourlyCalibrationIsOneSmallRequest(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "AAA")
	h.setLast("AAA", "2026-09-21 10:55:00.000") // a bit over an hour behind
	h.api.setSeries("AAA", "2026-09-21 09:00:00.000", "2026-09-21 12:05:00.000", 1)
	h.load(t)
	h.pass(t, 0)
	calls := h.api.callsFor("AAA")
	// needed = (12:05 - 10:55)/5m = 14 ; limit = 17 ; the API holds 14 labels in range
	if len(calls) != 1 || calls[0].limit != 17 {
		t.Fatalf("calls = %+v, want one request with limit 17", calls)
	}
	if n := len(h.sink.forSymbol("AAA")); n != 14 {
		t.Fatalf("rows = %d, want 14", n)
	}
}

func TestColdStartPagesBackwardsWithoutGapsOrDuplicates(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "NEW")
	h.api.setSeries("NEW", "2026-09-10 00:00:00.000", "2026-09-21 12:05:00.000", 1) // far more than the 48h window
	h.load(t)                                                                       // no rows in the database
	st := h.pass(t, 0)

	if st.ColdStart != 1 {
		t.Fatalf("ColdStart = %d, want 1", st.ColdStart)
	}
	calls := h.api.callsFor("NEW")
	if len(calls) != 2 {
		t.Fatalf("calls = %d, want 2 pages (500 + the remainder)", len(calls))
	}
	since := ts("2026-09-21 12:05:00.000").Add(-48 * time.Hour) // floorNow - 48h
	startLabel := since.Add(BarInterval)
	if !calls[0].start.Equal(startLabel) || calls[0].limit != 500 || !calls[0].end.Equal(h.fc.Now()) {
		t.Fatalf("page 1 = %+v, want start %v, limit 500, end=now", calls[0], startLabel)
	}
	if !calls[1].start.Equal(startLabel) {
		t.Fatalf("page 2 start moved: %v", calls[1].start)
	}
	rows := h.sink.forSymbol("NEW")
	if len(rows) != 576 { // labels (since+5m .. 12:05) every 5m over 48h
		t.Fatalf("rows = %d, want 576", len(rows))
	}
	seen := map[time.Time]bool{}
	for _, r := range rows {
		if seen[r.StartTime] {
			t.Fatalf("duplicate bar %v", r.StartTime)
		}
		seen[r.StartTime] = true
	}
	// page 2 ends just before the oldest label page 1 returned
	oldestOfPage1 := ts("2026-09-21 12:05:00.000").Add(-499 * BarInterval)
	if !calls[1].end.Equal(oldestOfPage1.Add(-time.Millisecond)) {
		t.Fatalf("page 2 end = %v, want %v", calls[1].end, oldestOfPage1.Add(-time.Millisecond))
	}
	if !h.last("NEW").Equal(ts("2026-09-21 12:00:00.000")) {
		t.Fatalf("state = %v", h.last("NEW"))
	}
}

func TestGapBeyondMaxBackfillIsCappedAndCounted(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "OLD")
	h.setLast("OLD", "2026-09-01 00:00:00.000") // 20 days behind
	h.api.setSeries("OLD", "2026-09-01 00:00:00.000", "2026-09-21 12:05:00.000", 1)
	h.load(t)
	st := h.pass(t, 0)
	if st.BeyondCap != 1 || testutil.ToFloat64(h.m.histBeyondCap) != 1 {
		t.Fatalf("BeyondCap = %d, want 1", st.BeyondCap)
	}
	capStart := floorBar(h.fc.Now().Add(-168 * time.Hour)) // 7 days
	first := h.api.callsFor("OLD")[0]
	if !first.start.Equal(capStart.Add(BarInterval)) {
		t.Fatalf("first request start = %v, want the capped start %v", first.start, capStart.Add(BarInterval))
	}
	rows := h.sink.forSymbol("OLD")
	if len(rows) == 0 || rows[0].StartTime.Before(capStart) {
		t.Fatalf("rows reach back before the cap: first=%v cap=%v", rows[0].StartTime, capStart)
	}
}

func TestPassSkipsUnalignedAndFutureLabels(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "AAA")
	h.setLast("AAA", "2026-09-21 11:45:00.000")
	h.api.ignoreEnd = true // let the fake hand back a label from the future
	h.api.series["AAA"] = []HistPoint{
		{Label: ts("2026-09-21 11:50:00.000"), OpenInterest: 1},
		{Label: ts("2026-09-21 11:52:00.000"), OpenInterest: 2}, // not on a boundary
		{Label: ts("2026-09-21 12:05:00.000"), OpenInterest: 3},
		{Label: ts("2026-09-21 12:10:00.000"), OpenInterest: 4}, // after "now" (12:07:30)
	}
	h.load(t)
	h.pass(t, 0)
	starts := map[time.Time]bool{}
	for _, r := range h.sink.forSymbol("AAA") {
		starts[r.StartTime] = true
	}
	if !starts[ts("2026-09-21 11:45:00.000")] || !starts[ts("2026-09-21 12:00:00.000")] || len(starts) != 2 {
		t.Fatalf("starts = %v, want only the two well-formed, already-published labels (11:50 -> 11:45, 12:05 -> 12:00)", starts)
	}
}

func TestEmptyAndInvalidSymbolAreNotFailures(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "NOHIST", "GONE")
	h.api.errFor["GONE"] = ErrInvalidSymbol
	h.load(t)
	st := h.pass(t, 0)
	if st.Failed != 0 || st.Fetched != 2 {
		t.Fatalf("stats = %+v, want 2 fetched and no failures", st)
	}
	if !h.last("NOHIST").IsZero() || !h.last("GONE").IsZero() {
		t.Fatal("no state should be recorded for symbols without data")
	}
}

func TestFailedSymbolIsRetriedOnceThenReported(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "OKAY", "FLAKY", "DOWN")
	for _, s := range h.syms {
		h.setLast(s, "2026-09-21 11:00:00.000")
		h.api.setSeries(s, "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000", 1)
	}
	h.api.failN["FLAKY"] = 1                        // fails once, works on the retry
	h.api.errFor["DOWN"] = errors.New("rate limit") // never works
	h.load(t)
	st := h.pass(t, 0)

	if st.Fetched != 2 || st.Failed != 1 {
		t.Fatalf("stats = %+v, want 2 fetched (OKAY + FLAKY after retry), 1 failed (DOWN)", st)
	}
	if n := len(h.api.callsFor("FLAKY")); n != 2 {
		t.Fatalf("FLAKY calls = %d, want 2", n)
	}
	if n := len(h.api.callsFor("DOWN")); n != 2 {
		t.Fatalf("DOWN calls = %d, want 2 (first try + one retry)", n)
	}
	if !h.last("DOWN").Equal(ts("2026-09-21 11:00:00.000")) {
		t.Fatalf("a failed symbol's state must not advance: %v", h.last("DOWN"))
	}
	if h.last("FLAKY").Equal(ts("2026-09-21 11:00:00.000")) {
		t.Fatal("the retried symbol's state should have advanced")
	}
}

func TestStateIsNotAdvancedWhenTheFlushFails(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "AAA")
	h.setLast("AAA", "2026-09-21 11:00:00.000")
	h.api.setSeries("AAA", "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000", 1)
	h.load(t)

	h.sink.flushErr = errors.New("clickhouse down")
	st := h.pass(t, 0)
	if st.Committed || !h.last("AAA").Equal(ts("2026-09-21 11:00:00.000")) {
		t.Fatalf("committed=%v last=%v: state must stay put after a failed flush", st.Committed, h.last("AAA"))
	}

	h.sink.flushErr = nil
	st = h.pass(t, 0) // the next pass refetches the same window
	if !st.Committed || !h.last("AAA").Equal(ts("2026-09-21 12:00:00.000")) {
		t.Fatalf("after recovery committed=%v last=%v", st.Committed, h.last("AAA"))
	}
}

func TestBiggestGapsAreFetchedFirst(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "SMALL", "BIG", "MID")
	h.setLast("SMALL", "2026-09-21 11:50:00.000") // 3 bars
	h.setLast("BIG", "2026-09-21 09:00:00.000")   // 37 bars
	h.setLast("MID", "2026-09-21 11:00:00.000")   // 13 bars
	for _, s := range h.syms {
		h.api.setSeries(s, "2026-09-21 08:00:00.000", "2026-09-21 12:05:00.000", 1)
	}
	h.load(t)
	h.pass(t, 0)
	var order []string
	for _, c := range h.api.calls {
		order = append(order, c.symbol)
	}
	if len(order) != 3 || order[0] != "BIG" || order[1] != "MID" || order[2] != "SMALL" {
		t.Fatalf("fetch order = %v, want BIG, MID, SMALL", order)
	}
}

func TestScheduledPassSpreadsRequestsEvenly(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "A", "B", "C", "D")
	for _, s := range h.syms {
		h.setLast(s, "2026-09-21 11:00:00.000")
		h.api.setSeries(s, "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000", 1)
	}
	h.load(t)
	start := h.fc.Now()
	h.pass(t, 20*time.Minute) // 4 jobs -> one every 5 minutes: at +0, +5, +10, +15
	within(t, h.fc.Now().Sub(start), 15*time.Minute, time.Second, "time until the last job's slot")
}

func TestStartupPassIsNotSpread(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "A", "B", "C")
	for _, s := range h.syms {
		h.setLast(s, "2026-09-21 11:00:00.000")
		h.api.setSeries(s, "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000", 1)
	}
	h.load(t)
	start := h.fc.Now()
	h.pass(t, 0)
	if !h.fc.Now().Equal(start) {
		t.Fatalf("a startup pass slept %v; it should only be paced by the /futures/data pool", h.fc.Now().Sub(start))
	}
}

func TestLiveVersusHistDiagnostics(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "AAA")
	h.setLast("AAA", "2026-09-21 11:45:00.000")
	h.api.setSeries("AAA", "2026-09-21 11:00:00.000", "2026-09-21 12:05:00.000", 100) // hist bars 11:45..12:00 (labels 11:50..12:05)
	// live saw the 11:50 bar only
	h.live.put("AAA", ts("2026-09-21 11:50:00.000"), 100.5)
	h.r.SetLiveSince(ts("2026-09-21 11:52:00.000")) // live started after the 11:45 bar closed
	h.load(t)
	st := h.pass(t, 0)

	if n := histogramCount(t, h.reg, "oi_live_vs_hist_rel_diff"); n != 1 {
		t.Fatalf("rel_diff observations = %d, want 1 (only the 11:50 bar had a live value)", n)
	}
	// bars: 11:45 (closed 11:50 < liveSince -> not a gap), 11:50 (has live), 11:55 and 12:00 (live missed them)
	if st.LiveGapBar != 2 || testutil.ToFloat64(h.m.liveGapBars) != 2 {
		t.Fatalf("live gaps = %d, want 2 (11:55 and 12:00)", st.LiveGapBar)
	}
}

func TestCrossSectionGauge(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "AAA", "BBB")
	h.setLast("AAA", "2026-09-21 11:00:00.000")
	h.setLast("BBB", "2026-09-21 11:00:00.000")
	h.api.setSeries("AAA", "2026-09-21 10:00:00.000", "2026-09-21 12:05:00.000", 1)
	h.api.errFor["BBB"] = errors.New("boom")
	h.load(t)
	h.pass(t, 0)
	if v := gaugeVal(t, h.reg, "oi_cross_section_complete_ratio"); v != 0.5 {
		t.Fatalf("cross-section ratio = %v, want 0.5 (AAA current, BBB failed)", v)
	}
}

func TestNewSymbolAppearingLaterIsBackfilled(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "AAA")
	h.setLast("AAA", "2026-09-21 11:55:00.000")
	h.load(t)
	h.pass(t, 0)
	h.syms = append(h.syms, "LISTED") // universe refresh added a symbol
	h.api.setSeries("LISTED", "2026-09-21 11:00:00.000", "2026-09-21 12:05:00.000", 1)
	st := h.pass(t, 0)
	if st.ColdStart != 1 || len(h.sink.forSymbol("LISTED")) == 0 {
		t.Fatalf("stats=%+v rows=%d: the new symbol should be cold-start backfilled", st, len(h.sink.forSymbol("LISTED")))
	}
}

func TestRunLoadsStateThenPassesOnSchedule(t *testing.T) {
	h := newHistHarness(t, HistConfig{}, "AAA")
	h.setLast("AAA", "2026-09-21 11:00:00.000")
	h.api.setSeries("AAA", "2026-09-21 10:00:00.000", "2026-09-21 14:00:00.000", 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var sleeps []time.Duration
	h.r.sleep = func(ctx context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		if len(sleeps) >= 2 {
			cancel() // stop after the second wait
		}
		return h.fc.Sleep(ctx, d)
	}
	h.r.Run(ctx)

	if h.api.totalCalls() == 0 {
		t.Fatal("the start-up pass did not run")
	}
	if len(sleeps) < 1 || sleeps[0] != 57*time.Minute+30*time.Second { // 12:07:30 -> 13:05:00
		t.Fatalf("first wait = %v, want 57m30s until hh:05", sleeps)
	}
}
