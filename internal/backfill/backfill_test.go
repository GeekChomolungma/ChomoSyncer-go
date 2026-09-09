package backfill

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/HarvestStars/chomosyncer-go/internal/chwriter"
	"github.com/HarvestStars/chomosyncer-go/internal/rediswin"
)

// --- fakes ---

type fetchCall struct {
	symbol, interval string
	since, until     time.Time
}

type fakeFetcher struct {
	mu      sync.Mutex
	calls   []fetchCall
	rows    map[string][]chwriter.Row // "SYM/iv" -> rows
	err     error
	blockCh chan struct{} // if non-nil, Fetch blocks until it is closed / receives
}

func (f *fakeFetcher) Fetch(ctx context.Context, sym, iv string, since, until time.Time) ([]chwriter.Row, error) {
	f.mu.Lock()
	f.calls = append(f.calls, fetchCall{sym, iv, since, until})
	bl := f.blockCh
	f.mu.Unlock()
	if bl != nil {
		select {
		case <-bl:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rows[sym+"/"+iv], nil
}
func (f *fakeFetcher) callCount() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }
func (f *fakeFetcher) lastCall() (fetchCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return fetchCall{}, false
	}
	return f.calls[len(f.calls)-1], true
}

type fakeArchive struct {
	mu         sync.Mutex
	rows       []chwriter.Row
	err        error
	flushErr   error
	flushCalls int
}

func (a *fakeArchive) Push(_ context.Context, r chwriter.Row) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	a.rows = append(a.rows, r)
	return nil
}

func (a *fakeArchive) Flush(_ context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.flushCalls++
	if a.flushErr != nil {
		return a.flushErr
	}
	return a.err
}

func (a *fakeArchive) count() int { a.mu.Lock(); defer a.mu.Unlock(); return len(a.rows) }

type fakeStore struct {
	mu        sync.Mutex
	maxTimes  map[string]map[string]time.Time      // iv -> sym -> max
	last      map[string]map[string][]chwriter.Row // iv -> sym -> ascending
	maxErr    error
	lastErr   error
	maxCalls  int
	lastCalls int
}

func (s *fakeStore) MaxStartTime(_ context.Context, iv string, _ []string) (map[string]time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxCalls++
	if s.maxErr != nil {
		return nil, s.maxErr
	}
	out := map[string]time.Time{}
	for k, v := range s.maxTimes[iv] {
		out[k] = v
	}
	return out, nil
}
func (s *fakeStore) LastBars(_ context.Context, iv string, _ []string, _ int) (map[string][]chwriter.Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastCalls++
	if s.lastErr != nil {
		return nil, s.lastErr
	}
	out := map[string][]chwriter.Row{}
	for k, v := range s.last[iv] {
		out[k] = v
	}
	return out, nil
}

type fakeRebuild struct {
	mu    sync.Mutex
	calls map[string][]rediswin.CompactBar // "SYM/iv"
	err   error
}

func (r *fakeRebuild) RebuildWindow(_ context.Context, sym, iv string, bars []rediswin.CompactBar) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	if r.calls == nil {
		r.calls = map[string][]rediswin.CompactBar{}
	}
	r.calls[sym+"/"+iv] = bars
	return nil
}
func (r *fakeRebuild) get(sym, iv string) ([]rediswin.CompactBar, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.calls[sym+"/"+iv]
	return v, ok
}

type fakeGate struct {
	mu   sync.Mutex
	held map[string]int
	max  map[string]int // peak hold count seen
}

func (g *fakeGate) Hold(s, i string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.held == nil {
		g.held = map[string]int{}
		g.max = map[string]int{}
	}
	g.held[s+"/"+i]++
	if g.held[s+"/"+i] > g.max[s+"/"+i] {
		g.max[s+"/"+i] = g.held[s+"/"+i]
	}
}
func (g *fakeGate) Release(s, i string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.held[s+"/"+i]--
}
func (g *fakeGate) isHeld(s, i string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.held[s+"/"+i] > 0
}
func (g *fakeGate) everHeld(s, i string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.max[s+"/"+i] > 0
}

// --- helpers ---

var fixedNow = time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)

type harness struct {
	b     *Backfiller
	fetch *fakeFetcher
	arc   map[string]*fakeArchive
	store *fakeStore
	rb    *fakeRebuild
	gate  *fakeGate
	reg   *prometheus.Registry
}

func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	h := &harness{
		fetch: &fakeFetcher{rows: map[string][]chwriter.Row{}},
		arc:   map[string]*fakeArchive{"1m": {}, "1h": {}},
		store: &fakeStore{maxTimes: map[string]map[string]time.Time{}, last: map[string]map[string][]chwriter.Row{}},
		rb:    &fakeRebuild{},
		gate:  &fakeGate{},
		reg:   prometheus.NewRegistry(),
	}
	if cfg.FlushWait == 0 {
		cfg.FlushWait = time.Millisecond
	}
	if cfg.Clock == nil {
		cfg.Clock = func() time.Time { return fixedNow }
	}
	cfg.Registerer = h.reg
	aw := map[string]ArchiveWriter{"1m": h.arc["1m"], "1h": h.arc["1h"]}
	b, err := New(cfg, h.fetch, aw, h.store, h.rb, h.gate)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.Start(ctx)
	t.Cleanup(func() { _ = b.Close() })
	h.b = b
	return h
}

func eventually(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	dl := time.Now().Add(d)
	for time.Now().Before(dl) {
		if cond() {
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func row(sym string, openMs int64, closeVal float64) chwriter.Row {
	return chwriter.Row{
		Symbol:    sym,
		StartTime: time.UnixMilli(openMs).UTC(),
		EndTime:   time.UnixMilli(openMs + 59_999).UTC(),
		Open:      1, High: 2, Low: 0.5, Close: closeVal, Volume: 10,
	}
}

// --- tests ---

func TestColdStartFlow(t *testing.T) {
	h := newHarness(t, Config{})

	base := fixedNow.Add(-3 * time.Minute).UnixMilli()
	fetched := []chwriter.Row{row("BTCUSDT", base, 1), row("BTCUSDT", base+60_000, 2), row("BTCUSDT", base+120_000, 3)}
	h.fetch.rows["BTCUSDT/1m"] = fetched
	h.store.last["1m"] = map[string][]chwriter.Row{"BTCUSDT": fetched} // CH now has them, ascending

	h.b.SubmitColdStart([]Key{{"btcusdt", "1m"}})

	eventually(t, 2*time.Second, h.b.Ready)

	if h.fetch.callCount() != 1 {
		t.Fatalf("fetch calls = %d, want 1", h.fetch.callCount())
	}
	fc, _ := h.fetch.lastCall()
	if fc.symbol != "BTCUSDT" || fc.interval != "1m" {
		t.Fatalf("fetch call = %+v", fc)
	}
	// CH empty and coldStartTime unset -> since ~ now - windowSize*ivDur (200*1m = 200m)
	wantSince := fixedNow.Add(-200 * time.Minute)
	if fc.since.Sub(wantSince).Abs() > time.Second {
		t.Fatalf("fetch since = %v, want ~%v", fc.since, wantSince)
	}
	if h.arc["1m"].count() != 3 {
		t.Fatalf("archived %d rows, want 3", h.arc["1m"].count())
	}
	bars, ok := h.rb.get("BTCUSDT", "1m")
	if !ok || len(bars) != 3 {
		t.Fatalf("rebuild bars = %v ok=%v", bars, ok)
	}
	if bars[0].StartTime != base+120_000 || bars[2].StartTime != base { // newest-first
		t.Fatalf("rebuild order wrong: %d..%d", bars[0].StartTime, bars[2].StartTime)
	}
	if h.gate.isHeld("BTCUSDT", "1m") {
		t.Fatal("gate should be released after cold start")
	}
	if !h.gate.everHeld("BTCUSDT", "1m") {
		t.Fatal("gate should have been held during cold start")
	}
	if v := testutil.ToFloat64(h.b.metrics.coldStartReady); v != 1 {
		t.Fatalf("backfill_ready = %v, want 1", v)
	}
}

func TestColdStartResumesFromCHMax(t *testing.T) {
	h := newHarness(t, Config{})
	chMax := fixedNow.Add(-5 * time.Minute)
	h.store.maxTimes["1m"] = map[string]time.Time{"BTCUSDT": chMax}

	h.b.SubmitColdStart([]Key{{"BTCUSDT", "1m"}})
	eventually(t, 2*time.Second, h.b.Ready)

	fc, _ := h.fetch.lastCall()
	want := chMax.Add(time.Minute) // next bar after the last we have
	if fc.since.Sub(want).Abs() > time.Second {
		t.Fatalf("fetch since = %v, want ~%v (chMax + 1 interval)", fc.since, want)
	}
}

func TestColdStartSkipsUpToDateSymbol(t *testing.T) {
	h := newHarness(t, Config{})
	// CH already has a bar 30s old -> next expected bar is in the future -> skip fetch
	h.store.maxTimes["1m"] = map[string]time.Time{"BTCUSDT": fixedNow.Add(-30 * time.Second)}
	h.store.last["1m"] = map[string][]chwriter.Row{"BTCUSDT": {row("BTCUSDT", fixedNow.Add(-30*time.Second).UnixMilli(), 9)}}

	h.b.SubmitColdStart([]Key{{"BTCUSDT", "1m"}})
	eventually(t, 2*time.Second, h.b.Ready)

	if h.fetch.callCount() != 0 {
		t.Fatalf("fetch calls = %d, want 0 (symbol up to date)", h.fetch.callCount())
	}
	// window still rebuilt from CH
	if _, ok := h.rb.get("BTCUSDT", "1m"); !ok {
		t.Fatal("window should still be rebuilt from CH even when no fetch needed")
	}
}

func TestColdStartResumesFromDeepCHMax(t *testing.T) {
	h := newHarness(t, Config{})
	// downtime was 10 days (240 hours)
	chMax := fixedNow.Add(-240 * time.Hour)
	h.store.maxTimes["1m"] = map[string]time.Time{"BTCUSDT": chMax}

	h.b.SubmitColdStart([]Key{{"BTCUSDT", "1m"}})
	eventually(t, 2*time.Second, h.b.Ready)

	fc, _ := h.fetch.lastCall()
	want := chMax.Add(time.Minute) // strictly resumes from ClickHouse latest time + 1 interval
	if !fc.since.Equal(want) {
		t.Fatalf("fetch since = %v, want exactly chMax + 1m = %v", fc.since, want)
	}
}

func TestShardReconnectNoCHDataUsesSince(t *testing.T) {
	h := newHarness(t, Config{})
	// down for 10h; no record in ClickHouse -> uses LastMsgAt - 1m without any maxGap truncation
	h.b.HandleGap(GapEvent{
		ShardID:     "kline_1m_0",
		Streams:     []string{"btcusdt@kline_1m"},
		LastMsgAt:   fixedNow.Add(-10 * time.Hour),
		ReconnectAt: fixedNow,
	})
	eventually(t, 2*time.Second, func() bool { return h.fetch.callCount() == 1 })
	fc, _ := h.fetch.lastCall()
	wantSince := fixedNow.Add(-10*time.Hour - time.Minute)
	if fc.since.Sub(wantSince).Abs() > time.Second {
		t.Fatalf("since = %v, want uncapped ~%v", fc.since, wantSince)
	}
}

func TestShardReconnectResumesFromCHMax(t *testing.T) {
	chMax := fixedNow.Add(-30 * time.Minute)
	h := newHarness(t, Config{})
	h.store.maxTimes["1m"] = map[string]time.Time{
		"BTCUSDT": chMax,
	}
	// Shard was down for 2 hours, but ClickHouse already has data up to -30m
	h.b.HandleGap(GapEvent{
		ShardID:     "kline_1m_0",
		Streams:     []string{"btcusdt@kline_1m"},
		LastMsgAt:   fixedNow.Add(-2 * time.Hour),
		ReconnectAt: fixedNow,
	})
	eventually(t, 2*time.Second, func() bool { return h.fetch.callCount() == 1 })
	fc, _ := h.fetch.lastCall()
	wantSince := chMax.Add(time.Minute)
	if !fc.since.Equal(wantSince) {
		t.Fatalf("since = %v, want chMax+1m = %v", fc.since, wantSince)
	}
}

func TestColdStartWithInitialDate(t *testing.T) {
	startDate := fixedNow.Add(-72 * time.Hour)
	h := newHarness(t, Config{
		ColdStartTime: startDate,
	})
	h.b.SubmitColdStart([]Key{{"BTCUSDT", "1m"}})
	eventually(t, 2*time.Second, h.b.Ready)

	if h.fetch.callCount() != 1 {
		t.Fatalf("fetch calls = %d, want 1", h.fetch.callCount())
	}
	fc, _ := h.fetch.lastCall()
	if !fc.since.Equal(startDate) {
		t.Fatalf("since = %v, want ColdStartTime %v", fc.since, startDate)
	}
}

func TestGapDebounce(t *testing.T) {
	now := fixedNow
	h := newHarness(t, Config{Clock: func() time.Time { return now }, GapDebounce: time.Minute})

	ev := GapEvent{ShardID: "s0", Streams: []string{"ethusdt@kline_1m"}, LastMsgAt: fixedNow.Add(-2 * time.Hour), ReconnectAt: fixedNow}
	h.b.HandleGap(ev)
	h.b.HandleGap(ev) // within debounce -> ignored
	eventually(t, 2*time.Second, func() bool { return h.fetch.callCount() >= 1 })
	time.Sleep(30 * time.Millisecond)
	if h.fetch.callCount() != 1 {
		t.Fatalf("fetch calls = %d, want 1 (second gap debounced)", h.fetch.callCount())
	}

	now = now.Add(2 * time.Minute) // past debounce
	h.b.HandleGap(ev)
	eventually(t, 2*time.Second, func() bool { return h.fetch.callCount() == 2 })
}

func TestInflightDedup(t *testing.T) {
	h := newHarness(t, Config{})
	h.fetch.blockCh = make(chan struct{})

	h.b.Submit(Request{Keys: []Key{{"BTCUSDT", "1m"}}, Reason: ReasonColdStart})
	eventually(t, time.Second, func() bool { return h.fetch.callCount() == 1 }) // first fetch started, blocked

	// second submit for the same key while inflight -> no new work
	h.b.Submit(Request{Keys: []Key{{"btcusdt", "1m"}}, Reason: ReasonShardReconnect})
	time.Sleep(30 * time.Millisecond)
	if h.fetch.callCount() != 1 {
		t.Fatalf("fetch calls = %d, want 1 (dup filtered)", h.fetch.callCount())
	}
	close(h.fetch.blockCh)
	eventually(t, 2*time.Second, func() bool { return !h.gate.isHeld("BTCUSDT", "1m") })
}

func TestGateReleasedOnStoreError(t *testing.T) {
	h := newHarness(t, Config{})
	h.store.lastErr = errors.New("ch read boom")
	h.fetch.rows["BTCUSDT/1h"] = []chwriter.Row{row("BTCUSDT", fixedNow.Add(-2*time.Hour).UnixMilli(), 1)}

	h.b.Submit(Request{Keys: []Key{{"BTCUSDT", "1h"}}, Since: fixedNow.Add(-3 * time.Hour), Until: fixedNow, Reason: ReasonShardReconnect})

	eventually(t, 2*time.Second, func() bool { return !h.gate.isHeld("BTCUSDT", "1h") })
	if _, ok := h.rb.get("BTCUSDT", "1h"); ok {
		t.Fatal("no rebuild expected when CH read failed")
	}
	if v := testutil.ToFloat64(h.b.metrics.errors.WithLabelValues("ch_read")); v != 1 {
		t.Fatalf("errors{ch_read} = %v, want 1", v)
	}
}

func TestGateReleasedOnArchiveFlushError(t *testing.T) {
	h := newHarness(t, Config{})
	h.arc["1h"].flushErr = errors.New("archive flush boom")
	h.fetch.rows["BTCUSDT/1h"] = []chwriter.Row{row("BTCUSDT", fixedNow.Add(-2*time.Hour).UnixMilli(), 1)}

	h.b.Submit(Request{Keys: []Key{{"BTCUSDT", "1h"}}, Since: fixedNow.Add(-3 * time.Hour), Until: fixedNow, Reason: ReasonShardReconnect})

	eventually(t, 2*time.Second, func() bool { return !h.gate.isHeld("BTCUSDT", "1h") })
	if _, ok := h.rb.get("BTCUSDT", "1h"); ok {
		t.Fatal("no rebuild expected when archive flush failed")
	}
	if h.store.lastCalls != 0 {
		t.Fatalf("LastBars calls = %d, want 0", h.store.lastCalls)
	}
	if v := testutil.ToFloat64(h.b.metrics.errors.WithLabelValues("archive")); v != 1 {
		t.Fatalf("errors{archive} = %v, want 1", v)
	}
	if h.arc["1h"].flushCalls != 1 {
		t.Fatalf("flushCalls = %d, want 1", h.arc["1h"].flushCalls)
	}
}

func TestReadyWhenNoColdStart(t *testing.T) {
	h := newHarness(t, Config{})
	if !h.b.Ready() {
		t.Fatal("Ready() should be true before any cold-start submission")
	}
}

func TestColdStartEmptyKeys(t *testing.T) {
	h := newHarness(t, Config{})
	h.b.SubmitColdStart(nil)
	if !h.b.Ready() {
		t.Fatal("Ready() should flip immediately for an empty universe")
	}
}

func TestWaitColdStartReturnsAfterCompletion(t *testing.T) {
	h := newHarness(t, Config{})
	h.b.SubmitColdStart([]Key{{"BTCUSDT", "1m"}, {"BTCUSDT", "1h"}})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := h.b.WaitColdStart(ctx); err != nil {
		t.Fatalf("WaitColdStart: %v", err)
	}
	if !h.b.Ready() {
		t.Fatal("Ready() should be true once WaitColdStart returns nil")
	}
}

func TestWaitColdStartHonoursContext(t *testing.T) {
	h := newHarness(t, Config{})
	h.fetch.blockCh = make(chan struct{}) // Fetch blocks -> cold start never finishes
	h.b.SubmitColdStart([]Key{{"BTCUSDT", "1m"}})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := h.b.WaitColdStart(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitColdStart err = %v, want context.DeadlineExceeded", err)
	}
	if h.b.Ready() {
		t.Fatal("Ready() must stay false while the cold-start fetch is blocked")
	}
	close(h.fetch.blockCh)
}

func TestNegativeGateTimeoutIsUnbounded(t *testing.T) {
	h := newHarness(t, Config{GateTimeout: -1})
	if h.b.cfg.gateTimeout != -1 {
		t.Fatalf("gateTimeout = %v, want -1 (sentinel preserved, no default applied)", h.b.cfg.gateTimeout)
	}

	// With the bound disabled, a slow request is not force-released: the gate
	// stays held until the fetch actually returns.
	h.fetch.blockCh = make(chan struct{})
	h.b.SubmitColdStart([]Key{{"BTCUSDT", "1m"}})
	eventually(t, time.Second, func() bool { return h.fetch.callCount() == 1 })
	time.Sleep(100 * time.Millisecond)
	if !h.gate.isHeld("BTCUSDT", "1m") {
		t.Fatal("gate released early despite gateTimeout < 0")
	}
	close(h.fetch.blockCh)
	eventually(t, 2*time.Second, func() bool { return !h.gate.isHeld("BTCUSDT", "1m") })
}

func TestParseStream(t *testing.T) {
	cases := map[string]struct {
		key Key
		ok  bool
	}{
		"btcusdt@kline_1m":  {Key{"BTCUSDT", "1m"}, true},
		"ETHUSDT@kline_1h":  {Key{"ETHUSDT", "1h"}, true},
		"btcusdt@markPrice": {Key{}, false},
		"btcusdt@kline_":    {Key{}, false},
		"@kline_1m":         {Key{}, false},
		"nonsense":          {Key{}, false},
	}
	for in, exp := range cases {
		k, ok := parseStream(in)
		if ok != exp.ok || k != exp.key {
			t.Errorf("parseStream(%q) = (%+v,%v), want (%+v,%v)", in, k, ok, exp.key, exp.ok)
		}
	}
}

func TestGracefulClose(t *testing.T) {
	h := newHarness(t, Config{})
	h.b.Submit(Request{Keys: []Key{{"BTCUSDT", "1m"}}, Reason: ReasonUniverseAdd})
	eventually(t, 2*time.Second, func() bool { return !h.gate.isHeld("BTCUSDT", "1m") })

	done := make(chan struct{})
	go func() { _ = h.b.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung")
	}
	// submit after close is a no-op
	h.b.Submit(Request{Keys: []Key{{"X", "1m"}}, Reason: ReasonUniverseAdd})
}

type fakeStreamFetcher struct {
	fakeFetcher
	batches [][]chwriter.Row
}

func (s *fakeStreamFetcher) FetchStream(ctx context.Context, sym, iv string, since, until time.Time, onBatch func([]chwriter.Row) error) error {
	s.mu.Lock()
	s.calls = append(s.calls, fetchCall{sym, iv, since, until})
	s.mu.Unlock()
	for _, b := range s.batches {
		if err := onBatch(b); err != nil {
			return err
		}
	}
	return nil
}

func TestStreamFetcherIntegration(t *testing.T) {
	sf := &fakeStreamFetcher{
		batches: [][]chwriter.Row{
			{row("BTCUSDT", fixedNow.Add(-2*time.Hour).UnixMilli(), 1)},
			{row("BTCUSDT", fixedNow.Add(-1*time.Hour).UnixMilli(), 2)},
		},
	}
	arc := map[string]ArchiveWriter{"1h": &fakeArchive{}}
	b, err := New(Config{Clock: func() time.Time { return fixedNow }}, sf, arc, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	b.Start(context.Background())
	defer b.Close()

	b.Submit(Request{Keys: []Key{{"BTCUSDT", "1h"}}, Since: fixedNow.Add(-3 * time.Hour), Reason: ReasonShardReconnect})
	eventually(t, 2*time.Second, func() bool { return sf.callCount() == 1 })
	fa := arc["1h"].(*fakeArchive)
	eventually(t, 2*time.Second, func() bool { return fa.count() == 2 })
}
