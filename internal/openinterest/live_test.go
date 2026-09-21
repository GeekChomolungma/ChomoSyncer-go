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

type fakeSnaps struct {
	mu    sync.Mutex
	fn    func(symbol string) (Snapshot, error)
	calls []string
}

func (f *fakeSnaps) Snapshot(_ context.Context, symbol string) (Snapshot, error) {
	f.mu.Lock()
	f.calls = append(f.calls, symbol)
	fn := f.fn
	f.mu.Unlock()
	return fn(symbol)
}

func newLiveHarness(t *testing.T, now string, syms ...string) (*Live, *fakeSnaps, *fakeSink, *liveCache, *metrics, *fakeClock) {
	t.Helper()
	fc := newFakeClock(now)
	src := &fakeSnaps{}
	sink := &fakeSink{}
	cache := newLiveCache(24)
	m := newMetrics(prometheus.NewRegistry())
	// CatchUp is 1ns so that Run's start-up catch-up round stays out of the tests that are
	// about something else; the catch-up tests set it explicitly.
	l := NewLive(LiveConfig{Workers: 3, RPS: 1e6, CatchUp: time.Nanosecond}, src, sink, func() []string { return syms }, cache, m, nil)
	l.now, l.sleep = fc.Now, fc.Sleep
	return l, src, sink, cache, m, fc
}

func TestNextRound(t *testing.T) {
	l, _, _, _, _, _ := newLiveHarness(t, "2026-09-21 12:00:00.000")
	cases := []struct{ name, now, boundary, start string }{
		{"plenty of time", "2026-09-21 12:07:30.000", "2026-09-21 12:10:00.000", "2026-09-21 12:09:30.000"},
		{"exactly on a boundary means the next one", "2026-09-21 12:05:00.000", "2026-09-21 12:10:00.000", "2026-09-21 12:09:30.000"},
		{"already inside the lead window: start now", "2026-09-21 12:09:40.000", "2026-09-21 12:10:00.000", "2026-09-21 12:09:40.000"},
		{"under 5s left: skip to the next boundary", "2026-09-21 12:09:57.000", "2026-09-21 12:15:00.000", "2026-09-21 12:14:30.000"},
		{"just past a boundary", "2026-09-21 12:10:01.000", "2026-09-21 12:15:00.000", "2026-09-21 12:14:30.000"},
		{"across midnight", "2026-09-21 23:58:00.000", "2026-09-22 00:00:00.000", "2026-09-21 23:59:30.000"},
	}
	for _, c := range cases {
		b, s := l.nextRound(ts(c.now))
		if !b.Equal(ts(c.boundary)) || !s.Equal(ts(c.start)) {
			t.Errorf("%s: nextRound(%s) = (%s, %s), want (%s, %s)", c.name, c.now, b.Format("15:04:05"), s.Format("15:04:05"), c.boundary, c.start)
		}
	}
}

func TestNextRoundNeverPicksABoundaryAlreadySnapshotted(t *testing.T) {
	l, _, _, _, _, _ := newLiveHarness(t, "2026-09-21 12:00:00.000")
	l.lastBoundary = ts("2026-09-21 12:10:00.000")
	// The round for 12:10 finished 9s before its boundary: 12:10 is done, so wait for 12:15.
	b, s := l.nextRound(ts("2026-09-21 12:09:51.000"))
	if !b.Equal(ts("2026-09-21 12:15:00.000")) || !s.Equal(ts("2026-09-21 12:14:30.000")) {
		t.Fatalf("nextRound = (%s, %s), want (12:15:00, 12:14:30)", b.Format("15:04:05"), s.Format("15:04:05"))
	}
	// The round overran its boundary; the next boundary is still the very next one.
	b, _ = l.nextRound(ts("2026-09-21 12:10:20.000"))
	if !b.Equal(ts("2026-09-21 12:15:00.000")) {
		t.Fatalf("after an overrun the boundary = %s, want 12:15:00", b.Format("15:04:05"))
	}
	// A boundary later than the last one is unaffected.
	b, _ = l.nextRound(ts("2026-09-21 12:12:00.000"))
	if !b.Equal(ts("2026-09-21 12:15:00.000")) {
		t.Fatalf("boundary = %s, want 12:15:00", b.Format("15:04:05"))
	}
}

func TestCycleStoresWhateverBinanceAnswersInTheClosingBar(t *testing.T) {
	l, src, sink, cache, m, _ := newLiveHarness(t, "2026-09-21 12:09:30.000", "AAA", "BBB", "CCC", "DDD", "EEE")
	boundary := ts("2026-09-21 12:10:00.000")
	src.fn = func(sym string) (Snapshot, error) {
		switch sym {
		case "AAA":
			return Snapshot{OpenInterest: 100, Time: boundary.Add(-20 * time.Second)}, nil
		case "BBB":
			return Snapshot{OpenInterest: 200, Time: boundary.Add(3 * time.Second)}, nil // slightly late
		case "CCC":
			return Snapshot{OpenInterest: 300, Time: boundary.Add(-2 * time.Minute)}, nil // stale: an illiquid symbol
		case "DDD":
			return Snapshot{OpenInterest: 400, Time: boundary.Add(-40 * time.Minute)}, nil // very stale: still what Binance says
		default:
			return Snapshot{}, errors.New("boom")
		}
	}
	st := l.Cycle(context.Background(), boundary)

	if st.Symbols != 5 || st.OK != 4 || st.Errors != 1 {
		t.Fatalf("stats = %+v, want 5 symbols: 4 ok, 1 error", st)
	}
	wantStart := ts("2026-09-21 12:05:00.000") // the bar that closes at 12:10, whatever the response's time says
	want := map[string]struct {
		oi   float64
		snap time.Time
	}{
		"AAA": {100, boundary.Add(-20 * time.Second)},
		"BBB": {200, boundary.Add(3 * time.Second)},
		"CCC": {300, boundary.Add(-2 * time.Minute)},
		"DDD": {400, boundary.Add(-40 * time.Minute)},
	}
	for sym, w := range want {
		rows := sink.forSymbol(sym)
		if len(rows) != 1 || !rows[0].StartTime.Equal(wantStart) || rows[0].SrcRank != RankLive {
			t.Fatalf("%s rows = %+v, want one live row for bar %v", sym, rows, wantStart)
		}
		if rows[0].SumOpenInterest != w.oi || !rows[0].SnapTime.Equal(w.snap) {
			t.Fatalf("%s row = %+v (value and snap_time must come from the response)", sym, rows[0])
		}
		if v, ok := cache.get(sym, wantStart); !ok || v != w.oi {
			t.Fatalf("live cache for %s = (%v, %v)", sym, v, ok)
		}
	}
	if len(sink.forSymbol("EEE")) != 0 {
		t.Fatal("a failed symbol must not produce a row")
	}
	for label, wantN := range map[string]float64{"ok": 4, "error": 1} {
		if got := testutil.ToFloat64(m.liveSnapshots.WithLabelValues(label)); got != wantN {
			t.Errorf("oi_live_snapshots_total{%s} = %v, want %v", label, got, wantN)
		}
	}
	if v := testutil.ToFloat64(m.liveCycleComplete); v != 0.8 {
		t.Fatalf("cycle complete ratio = %v, want 0.8", v)
	}
}

// A round must not run past the accept window after the boundary: symbols it never
// got to are counted as errors and the round returns.
func TestCycleStopsAtTheEndOfTheAcceptWindow(t *testing.T) {
	// now is 50ms before boundary+60s
	l, src, sink, _, m, _ := newLiveHarness(t, "2026-09-21 12:10:59.950", "AAA", "BBB", "CCC")
	src.fn = func(string) (Snapshot, error) { return Snapshot{}, context.DeadlineExceeded }
	release := make(chan struct{})
	defer close(release)
	l.src = blockingSnaps{release}
	start := time.Now()
	st := l.Cycle(context.Background(), ts("2026-09-21 12:10:00.000"))
	if time.Since(start) > 2*time.Second {
		t.Fatalf("Cycle ran for %v; it must stop at the end of the accept window", time.Since(start))
	}
	if st.OK != 0 || st.Errors != 3 || sink.count() != 0 {
		t.Fatalf("stats = %+v rows=%d, want all 3 counted as errors", st, sink.count())
	}
	if got := testutil.ToFloat64(m.liveSnapshots.WithLabelValues("error")); got != 3 {
		t.Fatalf("error counter = %v, want 3", got)
	}
}

type blockingSnaps struct{ release chan struct{} }

func (b blockingSnaps) Snapshot(ctx context.Context, _ string) (Snapshot, error) {
	select {
	case <-ctx.Done():
		return Snapshot{}, ctx.Err()
	case <-b.release:
		return Snapshot{}, errors.New("released")
	}
}

func TestRunWaitsForTheLeadThenSnapshots(t *testing.T) {
	l, src, sink, _, _, fc := newLiveHarness(t, "2026-09-21 12:07:30.000", "AAA")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var slept []time.Duration
	l.sleep = func(ctx context.Context, d time.Duration) error {
		slept = append(slept, d)
		return fc.Sleep(ctx, d)
	}
	src.fn = func(string) (Snapshot, error) {
		defer cancel() // stop after the first round's snapshot
		return Snapshot{OpenInterest: 7, Time: ts("2026-09-21 12:09:45.000")}, nil
	}
	l.Run(ctx)

	if len(slept) != 1 || slept[0] != 2*time.Minute { // 12:07:30 -> 12:09:30 (boundary 12:10 - 30s)
		t.Fatalf("waits = %v, want a single 2m wait until boundary-30s", slept)
	}
	rows := sink.forSymbol("AAA")
	if len(rows) != 1 || !rows[0].StartTime.Equal(ts("2026-09-21 12:05:00.000")) {
		t.Fatalf("rows = %+v", rows)
	}
}

func TestRunSnapshotsEachBoundaryOnce(t *testing.T) {
	// The fake clock does not advance during a round, so the round for 12:10 "finishes" at
	// 12:09:30 with 30s left. Without the memory of the last boundary Run starts a second
	// round for 12:10 at once; with it Run waits 5 minutes for the 12:15 round.
	l, src, sink, _, _, fc := newLiveHarness(t, "2026-09-21 12:07:30.000", "AAA")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var slept []time.Duration
	l.sleep = func(ctx context.Context, d time.Duration) error {
		slept = append(slept, d)
		if len(slept) == 2 {
			cancel()
			return ctx.Err()
		}
		return fc.Sleep(ctx, d)
	}
	rounds := 0
	src.fn = func(string) (Snapshot, error) {
		if rounds++; rounds >= 3 {
			cancel() // a regression would loop here forever; stop it so the test fails instead of hanging
		}
		return Snapshot{OpenInterest: 7, Time: fc.Now()}, nil
	}
	l.Run(ctx)

	if len(slept) != 2 || slept[0] != 2*time.Minute || slept[1] != 5*time.Minute {
		t.Fatalf("waits = %v, want 2m (to 12:09:30) then 5m (to 12:14:30)", slept)
	}
	if rows := sink.forSymbol("AAA"); len(rows) != 1 {
		t.Fatalf("got %d rows for boundary 12:10, want exactly one round", len(rows))
	}
}

func TestRunSnapshotsTheBarOfABoundaryItJustMissed(t *testing.T) {
	// Started at 12:08:00, 3 minutes after the 12:05 boundary: the bar that closed then
	// (start 12:00) never got its round. It is snapshotted at once, and the next regular
	// round is the one for 12:10.
	l, src, sink, cache, _, fc := newLiveHarness(t, "2026-09-21 12:08:00.000", "AAA", "BBB")
	l.cfg.CatchUp = 4 * time.Minute
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var slept []time.Duration
	l.sleep = func(ctx context.Context, d time.Duration) error {
		slept = append(slept, d)
		cancel()
		return ctx.Err()
	}
	src.fn = func(string) (Snapshot, error) { return Snapshot{OpenInterest: 7, Time: fc.Now()}, nil }
	l.Run(ctx)

	// The deadline is relative to now: measured from the long-gone 12:05 boundary it would be over already
	// and every request would be abandoned.
	for _, sym := range []string{"AAA", "BBB"} {
		rows := sink.forSymbol(sym)
		if len(rows) != 1 || !rows[0].StartTime.Equal(ts("2026-09-21 12:00:00.000")) || rows[0].SrcRank != RankLive {
			t.Fatalf("%s rows = %+v, want one live row for the bar 12:00", sym, rows)
		}
		if !rows[0].SnapTime.Equal(ts("2026-09-21 12:08:00.000")) {
			t.Fatalf("%s snap_time = %v, want the real snapshot time 12:08:00", sym, rows[0].SnapTime)
		}
	}
	if _, ok := cache.get("AAA", ts("2026-09-21 12:00:00.000")); !ok {
		t.Fatal("the catch-up snapshot must reach the live cache like any other")
	}
	if len(slept) != 1 || slept[0] != 90*time.Second { // 12:08:00 -> 12:09:30, the round for 12:10
		t.Fatalf("waits = %v, want a single 90s wait for the regular 12:10 round", slept)
	}
}

func TestRunDoesNotCatchUpLongAfterABoundary(t *testing.T) {
	// 12:09:10 is 4m10s after 12:05: hist has certainly published that bar by now.
	l, src, sink, _, _, fc := newLiveHarness(t, "2026-09-21 12:09:10.000", "AAA")
	l.cfg.CatchUp = 4 * time.Minute
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l.sleep = func(ctx context.Context, d time.Duration) error { cancel(); return ctx.Err() }
	src.fn = func(string) (Snapshot, error) { return Snapshot{OpenInterest: 7, Time: fc.Now()}, nil }
	l.Run(ctx)
	if sink.count() != 0 {
		t.Fatalf("%d rows written before the first wait, want none", sink.count())
	}
}

func TestLiveCacheKeepsOnlyTheNewest(t *testing.T) {
	c := newLiveCache(3)
	base := ts("2026-09-21 12:00:00.000")
	for i := 0; i < 5; i++ {
		c.put("AAA", base.Add(time.Duration(i)*BarInterval), float64(i))
	}
	if _, ok := c.get("AAA", base); ok {
		t.Fatal("the oldest entry should have been evicted")
	}
	if _, ok := c.get("AAA", base.Add(BarInterval)); ok {
		t.Fatal("the second oldest should have been evicted")
	}
	if v, ok := c.get("AAA", base.Add(4*BarInterval)); !ok || v != 4 {
		t.Fatalf("newest = (%v, %v)", v, ok)
	}
}
