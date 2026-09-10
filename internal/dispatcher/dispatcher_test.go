package dispatcher

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/HarvestStars/chomosyncer-go/internal/chwriter"
	"github.com/HarvestStars/chomosyncer-go/internal/rediswin"
)

// --- fakes ---

type fakeLive struct {
	mu   sync.Mutex
	bars []rediswin.LiveBar
	full bool
}

func (f *fakeLive) TryEnqueue(_, _ string, b rediswin.LiveBar) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.full {
		return rediswin.ErrBufferFull
	}
	f.bars = append(f.bars, b)
	return nil
}
func (f *fakeLive) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.bars) }

type windowCall struct {
	symbol, interval string
	bar              rediswin.CompactBar
}

type fakeWindow struct {
	mu    sync.Mutex
	calls []windowCall
	err   error
}

func (f *fakeWindow) PushBarAndTrim(_ context.Context, s, i string, b rediswin.CompactBar) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, windowCall{s, i, b})
	return f.err
}
func (f *fakeWindow) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }
func (f *fakeWindow) snapshot() []windowCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]windowCall(nil), f.calls...)
}

type fakeArchive struct {
	mu        sync.Mutex
	rows      []chwriter.Row
	intervals []string
	err       error
}

func (f *fakeArchive) TryPush(interval string, r chwriter.Row) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.intervals = append(f.intervals, interval)
	if f.err != nil {
		return f.err
	}
	f.rows = append(f.rows, r)
	return nil
}
func (f *fakeArchive) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.rows) }

type fakeReady struct {
	mu       sync.Mutex
	events   []rediswin.KlineReadyEvent
	err      error
	suppress bool // mimic the window gate: record nothing, return ("", nil)
	n        atomic.Int64
}

func (f *fakeReady) PublishKlineReady(_ context.Context, e rediswin.KlineReadyEvent) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return "", f.err
	}
	if f.suppress {
		return "", nil
	}
	f.events = append(f.events, e)
	return "id-" + strconv.FormatInt(f.n.Add(1), 10), nil
}
func (f *fakeReady) snapshot() []rediswin.KlineReadyEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]rediswin.KlineReadyEvent(nil), f.events...)
}
func (f *fakeReady) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.events) }
func (f *fakeReady) last() (rediswin.KlineReadyEvent, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.events) == 0 {
		return rediswin.KlineReadyEvent{}, false
	}
	return f.events[len(f.events)-1], true
}

type fakes struct {
	live    *fakeLive
	window  *fakeWindow
	archive *fakeArchive
	ready   *fakeReady
}

// --- helpers ---

func newTestDispatcher(t *testing.T, cfg Config, universe UniverseProvider) (*Dispatcher, *fakes) {
	t.Helper()
	f := &fakes{live: &fakeLive{}, window: &fakeWindow{}, archive: &fakeArchive{}, ready: &fakeReady{}}
	if cfg.Registerer == nil {
		cfg.Registerer = prometheus.NewRegistry()
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	d, err := New(ctx, cfg, Sinks{Live: f.live, Window: f.window, Archive: f.archive, Ready: f.ready}, universe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, f
}

func ev(symbol, interval string, openTime int64, final bool) KlineEvent {
	return KlineEvent{
		Symbol:              symbol,
		Interval:            interval,
		OpenTime:            openTime,
		CloseTime:           openTime + 59_999,
		Open:                "100",
		High:                "110",
		Low:                 "90",
		Close:               "105",
		Volume:              "1000",
		QuoteVolume:         "105000",
		TakerBuyVolume:      "600",
		TakerBuyQuoteVolume: "63000",
		TradeCount:          42,
		IsFinal:             final,
	}
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// --- tests ---

func TestUnclosedGoesToLiveOnly(t *testing.T) {
	d, f := newTestDispatcher(t, Config{}, nil)

	if err := d.HandleKlineEvent(ev("BTCUSDT", "1m", 1000, false)); err != nil {
		t.Fatalf("HandleKlineEvent: %v", err)
	}

	eventually(t, time.Second, func() bool { return f.live.count() == 1 })
	if f.window.count() != 0 || f.archive.count() != 0 || f.ready.count() != 0 {
		t.Fatalf("unclosed leaked downstream: window=%d archive=%d ready=%d",
			f.window.count(), f.archive.count(), f.ready.count())
	}
}

func TestClosedFansOutToAllFour(t *testing.T) {
	d, f := newTestDispatcher(t, Config{SectionTimeout: time.Hour}, NewStaticUniverse("BTCUSDT"))

	if err := d.HandleKlineEvent(ev("BTCUSDT", "1h", 1_700_000_000_000, true)); err != nil {
		t.Fatalf("HandleKlineEvent: %v", err)
	}

	eventually(t, time.Second, func() bool {
		return f.live.count() == 1 && f.window.count() == 1 && f.archive.count() == 1 && f.ready.count() == 1
	})

	got, _ := f.ready.last()
	if got.Interval != "1h" || got.Timestamp != 1_700_000_000_000 || got.SymbolsCount != 1 {
		t.Fatalf("kline_ready = %+v", got)
	}
	if v := testutil.ToFloat64(d.metrics.sectionPub.WithLabelValues("1h", "complete")); v != 1 {
		t.Fatalf("section_published{complete} = %v, want 1", v)
	}
}

func TestClosedConversion(t *testing.T) {
	d, f := newTestDispatcher(t, Config{SectionTimeout: time.Hour}, nil)

	e := ev("ethusdt", "1h", 1_720_000_000_000, true)
	e.Close = "3456.78"
	e.Volume = "12.5"
	if err := d.HandleKlineEvent(e); err != nil {
		t.Fatalf("HandleKlineEvent: %v", err)
	}
	eventually(t, time.Second, func() bool { return f.window.count() == 1 && f.archive.count() == 1 })

	wc := f.window.snapshot()[0]
	if wc.symbol != "ethusdt" || wc.bar.Close != 3456.78 || wc.bar.StartTime != 1_720_000_000_000 {
		t.Fatalf("compact bar = %+v (sym %q)", wc.bar, wc.symbol)
	}
	row := f.archive.rows[0]
	if row.Symbol != "ethusdt" || row.Close != 3456.78 || row.Volume != 12.5 {
		t.Fatalf("row = %+v", row)
	}
	if !row.StartTime.Equal(time.UnixMilli(1_720_000_000_000).UTC()) {
		t.Fatalf("row.StartTime = %v", row.StartTime)
	}
}

func TestMonotonicGuardClosedPath(t *testing.T) {
	d, f := newTestDispatcher(t, Config{SectionTimeout: time.Hour}, nil)

	for _, tc := range []struct {
		openTime int64
		accepted bool
	}{
		{1000, true},
		{1000, false}, // equal -> reject
		{999, false},  // older -> reject
		{2000, true},
	} {
		_ = d.HandleKlineEvent(ev("BTCUSDT", "1h", tc.openTime, true))
	}

	eventually(t, time.Second, func() bool { return f.window.count() == 2 })
	if f.live.count() != 4 {
		t.Fatalf("live count = %d, want 4 (guard must not touch the live path)", f.live.count())
	}
	if v := testutil.ToFloat64(d.metrics.outOfOrder.WithLabelValues("1h")); v != 2 {
		t.Fatalf("out_of_order{1h} = %v, want 2", v)
	}
}

func TestSectionCompletePublishesOnce(t *testing.T) {
	d, f := newTestDispatcher(t, Config{SectionTimeout: time.Hour}, NewStaticUniverse("A", "B", "C"))

	_ = d.HandleKlineEvent(ev("A", "1h", 1000, true))
	_ = d.HandleKlineEvent(ev("B", "1h", 1000, true))
	if f.ready.count() != 0 {
		t.Fatalf("published early: %d", f.ready.count())
	}
	_ = d.HandleKlineEvent(ev("C", "1h", 1000, true))

	eventually(t, time.Second, func() bool { return f.ready.count() == 1 })
	got, _ := f.ready.last()
	if got.SymbolsCount != 3 {
		t.Fatalf("symbols_count = %d, want 3", got.SymbolsCount)
	}

	// A straggler for the same section must not trigger a second publish.
	time.Sleep(50 * time.Millisecond)
	if f.ready.count() != 1 {
		t.Fatalf("republished: %d", f.ready.count())
	}
}

func TestDerivedSectionPublished(t *testing.T) {
	d, f := newTestDispatcher(t,
		Config{SectionTimeout: time.Hour, BaseInterval: "1m", ServeIntervals: []string{"5m", "1h"}},
		NewStaticUniverse("A", "B"))

	// openTime 240000 = the 5th 1m bar (indexes 0..4): it closes the 5m bucket
	// that started at 0, since (240000 + 60000) % 300000 == 0.
	// openTime 240000 is the 5th 1m bar (indexes 0..4); it closes the 5m bucket
	// starting at 0, since (240000 + 60000) % 300000 == 0.
	_ = d.HandleKlineEvent(ev("A", "1m", 240000, true))
	_ = d.HandleKlineEvent(ev("B", "1m", 240000, true))

	eventually(t, time.Second, func() bool { return f.ready.count() == 2 })

	var got1m, got5m, got1h bool
	for _, e := range f.ready.snapshot() {
		switch e.Interval {
		case "1m":
			got1m = e.Timestamp == 240000 && e.SymbolsCount == 2
		case "5m":
			got5m = e.Timestamp == 0 && e.SymbolsCount == 2
		case "1h":
			got1h = true
		}
	}
	if !got1m || !got5m || got1h {
		t.Fatalf("derived cascade wrong: 1m=%v 5m=%v 1h=%v events=%+v", got1m, got5m, got1h, f.ready.snapshot())
	}
	if v := testutil.ToFloat64(d.metrics.sectionPub.WithLabelValues("5m", "derived")); v != 1 {
		t.Fatalf("section_published{5m,derived} = %v, want 1", v)
	}

	// openTime 300000 is the 6th bar: (300000 + 60000) % 300000 == 60000, so it
	// closes no coarser bucket and must cascade to nothing.
	_ = d.HandleKlineEvent(ev("A", "1m", 300000, true))
	_ = d.HandleKlineEvent(ev("B", "1m", 300000, true))
	eventually(t, time.Second, func() bool { return f.ready.count() == 3 })
	time.Sleep(30 * time.Millisecond)
	if f.ready.count() != 3 {
		t.Fatalf("non-closing section produced a derived event: %+v", f.ready.snapshot())
	}
}

func TestDerivedSectionSuppressedWhenBaseGated(t *testing.T) {
	d, f := newTestDispatcher(t,
		Config{SectionTimeout: time.Hour, BaseInterval: "1m", ServeIntervals: []string{"5m"}},
		NewStaticUniverse("A", "B"))
	f.ready.suppress = true // window gate holds the 1m interval

	_ = d.HandleKlineEvent(ev("A", "1m", 240000, true))
	_ = d.HandleKlineEvent(ev("B", "1m", 240000, true))

	time.Sleep(80 * time.Millisecond)
	if f.ready.count() != 0 {
		t.Fatalf("derived event leaked while base was gated: %+v", f.ready.snapshot())
	}
}

func TestSectionTimeoutPublishes(t *testing.T) {
	d, f := newTestDispatcher(t,
		Config{SectionTimeout: 40 * time.Millisecond, SectionRetention: 300 * time.Millisecond},
		NewStaticUniverse("A", "B", "C"))

	_ = d.HandleKlineEvent(ev("A", "1h", 1000, true))

	eventually(t, time.Second, func() bool { return f.ready.count() == 1 })
	got, _ := f.ready.last()
	if got.SymbolsCount != 1 {
		t.Fatalf("symbols_count = %d, want 1", got.SymbolsCount)
	}
	if v := testutil.ToFloat64(d.metrics.sectionPub.WithLabelValues("1h", "timeout")); v != 1 {
		t.Fatalf("section_published{timeout} = %v, want 1", v)
	}
}

func TestSectionTimeoutThenStragglerNoRepublish(t *testing.T) {
	d, f := newTestDispatcher(t,
		Config{SectionTimeout: 40 * time.Millisecond, SectionRetention: 500 * time.Millisecond},
		NewStaticUniverse("A", "B", "C"))

	_ = d.HandleKlineEvent(ev("A", "1h", 1000, true))
	eventually(t, time.Second, func() bool { return f.ready.count() == 1 })

	// B's bar for the same section arrives late (passes the guard, first time
	// B is seen) — the published section must swallow it.
	_ = d.HandleKlineEvent(ev("B", "1h", 1000, true))
	time.Sleep(120 * time.Millisecond)
	if f.ready.count() != 1 {
		t.Fatalf("republished after straggler: %d", f.ready.count())
	}
}

func TestUnknownUniverseTimeoutOnly(t *testing.T) {
	d, f := newTestDispatcher(t,
		Config{SectionTimeout: 40 * time.Millisecond, SectionRetention: 300 * time.Millisecond},
		nil)

	_ = d.HandleKlineEvent(ev("A", "1h", 1000, true))
	_ = d.HandleKlineEvent(ev("B", "1h", 1000, true))

	// nil universe -> no "complete" path; nothing yet.
	time.Sleep(15 * time.Millisecond)
	if f.ready.count() != 0 {
		t.Fatalf("published without a known universe size: %d", f.ready.count())
	}

	eventually(t, time.Second, func() bool { return f.ready.count() == 1 })
	got, _ := f.ready.last()
	if got.SymbolsCount != 2 {
		t.Fatalf("symbols_count = %d, want 2", got.SymbolsCount)
	}
}

func TestNonUniverseSymbolArchivedNotCounted(t *testing.T) {
	d, f := newTestDispatcher(t, Config{SectionTimeout: time.Hour}, NewStaticUniverse("A", "B"))

	_ = d.HandleKlineEvent(ev("A", "1h", 1000, true))
	_ = d.HandleKlineEvent(ev("Z", "1h", 1000, true)) // not in universe
	_ = d.HandleKlineEvent(ev("B", "1h", 1000, true))

	eventually(t, time.Second, func() bool { return f.ready.count() == 1 })
	if f.window.count() != 3 || f.archive.count() != 3 {
		t.Fatalf("window=%d archive=%d, want 3/3 (all closed bars archived)", f.window.count(), f.archive.count())
	}
	got, _ := f.ready.last()
	if got.SymbolsCount != 2 {
		t.Fatalf("symbols_count = %d, want 2 (Z excluded)", got.SymbolsCount)
	}
}

func TestLiveBufferFullDoesNotBlockClosed(t *testing.T) {
	d, f := newTestDispatcher(t, Config{SectionTimeout: time.Hour}, NewStaticUniverse("A"))
	f.live.full = true

	if err := d.HandleKlineEvent(ev("A", "1h", 1000, true)); err != nil {
		t.Fatalf("HandleKlineEvent: %v", err)
	}
	eventually(t, time.Second, func() bool {
		return f.window.count() == 1 && f.archive.count() == 1 && f.ready.count() == 1
	})
	if v := testutil.ToFloat64(d.metrics.dropped.WithLabelValues("live")); v < 1 {
		t.Fatalf("dropped{live} = %v, want >= 1", v)
	}
}

func TestArchiveDropCounted(t *testing.T) {
	d, f := newTestDispatcher(t, Config{SectionTimeout: time.Hour}, NewStaticUniverse("A"))
	f.archive.err = chwriter.ErrBufferFull

	_ = d.HandleKlineEvent(ev("A", "1h", 1000, true))
	eventually(t, time.Second, func() bool { return f.window.count() == 1 && f.ready.count() == 1 })
	if v := testutil.ToFloat64(d.metrics.dropped.WithLabelValues("archive")); v != 1 {
		t.Fatalf("dropped{archive} = %v, want 1", v)
	}
}

func TestWindowErrorCountedSectionStillFires(t *testing.T) {
	d, f := newTestDispatcher(t, Config{SectionTimeout: time.Hour}, NewStaticUniverse("A"))
	f.window.err = errors.New("boom")

	_ = d.HandleKlineEvent(ev("A", "1h", 1000, true))
	eventually(t, time.Second, func() bool { return f.ready.count() == 1 })
	if v := testutil.ToFloat64(d.metrics.sinkErrors.WithLabelValues("window")); v < 1 {
		t.Fatalf("sink_errors{window} = %v, want >= 1", v)
	}
}

func TestParseErrorReturned(t *testing.T) {
	d, f := newTestDispatcher(t, Config{}, nil)

	bad := ev("BTCUSDT", "1m", 1000, false)
	bad.Open = "not-a-number"
	if err := d.HandleKlineEvent(bad); err == nil {
		t.Fatal("expected parse error")
	}
	time.Sleep(20 * time.Millisecond)
	if f.live.count() != 0 || f.window.count() != 0 {
		t.Fatal("malformed event leaked downstream")
	}
	if v := testutil.ToFloat64(d.metrics.parseErrors); v < 1 {
		t.Fatalf("parse_errors = %v, want >= 1", v)
	}
}

func TestGracefulCloseDrainsClosedQueue(t *testing.T) {
	d, f := newTestDispatcher(t, Config{}, nil) // nil universe, default 5s timeout -> no publishes during test

	const n = 40
	for i := 0; i < n; i++ {
		if err := d.HandleKlineEvent(ev("S"+strconv.Itoa(i), "1h", int64(1000+i), true)); err != nil {
			t.Fatalf("HandleKlineEvent %d: %v", i, err)
		}
	}

	done := make(chan struct{})
	go func() { _ = d.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close hung")
	}

	if f.window.count() != n || f.archive.count() != n {
		t.Fatalf("after Close window=%d archive=%d, want %d", f.window.count(), f.archive.count(), n)
	}
	if err := d.HandleKlineEvent(ev("X", "1h", 9999, true)); !errors.Is(err, ErrClosed) {
		t.Fatalf("HandleKlineEvent after Close = %v, want ErrClosed", err)
	}
}

func TestContextCancelStopsWorkers(t *testing.T) {
	f := &fakes{live: &fakeLive{}, window: &fakeWindow{}, archive: &fakeArchive{}, ready: &fakeReady{}}
	ctx, cancel := context.WithCancel(context.Background())
	d, err := New(ctx, Config{Registerer: prometheus.NewRegistry()},
		Sinks{Live: f.live, Window: f.window, Archive: f.archive, Ready: f.ready}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_ = d.HandleKlineEvent(ev("A", "1h", 1000, true))
	cancel()

	eventually(t, 2*time.Second, func() bool {
		return errors.Is(d.HandleKlineEvent(ev("A", "1h", 2000, true)), ErrClosed)
	})

	done := make(chan struct{})
	go func() { _ = d.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung after context cancellation")
	}
}

func TestNilSinkRejected(t *testing.T) {
	ctx := context.Background()
	if _, err := New(ctx, Config{}, Sinks{}, nil); err == nil {
		t.Fatal("expected error for missing sinks")
	}
	if _, err := New(ctx, Config{}, Sinks{Live: &fakeLive{}, Window: &fakeWindow{}, Archive: &fakeArchive{}}, nil); err == nil {
		t.Fatal("expected error for missing Ready sink")
	}
}

func TestEventsMetricPartitionedByKind(t *testing.T) {
	d, _ := newTestDispatcher(t, Config{SectionTimeout: time.Hour}, nil)

	_ = d.HandleKlineEvent(ev("A", "1h", 1000, false))
	_ = d.HandleKlineEvent(ev("A", "1h", 1001, false))
	_ = d.HandleKlineEvent(ev("A", "1h", 1002, true))

	if v := testutil.ToFloat64(d.metrics.events.WithLabelValues("1h", "live")); v != 2 {
		t.Fatalf("events{1h,live} = %v, want 2", v)
	}
	if v := testutil.ToFloat64(d.metrics.events.WithLabelValues("1h", "closed")); v != 1 {
		t.Fatalf("events{1h,closed} = %v, want 1", v)
	}
}
