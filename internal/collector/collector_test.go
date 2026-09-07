package collector

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/HarvestStars/chomosyncer-go/internal/dispatcher"
)

// --- fakes ---

type fakeClient struct {
	id      string
	onEvent func(dispatcher.KlineEvent)

	mu         sync.Mutex
	connectN   int
	streams    map[string]struct{}
	subCalls   [][]string
	unsubCalls [][]string
	connectErr error
	closed     bool

	lastMsg atomic.Int64
	errCh   chan error
	errOnce sync.Once
}

func (f *fakeClient) Connect(_ context.Context, streams []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connectN++
	if f.connectErr != nil {
		return f.connectErr
	}
	f.streams = map[string]struct{}{}
	for _, s := range streams {
		f.streams[s] = struct{}{}
	}
	f.lastMsg.Store(time.Now().UnixNano())
	return nil
}

func (f *fakeClient) Subscribe(_ context.Context, streams []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subCalls = append(f.subCalls, streams)
	for _, s := range streams {
		f.streams[s] = struct{}{}
	}
	return nil
}

func (f *fakeClient) Unsubscribe(_ context.Context, streams []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unsubCalls = append(f.unsubCalls, streams)
	for _, s := range streams {
		delete(f.streams, s)
	}
	return nil
}

func (f *fakeClient) LastMessageAt() time.Time { return time.Unix(0, f.lastMsg.Load()) }
func (f *fakeClient) Errors() <-chan error     { return f.errCh }

func (f *fakeClient) Close() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	f.errOnce.Do(func() { close(f.errCh) })
	return nil
}

func (f *fakeClient) push(ev dispatcher.KlineEvent) {
	f.lastMsg.Store(time.Now().UnixNano())
	f.onEvent(ev)
}

func (f *fakeClient) makeStale() { f.lastMsg.Store(time.Now().Add(-time.Hour).UnixNano()) }

func (f *fakeClient) streamSet() map[string]struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneSet(f.streams)
}

type fakeFactory struct {
	mu          sync.Mutex
	clients     map[string][]*fakeClient
	failFirst   map[string]int // per-shard: fail this many Connect calls
	connectSeen map[string]int
}

func newFakeFactory() *fakeFactory {
	return &fakeFactory{
		clients:     map[string][]*fakeClient{},
		failFirst:   map[string]int{},
		connectSeen: map[string]int{},
	}
}

func (f *fakeFactory) make(id string, onEvent func(dispatcher.KlineEvent)) streamClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	fc := &fakeClient{id: id, onEvent: onEvent, streams: map[string]struct{}{}, errCh: make(chan error, 8)}
	fc.lastMsg.Store(time.Now().UnixNano())
	f.connectSeen[id]++
	if f.failFirst[id] > 0 {
		fc.connectErr = errors.New("fake connect failure")
		f.failFirst[id]--
	}
	f.clients[id] = append(f.clients[id], fc)
	return fc
}

func (f *fakeFactory) latest(id string) *fakeClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	cs := f.clients[id]
	if len(cs) == 0 {
		return nil
	}
	return cs[len(cs)-1]
}

func (f *fakeFactory) countCreated(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.clients[id])
}

type fakeSink struct {
	mu     sync.Mutex
	events []dispatcher.KlineEvent
	err    error
}

func (s *fakeSink) HandleKlineEvent(ev dispatcher.KlineEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
	return s.err
}
func (s *fakeSink) count() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.events) }

// --- helpers ---

func newTestCollector(t *testing.T, cfg Config) (*Collector, *fakeFactory, *fakeSink) {
	t.Helper()
	f := newFakeFactory()
	sink := &fakeSink{}
	if cfg.Registerer == nil {
		cfg.Registerer = prometheus.NewRegistry()
	}
	// snappy timings for tests
	if cfg.ReconnectBase == 0 {
		cfg.ReconnectBase = 5 * time.Millisecond
	}
	if cfg.ReconnectMax == 0 {
		cfg.ReconnectMax = 20 * time.Millisecond
	}
	if cfg.WatchdogInterval == 0 {
		cfg.WatchdogInterval = 10 * time.Millisecond
	}
	if cfg.ConnectStagger == 0 {
		cfg.ConnectStagger = time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c, err := New(ctx, cfg, sink, withClientFactory(f.make))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, f, sink
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func closedEvent(symbol, interval string, openTime int64) dispatcher.KlineEvent {
	return dispatcher.KlineEvent{
		Symbol: symbol, Interval: interval, OpenTime: openTime, CloseTime: openTime + 59_999,
		Open: "100", High: "110", Low: "90", Close: "105",
		Volume: "10", QuoteVolume: "1050", TakerBuyVolume: "6", TakerBuyQuoteVolume: "630",
		TradeCount: 7, IsFinal: true,
	}
}

// --- tests ---

func TestSetSymbolsCreatesShards(t *testing.T) {
	c, f, _ := newTestCollector(t, Config{Intervals: []string{"1m", "1h"}, ShardsPerInterval: 1})

	c.SetSymbols([]string{"BTCUSDT", "ETHUSDT"})

	eventually(t, time.Second, func() bool {
		return f.latest("kline_1m_0") != nil && f.latest("kline_1h_0") != nil &&
			f.latest("kline_1m_0").connectN >= 1 && f.latest("kline_1h_0").connectN >= 1
	})

	got := f.latest("kline_1m_0").streamSet()
	if _, ok := got["btcusdt@kline_1m"]; !ok {
		t.Fatalf("kline_1m_0 streams = %v", got)
	}
	if _, ok := got["ethusdt@kline_1m"]; !ok {
		t.Fatalf("kline_1m_0 streams = %v", got)
	}
	if len(c.ShardIDs()) != 2 {
		t.Fatalf("ShardIDs = %v", c.ShardIDs())
	}
}

func TestForwardsEventsToSink(t *testing.T) {
	c, f, sink := newTestCollector(t, Config{Intervals: []string{"1m"}, ShardsPerInterval: 1})
	c.SetSymbols([]string{"BTCUSDT"})
	eventually(t, time.Second, func() bool { return f.latest("kline_1m_0") != nil && f.latest("kline_1m_0").connectN >= 1 })

	fc := f.latest("kline_1m_0")
	fc.push(closedEvent("BTCUSDT", "1m", 1000))
	fc.push(dispatcher.KlineEvent{Symbol: "BTCUSDT", Interval: "1m", OpenTime: 1060, Open: "1", High: "1", Low: "1", Close: "1", Volume: "1", QuoteVolume: "1", TakerBuyVolume: "1", TakerBuyQuoteVolume: "1"})

	eventually(t, time.Second, func() bool { return sink.count() == 2 })
	if v := testutil.ToFloat64(c.metrics.wsMessages.WithLabelValues("kline_1m_0")); v != 2 {
		t.Fatalf("ws_messages_total = %v, want 2", v)
	}
	if v := testutil.ToFloat64(c.metrics.klineIngested.WithLabelValues("BTCUSDT", "1m")); v != 1 {
		t.Fatalf("kline_ingested_total{BTCUSDT,1m} = %v, want 1 (only the closed bar)", v)
	}
}

func TestReconnectOnError(t *testing.T) {
	c, f, _ := newTestCollector(t, Config{Intervals: []string{"1m"}, ShardsPerInterval: 1})
	c.SetSymbols([]string{"BTCUSDT"})
	eventually(t, time.Second, func() bool { return f.countCreated("kline_1m_0") == 1 && f.latest("kline_1m_0").connectN == 1 })

	f.latest("kline_1m_0").errCh <- errors.New("stream blew up")

	eventually(t, 2*time.Second, func() bool {
		return f.countCreated("kline_1m_0") >= 2 && f.latest("kline_1m_0").connectN >= 1
	})
	if v := testutil.ToFloat64(c.metrics.wsReconnects.WithLabelValues("kline_1m_0")); v < 1 {
		t.Fatalf("ws_reconnects_total = %v, want >= 1", v)
	}
	if v := testutil.ToFloat64(c.metrics.wsStatus.WithLabelValues("kline_1m_0")); v != 1 {
		t.Fatalf("ws_connection_status = %v, want 1 after reconnect", v)
	}
}

func TestReconnectOnStale(t *testing.T) {
	c, f, _ := newTestCollector(t, Config{
		Intervals: []string{"1m"}, ShardsPerInterval: 1,
		StaleTimeout: 30 * time.Millisecond, WatchdogInterval: 5 * time.Millisecond,
	})
	c.SetSymbols([]string{"BTCUSDT"})
	eventually(t, time.Second, func() bool { return f.latest("kline_1m_0") != nil })

	f.latest("kline_1m_0").makeStale()

	eventually(t, 2*time.Second, func() bool { return f.countCreated("kline_1m_0") >= 2 })
}

func TestSubscriptionDeltaOnSymbolChange(t *testing.T) {
	c, f, _ := newTestCollector(t, Config{
		Intervals: []string{"1m"}, ShardsPerInterval: 1, WatchdogInterval: 5 * time.Millisecond,
	})
	c.SetSymbols([]string{"BTCUSDT", "ETHUSDT"})
	eventually(t, time.Second, func() bool { return f.latest("kline_1m_0") != nil && f.latest("kline_1m_0").connectN >= 1 })
	fc := f.latest("kline_1m_0")

	c.SetSymbols([]string{"BTCUSDT", "SOLUSDT"})

	eventually(t, time.Second, func() bool {
		s := fc.streamSet()
		_, hasSol := s["solusdt@kline_1m"]
		_, hasEth := s["ethusdt@kline_1m"]
		return hasSol && !hasEth
	})
	if v := testutil.ToFloat64(c.metrics.subUpdates.WithLabelValues("kline_1m_0")); v < 1 {
		t.Fatalf("ws_subscription_updates_total = %v, want >= 1", v)
	}
	// same client, no reconnect
	if f.countCreated("kline_1m_0") != 1 {
		t.Fatalf("delta must not reconnect; clients created = %d", f.countCreated("kline_1m_0"))
	}
}

func TestShardRemovedWhenBucketEmpties(t *testing.T) {
	// find two symbols that land in different buckets of 4
	var a, b string
	cands := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT"}
	for i := 0; i < len(cands) && b == ""; i++ {
		for j := i + 1; j < len(cands); j++ {
			if bucketOf(cands[i], 4) != bucketOf(cands[j], 4) {
				a, b = cands[i], cands[j]
			}
		}
	}
	if b == "" {
		t.Skip("no two candidate symbols in distinct buckets")
	}

	c, _, _ := newTestCollector(t, Config{Intervals: []string{"1m"}, ShardsPerInterval: 4})
	c.SetSymbols([]string{a, b})
	eventually(t, time.Second, func() bool { return len(c.ShardIDs()) == 2 })

	c.SetSymbols([]string{a})
	eventually(t, time.Second, func() bool { return len(c.ShardIDs()) == 1 })
}

func TestConnectFailureRetries(t *testing.T) {
	c, f, _ := newTestCollector(t, Config{Intervals: []string{"1m"}, ShardsPerInterval: 1})
	f.failFirst["kline_1m_0"] = 2

	c.SetSymbols([]string{"BTCUSDT"})

	eventually(t, 2*time.Second, func() bool {
		return f.countCreated("kline_1m_0") >= 3 &&
			testutil.ToFloat64(c.metrics.wsStatus.WithLabelValues("kline_1m_0")) == 1
	})
}

func TestGracefulClose(t *testing.T) {
	c, f, _ := newTestCollector(t, Config{Intervals: []string{"1m"}, ShardsPerInterval: 1})
	c.SetSymbols([]string{"BTCUSDT"})
	eventually(t, time.Second, func() bool { return f.latest("kline_1m_0") != nil })
	fc := f.latest("kline_1m_0")

	done := make(chan struct{})
	go func() { _ = c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close hung")
	}

	fc.mu.Lock()
	closed := fc.closed
	fc.mu.Unlock()
	if !closed {
		t.Fatal("shard client not closed on Close")
	}

	before := f.countCreated("kline_1m_0")
	c.SetSymbols([]string{"BTCUSDT", "ETHUSDT"})
	time.Sleep(20 * time.Millisecond)
	if f.countCreated("kline_1m_0") != before || len(c.ShardIDs()) != 0 {
		t.Fatal("SetSymbols after Close must be a no-op")
	}
}

func TestContextCancelStopsShards(t *testing.T) {
	f := newFakeFactory()
	sink := &fakeSink{}
	ctx, cancel := context.WithCancel(context.Background())
	c, err := New(ctx, Config{
		Intervals: []string{"1m"}, ShardsPerInterval: 1, Registerer: prometheus.NewRegistry(),
		ReconnectBase: 5 * time.Millisecond, WatchdogInterval: 5 * time.Millisecond, ConnectStagger: time.Millisecond,
	}, sink, withClientFactory(f.make))
	if err != nil {
		t.Fatal(err)
	}
	c.SetSymbols([]string{"BTCUSDT"})
	eventually(t, time.Second, func() bool { return f.latest("kline_1m_0") != nil })

	cancel()

	done := make(chan struct{})
	go func() { _ = c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung after context cancellation")
	}
}

func TestNilSinkRejected(t *testing.T) {
	if _, err := New(context.Background(), Config{}, nil); err == nil {
		t.Fatal("expected error for nil sink")
	}
}

func TestOnGapReportedAfterReconnect(t *testing.T) {
	var (
		mu   sync.Mutex
		gaps []GapEvent
	)
	f := newFakeFactory()
	sink := &fakeSink{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c, err := New(ctx, Config{
		Intervals: []string{"1m"}, ShardsPerInterval: 1,
		ReconnectBase: 5 * time.Millisecond, WatchdogInterval: 5 * time.Millisecond, ConnectStagger: time.Millisecond,
		Registerer: prometheus.NewRegistry(),
		OnGap: func(g GapEvent) {
			mu.Lock()
			gaps = append(gaps, g)
			mu.Unlock()
		},
	}, sink, withClientFactory(f.make))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	c.SetSymbols([]string{"BTCUSDT"})
	eventually(t, time.Second, func() bool { return f.latest("kline_1m_0") != nil && f.latest("kline_1m_0").connectN == 1 })

	fc1 := f.latest("kline_1m_0")
	fc1.push(closedEvent("BTCUSDT", "1m", 1000)) // set LastMessageAt to ~now
	downApprox := time.Now()
	fc1.errCh <- errors.New("stream blew up")

	eventually(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(gaps) == 1
	})

	mu.Lock()
	g := gaps[0]
	mu.Unlock()
	if g.ShardID != "kline_1m_0" {
		t.Fatalf("gap ShardID = %q", g.ShardID)
	}
	if len(g.Streams) != 1 || g.Streams[0] != "btcusdt@kline_1m" {
		t.Fatalf("gap Streams = %v", g.Streams)
	}
	if g.LastMsgAt.Sub(downApprox).Abs() > time.Second {
		t.Fatalf("gap LastMsgAt = %v, want ~%v", g.LastMsgAt, downApprox)
	}
	if !g.ReconnectAt.After(g.LastMsgAt) {
		t.Fatalf("ReconnectAt %v not after LastMsgAt %v", g.ReconnectAt, g.LastMsgAt)
	}
}

func TestOnGapNotFiredOnFirstConnect(t *testing.T) {
	var n atomic.Int32
	f := newFakeFactory()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c, err := New(ctx, Config{
		Intervals: []string{"1m"}, ShardsPerInterval: 1, ConnectStagger: time.Millisecond,
		Registerer: prometheus.NewRegistry(),
		OnGap:      func(GapEvent) { n.Add(1) },
	}, &fakeSink{}, withClientFactory(f.make))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	c.SetSymbols([]string{"BTCUSDT"})
	eventually(t, time.Second, func() bool { return f.latest("kline_1m_0") != nil })
	time.Sleep(30 * time.Millisecond)
	if n.Load() != 0 {
		t.Fatalf("OnGap fired %d times on first connect, want 0", n.Load())
	}
}
