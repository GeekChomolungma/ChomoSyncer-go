package rediswin

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
)

func newTestWriter(t *testing.T, cfg Config) (*Writer, *redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr:                  mr.Addr(),
		DialTimeout:           500 * time.Millisecond,
		ReadTimeout:           500 * time.Millisecond,
		WriteTimeout:          500 * time.Millisecond,
		MaxRetries:            -1, // fail fast in tests instead of backing off
		ContextTimeoutEnabled: true,
	})
	t.Cleanup(func() { _ = client.Close() })
	if cfg.Registerer == nil {
		cfg.Registerer = prometheus.NewRegistry()
	}
	w, err := New(client, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return w, client, mr
}

func barN(i int) CompactBar {
	return CompactBar{
		StartTime: int64(1_700_000_000_000 + i*3_600_000),
		Open:      float64(i),
		High:      float64(i) + 1,
		Low:       float64(i) - 1,
		Close:     float64(i) + 0.5,
		Volume:    10,
	}
}

func TestKeyFormat(t *testing.T) {
	w, _, _ := newTestWriter(t, Config{})
	if got := w.Key("ethusdt", "1m"); got != "kline:ETHUSDT:1m" {
		t.Fatalf("Key = %q, want kline:ETHUSDT:1m", got)
	}

	w2, _, _ := newTestWriter(t, Config{KeyPrefix: "md"})
	if got := w2.Key("BTCUSDT", "1h"); got != "md:BTCUSDT:1h" {
		t.Fatalf("Key = %q, want md:BTCUSDT:1h", got)
	}
}

func TestPushBarAndTrimKeepsWindowAt200(t *testing.T) {
	w, client, _ := newTestWriter(t, Config{})
	ctx := context.Background()
	key := w.Key("btcusdt", "1h")

	const pushed = 250
	for i := 1; i <= pushed; i++ {
		if err := w.PushBarAndTrim(ctx, "btcusdt", "1h", barN(i)); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}

	n, err := client.LLen(ctx, key).Result()
	if err != nil {
		t.Fatalf("LLEN: %v", err)
	}
	if n != DefaultWindowSize {
		t.Fatalf("LLEN = %d, want %d", n, DefaultWindowSize)
	}

	// LPUSH puts newest at the head; after trimming, index 0 is bar #250 and
	// index 199 is bar #51.
	assertOpenAt := func(idx int64, wantOpen int) {
		t.Helper()
		s, err := client.LIndex(ctx, key, idx).Result()
		if err != nil {
			t.Fatalf("LINDEX %d: %v", idx, err)
		}
		var arr [compactBarLen]float64
		if err := json.Unmarshal([]byte(s), &arr); err != nil {
			t.Fatalf("decode entry %d: %v", idx, err)
		}
		if int(arr[1]) != wantOpen {
			t.Fatalf("entry %d open = %v, want %d", idx, arr[1], wantOpen)
		}
	}
	assertOpenAt(0, pushed)
	assertOpenAt(DefaultWindowSize-1, pushed-DefaultWindowSize+1)
}

func TestPushBarAndTrimCustomWindow(t *testing.T) {
	w, client, _ := newTestWriter(t, Config{WindowSize: 5})
	ctx := context.Background()
	key := w.Key("BTCUSDT", "1m")

	for i := 1; i <= 20; i++ {
		if err := w.PushBarAndTrim(ctx, "BTCUSDT", "1m", barN(i)); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	if n, _ := client.LLen(ctx, key).Result(); n != 5 {
		t.Fatalf("LLEN = %d, want 5", n)
	}
}

func TestPushBarAndTrimAtomic(t *testing.T) {
	w, client, _ := newTestWriter(t, Config{Atomic: true})
	ctx := context.Background()
	key := w.Key("BTCUSDT", "1h")

	for i := 1; i <= 300; i++ {
		if err := w.PushBarAndTrim(ctx, "BTCUSDT", "1h", barN(i)); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	if n, _ := client.LLen(ctx, key).Result(); n != DefaultWindowSize {
		t.Fatalf("LLEN = %d, want %d", n, DefaultWindowSize)
	}
}

func TestPushBarsAndTrimBatch(t *testing.T) {
	w, client, _ := newTestWriter(t, Config{})
	ctx := context.Background()
	symbols := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}

	for round := 1; round <= 4; round++ {
		batch := make([]SymbolBar, 0, len(symbols))
		for _, s := range symbols {
			batch = append(batch, SymbolBar{Symbol: s, Interval: "1h", Bar: barN(round)})
		}
		if err := w.PushBarsAndTrim(ctx, batch); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
	}

	for _, s := range symbols {
		n, err := client.LLen(ctx, "kline:"+s+":1h").Result()
		if err != nil {
			t.Fatalf("LLEN %s: %v", s, err)
		}
		if n != 4 {
			t.Fatalf("%s LLEN = %d, want 4", s, n)
		}
	}
	if got := testutil.ToFloat64(w.metrics.barsPushed); got != 12 {
		t.Fatalf("bars_pushed = %v, want 12", got)
	}
}

func TestPushBarsAndTrimEmpty(t *testing.T) {
	w, _, _ := newTestWriter(t, Config{})
	if err := w.PushBarsAndTrim(context.Background(), nil); err != nil {
		t.Fatalf("empty batch should be a no-op, got %v", err)
	}
}

func TestPublishKlineReady(t *testing.T) {
	w, client, _ := newTestWriter(t, Config{})
	ctx := context.Background()

	id, err := w.PublishKlineReady(ctx, KlineReadyEvent{
		Interval:     "1h",
		Timestamp:    1_719_835_200_000,
		SymbolsCount: 182,
	})
	if err != nil {
		t.Fatalf("PublishKlineReady: %v", err)
	}
	if id == "" {
		t.Fatal("empty stream entry id")
	}

	msgs, err := client.XRange(ctx, DefaultStreamKey, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("stream len = %d, want 1", len(msgs))
	}
	v := msgs[0].Values
	if v["interval"] != "1h" {
		t.Errorf("interval = %v", v["interval"])
	}
	if v["timestamp"] != strconv.Itoa(1_719_835_200_000) {
		t.Errorf("timestamp = %v", v["timestamp"])
	}
	if v["symbols_count"] != "182" {
		t.Errorf("symbols_count = %v", v["symbols_count"])
	}

	if got := testutil.ToFloat64(w.metrics.klineReady); got != 1 {
		t.Fatalf("kline_ready_published_total = %v, want 1", got)
	}
}

func TestPublishKlineReadyRequiresInterval(t *testing.T) {
	w, _, _ := newTestWriter(t, Config{})
	if _, err := w.PublishKlineReady(context.Background(), KlineReadyEvent{Timestamp: 1}); err == nil {
		t.Fatal("expected error when Interval is empty")
	}
}

func TestPublishKlineReadyStreamBounded(t *testing.T) {
	w, client, _ := newTestWriter(t, Config{StreamMaxLen: 10})
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		if _, err := w.PublishKlineReady(ctx, KlineReadyEvent{Interval: "1m", Timestamp: int64(i)}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	n, err := client.XLen(ctx, DefaultStreamKey).Result()
	if err != nil {
		t.Fatalf("XLEN: %v", err)
	}
	if n > 10 {
		t.Fatalf("stream len = %d, want <= 10 (approx MAXLEN)", n)
	}
}

func TestContextCancellation(t *testing.T) {
	w, _, _ := newTestWriter(t, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := w.PushBarAndTrim(ctx, "BTCUSDT", "1h", barN(1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("PushBarAndTrim err = %v, want context.Canceled", err)
	}
	if err := w.PushBarsAndTrim(ctx, []SymbolBar{{Symbol: "BTCUSDT", Interval: "1h", Bar: barN(1)}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("PushBarsAndTrim err = %v, want context.Canceled", err)
	}
	if _, err := w.PublishKlineReady(ctx, KlineReadyEvent{Interval: "1h"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("PublishKlineReady err = %v, want context.Canceled", err)
	}
}

func TestRedisFailureIsWrappedAndCounted(t *testing.T) {
	w, _, mr := newTestWriter(t, Config{})
	mr.Close() // take the server down

	if err := w.PushBarAndTrim(context.Background(), "BTCUSDT", "1h", barN(1)); err == nil {
		t.Fatal("expected error when redis is unavailable")
	}
	if _, err := w.PublishKlineReady(context.Background(), KlineReadyEvent{Interval: "1h"}); err == nil {
		t.Fatal("expected xadd error when redis is unavailable")
	}

	if got := testutil.ToFloat64(w.metrics.errors.WithLabelValues(opPushTrim)); got < 1 {
		t.Fatalf("push_trim error metric = %v, want >= 1", got)
	}
	if got := testutil.ToFloat64(w.metrics.errors.WithLabelValues(opXAdd)); got < 1 {
		t.Fatalf("xadd error metric = %v, want >= 1", got)
	}
}

func TestLatencyMetricObserved(t *testing.T) {
	w, _, _ := newTestWriter(t, Config{})
	if err := w.PushBarAndTrim(context.Background(), "BTCUSDT", "1h", barN(1)); err != nil {
		t.Fatalf("push: %v", err)
	}
	if got := testutil.CollectAndCount(w.metrics.pipelineLatency); got == 0 {
		t.Fatal("expected redis_pipeline_latency_seconds to have samples")
	}
}

func TestNilClientRejected(t *testing.T) {
	if _, err := New(nil, Config{}); err == nil {
		t.Fatal("expected error for nil redis client")
	}
}
