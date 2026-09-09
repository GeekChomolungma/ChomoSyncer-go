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

func newTestLiveWriter(t *testing.T, cfg LiveBarConfig) (*LiveBarWriter, *redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr:                  mr.Addr(),
		DialTimeout:           500 * time.Millisecond,
		ReadTimeout:           500 * time.Millisecond,
		WriteTimeout:          500 * time.Millisecond,
		MaxRetries:            -1,
		ContextTimeoutEnabled: true,
	})
	t.Cleanup(func() { _ = client.Close() })

	if cfg.Registerer == nil {
		cfg.Registerer = prometheus.NewRegistry()
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	w, err := NewLiveBarWriter(ctx, client, cfg)
	if err != nil {
		t.Fatalf("NewLiveBarWriter: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, client, mr
}

func liveBar(i int, final bool) LiveBar {
	return LiveBar{
		StartTime:           int64(1_700_000_000_000 + i*60_000),
		Open:                float64(i),
		High:                float64(i) + 2,
		Low:                 float64(i) - 2,
		Close:               float64(i) + 0.5,
		Volume:              100 + float64(i),
		QuoteVolume:         1000 + float64(i),
		TakerBuyVolume:      50 + float64(i),
		TakerBuyQuoteVolume: 500 + float64(i),
		TradesCount:         int64(i * 3),
		IsFinal:             final,
	}
}

// --- bar-level helpers ---

func TestNewLiveBar(t *testing.T) {
	b, err := NewLiveBar(
		1_719_835_200_000,
		"60250.5", "60800.0", "60100.2", "60720.0",
		"12450.85", "75602300.5",
		"6120.40", "37150000.2",
		918, true,
	)
	if err != nil {
		t.Fatalf("NewLiveBar: %v", err)
	}
	if b.StartTime != 1_719_835_200_000 || b.Close != 60720.0 || b.TradesCount != 918 || !b.IsFinal {
		t.Fatalf("unexpected bar: %+v", b)
	}
}

func TestNewLiveBarBadNumber(t *testing.T) {
	if _, err := NewLiveBar(1, "1", "1", "x", "1", "1", "1", "1", "1", 0, false); err == nil {
		t.Fatal("expected parse error on malformed low price")
	}
}

func TestLiveBarHashFieldsOrder(t *testing.T) {
	f := liveBar(1, false).hashFields()
	if len(f) != 22 {
		t.Fatalf("want 22 HSET args (11 pairs), got %d", len(f))
	}
	want := []string{"t", "o", "h", "l", "c", "v", "qv", "tbv", "tbqv", "n", "x"}
	for i, name := range want {
		if f[i*2] != name {
			t.Fatalf("field %d = %v, want %q", i, f[i*2], name)
		}
	}
	if f[21] != 0 {
		t.Fatalf("x flag = %v, want 0 for unclosed bar", f[21])
	}
	if liveBar(1, true).hashFields()[21] != 1 {
		t.Fatal("x flag should be 1 for closed bar")
	}
}

func TestLiveBarMarshalCompact(t *testing.T) {
	p, err := liveBar(4, true).marshalCompact()
	if err != nil {
		t.Fatalf("marshalCompact: %v", err)
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(p, &arr); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(arr) != liveBarCompactLen {
		t.Fatalf("want %d elements, got %d: %s", liveBarCompactLen, len(arr), p)
	}
	var nums [liveBarCompactLen]float64
	if err := json.Unmarshal(p, &nums); err != nil {
		t.Fatalf("decode nums: %v", err)
	}
	if int64(nums[0]) != liveBar(4, true).StartTime {
		t.Errorf("elem 0 (t) = %v", nums[0])
	}
	if nums[9] != 12 { // TradesCount = i*3 = 12
		t.Errorf("elem 9 (n) = %v, want 12", nums[9])
	}
	if nums[10] != 1 {
		t.Errorf("elem 10 (x) = %v, want 1", nums[10])
	}
}

func TestParseIntervalDuration(t *testing.T) {
	cases := map[string]struct {
		want time.Duration
		ok   bool
	}{
		"1m":  {time.Minute, true},
		"3m":  {3 * time.Minute, true},
		"15m": {15 * time.Minute, true},
		"1h":  {time.Hour, true},
		"4h":  {4 * time.Hour, true},
		"1d":  {24 * time.Hour, true},
		"1w":  {7 * 24 * time.Hour, true},
		"1M":  {30 * 24 * time.Hour, true},
		"":    {0, false},
		"h":   {0, false},
		"0m":  {0, false},
		"zz":  {0, false},
		"5x":  {0, false},
	}
	for in, exp := range cases {
		got, ok := parseIntervalDuration(in)
		if ok != exp.ok || got != exp.want {
			t.Errorf("parseIntervalDuration(%q) = (%v, %v), want (%v, %v)", in, got, ok, exp.want, exp.ok)
		}
	}
}

// --- writer tests ---

func TestLiveBarWriterWritesHash(t *testing.T) {
	w, client, _ := newTestLiveWriter(t, LiveBarConfig{})
	ctx := context.Background()
	key := w.Key("btcusdt", "1h")

	if err := w.TryEnqueue("btcusdt", "1h", liveBar(7, false)); err != nil {
		t.Fatalf("TryEnqueue: %v", err)
	}

	eventually(t, 6*time.Second, func() bool {
		n, _ := client.Exists(ctx, key).Result()
		return n == 1 && testutil.ToFloat64(w.metrics.updates) == 1
	})

	got, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatalf("HGETALL: %v", err)
	}
	b := liveBar(7, false)
	if got["t"] != strconv.FormatInt(b.StartTime, 10) {
		t.Errorf("t = %q", got["t"])
	}
	if mustF(t, got["c"]) != b.Close || mustF(t, got["qv"]) != b.QuoteVolume {
		t.Errorf("numeric mismatch: %#v", got)
	}
	if got["n"] != "21" { // 7*3
		t.Errorf("n = %q, want 21", got["n"])
	}
	if got["x"] != "0" {
		t.Errorf("x = %q, want 0", got["x"])
	}

	ttl, _ := client.PTTL(ctx, key).Result()
	if ttl <= 0 || ttl > 2*time.Hour {
		t.Errorf("PTTL = %v, want (0, 2h] for interval 1h ×2", ttl)
	}
	if v := testutil.ToFloat64(w.metrics.updates); v != 1 {
		t.Errorf("updates_total = %v, want 1", v)
	}
}

func TestLiveBarWriterOverwrites(t *testing.T) {
	w, client, _ := newTestLiveWriter(t, LiveBarConfig{Workers: 1})
	ctx := context.Background()
	key := w.Key("BTCUSDT", "1m")

	for i := 1; i <= 30; i++ {
		if err := w.TryEnqueue("BTCUSDT", "1m", liveBar(i, false)); err != nil {
			t.Fatalf("TryEnqueue %d: %v", i, err)
		}
	}

	eventually(t, 5*time.Second, func() bool {
		v, err := client.HGet(ctx, key, "c").Result()
		return err == nil && mustF(t, v) == liveBar(30, false).Close
	})
	if n, _ := client.HLen(ctx, key).Result(); n != 11 {
		t.Fatalf("HLEN = %d, want 11 (one snapshot only)", n)
	}
}

func TestLiveBarTTLFromInterval(t *testing.T) {
	w, client, _ := newTestLiveWriter(t, LiveBarConfig{})
	ctx := context.Background()

	type tc struct {
		sym, interval string
		maxTTL        time.Duration
	}
	for _, c := range []tc{
		{"AAAUSDT", "1m", 2 * time.Minute},
		{"BBBUSDT", "5m", 10 * time.Minute},
		{"CCCUSDT", "zzz", DefaultLiveDefaultTTL}, // unparseable → default
	} {
		if err := w.TryEnqueue(c.sym, c.interval, liveBar(1, false)); err != nil {
			t.Fatalf("TryEnqueue %s: %v", c.sym, err)
		}
		key := w.Key(c.sym, c.interval)
		eventually(t, 6*time.Second, func() bool {
			n, _ := client.Exists(ctx, key).Result()
			return n == 1
		})
		ttl, _ := client.PTTL(ctx, key).Result()
		if ttl <= 0 || ttl > c.maxTTL {
			t.Errorf("%s PTTL = %v, want (0, %v]", c.sym, ttl, c.maxTTL)
		}
	}
}

func TestLiveBarKeyFormat(t *testing.T) {
	w, _, _ := newTestLiveWriter(t, LiveBarConfig{})
	if got := w.Key("ethusdt", "1m"); got != "livebar:ETHUSDT:1m" {
		t.Fatalf("Key = %q", got)
	}
	w2, _, _ := newTestLiveWriter(t, LiveBarConfig{KeyPrefix: "lb"})
	if got := w2.Key("BTCUSDT", "1h"); got != "lb:BTCUSDT:1h" {
		t.Fatalf("Key = %q", got)
	}
}

func TestLiveBarTryEnqueueDropsWhenFull(t *testing.T) {
	w, _, _ := newTestLiveWriter(t, LiveBarConfig{Workers: 1, ChannelSize: 1})

	var fullSeen bool
	for i := 0; i < 5000; i++ {
		if err := w.TryEnqueue("BTCUSDT", "1m", liveBar(i, false)); errors.Is(err, ErrBufferFull) {
			fullSeen = true
			break
		}
	}
	if !fullSeen {
		t.Fatal("expected ErrBufferFull once the channel saturates")
	}
	if v := testutil.ToFloat64(w.metrics.dropped); v < 1 {
		t.Fatalf("dropped_total = %v, want >= 1", v)
	}
}

func TestLiveBarGracefulClose(t *testing.T) {
	w, client, _ := newTestLiveWriter(t, LiveBarConfig{Workers: 3})
	ctx := context.Background()

	const n = 60
	for i := 0; i < n; i++ {
		if err := w.TryEnqueue("SYM"+strconv.Itoa(i)+"USDT", "1h", liveBar(i, false)); err != nil {
			t.Fatalf("TryEnqueue %d: %v", i, err)
		}
	}

	done := make(chan struct{})
	go func() { _ = w.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close hung")
	}

	if v := testutil.ToFloat64(w.metrics.updates); v != n {
		t.Fatalf("updates_total = %v, want %d (all drained on Close)", v, n)
	}
	keys, _ := client.Keys(ctx, "livebar:*").Result()
	if len(keys) != n {
		t.Fatalf("live keys = %d, want %d", len(keys), n)
	}

	if err := w.TryEnqueue("BTCUSDT", "1h", liveBar(1, false)); !errors.Is(err, ErrClosed) {
		t.Fatalf("TryEnqueue after Close = %v, want ErrClosed", err)
	}
}

func TestLiveBarContextCancel(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	w, err := NewLiveBarWriter(ctx, client, LiveBarConfig{Registerer: prometheus.NewRegistry()})
	if err != nil {
		t.Fatalf("NewLiveBarWriter: %v", err)
	}

	_ = w.TryEnqueue("BTCUSDT", "1h", liveBar(1, false))
	cancel()

	eventually(t, 6*time.Second, func() bool {
		return errors.Is(w.TryEnqueue("BTCUSDT", "1h", liveBar(2, false)), ErrClosed)
	})

	done := make(chan struct{})
	go func() { _ = w.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung after context cancellation")
	}
}

func TestLiveBarPublish(t *testing.T) {
	w, client, _ := newTestLiveWriter(t, LiveBarConfig{Publish: true})

	subCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sub := client.Subscribe(subCtx, "livebar.1h")
	defer func() { _ = sub.Close() }()
	if _, err := sub.Receive(subCtx); err != nil { // wait for subscription to land
		t.Fatalf("subscribe: %v", err)
	}

	if err := w.TryEnqueue("BTCUSDT", "1h", liveBar(4, true)); err != nil {
		t.Fatalf("TryEnqueue: %v", err)
	}

	msg, err := sub.ReceiveMessage(subCtx)
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	var nums [liveBarCompactLen]float64
	if err := json.Unmarshal([]byte(msg.Payload), &nums); err != nil {
		t.Fatalf("decode payload %q: %v", msg.Payload, err)
	}
	if int64(nums[0]) != liveBar(4, true).StartTime || nums[10] != 1 || nums[9] != 12 {
		t.Fatalf("payload nums = %v", nums)
	}

	// Hash write still happened alongside the publish.
	eventually(t, 6*time.Second, func() bool {
		n, _ := client.Exists(context.Background(), w.Key("BTCUSDT", "1h")).Result()
		return n == 1
	})
}

func TestNewLiveBarWriterNilClient(t *testing.T) {
	if _, err := NewLiveBarWriter(context.Background(), nil, LiveBarConfig{}); err == nil {
		t.Fatal("expected error for nil client")
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

func mustF(t *testing.T, s string) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("parse float %q: %v", s, err)
	}
	return v
}
