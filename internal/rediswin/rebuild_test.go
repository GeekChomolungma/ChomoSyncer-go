package rediswin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestStartTimeOf(t *testing.T) {
	cases := map[string]struct {
		want int64
		ok   bool
	}{
		`[1719835200000,1,2,3]`: {1719835200000, true},
		`[0,1]`:                 {0, true},
		`[-5,1]`:                {-5, true},
		`[]`:                    {0, false},
		`{"t":1}`:               {0, false},
		`[abc,1]`:               {0, false},
		``:                      {0, false},
	}
	for in, exp := range cases {
		got, ok := startTimeOf([]byte(in))
		if ok != exp.ok || got != exp.want {
			t.Errorf("startTimeOf(%q) = (%d,%v), want (%d,%v)", in, got, ok, exp.want, exp.ok)
		}
	}
}

func TestPushBarAndTrimMonotonic(t *testing.T) {
	w, client, _ := newTestWriter(t, Config{})
	ctx := context.Background()
	key := w.Key("BTCUSDT", "1h")

	push := func(ts int64) {
		t.Helper()
		if err := w.PushBarAndTrim(ctx, "BTCUSDT", "1h", CompactBar{StartTime: ts, Close: float64(ts)}); err != nil {
			t.Fatalf("push ts=%d: %v", ts, err)
		}
	}

	push(100)
	push(100) // duplicate: skipped
	push(99)  // older: skipped
	push(101) // newer: accepted

	n, _ := client.LLen(ctx, key).Result()
	if n != 2 {
		t.Fatalf("LLEN = %d, want 2 (only 100 and 101)", n)
	}
	head, _ := client.LIndex(ctx, key, 0).Result()
	var arr []float64
	_ = json.Unmarshal([]byte(head), &arr)
	if int64(arr[0]) != 101 {
		t.Fatalf("head start_time = %v, want 101", arr[0])
	}
	if got := testutil.ToFloat64(w.metrics.barsPushed); got != 2 {
		t.Fatalf("bars_pushed = %v, want 2", got)
	}
	if got := testutil.ToFloat64(w.metrics.barsSkipped); got != 2 {
		t.Fatalf("bars_skipped = %v, want 2", got)
	}
}

func TestRebuildWindow(t *testing.T) {
	w, client, _ := newTestWriter(t, Config{WindowSize: 5})
	ctx := context.Background()
	key := w.Key("BTCUSDT", "1m")

	// seed some junk to prove it is a full replace
	_ = client.RPush(ctx, key, "junk1", "junk2").Err()

	// newest-first, 8 bars, WindowSize 5 -> keep the 5 newest
	bars := make([]CompactBar, 8)
	for i := 0; i < 8; i++ {
		bars[i] = CompactBar{StartTime: int64(1000 - i), Close: float64(1000 - i)} // 1000,999,...,993
	}
	if err := w.RebuildWindow(ctx, "BTCUSDT", "1m", bars); err != nil {
		t.Fatalf("RebuildWindow: %v", err)
	}

	n, _ := client.LLen(ctx, key).Result()
	if n != 5 {
		t.Fatalf("LLEN = %d, want 5", n)
	}
	head, _ := client.LIndex(ctx, key, 0).Result()
	tail, _ := client.LIndex(ctx, key, 4).Result()
	var h, tl []float64
	_ = json.Unmarshal([]byte(head), &h)
	_ = json.Unmarshal([]byte(tail), &tl)
	if int64(h[0]) != 1000 || int64(tl[0]) != 996 {
		t.Fatalf("window = [%v..%v], want [1000..996]", h[0], tl[0])
	}
	if got := testutil.ToFloat64(w.metrics.windowsRebuilt); got != 1 {
		t.Fatalf("windows_rebuilt = %v, want 1", got)
	}
}

func TestRebuildWindowEmptyClears(t *testing.T) {
	w, client, _ := newTestWriter(t, Config{})
	ctx := context.Background()
	key := w.Key("BTCUSDT", "1m")

	_ = client.RPush(ctx, key, "a", "b").Err()
	if err := w.RebuildWindow(ctx, "BTCUSDT", "1m", nil); err != nil {
		t.Fatalf("RebuildWindow(nil): %v", err)
	}
	if n, _ := client.Exists(ctx, key).Result(); n != 0 {
		t.Fatalf("key should be gone after empty rebuild, exists=%d", n)
	}
}

func TestRebuildThenLiveSeam(t *testing.T) {
	w, client, _ := newTestWriter(t, Config{})
	ctx := context.Background()
	key := w.Key("ETHUSDT", "1h")

	// CH-derived tail, newest-first: 300, 240, 180, ...
	ch := []CompactBar{
		{StartTime: 300, Close: 3},
		{StartTime: 240, Close: 2.4},
		{StartTime: 180, Close: 1.8},
	}
	if err := w.RebuildWindow(ctx, "ETHUSDT", "1h", ch); err != nil {
		t.Fatal(err)
	}

	// live bar == current head: must be skipped (idempotent seam)
	_ = w.PushBarAndTrim(ctx, "ETHUSDT", "1h", CompactBar{StartTime: 300, Close: 99})
	// live bar older than head: skipped
	_ = w.PushBarAndTrim(ctx, "ETHUSDT", "1h", CompactBar{StartTime: 120})
	// genuine next bar: prepended
	_ = w.PushBarAndTrim(ctx, "ETHUSDT", "1h", CompactBar{StartTime: 360, Close: 3.6})

	n, _ := client.LLen(ctx, key).Result()
	if n != 4 {
		t.Fatalf("LLEN = %d, want 4 (300,240,180 + 360)", n)
	}
	head, _ := client.LIndex(ctx, key, 0).Result()
	var arr []float64
	_ = json.Unmarshal([]byte(head), &arr)
	if int64(arr[0]) != 360 || arr[4] != 3.6 { // [t,o,h,l,c,...] -> close at idx 4
		t.Fatalf("head = %v, want start_time=360 close=3.6", arr)
	}
}
