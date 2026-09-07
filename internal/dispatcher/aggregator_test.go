package dispatcher

import (
	"testing"
	"time"
)

func TestParseIntervalDuration(t *testing.T) {
	cases := map[string]struct {
		want time.Duration
		ok   bool
	}{
		"1m":   {time.Minute, true},
		"3m":   {3 * time.Minute, true},
		"15m":  {15 * time.Minute, true},
		"30m":  {30 * time.Minute, true},
		"1h":   {time.Hour, true},
		"4h":   {4 * time.Hour, true},
		"1d":   {24 * time.Hour, true},
		"3d":   {3 * 24 * time.Hour, true},
		"1w":   {7 * 24 * time.Hour, true},
		"1M":   {30 * 24 * time.Hour, true},
		"1s":   {time.Second, true},
		"":     {0, false},
		"h":    {0, false},
		"0m":   {0, false},
		"zz":   {0, false},
		"5x":   {0, false},
		"1.5h": {0, false},
	}
	for in, exp := range cases {
		got, ok := parseIntervalDuration(in)
		if ok != exp.ok || got != exp.want {
			t.Errorf("parseIntervalDuration(%q) = (%v,%v), want (%v,%v)", in, got, ok, exp.want, exp.ok)
		}
	}
}

func TestStaticUniverse(t *testing.T) {
	u := NewStaticUniverse("btcusdt", "ETHUSDT")
	if u.Size() != 2 {
		t.Fatalf("Size = %d, want 2", u.Size())
	}
	if !u.Has("BTCUSDT") || !u.Has("ethusdt") {
		t.Fatal("case-insensitive membership broken")
	}
	if u.Has("SOLUSDT") {
		t.Fatal("unexpected membership")
	}
}
