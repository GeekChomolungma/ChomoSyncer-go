package openinterest

import (
	"testing"
	"time"
)

func ts(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05.000", s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func TestLiveBarStart(t *testing.T) {
	const accept = 60 * time.Second
	cases := []struct {
		name string
		in   string
		want string // "" = rejected
	}{
		{"30s before the boundary (start of a cycle)", "2026-09-17 14:09:30.000", "2026-09-17 14:05:00.000"},
		{"9s before the boundary (end of a cycle)", "2026-09-17 14:09:51.000", "2026-09-17 14:05:00.000"},
		{"41s past 14:09 mark", "2026-09-17 14:09:41.000", "2026-09-17 14:05:00.000"},
		{"exactly on the boundary", "2026-09-17 14:10:00.000", "2026-09-17 14:05:00.000"},
		{"slightly late", "2026-09-17 14:10:04.000", "2026-09-17 14:05:00.000"},
		{"exactly 60s late is still accepted", "2026-09-17 14:11:00.000", "2026-09-17 14:05:00.000"},
		{"exactly 60s early is still accepted", "2026-09-17 14:09:00.000", "2026-09-17 14:05:00.000"},
		{"61s late is rejected", "2026-09-17 14:11:01.000", ""},
		{"61s early is rejected", "2026-09-17 14:08:59.000", ""},
		{"mid-bar is rejected", "2026-09-17 14:07:30.000", ""},
		{"hour boundary maps to the previous hour's last bar", "2026-09-17 15:00:02.000", "2026-09-17 14:55:00.000"},
		{"day boundary maps to 23:55 of the previous day", "2026-09-18 00:00:05.000", "2026-09-17 23:55:00.000"},
		{"just before the day boundary", "2026-09-17 23:59:45.000", "2026-09-17 23:55:00.000"},
	}
	for _, c := range cases {
		got, ok := liveBarStart(ts(c.in), accept)
		if c.want == "" {
			if ok {
				t.Errorf("%s: %s accepted as %v, want rejected", c.name, c.in, got)
			}
			continue
		}
		if !ok || !got.Equal(ts(c.want)) {
			t.Errorf("%s: liveBarStart(%s) = (%v, %v), want %s", c.name, c.in, got, ok, c.want)
		}
	}
}

func TestLiveBarStartIgnoresInputZone(t *testing.T) {
	loc := time.FixedZone("UTC+8", 8*3600)
	in := time.Date(2026, 9, 17, 22, 9, 41, 0, loc) // = 14:09:41 UTC
	got, ok := liveBarStart(in, time.Minute)
	if !ok || !got.Equal(ts("2026-09-17 14:05:00.000")) || got.Location() != time.UTC {
		t.Fatalf("got (%v, %v)", got, ok)
	}
}

func TestHistBarStart(t *testing.T) {
	cases := []struct{ label, want string }{
		{"2026-09-17 14:10:00.000", "2026-09-17 14:05:00.000"},
		{"2026-09-17 00:00:00.000", "2026-09-16 23:55:00.000"}, // first label of a day -> last bar of the previous day
		{"2026-09-17 00:05:00.000", "2026-09-17 00:00:00.000"},
		{"2026-09-17 23:55:00.000", "2026-09-17 23:50:00.000"},
	}
	for _, c := range cases {
		got, ok := histBarStart(ts(c.label))
		if !ok || !got.Equal(ts(c.want)) {
			t.Errorf("histBarStart(%s) = (%v, %v), want %s", c.label, got, ok, c.want)
		}
	}
	for _, bad := range []string{"2026-09-17 14:10:00.001", "2026-09-17 14:11:00.000", "2026-09-17 14:10:30.000"} {
		if _, ok := histBarStart(ts(bad)); ok {
			t.Errorf("histBarStart(%s) accepted an unaligned label", bad)
		}
	}
}

// live and hist must agree: the same boundary maps to the same row.
func TestLiveAndHistShareTheSameRow(t *testing.T) {
	boundary := ts("2026-09-17 14:10:00.000")
	live, ok1 := liveBarStart(boundary.Add(-20*time.Second), time.Minute)
	hist, ok2 := histBarStart(boundary)
	if !ok1 || !ok2 || !live.Equal(hist) {
		t.Fatalf("live=%v hist=%v", live, hist)
	}
}

func TestNewestPublishedStart(t *testing.T) {
	cases := []struct{ now, want string }{
		// 14:11:14 - 4m = 14:07:14 -> boundary 14:05 (label) -> bar 14:00
		{"2026-09-17 14:11:14.000", "2026-09-17 14:00:00.000"},
		// exactly on the edge: 14:09:00 - 4m = 14:05:00 -> label 14:05 -> bar 14:00
		{"2026-09-17 14:09:00.000", "2026-09-17 14:00:00.000"},
		{"2026-09-17 14:08:59.000", "2026-09-17 13:55:00.000"},
		{"2026-09-17 00:02:00.000", "2026-09-16 23:50:00.000"},
	}
	for _, c := range cases {
		if got := newestPublishedStart(ts(c.now), 4*time.Minute); !got.Equal(ts(c.want)) {
			t.Errorf("newestPublishedStart(%s) = %v, want %s", c.now, got, c.want)
		}
	}
}

func TestFloorBar(t *testing.T) {
	if got := floorBar(ts("2026-09-17 14:07:59.999")); !got.Equal(ts("2026-09-17 14:05:00.000")) {
		t.Fatalf("got %v", got)
	}
}
