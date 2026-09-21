package openinterest

import (
	"context"
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
	l, src, sink, _, _, _ := newLiveHarness(t, "2026-09-17 14:09:30.000", "AAA")
	src.fn = func(string) (Snapshot, error) {
		return Snapshot{OpenInterest: 1, Time: boundary.Add(-20 * time.Second)}, nil
	}
	l.Cycle(context.Background(), boundary)
	rows := sink.forSymbol("AAA")
	hist, ok := histBarStart(boundary)
	if len(rows) != 1 || !ok || !rows[0].StartTime.Equal(hist) {
		t.Fatalf("live rows=%+v hist=%v", rows, hist)
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
