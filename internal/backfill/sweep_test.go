package backfill

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type fakeFinder struct {
	gaps map[string][]Range
	err  error
	got  [2]time.Time
}

func (f *fakeFinder) FindGaps(_ context.Context, _ string, from, to time.Time) (map[string][]Range, error) {
	f.got = [2]time.Time{from, to}
	return f.gaps, f.err
}

// fakeSub answers each submit with a scripted outcome per range.
type fakeSub struct {
	mu      sync.Mutex
	reqs    []Request
	fetched func(Key, Range) (int, error)
	skipped int
}

func (s *fakeSub) Submit(r Request) {
	s.mu.Lock()
	s.reqs = append(s.reqs, r)
	s.mu.Unlock()
	var out []RangeOutcome
	for k, rs := range r.Ranges {
		for _, rg := range rs {
			n, err := s.fetched(k, rg)
			out = append(out, RangeOutcome{Key: k, Range: rg, Fetched: n, Err: err})
		}
	}
	r.deliver(RepairResult{Outcomes: out, SkippedKeys: s.skipped})
}

func newSweeper(t *testing.T, f *fakeFinder, s *fakeSub, mut func(*SweepConfig)) *Sweeper {
	t.Helper()
	cfg := SweepConfig{Clock: func() time.Time { return fixedNow.Add(30 * time.Second) }, Registerer: prometheus.NewRegistry()}
	if mut != nil {
		mut(&cfg)
	}
	sw, err := NewSweeper(cfg, f, s)
	if err != nil {
		t.Fatal(err)
	}
	return sw
}

func TestSweepWindowAndRepair(t *testing.T) {
	h := fixedNow.Add(-3 * time.Hour)
	f := &fakeFinder{gaps: map[string][]Range{"BTCUSDT": {{h, h.Add(time.Minute)}}}}
	s := &fakeSub{fetched: func(Key, Range) (int, error) { return 1, nil }}
	sw := newSweeper(t, f, s, nil)

	st, err := sw.SweepOnce(context.Background())
	if err != nil || st.Repaired != 1 || st.Found != 1 {
		t.Fatalf("st=%+v err=%v", st, err)
	}
	// now = 12:00:30, settle 3m -> to = 11:57:00, window 24h
	wantTo := fixedNow.Add(-3 * time.Minute)
	if !f.got[1].Equal(wantTo) || !f.got[0].Equal(wantTo.Add(-24*time.Hour)) {
		t.Fatalf("scan window = %v", f.got)
	}
	if s.reqs[0].Reason != ReasonSweep {
		t.Fatalf("reason = %v", s.reqs[0].Reason)
	}
}

func TestSweepRemembersEmptyRanges(t *testing.T) {
	h := fixedNow.Add(-3 * time.Hour)
	f := &fakeFinder{gaps: map[string][]Range{"BTCUSDT": {{h, h.Add(time.Minute)}}}}
	s := &fakeSub{fetched: func(Key, Range) (int, error) { return 0, nil }}
	sw := newSweeper(t, f, s, nil)

	st, _ := sw.SweepOnce(context.Background())
	if st.Empty != 1 {
		t.Fatalf("first sweep st=%+v", st)
	}
	st, _ = sw.SweepOnce(context.Background())
	if st.Skipped != 1 || st.Submitted != 0 || len(s.reqs) != 1 {
		t.Fatalf("second sweep must not re-request a known-empty range: st=%+v reqs=%d", st, len(s.reqs))
	}
}

func TestSweepEmptyCacheExpires(t *testing.T) {
	h := fixedNow.Add(-3 * time.Hour)
	f := &fakeFinder{gaps: map[string][]Range{"BTCUSDT": {{h, h.Add(time.Minute)}}}}
	s := &fakeSub{fetched: func(Key, Range) (int, error) { return 0, nil }}
	now := fixedNow
	var mu sync.Mutex
	sw := newSweeper(t, f, s, func(c *SweepConfig) {
		c.EmptyTTL = time.Hour
		c.Clock = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	})
	sw.SweepOnce(context.Background())
	mu.Lock()
	now = now.Add(2 * time.Hour)
	mu.Unlock()
	if st, _ := sw.SweepOnce(context.Background()); st.Submitted != 1 {
		t.Fatalf("expired empty entry should be retried: %+v", st)
	}
}

func TestSweepFailureAndSkippedAreNotCachedAsEmpty(t *testing.T) {
	h := fixedNow.Add(-3 * time.Hour)
	f := &fakeFinder{gaps: map[string][]Range{"BTCUSDT": {{h, h.Add(time.Minute)}}}}
	s := &fakeSub{fetched: func(Key, Range) (int, error) { return 0, errors.New("429") }}
	sw := newSweeper(t, f, s, nil)
	if st, _ := sw.SweepOnce(context.Background()); st.Failed != 1 {
		t.Fatalf("st=%+v", st)
	}
	if st, _ := sw.SweepOnce(context.Background()); st.Submitted != 1 {
		t.Fatalf("a failed range must be retried: %+v", st)
	}
}

func TestSweepCapsRanges(t *testing.T) {
	h := fixedNow.Add(-5 * time.Hour)
	var rs []Range
	for i := 0; i < 10; i++ {
		from := h.Add(time.Duration(i*2) * time.Minute)
		rs = append(rs, Range{from, from.Add(time.Minute)})
	}
	f := &fakeFinder{gaps: map[string][]Range{"BTCUSDT": rs}}
	s := &fakeSub{fetched: func(Key, Range) (int, error) { return 1, nil }}
	sw := newSweeper(t, f, s, func(c *SweepConfig) { c.MaxRanges = 4 })
	st, _ := sw.SweepOnce(context.Background())
	if st.Found != 10 || st.Submitted != 4 {
		t.Fatalf("st=%+v", st)
	}
}

func TestSweepFindError(t *testing.T) {
	f := &fakeFinder{err: errors.New("ch down")}
	s := &fakeSub{fetched: func(Key, Range) (int, error) { return 1, nil }}
	sw := newSweeper(t, f, s, nil)
	if _, err := sw.SweepOnce(context.Background()); err == nil || len(s.reqs) != 0 {
		t.Fatalf("err=%v reqs=%d", err, len(s.reqs))
	}
}
