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

type fakeFlusher struct {
	mu      sync.Mutex
	batches [][]Row
	calls   int
	failN   int // the first failN calls fail
	closed  bool
}

func (f *fakeFlusher) Flush(_ context.Context, rows []Row) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failN {
		return errors.New("boom")
	}
	f.batches = append(f.batches, append([]Row(nil), rows...))
	return nil
}

func (f *fakeFlusher) Close() error { f.mu.Lock(); f.closed = true; f.mu.Unlock(); return nil }

func (f *fakeFlusher) rowCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, b := range f.batches {
		n += len(b)
	}
	return n
}

func (f *fakeFlusher) batchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
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

func row(sym string, start string, oi float64, rank uint8) Row {
	st := ts(start)
	return Row{Symbol: sym, StartTime: st, SumOpenInterest: oi, SnapTime: st.Add(BarInterval), SrcRank: rank}
}

func newTestWriter(t *testing.T, cfg WriterConfig, f Flusher) (*Writer, *metrics) {
	t.Helper()
	m := newMetrics(prometheus.NewRegistry())
	w := NewWriter(cfg, f, nil, m)
	t.Cleanup(func() { _ = w.Close() })
	return w, m
}

func TestWriterFlushesWhenBatchSizeReached(t *testing.T) {
	f := &fakeFlusher{}
	w, m := newTestWriter(t, WriterConfig{BatchSize: 3, FlushInterval: time.Hour}, f)
	for i := 0; i < 3; i++ {
		if err := w.Push(context.Background(), row("BTCUSDT", "2026-09-17 14:05:00.000", float64(i), RankLive)); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, time.Second, func() bool { return f.batchCount() == 1 })
	if f.rowCount() != 3 {
		t.Fatalf("rows = %d, want 3", f.rowCount())
	}
	if v := testutil.ToFloat64(m.rowsWritten); v != 3 {
		t.Fatalf("rows_written = %v, want 3", v)
	}
}

func TestWriterFlushesOnInterval(t *testing.T) {
	f := &fakeFlusher{}
	w, _ := newTestWriter(t, WriterConfig{BatchSize: 1000, FlushInterval: 10 * time.Millisecond}, f)
	_ = w.Push(context.Background(), row("BTCUSDT", "2026-09-17 14:05:00.000", 1, RankLive))
	eventually(t, time.Second, func() bool { return f.rowCount() == 1 })
}

func TestWriterFlushIsABarrier(t *testing.T) {
	f := &fakeFlusher{}
	w, _ := newTestWriter(t, WriterConfig{BatchSize: 1000, FlushInterval: time.Hour}, f)
	for i := 0; i < 5; i++ {
		_ = w.Push(context.Background(), row("BTCUSDT", "2026-09-17 14:05:00.000", float64(i), RankHist))
	}
	if err := w.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.rowCount() != 5 {
		t.Fatalf("after Flush rows = %d, want 5", f.rowCount())
	}
}

func TestWriterRetriesThenSucceeds(t *testing.T) {
	f := &fakeFlusher{failN: 2}
	w, m := newTestWriter(t, WriterConfig{BatchSize: 1, FlushInterval: time.Hour, RetryBackoff: time.Millisecond, MaxRetryBackoff: 2 * time.Millisecond}, f)
	_ = w.Push(context.Background(), row("BTCUSDT", "2026-09-17 14:05:00.000", 1, RankLive))
	eventually(t, 2*time.Second, func() bool { return f.rowCount() == 1 })
	if v := testutil.ToFloat64(m.flushErrors); v != 2 {
		t.Fatalf("flush_errors = %v, want 2", v)
	}
	if v := testutil.ToFloat64(m.rowsDropped); v != 0 {
		t.Fatalf("rows_dropped = %v, want 0", v)
	}
}

func TestWriterDropsBatchAfterRetries(t *testing.T) {
	f := &fakeFlusher{failN: 1000}
	w, m := newTestWriter(t, WriterConfig{BatchSize: 1, FlushInterval: time.Hour, MaxRetries: 2, RetryBackoff: time.Millisecond, MaxRetryBackoff: 2 * time.Millisecond}, f)
	_ = w.Push(context.Background(), row("BTCUSDT", "2026-09-17 14:05:00.000", 1, RankLive))
	eventually(t, 2*time.Second, func() bool { return testutil.ToFloat64(m.rowsDropped) == 1 })
	if v := testutil.ToFloat64(m.flushErrors); v != 3 {
		t.Fatalf("flush_errors = %v, want 3 (1 try + 2 retries)", v)
	}
}

func TestWriterCloseDrainsAndClosesFlusher(t *testing.T) {
	f := &fakeFlusher{}
	m := newMetrics(prometheus.NewRegistry())
	w := NewWriter(WriterConfig{BatchSize: 1000, FlushInterval: time.Hour}, f, nil, m)
	for i := 0; i < 7; i++ {
		_ = w.Push(context.Background(), row("BTCUSDT", "2026-09-17 14:05:00.000", float64(i), RankLive))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if f.rowCount() != 7 {
		t.Fatalf("Close flushed %d rows, want 7", f.rowCount())
	}
	if !f.closed {
		t.Fatal("flusher was not closed")
	}
	if err := w.Push(context.Background(), row("BTCUSDT", "2026-09-17 14:05:00.000", 1, RankLive)); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("Push after Close: err = %v, want ErrWriterClosed", err)
	}
	if err := w.Close(); err != nil { // idempotent
		t.Fatal(err)
	}
}

func TestWriterRejectsInvalidRows(t *testing.T) {
	w, _ := newTestWriter(t, WriterConfig{}, &fakeFlusher{})
	bad := []Row{
		{},
		row("", "2026-09-17 14:05:00.000", 1, RankLive),
		row("BTCUSDT", "2026-09-17 14:05:00.000", -1, RankLive),
		row("BTCUSDT", "2026-09-17 14:05:00.000", 1, 0),
		row("BTCUSDT", "2026-09-17 14:05:00.000", 1, 9),
		{Symbol: "BTCUSDT", StartTime: ts("2026-09-17 14:05:01.000"), SumOpenInterest: 1, SrcRank: RankLive}, // not on a boundary
	}
	for i, r := range bad {
		if err := w.Push(context.Background(), r); err == nil {
			t.Errorf("row %d (%+v) was accepted", i, r)
		}
	}
}

func TestWriterPushHonoursContextWhenQueueFull(t *testing.T) {
	f := &fakeFlusher{failN: 1 << 30}
	// tiny queue + a flusher that always fails with long backoff => queue stays full
	w, _ := newTestWriter(t, WriterConfig{BatchSize: 1, ChannelSize: 1, FlushInterval: time.Hour, MaxRetries: 50, RetryBackoff: time.Hour, MaxRetryBackoff: time.Hour, ShutdownTimeout: 10 * time.Millisecond}, f)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	var err error
	for i := 0; i < 10 && err == nil; i++ {
		err = w.Push(ctx, row("BTCUSDT", "2026-09-17 14:05:00.000", float64(i), RankLive))
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded once the queue is full", err)
	}
}
