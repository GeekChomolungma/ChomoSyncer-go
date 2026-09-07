package chwriter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeFlusher is a controllable Flusher for unit tests.
type fakeFlusher struct {
	mu      sync.Mutex
	batches [][]Row
	total   int
	calls   int

	failFirst int           // fail the first N calls, then succeed
	failAll   bool          // fail every call
	delay     time.Duration // artificial per-call latency
	closed    atomic.Bool
}

func (f *fakeFlusher) Flush(ctx context.Context, rows []Row) error {
	f.mu.Lock()
	f.calls++
	call := f.calls
	delay := f.delay
	f.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.failAll || call <= f.failFirst {
		return errors.New("fakeFlusher: injected failure")
	}

	cp := make([]Row, len(rows))
	copy(cp, rows)
	f.mu.Lock()
	f.batches = append(f.batches, cp)
	f.total += len(rows)
	f.mu.Unlock()
	return nil
}

func (f *fakeFlusher) Close() error {
	f.closed.Store(true)
	return nil
}

func (f *fakeFlusher) snapshot() (batches int, totalRows int, calls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches), f.total, f.calls
}

// --- helpers ---

func newTestWriter(t *testing.T, ctx context.Context, cfg Config, ff *fakeFlusher) *BatchWriter {
	t.Helper()
	if cfg.Registerer == nil {
		cfg.Registerer = prometheus.NewRegistry()
	}
	w, err := NewWithFlusher(ctx, cfg, ff)
	if err != nil {
		t.Fatalf("NewWithFlusher: %v", err)
	}
	return w
}

func sampleRow(i int) Row {
	return Row{
		Symbol:    "BTCUSDT",
		StartTime: time.UnixMilli(int64(1_700_000_000_000 + i*60_000)).UTC(),
		EndTime:   time.UnixMilli(int64(1_700_000_000_000 + i*60_000 + 59_999)).UTC(),
		Open:      100 + float64(i),
		High:      101 + float64(i),
		Low:       99 + float64(i),
		Close:     100.5 + float64(i),
		Volume:    10,
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

func TestFlushOnBatchSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ff := &fakeFlusher{}
	w := newTestWriter(t, ctx, Config{
		BatchSize:     100,
		FlushInterval: time.Hour, // make sure only size triggers flushes
	}, ff)

	for i := 0; i < 250; i++ {
		if err := w.Push(ctx, sampleRow(i)); err != nil {
			t.Fatalf("Push %d: %v", i, err)
		}
	}

	// Two full batches should flush without any Close / timer help.
	eventually(t, 2*time.Second, func() bool {
		b, total, _ := ff.snapshot()
		return b == 2 && total == 200
	})

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	b, total, _ := ff.snapshot()
	if b != 3 || total != 250 {
		t.Fatalf("after Close want 3 batches / 250 rows, got %d / %d", b, total)
	}
	if !ff.closed.Load() {
		t.Fatal("expected underlying flusher to be closed")
	}
}

func TestFlushOnInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ff := &fakeFlusher{}
	w := newTestWriter(t, ctx, Config{
		BatchSize:     1_000_000, // never reached
		FlushInterval: 50 * time.Millisecond,
	}, ff)
	defer w.Close()

	for i := 0; i < 7; i++ {
		if err := w.Push(ctx, sampleRow(i)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	eventually(t, 2*time.Second, func() bool {
		_, total, _ := ff.snapshot()
		return total == 7
	})
}

func TestGracefulShutdownFlushesRemainder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ff := &fakeFlusher{}
	w := newTestWriter(t, ctx, Config{
		BatchSize:     5000,
		FlushInterval: time.Hour,
	}, ff)

	for i := 0; i < 42; i++ {
		if err := w.Push(ctx, sampleRow(i)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	// Nothing should have flushed yet.
	if b, _, _ := ff.snapshot(); b != 0 {
		t.Fatalf("expected no flush before Close, got %d batches", b)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	b, total, _ := ff.snapshot()
	if b != 1 || total != 42 {
		t.Fatalf("want 1 batch / 42 rows after Close, got %d / %d", b, total)
	}

	// Push after Close is rejected.
	if err := w.Push(context.Background(), sampleRow(0)); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed after Close, got %v", err)
	}
}

func TestContextCancellationFlushesAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ff := &fakeFlusher{}
	w := newTestWriter(t, ctx, Config{
		BatchSize:     5000,
		FlushInterval: time.Hour,
	}, ff)

	for i := 0; i < 30; i++ {
		if err := w.Push(ctx, sampleRow(i)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	cancel()

	select {
	case <-w.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("run loop did not stop after context cancellation")
	}

	_, total, _ := ff.snapshot()
	if total != 30 {
		t.Fatalf("want 30 rows flushed on cancellation, got %d", total)
	}

	// Close after cancellation must still be safe and not hang.
	if err := w.Close(); err != nil {
		t.Fatalf("Close after cancel: %v", err)
	}
}

func TestRetryThenSucceed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ff := &fakeFlusher{failFirst: 2}
	reg := prometheus.NewRegistry()
	w := newTestWriter(t, ctx, Config{
		BatchSize:     10,
		FlushInterval: time.Hour,
		MaxRetries:    5,
		RetryBackoff:  5 * time.Millisecond,
		Registerer:    reg,
	}, ff)
	defer w.Close()

	for i := 0; i < 10; i++ {
		if err := w.Push(ctx, sampleRow(i)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	eventually(t, 2*time.Second, func() bool {
		_, total, _ := ff.snapshot()
		return total == 10
	})

	if got := testutil.ToFloat64(w.metrics.retries); got < 2 {
		t.Fatalf("want >=2 retries recorded, got %v", got)
	}
	if got := testutil.ToFloat64(w.metrics.rowsDropped); got != 0 {
		t.Fatalf("want 0 dropped rows, got %v", got)
	}
}

func TestRetryExhaustedDropsRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ff := &fakeFlusher{failAll: true}
	w := newTestWriter(t, ctx, Config{
		BatchSize:       10,
		FlushInterval:   time.Hour,
		MaxRetries:      2,
		RetryBackoff:    2 * time.Millisecond,
		ShutdownTimeout: time.Second,
	}, ff)

	for i := 0; i < 10; i++ {
		if err := w.Push(ctx, sampleRow(i)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	// Close must return even though every flush fails.
	done := make(chan error, 1)
	go func() { done <- w.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close hung while every flush failed")
	}

	if got := testutil.ToFloat64(w.metrics.rowsDropped); got != 10 {
		t.Fatalf("want 10 dropped rows, got %v", got)
	}
	if _, total, _ := ff.snapshot(); total != 0 {
		t.Fatalf("want 0 rows persisted, got %d", total)
	}
}

func TestTryPushBufferFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Block the run loop inside Flush so the channel cannot drain.
	ff := &fakeFlusher{delay: 150 * time.Millisecond}
	w := newTestWriter(t, ctx, Config{
		BatchSize:       1,
		FlushInterval:   time.Hour,
		ChannelSize:     4,
		MaxRetries:      1,
		ShutdownTimeout: 100 * time.Millisecond,
	}, ff)
	defer w.Close()

	var fullSeen bool
	for i := 0; i < 1000; i++ {
		if err := w.TryPush(sampleRow(i)); errors.Is(err, ErrBufferFull) {
			fullSeen = true
			break
		}
	}
	if !fullSeen {
		t.Fatal("expected ErrBufferFull once the buffer saturates")
	}
	if got := testutil.ToFloat64(w.metrics.rowsDropped); got < 1 {
		t.Fatalf("want dropped-rows counter incremented on overflow, got %v", got)
	}
}

func TestConcurrentPush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ff := &fakeFlusher{}
	w := newTestWriter(t, ctx, Config{
		BatchSize:     500,
		FlushInterval: 20 * time.Millisecond,
		ChannelSize:   50_000,
	}, ff)

	const (
		writers = 16
		perW    = 1000
	)
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perW; i++ {
				if err := w.Push(ctx, sampleRow(i)); err != nil {
					t.Errorf("Push: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, total, _ := ff.snapshot(); total != writers*perW {
		t.Fatalf("want %d rows, got %d", writers*perW, total)
	}
}

func TestBufferGaugeReflectsBacklog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ff := &fakeFlusher{delay: 80 * time.Millisecond} // stall the loop
	w := newTestWriter(t, ctx, Config{
		BatchSize:       25,
		FlushInterval:   time.Hour,
		ChannelSize:     1000,
		ShutdownTimeout: 100 * time.Millisecond,
	}, ff)

	for i := 0; i < 200; i++ {
		if err := w.Push(ctx, sampleRow(i)); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	eventually(t, time.Second, func() bool {
		return testutil.ToFloat64(w.metrics.bufferSize) > 0
	})

	// Cancel rather than draining all 200 rows at 80ms/batch through Close.
	cancel()
	<-w.Done()
}

func TestNilFlusherRejected(t *testing.T) {
	_, err := NewWithFlusher(context.Background(), Config{}, nil)
	if err == nil {
		t.Fatal("expected error for nil flusher")
	}
}
