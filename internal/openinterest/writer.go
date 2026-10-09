package openinterest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// batchRow is a row type the batching writer can carry: fapi_oi_5m's Row or
// fapi_ls_ratio_5m's LSRow.
type batchRow interface {
	validate() error
}

// rowSink accepts rows for persistence. *batchWriter implements it.
type rowSink[R batchRow] interface {
	Push(ctx context.Context, r R) error
}

// rowFlusher writes one batch of rows to the backing store.
type rowFlusher[R batchRow] interface {
	Flush(ctx context.Context, rows []R) error
}

// RowSink accepts open-interest rows for persistence. *Writer implements it.
type RowSink = rowSink[Row]

// Flusher writes one batch of open-interest rows to the backing store.
type Flusher = rowFlusher[Row]

// ErrWriterClosed is returned by Push after Close.
var ErrWriterClosed = errors.New("openinterest: writer closed")

// WriterConfig tunes the batching writer. Zero values take the defaults.
type WriterConfig struct {
	BatchSize       int           // flush once this many rows are buffered (default 2000)
	FlushInterval   time.Duration // ...or this long after the previous flush (default 1s)
	ChannelSize     int           // ingest queue capacity (default 20000)
	MaxRetries      int           // retries per batch before it is dropped (default 5)
	RetryBackoff    time.Duration // first retry delay, doubling (default 200ms)
	MaxRetryBackoff time.Duration // (default 5s)
	ShutdownTimeout time.Duration // bound on the final flush in Close (default 15s)
}

func (c WriterConfig) withDefaults() WriterConfig {
	if c.BatchSize <= 0 {
		c.BatchSize = 2000
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
	if c.ChannelSize <= 0 {
		c.ChannelSize = 20000
	}
	if c.MaxRetries < 0 {
		c.MaxRetries = 0
	} else if c.MaxRetries == 0 {
		c.MaxRetries = 5
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 200 * time.Millisecond
	}
	if c.MaxRetryBackoff <= 0 {
		c.MaxRetryBackoff = 5 * time.Second
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 15 * time.Second
	}
	return c
}

// batchWriter batches rows and flushes them to a flusher when either BatchSize rows
// are buffered or FlushInterval has elapsed. It never issues single-row inserts. It
// is a small counterpart of chwriter.BatchWriter for this package's row types; the
// kline writer is deliberately left untouched.
type batchWriter[R batchRow] struct {
	cfg     WriterConfig
	flusher rowFlusher[R]
	log     *slog.Logger
	m       *writerMetrics

	in       chan R
	flushReq chan chan error
	stop     chan struct{}
	done     chan struct{}

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// Writer is the batching writer of fapi_oi_5m rows.
type Writer = batchWriter[Row]

// NewWriter starts the open-interest writer's run loop. Close it to drain and stop.
func NewWriter(cfg WriterConfig, flusher Flusher, log *slog.Logger, m *metrics) *Writer {
	if m == nil {
		m = newMetrics(nil)
	}
	return newBatchWriter[Row](cfg, flusher, log, "oi_writer", &m.writerMetrics)
}

func newBatchWriter[R batchRow](cfg WriterConfig, flusher rowFlusher[R], log *slog.Logger, component string, m *writerMetrics) *batchWriter[R] {
	cfg = cfg.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	w := &batchWriter[R]{
		cfg:      cfg,
		flusher:  flusher,
		log:      log.With("component", component),
		m:        m,
		in:       make(chan R, cfg.ChannelSize),
		flushReq: make(chan chan error),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go w.loop()
	return w
}

// Push queues a row, blocking while the queue is full until ctx ends.
func (w *batchWriter[R]) Push(ctx context.Context, r R) error {
	if err := r.validate(); err != nil {
		return err
	}
	if w.closed.Load() {
		return ErrWriterClosed
	}
	select {
	case w.in <- r:
		w.m.bufferRows.Set(float64(len(w.in)))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-w.stop:
		return ErrWriterClosed
	}
}

// Flush blocks until every row queued before the call has been flushed (or
// dropped after its retries). It returns an error if any batch was dropped since
// the previous Flush, including batches flushed in the background in between.
func (w *batchWriter[R]) Flush(ctx context.Context) error {
	reply := make(chan error, 1)
	select {
	case w.flushReq <- reply:
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return ErrWriterClosed
	}
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops accepting rows, flushes what is queued (bounded by ShutdownTimeout)
// and closes the flusher if it is an io.Closer. Safe to call more than once.
func (w *batchWriter[R]) Close() error {
	w.closeOnce.Do(func() {
		w.closed.Store(true)
		close(w.stop)
		<-w.done
		if c, ok := w.flusher.(io.Closer); ok {
			w.closeErr = c.Close()
		}
	})
	return w.closeErr
}

func (w *batchWriter[R]) loop() {
	defer close(w.done)
	batch := make([]R, 0, w.cfg.BatchSize)
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()

	// dropped remembers a batch lost (after its retries) by a size- or timer-
	// triggered flush, so the next Flush barrier reports it: a caller advancing its
	// state after a successful barrier must not step over rows that never landed.
	var dropped error
	flush := func(ctx context.Context) error {
		if len(batch) == 0 {
			return nil
		}
		err := w.flushWithRetry(ctx, batch)
		batch = batch[:0]
		w.m.bufferRows.Set(float64(len(w.in)))
		if err != nil {
			dropped = err
		}
		return err
	}
	drain := func() {
		for {
			select {
			case r := <-w.in:
				batch = append(batch, r)
			default:
				return
			}
		}
	}

	for {
		select {
		case r := <-w.in:
			batch = append(batch, r)
			if len(batch) >= w.cfg.BatchSize {
				_ = flush(context.Background())
			}
		case <-ticker.C:
			_ = flush(context.Background())
		case reply := <-w.flushReq:
			drain()
			err := flush(context.Background())
			if err == nil && dropped != nil {
				err = fmt.Errorf("a batch was dropped since the last flush barrier: %w", dropped)
			}
			dropped = nil
			reply <- err
		case <-w.stop:
			drain()
			ctx, cancel := context.WithTimeout(context.Background(), w.cfg.ShutdownTimeout)
			if err := flush(ctx); err != nil {
				w.log.Error("final flush failed", "err", err)
			}
			cancel()
			return
		}
	}
}

func (w *batchWriter[R]) flushWithRetry(ctx context.Context, rows []R) error {
	start := time.Now()
	backoff := w.cfg.RetryBackoff
	var err error
	for attempt := 0; attempt <= w.cfg.MaxRetries; attempt++ {
		if err = w.flusher.Flush(ctx, rows); err == nil {
			w.m.rowsWritten.Add(float64(len(rows)))
			w.m.flushSeconds.Observe(time.Since(start).Seconds())
			return nil
		}
		w.m.flushErrors.Inc()
		w.log.Warn("flush failed", "attempt", attempt+1, "rows", len(rows), "err", err)
		if attempt == w.cfg.MaxRetries || ctx.Err() != nil {
			break
		}
		// Back off with 50-100% jitter, but never hold up Close: once the writer is
		// stopping, remaining attempts run back to back instead of sleeping.
		sleep := backoff/2 + time.Duration(rand.Int63n(int64(backoff)/2+1))
		t := time.NewTimer(sleep)
		select {
		case <-t.C:
		case <-ctx.Done():
		case <-w.stop:
		}
		t.Stop()
		if backoff *= 2; backoff > w.cfg.MaxRetryBackoff {
			backoff = w.cfg.MaxRetryBackoff
		}
	}
	w.m.rowsDropped.Add(float64(len(rows)))
	w.log.Error("dropping batch after retries", "rows", len(rows), "err", err)
	return err
}
