package openinterest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// RowSink accepts rows for persistence. *Writer implements it.
type RowSink interface {
	Push(ctx context.Context, r Row) error
}

// Flusher writes one batch of rows to the backing store.
type Flusher interface {
	Flush(ctx context.Context, rows []Row) error
}

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

// Writer batches rows and flushes them to a Flusher when either BatchSize rows are
// buffered or FlushInterval has elapsed. It never issues single-row inserts. It is
// a small counterpart of chwriter.BatchWriter for this package's Row type; the
// kline writer is deliberately left untouched.
type Writer struct {
	cfg     WriterConfig
	flusher Flusher
	log     *slog.Logger
	m       *metrics

	in       chan Row
	flushReq chan chan error
	stop     chan struct{}
	done     chan struct{}

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// NewWriter starts the writer's run loop. Close it to drain and stop.
func NewWriter(cfg WriterConfig, flusher Flusher, log *slog.Logger, m *metrics) *Writer {
	cfg = cfg.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	if m == nil {
		m = newMetrics(nil)
	}
	w := &Writer{
		cfg:      cfg,
		flusher:  flusher,
		log:      log.With("component", "oi_writer"),
		m:        m,
		in:       make(chan Row, cfg.ChannelSize),
		flushReq: make(chan chan error),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go w.loop()
	return w
}

// Push queues a row, blocking while the queue is full until ctx ends.
func (w *Writer) Push(ctx context.Context, r Row) error {
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
// dropped after its retries).
func (w *Writer) Flush(ctx context.Context) error {
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
func (w *Writer) Close() error {
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

func (w *Writer) loop() {
	defer close(w.done)
	batch := make([]Row, 0, w.cfg.BatchSize)
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func(ctx context.Context) error {
		if len(batch) == 0 {
			return nil
		}
		err := w.flushWithRetry(ctx, batch)
		batch = batch[:0]
		w.m.bufferRows.Set(float64(len(w.in)))
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
			reply <- flush(context.Background())
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

func (w *Writer) flushWithRetry(ctx context.Context, rows []Row) error {
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
