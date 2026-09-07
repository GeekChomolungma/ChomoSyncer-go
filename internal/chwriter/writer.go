package chwriter

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

// Errors returned by Push / TryPush.
var (
	// ErrClosed is returned once the writer has begun shutting down.
	ErrClosed = errors.New("chwriter: writer is closed")
	// ErrBufferFull is returned by TryPush when the ingest channel is full.
	ErrBufferFull = errors.New("chwriter: ingest buffer is full")
)

// Flusher writes one batch of rows to the backing store. Implementations must be
// safe for use from a single goroutine (the writer's run loop) and should treat
// ctx cancellation as "stop trying". The default implementation
// (clickHouseFlusher) additionally reconnects transparently on a broken
// connection.
type Flusher interface {
	Flush(ctx context.Context, rows []Row) error
}

// BatchWriter accumulates kline rows and flushes them to a Flusher in batches,
// triggered by whichever comes first: BatchSize rows buffered, or FlushInterval
// elapsed since the last flush. It is safe for concurrent use.
//
// Lifetime:
//   - The context passed to New/NewWithFlusher bounds the run loop. When it is
//     cancelled the writer drains whatever is already queued, performs a final
//     bounded flush, and stops.
//   - Close() performs the same graceful drain-and-flush explicitly and blocks
//     until it completes; it also closes the default Flusher's connection.
type BatchWriter struct {
	cfg     resolvedConfig
	flusher Flusher
	log     *slog.Logger
	metrics *metrics

	input  chan Row
	stopCh chan struct{} // closed by Close to request graceful shutdown
	done   chan struct{} // closed by the run loop on exit

	closeOnce sync.Once
	closed    atomic.Bool
}

// New builds a BatchWriter backed by a real ClickHouse connection (native
// protocol, github.com/ClickHouse/clickhouse-go/v2). The connection is
// established eagerly but a failure here is not fatal: the run loop will keep
// retrying and reconnecting on every flush.
func New(ctx context.Context, cfg Config) (*BatchWriter, error) {
	f, err := newClickHouseFlusher(cfg)
	if err != nil {
		return nil, err
	}
	return NewWithFlusher(ctx, cfg, f)
}

// NewWithFlusher builds a BatchWriter around a caller-supplied Flusher. Useful
// for tests and for alternative sinks.
func NewWithFlusher(ctx context.Context, cfg Config, flusher Flusher) (*BatchWriter, error) {
	if flusher == nil {
		return nil, errors.New("chwriter: flusher must not be nil")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	w := &BatchWriter{
		cfg:     cfg.resolve(),
		flusher: flusher,
		log:     log.With("component", "clickhouse_batch_writer"),
		metrics: newMetrics(cfg.Registerer),
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
	}
	w.input = make(chan Row, w.cfg.channelSize)
	go w.loop(ctx)
	return w, nil
}

// Push enqueues a row, blocking while the ingest buffer is full until space
// frees up, ctx is cancelled, or the writer closes. Use this from paths that
// can tolerate backpressure.
func (w *BatchWriter) Push(ctx context.Context, row Row) error {
	if w.closed.Load() {
		return ErrClosed
	}
	select {
	case w.input <- row:
		return nil
	case <-w.stopCh:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TryPush enqueues a row without blocking. It returns ErrBufferFull immediately
// if the ingest buffer has no room. Use this from latency-critical paths such as
// the WebSocket read loop, where blocking would stall the whole stream.
func (w *BatchWriter) TryPush(row Row) error {
	if w.closed.Load() {
		return ErrClosed
	}
	select {
	case w.input <- row:
		return nil
	default:
		w.metrics.rowsDropped.Inc()
		return ErrBufferFull
	}
}

// Close requests a graceful shutdown: it stops accepting new rows, flushes
// everything still buffered (bounded by ShutdownTimeout), waits for the run loop
// to exit, and closes the underlying Flusher if it is an io.Closer. It is safe
// to call multiple times and from multiple goroutines.
func (w *BatchWriter) Close() error {
	w.closeOnce.Do(func() {
		w.closed.Store(true)
		close(w.stopCh)
	})
	<-w.done
	if c, ok := w.flusher.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// Done returns a channel closed when the run loop has fully stopped.
func (w *BatchWriter) Done() <-chan struct{} { return w.done }

func (w *BatchWriter) loop(rootCtx context.Context) {
	defer close(w.done)

	ticker := time.NewTicker(w.cfg.flushInterval)
	defer ticker.Stop()

	batch := make([]Row, 0, w.cfg.batchSize)

	// flush writes the current batch (if any) and resets it. ctx bounds the
	// flush; during shutdown a fresh bounded context is supplied because
	// rootCtx may already be done.
	flush := func(ctx context.Context, reason string) {
		if len(batch) == 0 {
			return
		}
		w.flushWithRetry(ctx, batch, reason)
		batch = batch[:0]
		w.updateBufferGauge(len(batch))
	}

	shutdown := func(reason string) {
		w.closed.Store(true)
		w.drainInto(&batch, reason)
		ctx, cancel := context.WithTimeout(context.Background(), w.cfg.shutdownTimeout)
		defer cancel()
		flush(ctx, reason)
		w.log.Info("clickhouse batch writer stopped", "reason", reason)
	}

	for {
		select {
		case <-rootCtx.Done():
			shutdown("context_cancelled")
			return

		case <-w.stopCh:
			shutdown("close")
			return

		case row := <-w.input:
			batch = append(batch, row)
			w.updateBufferGauge(len(batch))
			if len(batch) >= w.cfg.batchSize {
				flush(rootCtx, "batch_full")
				ticker.Reset(w.cfg.flushInterval)
			}

		case <-ticker.C:
			flush(rootCtx, "interval")
		}
	}
}

// drainInto pulls every row currently queued in the ingest channel into batch,
// flushing whenever batch fills. It never blocks waiting for new rows; callers
// must have set w.closed first so producers stop enqueuing.
func (w *BatchWriter) drainInto(batch *[]Row, reason string) {
	for {
		select {
		case row := <-w.input:
			*batch = append(*batch, row)
			if len(*batch) >= w.cfg.batchSize {
				ctx, cancel := context.WithTimeout(context.Background(), w.cfg.shutdownTimeout)
				w.flushWithRetry(ctx, *batch, reason+"_batch_full")
				cancel()
				*batch = (*batch)[:0]
			}
		default:
			return
		}
	}
}

// flushWithRetry attempts a flush, retrying with exponential backoff + jitter up
// to MaxRetries. On permanent failure the rows are dropped and counted.
func (w *BatchWriter) flushWithRetry(ctx context.Context, rows []Row, reason string) {
	backoff := w.cfg.retryBackoff
	for attempt := 0; ; attempt++ {
		start := time.Now()
		err := w.flusher.Flush(ctx, rows)
		elapsed := time.Since(start).Seconds()
		if err == nil {
			w.metrics.observeFlushOK(elapsed, len(rows))
			if attempt > 0 {
				w.log.Info("clickhouse flush recovered", "attempts", attempt+1, "rows", len(rows), "reason", reason)
			}
			return
		}
		w.metrics.observeFlushErr(elapsed)

		if attempt >= w.cfg.maxRetries || ctx.Err() != nil {
			w.log.Error("clickhouse flush failed permanently; dropping rows",
				"rows", len(rows), "attempts", attempt+1, "reason", reason, "err", err)
			w.metrics.rowsDropped.Add(float64(len(rows)))
			return
		}

		w.metrics.retries.Inc()
		sleep := backoff + time.Duration(rand.Int63n(int64(backoff)/2+1))
		w.log.Warn("clickhouse flush failed; will retry",
			"attempt", attempt+1, "rows", len(rows), "retry_in", sleep, "reason", reason, "err", err)
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			w.log.Error("clickhouse flush aborted during backoff; dropping rows",
				"rows", len(rows), "reason", reason, "err", ctx.Err())
			w.metrics.rowsDropped.Add(float64(len(rows)))
			return
		}
		if backoff = backoff * 2; backoff > w.cfg.maxRetryBackoff {
			backoff = w.cfg.maxRetryBackoff
		}
	}
}

func (w *BatchWriter) updateBufferGauge(batchLen int) {
	w.metrics.bufferSize.Set(float64(len(w.input) + batchLen))
}
