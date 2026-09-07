package rediswin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// operation labels for metrics.
const (
	opPushTrim = "push_trim"
	opXAdd     = "xadd"
)

// Writer maintains the per-symbol rolling windows and publishes cross-section
// notifications. It is safe for concurrent use; the caller owns the lifecycle
// of the redis.Cmdable it is given (Writer never closes it).
type Writer struct {
	rdb     redis.Cmdable
	cfg     resolvedConfig
	metrics *metrics
	log     *slog.Logger
}

// New builds a Writer around any go-redis client (*redis.Client,
// *redis.ClusterClient, ...). It fails only on a nil client.
func New(rdb redis.Cmdable, cfg Config) (*Writer, error) {
	if rdb == nil {
		return nil, errors.New("rediswin: redis client must not be nil")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Writer{
		rdb:     rdb,
		cfg:     cfg.resolve(),
		metrics: newMetrics(cfg.Registerer),
		log:     log.With("component", "redis_sliding_window"),
	}, nil
}

// Key returns the List key for a symbol/interval, e.g. "kline:BTCUSDT:1h".
// The symbol is upper-cased to match the design doc's key namespace and
// Binance's kline-stream "s" field, so callers may pass either case.
func (w *Writer) Key(symbol, interval string) string {
	return w.cfg.keyPrefix + ":" + strings.ToUpper(symbol) + ":" + interval
}

// StreamKey is the notification stream this writer publishes to.
func (w *Writer) StreamKey() string { return w.cfg.streamKey }

func (w *Writer) pipeline() redis.Pipeliner {
	if w.cfg.atomic {
		return w.rdb.TxPipeline()
	}
	return w.rdb.Pipeline()
}

// PushBarAndTrim LPUSHes one closed bar onto the symbol's rolling window and
// LTRIMs it back to WindowSize entries, in a single round-trip (design doc
// section 3.2). A serialization failure is returned before any Redis I/O.
func (w *Writer) PushBarAndTrim(ctx context.Context, symbol, interval string, bar CompactBar) error {
	payload, err := bar.Marshal()
	if err != nil {
		return err
	}
	return w.PushRawAndTrim(ctx, symbol, interval, payload)
}

// PushRawAndTrim is PushBarAndTrim for a caller that already holds the encoded
// compact-bar payload (e.g. a replay / backfill path).
func (w *Writer) PushRawAndTrim(ctx context.Context, symbol, interval string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := w.Key(symbol, interval)

	start := time.Now()
	pipe := w.pipeline()
	pipe.LPush(ctx, key, payload)
	pipe.LTrim(ctx, key, 0, w.cfg.trimStop)
	_, err := pipe.Exec(ctx)
	w.metrics.observe(opPushTrim, time.Since(start).Seconds(), err)
	if err != nil {
		w.log.WarnContext(ctx, "redis push+trim failed", "key", key, "err", err)
		return fmt.Errorf("rediswin: push+trim %q: %w", key, err)
	}
	w.metrics.barsPushed.Inc()
	return nil
}

// SymbolBar pairs a closed bar with its symbol/interval for batch writes.
type SymbolBar struct {
	Symbol   string
	Interval string
	Bar      CompactBar
}

// PushBarsAndTrim writes a whole cross-section of closed bars in one pipeline
// round-trip: LPUSH + LTRIM per entry. All bars are serialized up front, so a
// single bad bar aborts the call before any Redis I/O. On a Redis-side failure
// the error names how many bars were in the batch; individual commands are not
// rolled back unless Config.Atomic is set.
func (w *Writer) PushBarsAndTrim(ctx context.Context, bars []SymbolBar) error {
	if len(bars) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	type keyed struct {
		key     string
		payload []byte
	}
	items := make([]keyed, len(bars))
	for i, sb := range bars {
		p, err := sb.Bar.Marshal()
		if err != nil {
			return fmt.Errorf("rediswin: bar %d (%s/%s): %w", i, sb.Symbol, sb.Interval, err)
		}
		items[i] = keyed{key: w.Key(sb.Symbol, sb.Interval), payload: p}
	}

	start := time.Now()
	pipe := w.pipeline()
	for _, it := range items {
		pipe.LPush(ctx, it.key, it.payload)
		pipe.LTrim(ctx, it.key, 0, w.cfg.trimStop)
	}
	_, err := pipe.Exec(ctx)
	w.metrics.observe(opPushTrim, time.Since(start).Seconds(), err)
	if err != nil {
		w.log.WarnContext(ctx, "redis batch push+trim failed", "bars", len(bars), "err", err)
		return fmt.Errorf("rediswin: batch push+trim (%d bars): %w", len(bars), err)
	}
	w.metrics.barsPushed.Add(float64(len(bars)))
	return nil
}

// KlineReadyEvent is the cross-section-ready notification payload
// (design doc section 4.1). It is written to the stream as discrete fields
// "interval", "timestamp", "symbols_count".
type KlineReadyEvent struct {
	Interval     string // "1h", "1m"
	Timestamp    int64  // closed cross-section's k.t, milliseconds
	SymbolsCount int
}

// PublishKlineReady XADDs a kline_ready event onto the notification stream once
// every symbol's bar for the period has been written. It returns the generated
// stream entry ID.
func (w *Writer) PublishKlineReady(ctx context.Context, evt KlineReadyEvent) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if evt.Interval == "" {
		return "", errors.New("rediswin: KlineReadyEvent.Interval is required")
	}

	args := &redis.XAddArgs{
		Stream: w.cfg.streamKey,
		Values: map[string]any{
			"interval":      evt.Interval,
			"timestamp":     evt.Timestamp,
			"symbols_count": evt.SymbolsCount,
		},
	}
	if w.cfg.streamMaxLen > 0 {
		args.MaxLen = w.cfg.streamMaxLen
		args.Approx = true
	}

	start := time.Now()
	id, err := w.rdb.XAdd(ctx, args).Result()
	w.metrics.observe(opXAdd, time.Since(start).Seconds(), err)
	if err != nil {
		w.log.WarnContext(ctx, "redis xadd kline_ready failed", "stream", w.cfg.streamKey, "err", err)
		return "", fmt.Errorf("rediswin: xadd %q: %w", w.cfg.streamKey, err)
	}
	w.metrics.klineReady.Inc()
	return id, nil
}
