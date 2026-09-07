package rediswin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// operation labels for metrics.
const (
	opPushTrim = "push_trim"
	opRebuild  = "rebuild"
	opXAdd     = "xadd"
)

// monotonicPushScript prepends a compact bar onto a window List only when it is
// strictly newer than the current head, then trims to WindowSize. Returns 1 if
// written, 0 if skipped. Idempotent: re-pushing the same or an older bar is a
// no-op, which is what makes the "rebuild from ClickHouse, then resume live
// pushes" seam safe. start_time is the first integer of the compact array.
var monotonicPushScript = redis.NewScript(`
local key = KEYS[1]
local payload = ARGV[1]
local ts = tonumber(ARGV[2])
local stop = tonumber(ARGV[3])
local head = redis.call('LINDEX', key, 0)
if head then
  local ht = tonumber(string.match(head, '^%[(%-?%d+)'))
  if ht ~= nil and ts <= ht then
    return 0
  end
end
redis.call('LPUSH', key, payload)
redis.call('LTRIM', key, 0, stop)
return 1
`)

// rebuildWindowScript atomically replaces a window with the given bars
// (newest-first) and trims to WindowSize. Returns the resulting LLEN.
var rebuildWindowScript = redis.NewScript(`
local key = KEYS[1]
local stop = tonumber(ARGV[1])
redis.call('DEL', key)
for i = 2, #ARGV do
  redis.call('RPUSH', key, ARGV[i])
end
if redis.call('EXISTS', key) == 1 then
  redis.call('LTRIM', key, 0, stop)
end
return redis.call('LLEN', key)
`)

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
	// No eager SCRIPT LOAD here: pushOne / RebuildWindow use Script.Run (auto
	// EVAL fallback on NOSCRIPT) and PushBarsAndTrim reloads + retries once. A
	// preload would just stall New when Redis is briefly unreachable at boot.
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

// PushBarAndTrim prepends one closed bar onto the symbol's rolling window (only
// if newer than the head) and trims to WindowSize, atomically via a Lua script.
// A serialization failure is returned before any Redis I/O.
func (w *Writer) PushBarAndTrim(ctx context.Context, symbol, interval string, bar CompactBar) error {
	payload, err := bar.Marshal()
	if err != nil {
		return err
	}
	return w.pushOne(ctx, symbol, interval, payload, bar.StartTime)
}

// PushRawAndTrim is PushBarAndTrim for a caller that already holds the encoded
// compact-bar payload (e.g. a replay / backfill path). The start_time is read
// from the payload's leading integer.
func (w *Writer) PushRawAndTrim(ctx context.Context, symbol, interval string, payload []byte) error {
	ts, ok := startTimeOf(payload)
	if !ok {
		return fmt.Errorf("rediswin: payload is not a compact-bar array: %.32q", payload)
	}
	return w.pushOne(ctx, symbol, interval, payload, ts)
}

func (w *Writer) pushOne(ctx context.Context, symbol, interval string, payload []byte, ts int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := w.Key(symbol, interval)

	start := time.Now()
	res, err := monotonicPushScript.Run(ctx, w.rdb, []string{key}, payload, ts, w.cfg.trimStop).Int64()
	w.metrics.observe(opPushTrim, time.Since(start).Seconds(), err)
	if err != nil {
		w.log.WarnContext(ctx, "redis push+trim failed", "key", key, "err", err)
		return fmt.Errorf("rediswin: push+trim %q: %w", key, err)
	}
	if res == 1 {
		w.metrics.barsPushed.Inc()
	} else {
		w.metrics.barsSkipped.Inc()
	}
	return nil
}

// SymbolBar pairs a closed bar with its symbol/interval for batch writes.
type SymbolBar struct {
	Symbol   string
	Interval string
	Bar      CompactBar
}

// PushBarsAndTrim writes a whole cross-section of closed bars, one monotonic
// push per key, pipelined into a single round-trip. All bars are serialized up
// front, so a single bad bar aborts the call before any Redis I/O.
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
		ts      int64
	}
	items := make([]keyed, len(bars))
	for i, sb := range bars {
		p, err := sb.Bar.Marshal()
		if err != nil {
			return fmt.Errorf("rediswin: bar %d (%s/%s): %w", i, sb.Symbol, sb.Interval, err)
		}
		items[i] = keyed{key: w.Key(sb.Symbol, sb.Interval), payload: p, ts: sb.Bar.StartTime}
	}

	run := func() (pushed, skipped int, err error) {
		pipe := w.pipeline()
		cmds := make([]*redis.Cmd, len(items))
		for i, it := range items {
			cmds[i] = monotonicPushScript.EvalSha(ctx, pipe, []string{it.key}, it.payload, it.ts, w.cfg.trimStop)
		}
		_, execErr := pipe.Exec(ctx)
		for _, c := range cmds {
			v, e := c.Int64()
			if e != nil {
				if err == nil && isNoScript(e) {
					err = e
				}
				continue
			}
			if v == 1 {
				pushed++
			} else {
				skipped++
			}
		}
		if err == nil && execErr != nil && !isNoScript(execErr) {
			err = execErr
		}
		return pushed, skipped, err
	}

	start := time.Now()
	pushed, skipped, err := run()
	if err != nil && isNoScript(err) {
		_ = monotonicPushScript.Load(ctx, w.rdb).Err()
		pushed, skipped, err = run()
	}
	w.metrics.observe(opPushTrim, time.Since(start).Seconds(), err)
	if err != nil {
		w.log.WarnContext(ctx, "redis batch push+trim failed", "bars", len(bars), "err", err)
		return fmt.Errorf("rediswin: batch push+trim (%d bars): %w", len(bars), err)
	}
	w.metrics.barsPushed.Add(float64(pushed))
	w.metrics.barsSkipped.Add(float64(skipped))
	return nil
}

// RebuildWindow atomically replaces a symbol's rolling window with barsNewestFirst
// (most-recent bar at index 0, matching the live layout) and trims to WindowSize.
// Used by the gapfill path to materialise the authoritative ClickHouse tail into
// Redis. An empty slice clears the window.
func (w *Writer) RebuildWindow(ctx context.Context, symbol, interval string, barsNewestFirst []CompactBar) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := w.Key(symbol, interval)

	args := make([]any, 0, len(barsNewestFirst)+1)
	args = append(args, w.cfg.trimStop)
	for i := range barsNewestFirst {
		p, err := barsNewestFirst[i].Marshal()
		if err != nil {
			return fmt.Errorf("rediswin: rebuild %q bar %d: %w", key, i, err)
		}
		args = append(args, p)
	}

	start := time.Now()
	_, err := rebuildWindowScript.Run(ctx, w.rdb, []string{key}, args...).Int64()
	w.metrics.observe(opRebuild, time.Since(start).Seconds(), err)
	if err != nil {
		w.log.WarnContext(ctx, "redis window rebuild failed", "key", key, "err", err)
		return fmt.Errorf("rediswin: rebuild window %q: %w", key, err)
	}
	w.metrics.windowsRebuilt.Inc()
	return nil
}

func (w *Writer) pipeline() redis.Pipeliner {
	if w.cfg.atomic {
		return w.rdb.TxPipeline()
	}
	return w.rdb.Pipeline()
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

// startTimeOf reads the leading integer of a compact-bar array, e.g.
// "[1719835200000,60250.5,...]" -> 1719835200000.
func startTimeOf(p []byte) (int64, bool) {
	if len(p) < 2 || p[0] != '[' {
		return 0, false
	}
	j := 1
	if j < len(p) && p[j] == '-' {
		j++
	}
	digits := j
	for j < len(p) && p[j] >= '0' && p[j] <= '9' {
		j++
	}
	if j == digits {
		return 0, false
	}
	n, err := strconv.ParseInt(string(p[1:j]), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func isNoScript(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOSCRIPT")
}
