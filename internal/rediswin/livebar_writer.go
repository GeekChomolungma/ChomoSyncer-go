package rediswin

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
)

// Errors from LiveBarWriter enqueue paths.
var (
	// ErrClosed is returned once the writer has begun shutting down.
	ErrClosed = errors.New("rediswin: live bar writer is closed")
	// ErrBufferFull is returned by TryEnqueue when the ingest channel is full.
	ErrBufferFull = errors.New("rediswin: live bar buffer is full")
)

// Live-bar defaults (design doc section 3.3).
const (
	DefaultLivePrefix     = "livebar"
	DefaultLiveWorkers    = 2
	DefaultLiveChannel    = 8192
	DefaultLiveWriteTO    = 200 * time.Millisecond
	DefaultLiveTTLMult    = 2
	DefaultLiveDefaultTTL = 2 * time.Hour
	liveOpWrite           = "livebar"
)

// LiveBarConfig configures a LiveBarWriter.
type LiveBarConfig struct {
	// KeyPrefix overrides the "livebar" namespace. Defaults to DefaultLivePrefix.
	KeyPrefix string
	// Workers is the number of goroutines draining the ingest channel and
	// performing Redis writes. Defaults to DefaultLiveWorkers.
	Workers int
	// ChannelSize is the ingest channel capacity. Defaults to DefaultLiveChannel.
	ChannelSize int
	// WriteTimeout bounds a single HSET+PEXPIRE(+PUBLISH) round-trip.
	// Defaults to DefaultLiveWriteTO.
	WriteTimeout time.Duration
	// TTLMultiple sets PEXPIRE to TTLMultiple × interval. Defaults to DefaultLiveTTLMult.
	TTLMultiple int
	// DefaultTTL is used when the interval string cannot be parsed.
	// Defaults to DefaultLiveDefaultTTL.
	DefaultTTL time.Duration
	// Publish, when true, also PUBLISHes each update to
	// "<ChannelPrefix>.<interval>" as an 11-element compact array.
	Publish bool
	// ChannelPrefix is the Pub/Sub channel namespace. Defaults to DefaultLivePrefix.
	ChannelPrefix string

	// Registerer receives the writer's Prometheus metrics. When nil a private
	// registry is used.
	Registerer prometheus.Registerer
	// Logger is used for operational warnings. Defaults to slog.Default().
	Logger *slog.Logger
}

type resolvedLiveConfig struct {
	keyPrefix     string
	workers       int
	channelSize   int
	writeTimeout  time.Duration
	ttlMultiple   int
	defaultTTL    time.Duration
	publish       bool
	channelPrefix string
}

func (c LiveBarConfig) resolve() resolvedLiveConfig {
	rc := resolvedLiveConfig{
		keyPrefix:     c.KeyPrefix,
		workers:       c.Workers,
		channelSize:   c.ChannelSize,
		writeTimeout:  c.WriteTimeout,
		ttlMultiple:   c.TTLMultiple,
		defaultTTL:    c.DefaultTTL,
		publish:       c.Publish,
		channelPrefix: c.ChannelPrefix,
	}
	if rc.keyPrefix == "" {
		rc.keyPrefix = DefaultLivePrefix
	}
	if rc.workers <= 0 {
		rc.workers = DefaultLiveWorkers
	}
	if rc.channelSize <= 0 {
		rc.channelSize = DefaultLiveChannel
	}
	if rc.writeTimeout <= 0 {
		rc.writeTimeout = DefaultLiveWriteTO
	}
	if rc.ttlMultiple <= 0 {
		rc.ttlMultiple = DefaultLiveTTLMult
	}
	if rc.defaultTTL <= 0 {
		rc.defaultTTL = DefaultLiveDefaultTTL
	}
	if rc.channelPrefix == "" {
		rc.channelPrefix = rc.keyPrefix
	}
	return rc
}

// LiveClientOptions returns redis.Options tuned for the fire-and-forget live
// feed: its own small pool, short timeouts, no retries. Build a dedicated
// *redis.Client from these and pass it to NewLiveBarWriter, separate from the
// closed-bar client, so a burst of live writes cannot starve the closed-bar
// connection pool (design doc section 3.3).
func LiveClientOptions(addr string) *redis.Options {
	return &redis.Options{
		Addr:                  addr,
		PoolSize:              8,
		MinIdleConns:          2,
		DialTimeout:           2 * time.Second,
		ReadTimeout:           300 * time.Millisecond,
		WriteTimeout:          300 * time.Millisecond,
		PoolTimeout:           200 * time.Millisecond,
		MaxRetries:            -1,
		ContextTimeoutEnabled: true,
	}
}

type liveMetrics struct {
	updates prometheus.Counter
	dropped prometheus.Counter
	latency *prometheus.HistogramVec
}

func newLiveMetrics(reg prometheus.Registerer, queueLen func() float64) *liveMetrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)
	m := &liveMetrics{
		updates: f.NewCounter(prometheus.CounterOpts{
			Name: "redis_livebar_updates_total",
			Help: "Live (unclosed) bar snapshots written to Redis.",
		}),
		dropped: f.NewCounter(prometheus.CounterOpts{
			Name: "redis_livebar_dropped_total",
			Help: "Live bar snapshots dropped because the ingest channel was full.",
		}),
		latency: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "redis_livebar_latency_seconds",
			Help:    "Latency of a single live bar HSET+PEXPIRE(+PUBLISH) round-trip.",
			Buckets: []float64{.0002, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5},
		}, []string{"status"}),
	}
	f.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "redis_livebar_queue_length",
		Help: "Live bar ingest channel backlog.",
	}, queueLen)
	return m
}

func (m *liveMetrics) observe(seconds float64, err error) {
	status := "ok"
	if err != nil {
		status = "error"
	}
	m.latency.WithLabelValues(status).Observe(seconds)
}

type liveJob struct {
	key      string
	interval string
	fields   []any
	payload  []byte // compact array for PUBLISH; nil when Publish is disabled
}

// LiveBarWriter buffers live (possibly unclosed) bar snapshots and writes them
// to Redis from a pool of worker goroutines, fire-and-forget. It mirrors
// chwriter.BatchWriter's "buffered channel + background goroutines" shape but
// never retries and drops on overflow, because a lost live frame is replaced by
// the next one ~250ms later.
//
// The caller owns the redis.Cmdable's lifecycle (Close does not touch it) and
// should give this writer its own dedicated client — see LiveClientOptions.
type LiveBarWriter struct {
	rdb     redis.Cmdable
	cfg     resolvedLiveConfig
	metrics *liveMetrics
	log     *slog.Logger

	input  chan liveJob
	stopCh chan struct{}
	wg     sync.WaitGroup

	closeOnce sync.Once
	closed    atomic.Bool
}

// NewLiveBarWriter starts the worker pool. The ctx bounds the workers' lifetime:
// cancelling it drains the channel and stops them, equivalent to Close.
func NewLiveBarWriter(ctx context.Context, rdb redis.Cmdable, cfg LiveBarConfig) (*LiveBarWriter, error) {
	if rdb == nil {
		return nil, errors.New("rediswin: live bar redis client must not be nil")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	w := &LiveBarWriter{
		rdb:    rdb,
		cfg:    cfg.resolve(),
		log:    log.With("component", "redis_live_bar"),
		stopCh: make(chan struct{}),
	}
	w.input = make(chan liveJob, w.cfg.channelSize)
	w.metrics = newLiveMetrics(cfg.Registerer, func() float64 { return float64(len(w.input)) })

	for i := 0; i < w.cfg.workers; i++ {
		w.wg.Add(1)
		go w.worker(ctx)
	}
	return w, nil
}

// Key returns the Hash key for a symbol/interval, e.g. "livebar:BTCUSDT:1h".
func (w *LiveBarWriter) Key(symbol, interval string) string {
	return w.cfg.keyPrefix + ":" + strings.ToUpper(symbol) + ":" + interval
}

// TryEnqueue hands a snapshot to the worker pool without blocking. It returns
// ErrBufferFull (and increments redis_livebar_dropped_total) if the channel is
// full. This is the WebSocket hot-path entry point.
func (w *LiveBarWriter) TryEnqueue(symbol, interval string, bar LiveBar) error {
	if w.closed.Load() {
		return ErrClosed
	}
	job, err := w.newJob(symbol, interval, bar)
	if err != nil {
		return err
	}
	select {
	case w.input <- job:
		return nil
	default:
		w.metrics.dropped.Inc()
		return ErrBufferFull
	}
}

// Enqueue is the blocking variant: it waits for channel space, ctx cancellation,
// or writer shutdown.
func (w *LiveBarWriter) Enqueue(ctx context.Context, symbol, interval string, bar LiveBar) error {
	if w.closed.Load() {
		return ErrClosed
	}
	job, err := w.newJob(symbol, interval, bar)
	if err != nil {
		return err
	}
	select {
	case w.input <- job:
		return nil
	case <-w.stopCh:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops accepting new snapshots, lets the workers drain what is already
// queued, and waits for them to exit. It never closes the injected client.
func (w *LiveBarWriter) Close() error {
	w.closeOnce.Do(func() {
		w.closed.Store(true)
		close(w.stopCh)
	})
	w.wg.Wait()
	return nil
}

func (w *LiveBarWriter) newJob(symbol, interval string, bar LiveBar) (liveJob, error) {
	j := liveJob{
		key:      w.Key(symbol, interval),
		interval: interval,
		fields:   bar.hashFields(),
	}
	if w.cfg.publish {
		p, err := bar.marshalCompact()
		if err != nil {
			return liveJob{}, err
		}
		j.payload = p
	}
	return j, nil
}

func (w *LiveBarWriter) worker(ctx context.Context) {
	defer w.wg.Done()
	for {
		select {
		case <-ctx.Done():
			w.closed.Store(true)
			w.drain()
			return
		case <-w.stopCh:
			w.drain()
			return
		case job := <-w.input:
			w.write(job)
		}
	}
}

// drain flushes whatever is already queued without blocking for more. Multiple
// workers may run this concurrently on shutdown; channel receives are safe.
func (w *LiveBarWriter) drain() {
	for {
		select {
		case job := <-w.input:
			w.write(job)
		default:
			return
		}
	}
}

func (w *LiveBarWriter) write(j liveJob) {
	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.writeTimeout)
	defer cancel()

	ttl := w.ttlFor(j.interval)

	start := time.Now()
	pipe := w.rdb.Pipeline()
	pipe.HSet(ctx, j.key, j.fields...)
	pipe.PExpire(ctx, j.key, ttl)
	if j.payload != nil {
		pipe.Publish(ctx, w.cfg.channelPrefix+"."+j.interval, j.payload)
	}
	_, err := pipe.Exec(ctx)
	w.metrics.observe(time.Since(start).Seconds(), err)

	if err != nil {
		w.log.WarnContext(ctx, "live bar write failed", "key", j.key, "err", err)
		return
	}
	w.metrics.updates.Inc()
}

func (w *LiveBarWriter) ttlFor(interval string) time.Duration {
	if d, ok := parseIntervalDuration(interval); ok {
		return time.Duration(w.cfg.ttlMultiple) * d
	}
	return w.cfg.defaultTTL
}
