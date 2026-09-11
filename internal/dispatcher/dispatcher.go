package dispatcher

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/HarvestStars/chomosyncer-go/internal/chwriter"
	"github.com/HarvestStars/chomosyncer-go/internal/rediswin"
)

// Errors returned by HandleKlineEvent.
var (
	// ErrClosed is returned once the dispatcher is shutting down.
	ErrClosed = errors.New("dispatcher: closed")
	// ErrBusy is returned when the closed-bar queue is full.
	ErrBusy = errors.New("dispatcher: closed-bar queue is full")
)

// LiveSink receives every event (closed or not) as a live snapshot.
// *rediswin.LiveBarWriter satisfies it.
type LiveSink interface {
	TryEnqueue(symbol, interval string, bar rediswin.LiveBar) error
}

// WindowSink receives closed bars for the 200-bar rolling window.
// *rediswin.Writer satisfies it.
type WindowSink interface {
	PushBarAndTrim(ctx context.Context, symbol, interval string, bar rediswin.CompactBar) error
}

// ArchiveSink receives closed bars for the ClickHouse archive, keyed by
// interval so the caller can route to a per-interval table / writer. The app
// layer provides the router (one *chwriter.BatchWriter per interval).
type ArchiveSink interface {
	TryPush(interval string, row chwriter.Row) error
}

// ReadyPublisher publishes the cross-section-ready event.
// *rediswin.Writer satisfies it.
type ReadyPublisher interface {
	PublishKlineReady(ctx context.Context, evt rediswin.KlineReadyEvent) (string, error)
}

// Compile-time checks that the production types wire in without adapters.
var (
	_ LiveSink       = (*rediswin.LiveBarWriter)(nil)
	_ WindowSink     = (*rediswin.Writer)(nil)
	_ ReadyPublisher = (*rediswin.Writer)(nil)
)

// Sinks bundles the four downstream targets.
type Sinks struct {
	Live    LiveSink
	Window  WindowSink
	Archive ArchiveSink
	Ready   ReadyPublisher
}

// Config tunes the dispatcher. Zero values pick the documented defaults.
type Config struct {
	// ClosedWorkers drain the closed-bar queue and perform the window push +
	// archive push + section mark. Default 4.
	ClosedWorkers int
	// ClosedQueueSize is the closed-bar queue capacity. Default 4096.
	ClosedQueueSize int
	// SectionTimeout is the fallback: publish kline_ready this long after the
	// first bar of a cross-section even if not every universe symbol is in.
	// Default 5s.
	SectionTimeout time.Duration
	// SectionRetention keeps a section in a published state after it fires so
	// late stragglers do not re-open it. Default: one interval period.
	SectionRetention time.Duration
	// PublishTimeout bounds a single PublishKlineReady call. Default 5s.
	PublishTimeout time.Duration

	// BaseInterval is the only interval the collector ingests. Default "1m".
	BaseInterval string
	// ServeIntervals are coarser intervals whose kline_ready is *derived*: when
	// the base section for the bar that closes a coarser bucket publishes, a
	// kline_ready for that coarser interval is emitted too (same symbol count).
	// No window / archive is written for them — consumers read the ClickHouse
	// rollup tables. Entries must be epoch-aligned integer multiples of the base.
	ServeIntervals []string

	Registerer prometheus.Registerer
	Logger     *slog.Logger
}

const (
	defaultClosedWorkers   = 4
	defaultClosedQueueSize = 4096
	defaultSectionTimeout  = 5 * time.Second
	defaultPublishTimeout  = 5 * time.Second
	defaultRetentionFloor  = 2 * time.Minute
)

type closedJob struct {
	symbol   string
	interval string
	openTime int64
	bar      rediswin.CompactBar
	row      chwriter.Row
	hasRow   bool
}

// Dispatcher fans decoded kline events out to Redis (live + window), ClickHouse,
// and the cross-section aggregator. HandleKlineEvent is safe for concurrent use
// by the collector's read goroutine(s) and never blocks on network I/O.
type Dispatcher struct {
	sinks   Sinks
	agg     *aggregator
	metrics *metrics
	log     *slog.Logger

	closedCh chan closedJob
	stopCh   chan struct{}
	wg       sync.WaitGroup

	closeOnce sync.Once
	closed    atomic.Bool

	// monotonic guard for the closed path; only the calling goroutine(s) touch
	// it, protected by guardMu because HandleKlineEvent may run concurrently.
	guardMu    sync.Mutex
	lastClosed map[string]int64
}

// New builds a Dispatcher and starts its closed-bar workers. The ctx bounds the
// workers' lifetime; cancelling it is equivalent to Close.
//
// universe may be nil ("unknown universe"): the cross-section aggregator then
// publishes kline_ready on the timeout fallback only. The hourly-refreshing
// implementation arrives with the universe module (3b).
func New(ctx context.Context, cfg Config, sinks Sinks, universe UniverseProvider) (*Dispatcher, error) {
	if sinks.Live == nil || sinks.Window == nil || sinks.Archive == nil || sinks.Ready == nil {
		return nil, errors.New("dispatcher: all four sinks (Live, Window, Archive, Ready) are required")
	}
	if cfg.ClosedWorkers <= 0 {
		cfg.ClosedWorkers = defaultClosedWorkers
	}
	if cfg.ClosedQueueSize <= 0 {
		cfg.ClosedQueueSize = defaultClosedQueueSize
	}
	if cfg.SectionTimeout <= 0 {
		cfg.SectionTimeout = defaultSectionTimeout
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = defaultPublishTimeout
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	log = log.With("component", "dispatcher")

	d := &Dispatcher{
		sinks:      sinks,
		metrics:    newMetrics(cfg.Registerer),
		log:        log,
		closedCh:   make(chan closedJob, cfg.ClosedQueueSize),
		stopCh:     make(chan struct{}),
		lastClosed: make(map[string]int64),
	}
	d.agg = newAggregator(
		sinks.Ready, universe, d.metrics, log, &d.closed,
		cfg.SectionTimeout, cfg.SectionRetention, defaultRetentionFloor, cfg.PublishTimeout,
	)

	baseIv := cfg.BaseInterval
	if baseIv == "" {
		baseIv = "1m"
	}
	baseMS := int64(60_000)
	if dur, ok := parseIntervalDuration(baseIv); ok {
		baseMS = dur.Milliseconds()
	}
	var derived []derivedInterval
	for _, iv := range cfg.ServeIntervals {
		dur, ok := parseIntervalDuration(iv)
		if !ok || dur.Milliseconds() <= baseMS || dur.Milliseconds()%baseMS != 0 {
			log.Warn("ignoring un-derivable serve interval", "interval", iv)
			continue
		}
		derived = append(derived, derivedInterval{name: iv, ms: dur.Milliseconds()})
	}
	d.agg.configureDerived(baseIv, baseMS, derived)

	for i := 0; i < cfg.ClosedWorkers; i++ {
		d.wg.Add(1)
		go d.worker(ctx)
	}
	return d, nil
}

// HandleKlineEvent routes one decoded event. It returns nil for a normal
// unclosed frame, ErrClosed during shutdown, ErrBusy if the closed-bar queue is
// full, or a parse error if the event's numeric fields are malformed. Live-sink
// back-pressure is swallowed (counted, not returned).
func (d *Dispatcher) HandleKlineEvent(e KlineEvent) error {
	if d.closed.Load() {
		return ErrClosed
	}

	kind := "live"
	if e.IsFinal {
		kind = "closed"
	}
	d.metrics.events.WithLabelValues(e.Interval, kind).Inc()

	// 1. Live snapshot — every event, closed or not (design doc 3.3).
	lb, err := e.toLiveBar()
	if err != nil {
		d.metrics.parseErrors.Inc()
		d.log.Warn("drop event: bad numeric field", "symbol", e.Symbol, "interval", e.Interval, "err", err)
		return err
	}
	if lerr := d.sinks.Live.TryEnqueue(e.Symbol, e.Interval, lb); lerr != nil {
		d.metrics.dropped.WithLabelValues("live").Inc()
	}

	if !e.IsFinal {
		return nil
	}

	// 2. Monotonic guard: drop replays / reorders on the closed path.
	if !d.acceptClosed(e.Symbol, e.Interval, e.OpenTime) {
		d.metrics.outOfOrder.WithLabelValues(e.Interval).Inc()
		return nil
	}

	// 3. Build closed-bar payloads and hand off to a worker.
	job := closedJob{symbol: e.Symbol, interval: e.Interval, openTime: e.OpenTime}
	if cb, cerr := e.toCompactBar(); cerr == nil {
		job.bar = cb
	} else {
		d.metrics.parseErrors.Inc()
		return cerr
	}
	if row, rerr := e.toRow(); rerr == nil {
		job.row = row
		job.hasRow = true
	} else {
		d.metrics.parseErrors.Inc() // archive skipped, window+section still proceed
	}

	select {
	case d.closedCh <- job:
		return nil
	default:
		d.metrics.dropped.WithLabelValues("closed").Inc()
		d.log.Warn("closed-bar queue full; dropped", "symbol", e.Symbol, "interval", e.Interval, "open_time", e.OpenTime)
		return ErrBusy
	}
}

func (d *Dispatcher) acceptClosed(symbol, interval string, openTime int64) bool {
	k := symbol + "|" + interval
	d.guardMu.Lock()
	defer d.guardMu.Unlock()
	if prev, ok := d.lastClosed[k]; ok && openTime <= prev {
		return false
	}
	d.lastClosed[k] = openTime
	return true
}

func (d *Dispatcher) worker(ctx context.Context) {
	defer d.wg.Done()
	for {
		select {
		case <-ctx.Done():
			d.closed.Store(true)
			d.drain()
			return
		case <-d.stopCh:
			d.drain()
			return
		case job := <-d.closedCh:
			d.handleClosed(job)
		}
	}
}

func (d *Dispatcher) drain() {
	for {
		select {
		case job := <-d.closedCh:
			d.handleClosed(job)
		default:
			return
		}
	}
}

func (d *Dispatcher) handleClosed(job closedJob) {
	// Window push first, then section mark: by the time a section completes and
	// kline_ready fires, every counted symbol's window is already written.
	// 4. closed kline window, kline:symbol:1m
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := d.sinks.Window.PushBarAndTrim(ctx, job.symbol, job.interval, job.bar); err != nil {
		d.metrics.sinkErrors.WithLabelValues("window").Inc()
		d.log.Warn("window push failed", "symbol", job.symbol, "interval", job.interval, "err", err)
	}
	cancel()

	if job.hasRow {
		// 5. clickhouse archive push, kline_archive:1m
		if err := d.sinks.Archive.TryPush(job.interval, job.row); err != nil {
			d.metrics.dropped.WithLabelValues("archive").Inc()
		}
	}

	// 6. cross-section aggregator: mark this symbol's closed bar for the interval.
	d.agg.mark(job.symbol, job.interval, job.openTime)
}

// Close stops accepting events, lets the workers drain the closed-bar queue,
// halts the aggregator's timers, and waits for the workers to exit. It does not
// close the injected sinks. Safe to call multiple times.
func (d *Dispatcher) Close() error {
	d.closeOnce.Do(func() {
		d.closed.Store(true)
		close(d.stopCh)
	})
	d.wg.Wait()
	d.agg.stop()
	return nil
}
