// Package collector owns the Binance USDⓈ-M futures kline WebSocket feed. It
// subscribes every tradable symbol (from the universe module) across the
// configured intervals, sharded primarily by interval and then by a stable
// symbol hash, supervises each shard connection with unlimited jittered
// exponential-backoff reconnect and a staleness watchdog, and forwards every
// decoded event to an EventSink (the dispatcher).
//
// The collector is downstream-agnostic: it knows nothing of Redis or
// ClickHouse, only EventSink.HandleKlineEvent. It is also the only package that
// imports the Binance connector — everything else sees dispatcher.KlineEvent.
package collector

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/HarvestStars/chomosyncer-go/internal/dispatcher"
)

// EventSink receives decoded kline events. *dispatcher.Dispatcher satisfies it.
type EventSink interface {
	HandleKlineEvent(dispatcher.KlineEvent) error
}

var _ EventSink = (*dispatcher.Dispatcher)(nil)

// GapEvent reports that a shard missed data between LastMsgAt (its last frame
// before the drop) and ReconnectAt (when it came back), across the given
// streams. The app forwards it to the backfiller; collector does not import
// backfill.
type GapEvent struct {
	ShardID     string
	Streams     []string
	LastMsgAt   time.Time
	ReconnectAt time.Time
}

// streamClient is the minimal WebSocket-streams surface a shard needs. The
// production implementation wraps binance-connector-go; tests inject a fake.
// onEvent (passed to the factory) is called for every decoded kline frame.
type streamClient interface {
	// Connect dials and subscribes the given streams via the connection URL.
	Connect(ctx context.Context, streams []string) error
	// Subscribe / Unsubscribe adjust the live subscription set.
	Subscribe(ctx context.Context, streams []string) error
	Unsubscribe(ctx context.Context, streams []string) error
	// LastMessageAt is the time of the most recent frame of any kind, for the
	// staleness watchdog.
	LastMessageAt() time.Time
	// Errors reports fatal connection errors (e.g. reconnect-attempts-exhausted).
	Errors() <-chan error
	// Close tears the connection down.
	Close() error
}

type clientFactory func(shardID string, onEvent func(dispatcher.KlineEvent)) streamClient

// Config tunes the collector. Zero fields take the documented defaults.
type Config struct {
	// Intervals to subscribe per symbol. Default {"1m","1h"}.
	Intervals []string
	// ShardsPerInterval is the fixed bucket count per interval. Fixed so the
	// symbol→shard hash stays stable across universe refreshes. Default 4.
	ShardsPerInterval int
	// WSBaseURL is the futures stream root. Default wss://fstream.binance.com.
	WSBaseURL string
	// ConnectStagger delays each additional shard's first dial, to stay under
	// Binance's connection-rate limit. Default 300ms.
	ConnectStagger time.Duration
	// ReconnectBase / ReconnectMax bound the shard's own jittered exponential
	// backoff (unlimited attempts). Defaults 1s / 30s.
	ReconnectBase time.Duration
	ReconnectMax  time.Duration
	// StaleTimeout forces a reconnect when no frame has arrived for this long.
	// Default 60s.
	StaleTimeout time.Duration
	// WatchdogInterval is how often a shard checks staleness and applies
	// subscription deltas. Default 10s.
	WatchdogInterval time.Duration
	// ConnectTimeout bounds a single dial+subscribe. Default 20s.
	ConnectTimeout time.Duration
	// OnGap, if set, is called (from a shard goroutine) after a shard reconnects
	// following a drop. Must be non-blocking / fast.
	OnGap func(GapEvent)

	Registerer prometheus.Registerer
	Logger     *slog.Logger
}

type resolvedConfig struct {
	intervals         []string
	shardsPerInterval int
	wsBaseURL         string
	connectStagger    time.Duration
	reconnectBase     time.Duration
	reconnectMax      time.Duration
	staleTimeout      time.Duration
	watchdogInterval  time.Duration
	connectTimeout    time.Duration
}

func (c Config) resolve() resolvedConfig {
	rc := resolvedConfig{
		intervals:         c.Intervals,
		shardsPerInterval: c.ShardsPerInterval,
		wsBaseURL:         c.WSBaseURL,
		connectStagger:    c.ConnectStagger,
		reconnectBase:     c.ReconnectBase,
		reconnectMax:      c.ReconnectMax,
		staleTimeout:      c.StaleTimeout,
		watchdogInterval:  c.WatchdogInterval,
		connectTimeout:    c.ConnectTimeout,
	}
	if len(rc.intervals) == 0 {
		rc.intervals = []string{"1m", "1h"}
	}
	if rc.shardsPerInterval < 1 {
		rc.shardsPerInterval = 4
	}
	if rc.wsBaseURL == "" {
		rc.wsBaseURL = "wss://fstream.binance.com"
	}
	if rc.connectStagger <= 0 {
		rc.connectStagger = 300 * time.Millisecond
	}
	if rc.reconnectBase <= 0 {
		rc.reconnectBase = time.Second
	}
	if rc.reconnectMax <= 0 {
		rc.reconnectMax = 30 * time.Second
	}
	if rc.reconnectMax < rc.reconnectBase {
		rc.reconnectMax = rc.reconnectBase
	}
	if rc.staleTimeout <= 0 {
		rc.staleTimeout = 60 * time.Second
	}
	if rc.watchdogInterval <= 0 {
		rc.watchdogInterval = 10 * time.Second
	}
	if rc.connectTimeout <= 0 {
		rc.connectTimeout = 20 * time.Second
	}
	return rc
}

// Option customizes a Collector at construction.
type Option func(*Collector)

// withClientFactory overrides the streamClient factory (tests).
func withClientFactory(f clientFactory) Option {
	return func(c *Collector) { c.factory = f }
}

// Collector supervises the shard connections.
type Collector struct {
	cfg     resolvedConfig
	sink    EventSink
	factory clientFactory
	onGap   func(GapEvent)
	log     *slog.Logger
	metrics *metrics
	rootCtx context.Context

	mu     sync.Mutex
	shards map[string]*shard

	closeOnce sync.Once
	closed    atomic.Bool
	wg        sync.WaitGroup
}

// New builds a Collector. No connection is made until SetSymbols is called.
// The ctx bounds every shard's lifetime; cancelling it is equivalent to Close.
func New(ctx context.Context, cfg Config, sink EventSink, opts ...Option) (*Collector, error) {
	if sink == nil {
		return nil, errors.New("collector: sink must not be nil")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	c := &Collector{
		cfg:     cfg.resolve(),
		sink:    sink,
		onGap:   cfg.OnGap,
		log:     log.With("component", "collector"),
		metrics: newMetrics(cfg.Registerer),
		rootCtx: ctx,
		shards:  make(map[string]*shard),
	}
	c.factory = func(id string, onEvent func(dispatcher.KlineEvent)) streamClient {
		return newBinanceStreamClient(id, c.cfg.wsBaseURL, onEvent, c.log)
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// SetSymbols reconciles the shard set to cover exactly symbols × intervals.
// Called once at startup and again on every hourly universe refresh: new shards
// are created (staggered), existing shards get subscription deltas, and shards
// whose buckets emptied are torn down.
func (c *Collector) SetSymbols(symbols []string) {
	if c.closed.Load() {
		return
	}
	desired := planStreams(symbols, c.cfg.intervals, c.cfg.shardsPerInterval)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return
	}

	newShardDelay := time.Duration(0)
	for id, streams := range desired {
		if sh := c.shards[id]; sh != nil {
			sh.setDesired(streams)
			continue
		}
		sh := newShard(c, id, streams, newShardDelay)
		c.shards[id] = sh
		newShardDelay += c.cfg.connectStagger
		c.wg.Add(1)
		go func(s *shard) {
			defer c.wg.Done()
			s.run(c.rootCtx)
		}(sh)
	}

	for id, sh := range c.shards {
		if _, ok := desired[id]; !ok {
			go sh.stop() // async: don't hold c.mu while waiting for the loop
			delete(c.shards, id)
		}
	}
	c.metrics.shardsActive.Set(float64(len(c.shards)))
	c.log.Info("symbol set applied", "symbols", len(symbols), "shards", len(c.shards))
}

// Close stops every shard and waits for the supervisors to exit. It does not
// close the sink.
func (c *Collector) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.mu.Lock()
		shards := make([]*shard, 0, len(c.shards))
		for _, sh := range c.shards {
			shards = append(shards, sh)
		}
		c.mu.Unlock()
		var wg sync.WaitGroup
		for _, sh := range shards {
			wg.Add(1)
			go func(s *shard) { defer wg.Done(); s.stop() }(sh)
		}
		wg.Wait()

		c.mu.Lock()
		c.shards = make(map[string]*shard)
		c.metrics.shardsActive.Set(0)
		c.mu.Unlock()
	})
	c.wg.Wait()
	return nil
}

// ShardIDs returns the currently managed shard ids (for tests / introspection).
func (c *Collector) ShardIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.shards))
	for id := range c.shards {
		out = append(out, id)
	}
	return out
}
