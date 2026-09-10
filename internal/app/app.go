package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"

	"github.com/HarvestStars/chomosyncer-go/internal/backfill"
	"github.com/HarvestStars/chomosyncer-go/internal/chwriter"
	"github.com/HarvestStars/chomosyncer-go/internal/collector"
	"github.com/HarvestStars/chomosyncer-go/internal/dispatcher"
	"github.com/HarvestStars/chomosyncer-go/internal/metrics"
	"github.com/HarvestStars/chomosyncer-go/internal/rediswin"
	"github.com/HarvestStars/chomosyncer-go/internal/universe"
	"github.com/HarvestStars/chomosyncer-go/internal/windowgate"
)

// App holds every wired component. New builds it without touching the network
// (beyond a best-effort ClickHouse ping); Run starts the feed.
type App struct {
	cfg Config
	log *slog.Logger

	metrics   *metrics.Metrics
	closedRDB *redis.Client
	liveRDB   *redis.Client
	chWriters map[string]*chwriter.BatchWriter
	win       *rediswin.Writer
	live      *rediswin.LiveBarWriter
	univ      *universe.Monitor
	disp      *dispatcher.Dispatcher
	col       *collector.Collector

	gate       *windowgate.Gate
	backfiller *backfill.Backfiller
	chStore    *backfill.CHStore

	coldStartSubmitted atomic.Bool
	symMu              sync.Mutex
	prevSymbols        map[string]struct{}

	shutdownOnce sync.Once
}

// New constructs and wires the pipeline. On any error it tears down whatever was
// already built and returns.
func New(ctx context.Context, cfg Config) (*App, error) {
	cfg = cfg.withDefaults()
	a := &App{
		cfg:         cfg,
		log:         cfg.Logger.With("component", "app"),
		chWriters:   map[string]*chwriter.BatchWriter{},
		prevSymbols: map[string]struct{}{},
	}

	fail := func(format string, args ...any) (*App, error) {
		_ = a.Shutdown(context.Background())
		return nil, fmt.Errorf("app: "+format, args...)
	}

	mxAddr := cfg.App.MetricsAddr
	if mxAddr == "off" {
		mxAddr = ""
	}
	mx, err := metrics.New(metrics.Config{
		Addr:            mxAddr,
		ShutdownTimeout: cfg.App.ShutdownTimeout,
		Version:         cfg.Version,
		Logger:          cfg.Logger,
		// live closure: not-ready until the cold-start backfill (if any) completes.
		Readiness: func() bool { return a.backfiller == nil || a.backfiller.Ready() },
	})
	if err != nil {
		return fail("metrics: %w", err)
	}
	a.metrics = mx
	reg := mx.Registerer()

	a.closedRDB = redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		DB:           cfg.Redis.DB,
		Password:     cfg.Redis.Password,
		PoolSize:     cfg.Redis.PoolSize,
		DialTimeout:  cfg.Redis.DialTimeout,
		ReadTimeout:  cfg.Redis.ReadTimeout,
		WriteTimeout: cfg.Redis.WriteTimeout,
	})
	liveAddr := cfg.Redis.Live.Addr
	if liveAddr == "" {
		liveAddr = cfg.Redis.Addr
	}
	liveOpts := rediswin.LiveClientOptions(liveAddr)
	liveOpts.DB = cfg.Redis.Live.DB
	liveOpts.Password = cfg.Redis.Live.Password
	if cfg.Redis.Live.PoolSize > 0 {
		liveOpts.PoolSize = cfg.Redis.Live.PoolSize
	}
	a.liveRDB = redis.NewClient(liveOpts)

	// One ClickHouse batch writer per interval -> per-interval table. Each gets a
	// registerer wrapped with an {interval} label so the (shared) metric names
	// do not collide.
	for _, iv := range cfg.Collector.Intervals {
		w, err := chwriter.New(ctx, chwriter.Config{
			Addrs:           cfg.ClickHouse.Addrs,
			Database:        cfg.ClickHouse.Database,
			Username:        cfg.ClickHouse.Username,
			Password:        cfg.ClickHouse.Password,
			Table:           cfg.ClickHouse.TablePrefix + "_" + iv,
			DialTimeout:     cfg.ClickHouse.DialTimeout,
			TLS:             cfg.ClickHouse.TLS,
			BatchSize:       cfg.ClickHouse.BatchSize,
			FlushInterval:   cfg.ClickHouse.FlushInterval,
			ChannelSize:     cfg.ClickHouse.ChannelSize,
			MaxRetries:      cfg.ClickHouse.MaxRetries,
			RetryBackoff:    cfg.ClickHouse.RetryBackoff,
			MaxRetryBackoff: cfg.ClickHouse.MaxRetryBackoff,
			ShutdownTimeout: cfg.ClickHouse.ShutdownTimeout,
			Registerer:      prometheus.WrapRegistererWith(prometheus.Labels{"interval": iv}, reg),
			Logger:          cfg.Logger,
		})
		if err != nil {
			return fail("clickhouse writer (%s): %w", iv, err)
		}
		a.chWriters[iv] = w
	}

	// Offline "backfill-only" mode: build only the historical-pull path
	// (universe discovery -> REST fetch -> ClickHouse archive) and skip the
	// entire live pipeline (WS collector, dispatcher, Redis writers, window
	// gate). Run() drives one whole-universe cold-start pass and exits. This is
	// phase one of a two-phase cold start: load deep history offline, verify it,
	// then start the live service with a small cold_start_date.
	if cfg.Backfill.OfflineOnly {
		a.univ = newUniverse(cfg, reg)
		a.chStore, err = newColdStore(cfg)
		if err != nil {
			return fail("clickhouse read store: %w", err)
		}
		a.backfiller, err = newBackfiller(cfg, reg, a.chWriters, a.chStore, nil, nil)
		if err != nil {
			return fail("backfiller: %w", err)
		}
		a.log.Info("backfill-only (offline) mode: WS collector, dispatcher and Redis writers are disabled")
		return a, nil
	}

	a.win, err = rediswin.New(a.closedRDB, rediswin.Config{
		KeyPrefix:    cfg.Redis.Window.KeyPrefix,
		WindowSize:   cfg.Redis.Window.WindowSize,
		StreamKey:    cfg.Redis.Window.StreamKey,
		StreamMaxLen: cfg.Redis.Window.StreamMaxLen,
		Atomic:       cfg.Redis.Window.Atomic,
		Registerer:   reg,
		Logger:       cfg.Logger,
	})
	if err != nil {
		return fail("redis window writer: %w", err)
	}

	a.live, err = rediswin.NewLiveBarWriter(ctx, a.liveRDB, rediswin.LiveBarConfig{
		KeyPrefix:     cfg.Redis.Live.KeyPrefix,
		Workers:       cfg.Redis.Live.Workers,
		ChannelSize:   cfg.Redis.Live.ChannelSize,
		WriteTimeout:  cfg.Redis.Live.WriteTimeout,
		TTLMultiple:   cfg.Redis.Live.TTLMultiple,
		DefaultTTL:    cfg.Redis.Live.DefaultTTL,
		Publish:       cfg.Redis.Live.Publish,
		ChannelPrefix: cfg.Redis.Live.ChannelPrefix,
		Registerer:    reg,
		Logger:        cfg.Logger,
	})
	if err != nil {
		return fail("redis live bar writer: %w", err)
	}

	a.univ = newUniverse(cfg, reg)

	router := &archiveRouter{
		writers: archiveWritersOf(a.chWriters),
		unrouted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "chomosyncer_archive_unrouted_total",
			Help: "Closed bars dropped because no ClickHouse writer is configured for their interval.",
		}),
	}

	// The window gate suppresses live closed-window writes (and kline_ready for
	// that interval) while gapfill rebuilds a key from ClickHouse.
	a.gate = windowgate.New(reg)

	a.disp, err = dispatcher.New(ctx, dispatcher.Config{
		ClosedWorkers:   cfg.Dispatcher.ClosedWorkers,
		ClosedQueueSize: cfg.Dispatcher.ClosedQueueSize,
		SectionTimeout:  cfg.Dispatcher.SectionTimeout,
		PublishTimeout:  cfg.Dispatcher.PublishTimeout,
		BaseInterval:    cfg.Collector.Intervals[0], // pinned to "1m" by config.Validate; withDefaults guarantees len>=1
		ServeIntervals:  cfg.Collector.ServeIntervals,
		Registerer:      reg,
		Logger:          cfg.Logger,
	}, dispatcher.Sinks{
		Live:    a.live,
		Window:  a.gate.WrapWindow(a.win),
		Archive: router,
		Ready:   a.gate.WrapReady(a.win),
	}, a.univ)
	if err != nil {
		return fail("dispatcher: %w", err)
	}

	// --- gapfill / backfiller (optional) ---
	if cfg.Backfill.Enabled {
		a.chStore, err = newColdStore(cfg)
		if err != nil {
			return fail("clickhouse read store: %w", err)
		}
		a.backfiller, err = newBackfiller(cfg, reg, a.chWriters, a.chStore, a.win, gateAdapter{a.gate})
		if err != nil {
			return fail("backfiller: %w", err)
		}
	}

	colCfg := collector.Config{
		Intervals:         cfg.Collector.Intervals,
		ShardsPerInterval: cfg.Collector.ShardsPerInterval,
		WSBaseURL:         cfg.Collector.WSURL,
		ConnectStagger:    cfg.Collector.ConnectStagger,
		ConnectTimeout:    cfg.Collector.ConnectTimeout,
		ReconnectBase:     cfg.Collector.ReconnectBase,
		ReconnectMax:      cfg.Collector.ReconnectMax,
		StaleTimeout:      cfg.Collector.StaleTimeout,
		WatchdogInterval:  cfg.Collector.WatchdogInterval,
		Registerer:        reg,
		Logger:            cfg.Logger,
	}
	if a.backfiller != nil {
		bf := a.backfiller
		colCfg.OnGap = func(g collector.GapEvent) {
			bf.HandleGap(backfill.GapEvent{
				ShardID:     g.ShardID,
				Streams:     g.Streams,
				LastMsgAt:   g.LastMsgAt,
				ReconnectAt: g.ReconnectAt,
			})
		}
	}
	a.col, err = collector.New(ctx, colCfg, a.disp)
	if err != nil {
		return fail("collector: %w", err)
	}

	a.univ.OnChange(a.onUniverseChange)

	return a, nil
}

// newUniverse builds the market-discovery monitor.
func newUniverse(cfg Config, reg prometheus.Registerer) *universe.Monitor {
	return universe.New(universe.Config{
		BaseURL:         cfg.Universe.RESTURL,
		RefreshInterval: cfg.Universe.RefreshInterval,
		RefreshOffset:   cfg.Universe.RefreshOffset,
		HTTPTimeout:     cfg.Universe.HTTPTimeout,
		QuoteAssets:     cfg.Universe.QuoteAssets,
		ContractType:    cfg.Universe.ContractType,
		Status:          cfg.Universe.Status,
		Registerer:      reg,
		Logger:          cfg.Logger,
	})
}

// newColdStore builds the ClickHouse read-back store used by the backfiller to
// resume from max(start_time) and to materialise Redis windows.
func newColdStore(cfg Config) (*backfill.CHStore, error) {
	return backfill.NewCHStore(backfill.CHStoreConfig{
		Addrs:       cfg.ClickHouse.Addrs,
		Database:    cfg.ClickHouse.Database,
		Username:    cfg.ClickHouse.Username,
		Password:    cfg.ClickHouse.Password,
		TablePrefix: cfg.ClickHouse.TablePrefix,
		DialTimeout: cfg.ClickHouse.DialTimeout,
		TLS:         cfg.ClickHouse.TLS,
	})
}

// newBackfiller wires a Backfiller. In offline backfill-only mode rebuild and
// gate are nil (no Redis window to rebuild, no live window to gate) and the
// per-request gate timeout is disabled so a multi-hour historical pull runs to
// completion instead of being force-released after 5 minutes.
func newBackfiller(
	cfg Config,
	reg prometheus.Registerer,
	chWriters map[string]*chwriter.BatchWriter,
	store *backfill.CHStore,
	rebuild backfill.WindowRebuilder,
	gate backfill.Gate,
) (*backfill.Backfiller, error) {
	fetcher := backfill.NewBinanceFetcher(backfill.FetcherConfig{
		BaseURL:    cfg.Universe.RESTURL,
		RPS:        cfg.Backfill.RestRPS,
		Registerer: reg,
		Logger:     cfg.Logger,
	})
	archiveMap := make(map[string]backfill.ArchiveWriter, len(chWriters))
	for iv, w := range chWriters {
		archiveMap[iv] = w
	}
	coldStartTime, err := cfg.Backfill.ParseColdStartTime()
	if err != nil {
		return nil, fmt.Errorf("cold_start_date: %w", err)
	}
	gateTimeout := cfg.Backfill.GateTimeout
	if cfg.Backfill.OfflineOnly {
		gateTimeout = -1 // unbounded: the historical pull must run to completion
	}
	return backfill.New(backfill.Config{
		ColdStartTime: coldStartTime,
		GapDebounce:   cfg.Backfill.GapDebounce,
		Workers:       cfg.Backfill.Workers,
		QueueSize:     cfg.Backfill.QueueSize,
		WindowSize:    cfg.Redis.Window.WindowSize,
		GateTimeout:   gateTimeout,
		FlushWait:     cfg.Backfill.FlushWait,
		Registerer:    reg,
		Logger:        cfg.Logger,
	}, fetcher, archiveMap, store, rebuild, gate)
}

// onUniverseChange fans a refreshed universe out to the collector, and (after
// cold start) enqueues a scoped backfill for any newly listed symbols.
func (a *App) onUniverseChange(s universe.Snapshot) {
	a.log.Info("universe snapshot", "symbols", len(s.Symbols))
	if a.col == nil {
		return // backfill-only (offline) mode: no live collector to feed
	}
	a.col.SetSymbols(s.Symbols)

	if a.backfiller == nil {
		return
	}
	a.symMu.Lock()
	var added []string
	for _, sym := range s.Symbols {
		if _, ok := a.prevSymbols[sym]; !ok {
			added = append(added, sym)
		}
	}
	next := make(map[string]struct{}, len(s.Symbols))
	for _, sym := range s.Symbols {
		next[sym] = struct{}{}
	}
	a.prevSymbols = next
	a.symMu.Unlock()

	// The first snapshot (pre-cold-start) just seeds prevSymbols; cold start
	// covers those keys.
	// Here is only for when each overnight refresh, and we only want to backfill the newly added symbols.
	if a.coldStartSubmitted.Load() && len(added) > 0 {
		a.log.Info("universe added symbols; scheduling backfill", "count", len(added))
		a.backfiller.Submit(backfill.Request{
			Keys:   keysOf(added, a.cfg.Collector.Intervals),
			Reason: backfill.ReasonUniverseAdd,
		})
	}
}

// Run starts the metrics server, the backfiller, and the universe refresh loop
// (whose first refresh drives the initial subscriptions and the cold-start
// backfill), then blocks until ctx is cancelled and shuts down gracefully.
//
// In offline backfill-only mode it instead runs a single whole-universe
// cold-start pass and returns (see runBackfillOnly).
func (a *App) Run(ctx context.Context) error {
	if a.cfg.Backfill.OfflineOnly {
		return a.runBackfillOnly(ctx)
	}
	if err := a.metrics.Start(); err != nil {
		_ = a.Shutdown(context.Background())
		return fmt.Errorf("app: start metrics: %w", err)
	}
	if a.backfiller != nil {
		a.backfiller.Start(ctx)
	}

	// do the fn 'onUniverseChange' -> SetSymbols -> shards -> all subscriptions -> handle events -> dispatch to sinks (live, window, archive) -> write to redis and clickhouse
	if err := a.univ.Start(ctx); err != nil {
		_ = a.Shutdown(context.Background())
		return fmt.Errorf("app: start universe: %w", err)
	}

	if a.backfiller != nil {
		syms := a.univ.Snapshot().Symbols
		a.coldStartSubmitted.Store(true) // here is the first time we activate backfiller
		a.backfiller.SubmitColdStart(keysOf(syms, a.cfg.Collector.Intervals))
		a.log.Info("cold-start backfill submitted", "keys", len(syms)*len(a.cfg.Collector.Intervals))
	}

	a.log.Info("chomosyncer-go running",
		"metrics_addr", a.metrics.Addr(),
		"intervals", a.cfg.Collector.Intervals,
		"symbols", len(a.univ.Snapshot().Symbols),
		"backfill", a.backfiller != nil)

	<-ctx.Done()
	a.log.Info("shutdown signal received; draining")

	sctx, cancel := context.WithTimeout(context.Background(), a.cfg.App.ShutdownTimeout)
	defer cancel()
	return a.Shutdown(sctx)
}

// runBackfillOnly drives a single whole-universe cold-start backfill into
// ClickHouse and returns once it finishes (or ctx is cancelled). No live
// pipeline is started. It returns nil only if the backfill ran to completion,
// so a caller can gate "now start the live service" on a zero exit code; an
// interrupted run returns ctx.Err() and its progress is durable in ClickHouse
// (re-run to resume from max(start_time)).
func (a *App) runBackfillOnly(ctx context.Context) error {
	if err := a.metrics.Start(); err != nil {
		_ = a.Shutdown(context.Background())
		return fmt.Errorf("app: start metrics: %w", err)
	}
	a.backfiller.Start(ctx)

	if err := a.univ.Start(ctx); err != nil {
		_ = a.Shutdown(context.Background())
		return fmt.Errorf("app: start universe: %w", err)
	}

	syms := a.univ.Snapshot().Symbols
	a.coldStartSubmitted.Store(true)
	a.backfiller.SubmitColdStart(keysOf(syms, a.cfg.Collector.Intervals))
	a.log.Info("offline backfill started",
		"symbols", len(syms),
		"intervals", a.cfg.Collector.Intervals,
		"cold_start_date", a.cfg.Backfill.ColdStartDate,
		"metrics_addr", a.metrics.Addr())

	waitErr := a.backfiller.WaitColdStart(ctx)

	sctx, cancel := context.WithTimeout(context.Background(), a.cfg.App.ShutdownTimeout)
	defer cancel()
	shutErr := a.Shutdown(sctx)

	if waitErr != nil {
		a.log.Warn("offline backfill interrupted before completion; progress is persisted in ClickHouse, re-run to resume", "err", waitErr)
		return waitErr
	}
	a.log.Info("offline backfill complete; verify with cmd/test-tools/check_clickhouse_integrity.py, then start the live service")
	return shutErr
}

// Shutdown closes every component in reverse dependency order. Safe to call more
// than once and safe on a partially-built App.
func (a *App) Shutdown(ctx context.Context) error {
	var errs []error
	a.shutdownOnce.Do(func() {
		if a.col != nil {
			errs = append(errs, a.col.Close())
		}
		if a.backfiller != nil {
			errs = append(errs, a.backfiller.Close())
		}
		if a.disp != nil {
			errs = append(errs, a.disp.Close())
		}
		if a.live != nil {
			errs = append(errs, a.live.Close())
		}
		for iv, w := range a.chWriters {
			if w == nil {
				continue
			}
			if err := w.Close(); err != nil {
				errs = append(errs, fmt.Errorf("clickhouse writer (%s): %w", iv, err))
			}
		}
		if a.univ != nil {
			errs = append(errs, a.univ.Close())
		}
		if a.chStore != nil {
			errs = append(errs, a.chStore.Close())
		}
		if a.closedRDB != nil {
			errs = append(errs, a.closedRDB.Close())
		}
		if a.liveRDB != nil {
			errs = append(errs, a.liveRDB.Close())
		}
		if a.metrics != nil {
			errs = append(errs, a.metrics.Close(ctx))
		}
		a.log.Info("shutdown complete")
	})
	return errors.Join(errs...)
}

// --- ClickHouse archive routing ---

type archiveWriter interface {
	TryPush(chwriter.Row) error
}

var _ archiveWriter = (*chwriter.BatchWriter)(nil)

// archiveRouter implements dispatcher.ArchiveSink by dispatching each closed bar
// to the per-interval ClickHouse writer.
type archiveRouter struct {
	writers  map[string]archiveWriter
	unrouted prometheus.Counter
}

var _ dispatcher.ArchiveSink = (*archiveRouter)(nil)

func (r *archiveRouter) TryPush(interval string, row chwriter.Row) error {
	w := r.writers[interval]
	if w == nil {
		r.unrouted.Inc()
		return fmt.Errorf("app: no ClickHouse writer for interval %q", interval)
	}
	return w.TryPush(row)
}

func archiveWritersOf(m map[string]*chwriter.BatchWriter) map[string]archiveWriter {
	out := make(map[string]archiveWriter, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// --- window-gate adapter for the backfiller ---

type gateAdapter struct{ g *windowgate.Gate }

var _ backfill.Gate = gateAdapter{}

func (a gateAdapter) Hold(symbol, interval string) {
	a.g.Hold(windowgate.Key{Symbol: symbol, Interval: interval})
}
func (a gateAdapter) Release(symbol, interval string) {
	a.g.Release(windowgate.Key{Symbol: symbol, Interval: interval})
}

func keysOf(symbols, intervals []string) []backfill.Key {
	out := make([]backfill.Key, 0, len(symbols)*len(intervals))
	for _, s := range symbols {
		for _, iv := range intervals {
			out = append(out, backfill.Key{Symbol: s, Interval: iv})
		}
	}
	return out
}
