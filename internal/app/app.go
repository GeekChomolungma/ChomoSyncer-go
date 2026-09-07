package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"

	"github.com/HarvestStars/chomosyncer-go/internal/chwriter"
	"github.com/HarvestStars/chomosyncer-go/internal/collector"
	"github.com/HarvestStars/chomosyncer-go/internal/dispatcher"
	"github.com/HarvestStars/chomosyncer-go/internal/metrics"
	"github.com/HarvestStars/chomosyncer-go/internal/rediswin"
	"github.com/HarvestStars/chomosyncer-go/internal/universe"
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

	shutdownOnce sync.Once
}

// New constructs and wires the pipeline. On any error it tears down whatever was
// already built and returns.
func New(ctx context.Context, cfg Config) (*App, error) {
	cfg = cfg.withDefaults()
	a := &App{
		cfg:       cfg,
		log:       cfg.Logger.With("component", "app"),
		chWriters: map[string]*chwriter.BatchWriter{},
	}

	fail := func(format string, args ...any) (*App, error) {
		_ = a.Shutdown(context.Background())
		return nil, fmt.Errorf("app: "+format, args...)
	}

	mxAddr := cfg.MetricsAddr
	if mxAddr == "off" {
		mxAddr = ""
	}
	mx, err := metrics.New(metrics.Config{Addr: mxAddr, Version: cfg.Version, Logger: cfg.Logger})
	if err != nil {
		return fail("metrics: %w", err)
	}
	a.metrics = mx
	reg := mx.Registerer()

	a.closedRDB = redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr, DB: cfg.RedisDB, Password: cfg.RedisPassword,
	})
	liveOpts := rediswin.LiveClientOptions(cfg.LiveRedisAddr)
	liveOpts.DB = cfg.LiveRedisDB
	liveOpts.Password = cfg.LiveRedisPassword
	a.liveRDB = redis.NewClient(liveOpts)

	// One ClickHouse batch writer per interval -> per-interval table. Each gets a
	// registerer wrapped with an {interval} label so the (shared) metric names
	// do not collide.
	for _, iv := range cfg.Intervals {
		w, err := chwriter.New(ctx, chwriter.Config{
			Addrs:       cfg.CHAddrs,
			Database:    cfg.CHDatabase,
			Username:    cfg.CHUsername,
			Password:    cfg.CHPassword,
			Table:       cfg.CHTablePrefix + "_" + iv,
			DialTimeout: cfg.CHDialTimeout,
			Registerer:  prometheus.WrapRegistererWith(prometheus.Labels{"interval": iv}, reg),
			Logger:      cfg.Logger,
		})
		if err != nil {
			return fail("clickhouse writer (%s): %w", iv, err)
		}
		a.chWriters[iv] = w
	}

	a.win, err = rediswin.New(a.closedRDB, rediswin.Config{Registerer: reg, Logger: cfg.Logger})
	if err != nil {
		return fail("redis window writer: %w", err)
	}

	a.live, err = rediswin.NewLiveBarWriter(ctx, a.liveRDB, rediswin.LiveBarConfig{
		Publish: cfg.LivePublish, Registerer: reg, Logger: cfg.Logger,
	})
	if err != nil {
		return fail("redis live bar writer: %w", err)
	}

	a.univ = universe.New(universe.Config{
		BaseURL: cfg.RESTBaseURL, Registerer: reg, Logger: cfg.Logger,
	})

	router := &archiveRouter{
		writers: archiveWritersOf(a.chWriters),
		unrouted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "chomosyncer_archive_unrouted_total",
			Help: "Closed bars dropped because no ClickHouse writer is configured for their interval.",
		}),
	}

	a.disp, err = dispatcher.New(ctx, dispatcher.Config{
		SectionTimeout: cfg.SectionTimeout, Registerer: reg, Logger: cfg.Logger,
	}, dispatcher.Sinks{Live: a.live, Window: a.win, Archive: router, Ready: a.win}, a.univ)
	if err != nil {
		return fail("dispatcher: %w", err)
	}

	a.col, err = collector.New(ctx, collector.Config{
		Intervals:         cfg.Intervals,
		ShardsPerInterval: cfg.ShardsPerInterval,
		WSBaseURL:         cfg.WSBaseURL,
		Registerer:        reg,
		Logger:            cfg.Logger,
	}, a.disp)
	if err != nil {
		return fail("collector: %w", err)
	}

	a.univ.OnChange(func(s universe.Snapshot) {
		a.log.Info("universe snapshot", "symbols", len(s.Symbols))
		a.col.SetSymbols(s.Symbols)
	})

	return a, nil
}

// Run starts the metrics server and the universe refresh loop (whose first
// refresh drives the initial subscriptions), then blocks until ctx is cancelled
// and shuts everything down gracefully.
func (a *App) Run(ctx context.Context) error {
	if err := a.metrics.Start(); err != nil {
		_ = a.Shutdown(context.Background())
		return fmt.Errorf("app: start metrics: %w", err)
	}
	if err := a.univ.Start(ctx); err != nil {
		_ = a.Shutdown(context.Background())
		return fmt.Errorf("app: start universe: %w", err)
	}

	a.log.Info("chomosyncer-go running",
		"metrics_addr", a.metrics.Addr(),
		"intervals", a.cfg.Intervals,
		"symbols", len(a.univ.Snapshot().Symbols))

	<-ctx.Done()
	a.log.Info("shutdown signal received; draining")

	sctx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer cancel()
	return a.Shutdown(sctx)
}

// Shutdown closes every component in reverse dependency order. Safe to call more
// than once and safe on a partially-built App.
func (a *App) Shutdown(ctx context.Context) error {
	var errs []error
	a.shutdownOnce.Do(func() {
		if a.col != nil {
			errs = append(errs, a.col.Close())
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
