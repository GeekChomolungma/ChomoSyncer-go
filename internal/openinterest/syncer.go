package openinterest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/HarvestStars/chomosyncer-go/internal/weightgate"
)

// Config configures the whole module.
type Config struct {
	HistEnabled bool // start-up catch-up and hourly calibration from openInterestHist
	LiveEnabled bool // pre-close snapshots from /fapi/v1/openInterest

	Table string // target table (default DefaultTable)

	Live LiveConfig
	Hist HistConfig

	// /futures/data pool (openInterestHist)
	DataRPS       float64 // token-bucket rate (default 2)
	DataWindowCap int     // hard cap of requests per sliding 5 minutes (default 900 of 1000)

	Writer WriterConfig
}

// Enabled reports whether anything should run.
func (c Config) Enabled() bool { return c.HistEnabled || c.LiveEnabled }

func (c Config) withDefaults() Config {
	c.Live = c.Live.withDefaults()
	c.Hist = c.Hist.withDefaults()
	if c.Table == "" {
		c.Table = DefaultTable
	}
	if c.DataRPS <= 0 {
		c.DataRPS = 2
	}
	if c.DataWindowCap <= 0 {
		c.DataWindowCap = 900
	}
	return c
}

// Validate rejects settings that cannot work, after applying defaults.
func (c Config) Validate() error {
	c = c.withDefaults()
	switch {
	case c.Live.Lead >= BarInterval:
		return fmt.Errorf("open_interest.live_lead (%v) must be shorter than the 5m bar", c.Live.Lead)
	case c.Live.AcceptWindow >= BarInterval:
		return fmt.Errorf("open_interest.live_accept_window (%v) must be shorter than the 5m bar: a round may not run into the next one", c.Live.AcceptWindow)
	case c.Live.Lead < c.Live.MinRemaining:
		return fmt.Errorf("open_interest.live_lead (%v) must not be shorter than the minimum remaining time (%v)", c.Live.Lead, c.Live.MinRemaining)
	case c.Hist.Offset >= c.Hist.Interval:
		return fmt.Errorf("open_interest.hist_reconcile_offset (%v) must be shorter than hist_reconcile_interval (%v)", c.Hist.Offset, c.Hist.Interval)
	case c.Hist.Spread > c.Hist.Interval:
		return fmt.Errorf("open_interest.hist_reconcile_spread (%v) must not exceed hist_reconcile_interval (%v)", c.Hist.Spread, c.Hist.Interval)
	case c.Hist.MaxLimit > 500:
		return fmt.Errorf("open_interest.hist_max_limit (%d) exceeds Binance's documented maximum of 500", c.Hist.MaxLimit)
	case c.Hist.ColdStartWindow > c.Hist.MaxBackfill:
		return fmt.Errorf("open_interest.hist_cold_start_window (%v) must not exceed hist_max_backfill (%v)", c.Hist.ColdStartWindow, c.Hist.MaxBackfill)
	case c.Hist.MaxBackfill > 30*24*time.Hour:
		return fmt.Errorf("open_interest.hist_max_backfill (%v) exceeds the ~30 days Binance keeps", c.Hist.MaxBackfill)
	}
	return nil
}

// Deps are the collaborators of a Syncer; tests substitute fakes.
type Deps struct {
	Symbols   func() []string // the current universe
	Snapshots SnapshotSource
	History   HistSource
	Store     Store
	Sink      RowSink

	Metrics *metrics
	Logger  *slog.Logger
	// Closers are closed, in order, by Syncer.Close after the loops have stopped
	// (writer first, so its queue is drained before the store goes away).
	Closers []io.Closer
}

// Syncer runs the live snapshotter and the hist reconciler against a shared sink.
// The two use different rate-limit pools (/fapi weight vs /futures/data
// requests), so they run side by side: hist catching up at start-up never delays
// a live snapshot.
type Syncer struct {
	cfg  Config
	log  *slog.Logger
	live *Live
	hist *Reconciler
	dep  Deps

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	closed  bool
}

// New wires a Syncer from its collaborators.
func New(cfg Config, d Deps) (*Syncer, error) {
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if d.Symbols == nil || d.Sink == nil {
		return nil, errors.New("openinterest: Symbols and Sink are required")
	}
	if cfg.LiveEnabled && d.Snapshots == nil {
		return nil, errors.New("openinterest: live is enabled but no SnapshotSource was given")
	}
	if cfg.HistEnabled && (d.History == nil || d.Store == nil) {
		return nil, errors.New("openinterest: hist is enabled but no HistSource or Store was given")
	}
	if d.Metrics == nil {
		d.Metrics = newMetrics(nil)
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	s := &Syncer{cfg: cfg, log: d.Logger.With("component", "openinterest"), dep: d}
	cache := newLiveCache(24)
	if cfg.LiveEnabled {
		s.live = NewLive(cfg.Live, d.Snapshots, d.Sink, d.Symbols, cache, d.Metrics, d.Logger)
	}
	if cfg.HistEnabled {
		s.hist = NewReconciler(cfg.Hist, d.History, d.Store, d.Sink, d.Symbols, cache, d.Metrics, d.Logger)
	}
	return s, nil
}

// Build assembles a Syncer with the production collaborators: the ClickHouse
// writer and store, the REST client (live via the shared /fapi weight gate, hist
// via its own /futures/data pool). It returns (nil, nil) when nothing is enabled.
func Build(cfg Config, ch CHConfig, baseURL string, gate *weightgate.Gate, symbols func() []string, reg prometheus.Registerer, log *slog.Logger) (*Syncer, error) {
	cfg = cfg.withDefaults()
	if !cfg.Enabled() {
		return nil, nil
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	ch.Table = cfg.Table
	m := newMetrics(reg)
	flusher, err := NewCHFlusher(ch)
	if err != nil {
		return nil, err
	}
	store, err := NewCHStore(ch)
	if err != nil {
		return nil, err
	}
	writer := NewWriter(cfg.Writer, flusher, log, m)
	pool := NewDataPool(DataPoolConfig{RPS: cfg.DataRPS, WindowCap: cfg.DataWindowCap, Metrics: m, Logger: log})
	client := NewClient(ClientConfig{BaseURL: baseURL, Gate: gate, Pool: pool, Metrics: m, Logger: log})
	s, err := New(cfg, Deps{
		Symbols: symbols, Snapshots: client, History: client, Store: store, Sink: writer,
		Metrics: m, Logger: log, Closers: []io.Closer{writer, store},
	})
	if err != nil {
		_ = writer.Close()
		_ = store.Close()
		return nil, err
	}
	return s, nil
}

// Start launches the enabled loops and returns immediately. ctx bounds them.
func (s *Syncer) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("openinterest: syncer is closed")
	}
	if s.started {
		return errors.New("openinterest: syncer already started")
	}
	s.started = true
	ctx, s.cancel = context.WithCancel(ctx)

	if s.live != nil {
		if s.hist != nil {
			s.hist.SetLiveSince(time.Now())
		}
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.live.Run(ctx) }()
	}
	if s.hist != nil {
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.hist.Run(ctx) }()
	}
	s.log.Info("open-interest sync started", "live", s.live != nil, "hist", s.hist != nil)
	return nil
}

// Close stops the loops, waits for them, then closes the sink and store so queued
// rows are flushed. Safe to call more than once.
func (s *Syncer) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	cancel := s.cancel
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	s.wg.Wait()
	var errs []error
	for _, c := range s.dep.Closers {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
