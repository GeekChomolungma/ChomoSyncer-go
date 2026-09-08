// Package universe discovers the full set of Binance USDⓈ-M perpetual symbols
// the service collects (design doc section 2.1).
//
// Scope: this is a whole-market collector. The universe is *every* symbol with
// quoteAsset USDT, contractType PERPETUAL and status TRADING — no liquidity or
// activity filtering. Activity selection is a consumer/strategy concern done on
// the collected data.
//
// The list changes only when Binance lists or delists a contract, so it is
// refreshed once per day, aligned to just after 00:00 UTC (exchange time).
//
// Monitor also implements dispatcher.UniverseProvider (Size / Has): with a
// whole-market universe, Size is the market size and Has answers "is this a
// currently tradable contract".
package universe

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/prometheus/client_golang/prometheus"
)

// Defaults.
const (
	DefaultBaseURL         = "https://fapi.binance.com"
	DefaultRefreshInterval = 24 * time.Hour
	DefaultRefreshOffset   = 2 * time.Minute
	DefaultHTTPTimeout     = 20 * time.Second
)

// Config configures a Monitor.
type Config struct {
	// BaseURL is the futures REST root. Defaults to DefaultBaseURL.
	BaseURL string
	// RefreshInterval is the poll period. Defaults to 24h. When >= 1h the loop
	// aligns each run to 00:00 UTC + RefreshOffset; when < 1h it simply ticks
	// (test mode).
	RefreshInterval time.Duration
	// RefreshOffset delays the daily aligned run past midnight UTC so exchange
	// listing/delisting settlement has completed. Defaults to 2m.
	RefreshOffset time.Duration
	// HTTPTimeout bounds a single REST call. Defaults to DefaultHTTPTimeout.
	HTTPTimeout time.Duration
	// HTTPClient overrides the HTTP client (tests point it at httptest).
	HTTPClient *http.Client

	// QuoteAssets filters contracts by quote asset. Defaults to []string{"USDT"}.
	QuoteAssets []string
	// ContractType filters contracts by type. Defaults to "PERPETUAL".
	ContractType string
	// Status filters contracts by trading status. Defaults to "TRADING".
	Status string

	Registerer prometheus.Registerer
	Logger     *slog.Logger
}

// Snapshot is one refresh result. Symbols is sorted and read-only.
type Snapshot struct {
	Symbols     []string
	RefreshedAt time.Time
}

// Monitor polls Binance and publishes the tradable-symbol snapshot.
type Monitor struct {
	cfg     resolvedConfig
	http    *http.Client
	log     *slog.Logger
	metrics *metrics

	mu   sync.RWMutex
	snap Snapshot
	set  map[string]struct{}
	subs []func(Snapshot)

	started   atomic.Bool
	stopCh    chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once
}

type resolvedConfig struct {
	baseURL      string
	refresh      time.Duration
	offset       time.Duration
	quoteAssets  map[string]struct{}
	contractType string
	status       string
}

// New builds a Monitor. It performs no I/O; call Start (or Refresh).
func New(cfg Config) *Monitor {
	rc := resolvedConfig{
		baseURL: cfg.BaseURL,
		refresh: cfg.RefreshInterval,
		offset:  cfg.RefreshOffset,
	}
	if rc.baseURL == "" {
		rc.baseURL = DefaultBaseURL
	}
	if rc.refresh <= 0 {
		rc.refresh = DefaultRefreshInterval
	}
	if rc.offset < 0 {
		rc.offset = 0
	} else if rc.offset == 0 {
		rc.offset = DefaultRefreshOffset
	}

	quoteMap := make(map[string]struct{})
	for _, qa := range cfg.QuoteAssets {
		if qa != "" {
			quoteMap[qa] = struct{}{}
		}
	}
	if len(quoteMap) == 0 {
		quoteMap["USDT"] = struct{}{}
	}
	rc.quoteAssets = quoteMap

	rc.contractType = cfg.ContractType
	if rc.contractType == "" {
		rc.contractType = "PERPETUAL"
	}
	rc.status = cfg.Status
	if rc.status == "" {
		rc.status = "TRADING"
	}
	httpTO := cfg.HTTPTimeout
	if httpTO <= 0 {
		httpTO = DefaultHTTPTimeout
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: httpTO}
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Monitor{
		cfg:     rc,
		http:    hc,
		log:     log.With("component", "universe_monitor"),
		metrics: newMetrics(cfg.Registerer),
		set:     map[string]struct{}{},
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

// OnChange registers a callback invoked (synchronously, outside the lock) after
// every successful refresh, including the first. Register before Start.
func (m *Monitor) OnChange(fn func(Snapshot)) {
	m.mu.Lock()
	m.subs = append(m.subs, fn)
	m.mu.Unlock()
}

// Start performs the first refresh synchronously (failures surface at startup),
// then runs the periodic refresh loop in a background goroutine.
func (m *Monitor) Start(ctx context.Context) error {
	if _, err := m.Refresh(ctx); err != nil {
		return err
	}
	m.started.Store(true)
	go m.loop(ctx)
	return nil
}

// Close stops the refresh loop and waits for it to exit. It is safe to call
// even if Start was never run (or failed).
func (m *Monitor) Close() error {
	m.closeOnce.Do(func() { close(m.stopCh) })
	if m.started.Load() {
		<-m.doneCh
	}
	return nil
}

func (m *Monitor) loop(ctx context.Context) {
	defer close(m.doneCh)
	for {
		timer := time.NewTimer(m.nextDelay(time.Now()))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-m.stopCh:
			timer.Stop()
			return
		case <-timer.C:
			rctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			if _, err := m.Refresh(rctx); err != nil {
				m.log.Warn("universe refresh failed; keeping previous snapshot", "err", err)
			}
			cancel()
		}
	}
}

// nextDelay returns how long to wait for the next refresh. For sub-hour
// intervals it is just the interval (test mode); otherwise it is the time until
// the next 00:00 UTC + RefreshOffset.
func (m *Monitor) nextDelay(now time.Time) time.Duration {
	if m.cfg.refresh < time.Hour {
		return m.cfg.refresh
	}
	utc := now.UTC()
	midnight := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
	target := midnight.Add(m.cfg.offset)
	if !target.After(utc) {
		target = target.Add(24 * time.Hour)
	}
	return target.Sub(utc)
}

// Refresh polls exchangeInfo, rebuilds the snapshot, stores it, and notifies
// subscribers. On error the previous snapshot is left intact.
func (m *Monitor) Refresh(ctx context.Context) (Snapshot, error) {
	start := time.Now()
	symbols, err := m.fetch(ctx)
	m.metrics.refreshDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		m.metrics.refreshTotal.WithLabelValues("error").Inc()
		return Snapshot{}, err
	}
	m.metrics.refreshTotal.WithLabelValues("ok").Inc()

	snap := Snapshot{Symbols: symbols, RefreshedAt: time.Now().UTC()}
	set := make(map[string]struct{}, len(symbols))
	for _, s := range symbols {
		set[s] = struct{}{}
	}

	m.mu.Lock()
	prev := len(m.set)
	m.snap = snap
	m.set = set
	subs := make([]func(Snapshot), len(m.subs))
	copy(subs, m.subs)
	m.mu.Unlock()

	m.metrics.size.Set(float64(len(symbols)))
	m.metrics.lastRefresh.Set(float64(snap.RefreshedAt.Unix()))
	m.log.Info("universe refreshed", "symbols", len(symbols), "prev", prev)

	for _, fn := range subs {
		fn(snap)
	}
	return snap, nil
}

// Snapshot returns the most recent successful refresh (zero value before the first).
func (m *Monitor) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snap
}

// Size implements dispatcher.UniverseProvider.
func (m *Monitor) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.set)
}

// Has implements dispatcher.UniverseProvider.
func (m *Monitor) Has(symbol string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.set[symbol]
	return ok
}

// --- REST ---

type exchangeInfoResp struct {
	Symbols []struct {
		Symbol       string `json:"symbol"`
		ContractType string `json:"contractType"`
		Status       string `json:"status"`
		QuoteAsset   string `json:"quoteAsset"`
	} `json:"symbols"`
}

func (m *Monitor) fetch(ctx context.Context) ([]string, error) {
	var info exchangeInfoResp
	if err := m.getJSON(ctx, "/fapi/v1/exchangeInfo", &info); err != nil {
		return nil, fmt.Errorf("exchangeInfo: %w", err)
	}
	out := make([]string, 0, len(info.Symbols))
	for _, s := range info.Symbols {
		if _, ok := m.cfg.quoteAssets[s.QuoteAsset]; ok &&
			(m.cfg.contractType == "" || s.ContractType == m.cfg.contractType) &&
			(m.cfg.status == "" || s.Status == m.cfg.status) {
			out = append(out, s.Symbol)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (m *Monitor) getJSON(ctx context.Context, path string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.cfg.baseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := m.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d: %s", resp.StatusCode, truncate(body, 200))
	}
	return sonic.Unmarshal(body, v)
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n])
	}
	return string(b)
}
