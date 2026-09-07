package dispatcher

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HarvestStars/chomosyncer-go/internal/rediswin"
)

type sectionKey struct {
	interval string
	openTime int64
}

type sectionState struct {
	symbols   map[string]struct{}
	published bool
	timer     *time.Timer // completion-timeout timer, then reused as retention/evict timer
}

// aggregator tracks, per (interval, openTime), which universe symbols have
// reported a closed bar, and emits exactly one kline_ready per cross-section —
// either when every universe symbol is in, or when the timeout fallback fires.
type aggregator struct {
	ready     ReadyPublisher
	universe  UniverseProvider
	metrics   *metrics
	log       *slog.Logger
	closed    *atomic.Bool
	publishTO time.Duration // bound on the PublishKlineReady call itself

	timeout      time.Duration
	retention    time.Duration // 0 => derive from the interval
	defaultReten time.Duration

	mu       sync.Mutex
	sections map[sectionKey]*sectionState
}

func newAggregator(
	ready ReadyPublisher,
	universe UniverseProvider,
	m *metrics,
	log *slog.Logger,
	closed *atomic.Bool,
	timeout, retention, defaultRetention, publishTO time.Duration,
) *aggregator {
	return &aggregator{
		ready:        ready,
		universe:     universe,
		metrics:      m,
		log:          log,
		closed:       closed,
		publishTO:    publishTO,
		timeout:      timeout,
		retention:    retention,
		defaultReten: defaultRetention,
		sections:     make(map[sectionKey]*sectionState),
	}
}

// mark records that symbol's bar for (interval, openTime) has closed. It is
// safe for concurrent use by the closed-bar workers.
func (a *aggregator) mark(symbol, interval string, openTime int64) {
	if a.universe != nil && !a.universe.Has(symbol) {
		return // archived & windowed elsewhere, but not part of the cross-section
	}
	key := sectionKey{interval: interval, openTime: openTime}

	a.mu.Lock()
	st := a.sections[key]
	if st == nil {
		st = &sectionState{symbols: make(map[string]struct{})}
		st.timer = time.AfterFunc(a.timeout, func() { a.onTimeout(key) })
		a.sections[key] = st
		a.metrics.sectionPending.Set(float64(len(a.sections)))
	}
	if st.published {
		a.mu.Unlock()
		return
	}
	st.symbols[symbol] = struct{}{}
	n := len(st.symbols)

	want := 0
	if a.universe != nil {
		want = a.universe.Size()
	}
	complete := want > 0 && n >= want
	if complete {
		st.published = true
		st.timer.Stop()
	}
	a.mu.Unlock()

	if complete {
		a.publish(key, n, "complete")
	}
}

func (a *aggregator) onTimeout(key sectionKey) {
	a.mu.Lock()
	st := a.sections[key]
	if st == nil || st.published {
		a.mu.Unlock()
		return
	}
	st.published = true
	n := len(st.symbols)
	a.mu.Unlock()

	a.publish(key, n, "timeout")
}

func (a *aggregator) publish(key sectionKey, n int, reason string) {
	if a.closed.Load() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), a.publishTO)
	_, err := a.ready.PublishKlineReady(ctx, rediswin.KlineReadyEvent{
		Interval:     key.interval,
		Timestamp:    key.openTime,
		SymbolsCount: n,
	})
	cancel()

	if err != nil {
		a.log.Warn("publish kline_ready failed",
			"interval", key.interval, "open_time", key.openTime, "symbols", n, "reason", reason, "err", err)
		a.metrics.sectionErrors.Inc()
	} else {
		a.metrics.sectionPub.WithLabelValues(key.interval, reason).Inc()
		a.metrics.sectionSize.WithLabelValues(key.interval).Observe(float64(n))
		a.log.Debug("kline_ready published",
			"interval", key.interval, "open_time", key.openTime, "symbols", n, "reason", reason)
	}

	// Keep the key in a published state for one bar period so late stragglers
	// are swallowed instead of opening a fresh (double-publishing) section.
	a.mu.Lock()
	if st := a.sections[key]; st != nil {
		st.timer = time.AfterFunc(a.retentionFor(key.interval), func() { a.evict(key) })
	}
	a.mu.Unlock()
}

func (a *aggregator) retentionFor(interval string) time.Duration {
	if a.retention > 0 {
		return a.retention
	}
	if d, ok := parseIntervalDuration(interval); ok {
		return d
	}
	return a.defaultReten
}

func (a *aggregator) evict(key sectionKey) {
	a.mu.Lock()
	delete(a.sections, key)
	a.metrics.sectionPending.Set(float64(len(a.sections)))
	a.mu.Unlock()
}

// stop halts every pending timer so nothing fires after shutdown.
func (a *aggregator) stop() {
	a.mu.Lock()
	for _, st := range a.sections {
		if st.timer != nil {
			st.timer.Stop()
		}
	}
	a.mu.Unlock()
}

// parseIntervalDuration mirrors the Binance interval grammar. Kept local to the
// dispatcher so it does not depend on rediswin internals.
func parseIntervalDuration(interval string) (time.Duration, bool) {
	if len(interval) < 2 {
		return 0, false
	}
	unit := interval[len(interval)-1]
	n := 0
	for _, c := range interval[:len(interval)-1] {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	if n <= 0 {
		return 0, false
	}
	switch unit {
	case 's':
		return time.Duration(n) * time.Second, true
	case 'm':
		return time.Duration(n) * time.Minute, true
	case 'h':
		return time.Duration(n) * time.Hour, true
	case 'd':
		return time.Duration(n) * 24 * time.Hour, true
	case 'w':
		return time.Duration(n) * 7 * 24 * time.Hour, true
	case 'M':
		return time.Duration(n) * 30 * 24 * time.Hour, true
	default:
		return 0, false
	}
}
