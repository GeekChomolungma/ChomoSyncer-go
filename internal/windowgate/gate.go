// Package windowgate temporarily suppresses Redis closed-window writes (and the
// kline_ready events for that interval) for keys that the gapfill path is
// currently rebuilding from ClickHouse.
//
// It is a thin adapter layer: Gate.WrapWindow / Gate.WrapReady return
// dispatcher.WindowSink / dispatcher.ReadyPublisher implementations that the app
// wires into the dispatcher in place of the raw *rediswin.Writer. The dispatcher
// and rediswin themselves are unchanged.
//
// While a key is held, live closed bars for it still flow to ClickHouse (the
// archive sink is not gated), so the subsequent RebuildWindow picks them up.
package windowgate

import (
	"context"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/HarvestStars/chomosyncer-go/internal/dispatcher"
	"github.com/HarvestStars/chomosyncer-go/internal/rediswin"
)

// Key is a (symbol, interval). Symbol case is normalised on the way in.
type Key struct {
	Symbol   string
	Interval string
}

func norm(k Key) Key {
	return Key{Symbol: strings.ToUpper(k.Symbol), Interval: k.Interval}
}

// Gate tracks which keys / intervals are currently held. Ref-counted so
// overlapping Hold callers (cold-start + a concurrent shard reconnect) compose
// correctly.
type Gate struct {
	mu       sync.RWMutex
	keys     map[Key]int    // key -> hold count
	byInterv map[string]int // interval -> number of held keys

	metrics *gateMetrics
}

// New builds a Gate and registers its metrics.
func New(reg prometheus.Registerer) *Gate {
	g := &Gate{
		keys:     map[Key]int{},
		byInterv: map[string]int{},
		metrics:  newGateMetrics(reg),
	}
	g.metrics.registerHeldGauge(g.HeldCount)
	return g
}

// Hold marks keys as held (ref-count +1 each). Idempotent-safe to pair with an
// equal number of Release calls.
func (g *Gate) Hold(keys ...Key) {
	if len(keys) == 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, raw := range keys {
		k := norm(raw)
		if g.keys[k] == 0 {
			g.byInterv[k.Interval]++
		}
		g.keys[k]++
	}
}

// Release drops one hold from each key. Releasing a key that is not held is a
// no-op.
func (g *Gate) Release(keys ...Key) {
	if len(keys) == 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, raw := range keys {
		k := norm(raw)
		n := g.keys[k]
		if n <= 0 {
			continue
		}
		if n == 1 {
			delete(g.keys, k)
			if g.byInterv[k.Interval] <= 1 {
				delete(g.byInterv, k.Interval)
			} else {
				g.byInterv[k.Interval]--
			}
		} else {
			g.keys[k] = n - 1
		}
	}
}

// HeldWindow reports whether the given key's closed-window writes are suppressed.
func (g *Gate) HeldWindow(symbol, interval string) bool {
	k := norm(Key{Symbol: symbol, Interval: interval})
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.keys[k] > 0
}

// HeldInterval reports whether any key of the interval is held (=> kline_ready
// for that interval is suppressed).
func (g *Gate) HeldInterval(interval string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.byInterv[interval] > 0
}

// HeldCount is the number of distinct keys currently held.
func (g *Gate) HeldCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.keys)
}

// WrapWindow returns a WindowSink that drops pushes for held keys.
func (g *Gate) WrapWindow(inner dispatcher.WindowSink) dispatcher.WindowSink {
	return gatedWindow{g: g, inner: inner}
}

// WrapReady returns a ReadyPublisher that drops kline_ready for held intervals.
func (g *Gate) WrapReady(inner dispatcher.ReadyPublisher) dispatcher.ReadyPublisher {
	return gatedReady{g: g, inner: inner}
}

type gatedWindow struct {
	g     *Gate
	inner dispatcher.WindowSink
}

func (w gatedWindow) PushBarAndTrim(ctx context.Context, symbol, interval string, bar rediswin.CompactBar) error {
	if w.g.HeldWindow(symbol, interval) {
		w.g.metrics.windowSuppressed.WithLabelValues(interval).Inc()
		return nil
	}
	return w.inner.PushBarAndTrim(ctx, symbol, interval, bar)
}

type gatedReady struct {
	g     *Gate
	inner dispatcher.ReadyPublisher
}

func (r gatedReady) PublishKlineReady(ctx context.Context, evt rediswin.KlineReadyEvent) (string, error) {
	if r.g.HeldInterval(evt.Interval) {
		r.g.metrics.readySuppressed.WithLabelValues(evt.Interval).Inc()
		return "", nil
	}
	return r.inner.PublishKlineReady(ctx, evt)
}

// compile-time checks.
var (
	_ dispatcher.WindowSink     = gatedWindow{}
	_ dispatcher.ReadyPublisher = gatedReady{}
)

type gateMetrics struct {
	reg              prometheus.Registerer
	windowSuppressed *prometheus.CounterVec
	readySuppressed  *prometheus.CounterVec
}

func newGateMetrics(reg prometheus.Registerer) *gateMetrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)
	return &gateMetrics{
		reg: reg,
		windowSuppressed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "redis_window_gated_pushes_total",
			Help: "Live closed-window pushes dropped while the key was held by gapfill.",
		}, []string{"interval"}),
		readySuppressed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "dispatcher_kline_ready_suppressed_total",
			Help: "kline_ready events dropped while an interval had a gapfill-held key.",
		}, []string{"interval"}),
	}
}

func (m *gateMetrics) registerHeldGauge(count func() int) {
	promauto.With(m.reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "windowgate_held_keys",
		Help: "Distinct (symbol,interval) keys whose window writes are currently suppressed by gapfill.",
	}, func() float64 { return float64(count()) })
}
