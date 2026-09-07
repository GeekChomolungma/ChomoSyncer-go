package rediswin

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// metrics is the Prometheus surface for the sliding-window writer. The
// histogram name follows the design doc's Observability table (section 6):
// redis_pipeline_latency_seconds.
type metrics struct {
	pipelineLatency *prometheus.HistogramVec // {op,status}
	barsPushed      prometheus.Counter
	klineReady      prometheus.Counter
	errors          *prometheus.CounterVec // {op}
}

func newMetrics(reg prometheus.Registerer) *metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)
	return &metrics{
		pipelineLatency: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "redis_pipeline_latency_seconds",
			Help:    "Round-trip latency of a Redis sliding-window pipeline or stream write.",
			Buckets: []float64{.0002, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"op", "status"}),
		barsPushed: f.NewCounter(prometheus.CounterOpts{
			Name: "redis_bars_pushed_total",
			Help: "Closed bars LPUSH-ed into a rolling window.",
		}),
		klineReady: f.NewCounter(prometheus.CounterOpts{
			Name: "redis_kline_ready_published_total",
			Help: "kline_ready cross-section events XADD-ed to the notification stream.",
		}),
		errors: f.NewCounterVec(prometheus.CounterOpts{
			Name: "redis_pipeline_errors_total",
			Help: "Redis sliding-window / stream write failures.",
		}, []string{"op"}),
	}
}

func (m *metrics) observe(op string, seconds float64, err error) {
	status := "ok"
	if err != nil {
		status = "error"
		m.errors.WithLabelValues(op).Inc()
	}
	m.pipelineLatency.WithLabelValues(op, status).Observe(seconds)
}
