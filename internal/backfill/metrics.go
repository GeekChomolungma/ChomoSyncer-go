package backfill

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	requests       *prometheus.CounterVec   // {reason}
	dropped        prometheus.Counter       // queue full
	barsFetched    *prometheus.CounterVec   // {interval}
	barsWritten    *prometheus.CounterVec   // {interval}
	windowsRebuilt *prometheus.CounterVec   // {interval}
	errors         *prometheus.CounterVec   // {stage}
	duration       *prometheus.HistogramVec // {reason}
	coldStartReady prometheus.GaugeFunc
}

func newMetrics(reg prometheus.Registerer, ready func() bool) *metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)
	m := &metrics{
		requests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "backfill_requests_total", Help: "Backfill requests by trigger reason.",
		}, []string{"reason"}),
		dropped: f.NewCounter(prometheus.CounterOpts{
			Name: "backfill_requests_dropped_total", Help: "Backfill requests dropped because the queue was full.",
		}),
		barsFetched: f.NewCounterVec(prometheus.CounterOpts{
			Name: "backfill_bars_fetched_total", Help: "Closed bars fetched from the Binance REST /klines endpoint.",
		}, []string{"interval"}),
		barsWritten: f.NewCounterVec(prometheus.CounterOpts{
			Name: "backfill_bars_written_total", Help: "Backfilled bars pushed to the ClickHouse archive.",
		}, []string{"interval"}),
		windowsRebuilt: f.NewCounterVec(prometheus.CounterOpts{
			Name: "backfill_windows_rebuilt_total", Help: "Redis rolling windows rebuilt from ClickHouse.",
		}, []string{"interval"}),
		errors: f.NewCounterVec(prometheus.CounterOpts{
			Name: "backfill_errors_total", Help: "Backfill errors by stage (rest|archive|ch_max|ch_read|rebuild).",
		}, []string{"stage"}),
		duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "backfill_duration_seconds",
			Help:    "End-to-end duration of a backfill request.",
			Buckets: []float64{.5, 1, 2, 5, 10, 30, 60, 120, 300},
		}, []string{"reason"}),
	}
	m.coldStartReady = f.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "backfill_ready", Help: "1 once the cold-start backfill has completed.",
	}, func() float64 {
		if ready() {
			return 1
		}
		return 0
	})
	return m
}
