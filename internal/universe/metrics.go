package universe

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	refreshTotal    *prometheus.CounterVec // {status}
	refreshDuration prometheus.Histogram
	size            prometheus.Gauge
	lastRefresh     prometheus.Gauge // unix seconds
}

func newMetrics(reg prometheus.Registerer) *metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)
	return &metrics{
		refreshTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "universe_refresh_total",
			Help: "Universe refresh attempts partitioned by outcome.",
		}, []string{"status"}),
		refreshDuration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "universe_refresh_duration_seconds",
			Help:    "Wall-clock duration of a universe refresh (exchangeInfo call).",
			Buckets: []float64{.05, .1, .25, .5, 1, 2, 5, 10},
		}),
		size: f.NewGauge(prometheus.GaugeOpts{
			Name: "universe_size",
			Help: "Tradable USDT-margined PERPETUAL symbols (whole market).",
		}),
		lastRefresh: f.NewGauge(prometheus.GaugeOpts{
			Name: "universe_last_refresh_timestamp_seconds",
			Help: "Unix time of the last successful universe refresh.",
		}),
	}
}
