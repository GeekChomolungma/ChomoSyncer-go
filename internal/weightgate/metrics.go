package weightgate

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	used         prometheus.Gauge       // last X-MBX-USED-WEIGHT-1M seen on any /fapi response
	acquired     *prometheus.CounterVec // {class} weight charged to the class budget
	wait         *prometheus.HistogramVec
	backpressure *prometheus.CounterVec // {class} waits caused by the used-weight threshold
	pauses       prometheus.Counter
	pausedUntil  prometheus.Gauge // unix seconds
}

func newMetrics(reg prometheus.Registerer) *metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)
	return &metrics{
		used: f.NewGauge(prometheus.GaugeOpts{
			Name: "weightgate_used_weight_1m",
			Help: "Most recent X-MBX-USED-WEIGHT-1M response header value seen on any /fapi request (Binance's own count of weight used in the current minute; limit 2400).",
		}),
		acquired: f.NewCounterVec(prometheus.CounterOpts{
			Name: "weightgate_weight_acquired_total",
			Help: "Request weight admitted by the gate, by class.",
		}, []string{"class"}),
		wait: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "weightgate_wait_seconds",
			Help:    "Time a request waited at the gate (pause, backpressure and token wait combined), by class.",
			Buckets: []float64{.001, .01, .05, .25, 1, 2.5, 5, 15, 30, 60},
		}, []string{"class"}),
		backpressure: f.NewCounterVec(prometheus.CounterOpts{
			Name: "weightgate_backpressure_waits_total",
			Help: "Times a request had to wait for the minute to end because Binance's reported used weight was above the class threshold.",
		}, []string{"class"}),
		pauses: f.NewCounter(prometheus.CounterOpts{
			Name: "weightgate_pauses_total",
			Help: "Global pauses started after a 429/418 response.",
		}),
		pausedUntil: f.NewGauge(prometheus.GaugeOpts{
			Name: "weightgate_paused_until_timestamp_seconds",
			Help: "Unix time until which all /fapi requests are paused after a 429/418 (0 = never paused; a past value = not paused now).",
		}),
	}
}
