package chwriter

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// metrics is the Prometheus surface for the batch writer. The names follow the
// design doc's Observability table (section 6).
type metrics struct {
	bufferSize   prometheus.Gauge // clickhouse_buffer_size
	flushLatency *prometheus.HistogramVec
	flushTotal   *prometheus.CounterVec
	rowsFlushed  prometheus.Counter
	rowsDropped  prometheus.Counter
	retries      prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	if reg == nil {
		// Keep metrics fully functional even when the caller does not want
		// them exposed; a private registry avoids duplicate-registration
		// panics between independent writers in tests.
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)
	return &metrics{
		bufferSize: f.NewGauge(prometheus.GaugeOpts{
			Name: "clickhouse_buffer_size",
			Help: "Kline rows currently buffered in memory awaiting a ClickHouse flush.",
		}),
		flushLatency: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "clickhouse_flush_latency_seconds",
			Help:    "Wall-clock latency of a single ClickHouse batch flush attempt.",
			Buckets: prometheus.DefBuckets,
		}, []string{"status"}),
		flushTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "clickhouse_flush_total",
			Help: "ClickHouse batch flush attempts, partitioned by outcome.",
		}, []string{"status"}),
		rowsFlushed: f.NewCounter(prometheus.CounterOpts{
			Name: "clickhouse_rows_flushed_total",
			Help: "Kline rows successfully written to ClickHouse.",
		}),
		rowsDropped: f.NewCounter(prometheus.CounterOpts{
			Name: "clickhouse_rows_dropped_total",
			Help: "Kline rows dropped after exhausting retries or on ingest-channel overflow.",
		}),
		retries: f.NewCounter(prometheus.CounterOpts{
			Name: "clickhouse_flush_retries_total",
			Help: "ClickHouse flush retry attempts.",
		}),
	}
}

func (m *metrics) observeFlushOK(seconds float64, rows int) {
	m.flushLatency.WithLabelValues("ok").Observe(seconds)
	m.flushTotal.WithLabelValues("ok").Inc()
	m.rowsFlushed.Add(float64(rows))
}

func (m *metrics) observeFlushErr(seconds float64) {
	m.flushLatency.WithLabelValues("error").Observe(seconds)
	m.flushTotal.WithLabelValues("error").Inc()
}
