// Package metrics defines the Prometheus metrics of the gateway.
// All metric names share the "opgate_" namespace.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

// Metrics holds every metric the gateway exposes.
type Metrics struct {
	reg *prometheus.Registry

	Requests *prometheus.CounterVec // label: kind

	SessionResolveHit      prometheus.Counter
	SessionResolveMiss     prometheus.Counter
	SessionClientSupplied  prometheus.Counter
	SessionStoreError      *prometheus.CounterVec // label: op
	StreamCompleted        prometheus.Counter
	StreamAborted          prometheus.Counter
	PageFlushSuccess       prometheus.Counter
	PageFlushError         prometheus.Counter
	FinalizeSuccess        prometheus.Counter
	FinalizeError          prometheus.Counter
	SessionMappingCreated  prometheus.Counter
	SessionMappingError    prometheus.Counter

	StreamDuration    prometheus.Histogram
	ResponseBytes     prometheus.Histogram
	PageCount         prometheus.Histogram
	RedisLatency      *prometheus.HistogramVec // label: op
	FinalizeDuration  prometheus.Histogram
}

// New builds and registers all metrics on a private registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{reg: reg}

	m.Requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "opgate_requests_total",
		Help: "Total number of proxied requests by kind (chat/passthrough).",
	}, []string{"kind"})

	m.SessionResolveHit = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "opgate_session_resolve_hit_total",
		Help: "Session lookups that hit an existing mapping.",
	})
	m.SessionResolveMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "opgate_session_resolve_miss_total",
		Help: "Session lookups that missed and generated a new session.",
	})
	m.SessionClientSupplied = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "opgate_session_client_supplied_total",
		Help: "Requests that supplied x-opencode-session themselves.",
	})
	m.SessionStoreError = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "opgate_session_store_error_total",
		Help: "Errors talking to the session store, by Redis operation.",
	}, []string{"op"})

	m.StreamCompleted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "opgate_stream_completed_total",
		Help: "Upstream response streams that reached EOF.",
	})
	m.StreamAborted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "opgate_stream_aborted_total",
		Help: "Response streams aborted before EOF (upstream error or client disconnect).",
	})

	m.PageFlushSuccess = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "opgate_page_flush_success_total",
		Help: "Temporary response pages successfully written to Redis.",
	})
	m.PageFlushError = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "opgate_page_flush_error_total",
		Help: "Temporary response page writes that failed.",
	})

	m.FinalizeSuccess = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "opgate_finalize_success_total",
		Help: "Finalizations that ended with a session mapping write.",
	})
	m.FinalizeError = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "opgate_finalize_error_total",
		Help: "Finalizations that failed; no session mapping was written.",
	})

	m.SessionMappingCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "opgate_session_mapping_created_total",
		Help: "stateHash -> sessionID mappings successfully written.",
	})
	m.SessionMappingError = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "opgate_session_mapping_error_total",
		Help: "Session mapping writes that failed.",
	})

	m.StreamDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "opgate_stream_duration_seconds",
		Help:    "Duration of upstream response streams.",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
	})
	m.ResponseBytes = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "opgate_response_bytes",
		Help:    "Size of collected upstream responses in bytes.",
		Buckets: []float64{1e4, 1e5, 1e6, 4e6, 1e7, 5e7, 1e8, 5e8, 1e9},
	})
	m.PageCount = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "opgate_page_count",
		Help:    "Number of Redis pages flushed per request.",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096},
	})
	m.RedisLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "opgate_redis_latency_seconds",
		Help:    "Latency of Redis operations.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"op"})
	m.FinalizeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "opgate_finalize_duration_seconds",
		Help:    "Duration of response finalization.",
		Buckets: []float64{0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
	})

	reg.MustRegister(
		m.Requests,
		m.SessionResolveHit, m.SessionResolveMiss, m.SessionClientSupplied,
		m.SessionStoreError,
		m.StreamCompleted, m.StreamAborted,
		m.PageFlushSuccess, m.PageFlushError,
		m.FinalizeSuccess, m.FinalizeError,
		m.SessionMappingCreated, m.SessionMappingError,
		m.StreamDuration, m.ResponseBytes, m.PageCount,
		m.RedisLatency, m.FinalizeDuration,
	)
	return m
}

// Handler returns the /metrics HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// SessionStoreErrorInc increments the session store error counter for op.
func (m *Metrics) SessionStoreErrorInc(op string) {
	m.SessionStoreError.WithLabelValues(op).Inc()
}
