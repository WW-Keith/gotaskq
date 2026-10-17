package observe

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every Prometheus instrument in GoTaskQ.
// Create once in main(), inject into Server and Worker.
type Metrics struct {
	registry *prometheus.Registry

	// JobsEnqueued counts jobs submitted via POST /jobs.
	// Labels: queue, type
	JobsEnqueued *prometheus.CounterVec

	// JobDuration measures time from dequeue to completion.
	// Labels: queue, type, status (succeeded|failed)
	// Buckets cover fast (100ms) to slow (5min) jobs.
	JobDuration *prometheus.HistogramVec

	// QueueDepth is the current number of pending jobs per queue.
	// Updated by the worker on each poll cycle.
	// This is the metric the HPA will use to autoscale in Cell 17.
	QueueDepth *prometheus.GaugeVec

	// WorkerActive is the number of goroutines currently running a job.
	WorkerActive *prometheus.GaugeVec

	// JobRetries counts jobs scheduled for retry.
	// Labels: queue, type
	JobRetries *prometheus.CounterVec

	// JobsDLQ counts jobs moved to the dead-letter queue.
	// Labels: queue, type
	JobsDLQ *prometheus.CounterVec
}

// New creates a process-local metrics registry.
func New() *Metrics {
	registry := prometheus.NewRegistry()
	registerer := promauto.With(registry)
	return &Metrics{
		registry: registry,

		JobsEnqueued: registerer.NewCounterVec(prometheus.CounterOpts{
			Name: "gotaskq_jobs_enqueued_total",
			Help: "Total number of jobs submitted to the queue.",
		}, []string{"queue", "type"}),

		JobDuration: registerer.NewHistogramVec(prometheus.HistogramOpts{
			Name: "gotaskq_job_duration_seconds",
			Help: "Time from dequeue to completion in seconds.",
			// Covers: 100ms, 500ms, 1s, 5s, 30s, 60s, 300s
			// Tune these to your actual job latency distribution.
			Buckets: []float64{0.1, 0.5, 1, 5, 30, 60, 300},
		}, []string{"queue", "type", "status"}),

		QueueDepth: registerer.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gotaskq_queue_depth",
			Help: "Current number of pending jobs per queue.",
		}, []string{"queue"}),

		WorkerActive: registerer.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gotaskq_worker_active",
			Help: "Number of goroutines currently executing a job.",
		}, []string{"queue"}),

		JobRetries: registerer.NewCounterVec(prometheus.CounterOpts{
			Name: "gotaskq_job_retries_total",
			Help: "Total number of jobs scheduled for retry.",
		}, []string{"queue", "type"}),

		JobsDLQ: registerer.NewCounterVec(prometheus.CounterOpts{
			Name: "gotaskq_jobs_dlq_total",
			Help: "Total number of jobs moved to the dead-letter queue.",
		}, []string{"queue", "type"}),
	}
}

// Handler exposes this process's metrics without registering them globally.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
