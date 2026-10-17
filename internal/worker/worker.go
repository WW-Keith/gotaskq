package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/WW-Keith/gotaskq/internal/broker"
	"github.com/WW-Keith/gotaskq/internal/observe"
	"github.com/WW-Keith/gotaskq/internal/queue"
	"github.com/WW-Keith/gotaskq/internal/store"
)

const jobTimeout = 5 * time.Minute

// Worker subscribes to a queue via the broker and executes jobs.
type Worker struct {
	store    store.Store
	broker   broker.Broker
	executor *Executor
	dlq      *queue.DLQ
	policy   queue.Policy
	consumer string // unique name for this worker instance in the consumer group
	queue    string
	metrics  *observe.Metrics
	logger   *slog.Logger
}

// New creates a worker. consumer should be unique per process+goroutine.
func New(s store.Store, b broker.Broker, e *Executor, queueName, consumer string, metrics ...*observe.Metrics) *Worker {
	var m *observe.Metrics
	if len(metrics) > 0 {
		m = metrics[0]
	}
	return &Worker{
		store:    s,
		broker:   b,
		executor: e,
		dlq:      queue.NewDLQ(s),
		policy:   queue.Default,
		consumer: consumer,
		queue:    queueName,
		metrics:  m,
		logger:   slog.Default(),
	}
}

// Run blocks on Subscribe until ctx is cancelled.
// Each received message is processed synchronously before the next Subscribe.
// Run blocks on Subscribe until ctx is cancelled.
// On each iteration it picks a queue using weighted random selection
// so critical jobs are served more often without starving lower tiers.
func (w *Worker) Run(ctx context.Context) {
	w.logger.Info("worker started", "consumer", w.consumer)

	// Ensure consumer groups exist for all queues before reading.
	for _, q := range queue.All {
		if err := w.broker.EnsureGroup(ctx, q); err != nil {
			w.logger.Error("ensure group failed", "queue", q, "err", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("worker shutting down")
			return
		default:
		}

		// Pick queue by weight on every iteration.
		selectedQueue := queue.NextQueue()
		w.refreshQueueDepth(ctx, selectedQueue)

		msg, err := w.broker.Subscribe(ctx, selectedQueue, w.consumer)
		if err != nil {
			w.logger.Error("subscribe error",
				"queue", selectedQueue, "err", err)
			// Small backoff to avoid hammering Redis on repeated errors
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		if msg == nil {
			continue // timeout, nothing in this queue — try again
		}

		w.processMessage(ctx, selectedQueue, msg)
	}
}

func (w *Worker) processMessage(ctx context.Context, queueName string, msg *broker.Message) {
	job, err := w.store.Claim(ctx, msg.JobID)
	if err != nil {
		w.logger.Error("claim job failed",
			"job_id", msg.JobID, "err", err)
		_ = w.broker.Ack(ctx, queueName, msg.EntryID)
		return
	}
	if job == nil {
		if err := w.broker.Ack(ctx, queueName, msg.EntryID); err != nil {
			w.logger.Error("ack unclaimable job failed", "job_id", msg.JobID, "err", err)
		}
		return
	}
	w.logger.Info("executing job",
		"id", job.ID, "type", job.Type,
		"attempt", job.Attempts, "max_retry", job.MaxRetry)

	jobCtx, cancel := context.WithTimeout(ctx, jobTimeout)
	defer cancel()

	started := time.Now()
	if w.metrics != nil {
		w.metrics.WorkerActive.WithLabelValues(job.Queue).Inc()
		defer w.metrics.WorkerActive.WithLabelValues(job.Queue).Dec()
	}
	execErr := w.executor.Execute(jobCtx, job)
	if w.metrics != nil {
		status := "succeeded"
		if execErr != nil {
			status = "failed"
		}
		w.metrics.JobDuration.WithLabelValues(job.Queue, job.Type, status).Observe(time.Since(started).Seconds())
	}

	if execErr == nil {
		w.logger.Info("job succeeded", "id", job.ID)
		if err := w.store.Ack(ctx, job.ID); err != nil {
			w.logger.Error("persist job success failed", "id", job.ID, "err", err)
			return
		}
		w.ackMessage(ctx, queueName, msg.EntryID)
		return
	}

	// Job failed — decide retry or DLQ
	w.logger.Error("job failed",
		"id", job.ID, "attempt", job.Attempts,
		"max_retry", job.MaxRetry, "err", execErr)

	switch w.policy.Decide(job) {

	case queue.OutcomeRetry:
		retryAt := w.policy.NextRetryAt(job.Attempts)
		w.logger.Info("scheduling retry",
			"id", job.ID, "retry_at", retryAt)
		if err := w.store.ScheduleRetry(ctx, job.ID, execErr.Error(), retryAt); err != nil {
			w.logger.Error("schedule retry failed", "id", job.ID, "err", err)
			return
		}
		if w.metrics != nil {
			w.metrics.JobRetries.WithLabelValues(job.Queue, job.Type).Inc()
		}

	case queue.OutcomeDLQ:
		w.logger.Warn("job exhausted retries — moving to DLQ",
			"id", job.ID, "attempts", job.Attempts)
		if err := w.dlq.Move(ctx, job, execErr.Error()); err != nil {
			w.logger.Error("dlq move failed", "id", job.ID, "err", err)
			return
		}
		if w.metrics != nil {
			w.metrics.JobsDLQ.WithLabelValues(job.Queue, job.Type).Inc()
		}
	}
	w.ackMessage(ctx, queueName, msg.EntryID)
}

func (w *Worker) ackMessage(ctx context.Context, queueName, entryID string) {
	if err := w.broker.Ack(ctx, queueName, entryID); err != nil {
		w.logger.Error("broker ack failed", "entry_id", entryID, "err", err)
	}
}

func (w *Worker) refreshQueueDepth(ctx context.Context, queueName string) {
	if w.metrics == nil {
		return
	}
	counter, ok := w.store.(interface {
		PendingCount(context.Context, string) (int, error)
	})
	if !ok {
		return
	}
	count, err := counter.PendingCount(ctx, queueName)
	if err != nil {
		w.logger.Error("read queue depth failed", "queue", queueName, "err", err)
		return
	}
	w.metrics.QueueDepth.WithLabelValues(queueName).Set(float64(count))
}
