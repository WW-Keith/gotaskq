package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/WW-Keith/gotaskq/internal/broker"
	"github.com/WW-Keith/gotaskq/internal/model"
)

const (
	defaultInterval = time.Second
	dispatchBatch   = 100
)

type store interface {
	DueScheduledJobs(context.Context, time.Time, int) ([]*model.Job, error)
	MarkPublished(context.Context, string) error
	MaterializeDueSchedules(context.Context, time.Time, int) (int, error)
}

// Scheduler publishes delayed and retried jobs when they become eligible.
type Scheduler struct {
	store    store
	broker   broker.Broker
	interval time.Duration
	logger   *slog.Logger
}

func New(s store, b broker.Broker, interval time.Duration) *Scheduler {
	if interval <= 0 {
		interval = defaultInterval
	}
	return &Scheduler{
		store:    s,
		broker:   b,
		interval: interval,
		logger:   slog.Default(),
	}
}

// Run dispatches due jobs immediately, then waits for each subsequent tick.
func (s *Scheduler) Run(ctx context.Context) error {
	if err := s.dispatch(ctx); err != nil && ctx.Err() == nil {
		s.logger.Error("scheduled job dispatch failed", "err", err)
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.dispatch(ctx); err != nil && ctx.Err() == nil {
				s.logger.Error("scheduled job dispatch failed", "err", err)
			}
		}
	}
}

func (s *Scheduler) dispatch(ctx context.Context) error {
	if _, err := s.store.MaterializeDueSchedules(ctx, time.Now().UTC(), dispatchBatch); err != nil {
		return fmt.Errorf("materialize cron schedules: %w", err)
	}
	jobs, err := s.store.DueScheduledJobs(ctx, time.Now().UTC(), dispatchBatch)
	if err != nil {
		return fmt.Errorf("load due jobs: %w", err)
	}

	for _, job := range jobs {
		if err := s.broker.Publish(ctx, job.Queue, job.ID); err != nil {
			return fmt.Errorf("publish scheduled job %s: %w", job.ID, err)
		}
		if err := s.store.MarkPublished(ctx, job.ID); err != nil {
			return fmt.Errorf("mark scheduled job %s published: %w", job.ID, err)
		}
	}
	return nil
}
