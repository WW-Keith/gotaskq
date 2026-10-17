package queue

import (
	"context"
	"fmt"

	"github.com/WW-Keith/gotaskq/internal/model"
	"github.com/WW-Keith/gotaskq/internal/store"
)

// DLQ manages jobs that have exhausted all retry attempts.
type DLQ struct {
	store store.Store
}

// NewDLQ creates a dead-letter queue manager.
func NewDLQ(s store.Store) *DLQ {
	return &DLQ{store: s}
}

// Move permanently fails the job and records why it was sent to the DLQ.
func (d *DLQ) Move(ctx context.Context, job *model.Job, reason string) error {
	if err := d.store.MarkFailed(ctx, job.ID, reason); err != nil {
		return fmt.Errorf("move to dlq: %w", err)
	}
	return nil
}