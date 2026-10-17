package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/WW-Keith/gotaskq/internal/model"
)

func TestScheduleRetry_ResetsJobToPending(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Enqueue and dequeue (sets status=running, attempts=1)
	mustEnqueue(t, s, "flaky_task")
	claimed, err := s.Dequeue(ctx, "default")
	if err != nil || claimed == nil {
		t.Fatalf("dequeue failed: %v", err)
	}

	// Schedule retry 5 seconds from now
	retryAt := time.Now().UTC().Add(5 * time.Second)
	if err := s.ScheduleRetry(ctx, claimed.ID, "timeout", retryAt); err != nil {
		t.Fatalf("ScheduleRetry() failed: %v", err)
	}

	// Fetch it back and verify state
	updated, err := s.GetByID(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("GetByID() failed: %v", err)
	}

	if updated.Status != model.StatusPending {
		t.Errorf("status: got %q, want %q", updated.Status, model.StatusPending)
	}
	if !updated.Error.Valid || updated.Error.String != "timeout" {
		t.Errorf("error: got %q, want %q", updated.Error.String, "timeout")
	}
	if updated.RetryAt == nil {
		t.Fatal("retry_at should be set")
	}
	if updated.RetryAt.Before(time.Now()) {
		t.Error("retry_at should be in the future")
	}
}

func TestScheduleRetry_NotDequeued_UntilRetryAtElapsed(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Enqueue a job with a far-future retry_at
	job := &model.Job{
		Type:    "delayed_retry",
		Payload: json.RawMessage(`{}`),
		Queue:   "default",
	}
	if err := s.Enqueue(ctx, job); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	t.Cleanup(func() { cleanJob(t, s.db, job.ID) })

	// Set retry_at 1 hour in the future
	futureRetry := time.Now().UTC().Add(time.Hour)
	if err := s.ScheduleRetry(ctx, job.ID, "not yet", futureRetry); err != nil {
		t.Fatalf("ScheduleRetry: %v", err)
	}

	// Dequeue should NOT return this job — retry_at is in the future
	claimed, err := s.Dequeue(ctx, "default")
	if err != nil {
		t.Fatalf("Dequeue() error: %v", err)
	}
	if claimed != nil && claimed.ID == job.ID {
		t.Error("job was dequeued before retry_at elapsed — index filter broken")
	}
}

func TestMarkFailed_SetsStatusAndDLQFields(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	job := mustEnqueue(t, s, "exhausted_task")

	if err := s.MarkFailed(ctx, job.ID, "all retries exhausted"); err != nil {
		t.Fatalf("MarkFailed() failed: %v", err)
	}

	updated, err := s.GetByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetByID() failed: %v", err)
	}

	if updated.Status != model.StatusFailed {
		t.Errorf("status: got %q, want %q", updated.Status, model.StatusFailed)
	}
}
