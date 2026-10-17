package store

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/WW-Keith/gotaskq/internal/model"
)

// helper: enqueue a job and return it - reduces test boilerplate
func mustEnqueue(t *testing.T, s *PostgresStore, jobType string) *model.Job {
	t.Helper()
	job := &model.Job{
		Type:    jobType,
		Payload: json.RawMessage(`{"test":true}`),
		Queue:   "default",
	}
	if err := s.Enqueue(context.Background(), job); err != nil {
		t.Fatalf("must enqueue: %v", err)
	}
	t.Cleanup(func() { cleanJob(t, s.db, job.ID) })
	return job
}

func TestDequeue_ClaimsJob(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	original := mustEnqueue(t, s, "send_email")

	claimed, err := s.Dequeue(ctx, "default")
	if err != nil {
		t.Fatalf("Dequeue() error: %v", err)
	}
	if claimed == nil {
		t.Fatalf("Dequeue() returned nil - expected a job")
	}
	if claimed.ID != original.ID {
		t.Errorf("got job %q, want %q", claimed.ID, original.ID)
	}
	if claimed.Status != model.StatusRunning {
		t.Errorf("status got %q, want %q", claimed.Status, model.StatusRunning)
	}
	if claimed.Attempts != 1 {
		t.Errorf("attempts: got %d, want 1", claimed.Attempts)
	}
	if claimed.StartedAt == nil {
		t.Error("started_at should be set after dequeue")
	}
}

func TestDeque_EmptyQueue(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Use a queue name that definitely has no jobs
	job, err := s.Dequeue(ctx, "empty-queue-query-xyz")
	if err != nil {
		t.Fatalf("Dequeue() on empty queue returned an error: %v", err)
	}
	if job != nil {
		t.Fatalf("expected nil job on empty queue, got %+v", job)
	}
}

func TestDequeue_RespectsQueue(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Enqueue into critical - should not be dequeued from default
	criticalJob := &model.Job{
		Type:    "payment",
		Payload: json.RawMessage(`{}`),
		Queue:   "critical",
	}
	if err := s.Enqueue(ctx, criticalJob); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	t.Cleanup(func() { cleanJob(t, s.db, criticalJob.ID) })

	// Dequeue from default should get nothing
	job, err := s.Dequeue(ctx, "default")
	if err != nil {
		t.Fatalf("dequeue() error: %v", err)
	}
	if job != nil && job.ID == criticalJob.ID {
		t.Error("default from wrong queue")
	}
}

func TestDequeue_SkipLocked(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Enqueue exactly two jobs
	job1 := mustEnqueue(t, s, "task_a")
	job2 := mustEnqueue(t, s, "task_b")

	// Dequeue both - each call should return a different job
	claimed1, err := s.Dequeue(ctx, "default")
	if err != nil || claimed1 == nil {
		t.Fatalf("first dequeue failed: %v", err)
	}
	claimed2, err := s.Dequeue(ctx, "default")
	if err != nil || claimed2 == nil {
		t.Fatalf("second dequeue failed: %v", err)
	}
	if claimed1.ID == claimed2.ID {
		t.Errorf("both dequeue returned the same job: %s", claimed1.ID)
	}

	// Both original IDs should be claimed
	claimedIDs := map[string]bool{claimed1.ID: true, claimed2.ID: true}
	if !claimedIDs[job1.ID] || !claimedIDs[job2.ID] {
		t.Errorf("unexpectedIDs: got %v, want %v and %v", claimedIDs, job1.ID, job2.ID)
	}
}

func TestDequeue_ConcurrentWorkers(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const numJobs = 10

	// Enqueue 10 jobs
	for i := 0; i < numJobs; i++ {
		job := mustEnqueue(t, s, "concurrent_task")
		_ = job
	}

	// 5 goroutines all dequeue simultaneously
	var (
		mu      sync.Mutex
		claimed []string
		wg      sync.WaitGroup
	)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, err := s.Dequeue(ctx, "default")
			if err != nil {
				t.Errorf("concurrent Dequeue() error: %v", err)
				return
			}
			if job != nil {
				mu.Lock()
				claimed = append(claimed, job.ID)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// Verify no duplicates - each job claimed by exactly one worker
	seen := map[string]bool{}
	for _, id := range claimed {
		if seen[id] {
			t.Errorf("duplicate claim: job %s claimed twice", id)
		}
		seen[id] = true
	}
	t.Logf("5 concurrent workers claimed %d unique jobs", len(claimed))
}

func TestAck_MarksSucceeded(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	job := mustEnqueue(t, s, "send_email")

	// Must dequeue first only running jobs can be acked
	claimed, err := s.Dequeue(ctx, "default")
	if err != nil {
		t.Fatalf("Dequeue() error: %v", err)
	}
	if claimed == nil {
		t.Fatal("nothing to ack")
	}

	if err := s.Ack(ctx, claimed.ID); err != nil {
		t.Fatalf("Ack() failed: %v", err)
	}

	// Read it back and confirm status
	updated, err := s.GetByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetByID() failed: %v", err)
	}

	if updated.Status != model.StatusSucceeded {
		t.Errorf("status: got %q want %q", updated.Status, model.StatusSucceeded)
	}

	if updated.DoneAt == nil {
		t.Error("done_at should be set after Ack")
	}
}

func TestNack_MarksFailed(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	job := mustEnqueue(t, s, "broken_task")
	claimed, err := s.Dequeue(ctx, "default")
	if err != nil {
		t.Fatalf("Dequeue() error: %v", err)
	}
	if claimed == nil {
		t.Fatal("nothing to nack")
	}

	if err := s.Nack(ctx, claimed.ID, "connection refused"); err != nil {
		t.Fatalf("Nack() failed: %v", err)
	}

	updated, err := s.GetByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetByID() failed: %v", err)
	}

	if updated.Status != model.StatusFailed {
		t.Errorf("status: got %q, want %q", updated.Status, model.StatusFailed)
	}

	if !updated.Error.Valid || updated.Error.String != "connection refused" {
		t.Errorf("error: got %q, want %q", updated.Error.String, "connection refused")
	}
}

func TestGetByID_NotFound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, err := s.GetByID(ctx, "does-not-exist")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestDequeue_PriorityOrder(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Enqueue a low-priority job first
	low := &model.Job{
		Type:     "report",
		Payload:  json.RawMessage(`{}`),
		Queue:    "default",
		Priority: model.PriorityLow,
	}
	if err := s.Enqueue(ctx, low); err != nil {
		t.Fatalf("enqueue low: %v", err)
	}
	t.Cleanup(func() { cleanJob(t, s.db, low.ID) })

	// Enqueue a critical-priority job second
	crit := &model.Job{
		Type:     "payment",
		Payload:  json.RawMessage(`{}`),
		Queue:    "default",
		Priority: model.PriorityCritical,
	}
	if err := s.Enqueue(ctx, crit); err != nil {
		t.Fatalf("enqueue critical: %v", err)
	}
	t.Cleanup(func() { cleanJob(t, s.db, crit.ID) })

	// Dequeue should return the critical job first
	// (lower Priority int = higher urgency, ORDER BY priority ASC)
	claimed, err := s.Dequeue(ctx, "default")
	if err != nil || claimed == nil {
		t.Fatalf("dequeue: %v", err)
	}

	if claimed.ID != crit.ID {
		t.Errorf("expected critical job first, got type=%q priority=%d",
			claimed.Type, claimed.Priority)
	}
	t.Logf("correctly dequeued %q (priority=%d) before %q (priority=%d)",
		claimed.Type, claimed.Priority, low.Type, low.Priority)
}