package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/WW-Keith/gotaskq/internal/model"
	_"github.com/lib/pq"
)

// helper: open a real store, skip test if DB unreachable
func newTestStore(t *testing.T) *PostgresStore {
	t.Helper()
	s, err := NewPostgresStore(testDSN())
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	return  s
}

// helper: clean up jobs created during a test
func cleanJob(t *testing.T, db *sql.DB, id string) {
	t.Helper()
	_, _ = db.Exec("DELETE FROM jobs WHERE id = $1", id)
}

func TestEnqueue_Basic(t *testing.T)  {
	s := newTestStore(t)
	ctx := context.Background()

	job := &model.Job {
		Type: "send email",
		Payload: json.RawMessage(`{"to":"keith@example.com"}`),
		Queue: "default",
	}

	if err := s.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue() failed: %v", err)
	}
	if job.ID == ""{
		t.Error("expected job id to be st after Enqueue, got empty string")
	}

	// Status must be pending
	if job.Status != model.StatusPending {
		t.Errorf("expected status %q, got %q", model.StatusPending, job.Status)
	}
	
	// Timestamp must be set and recent
	if job.CreatedAt.IsZero(){
		t.Error("expected created at to be set")
	}
	if time.Since(job.CreatedAt) > 5 * time.Second {
		t.Error("CreatedAt looks stale - expected it to be just now")
	}
	t.Logf("enqueued job id = %s", job.ID)
	cleanJob(t, s.db, job.ID)
}

func TestEnqueue_SetDefaults(t *testing.T){
	s := newTestStore(t)
	ctx := context.Background()

	// Deliberately leave Queue and MaxRetry empty
	job := &model.Job{
		Type: "resize image",
		Payload: json.RawMessage(`{"url":"http://example.com/img.png"}`),
	}
	
	if err := s.Enqueue(ctx,job); err != nil{
		t.Fatalf("enqueue() failed: %v", err)
	}

	if job.Queue != "default" {
		t.Errorf("expected default queue, got %q", job.Queue)
	}

	if job.MaxRetry != 3 {
		t.Errorf("expected max_retry to be 3, got %d", job.MaxRetry)
	}

	cleanJob(t, s.db, job.ID)
}

func TestEnqueue_PersistedToDatabase(t *testing.T){
	s := newTestStore(t)
	ctx := context.Background()

	job := &model.Job{
		Type: "payment",
		Payload: json.RawMessage(`{"amount":4000}`),
		Queue: "critical",
		Priority: model.PriorityCritical,
	}

	if err := s.Enqueue(ctx, job); err != nil{
		t.Fatalf("Enqueue() failed: %v", err)
	}

	// Read the row back directly - confirm it actually hit the DB
	var (
		gotType string
		gotQueue string
		gotStatus string
	)
	err := s.db.QueryRowContext(ctx,
		"SELECT type, queue, status FROM jobs WHERE id = $1", job.ID,
	).Scan(&gotType, &gotQueue, &gotStatus)
	if err != nil{
		t.Fatalf("read back failed: %v", err)
	}

	if gotType != "payment" {
		t.Errorf("type: got %q, want %q", gotType, "payment")
	}

	if gotQueue != "critical" {
		t.Errorf("queue: got %q, want %q", gotQueue, "critical")
	}

	if gotStatus != string(model.StatusPending){
		t.Errorf("status: got %q, want %q", gotStatus, model.StatusPending)
	}

	cleanJob(t, s.db, job.ID)
}

