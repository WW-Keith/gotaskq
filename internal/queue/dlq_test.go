package queue

import (
	"context"
	"testing"
	"time"

	"github.com/WW-Keith/gotaskq/internal/model"
	"github.com/WW-Keith/gotaskq/internal/store"
)

// fakeStore for DLQ tests
type fakeDLQStore struct {
	failedID     string
	failedReason string
}

func (f *fakeDLQStore) MarkFailed(_ context.Context, id, reason string) error {
	f.failedID = id
	f.failedReason = reason
	return nil
}
func (f *fakeDLQStore) MarkPublished(_ context.Context, _ string) error { return nil }
func (f *fakeDLQStore) CreateSchedule(_ context.Context, _ *model.Schedule) error {
	return nil
}
func (f *fakeDLQStore) DeleteSchedule(context.Context, string) error { return nil }
func (f *fakeDLQStore) MaterializeDueSchedules(context.Context, time.Time, int) (int, error) {
	return 0, nil
}

// unused Store methods
func (f *fakeDLQStore) Enqueue(_ context.Context, _ *model.Job) error           { return nil }
func (f *fakeDLQStore) Dequeue(_ context.Context, _ string) (*model.Job, error) { return nil, nil }
func (f *fakeDLQStore) Claim(_ context.Context, _ string) (*model.Job, error) {
	return nil, nil
}
func (f *fakeDLQStore) Ack(_ context.Context, _ string) error     { return nil }
func (f *fakeDLQStore) Nack(_ context.Context, _, _ string) error { return nil }
func (f *fakeDLQStore) GetByID(_ context.Context, _ string) (*model.Job, error) {
	return nil, store.ErrNotFound
}
func (f *fakeDLQStore) List(_ context.Context, _ model.Status, _, _ int) ([]*model.Job, error) {
	return nil, nil
}
func (f *fakeDLQStore) Cancel(_ context.Context, _ string) error { return nil }
func (f *fakeDLQStore) Ping(_ context.Context) error             { return nil }
func (f *fakeDLQStore) ScheduleRetry(_ context.Context, _, _ string, _ time.Time) error {
	return nil
}

func TestDLQ_Move(t *testing.T) {
	fs := &fakeDLQStore{}
	dlq := NewDLQ(fs)

	job := &model.Job{ID: "job-dead", Type: "broken_task"}
	reason := "connection refused after 3 attempts"

	if err := dlq.Move(context.Background(), job, reason); err != nil {
		t.Fatalf("Move() failed: %v", err)
	}

	if fs.failedID != "job-dead" {
		t.Errorf("failedID: got %q, want %q", fs.failedID, "job-dead")
	}
	if fs.failedReason != reason {
		t.Errorf("failedReason: got %q, want %q", fs.failedReason, reason)
	}
}
