package worker

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WW-Keith/gotaskq/internal/broker"
	"github.com/WW-Keith/gotaskq/internal/model"
	"github.com/WW-Keith/gotaskq/internal/store"
)

// fakeStore for worker tests — no real DB needed.
type fakeStore struct {
	jobs   []*model.Job
	acked  []string
	nacked []string
}

func (f *fakeStore) ScheduleRetry(_ context.Context, id, _ string, _ time.Time) error {
	return nil
}
func (f *fakeStore) MarkFailed(_ context.Context, _, _ string) error {
	return nil
}
func (f *fakeStore) MarkPublished(_ context.Context, _ string) error { return nil }
func (f *fakeStore) CreateSchedule(_ context.Context, _ *model.Schedule) error {
	return nil
}
func (f *fakeStore) DeleteSchedule(context.Context, string) error { return nil }
func (f *fakeStore) MaterializeDueSchedules(context.Context, time.Time, int) (int, error) {
	return 0, nil
}

func (f *fakeStore) Dequeue(_ context.Context, _ string) (*model.Job, error) {
	if len(f.jobs) == 0 {
		return nil, nil
	}
	job := f.jobs[0]
	f.jobs = f.jobs[1:]
	return job, nil
}
func (f *fakeStore) Claim(_ context.Context, _ string) (*model.Job, error) {
	if len(f.jobs) == 0 {
		return nil, nil
	}
	job := f.jobs[0]
	f.jobs = f.jobs[1:]
	return job, nil
}
func (f *fakeStore) Ack(_ context.Context, id string) error {
	f.acked = append(f.acked, id)
	return nil
}
func (f *fakeStore) Nack(_ context.Context, id, _ string) error {
	f.nacked = append(f.nacked, id)
	return nil
}
func (f *fakeStore) Enqueue(_ context.Context, _ *model.Job) error { return nil }
func (f *fakeStore) GetByID(_ context.Context, _ string) (*model.Job, error) {
	return nil, store.ErrNotFound
}
func (f *fakeStore) Cancel(_ context.Context, _ string) error { return nil }
func (f *fakeStore) Ping(_ context.Context) error             { return nil }
func (f *fakeStore) List(_ context.Context, _ model.Status, _, _ int) ([]*model.Job, error) {
	return nil, nil
}

type fakeBroker struct {
	messages []*broker.Message
	acked    []string
}

func (f *fakeBroker) Publish(context.Context, string, string) error { return nil }
func (f *fakeBroker) Subscribe(ctx context.Context, _, _ string) (*broker.Message, error) {
	if len(f.messages) > 0 {
		message := f.messages[0]
		f.messages = f.messages[1:]
		return message, nil
	}
	<-ctx.Done()
	return nil, nil
}
func (f *fakeBroker) Ack(_ context.Context, _, entryID string) error {
	f.acked = append(f.acked, entryID)
	return nil
}
func (f *fakeBroker) EnsureGroup(context.Context, string) error { return nil }

func TestWorker_ExecutesJob(t *testing.T) {
	fs := &fakeStore{
		jobs: []*model.Job{
			{
				ID:      "job-1",
				Type:    "send_email",
				Payload: json.RawMessage(`{"to":"keith@example.com"}`),
				Queue:   "default",
			},
		},
	}
	fb := &fakeBroker{messages: []*broker.Message{{EntryID: "entry-1", JobID: "job-1"}}}

	exec := NewExecutor()
	var executed atomic.Bool
	exec.Register("send_email", func(ctx context.Context, job *model.Job) error {
		executed.Store(true)
		return nil
	})

	w := New(fs, fb, exec, "default", "test")

	// Run the worker with a context that cancels after a short time.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	w.Run(ctx)

	if !executed.Load() {
		t.Error("expected handler to be called, it was not")
	}
	if len(fs.acked) != 1 || fs.acked[0] != "job-1" {
		t.Errorf("expected job-1 to be acked, got: %v", fs.acked)
	}
	if len(fb.acked) != 1 || fb.acked[0] != "entry-1" {
		t.Errorf("expected broker entry to be acked, got: %v", fb.acked)
	}
}

func TestWorker_NacksFailedJob(t *testing.T) {
	fs := &fakeStore{
		jobs: []*model.Job{
			{ID: "job-2", Type: "broken", Payload: json.RawMessage(`{}`), Queue: "default", Attempts: 3, MaxRetry: 3},
		},
	}
	fb := &fakeBroker{messages: []*broker.Message{{EntryID: "entry-2", JobID: "job-2"}}}

	exec := NewExecutor()
	exec.Register("broken", func(ctx context.Context, job *model.Job) error {
		return context.DeadlineExceeded // simulate any error
	})

	w := New(fs, fb, exec, "default", "test")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	w.Run(ctx)

	if len(fs.acked) != 0 {
		t.Errorf("expected no acks for failed job, got: %v", fs.acked)
	}
	if len(fb.acked) != 1 {
		t.Errorf("expected failed broker message to be acked, got: %v", fb.acked)
	}
}

func TestWorker_EmptyQueue(t *testing.T) {
	// No jobs in the store — worker should idle and exit cleanly on cancel.
	fs := &fakeStore{}

	exec := NewExecutor()
	fb := &fakeBroker{}
	w := New(fs, fb, exec, "default", "test")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Should return without hanging.
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
		// good — worker exited cleanly
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not exit after context cancellation")
	}
}

func TestWorker_GracefulShutdown(t *testing.T) {
	// Job takes 150ms. Context cancels after 50ms.
	// Worker must finish the job before returning.
	fs := &fakeStore{
		jobs: []*model.Job{
			{ID: "job-3", Type: "slow", Payload: json.RawMessage(`{}`), Queue: "default"},
		},
	}
	fb := &fakeBroker{messages: []*broker.Message{{EntryID: "entry-3", JobID: "job-3"}}}

	exec := NewExecutor()
	var finished atomic.Bool
	exec.Register("slow", func(ctx context.Context, job *model.Job) error {
		time.Sleep(150 * time.Millisecond)
		finished.Store(true)
		return nil
	})

	w := New(fs, fb, exec, "default", "test")

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	// Cancel while job is mid-execution
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Wait for worker to finish
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not exit after context cancellation")
	}

	// Job must have completed despite the cancellation
	if !finished.Load() {
		t.Error("expected job to finish before worker exited")
	}
	if len(fs.acked) != 1 {
		t.Errorf("expected job acked, got acks: %v", fs.acked)
	}
}
