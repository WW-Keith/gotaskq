package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/WW-Keith/gotaskq/internal/model"
)

func TestExecutor_Success(t *testing.T) {
	e := NewExecutor()

	called := false
	e.Register("send_email", func(ctx context.Context, job *model.Job) error {
		called = true
		return nil
	})

	job := &model.Job{ID: "1", Type: "send_email"}
	if err := e.Execute(context.Background(), job); err != nil {
		t.Fatalf("Execute() unexpected error: %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
}

func TestExecutor_HandlerError(t *testing.T) {
	e := NewExecutor()

	boom := errors.New("something went wrong")
	e.Register("broken_job", func(ctx context.Context, job *model.Job) error {
		return boom
	})

	job := &model.Job{ID: "2", Type: "broken_job"}
	err := e.Execute(context.Background(), job)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, boom) {
		t.Errorf("got %v, want %v", err, boom)
	}
}

func TestExecutor_UnknownType(t *testing.T) {
	e := NewExecutor()

	job := &model.Job{ID: "3", Type: "does_not_exist"}
	err := e.Execute(context.Background(), job)
	if err == nil {
		t.Fatal("expected error for unknown type, got nil")
	}
	t.Logf("got expected error: %v", err)
}

func TestExecutor_PanicRecovery(t *testing.T) {
	e := NewExecutor()

	e.Register("panic_job", func(ctx context.Context, job *model.Job) error {
		panic("handler fell over")
	})

	job := &model.Job{ID: "4", Type: "panic_job"}
	err := e.Execute(context.Background(), job)

	// Panic must be caught — must not propagate up and crash the worker.
	if err == nil {
		t.Fatal("expected error from panic recovery, got nil")
	}
	t.Logf("panic caught as error: %v", err)
}

func TestExecutor_ContextCancellation(t *testing.T) {
	e := NewExecutor()

	e.Register("slow_job", func(ctx context.Context, job *model.Job) error {
		// A well-behaved handler checks ctx.Done() and returns early.
		<-ctx.Done()
		return ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	job := &model.Job{ID: "5", Type: "slow_job"}
	err := e.Execute(ctx, job)
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want context.Canceled", err)
	}
}