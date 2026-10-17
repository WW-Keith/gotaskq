package worker

import (
	"context"
	"fmt"

	"github.com/WW-Keith/gotaskq/internal/model"
)

// HandlerFunc is the signature every job handler must implement.
// Return nil for success, an error for failure.
type HandlerFunc func(ctx context.Context, job *model.Job) error

// Executor holds the registry of job type → handler function.
// Register handlers at startup; Execute is called per job.
type Executor struct {
	handlers map[string]HandlerFunc
}

// NewExecutor creates an empty executor.
func NewExecutor() *Executor {
	return &Executor{
		handlers: make(map[string]HandlerFunc),
	}
}

// Register maps a job type name to a handler function.
// Call this at startup before the worker loop begins.
// Registering the same type twice overwrites the previous handler.
func (e *Executor) Register(jobType string, h HandlerFunc) {
	e.handlers[jobType] = h
}

// Execute runs the handler for the given job.
// If the handler panics, Execute catches it and returns it as an error —
// a panicking handler must never crash the entire worker process.
func (e *Executor) Execute(ctx context.Context, job *model.Job) (err error) {
	// Recover from panics in handlers.
	// Named return 'err' lets the deferred function set the return value.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()

	h, ok := e.handlers[job.Type]
	if !ok {
		return fmt.Errorf("no handler registered for job type %q", job.Type)
	}

	return h(ctx, job)
}