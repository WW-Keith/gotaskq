package store

import (
	"context"
	"errors"
	"time"

	"github.com/WW-Keith/gotaskq/internal/model"
)

//Sentimental error checks against these with errors.Is().
//never by comparing error strings

var (
	ErrNotFound       = errors.New("job not found")
	ErrorCannotCancel = errors.New("job cannot be cancelled in its current state")
	ErrConflict       = ErrorCannotCancel
)

//Store is the persistence interface. The rest of the codebase depends on this interface.
//This makes testing straight forwar - swap in a fake, no DB needed

type Store interface {
	//Enqueue persists a new job with status pending
	Enqueue(ctx context.Context, job *model.Job) error

	//Dequeue automatically claims the next available job for the queue
	//Returns nil when queue is empty not an error
	Dequeue(ctx context.Context, queue string) (*model.Job, error)

	// Claim atomically claims a specific pending job delivered by the broker.
	Claim(ctx context.Context, id string) (*model.Job, error)

	//Ack marks job as succeeded
	Ack(ctx context.Context, id string) error

	//Nack marks a job failed with an error message
	Nack(ctx context.Context, id, errMsg string) error

	//GetByID retrieves a single job by ID
	//Returns ErrNotFound if the job does not exist
	GetByID(ctx context.Context, id string) (*model.Job, error)

	//List returns a job filtered by status. Pass an empty string for all statuses
	List(ctx context.Context, status model.Status, limit, offset int) ([]*model.Job, error)

	//Cancel marks a pending job cancelled
	//Returns ErrCannotCancel if job is already running or done
	Cancel(ctx context.Context, id string) error

	//Ping verifies the database connection is alive
	Ping(ctx context.Context) error

	// ScheduleRetry resets a failed job to pending with a future retry_at.
	ScheduleRetry(ctx context.Context, id, errMsg string, retryAt time.Time) error

	// MarkFailed permanently fails a job and records the DLQ reason.
	MarkFailed(ctx context.Context, id, reason string) error

	// MarkPublished records that the broker accepted a job for delivery.
	MarkPublished(ctx context.Context, id string) error

	CreateSchedule(ctx context.Context, schedule *model.Schedule) error
	DeleteSchedule(ctx context.Context, id string) error
	MaterializeDueSchedules(ctx context.Context, now time.Time, limit int) (int, error)
}
