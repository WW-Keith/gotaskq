package model

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// Status is the lifecycle state of a job.
// Using a typed string prevents raw string comparisons.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

// Priority controls dequeue order. Lower int = higher urgency.
type Priority int8

const (
	PriorityCritical Priority = 0
	PriorityDefault  Priority = 5
	PriorityLow      Priority = 10
)

// Job is the core domain object. Every operation in the system
// creates, reads, or transitions a Job.
type Job struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`    // handler name: "send_email", "resize_image"
	Payload   json.RawMessage `json:"payload"` // arbitrary JSON, handler decodes it
	Status    Status          `json:"status"`
	Priority  Priority        `json:"priority"`
	Queue     string          `json:"queue"` // "default", "critical", "low"
	Attempts  int             `json:"attempts"`
	MaxRetry  int             `json:"max_retry"`
	Error     sql.NullString  `json:"error"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
	RunAt     *time.Time      `json:"run_at,omitempty"` // nil = run immediately
	RetryAt   *time.Time      `json:"retry_at,omitempty"`
	StartedAt *time.Time      `json:"started_at,omitempty"`
	DoneAt    *time.Time      `json:"done_at,omitempty"`

}

// Valid returns an error description if the job is missing required fields
func (j *Job) Valid() error {
	if j.Type == "" {
		return fmt.Errorf("job type is required")
	}
	if len(j.Payload) == 0 {
		return fmt.Errorf("payload is required")
	}
	return nil
}

// IsFinal returns true if the job has reached a terminal state.
// and will never be executed again
func (j *Job) IsFinal() bool {
	return j.Status == StatusSucceeded ||
		j.Status == StatusFailed ||
		j.Status == StatusCancelled
}
