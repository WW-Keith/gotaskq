package broker

import (
	"context"
	//"github.com/WW-Keith/gotaskq/internal/broker"
)

// Broker is the message transport layer.
// It is separate from Store — the store persists job state,
// the broker handles delivery between server and workers.
//
// The broker carries job IDs only, never full payloads.
// Workers fetch the full job from the store using GetByID.
type Broker interface {
	// Publish sends a job ID to the given queue.
	// Called by the API server after Enqueue succeeds.
	Publish(ctx context.Context, queue, jobID string) error

	// Subscribe blocks until a message is available for this consumer.
	// Returns nil, nil when the block timeout elapses with no messages —
	// the caller should loop and call Subscribe again.
	Subscribe(ctx context.Context, queue, consumer string) (*Message, error)

	// Ack removes the message from the pending entry list.
	// Call this after the job has been successfully processed.
	Ack(ctx context.Context, queue, entryID string) error

	// EnsureGroup creates the consumer group if it does not exist.
	// Safe to call multiple times — idempotent.
	EnsureGroup(ctx context.Context, queue string) error
}

// Message is what Subscribe delivers to a consumer.
type Message struct {
	EntryID string // Redis stream entry ID — pass back to Ack
	JobID   string // your domain job ID — look up in store with GetByID
}
