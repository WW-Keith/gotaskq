package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// streamPrefix is prepended to every queue name.
	// "default" becomes "gotaskq:default" in Redis.
	streamPrefix = "gotaskq:"

	// groupName is the consumer group all workers join.
	groupName = "workers"

	// blockTimeout is how long Subscribe waits before returning nil.
	// Must be less than the Redis client's ReadTimeout.
	blockTimeout = 2 * time.Second
)

// RedisBroker implements Broker using Redis Streams.
type RedisBroker struct {
	client *redis.Client
}

// NewRedisBroker connects to Redis and returns a ready broker.
// addr example: "localhost:6379"
func NewRedisBroker(addr string) (*RedisBroker, error) {
	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		PoolSize:    20,
		DialTimeout: 5 * time.Second,
		// ReadTimeout must exceed blockTimeout or the client errors
		// before Redis can respond to the blocking XREADGROUP call.
		ReadTimeout: blockTimeout + time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &RedisBroker{client: client}, nil
}

// EnsureGroup creates the consumer group for the given queue.
// Uses MKSTREAM so the stream is created if it doesn't exist yet.
// Starting at "0" preserves messages published before the first worker starts.
func (b *RedisBroker) EnsureGroup(ctx context.Context, queue string) error {
	key := streamPrefix + queue
	err := b.client.XGroupCreateMkStream(ctx, key, groupName, "0").Err()
	if err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists" {
		// Group already exists — that's fine, this call is idempotent.
		return nil
	}
	return err
}

// Publish adds a job ID to the Redis stream for the given queue.
// The stream entry contains one field: "job_id".
// Called by the API server after persisting the job to Postgres.
func (b *RedisBroker) Publish(ctx context.Context, queue, jobID string) error {
	key := streamPrefix + queue
	return b.client.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		ID:     "*", // auto-generate the entry ID
		Values: map[string]any{
			"job_id": jobID,
		},
	}).Err()
}

// Subscribe blocks up to blockTimeout waiting for a message.
// Returns nil, nil when the timeout elapses with no messages.
// The consumer name should be unique per goroutine — used by Redis
// to track which messages are pending for which consumer.
func (b *RedisBroker) Subscribe(ctx context.Context, queue, consumer string) (*Message, error) {
	key := streamPrefix + queue

	streams, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumer,
		Streams:  []string{key, ">"}, // ">" = only new, undelivered messages
		Count:    1,
		Block:    blockTimeout,
	}).Result()

	if err == redis.Nil {
		// Timeout — no messages arrived. Not an error.
		return nil, nil
	}
	if err != nil {
		// Check if context was cancelled — don't log that as an error.
		if ctx.Err() != nil {
			return nil, nil
		}
		return nil, fmt.Errorf("xreadgroup: %w", err)
	}

	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return nil, nil
	}

	msg := streams[0].Messages[0]
	jobID, _ := msg.Values["job_id"].(string)

	return &Message{
		EntryID: msg.ID,
		JobID:   jobID,
	}, nil
}

// Ack removes the message from the Pending Entry List (PEL).
// A message stays in the PEL until Ack is called — if the worker
// crashes before Ack, the message can be reclaimed by another worker.
func (b *RedisBroker) Ack(ctx context.Context, queue, entryID string) error {
	key := streamPrefix + queue
	return b.client.XAck(ctx, key, groupName, entryID).Err()
}
