package broker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func testRedisAddr() string {
	if addr := os.Getenv("TEST_REDIS_URL"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

func newTestBroker(t *testing.T) *RedisBroker {
	t.Helper()
	b, err := NewRedisBroker(testRedisAddr())
	if err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	return b
}

func TestRedisBroker_Publish_Subscribe(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()

	queue := "test-pubsub"

	if err := b.EnsureGroup(ctx, queue); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	// Publish a job ID
	if err := b.Publish(ctx, queue, "job-abc-123"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Subscribe should receive it
	msg, err := b.Subscribe(ctx, queue, "test-consumer-1")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if msg == nil {
		t.Fatal("Subscribe returned nil — expected a message")
	}
	if msg.JobID != "job-abc-123" {
		t.Errorf("JobID: got %q, want %q", msg.JobID, "job-abc-123")
	}
	if msg.EntryID == "" {
		t.Error("EntryID should be set")
	}

	// Ack it
	if err := b.Ack(ctx, queue, msg.EntryID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

func TestRedisBroker_Subscribe_EmptyQueue(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()

	queue := "test-empty"
	if err := b.EnsureGroup(ctx, queue); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	// Subscribe on an empty queue — should return nil after blockTimeout
	start := time.Now()
	msg, err := b.Subscribe(ctx, queue, "test-consumer-2")
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Subscribe on empty queue returned error: %v", err)
	}
	if msg != nil {
		t.Fatalf("expected nil message on empty queue, got %+v", msg)
	}

	// Should have blocked for approximately blockTimeout
	if elapsed < blockTimeout-200*time.Millisecond {
		t.Errorf("returned too fast: %v (expected ~%v)", elapsed, blockTimeout)
	}
	t.Logf("blocked for %v before returning nil — correct", elapsed.Round(time.Millisecond))
}

func TestRedisBroker_EnsureGroup_Idempotent(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()

	queue := "test-idempotent"

	// Calling EnsureGroup twice must not error
	if err := b.EnsureGroup(ctx, queue); err != nil {
		t.Fatalf("first EnsureGroup: %v", err)
	}
	if err := b.EnsureGroup(ctx, queue); err != nil {
		t.Fatalf("second EnsureGroup: %v", err)
	}
}

func TestRedisBroker_GroupReadsMessagesPublishedBeforeCreation(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	queue := fmt.Sprintf("test-before-group-%d", time.Now().UnixNano())

	if err := b.Publish(ctx, queue, "job-before-group"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := b.EnsureGroup(ctx, queue); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	msg, err := b.Subscribe(ctx, queue, "late-consumer")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if msg == nil || msg.JobID != "job-before-group" {
		t.Fatalf("Subscribe() = %+v, want job-before-group", msg)
	}
}

func TestRedisBroker_TwoConsumers_EachGetDifferentMessage(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()

	queue := "test-fanout"
	if err := b.EnsureGroup(ctx, queue); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	// Publish two jobs
	if err := b.Publish(ctx, queue, "job-1"); err != nil {
		t.Fatalf("Publish job-1: %v", err)
	}
	if err := b.Publish(ctx, queue, "job-2"); err != nil {
		t.Fatalf("Publish job-2: %v", err)
	}

	// Two consumers — each should get a different job
	msg1, err := b.Subscribe(ctx, queue, "consumer-a")
	if err != nil || msg1 == nil {
		t.Fatalf("consumer-a Subscribe: err=%v msg=%v", err, msg1)
	}

	msg2, err := b.Subscribe(ctx, queue, "consumer-b")
	if err != nil || msg2 == nil {
		t.Fatalf("consumer-b Subscribe: err=%v msg=%v", err, msg2)
	}

	if msg1.JobID == msg2.JobID {
		t.Errorf("both consumers got the same job: %s", msg1.JobID)
	}

	t.Logf("consumer-a got %s, consumer-b got %s", msg1.JobID, msg2.JobID)

	_ = b.Ack(ctx, queue, msg1.EntryID)
	_ = b.Ack(ctx, queue, msg2.EntryID)
}
