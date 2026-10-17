package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/WW-Keith/gotaskq/internal/broker"
	"github.com/WW-Keith/gotaskq/internal/model"
)

type testStore struct {
	jobs      []*model.Job
	published []string
	err       error
}

func (s *testStore) DueScheduledJobs(_ context.Context, _ time.Time, _ int) ([]*model.Job, error) {
	return s.jobs, s.err
}

func (s *testStore) MarkPublished(_ context.Context, id string) error {
	s.published = append(s.published, id)
	return nil
}

func (s *testStore) MaterializeDueSchedules(context.Context, time.Time, int) (int, error) {
	return 0, nil
}

type testBroker struct {
	published []string
	err       error
}

func (b *testBroker) Publish(_ context.Context, _, id string) error {
	b.published = append(b.published, id)
	return b.err
}

func (b *testBroker) Subscribe(context.Context, string, string) (*broker.Message, error) {
	return nil, nil
}

func (b *testBroker) Ack(context.Context, string, string) error { return nil }

func (b *testBroker) EnsureGroup(context.Context, string) error { return nil }

func TestDispatchPublishesThenMarksJobs(t *testing.T) {
	s := &testStore{jobs: []*model.Job{{ID: "job-1", Queue: "default"}}}
	b := &testBroker{}
	scheduler := New(s, b, time.Second)

	if err := scheduler.dispatch(context.Background()); err != nil {
		t.Fatalf("dispatch() error = %v", err)
	}
	if len(b.published) != 1 || b.published[0] != "job-1" {
		t.Fatalf("published IDs = %v, want [job-1]", b.published)
	}
	if len(s.published) != 1 || s.published[0] != "job-1" {
		t.Fatalf("marked IDs = %v, want [job-1]", s.published)
	}
}

func TestDispatchLeavesJobUnmarkedOnPublishFailure(t *testing.T) {
	s := &testStore{jobs: []*model.Job{{ID: "job-1", Queue: "default"}}}
	b := &testBroker{err: errors.New("redis unavailable")}
	scheduler := New(s, b, time.Second)

	if err := scheduler.dispatch(context.Background()); err == nil {
		t.Fatal("dispatch() expected publish failure")
	}
	if len(s.published) != 0 {
		t.Fatalf("marked jobs after publish failure = %v, want none", s.published)
	}
}
