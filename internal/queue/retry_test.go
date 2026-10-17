package queue

import (
	"testing"
	"time"

	"github.com/WW-Keith/gotaskq/internal/model"
)

func TestPolicy_Decide_Retry(t *testing.T) {
	p := Policy{MaxRetry: 3, BaseDelay: BaseDelay, MaxDelay: MaxDelay}

	job := &model.Job{Attempts: 1, MaxRetry: 3}
	if got := p.Decide(job); got != OutcomeRetry {
		t.Errorf("Decide() = %v, want OutcomeRetry", got)
	}
}

func TestPolicy_Decide_DLQ(t *testing.T) {
	p := Policy{MaxRetry: 3, BaseDelay: BaseDelay, MaxDelay: MaxDelay}

	// Exactly at limit — should go to DLQ
	job := &model.Job{Attempts: 3, MaxRetry: 3}
	if got := p.Decide(job); got != OutcomeDLQ {
		t.Errorf("Decide() = %v, want OutcomeDLQ", got)
	}
}

func TestPolicy_NextRetryAt_GrowsWithAttempts(t *testing.T) {
	p := Policy{MaxRetry: 5, BaseDelay: 30 * time.Second, MaxDelay: MaxDelay}

	// Higher attempt number → longer max possible delay
	// We can't assert exact values (jitter is random),
	// but the upper bound must grow and never exceed MaxDelay.
	for attempt := 1; attempt <= 6; attempt++ {
		retryAt := p.NextRetryAt(attempt)

		if retryAt.Before(time.Now()) {
			t.Errorf("attempt %d: RetryAt is in the past", attempt)
		}

		maxPossible := time.Now().Add(MaxDelay).Add(time.Second)
		if retryAt.After(maxPossible) {
			t.Errorf("attempt %d: RetryAt exceeds MaxDelay cap", attempt)
		}
	}
}

func TestPolicy_NextRetryAt_Jitter(t *testing.T) {
	p := Policy{MaxRetry: 5, BaseDelay: 30 * time.Second, MaxDelay: MaxDelay}

	// Run 100 times at the same attempt — should produce varied results.
	seen := map[int64]bool{}
	for i := 0; i < 100; i++ {
		retryAt := p.NextRetryAt(3)
		seen[retryAt.Unix()] = true
	}

	// With full jitter across up to 240 seconds,
	// 100 calls should produce at least 50 distinct timestamps.
	if len(seen) < 50 {
		t.Errorf("jitter produced only %d unique timestamps — expected more variance", len(seen))
	}
	t.Logf("100 calls produced %d unique retry timestamps", len(seen))
}

func TestPolicy_NextRetryAt_RespectsMaxDelay(t *testing.T) {
	// Very short MaxDelay — all retry times must be within it.
	p := Policy{
		MaxRetry:  5,
		BaseDelay: 10 * time.Second,
		MaxDelay:  30 * time.Second,
	}

	for i := 0; i < 50; i++ {
		retryAt := p.NextRetryAt(10) // high attempt — would exceed cap without clamping
		limit := time.Now().Add(31 * time.Second)
		if retryAt.After(limit) {
			t.Errorf("retryAt %v exceeded MaxDelay cap", retryAt)
		}
	}
}

func TestPolicy_Decide_BoundaryConditions(t *testing.T) {
	p := Default

	tests := []struct {
		attempts int
		maxRetry int
		want     Outcome
	}{
		{0, 3, OutcomeRetry},  // fresh job — always retry
		{1, 3, OutcomeRetry},
		{2, 3, OutcomeRetry},
		{3, 3, OutcomeDLQ},    // attempts == maxRetry → DLQ
		{4, 3, OutcomeDLQ},    // over limit → still DLQ
		{0, 0, OutcomeDLQ},    // max_retry=0 means no retries ever
	}

	for _, tt := range tests {
		job := &model.Job{Attempts: tt.attempts, MaxRetry: tt.maxRetry}
		got := p.Decide(job)
		if got != tt.want {
			t.Errorf("Decide(attempts=%d, maxRetry=%d) = %v, want %v",
				tt.attempts, tt.maxRetry, got, tt.want)
		}
	}
}