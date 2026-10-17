package queue

import (
	"math"
	"math/rand"
	"time"

	"github.com/WW-Keith/gotaskq/internal/model"
)

const (
	// BaseDelay is the starting backoff duration.
	// Attempt 1 waits up to 30s, attempt 2 up to 60s, and so on.
	BaseDelay = 30 * time.Second

	// MaxDelay caps the backoff so jobs don't wait longer than 1 hour.
	MaxDelay = 60 * time.Minute
)

// Policy holds the rules for how a job is retried.
type Policy struct {
	MaxRetry  int
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// Default is the policy applied when a job doesn't specify one.
var Default = Policy{
	MaxRetry:  3,
	BaseDelay: BaseDelay,
	MaxDelay:  MaxDelay,
}

// Outcome is the decision the worker acts on after a job failure.
type Outcome int

const (
	OutcomeRetry Outcome = iota // reschedule with a future retry_at
	OutcomeDLQ                  // exhausted — move to dead-letter queue
)

// Decide returns whether a failed job should be retried or sent to the DLQ.
func (p Policy) Decide(job *model.Job) Outcome {
	if job.Attempts < job.MaxRetry {
		return OutcomeRetry
	}
	return OutcomeDLQ
}

// NextRetryAt computes when the job should next be attempted.
// Uses exponential backoff with full jitter:
//
//	cap   = min(BaseDelay * 2^attempts, MaxDelay)
//	sleep = random(0, cap)
//
// Full jitter produces the most even load distribution —
// jobs don't all retry at the same moment after a mass failure.
func (p Policy) NextRetryAt(attempts int) time.Time {
	// cap = BaseDelay * 2^attempts, bounded by MaxDelay
	exp := math.Pow(2, float64(attempts))
	cap := time.Duration(float64(p.BaseDelay) * exp)
	if cap > p.MaxDelay {
		cap = p.MaxDelay
	}

	// Full jitter: random value in [0, cap)
	jitter := time.Duration(rand.Int63n(int64(cap)))

	return time.Now().UTC().Add(jitter)
}