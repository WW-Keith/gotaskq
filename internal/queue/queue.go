package queue

import "math/rand"

// Names for the three priority tiers.
// Use these constants everywhere — never raw strings.
const (
	QueueCritical = "critical"
	QueueDefault  = "default"
	QueueLow      = "low"

	// All returns all queue names as a slice — useful for setup loops.
)

// All is the ordered list of queue names.
var All = []string{QueueCritical, QueueDefault, QueueLow}

// queue pairs a name with its selection weight.
type weightedQueue struct {
	Name   string
	Weight int
}

// queues defines priority tiers and their relative selection weights.
// With weights 70/25/5:
//   - critical is chosen ~70% of the time
//   - default  is chosen ~25% of the time
//   - low      is chosen  ~5% of the time
//
// This prevents starvation — low-priority jobs always make progress,
// just less frequently than critical ones.
var queues = []weightedQueue{
	{QueueCritical, 70},
	{QueueDefault, 25},
	{QueueLow, 5},
}

// NextQueue picks a queue name using weighted random selection.
// Call this on each worker iteration to decide which queue to drain.
func NextQueue() string {
	total := 0
	for _, q := range queues {
		total += q.Weight
	}

	r := rand.Intn(total)
	for _, q := range queues {
		r -= q.Weight
		if r < 0 {
			return q.Name
		}
	}

	// Unreachable — but satisfies the compiler.
	return QueueDefault
}

// Valid returns true if name is a known queue.
// Use this in the API handler to reject unknown queue values.
func Valid(name string) bool {
	for _, q := range queues {
		if q.Name == name {
			return true
		}
	}
	return false
}