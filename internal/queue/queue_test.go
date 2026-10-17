package queue

import (
	"testing"
)

func TestValid_KnownQueues(t *testing.T) {
	for _, name := range []string{QueueCritical, QueueDefault, QueueLow} {
		if !Valid(name) {
			t.Errorf("Valid(%q) = false, want true", name)
		}
	}
}

func TestValid_UnknownQueue(t *testing.T) {
	if Valid("turbo") {
		t.Error("Valid(\"turbo\") = true, want false")
	}
	if Valid("") {
		t.Error("Valid(\"\") = true, want false")
	}
}

func TestNextQueue_ReturnsKnownQueue(t *testing.T) {
	for i := 0; i < 1000; i++ {
		q := NextQueue()
		if !Valid(q) {
			t.Errorf("NextQueue() returned unknown queue %q", q)
		}
	}
}

func TestNextQueue_WeightedDistribution(t *testing.T) {
	const iterations = 10_000

	counts := map[string]int{}
	for i := 0; i < iterations; i++ {
		counts[NextQueue()]++
	}

	criticalPct := float64(counts[QueueCritical]) / iterations * 100
	defaultPct := float64(counts[QueueDefault]) / iterations * 100
	lowPct := float64(counts[QueueLow]) / iterations * 100

	t.Logf("distribution over %d calls:", iterations)
	t.Logf("  critical: %.1f%% (target ~70%%)", criticalPct)
	t.Logf("  default:  %.1f%% (target ~25%%)", defaultPct)
	t.Logf("  low:      %.1f%% (target  ~5%%)", lowPct)

	// Allow ±5% variance from the target weights
	tolerance := 5.0

	if criticalPct < 70-tolerance || criticalPct > 70+tolerance {
		t.Errorf("critical queue: %.1f%% — outside expected range 65-75%%", criticalPct)
	}
	if defaultPct < 25-tolerance || defaultPct > 25+tolerance {
		t.Errorf("default queue: %.1f%% — outside expected range 20-30%%", defaultPct)
	}
	if lowPct < 5-tolerance || lowPct > 5+tolerance {
		t.Errorf("low queue: %.1f%% — outside expected range 0-10%%", lowPct)
	}
}

func TestNextQueue_NoStarvation(t *testing.T) {
	// Every queue must appear at least once in 1000 calls.
	// If any queue never appears, low-priority jobs would starve.
	counts := map[string]int{}
	for i := 0; i < 1000; i++ {
		counts[NextQueue()]++
	}

	for _, name := range All {
		if counts[name] == 0 {
			t.Errorf("queue %q never selected in 1000 calls — starvation risk", name)
		}
	}
}