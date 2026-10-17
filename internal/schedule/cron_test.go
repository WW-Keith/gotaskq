package schedule

import (
	"testing"
	"time"
)

func TestNextReturnsNextCronOccurrence(t *testing.T) {
	from := time.Date(2026, time.January, 1, 12, 1, 0, 0, time.UTC)
	got, err := Next("*/5 * * * *", from)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, time.January, 1, 12, 5, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("Next() = %s, want %s", got, want)
	}
}

func TestNextRejectsInvalidCron(t *testing.T) {
	if _, err := Next("not-a-cron", time.Now()); err == nil {
		t.Fatal("Next() accepted an invalid cron expression")
	}
}
