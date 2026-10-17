package schedule

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

func Next(expr string, from time.Time) (time.Time, error) {
	parsed, err := cron.ParseStandard("CRON_TZ=UTC " + expr)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse cron expression: %w", err)
	}
	next := parsed.Next(from.UTC())
	if next.IsZero() {
		return time.Time{}, fmt.Errorf("cron expression has no next time")
	}
	return next.UTC(), nil
}
