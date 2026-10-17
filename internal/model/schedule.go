package model

import (
	"encoding/json"
	"fmt"
	"time"
)

// Schedule describes a recurring job template.
type Schedule struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Queue     string          `json:"queue"`
	Priority  Priority        `json:"priority"`
	MaxRetry  int             `json:"max_retry"`
	Cron      string          `json:"cron"`
	NextRunAt time.Time       `json:"next_run_at"`
	CreatedAt time.Time       `json:"created_at"`
}

func (s *Schedule) Valid() error {
	if s.Type == "" {
		return fmt.Errorf("schedule type is required")
	}
	if len(s.Payload) == 0 {
		return fmt.Errorf("schedule payload is required")
	}
	if s.Cron == "" {
		return fmt.Errorf("cron expression is required")
	}
	return nil
}
