package api

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/WW-Keith/gotaskq/internal/model"
)

func TestCreateSchedule(t *testing.T) {
	server := newTestServer()
	response := doRequest(t, server, http.MethodPost, "/schedules", map[string]any{
		"type":    "send_email",
		"payload": map[string]string{"to": "test@example.com"},
		"cron":    "0 9 * * 1-5",
	})

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusCreated, response.Body)
	}
	var created model.Schedule
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" {
		t.Fatal("created schedule has no id")
	}
	if created.Cron != "0 9 * * 1-5" {
		t.Fatalf("cron = %q", created.Cron)
	}
	if !created.NextRunAt.After(time.Now()) {
		t.Fatalf("next run time is not in the future: %s", created.NextRunAt)
	}
}

func TestCreateScheduleRejectsInvalidCron(t *testing.T) {
	server := newTestServer()
	response := doRequest(t, server, http.MethodPost, "/schedules", map[string]any{
		"type":    "send_email",
		"payload": map[string]string{"to": "test@example.com"},
		"cron":    "not-a-cron",
	})
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusUnprocessableEntity, response.Body)
	}
}
