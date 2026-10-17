package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/WW-Keith/gotaskq/internal/model"
	"github.com/WW-Keith/gotaskq/internal/queue"
	"github.com/WW-Keith/gotaskq/internal/schedule"
	"github.com/WW-Keith/gotaskq/internal/store"
)

type createScheduleRequest struct {
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Queue    string          `json:"queue"`
	Priority model.Priority  `json:"priority"`
	MaxRetry int             `json:"max_retry"`
	Cron     string          `json:"cron"`
}

func (s *Server) handleCreateSchedule(w http.ResponseWriter, r *http.Request) {
	var req createScheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.Queue != "" && !queue.Valid(req.Queue) {
		writeError(w, http.StatusUnprocessableEntity,
			fmt.Sprintf("unknown queue %q — valid values: critical, default, low", req.Queue))
		return
	}

	now := time.Now().UTC()
	nextRunAt, err := schedule.Next(req.Cron, now)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	recurring := &model.Schedule{
		Type:      req.Type,
		Payload:   req.Payload,
		Queue:     req.Queue,
		Priority:  req.Priority,
		MaxRetry:  req.MaxRetry,
		Cron:      req.Cron,
		NextRunAt: nextRunAt,
	}
	if err := recurring.Valid(); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if err := s.store.CreateSchedule(r.Context(), recurring); err != nil {
		s.logger.Error("create schedule failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusCreated, recurring)
}

func (s *Server) handleDeleteSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing schedule id")
		return
	}
	if err := s.store.DeleteSchedule(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "schedule not found")
			return
		}
		s.logger.Error("delete schedule failed", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": id, "status": "deleted"})
}
