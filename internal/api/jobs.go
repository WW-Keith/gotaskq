package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/WW-Keith/gotaskq/internal/model"
	"github.com/WW-Keith/gotaskq/internal/queue"
	"github.com/WW-Keith/gotaskq/internal/store"
)

// enqueueRequest is the shape of the POST /jobs request body.
// Separate from model.Job — the caller doesn't set ID, Status, or timestamps.
type enqueueRequest struct {
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Queue    string          `json:"queue"`
	Priority model.Priority  `json:"priority"`
	MaxRetry int             `json:"max_retry"`
	RunAt    *time.Time      `json:"run_at,omitempty"`
}

// handleEnqueue handles POST /jobs.
// Decodes the request, validates it, enqueues the job, returns the created job.
func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	// Decode request body
	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	// Build the Job from the request
	job := &model.Job{
		Type:     req.Type,
		Payload:  req.Payload,
		Queue:    req.Queue,
		Priority: req.Priority,
		MaxRetry: req.MaxRetry,
		RunAt:    req.RunAt,
	}
	if job.RunAt != nil && !job.RunAt.After(time.Now()) {
		job.RunAt = nil
	}

	// Validate the queue before persisting the job.
	if req.Queue != "" && !queue.Valid(req.Queue) {
		writeError(w, http.StatusUnprocessableEntity,
			fmt.Sprintf("unknown queue %q — valid values: critical, default, low", req.Queue))
		return
	}

	// Enqueue — store fills in ID, Status, CreatedAt, defaults
	if err := s.store.Enqueue(r.Context(), job); err != nil {
		s.logger.Error("enqueue failed", "err", err)
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	if job.RunAt == nil || !job.RunAt.After(time.Now()) {
		if err := s.broker.Publish(r.Context(), job.Queue, job.ID); err != nil {
			s.logger.Error("publish failed", "id", job.ID, "err", err)
		} else if err := s.store.MarkPublished(r.Context(), job.ID); err != nil {
			s.logger.Error("mark published failed", "id", job.ID, "err", err)
		}
	}
	s.metrics.JobsEnqueued.WithLabelValues(job.Queue, job.Type).Inc()

	// 201 Created with the full job returned
	writeJSON(w, http.StatusCreated, job)
}

// handleGetJob handles GET /jobs/{id}.
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing job id")
		return
	}

	job, err := s.store.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		s.logger.Error("get job failed", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, job)
}

// handleCancel handles DELETE /jobs/{id}.
func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing job id")
		return
	}

	if err := s.store.Cancel(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeError(w, http.StatusConflict,
				"job cannot be cancelled — already running or finished")
			return
		}
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		s.logger.Error("cancel failed", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"id":     id,
		"status": "cancelled",
	})
}
