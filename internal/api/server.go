package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/WW-Keith/gotaskq/internal/broker"
	"github.com/WW-Keith/gotaskq/internal/observe"
	"github.com/WW-Keith/gotaskq/internal/store"
)

// Server holds all dependencies the API layer needs.
// Handlers are methods on Server so they access deps cleanly.
type Server struct {
	store   store.Store
	broker  broker.Broker
	metrics *observe.Metrics
	logger  *slog.Logger
	mux     *http.ServeMux
}

// NewServer wires up the server and registers all routes.
func NewServer(s store.Store, b broker.Broker, m *observe.Metrics) *Server {
	srv := &Server{
		store:   s,
		broker:  b,
		metrics: m,
		logger:  slog.Default(),
		mux:     http.NewServeMux(),
	}
	srv.routes()
	return srv
}

// ServeHTTP makes Server implement http.Handler.
// All requests flow through the logging middleware first.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.logging(s.mux).ServeHTTP(w, r)
}

// routes registers every endpoint.
// Go 1.22+ supports "METHOD /path" patterns natively — no external router needed.
func (s *Server) routes() {
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.Handle("GET /metrics", s.metrics.Handler())
	s.mux.HandleFunc("POST /jobs", s.handleEnqueue)
	s.mux.HandleFunc("POST /schedules", s.handleCreateSchedule)
	s.mux.HandleFunc("DELETE /schedules/{id}", s.handleDeleteSchedule)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("DELETE /jobs/{id}", s.handleCancel)
}

// logging middleware records method, path, status, and duration for every request.
func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		s.logger.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"duration", time.Since(start).String(),
		)
	})
}

// responseWriter wraps http.ResponseWriter to capture the status code.
// The default ResponseWriter doesn't expose what status was written.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

// handleHealth is a simple liveness check — no DB call, always fast.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- JSON helpers used by every handler ---

// writeJSON encodes v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Can't do much here — headers already sent.
		// In production this would go to a structured logger.
		slog.Error("encode response", "err", err)
	}
}

// writeError writes a standard JSON error body: {"error": "message"}.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
