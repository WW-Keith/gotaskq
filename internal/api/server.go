package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/WW-Keith/gotaskq/internal/store"
)

// Server holds all the dependecies that API layer needs
// Handlers are methods on the server so they acces deps cleanly
type Server struct {
	store  store.Store
	logger *slog.Logger
	mux    *http.ServeMux
}

// NewServer wires up the server and registers all routes
func NewServer(s store.Store) *Server {
	srv := &Server{
		store:  s,
		logger: slog.Default(),
		mux:    http.NewServeMux(),
	}
	srv.routes()
	return srv
}

// ServerHTTP makes Server implement http.Handler
// All requests flow through the logging middlware first
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.logging(s.mux).ServeHTTP(w, r)
}

// routes registers endpoints
// Go 1.22 supports "Method/Path" pattern natively
func (s *Server) routes() {
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("POST /jobs", s.handleEnqueue)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("DELETE /jobs/{id}", s.handleCancel)
}

//logging middleware records method, path, status and duration for every request
func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter:w, status: http.StatusOK}
		next.ServeHTTP(w, r)
		s.logger.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"duration", time.Since(start),
		)
	})
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

// handlerHealth is simple liveness check - no DB always fast
func (s*Server) handleHealth(w http.ResponseWriter, r *http.Request){
	writeJSON(w, http.StatusOK, map[string]string{"status":"ok"})
}

// --- JSON helpers used by every driver
// writeJSON encodes v as JSON and writes it with given status code.
func writeJSON(w http.ResponseWriter, status int, v any){
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encode response", "err", err)
	}
}

// writeError writes a standard JSON error body: {"error":"message"}
func writeError(w http.ResponseWriter, status int, msg string){
	writeJSON(w, status, map[string]string{"error": msg})
}