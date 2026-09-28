// Package api is Rocky's HTTP API (/v1).
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

type server struct {
	log  *slog.Logger
	jobs JobStore
}

// NewHandler returns the API's root handler.
func NewHandler(log *slog.Logger, store JobStore, deps []Dependency) http.Handler {
	s := &server{log: log, jobs: store}
	mux := http.NewServeMux()
	mux.Handle("GET /v1/health", &health{log: log, deps: deps})
	mux.HandleFunc("POST /v1/jobs", s.createJob)
	mux.HandleFunc("GET /v1/jobs/{id}", s.getJob)
	mux.HandleFunc("GET /v1/jobs/{id}/events", s.getJobEvents)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, http.StatusNotFound, "NOT_FOUND", "No route for "+r.Method+" "+r.URL.Path+".")
	})
	return withRequestID(withAccessLog(log, withRecover(log, mux)))
}

// errorBody is the error shape every endpoint returns (spec §36).
type errorBody struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	writeJSON(w, status, errorBody{apiError{Code: code, Message: msg, RequestID: RequestID(r.Context())}})
}

// internalError logs the cause and returns a generic 500; internals never reach clients.
func (s *server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.log.Error("request failed", "err", err, "request_id", RequestID(r.Context()))
	writeError(w, r, http.StatusInternalServerError, "INTERNAL", "An internal error occurred.")
}
