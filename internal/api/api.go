// Package api is Rocky's HTTP API (/v1).
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// NewHandler returns the API's root handler.
func NewHandler(log *slog.Logger, deps []Dependency) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /v1/health", &health{log: log, deps: deps})
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
