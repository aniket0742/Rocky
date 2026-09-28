package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/aniket0742/rocky/internal/jobs"
)

// JobStore is the persistence the API needs. *storage.Store implements it.
type JobStore interface {
	Enqueue(ctx context.Context, p jobs.EnqueueParams) (jobs.Job, bool, error)
	Get(ctx context.Context, id uuid.UUID) (jobs.Job, error)
	Events(ctx context.Context, id uuid.UUID) ([]jobs.Event, error)
}

const maxRequestBytes = 1 << 20

type createJobRequest struct {
	Type           string          `json:"type"`
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	Metadata       json.RawMessage `json:"metadata"`
	Priority       string          `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	IdempotencyKey string          `json:"idempotency_key"`
}

func (s *server) createJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, r, http.StatusRequestEntityTooLarge, "REQUEST_TOO_LARGE", "The request body is too large.")
			return
		}
		writeError(w, r, http.StatusBadRequest, "INVALID_BODY", "The request body could not be decoded: "+err.Error())
		return
	}

	priority, err := jobs.ParsePriority(req.Priority)
	p := jobs.EnqueueParams{
		Queue: req.Queue, Type: req.Type, Payload: req.Payload, Metadata: req.Metadata,
		Priority: priority, MaxAttempts: req.MaxAttempts, TimeoutSeconds: req.TimeoutSeconds,
		IdempotencyKey: req.IdempotencyKey, RequestID: RequestID(r.Context()),
	}
	if err == nil {
		err = p.Normalize()
	}
	if err != nil { // always a *jobs.ValidationError
		writeError(w, r, http.StatusBadRequest, "VALIDATION_FAILED", err.Error())
		return
	}

	job, created, err := s.jobs.Enqueue(r.Context(), p)
	switch {
	case errors.Is(err, jobs.ErrIdempotencyConflict):
		writeError(w, r, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED",
			"This idempotency key was already used for a job with a different queue, type or payload.")
	case err != nil:
		s.internalError(w, r, err)
	case created:
		w.Header().Set("Location", "/v1/jobs/"+job.ID.String())
		writeJSON(w, http.StatusCreated, job)
	default:
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, job)
	}
}

func (s *server) getJob(w http.ResponseWriter, r *http.Request) {
	if job, ok := s.loadJob(w, r); ok {
		writeJSON(w, http.StatusOK, job)
	}
}

func (s *server) getJobEvents(w http.ResponseWriter, r *http.Request) {
	job, ok := s.loadJob(w, r)
	if !ok {
		return
	}
	events, err := s.jobs.Events(r.Context(), job.ID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": events})
}

// loadJob resolves the {id} path value, writing a 404 or 500 itself on failure.
func (s *server) loadJob(w http.ResponseWriter, r *http.Request) (jobs.Job, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusNotFound, "JOB_NOT_FOUND", "The requested job does not exist.")
		return jobs.Job{}, false
	}
	job, err := s.jobs.Get(r.Context(), id)
	switch {
	case errors.Is(err, jobs.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "JOB_NOT_FOUND", "The requested job does not exist.")
		return job, false
	case err != nil:
		s.internalError(w, r, err)
		return job, false
	}
	return job, true
}
