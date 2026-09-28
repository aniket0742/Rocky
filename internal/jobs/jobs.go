// Package jobs defines Rocky's job domain: states, records and enqueue rules.
// It has no database or HTTP dependencies.
package jobs

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusScheduled  Status = "scheduled"
	StatusQueued     Status = "queued"
	StatusRunning    Status = "running"
	StatusRetrying   Status = "retrying"
	StatusSucceeded  Status = "succeeded"
	StatusDeadLetter Status = "dead_letter"
	StatusCancelled  Status = "cancelled"
)

var (
	ErrNotFound = errors.New("job not found")
	// ErrLeaseLost means the caller's lease is expired or superseded; it must abandon the job.
	ErrLeaseLost = errors.New("lease lost")
	// ErrIdempotencyConflict means the key was already used for a different job.
	ErrIdempotencyConflict = errors.New("idempotency key reused with different parameters")
)

// Job is a job's current state. The lease token is deliberately absent:
// it is a capability held only by the worker that owns the lease.
type Job struct {
	ID             uuid.UUID       `json:"id"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Status         Status          `json:"status"`
	Priority       Priority        `json:"priority"`
	Payload        json.RawMessage `json:"payload"`
	Metadata       json.RawMessage `json:"metadata"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	RunAt          time.Time       `json:"run_at"`
	IdempotencyKey *string         `json:"idempotency_key"`
	WorkerID       *string         `json:"worker_id"`
	LastError      *string         `json:"last_error"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	FinishedAt     *time.Time      `json:"finished_at"`
}

// Event is one entry in a job's execution history (job_events).
type Event struct {
	ID        int64           `json:"id"`
	Attempt   *int            `json:"attempt"`
	Type      string          `json:"type"`
	WorkerID  *string         `json:"worker_id"`
	Data      json.RawMessage `json:"data"`
	CreatedAt time.Time       `json:"created_at"`
}

// Attempt is one execution of a job.
type Attempt struct {
	Number    int        `json:"attempt"`
	WorkerID  string     `json:"worker_id"`
	Status    string     `json:"status"`
	Error     *string    `json:"error"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at"`
}

// Lease is a claimed job plus the token that fences its owner.
type Lease struct {
	JobID          uuid.UUID
	Type           string
	Payload        json.RawMessage
	Attempt        int
	MaxAttempts    int
	TimeoutSeconds int
	Token          uuid.UUID
	ExpiresAt      time.Time
}

// ClaimParams selects what a worker may claim.
type ClaimParams struct {
	Queue    string
	Types    []string // only jobs this worker has handlers for
	Limit    int
	WorkerID string
	// Grace is added to the job's timeout to form the lease duration.
	// Phase 2 replaces this with a short TTL renewed by heartbeats.
	Grace time.Duration
}

// Failure describes a failed attempt.
type Failure struct {
	Error     string
	Retryable bool
	RetryIn   time.Duration
}
