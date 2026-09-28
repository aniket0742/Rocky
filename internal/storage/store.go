// Package storage is Rocky's PostgreSQL persistence (ADR 0001).
//
// Every state transition runs in one transaction: a fenced UPDATE on jobs,
// then the attempts row, then the job_events row(s) (ADR 0005).
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aniket0742/rocky/internal/jobs"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

const jobColumns = `id, queue, type, status, priority, payload, metadata, attempt, max_attempts,
	timeout_seconds, run_at, idempotency_key, worker_id, last_error, lease_expires_at,
	created_at, updated_at, finished_at`

func scanJob(row pgx.Row, extra ...any) (jobs.Job, error) {
	var j jobs.Job
	dest := append([]any{&j.ID, &j.Queue, &j.Type, &j.Status, &j.Priority, &j.Payload, &j.Metadata,
		&j.Attempt, &j.MaxAttempts, &j.TimeoutSeconds, &j.RunAt, &j.IdempotencyKey, &j.WorkerID,
		&j.LastError, &j.LeaseExpiresAt, &j.CreatedAt, &j.UpdatedAt, &j.FinishedAt}, extra...)
	return j, row.Scan(dest...)
}

// Enqueue creates a job, or returns the existing one for a reused idempotency key.
// created is false on an idempotent replay. A key reused with a different
// queue, type or payload returns jobs.ErrIdempotencyConflict.
func (s *Store) Enqueue(ctx context.Context, p jobs.EnqueueParams) (job jobs.Job, created bool, err error) {
	var key *string
	if p.IdempotencyKey != "" {
		key = &p.IdempotencyKey
	}
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		job, err = scanJob(tx.QueryRow(ctx, `
			INSERT INTO jobs (queue, type, payload, metadata, priority, max_attempts, timeout_seconds,
			                  idempotency_key, run_at, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, coalesce($9, now()),
			        CASE WHEN coalesce($9, now()) <= now() THEN 'queued' ELSE 'scheduled' END)
			ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
			RETURNING `+jobColumns,
			p.Queue, p.Type, p.Payload, p.Metadata, int16(p.Priority), p.MaxAttempts, p.TimeoutSeconds, key, p.RunAt))
		if errors.Is(err, pgx.ErrNoRows) {
			return s.replay(ctx, tx, p, &job)
		}
		if err != nil {
			return err
		}
		created = true
		return insertEvent(ctx, tx, job.ID, nil, "enqueued", nil, eventData{RequestID: p.RequestID})
	})
	return job, created, err
}

// replay loads the job that owns p.IdempotencyKey. jsonb equality makes the
// payload comparison insensitive to key order and whitespace.
func (s *Store) replay(ctx context.Context, tx pgx.Tx, p jobs.EnqueueParams, job *jobs.Job) error {
	var same bool
	j, err := scanJob(tx.QueryRow(ctx, `
		SELECT `+jobColumns+`, (queue = $2 AND type = $3 AND payload = $4::jsonb)
		FROM jobs WHERE idempotency_key = $1`,
		p.IdempotencyKey, p.Queue, p.Type, p.Payload), &same)
	if err != nil {
		return fmt.Errorf("load job for idempotency key: %w", err)
	}
	if !same {
		return jobs.ErrIdempotencyConflict
	}
	*job = j
	return nil
}

func (s *Store) Get(ctx context.Context, id uuid.UUID) (jobs.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return j, jobs.ErrNotFound
	}
	return j, err
}

// Events returns a job's history, oldest first.
func (s *Store) Events(ctx context.Context, jobID uuid.UUID) ([]jobs.Event, error) {
	rows, _ := s.pool.Query(ctx, `
		SELECT id, attempt, type, worker_id, data, created_at
		FROM job_events WHERE job_id = $1 ORDER BY id`, jobID)
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (jobs.Event, error) {
		var e jobs.Event
		return e, r.Scan(&e.ID, &e.Attempt, &e.Type, &e.WorkerID, &e.Data, &e.CreatedAt)
	})
}

// Attempts returns a job's attempts, oldest first.
func (s *Store) Attempts(ctx context.Context, jobID uuid.UUID) ([]jobs.Attempt, error) {
	rows, _ := s.pool.Query(ctx, `
		SELECT attempt, worker_id, status, error, started_at, ended_at
		FROM attempts WHERE job_id = $1 ORDER BY attempt`, jobID)
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (jobs.Attempt, error) {
		var a jobs.Attempt
		return a, r.Scan(&a.Number, &a.WorkerID, &a.Status, &a.Error, &a.StartedAt, &a.EndedAt)
	})
}

// Claim leases up to p.Limit ready jobs. Each claim starts a new attempt with a fresh token.
func (s *Store) Claim(ctx context.Context, p jobs.ClaimParams) ([]jobs.Lease, error) {
	var leases []jobs.Lease
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, _ := tx.Query(ctx, `
			UPDATE jobs j
			SET status = 'running', attempt = j.attempt + 1, lease_token = gen_random_uuid(),
			    lease_expires_at = now() + make_interval(secs => j.timeout_seconds) + $5::interval,
			    worker_id = $4, updated_at = now()
			FROM (
			    SELECT id FROM jobs
			    WHERE queue = $1 AND status = 'queued' AND type = ANY($2)
			    ORDER BY priority, run_at, id
			    LIMIT $3
			    FOR UPDATE SKIP LOCKED
			) next
			WHERE j.id = next.id
			RETURNING j.id, j.type, j.payload, j.attempt, j.max_attempts, j.timeout_seconds,
			          j.lease_token, j.lease_expires_at`,
			p.Queue, p.Types, p.Limit, p.WorkerID, p.Grace)
		var err error
		leases, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (jobs.Lease, error) {
			var l jobs.Lease
			return l, r.Scan(&l.JobID, &l.Type, &l.Payload, &l.Attempt, &l.MaxAttempts,
				&l.TimeoutSeconds, &l.Token, &l.ExpiresAt)
		})
		if err != nil || len(leases) == 0 {
			return err
		}

		ids := make([]uuid.UUID, len(leases))
		for i, l := range leases {
			ids[i] = l.JobID
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO attempts (job_id, attempt, worker_id, lease_token)
			SELECT id, attempt, worker_id, lease_token FROM jobs WHERE id = ANY($1)`, ids); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO job_events (job_id, attempt, type, worker_id)
			SELECT id, attempt, 'claimed', worker_id FROM jobs WHERE id = ANY($1) ORDER BY id`, ids)
		return err
	})
	if err != nil {
		return nil, err
	}
	return leases, nil
}

// Complete marks the leased attempt succeeded. It returns jobs.ErrLeaseLost if
// the token does not match or the lease has expired (strict expiry).
func (s *Store) Complete(ctx context.Context, jobID, token uuid.UUID) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var attempt int
		var worker string
		err := tx.QueryRow(ctx, `
			UPDATE jobs
			SET status = 'succeeded', lease_token = NULL, lease_expires_at = NULL,
			    finished_at = now(), updated_at = now()
			WHERE id = $1 AND lease_token = $2 AND lease_expires_at > now()
			RETURNING attempt, worker_id`, jobID, token).Scan(&attempt, &worker)
		if errors.Is(err, pgx.ErrNoRows) {
			return jobs.ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if err := endAttempt(ctx, tx, jobID, attempt, "succeeded", nil); err != nil {
			return err
		}
		return insertEvent(ctx, tx, jobID, &attempt, "succeeded", &worker, eventData{})
	})
}

// Fail records a failed attempt. The job moves to retrying (run_at = now + RetryIn)
// if the failure is retryable and attempts remain, otherwise to dead_letter.
// It returns the job's new status, or jobs.ErrLeaseLost.
func (s *Store) Fail(ctx context.Context, jobID, token uuid.UUID, f jobs.Failure) (jobs.Status, error) {
	var status jobs.Status
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var (
			attempt, maxAttempts int
			worker               string
			runAt                time.Time
		)
		err := tx.QueryRow(ctx, `
			UPDATE jobs
			SET status      = CASE WHEN $4 AND attempt < max_attempts THEN 'retrying' ELSE 'dead_letter' END,
			    run_at      = CASE WHEN $4 AND attempt < max_attempts THEN now() + $5::interval ELSE run_at END,
			    finished_at = CASE WHEN $4 AND attempt < max_attempts THEN NULL ELSE now() END,
			    last_error = $3, lease_token = NULL, lease_expires_at = NULL, updated_at = now()
			WHERE id = $1 AND lease_token = $2 AND lease_expires_at > now()
			RETURNING attempt, max_attempts, worker_id, status, run_at`,
			jobID, token, f.Error, f.Retryable, f.RetryIn).Scan(&attempt, &maxAttempts, &worker, &status, &runAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return jobs.ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if err := endAttempt(ctx, tx, jobID, attempt, "failed", &f.Error); err != nil {
			return err
		}
		retryable := f.Retryable
		if err := insertEvent(ctx, tx, jobID, &attempt, "failed", &worker,
			eventData{Error: f.Error, Retryable: &retryable}); err != nil {
			return err
		}
		if status == jobs.StatusRetrying {
			return insertEvent(ctx, tx, jobID, &attempt, "retry_scheduled", nil, eventData{RetryAt: &runAt})
		}
		reason := "attempts_exhausted"
		if !f.Retryable {
			reason = "non_retryable"
		}
		return insertEvent(ctx, tx, jobID, &attempt, "dead_lettered", nil, eventData{Reason: reason})
	})
	return status, err
}

// Promote moves up to limit scheduled/retrying jobs whose run_at has arrived to queued.
// Safe to run concurrently from every worker.
func (s *Store) Promote(ctx context.Context, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'queued', updated_at = now()
		WHERE id IN (
		    SELECT id FROM jobs
		    WHERE status IN ('scheduled', 'retrying') AND run_at <= now()
		    ORDER BY run_at
		    LIMIT $1
		    FOR UPDATE SKIP LOCKED
		)`, limit)
	return tag.RowsAffected(), err
}

func endAttempt(ctx context.Context, tx pgx.Tx, jobID uuid.UUID, attempt int, status string, errMsg *string) error {
	_, err := tx.Exec(ctx, `
		UPDATE attempts SET status = $3, error = $4, ended_at = now()
		WHERE job_id = $1 AND attempt = $2`, jobID, attempt, status, errMsg)
	return err
}

// eventData is the typed shape of job_events.data.
type eventData struct {
	RequestID string     `json:"request_id,omitempty"`
	Error     string     `json:"error,omitempty"`
	Retryable *bool      `json:"retryable,omitempty"`
	RetryAt   *time.Time `json:"retry_at,omitempty"`
	Reason    string     `json:"reason,omitempty"`
}

func insertEvent(ctx context.Context, tx pgx.Tx, jobID uuid.UUID, attempt *int, typ string, worker *string, data eventData) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO job_events (job_id, attempt, type, worker_id, data)
		VALUES ($1, $2, $3, $4, $5)`, jobID, attempt, typ, worker, b)
	return err
}
