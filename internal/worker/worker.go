// Package worker claims jobs from Postgres, runs their handlers with bounded
// concurrency and reports results through fenced (lease-token-checked) writes.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/aniket0742/rocky/internal/jobs"
)

// Store is the persistence the worker needs. *storage.Store implements it.
type Store interface {
	Claim(ctx context.Context, p jobs.ClaimParams) ([]jobs.Lease, error)
	Complete(ctx context.Context, jobID, token uuid.UUID) error
	Fail(ctx context.Context, jobID, token uuid.UUID, f jobs.Failure) (jobs.Status, error)
	Promote(ctx context.Context, limit int) (int64, error)
}

type Config struct {
	ID           string
	Queues       []string
	Concurrency  int           // max handlers running at once
	PollInterval time.Duration // how often to claim when idle and to promote due jobs
}

const (
	// leaseGrace is added to a job's timeout to form its lease, so a handler that
	// respects its timeout always reports before the lease expires. Phase 2
	// replaces this with a short TTL renewed by heartbeats.
	leaseGrace     = 30 * time.Second
	promoteBatch   = 1000
	reportTimeout  = 10 * time.Second
	maxErrorLength = 4096
)

type Worker struct {
	cfg       Config
	store     Store
	handlers  map[string]Handler
	types     []string
	log       *slog.Logger
	nextQueue int // round-robin start; owned by the Run goroutine
}

func New(cfg Config, store Store, reg *Registry, log *slog.Logger) (*Worker, error) {
	switch {
	case cfg.ID == "":
		return nil, errors.New("worker: ID is required")
	case len(cfg.Queues) == 0:
		return nil, errors.New("worker: at least one queue is required")
	case cfg.Concurrency < 1:
		return nil, errors.New("worker: concurrency must be at least 1")
	case cfg.PollInterval <= 0:
		return nil, errors.New("worker: poll interval must be positive")
	case len(reg.handlers) == 0:
		return nil, errors.New("worker: no handlers registered")
	}
	return &Worker{
		cfg:      cfg,
		store:    store,
		handlers: reg.handlers,
		types:    reg.Types(),
		log:      log.With("worker_id", cfg.ID),
	}, nil
}

// Run claims and executes jobs until ctx is cancelled. It then stops claiming
// and waits for in-flight jobs to finish; handlers are not interrupted, and
// each is bounded by its job's timeout.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("worker started", "queues", w.cfg.Queues, "concurrency", w.cfg.Concurrency, "types", w.types)

	var promoter sync.WaitGroup
	promoter.Go(func() { w.promoteLoop(ctx) })

	var running sync.WaitGroup
	done := make(chan struct{}, w.cfg.Concurrency) // never blocks: at most Concurrency jobs in flight
	inflight := 0
	for {
		if free := w.cfg.Concurrency - inflight; free > 0 && ctx.Err() == nil {
			for _, l := range w.claim(ctx, free) {
				inflight++
				running.Go(func() {
					w.execute(l)
					done <- struct{}{}
				})
			}
		}

		select {
		case <-done:
			inflight-- // a slot freed: claim again immediately
		case <-time.After(w.cfg.PollInterval):
		case <-ctx.Done():
			w.log.Info("worker stopping; waiting for in-flight jobs", "inflight", inflight)
			running.Wait()
			promoter.Wait()
			w.log.Info("worker stopped")
			return nil
		}
	}
}

// claim fills up to n free slots, rotating which queue goes first so one busy
// queue cannot starve the others.
func (w *Worker) claim(ctx context.Context, n int) []jobs.Lease {
	var leases []jobs.Lease
	for i := range w.cfg.Queues {
		queue := w.cfg.Queues[(w.nextQueue+i)%len(w.cfg.Queues)]
		got, err := w.store.Claim(ctx, jobs.ClaimParams{
			Queue: queue, Types: w.types, Limit: n - len(leases), WorkerID: w.cfg.ID, Grace: leaseGrace,
		})
		if err != nil {
			if ctx.Err() == nil {
				w.log.Error("claim failed", "queue", queue, "err", err)
			}
			continue
		}
		if leases = append(leases, got...); len(leases) == n {
			break
		}
	}
	w.nextQueue++
	return leases
}

func (w *Worker) execute(l jobs.Lease) {
	log := w.log.With("job_id", l.JobID, "type", l.Type, "attempt", l.Attempt)
	timeout := time.Duration(l.TimeoutSeconds) * time.Second
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	err := call(ctx, w.handlers[l.Type], l.Payload, log)
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = fmt.Errorf("timed out after %s: %w", timeout, err)
	}
	cancel()

	// Report with a fresh context: the result must be recorded even during shutdown.
	rctx, rcancel := context.WithTimeout(context.Background(), reportTimeout)
	defer rcancel()

	if err == nil {
		err := w.store.Complete(rctx, l.JobID, l.Token)
		w.logReport(log, err, "job succeeded", "duration", time.Since(start))
		return
	}
	// Phase 1: every failure is retryable and retried immediately (backoff is Phase 2).
	status, ferr := w.store.Fail(rctx, l.JobID, l.Token, jobs.Failure{Error: truncate(err.Error()), Retryable: true})
	w.logReport(log, ferr, "job failed", "status", status, "err", err, "duration", time.Since(start))
}

func (w *Worker) logReport(log *slog.Logger, err error, msg string, attrs ...any) {
	switch {
	case err == nil:
		log.Info(msg, attrs...)
	case errors.Is(err, jobs.ErrLeaseLost):
		log.Warn("lease lost; result discarded", attrs...)
	default:
		// The job stays running until its lease expires (recovered by the Phase 2 reaper).
		log.Error("could not record result", append(attrs, "report_err", err)...)
	}
}

// call runs a handler, converting a panic into an error so one bad job cannot crash the worker.
func call(ctx context.Context, h Handler, payload []byte, log *slog.Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("handler panic", "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return h(ctx, payload)
}

func (w *Worker) promoteLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := w.store.Promote(ctx, promoteBatch)
			if err != nil && ctx.Err() == nil {
				w.log.Error("promote failed", "err", err)
			} else if n > 0 {
				w.log.Debug("promoted jobs", "count", n)
			}
		}
	}
}

// truncate bounds an error message, cutting on a UTF-8 boundary (Postgres rejects invalid UTF-8).
func truncate(s string) string {
	if len(s) <= maxErrorLength {
		return s
	}
	return strings.ToValidUTF8(s[:maxErrorLength], "") + "…"
}
