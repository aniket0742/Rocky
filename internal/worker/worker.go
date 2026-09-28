// Package worker claims jobs from Postgres, runs their handlers with bounded
// concurrency, keeps their leases alive with heartbeats and reports results
// through fenced (lease-token-checked) writes. Every worker also promotes due
// jobs and reaps expired leases, so no separate coordinator process is needed.
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
	"github.com/aniket0742/rocky/internal/retry"
)

// Store is the persistence the worker needs. *storage.Store implements it.
type Store interface {
	Claim(ctx context.Context, p jobs.ClaimParams) ([]jobs.Lease, error)
	Heartbeat(ctx context.Context, leases []jobs.LeaseRef, ttl time.Duration) ([]uuid.UUID, error)
	Complete(ctx context.Context, jobID, token uuid.UUID) error
	Fail(ctx context.Context, jobID, token uuid.UUID, f jobs.Failure) (jobs.Status, error)
	Promote(ctx context.Context, limit int) (int64, error)
	ExpiredLeases(ctx context.Context, limit int) ([]jobs.LeaseRef, error)
	Expire(ctx context.Context, l jobs.LeaseRef, retryIn time.Duration) (jobs.Status, error)
}

type Config struct {
	ID            string
	Queues        []string
	Concurrency   int           // max handlers running at once
	PollInterval  time.Duration // how often to claim when idle, promote due jobs and reap expired leases
	LeaseTTL      time.Duration // lease length; renewed every LeaseTTL/3 while a job runs
	ShutdownGrace time.Duration // how long in-flight jobs may finish once shutdown begins
	Retry         retry.Policy
}

const (
	promoteBatch   = 1000
	reapBatch      = 100
	reportTimeout  = 10 * time.Second
	maxErrorLength = 4096
)

var (
	errLeaseLost = errors.New("lease lost")
	errShutdown  = errors.New("interrupted by worker shutdown")
)

type Worker struct {
	cfg       Config
	store     Store
	handlers  map[string]Handler
	types     []string
	log       *slog.Logger
	nextQueue int // round-robin start; owned by the Run goroutine

	mu     sync.Mutex
	active map[uuid.UUID]*activeJob
}

// activeJob is a running job whose lease this worker renews.
type activeJob struct {
	ref       jobs.LeaseRef
	deadline  time.Time // job timeout: renewal stops here, so a handler that ignores it loses the lease
	cancel    context.CancelCauseFunc
	lost      bool // lease lost; no longer renewed
	reporting bool // result is being recorded; the report itself detects lease loss
}

func New(cfg Config, store Store, reg *Registry, log *slog.Logger) (*Worker, error) {
	switch {
	case cfg.ID == "":
		return nil, errors.New("worker: ID is required")
	case len(cfg.Queues) == 0:
		return nil, errors.New("worker: at least one queue is required")
	case cfg.Concurrency < 1:
		return nil, errors.New("worker: concurrency must be at least 1")
	case cfg.PollInterval <= 0 || cfg.LeaseTTL/3 <= 0: // heartbeats run every LeaseTTL/3
		return nil, errors.New("worker: poll interval and lease TTL must be positive")
	case cfg.ShutdownGrace < 0:
		return nil, errors.New("worker: shutdown grace must not be negative")
	case cfg.Retry.Base <= 0 || cfg.Retry.Max < cfg.Retry.Base:
		return nil, errors.New("worker: retry base must be positive and not exceed retry max")
	case len(reg.handlers) == 0:
		return nil, errors.New("worker: no handlers registered")
	}
	return &Worker{
		cfg:      cfg,
		store:    store,
		handlers: reg.handlers,
		types:    reg.Types(),
		log:      log.With("worker_id", cfg.ID),
		active:   map[uuid.UUID]*activeJob{},
	}, nil
}

// Run claims and executes jobs until ctx is cancelled, then shuts down
// gracefully (see drain). It returns once no job is left running, or once the
// remaining handlers have been abandoned to lease expiry.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("worker started", "queues", w.cfg.Queues, "concurrency", w.cfg.Concurrency,
		"types", w.types, "lease_ttl", w.cfg.LeaseTTL)

	// Heartbeats outlive ctx: jobs draining during shutdown must keep their leases.
	hbCtx, stopHeartbeats := context.WithCancel(context.Background())
	var loops sync.WaitGroup
	loops.Go(func() { every(hbCtx, w.cfg.LeaseTTL/3, w.heartbeat) })
	loops.Go(func() { every(ctx, w.cfg.PollInterval, w.maintain) })

	var running sync.WaitGroup
	done := make(chan struct{}, w.cfg.Concurrency) // never blocks: at most Concurrency jobs in flight
	inflight := 0
	for ctx.Err() == nil {
		if free := w.cfg.Concurrency - inflight; free > 0 {
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
		}
	}

	w.drain(&running)
	stopHeartbeats()
	loops.Wait()
	w.log.Info("worker stopped")
	return nil
}

// drain is graceful shutdown (spec §9.6). Claiming has already stopped.
//  1. In-flight jobs get ShutdownGrace to finish; heartbeats keep their leases alive.
//  2. Jobs still running are cancelled. Those that return are recorded as
//     interrupted and retried at once (the attempt still counts).
//  3. Handlers that ignore cancellation for one more lease TTL are abandoned;
//     their leases expire and any worker's reaper recovers them.
func (w *Worker) drain(running *sync.WaitGroup) {
	drained := make(chan struct{})
	go func() { running.Wait(); close(drained) }()

	w.log.Info("worker stopping; draining in-flight jobs", "inflight", w.inflight(), "grace", w.cfg.ShutdownGrace)
	select {
	case <-drained:
		return
	case <-time.After(w.cfg.ShutdownGrace):
	}

	w.log.Warn("grace period over; interrupting remaining jobs", "inflight", w.inflight())
	w.mu.Lock()
	for _, j := range w.active {
		j.cancel(errShutdown)
	}
	w.mu.Unlock()
	select {
	case <-drained:
	case <-time.After(w.cfg.LeaseTTL):
		w.log.Error("handlers ignored cancellation; abandoning them to lease expiry", "inflight", w.inflight())
	}
}

// claim fills up to n free slots, rotating which queue goes first so one busy
// queue cannot starve the others.
func (w *Worker) claim(ctx context.Context, n int) []jobs.Lease {
	var leases []jobs.Lease
	for i := range w.cfg.Queues {
		queue := w.cfg.Queues[(w.nextQueue+i)%len(w.cfg.Queues)]
		got, err := w.store.Claim(ctx, jobs.ClaimParams{
			Queue: queue, Types: w.types, Limit: n - len(leases), WorkerID: w.cfg.ID, LeaseTTL: w.cfg.LeaseTTL,
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

	// ctx is cancelled with a cause on lease loss (heartbeat) or shutdown (drain).
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	job := &activeJob{ref: l.Ref(), deadline: start.Add(timeout), cancel: cancel}
	w.mu.Lock()
	w.active[l.JobID] = job
	w.mu.Unlock()
	defer func() { // keep renewing until the result is recorded
		w.mu.Lock()
		delete(w.active, l.JobID)
		w.mu.Unlock()
	}()

	hctx, hcancel := context.WithTimeout(ctx, timeout)
	err := call(hctx, w.handlers[l.Type], l.Payload, log)
	timedOut := errors.Is(context.Cause(hctx), context.DeadlineExceeded)
	hcancel()

	w.mu.Lock()
	cause := context.Cause(ctx)
	job.reporting = true
	w.mu.Unlock()
	if errors.Is(cause, errLeaseLost) {
		log.Warn("lease lost while running; result discarded", "duration", time.Since(start))
		return
	}

	// Report with a fresh context: the result must be recorded even during shutdown.
	rctx, rcancel := context.WithTimeout(context.Background(), reportTimeout)
	defer rcancel()
	if err == nil {
		err := w.store.Complete(rctx, l.JobID, l.Token)
		w.logReport(log, err, "job succeeded", "duration", time.Since(start))
		return
	}
	f := w.failure(err, l.Attempt, cause, timedOut, timeout)
	status, ferr := w.store.Fail(rctx, l.JobID, l.Token, f)
	w.logReport(log, ferr, "job failed", "status", status, "err", f.Error, "retry_in", f.RetryIn, "duration", time.Since(start))
}

// failure classifies a handler error into a retry decision.
func (w *Worker) failure(err error, attempt int, cause error, timedOut bool, timeout time.Duration) jobs.Failure {
	f := jobs.Failure{Error: truncate(err.Error()), Retryable: true, RetryIn: w.cfg.Retry.Delay(attempt)}
	switch {
	case errors.Is(cause, errShutdown):
		// Not the job's fault, so no backoff; the attempt still counts toward max_attempts.
		f.Error, f.RetryIn = errShutdown.Error(), 0
	case timedOut:
		f.Error = truncate(fmt.Sprintf("timed out after %s: %v", timeout, err))
	case isNonRetryable(err):
		f.Retryable = false
	}
	return f
}

func (w *Worker) logReport(log *slog.Logger, err error, msg string, attrs ...any) {
	switch {
	case err == nil:
		log.Info(msg, attrs...)
	case errors.Is(err, jobs.ErrLeaseLost):
		log.Warn("lease lost; result discarded", attrs...)
	default:
		// The lease is no longer renewed, so the reaper recovers the job once it expires.
		log.Error("could not record result", append(attrs, "report_err", err)...)
	}
}

// heartbeat renews the leases of running jobs in one query. A job past its
// timeout is not renewed, so a handler that ignores its deadline loses the
// lease within one TTL. A lease the database refuses to renew is lost: the
// handler is cancelled and its result will be discarded.
func (w *Worker) heartbeat(ctx context.Context) {
	now := time.Now()
	var refs []jobs.LeaseRef
	w.mu.Lock()
	for _, j := range w.active {
		if !j.lost && now.Before(j.deadline) {
			refs = append(refs, j.ref)
		}
	}
	w.mu.Unlock()
	if len(refs) == 0 {
		return
	}

	renewed, err := w.store.Heartbeat(ctx, refs, w.cfg.LeaseTTL)
	if err != nil {
		// Nothing is known to be lost yet. If this persists past the TTL the
		// leases expire, and the next successful heartbeat reports them lost.
		if ctx.Err() == nil {
			w.log.Error("heartbeat failed", "leases", len(refs), "err", err)
		}
		return
	}
	ok := make(map[uuid.UUID]bool, len(renewed))
	for _, id := range renewed {
		ok[id] = true
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for _, r := range refs {
		if j := w.active[r.JobID]; j != nil && j.ref == r && !ok[r.JobID] && !j.reporting {
			w.log.Warn("lease lost; cancelling handler", "job_id", r.JobID, "attempt", r.Attempt)
			j.lost = true
			j.cancel(errLeaseLost)
		}
	}
}

// maintain promotes due jobs and recovers expired leases. Every worker runs
// it; SKIP LOCKED and fenced writes make concurrent runs safe.
func (w *Worker) maintain(ctx context.Context) {
	if n, err := w.store.Promote(ctx, promoteBatch); err != nil {
		if ctx.Err() == nil {
			w.log.Error("promote failed", "err", err)
		}
	} else if n > 0 {
		w.log.Debug("promoted jobs", "count", n)
	}

	expired, err := w.store.ExpiredLeases(ctx, reapBatch)
	if err != nil {
		if ctx.Err() == nil {
			w.log.Error("reaper failed", "err", err)
		}
		return
	}
	for _, ref := range expired {
		status, err := w.store.Expire(ctx, ref, w.cfg.Retry.Delay(ref.Attempt))
		switch {
		case errors.Is(err, jobs.ErrLeaseLost):
			// Renewed, completed or reaped by another worker in the meantime.
		case err != nil:
			if ctx.Err() == nil {
				w.log.Error("expiring lease failed", "job_id", ref.JobID, "err", err)
			}
		default:
			w.log.Warn("recovered expired lease", "job_id", ref.JobID, "attempt", ref.Attempt, "status", status)
		}
	}
}

func (w *Worker) inflight() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.active)
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

// every runs fn every interval until ctx is done.
func every(ctx context.Context, interval time.Duration, fn func(context.Context)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn(ctx)
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
