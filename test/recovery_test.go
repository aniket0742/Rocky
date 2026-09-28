package e2e

// Failure and recovery scenarios (spec §10), run against real Postgres and real workers.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/aniket0742/rocky/internal/jobs"
	"github.com/aniket0742/rocky/internal/retry"
	"github.com/aniket0742/rocky/internal/storage"
	"github.com/aniket0742/rocky/internal/testdb"
	"github.com/aniket0742/rocky/internal/worker"
)

var ctx = context.Background()

const ttl = 300 * time.Millisecond

func testWorkerConfig(id string) worker.Config {
	return worker.Config{
		ID: id, Queues: []string{"default"}, Concurrency: 4,
		PollInterval: 20 * time.Millisecond, LeaseTTL: ttl, ShutdownGrace: 2 * time.Second,
		Retry: retry.Policy{Base: 50 * time.Millisecond, Max: time.Second},
	}
}

func handlers(h map[string]worker.Handler) *worker.Registry {
	reg := worker.NewRegistry()
	for typ, fn := range h {
		reg.Register(typ, fn)
	}
	return reg
}

// runWorker starts a worker; the returned stop func cancels it and waits for Run to return.
func runWorker(t *testing.T, store worker.Store, cfg worker.Config, reg *worker.Registry) (stop func()) {
	t.Helper()
	w, err := worker.New(cfg, store, reg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	c, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = w.Run(c); close(done) }()
	var stopped atomic.Bool
	stop = func() {
		if stopped.CompareAndSwap(false, true) {
			cancel()
			<-done
		}
	}
	t.Cleanup(stop)
	return stop
}

// reaperOnly is a worker that handles none of the test's job types, so it only promotes and reaps.
func reaperOnly(t *testing.T, store worker.Store) {
	runWorker(t, store, testWorkerConfig("reaper"), handlers(map[string]worker.Handler{
		"unrelated": func(context.Context, []byte) error { return nil },
	}))
}

func newJob(t *testing.T, store *storage.Store, p jobs.EnqueueParams) jobs.Job {
	t.Helper()
	if err := p.Normalize(); err != nil {
		t.Fatal(err)
	}
	j, _, err := store.Enqueue(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func awaitStatus(t *testing.T, store *storage.Store, id uuid.UUID, want jobs.Status) jobs.Job {
	t.Helper()
	var j jobs.Job
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		var err error
		if j, err = store.Get(ctx, id); err != nil {
			t.Fatal(err)
		}
		if j.Status == want {
			return j
		}
	}
	t.Fatalf("job status %s, want %s", j.Status, want)
	return j
}

// history returns "type@worker" for each event, e.g. "claimed@worker-a".
func history(t *testing.T, store *storage.Store, id uuid.UUID) []string {
	t.Helper()
	events, err := store.Events(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range events {
		s := e.Type
		if e.WorkerID != nil {
			s += "@" + *e.WorkerID
		}
		out = append(out, s)
	}
	return out
}

func wantHistory(t *testing.T, store *storage.Store, id uuid.UUID, want ...string) {
	t.Helper()
	if got := history(t, store, id); !slices.Equal(got, want) {
		t.Fatalf("history:\n got %v\nwant %v", got, want)
	}
}

func claimAs(t *testing.T, store *storage.Store, workerID, typ string) jobs.Lease {
	t.Helper()
	leases, err := store.Claim(ctx, jobs.ClaimParams{Queue: "default", Types: []string{typ}, Limit: 1, WorkerID: workerID, LeaseTTL: ttl})
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v (%d leases)", err, len(leases))
	}
	return leases[0]
}

// A worker claims a job and dies. Its lease expires, another worker's reaper
// records the lost attempt, and the job is retried and completed.
func TestCrashedWorkerIsRecovered(t *testing.T) {
	store := storage.New(testdb.New(t))
	j := newJob(t, store, jobs.EnqueueParams{Type: "noop"})
	crashed := claimAs(t, store, "crashed", "noop") // never heartbeats or reports

	began := time.Now()
	runWorker(t, store, testWorkerConfig("survivor"), handlers(map[string]worker.Handler{
		"noop": func(context.Context, []byte) error { return nil },
	}))
	done := awaitStatus(t, store, j.ID, jobs.StatusSucceeded)
	t.Logf("recovered and completed %s after the crash (lease TTL %s)", time.Since(began).Round(time.Millisecond), ttl)

	if done.Attempt != 2 || *done.WorkerID != "survivor" {
		t.Fatalf("job: attempt %d, worker %s", done.Attempt, *done.WorkerID)
	}
	wantHistory(t, store, j.ID, "enqueued", "claimed@crashed", "lease_expired@crashed", "retry_scheduled",
		"claimed@survivor", "succeeded@survivor")
	if err := store.Complete(ctx, j.ID, crashed.Token); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatalf("crashed worker's late completion: err = %v, want ErrLeaseLost", err)
	}
}

// partitionedStore simulates a worker cut off from Postgres for heartbeats only.
type partitionedStore struct {
	*storage.Store
	cut atomic.Bool
}

func (p *partitionedStore) Heartbeat(c context.Context, l []jobs.LeaseRef, d time.Duration) ([]uuid.UUID, error) {
	if p.cut.Load() {
		return nil, errors.New("network partition")
	}
	return p.Store.Heartbeat(c, l, d)
}

// A partitioned (or paused) worker keeps running its handler after losing the
// lease. The job runs twice, as at-least-once allows, but only the current
// lease holder's result is recorded: the zombie's writes are fenced out.
func TestPartitionedWorkerIsFencedOut(t *testing.T) {
	store := storage.New(testdb.New(t))
	j := newJob(t, store, jobs.EnqueueParams{Type: "work"})

	release := make(chan struct{})
	var zombieFinished atomic.Bool
	partitioned := &partitionedStore{Store: store}
	zombieCfg := testWorkerConfig("zombie")
	zombieCfg.Concurrency = 1 // its only slot stays busy with the stuck handler
	runWorker(t, partitioned, zombieCfg, handlers(map[string]worker.Handler{
		"work": func(context.Context, []byte) error { <-release; zombieFinished.Store(true); return nil },
	}))
	awaitStatus(t, store, j.ID, jobs.StatusRunning)
	partitioned.cut.Store(true)

	runWorker(t, store, testWorkerConfig("healthy"), handlers(map[string]worker.Handler{
		"work": func(context.Context, []byte) error { return nil },
	}))
	awaitStatus(t, store, j.ID, jobs.StatusSucceeded)

	partitioned.cut.Store(false) // the partition heals and the zombie finishes its (duplicate) run
	close(release)
	for deadline := time.Now().Add(5 * time.Second); !zombieFinished.Load() && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(3 * ttl) // give the zombie time to try to report and heartbeat

	final := awaitStatus(t, store, j.ID, jobs.StatusSucceeded)
	if !zombieFinished.Load() || final.Attempt != 2 || *final.WorkerID != "healthy" {
		t.Fatalf("zombie finished=%v, job attempt=%d worker=%s", zombieFinished.Load(), final.Attempt, *final.WorkerID)
	}
	wantHistory(t, store, j.ID, "enqueued", "claimed@zombie", "lease_expired@zombie", "retry_scheduled",
		"claimed@healthy", "succeeded@healthy")
}

// A job that kills every worker that runs it still reaches dead-letter:
// each expired lease counts as a failed attempt.
func TestJobThatCrashesEveryWorkerDeadLetters(t *testing.T) {
	store := storage.New(testdb.New(t))
	reaperOnly(t, store)
	j := newJob(t, store, jobs.EnqueueParams{Type: "poison", MaxAttempts: 2})

	claimAs(t, store, "crashed-1", "poison")
	awaitStatus(t, store, j.ID, jobs.StatusQueued) // expired, retried after backoff
	claimAs(t, store, "crashed-2", "poison")
	dead := awaitStatus(t, store, j.ID, jobs.StatusDeadLetter)

	if *dead.LastError != "lease expired" {
		t.Fatalf("last_error = %q", *dead.LastError)
	}
	wantHistory(t, store, j.ID, "enqueued", "claimed@crashed-1", "lease_expired@crashed-1", "retry_scheduled",
		"claimed@crashed-2", "lease_expired@crashed-2", "dead_lettered")
}

// Heartbeats keep a lease alive for a job that runs far longer than the TTL,
// even while another worker's reaper is watching.
func TestHeartbeatsKeepLongJobAlive(t *testing.T) {
	store := storage.New(testdb.New(t))
	reaperOnly(t, store)
	j := newJob(t, store, jobs.EnqueueParams{Type: "long"})
	runWorker(t, store, testWorkerConfig("w"), handlers(map[string]worker.Handler{
		"long": func(context.Context, []byte) error { time.Sleep(5 * ttl); return nil },
	}))
	if done := awaitStatus(t, store, j.ID, jobs.StatusSucceeded); done.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", done.Attempt)
	}
	wantHistory(t, store, j.ID, "enqueued", "claimed@w", "succeeded@w")
}

// A handler that ignores its timeout cannot hold a job forever: renewal stops
// at the deadline, the lease expires and the reaper takes the job back.
func TestHandlerIgnoringTimeoutLosesLease(t *testing.T) {
	store := storage.New(testdb.New(t))
	j := newJob(t, store, jobs.EnqueueParams{Type: "stuck", TimeoutSeconds: 1, MaxAttempts: 1})
	release := make(chan struct{})
	stop := runWorker(t, store, testWorkerConfig("w"), handlers(map[string]worker.Handler{
		"stuck": func(context.Context, []byte) error { <-release; return nil }, // ignores ctx
	}))

	began := time.Now()
	dead := awaitStatus(t, store, j.ID, jobs.StatusDeadLetter)
	t.Logf("stuck job reclaimed %s after claim (timeout 1s, TTL %s)", time.Since(began).Round(time.Millisecond), ttl)
	if *dead.LastError != "lease expired" {
		t.Fatalf("last_error = %q", *dead.LastError)
	}

	close(release) // the handler finally returns; its result must be discarded
	stop()
	wantHistory(t, store, j.ID, "enqueued", "claimed@w", "lease_expired@w", "dead_lettered")
}

// Retry delays grow exponentially with ±20% jitter (Base·2^(n-1)).
func TestBackoffIsExponential(t *testing.T) {
	store := storage.New(testdb.New(t))
	cfg := testWorkerConfig("w")
	cfg.Retry = retry.Policy{Base: 200 * time.Millisecond, Max: 10 * time.Second}
	j := newJob(t, store, jobs.EnqueueParams{Type: "fail", MaxAttempts: 4})
	runWorker(t, store, cfg, handlers(map[string]worker.Handler{
		"fail": func(context.Context, []byte) error { return errors.New("downstream unavailable") },
	}))
	awaitStatus(t, store, j.ID, jobs.StatusDeadLetter)

	events, _ := store.Events(ctx, j.ID)
	var delays []time.Duration
	for _, e := range events {
		if e.Type == "retry_scheduled" {
			var d struct {
				RetryAt time.Time `json:"retry_at"`
			}
			if err := json.Unmarshal(e.Data, &d); err != nil {
				t.Fatal(err)
			}
			delays = append(delays, d.RetryAt.Sub(e.CreatedAt)) // both use the transaction's now()
		}
	}
	t.Logf("retry delays: %v", delays)
	if len(delays) != 3 {
		t.Fatalf("got %d retries, want 3", len(delays))
	}
	for i, d := range delays {
		want := cfg.Retry.Base << i
		if d < want*8/10 || d > want*12/10 {
			t.Errorf("retry %d: delay %s, want %s ±20%%", i+1, d, want)
		}
	}
}

func TestNonRetryableErrorDeadLettersImmediately(t *testing.T) {
	store := storage.New(testdb.New(t))
	j := newJob(t, store, jobs.EnqueueParams{Type: "bad_input", MaxAttempts: 5})
	runWorker(t, store, testWorkerConfig("w"), handlers(map[string]worker.Handler{
		"bad_input": func(context.Context, []byte) error { return worker.NonRetryable(errors.New("malformed payload")) },
	}))
	dead := awaitStatus(t, store, j.ID, jobs.StatusDeadLetter)
	events, _ := store.Events(ctx, j.ID)
	if dead.Attempt != 1 || string(events[len(events)-1].Data) != `{"reason": "non_retryable"}` {
		t.Fatalf("attempt %d, last event %s", dead.Attempt, events[len(events)-1].Data)
	}
}

// Shutdown with a long grace period: the job finishes, and heartbeats keep its
// lease alive throughout the drain (a watching reaper never expires it).
func TestShutdownDrainKeepsLeasesAlive(t *testing.T) {
	store := storage.New(testdb.New(t))
	reaperOnly(t, store)
	j := newJob(t, store, jobs.EnqueueParams{Type: "long"})
	stop := runWorker(t, store, testWorkerConfig("w"), handlers(map[string]worker.Handler{
		"long": func(context.Context, []byte) error { time.Sleep(4 * ttl); return nil },
	}))
	awaitStatus(t, store, j.ID, jobs.StatusRunning)
	stop() // returns once the job has finished

	if done := awaitStatus(t, store, j.ID, jobs.StatusSucceeded); done.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", done.Attempt)
	}
	wantHistory(t, store, j.ID, "enqueued", "claimed@w", "succeeded@w")
}

// Shutdown with a short grace period: the running job is interrupted, recorded
// as an interrupted attempt and retried at once by another worker.
func TestShutdownInterruptsAndRetriesElsewhere(t *testing.T) {
	store := storage.New(testdb.New(t))
	j := newJob(t, store, jobs.EnqueueParams{Type: "long", MaxAttempts: 3})
	cfg := testWorkerConfig("stopping")
	cfg.ShutdownGrace = 100 * time.Millisecond
	stop := runWorker(t, store, cfg, handlers(map[string]worker.Handler{
		"long": func(c context.Context, _ []byte) error { <-c.Done(); return c.Err() },
	}))
	awaitStatus(t, store, j.ID, jobs.StatusRunning)
	stop()

	interrupted, _ := store.Get(ctx, j.ID)
	if interrupted.Status != jobs.StatusRetrying && interrupted.Status != jobs.StatusQueued {
		t.Fatalf("after shutdown: status %s", interrupted.Status)
	}
	if !interrupted.RunAt.Before(time.Now()) || *interrupted.LastError != "interrupted by worker shutdown" {
		t.Fatalf("interrupted job should retry at once: run_at %s, last_error %q", interrupted.RunAt, *interrupted.LastError)
	}

	runWorker(t, store, testWorkerConfig("next"), handlers(map[string]worker.Handler{
		"long": func(context.Context, []byte) error { return nil },
	}))
	awaitStatus(t, store, j.ID, jobs.StatusSucceeded)
	wantHistory(t, store, j.ID, "enqueued", "claimed@stopping", "failed@stopping", "retry_scheduled",
		"claimed@next", "succeeded@next")
}
