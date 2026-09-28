package storage_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aniket0742/rocky/internal/jobs"
	"github.com/aniket0742/rocky/internal/storage"
	"github.com/aniket0742/rocky/internal/testdb"
)

var ctx = context.Background()

func setup(t *testing.T) (*storage.Store, *pgxpool.Pool) {
	pool := testdb.New(t)
	return storage.New(pool), pool
}

func enqueue(t *testing.T, s *storage.Store, p jobs.EnqueueParams) jobs.Job {
	t.Helper()
	if p.Type == "" {
		p.Type = "echo"
	}
	if err := p.Normalize(); err != nil {
		t.Fatal(err)
	}
	j, _, err := s.Enqueue(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func claim(t *testing.T, s *storage.Store, worker string, limit int) []jobs.Lease {
	t.Helper()
	leases, err := s.Claim(ctx, jobs.ClaimParams{
		Queue: "default", Types: []string{"echo"}, Limit: limit, WorkerID: worker, LeaseTTL: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return leases
}

func claimOne(t *testing.T, s *storage.Store, worker string) jobs.Lease {
	t.Helper()
	leases := claim(t, s, worker, 1)
	if len(leases) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(leases))
	}
	return leases[0]
}

func get(t *testing.T, s *storage.Store, id uuid.UUID) jobs.Job {
	t.Helper()
	j, err := s.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func eventTypes(t *testing.T, s *storage.Store, id uuid.UUID) []string {
	t.Helper()
	events, err := s.Events(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for _, e := range events {
		types = append(types, e.Type)
	}
	return types
}

func wantStatus(t *testing.T, s *storage.Store, id uuid.UUID, want jobs.Status) jobs.Job {
	t.Helper()
	j := get(t, s, id)
	if j.Status != want {
		t.Fatalf("status = %s, want %s", j.Status, want)
	}
	return j
}

func TestEnqueue(t *testing.T) {
	s, _ := setup(t)

	j := enqueue(t, s, jobs.EnqueueParams{Payload: json.RawMessage(`{"to":"a@b.c"}`), RequestID: "req_1"})
	if j.Status != jobs.StatusQueued || j.Queue != "default" || j.Priority != jobs.PriorityNormal || j.Attempt != 0 {
		t.Fatalf("unexpected job: %+v", j)
	}
	events, _ := s.Events(ctx, j.ID)
	if len(events) != 1 || events[0].Type != "enqueued" || string(events[0].Data) != `{"request_id": "req_1"}` {
		t.Fatalf("unexpected events: %+v", events)
	}

	future := time.Now().Add(time.Hour)
	if j := enqueue(t, s, jobs.EnqueueParams{RunAt: &future}); j.Status != jobs.StatusScheduled {
		t.Fatalf("future job status = %s, want scheduled", j.Status)
	}
}

func TestIdempotentEnqueue(t *testing.T) {
	s, pool := setup(t)
	p := jobs.EnqueueParams{Type: "echo", IdempotencyKey: "order-42", Payload: json.RawMessage(`{"a":1,"b":2}`)}
	first := enqueue(t, s, p)

	// Same key and semantically equal payload (different key order/whitespace) replays.
	p.Payload = json.RawMessage(`{ "b": 2, "a": 1 }`)
	_ = p.Normalize()
	again, created, err := s.Enqueue(ctx, p)
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("replay: id=%v created=%v err=%v", again.ID, created, err)
	}

	p.Payload = json.RawMessage(`{"a":999}`)
	if _, _, err := s.Enqueue(ctx, p); !errors.Is(err, jobs.ErrIdempotencyConflict) {
		t.Fatalf("different payload: err = %v, want ErrIdempotencyConflict", err)
	}

	// Concurrent submissions with a new key create exactly one job.
	p = jobs.EnqueueParams{Type: "echo", IdempotencyKey: "race"}
	_ = p.Normalize()
	var wg sync.WaitGroup
	ids := make([]uuid.UUID, 20)
	for i := range ids {
		wg.Go(func() {
			j, _, err := s.Enqueue(ctx, p)
			if err != nil {
				t.Error(err)
			}
			ids[i] = j.ID
		})
	}
	wg.Wait()
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE idempotency_key = 'race'`).Scan(&n)
	if n != 1 || slices.ContainsFunc(ids, func(id uuid.UUID) bool { return id != ids[0] }) {
		t.Fatalf("concurrent enqueue created %d jobs (ids %v)", n, ids)
	}
}

func TestConcurrentClaimsAreExclusive(t *testing.T) {
	s, _ := setup(t)
	const total = 60
	for range total {
		enqueue(t, s, jobs.EnqueueParams{})
	}

	var mu sync.Mutex
	seen := map[uuid.UUID]int{}
	var wg sync.WaitGroup
	for w := range 10 {
		wg.Go(func() {
			for {
				leases, err := s.Claim(ctx, jobs.ClaimParams{
					Queue: "default", Types: []string{"echo"}, Limit: 3, WorkerID: fmt.Sprint("worker-", w), LeaseTTL: time.Minute,
				})
				if err != nil {
					t.Error(err)
					return
				}
				if len(leases) == 0 {
					return
				}
				mu.Lock()
				for _, l := range leases {
					seen[l.JobID]++
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(seen) != total {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %s claimed %d times", id, n)
		}
	}
}

func TestClaimOrderAndFilters(t *testing.T) {
	s, _ := setup(t)
	low := enqueue(t, s, jobs.EnqueueParams{Priority: jobs.PriorityLow})
	critical := enqueue(t, s, jobs.EnqueueParams{Priority: jobs.PriorityCritical})
	enqueue(t, s, jobs.EnqueueParams{Type: "unregistered"})
	enqueue(t, s, jobs.EnqueueParams{Queue: "other"})
	future := time.Now().Add(time.Hour)
	enqueue(t, s, jobs.EnqueueParams{RunAt: &future})

	if l := claimOne(t, s, "w"); l.JobID != critical.ID {
		t.Fatal("critical job should be claimed first")
	}
	if l := claimOne(t, s, "w"); l.JobID != low.ID {
		t.Fatal("low job should be claimed second")
	}
	if leases := claim(t, s, "w", 10); len(leases) != 0 {
		t.Fatalf("claimed %d jobs of another type/queue or scheduled for later", len(leases))
	}
}

func TestComplete(t *testing.T) {
	s, _ := setup(t)
	j := enqueue(t, s, jobs.EnqueueParams{})
	l := claimOne(t, s, "worker-1")
	if l.Attempt != 1 || l.Token == uuid.Nil {
		t.Fatalf("unexpected lease: %+v", l)
	}
	running := wantStatus(t, s, j.ID, jobs.StatusRunning)
	if *running.WorkerID != "worker-1" || running.LeaseExpiresAt == nil {
		t.Fatalf("running job missing lease info: %+v", running)
	}

	if err := s.Complete(ctx, l.JobID, l.Token); err != nil {
		t.Fatal(err)
	}
	done := wantStatus(t, s, j.ID, jobs.StatusSucceeded)
	if done.FinishedAt == nil || done.LeaseExpiresAt != nil {
		t.Fatalf("succeeded job: %+v", done)
	}
	if err := s.Complete(ctx, l.JobID, l.Token); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatalf("second completion: err = %v, want ErrLeaseLost", err)
	}
	attempts, _ := s.Attempts(ctx, j.ID)
	if len(attempts) != 1 || attempts[0].Status != "succeeded" || attempts[0].EndedAt == nil {
		t.Fatalf("attempts: %+v", attempts)
	}
	if got := eventTypes(t, s, j.ID); !slices.Equal(got, []string{"enqueued", "claimed", "succeeded"}) {
		t.Fatalf("events = %v", got)
	}
}

// A worker whose lease was superseded (or who has the wrong token) cannot change the job.
func TestStaleWorkerIsFenced(t *testing.T) {
	s, _ := setup(t)
	j := enqueue(t, s, jobs.EnqueueParams{})
	a := claimOne(t, s, "worker-a")

	if err := s.Complete(ctx, j.ID, uuid.New()); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatalf("wrong token: err = %v, want ErrLeaseLost", err)
	}
	wantStatus(t, s, j.ID, jobs.StatusRunning)

	// Attempt 1 fails; the job is promoted and worker B claims attempt 2.
	if _, err := s.Fail(ctx, j.ID, a.Token, jobs.Failure{Error: "boom", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Promote(ctx, 100); err != nil {
		t.Fatal(err)
	}
	b := claimOne(t, s, "worker-b")
	if b.Attempt != 2 || b.Token == a.Token {
		t.Fatalf("second lease: %+v", b)
	}

	// Worker A comes back with its old token: every write is rejected.
	if err := s.Complete(ctx, j.ID, a.Token); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatalf("stale complete: err = %v", err)
	}
	if _, err := s.Fail(ctx, j.ID, a.Token, jobs.Failure{Error: "late"}); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatalf("stale fail: err = %v", err)
	}
	if running := wantStatus(t, s, j.ID, jobs.StatusRunning); *running.WorkerID != "worker-b" {
		t.Fatalf("worker = %s, want worker-b", *running.WorkerID)
	}
	if err := s.Complete(ctx, j.ID, b.Token); err != nil {
		t.Fatal(err)
	}
}

// Once a lease has expired it cannot be used, even if nobody reclaimed the job.
func TestStrictLeaseExpiry(t *testing.T) {
	s, pool := setup(t)
	j := enqueue(t, s, jobs.EnqueueParams{})
	l := claimOne(t, s, "w")
	if _, err := pool.Exec(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 millisecond' WHERE id = $1`, j.ID); err != nil {
		t.Fatal(err)
	}

	if err := s.Complete(ctx, j.ID, l.Token); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatalf("complete after expiry: err = %v, want ErrLeaseLost", err)
	}
	if _, err := s.Fail(ctx, j.ID, l.Token, jobs.Failure{Error: "x", Retryable: true}); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatalf("fail after expiry: err = %v, want ErrLeaseLost", err)
	}
	wantStatus(t, s, j.ID, jobs.StatusRunning) // until the reaper expires it (TestExpireRecoversLease)
}

func TestRetryWaitsForBackoff(t *testing.T) {
	s, _ := setup(t)
	j := enqueue(t, s, jobs.EnqueueParams{})
	l := claimOne(t, s, "w")
	status, err := s.Fail(ctx, j.ID, l.Token, jobs.Failure{Error: "timeout", Retryable: true, RetryIn: time.Hour})
	if err != nil || status != jobs.StatusRetrying {
		t.Fatalf("status=%s err=%v", status, err)
	}
	if n, _ := s.Promote(ctx, 100); n != 0 {
		t.Fatal("promoted a job before its run_at")
	}
	if leases := claim(t, s, "w", 1); len(leases) != 0 {
		t.Fatal("claimed a retrying job before its run_at")
	}
}

// The full failure timeline: retry once, then dead-letter when the budget is spent.
func TestRetryThenDeadLetter(t *testing.T) {
	s, _ := setup(t)
	j := enqueue(t, s, jobs.EnqueueParams{MaxAttempts: 2})

	l := claimOne(t, s, "w")
	if _, err := s.Fail(ctx, j.ID, l.Token, jobs.Failure{Error: "timeout", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Promote(ctx, 100); n != 1 {
		t.Fatalf("promoted %d jobs, want 1", n)
	}
	l = claimOne(t, s, "w")
	status, err := s.Fail(ctx, j.ID, l.Token, jobs.Failure{Error: "timeout again", Retryable: true})
	if err != nil || status != jobs.StatusDeadLetter {
		t.Fatalf("status=%s err=%v", status, err)
	}

	dead := wantStatus(t, s, j.ID, jobs.StatusDeadLetter)
	if dead.Attempt != 2 || dead.FinishedAt == nil || *dead.LastError != "timeout again" {
		t.Fatalf("dead job: %+v", dead)
	}
	want := []string{"enqueued", "claimed", "failed", "retry_scheduled", "claimed", "failed", "dead_lettered"}
	if got := eventTypes(t, s, j.ID); !slices.Equal(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	events, _ := s.Events(ctx, j.ID)
	if last := string(events[len(events)-1].Data); last != `{"reason": "attempts_exhausted"}` {
		t.Fatalf("dead_lettered data = %s", last)
	}
	attempts, _ := s.Attempts(ctx, j.ID)
	if len(attempts) != 2 || attempts[0].Status != "failed" || *attempts[1].Error != "timeout again" {
		t.Fatalf("attempts: %+v", attempts)
	}
}

func TestNonRetryableFailure(t *testing.T) {
	s, _ := setup(t)
	j := enqueue(t, s, jobs.EnqueueParams{MaxAttempts: 5})
	l := claimOne(t, s, "w")
	status, err := s.Fail(ctx, j.ID, l.Token, jobs.Failure{Error: "bad input", Retryable: false})
	if err != nil || status != jobs.StatusDeadLetter {
		t.Fatalf("status=%s err=%v", status, err)
	}
	events, _ := s.Events(ctx, j.ID)
	if last := string(events[len(events)-1].Data); last != `{"reason": "non_retryable"}` {
		t.Fatalf("dead_lettered data = %s", last)
	}
}

func expireNow(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 millisecond' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeatRenewsOnlyValidLeases(t *testing.T) {
	s, pool := setup(t)
	for range 3 {
		enqueue(t, s, jobs.EnqueueParams{})
	}
	leases := claim(t, s, "w", 3)
	valid, expired, wrongToken := leases[0], leases[1], leases[2]
	expireNow(t, pool, expired.JobID)
	wrongToken.Token = uuid.New()

	before := get(t, s, valid.JobID).LeaseExpiresAt
	renewed, err := s.Heartbeat(ctx, []jobs.LeaseRef{valid.Ref(), expired.Ref(), wrongToken.Ref()}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(renewed) != 1 || renewed[0] != valid.JobID {
		t.Fatalf("renewed %v, want only %s", renewed, valid.JobID)
	}
	if after := get(t, s, valid.JobID).LeaseExpiresAt; !after.After(before.Add(50 * time.Minute)) {
		t.Fatalf("lease not extended: %s -> %s", before, after)
	}
	if got := eventTypes(t, s, valid.JobID); len(got) != 2 {
		t.Fatalf("heartbeats must not write events: %v", got)
	}
}

// An expired lease becomes a failed attempt with backoff; the stale holder stays fenced out.
func TestExpireRecoversLease(t *testing.T) {
	s, pool := setup(t)
	j := enqueue(t, s, jobs.EnqueueParams{})
	l := claimOne(t, s, "crashed-worker")

	if refs, _ := s.ExpiredLeases(ctx, 10); len(refs) != 0 {
		t.Fatal("a live lease was reported as expired")
	}
	if _, err := s.Expire(ctx, l.Ref(), 0); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatalf("expiring a live lease: err = %v, want ErrLeaseLost", err)
	}

	expireNow(t, pool, j.ID)
	refs, err := s.ExpiredLeases(ctx, 10)
	if err != nil || len(refs) != 1 || refs[0] != l.Ref() {
		t.Fatalf("expired leases = %v, err = %v", refs, err)
	}
	status, err := s.Expire(ctx, refs[0], time.Minute)
	if err != nil || status != jobs.StatusRetrying {
		t.Fatalf("status=%s err=%v", status, err)
	}
	job := wantStatus(t, s, j.ID, jobs.StatusRetrying)
	if d := time.Until(job.RunAt); d < 50*time.Second || *job.LastError != "lease expired" {
		t.Fatalf("retry in %s, last_error %q", d, *job.LastError)
	}
	if _, err := s.Expire(ctx, refs[0], 0); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatal("the same lease was expired twice")
	}
	if err := s.Complete(ctx, j.ID, l.Token); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatal("crashed worker completed a job after its lease was recovered")
	}

	events, _ := s.Events(ctx, j.ID)
	if got := eventTypes(t, s, j.ID); !slices.Equal(got, []string{"enqueued", "claimed", "lease_expired", "retry_scheduled"}) {
		t.Fatalf("events = %v", got)
	}
	if *events[2].WorkerID != "crashed-worker" {
		t.Fatalf("lease_expired should name the worker that lost it: %+v", events[2])
	}
	if attempts, _ := s.Attempts(ctx, j.ID); attempts[0].Status != "lease_expired" {
		t.Fatalf("attempt status = %s", attempts[0].Status)
	}
}

// A job that keeps crashing its worker still reaches dead-letter.
func TestExpireOnLastAttemptDeadLetters(t *testing.T) {
	s, pool := setup(t)
	j := enqueue(t, s, jobs.EnqueueParams{MaxAttempts: 1})
	l := claimOne(t, s, "w")
	expireNow(t, pool, j.ID)
	if status, err := s.Expire(ctx, l.Ref(), time.Minute); err != nil || status != jobs.StatusDeadLetter {
		t.Fatalf("status=%s err=%v", status, err)
	}
	events, _ := s.Events(ctx, j.ID)
	if last := events[len(events)-1]; last.Type != "dead_lettered" || string(last.Data) != `{"reason": "attempts_exhausted"}` {
		t.Fatalf("last event = %s %s", last.Type, last.Data)
	}
}

// Around expiry, the holder's report and the reaper race; exactly one may win.
func TestReportAndReaperAreMutuallyExclusive(t *testing.T) {
	s, pool := setup(t)
	for i := range 20 {
		enqueue(t, s, jobs.EnqueueParams{})
		l := claimOne(t, s, "w")
		if i%2 == 0 {
			expireNow(t, pool, l.JobID)
		}
		var wg sync.WaitGroup
		var completeErr, expireErr error
		wg.Go(func() { completeErr = s.Complete(ctx, l.JobID, l.Token) })
		wg.Go(func() { _, expireErr = s.Expire(ctx, l.Ref(), 0) })
		wg.Wait()
		if (completeErr == nil) == (expireErr == nil) {
			t.Fatalf("round %d: complete err=%v, expire err=%v; want exactly one success", i, completeErr, expireErr)
		}
	}
}

// The database itself rejects impossible states, whatever the Go code does.
func TestSchemaInvariants(t *testing.T) {
	s, pool := setup(t)
	j := enqueue(t, s, jobs.EnqueueParams{MaxAttempts: 1})
	for _, stmt := range []string{
		`UPDATE jobs SET status = 'running' WHERE id = $1`,              // running without a lease
		`UPDATE jobs SET status = 'succeeded' WHERE id = $1`,            // terminal without finished_at
		`UPDATE jobs SET attempt = 2 WHERE id = $1`,                     // over the attempt budget
		`UPDATE jobs SET lease_token = gen_random_uuid() WHERE id = $1`, // lease while queued
	} {
		if _, err := pool.Exec(ctx, stmt, j.ID); err == nil {
			t.Errorf("accepted invalid state: %s", stmt)
		}
	}
}
