package worker

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/aniket0742/rocky/internal/jobs"
	"github.com/aniket0742/rocky/internal/retry"
)

// fakeStore hands out pending leases and records results.
type fakeStore struct {
	mu         sync.Mutex
	pending    []jobs.Lease
	limits     []int
	completed  []uuid.UUID
	failed     map[uuid.UUID]jobs.Failure
	heartbeats int
	dropLeases bool            // Heartbeat renews nothing: every lease is lost
	expired    []jobs.LeaseRef // returned once by ExpiredLeases
	expiredIn  map[uuid.UUID]time.Duration
}

func (f *fakeStore) Claim(_ context.Context, p jobs.ClaimParams) ([]jobs.Lease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limits = append(f.limits, p.Limit)
	n := min(p.Limit, len(f.pending))
	out := f.pending[:n:n]
	f.pending = f.pending[n:]
	return out, nil
}

func (f *fakeStore) Heartbeat(_ context.Context, refs []jobs.LeaseRef, _ time.Duration) ([]uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeats++
	if f.dropLeases {
		return nil, nil
	}
	var ids []uuid.UUID
	for _, r := range refs {
		ids = append(ids, r.JobID)
	}
	return ids, nil
}

func (f *fakeStore) Complete(_ context.Context, id, _ uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completed = append(f.completed, id)
	return nil
}

func (f *fakeStore) Fail(_ context.Context, id, _ uuid.UUID, fl jobs.Failure) (jobs.Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failed == nil {
		f.failed = map[uuid.UUID]jobs.Failure{}
	}
	f.failed[id] = fl
	return jobs.StatusRetrying, nil
}

func (f *fakeStore) Promote(context.Context, int) (int64, error) { return 0, nil }

func (f *fakeStore) ExpiredLeases(context.Context, int) ([]jobs.LeaseRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.expired
	f.expired = nil
	return out, nil
}

func (f *fakeStore) Expire(_ context.Context, l jobs.LeaseRef, retryIn time.Duration) (jobs.Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.expiredIn == nil {
		f.expiredIn = map[uuid.UUID]time.Duration{}
	}
	f.expiredIn[l.JobID] = retryIn
	return jobs.StatusRetrying, nil
}

func (f *fakeStore) reported() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.completed) + len(f.failed)
}

func (f *fakeStore) claimed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pending) == 0
}

func lease(typ string, payload string) jobs.Lease {
	return jobs.Lease{JobID: uuid.New(), Type: typ, Payload: []byte(payload), Attempt: 1, TimeoutSeconds: 30, Token: uuid.New()}
}

func testConfig(concurrency int) Config {
	return Config{
		ID: "test", Queues: []string{"default"}, Concurrency: concurrency,
		PollInterval: 10 * time.Millisecond, LeaseTTL: 60 * time.Millisecond, ShutdownGrace: time.Second,
		Retry: retry.Policy{Base: time.Second, Max: time.Hour},
	}
}

// start runs a worker in the background and returns a stop func that waits for Run to return.
func start(t *testing.T, store Store, reg *Registry, concurrency int) (stop func()) {
	return startWith(t, store, reg, testConfig(concurrency))
}

func startWith(t *testing.T, store Store, reg *Registry, cfg Config) (stop func()) {
	t.Helper()
	w, err := New(cfg, store, reg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- w.Run(ctx) }()
	return func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatal("condition not met within 5s")
}

func TestReportsResults(t *testing.T) {
	reg := NewRegistry()
	var gotPayload atomic.Value
	reg.Register("ok", func(_ context.Context, p []byte) error { gotPayload.Store(string(p)); return nil })
	reg.Register("boom", func(context.Context, []byte) error { return errors.New("boom") })
	reg.Register("panics", func(context.Context, []byte) error { panic("bad handler") })

	ok, boom, panics := lease("ok", `{"n":1}`), lease("boom", `{}`), lease("panics", `{}`)
	store := &fakeStore{pending: []jobs.Lease{ok, boom, panics}}
	stop := start(t, store, reg, 3)
	eventually(t, func() bool { return store.reported() == 3 })
	stop()

	if len(store.completed) != 1 || store.completed[0] != ok.JobID || gotPayload.Load() != `{"n":1}` {
		t.Fatalf("completed = %v, payload = %v", store.completed, gotPayload.Load())
	}
	if f := store.failed[boom.JobID]; f.Error != "boom" || !f.Retryable || f.RetryIn < 800*time.Millisecond || f.RetryIn > 1200*time.Millisecond {
		t.Fatalf("boom failure = %+v (want retryable, ~1s backoff)", f)
	}
	if f := store.failed[panics.JobID]; f.Error != "handler panicked: bad handler" {
		t.Fatalf("panic failure = %+v", f)
	}
}

func TestFailureClassification(t *testing.T) {
	reg := NewRegistry()
	reg.Register("permanent", func(context.Context, []byte) error { return NonRetryable(errors.New("bad input")) })
	reg.Register("transient", func(context.Context, []byte) error { return errors.New("try again") })
	permanent, transient := lease("permanent", `{}`), lease("transient", `{}`)
	transient.Attempt = 3 // third failure: Base·2² = 4s ±20%
	store := &fakeStore{pending: []jobs.Lease{permanent, transient}}
	stop := start(t, store, reg, 2)
	eventually(t, func() bool { return store.reported() == 2 })
	stop()

	if f := store.failed[permanent.JobID]; f.Retryable || f.Error != "bad input" {
		t.Fatalf("permanent failure = %+v", f)
	}
	if f := store.failed[transient.JobID]; !f.Retryable || f.RetryIn < 3200*time.Millisecond || f.RetryIn > 4800*time.Millisecond {
		t.Fatalf("transient failure = %+v (want ~4s backoff)", f)
	}
}

func TestConcurrencyIsBounded(t *testing.T) {
	const limit, total = 3, 10
	var running, peak atomic.Int32
	release := make(chan struct{})
	reg := NewRegistry()
	reg.Register("block", func(context.Context, []byte) error {
		n := running.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		<-release
		running.Add(-1)
		return nil
	})

	store := &fakeStore{}
	for range total {
		store.pending = append(store.pending, lease("block", `{}`))
	}
	stop := start(t, store, reg, limit)
	eventually(t, func() bool { return running.Load() == limit })
	time.Sleep(50 * time.Millisecond) // give the worker a chance to over-claim
	close(release)
	eventually(t, func() bool { return store.reported() == total })
	stop()

	if peak.Load() != limit {
		t.Fatalf("peak concurrency = %d, want %d", peak.Load(), limit)
	}
	for _, l := range store.limits {
		if l < 1 || l > limit {
			t.Fatalf("claimed with limit %d (concurrency %d)", l, limit)
		}
	}
}

func TestTimeoutFailsAttempt(t *testing.T) {
	reg := NewRegistry()
	reg.Register("slow", func(ctx context.Context, _ []byte) error { <-ctx.Done(); return ctx.Err() })
	l := lease("slow", `{}`)
	l.TimeoutSeconds = 1
	store := &fakeStore{pending: []jobs.Lease{l}}
	stop := start(t, store, reg, 1)
	eventually(t, func() bool { return store.reported() == 1 })
	stop()

	if f := store.failed[l.JobID]; !strings.HasPrefix(f.Error, "timed out after 1s") || !f.Retryable {
		t.Fatalf("failure = %+v", f)
	}
}

func TestHeartbeatKeepsLeaseWhileRunning(t *testing.T) {
	reg := NewRegistry()
	reg.Register("slow", func(context.Context, []byte) error { time.Sleep(300 * time.Millisecond); return nil })
	store := &fakeStore{pending: []jobs.Lease{lease("slow", `{}`)}}
	stop := start(t, store, reg, 1) // TTL 60ms: renewals every 20ms
	eventually(t, func() bool { return store.reported() == 1 })
	stop()
	if len(store.completed) != 1 || store.heartbeats < 5 {
		t.Fatalf("completed=%d heartbeats=%d", len(store.completed), store.heartbeats)
	}
}

func TestLostLeaseCancelsHandlerAndDiscardsResult(t *testing.T) {
	cause := make(chan error, 1)
	reg := NewRegistry()
	reg.Register("slow", func(ctx context.Context, _ []byte) error {
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return ctx.Err()
	})
	store := &fakeStore{pending: []jobs.Lease{lease("slow", `{}`)}, dropLeases: true}
	stop := start(t, store, reg, 1)
	defer stop()

	select {
	case err := <-cause:
		if !errors.Is(err, errLeaseLost) {
			t.Fatalf("handler cancelled with %v, want lease lost", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not cancelled after its lease was lost")
	}
	time.Sleep(50 * time.Millisecond)
	if store.reported() != 0 {
		t.Fatal("a worker that lost its lease reported a result")
	}
}

func TestShutdownWaitsForInFlightJobs(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	reg := NewRegistry()
	reg.Register("block", func(context.Context, []byte) error { close(started); <-release; return nil })
	store := &fakeStore{pending: []jobs.Lease{lease("block", `{}`)}}
	stop := start(t, store, reg, 1) // grace 1s
	<-started

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Run returned while a job was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-stopped
	if len(store.completed) != 1 {
		t.Fatal("in-flight job was not reported before shutdown")
	}
}

func TestShutdownInterruptsJobsAfterGrace(t *testing.T) {
	started := make(chan struct{})
	reg := NewRegistry()
	reg.Register("forever", func(ctx context.Context, _ []byte) error { close(started); <-ctx.Done(); return ctx.Err() })
	l := lease("forever", `{}`)
	store := &fakeStore{pending: []jobs.Lease{l}}
	cfg := testConfig(1)
	cfg.ShutdownGrace = 50 * time.Millisecond
	stop := startWith(t, store, reg, cfg)
	<-started
	stop()

	f, ok := store.failed[l.JobID]
	if !ok || f.Error != "interrupted by worker shutdown" || !f.Retryable || f.RetryIn != 0 {
		t.Fatalf("interrupted job failure = %+v (reported=%v); want retryable, no backoff", f, ok)
	}
}

func TestShutdownAbandonsHandlersThatIgnoreCancellation(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	reg := NewRegistry()
	reg.Register("stubborn", func(context.Context, []byte) error { close(started); <-release; return nil })
	store := &fakeStore{pending: []jobs.Lease{lease("stubborn", `{}`)}}
	cfg := testConfig(1)
	cfg.ShutdownGrace = 50 * time.Millisecond
	stop := startWith(t, store, reg, cfg)
	<-started

	begin := time.Now()
	stop() // returns after grace + one lease TTL instead of waiting forever
	if took := time.Since(begin); took > time.Second {
		t.Fatalf("shutdown took %s", took)
	}
	if store.reported() != 0 {
		t.Fatal("abandoned job should be left to lease expiry, not reported")
	}
}

func TestReaperExpiresLeasesWithBackoff(t *testing.T) {
	reg := NewRegistry()
	reg.Register("t", func(context.Context, []byte) error { return nil })
	ref := jobs.LeaseRef{JobID: uuid.New(), Token: uuid.New(), Attempt: 2} // second failure: Base·2 = 2s ±20%
	store := &fakeStore{expired: []jobs.LeaseRef{ref}}
	stop := start(t, store, reg, 1)
	eventually(t, func() bool { store.mu.Lock(); defer store.mu.Unlock(); return store.expiredIn != nil })
	stop()
	if d := store.expiredIn[ref.JobID]; d < 1600*time.Millisecond || d > 2400*time.Millisecond {
		t.Fatalf("expired lease retries in %s, want ~2s", d)
	}
}

func TestTruncateKeepsValidUTF8(t *testing.T) {
	s := truncate(strings.Repeat("é", maxErrorLength)) // 2 bytes each: the cut lands mid-rune
	if !utf8.ValidString(s) || len(s) > maxErrorLength+len("…") {
		t.Fatalf("invalid truncation (len %d)", len(s))
	}
}

func TestNewValidates(t *testing.T) {
	reg := NewRegistry()
	reg.Register("t", func(context.Context, []byte) error { return nil })
	for name, mutate := range map[string]func(*Config){
		"no id":          func(c *Config) { c.ID = "" },
		"no queues":      func(c *Config) { c.Queues = nil },
		"no concurrency": func(c *Config) { c.Concurrency = 0 },
		"no poll":        func(c *Config) { c.PollInterval = 0 },
		"no lease ttl":   func(c *Config) { c.LeaseTTL = 0 },
		"tiny lease ttl": func(c *Config) { c.LeaseTTL = 2 },
		"negative grace": func(c *Config) { c.ShutdownGrace = -1 },
		"no retry base":  func(c *Config) { c.Retry.Base = 0 },
		"max below base": func(c *Config) { c.Retry.Max = c.Retry.Base / 2 },
	} {
		cfg := testConfig(1)
		mutate(&cfg)
		if _, err := New(cfg, &fakeStore{}, reg, slog.New(slog.DiscardHandler)); err == nil {
			t.Errorf("%s: accepted invalid config", name)
		}
	}
	if _, err := New(testConfig(1), &fakeStore{}, NewRegistry(), slog.New(slog.DiscardHandler)); err == nil {
		t.Error("accepted empty registry")
	}
}

func TestRegisterRejectsDuplicates(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate handler")
		}
	}()
	reg := NewRegistry()
	h := func(context.Context, []byte) error { return nil }
	reg.Register("t", h)
	reg.Register("t", h)
}
