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
)

// fakeStore hands out pending leases and records results.
type fakeStore struct {
	mu        sync.Mutex
	pending   []jobs.Lease
	limits    []int
	completed []uuid.UUID
	failed    map[uuid.UUID]jobs.Failure
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

func (f *fakeStore) reported() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.completed) + len(f.failed)
}

func lease(typ string, payload string) jobs.Lease {
	return jobs.Lease{JobID: uuid.New(), Type: typ, Payload: []byte(payload), Attempt: 1, TimeoutSeconds: 30, Token: uuid.New()}
}

// start runs a worker in the background and returns a stop func that waits for Run to return.
func start(t *testing.T, store Store, reg *Registry, concurrency int) (stop func()) {
	t.Helper()
	w, err := New(Config{ID: "test", Queues: []string{"default"}, Concurrency: concurrency, PollInterval: 10 * time.Millisecond},
		store, reg, slog.New(slog.DiscardHandler))
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
	if f := store.failed[boom.JobID]; f.Error != "boom" || !f.Retryable {
		t.Fatalf("boom failure = %+v", f)
	}
	if f := store.failed[panics.JobID]; f.Error != "handler panicked: bad handler" {
		t.Fatalf("panic failure = %+v", f)
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

	if f := store.failed[l.JobID]; !strings.HasPrefix(f.Error, "timed out after 1s") {
		t.Fatalf("failure = %+v", f)
	}
}

func TestShutdownWaitsForInFlightJobs(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	reg := NewRegistry()
	reg.Register("block", func(context.Context, []byte) error { close(started); <-release; return nil })
	store := &fakeStore{pending: []jobs.Lease{lease("block", `{}`)}}
	stop := start(t, store, reg, 1)
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

func TestTruncateKeepsValidUTF8(t *testing.T) {
	s := truncate(strings.Repeat("é", maxErrorLength)) // 2 bytes each: the cut lands mid-rune
	if !utf8.ValidString(s) || len(s) > maxErrorLength+len("…") {
		t.Fatalf("invalid truncation (len %d)", len(s))
	}
}

func TestNewValidates(t *testing.T) {
	reg := NewRegistry()
	reg.Register("t", func(context.Context, []byte) error { return nil })
	good := Config{ID: "w", Queues: []string{"q"}, Concurrency: 1, PollInterval: time.Second}
	for _, cfg := range []Config{
		{Queues: good.Queues, Concurrency: 1, PollInterval: time.Second},
		{ID: "w", Concurrency: 1, PollInterval: time.Second},
		{ID: "w", Queues: good.Queues, PollInterval: time.Second},
		{ID: "w", Queues: good.Queues, Concurrency: 1},
	} {
		if _, err := New(cfg, &fakeStore{}, reg, slog.New(slog.DiscardHandler)); err == nil {
			t.Errorf("accepted invalid config %+v", cfg)
		}
	}
	if _, err := New(good, &fakeStore{}, NewRegistry(), slog.New(slog.DiscardHandler)); err == nil {
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
