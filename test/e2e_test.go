// Package e2e exercises the Phase 1 definition of done through the public API:
// a job created over HTTP is executed by a real worker, and its full history is
// readable from job_events.
package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aniket0742/rocky/internal/api"
	"github.com/aniket0742/rocky/internal/jobs"
	"github.com/aniket0742/rocky/internal/storage"
	"github.com/aniket0742/rocky/internal/testdb"
	"github.com/aniket0742/rocky/internal/worker"
)

func TestJobLifecycleThroughAPI(t *testing.T) {
	store := storage.New(testdb.New(t))
	log := slog.New(slog.DiscardHandler)
	srv := httptest.NewServer(api.NewHandler(log, store, nil))
	defer srv.Close()

	reg := worker.NewRegistry()
	reg.Register("noop", func(context.Context, []byte) error { return nil })
	reg.Register("fail", func(context.Context, []byte) error { return errors.New("always fails") })
	w, err := worker.New(worker.Config{ID: "e2e-worker", Queues: []string{"default"}, Concurrency: 4, PollInterval: 20 * time.Millisecond}, store, reg, log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { _ = w.Run(ctx); close(stopped) }()
	defer func() { cancel(); <-stopped }()

	t.Run("success", func(t *testing.T) {
		job := create(t, srv, `{"type":"noop","payload":{"to":"a@b.c"}}`, http.StatusCreated)
		done := waitFor(t, srv, job.ID.String(), jobs.StatusSucceeded)
		if done.Attempt != 1 || *done.WorkerID != "e2e-worker" || done.FinishedAt == nil {
			t.Fatalf("finished job: %+v", done)
		}
		wantEvents(t, srv, job.ID.String(), "enqueued", "claimed", "succeeded")
	})

	t.Run("retry then dead letter", func(t *testing.T) {
		job := create(t, srv, `{"type":"fail","max_attempts":2}`, http.StatusCreated)
		dead := waitFor(t, srv, job.ID.String(), jobs.StatusDeadLetter)
		if dead.Attempt != 2 || *dead.LastError != "always fails" {
			t.Fatalf("dead job: %+v", dead)
		}
		wantEvents(t, srv, job.ID.String(),
			"enqueued", "claimed", "failed", "retry_scheduled", "claimed", "failed", "dead_lettered")
	})

	t.Run("idempotent create", func(t *testing.T) {
		body := `{"type":"noop","idempotency_key":"e2e-order-1"}`
		first := create(t, srv, body, http.StatusCreated)
		if again := create(t, srv, body, http.StatusOK); again.ID != first.ID {
			t.Fatalf("replay created a new job: %s != %s", again.ID, first.ID)
		}
	})
}

func create(t *testing.T, srv *httptest.Server, body string, wantStatus int) jobs.Job {
	t.Helper()
	resp, err := http.Post(srv.URL+"/v1/jobs", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST /v1/jobs: status %d, want %d", resp.StatusCode, wantStatus)
	}
	var job jobs.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	return job
}

func getJSON(t *testing.T, srv *httptest.Server, path string, v any) {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", path, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, srv *httptest.Server, id string, want jobs.Status) jobs.Job {
	t.Helper()
	var job jobs.Job
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		getJSON(t, srv, "/v1/jobs/"+id, &job)
		if job.Status == want {
			return job
		}
	}
	t.Fatalf("job %s: status %s, want %s", id, job.Status, want)
	return job
}

func wantEvents(t *testing.T, srv *httptest.Server, id string, want ...string) {
	t.Helper()
	var body struct{ Data []jobs.Event }
	getJSON(t, srv, "/v1/jobs/"+id+"/events", &body)
	var got []string
	for _, e := range body.Data {
		got = append(got, e.Type)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}
