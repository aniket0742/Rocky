package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/aniket0742/rocky/internal/jobs"
)

type fakeJobs struct {
	job     jobs.Job
	created bool
	err     error
	got     jobs.EnqueueParams
}

func (f *fakeJobs) Enqueue(_ context.Context, p jobs.EnqueueParams) (jobs.Job, bool, error) {
	f.got = p
	return f.job, f.created, f.err
}

func (f *fakeJobs) Get(_ context.Context, id uuid.UUID) (jobs.Job, error) {
	if id != f.job.ID {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return f.job, nil
}

func (f *fakeJobs) Events(context.Context, uuid.UUID) ([]jobs.Event, error) {
	return []jobs.Event{{ID: 1, Type: "enqueued", Data: json.RawMessage(`{}`)}}, nil
}

func do(store JobStore, method, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	NewHandler(discard, store, nil).ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body errorBody
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	return body.Error.Code
}

func TestCreateJob(t *testing.T) {
	store := &fakeJobs{job: jobs.Job{ID: uuid.New(), Status: jobs.StatusQueued, Priority: jobs.PriorityHigh}, created: true}
	rec := do(store, "POST", "/v1/jobs", `{"type":"send_email","payload":{"to":"a@b.c"},"priority":"high"}`)

	if rec.Code != 201 || rec.Header().Get("Location") != "/v1/jobs/"+store.job.ID.String() {
		t.Fatalf("got %d, Location %q", rec.Code, rec.Header().Get("Location"))
	}
	if g := store.got; g.Type != "send_email" || g.Queue != "default" || g.Priority != jobs.PriorityHigh ||
		g.MaxAttempts != 5 || g.RequestID == "" || string(g.Payload) != `{"to":"a@b.c"}` {
		t.Fatalf("store got %+v", g)
	}
	var job map[string]any
	_ = json.NewDecoder(rec.Body).Decode(&job)
	if job["priority"] != "high" || job["status"] != "queued" {
		t.Fatalf("response = %v", job)
	}
}

func TestCreateJobErrors(t *testing.T) {
	tests := []struct {
		name     string
		store    *fakeJobs
		body     string
		wantCode int
		wantErr  string
	}{
		{"missing type", &fakeJobs{}, `{}`, 400, "VALIDATION_FAILED"},
		{"bad priority", &fakeJobs{}, `{"type":"t","priority":"urgent"}`, 400, "VALIDATION_FAILED"},
		{"unknown field", &fakeJobs{}, `{"type":"t","tpye":"x"}`, 400, "INVALID_BODY"},
		{"malformed", &fakeJobs{}, `{"type":`, 400, "INVALID_BODY"},
		{"too large", &fakeJobs{}, `{"type":"t","payload":"` + strings.Repeat("x", maxRequestBytes) + `"}`, 413, "REQUEST_TOO_LARGE"},
		{"key reused", &fakeJobs{err: jobs.ErrIdempotencyConflict}, `{"type":"t","idempotency_key":"k"}`, 409, "IDEMPOTENCY_KEY_REUSED"},
		{"store failure", &fakeJobs{err: errors.New("pg: connection refused")}, `{"type":"t"}`, 500, "INTERNAL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(tt.store, "POST", "/v1/jobs", tt.body)
			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tt.wantCode, rec.Body)
			}
			if strings.Contains(rec.Body.String(), "connection refused") {
				t.Fatal("internal error leaked to client")
			}
			if code := errorCode(t, rec); code != tt.wantErr {
				t.Fatalf("code = %s, want %s", code, tt.wantErr)
			}
		})
	}
}

func TestCreateJobIdempotentReplay(t *testing.T) {
	store := &fakeJobs{job: jobs.Job{ID: uuid.New()}, created: false}
	rec := do(store, "POST", "/v1/jobs", `{"type":"t","idempotency_key":"order-42"}`)
	if rec.Code != 200 || rec.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("got %d, replay header %q", rec.Code, rec.Header().Get("Idempotent-Replayed"))
	}
}

func TestGetJob(t *testing.T) {
	store := &fakeJobs{job: jobs.Job{ID: uuid.New(), Type: "t"}}
	if rec := do(store, "GET", "/v1/jobs/"+store.job.ID.String(), ""); rec.Code != 200 {
		t.Fatalf("existing job: %d", rec.Code)
	}
	for _, path := range []string{"/v1/jobs/" + uuid.NewString(), "/v1/jobs/not-a-uuid", "/v1/jobs/not-a-uuid/events"} {
		if rec := do(store, "GET", path, ""); rec.Code != 404 || errorCode(t, rec) != "JOB_NOT_FOUND" {
			t.Fatalf("%s: got %d", path, rec.Code)
		}
	}

	rec := do(store, "GET", "/v1/jobs/"+store.job.ID.String()+"/events", "")
	var body struct{ Data []jobs.Event }
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil || rec.Code != 200 || len(body.Data) != 1 {
		t.Fatalf("events: %d %+v %v", rec.Code, body, err)
	}
}

func TestUnknownRoute(t *testing.T) {
	if rec := do(&fakeJobs{}, "GET", "/v2/nope", ""); rec.Code != 404 || errorCode(t, rec) != "NOT_FOUND" {
		t.Fatalf("got %d", rec.Code)
	}
}
