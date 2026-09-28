package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

var discard = slog.New(slog.DiscardHandler)

func TestHealth(t *testing.T) {
	ok := func(context.Context) error { return nil }
	down := func(context.Context) error { return errors.New("boom") }

	tests := []struct {
		name       string
		deps       []Dependency
		wantCode   int
		wantStatus string
		wantChecks map[string]string
	}{
		{"all ok", []Dependency{{"postgres", ok, true}, {"redis", ok, false}},
			200, "ok", map[string]string{"postgres": "ok", "redis": "ok"}},
		{"optional down degrades", []Dependency{{"postgres", ok, true}, {"redis", down, false}},
			200, "degraded", map[string]string{"postgres": "ok", "redis": "down"}},
		{"required down is unavailable", []Dependency{{"postgres", down, true}, {"redis", down, false}},
			503, "unavailable", map[string]string{"postgres": "down", "redis": "down"}},
		{"unconfigured is disabled", []Dependency{{"postgres", ok, true}, {Name: "redis"}},
			200, "ok", map[string]string{"postgres": "ok", "redis": "disabled"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			NewHandler(discard, nil, tt.deps).ServeHTTP(rec, httptest.NewRequest("GET", "/v1/health", nil))

			var got healthResponse
			if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
				t.Fatal(err)
			}
			if rec.Code != tt.wantCode || got.Status != tt.wantStatus || !maps.Equal(got.Checks, tt.wantChecks) {
				t.Fatalf("got %d %+v, want %d %s %v", rec.Code, got, tt.wantCode, tt.wantStatus, tt.wantChecks)
			}
		})
	}
}

func TestRequestID(t *testing.T) {
	h := NewHandler(discard, nil, nil)
	for _, tc := range []struct {
		in   string
		keep bool
	}{{"", false}, {"client-123", true}, {"bad id!", false}} {
		req := httptest.NewRequest("GET", "/v1/health", nil)
		req.Header.Set("X-Request-ID", tc.in)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		got := rec.Header().Get("X-Request-ID")
		if tc.keep && got != tc.in || !tc.keep && !strings.HasPrefix(got, "req_") {
			t.Errorf("X-Request-ID %q -> %q", tc.in, got)
		}
	}
}

func TestPanicReturnsErrorShape(t *testing.T) {
	boom := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") })
	rec := httptest.NewRecorder()
	withRequestID(withRecover(discard, boom)).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	var body errorBody
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if rec.Code != 500 || body.Error.Code != "INTERNAL" || body.Error.RequestID == "" {
		t.Fatalf("got %d %+v", rec.Code, body)
	}
}
