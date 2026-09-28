package config

import (
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	c, err := load(env(map[string]string{"ROCKY_DATABASE_URL": "postgres://x"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.APIAddr != ":8080" || c.LogFormat != "text" || c.LogLevel != slog.LevelInfo || c.ShutdownTimeout != 15*time.Second {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	if !slices.Equal(c.WorkerQueues, []string{"default"}) || c.WorkerConcurrency != 10 || c.WorkerPollInterval != time.Second {
		t.Fatalf("unexpected worker defaults: %+v", c)
	}
	if c.RedisURL != "" {
		t.Fatalf("redis should be optional, got %q", c.RedisURL)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := load(env(map[string]string{
		"ROCKY_LOG_FORMAT":           "xml",
		"ROCKY_LOG_LEVEL":            "loud",
		"ROCKY_SHUTDOWN_TIMEOUT":     "soon",
		"ROCKY_WORKER_QUEUES":        " , ",
		"ROCKY_WORKER_CONCURRENCY":   "0",
		"ROCKY_WORKER_POLL_INTERVAL": "-1s",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"ROCKY_DATABASE_URL", "ROCKY_LOG_FORMAT", "ROCKY_LOG_LEVEL", "ROCKY_SHUTDOWN_TIMEOUT",
		"ROCKY_WORKER_QUEUES", "ROCKY_WORKER_CONCURRENCY", "ROCKY_WORKER_POLL_INTERVAL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %s: %v", want, err)
		}
	}
}

func TestLoadWorkerQueues(t *testing.T) {
	c, err := load(env(map[string]string{"ROCKY_DATABASE_URL": "x", "ROCKY_WORKER_QUEUES": "emails, reports ,"}))
	if err != nil || !slices.Equal(c.WorkerQueues, []string{"emails", "reports"}) {
		t.Fatalf("queues = %v, err = %v", c.WorkerQueues, err)
	}
}
