package config

import (
	"log/slog"
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
	if c.RedisURL != "" {
		t.Fatalf("redis should be optional, got %q", c.RedisURL)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := load(env(map[string]string{
		"ROCKY_LOG_FORMAT":       "xml",
		"ROCKY_LOG_LEVEL":        "loud",
		"ROCKY_SHUTDOWN_TIMEOUT": "soon",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"ROCKY_DATABASE_URL", "ROCKY_LOG_FORMAT", "ROCKY_LOG_LEVEL", "ROCKY_SHUTDOWN_TIMEOUT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %s: %v", want, err)
		}
	}
}
