// Package config loads Rocky's runtime configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	APIAddr         string
	DatabaseURL     string
	RedisURL        string // optional: Rocky runs correctly without Redis (ADR 0002)
	LogLevel        slog.Level
	LogFormat       string // "text" or "json"
	ShutdownTimeout time.Duration

	WorkerID           string // optional: defaults to hostname plus a random suffix
	WorkerQueues       []string
	WorkerConcurrency  int
	WorkerPollInterval time.Duration
	WorkerLeaseTTL     time.Duration

	RetryBase time.Duration // delay after the first failed attempt; doubles each attempt
	RetryMax  time.Duration
}

func Load() (Config, error) { return load(os.Getenv) }

func load(getenv func(string) string) (Config, error) {
	c := Config{
		APIAddr:     orDefault(getenv("ROCKY_API_ADDR"), ":8080"),
		DatabaseURL: getenv("ROCKY_DATABASE_URL"),
		RedisURL:    getenv("ROCKY_REDIS_URL"),
		LogFormat:   orDefault(getenv("ROCKY_LOG_FORMAT"), "text"),
		WorkerID:    getenv("ROCKY_WORKER_ID"),
	}

	var errs []error
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("ROCKY_DATABASE_URL is required"))
	}
	if c.LogFormat != "text" && c.LogFormat != "json" {
		errs = append(errs, fmt.Errorf("ROCKY_LOG_FORMAT must be text or json, got %q", c.LogFormat))
	}
	if err := c.LogLevel.UnmarshalText([]byte(orDefault(getenv("ROCKY_LOG_LEVEL"), "info"))); err != nil {
		errs = append(errs, fmt.Errorf("ROCKY_LOG_LEVEL: %w", err))
	}
	var err error
	if c.ShutdownTimeout, err = time.ParseDuration(orDefault(getenv("ROCKY_SHUTDOWN_TIMEOUT"), "15s")); err != nil {
		errs = append(errs, fmt.Errorf("ROCKY_SHUTDOWN_TIMEOUT: %w", err))
	}
	for _, q := range strings.Split(orDefault(getenv("ROCKY_WORKER_QUEUES"), "default"), ",") {
		if q = strings.TrimSpace(q); q != "" {
			c.WorkerQueues = append(c.WorkerQueues, q)
		}
	}
	if len(c.WorkerQueues) == 0 {
		errs = append(errs, errors.New("ROCKY_WORKER_QUEUES must name at least one queue"))
	}
	if c.WorkerConcurrency, err = strconv.Atoi(orDefault(getenv("ROCKY_WORKER_CONCURRENCY"), "10")); err != nil || c.WorkerConcurrency < 1 {
		errs = append(errs, errors.New("ROCKY_WORKER_CONCURRENCY must be a positive integer"))
	}
	if c.WorkerPollInterval, err = time.ParseDuration(orDefault(getenv("ROCKY_WORKER_POLL_INTERVAL"), "1s")); err != nil || c.WorkerPollInterval <= 0 {
		errs = append(errs, errors.New("ROCKY_WORKER_POLL_INTERVAL must be a positive duration"))
	}
	for _, d := range []struct {
		name string
		dst  *time.Duration
		def  string
	}{
		{"ROCKY_WORKER_LEASE_TTL", &c.WorkerLeaseTTL, "30s"},
		{"ROCKY_RETRY_BASE", &c.RetryBase, "2s"},
		{"ROCKY_RETRY_MAX", &c.RetryMax, "1h"},
	} {
		if *d.dst, err = time.ParseDuration(orDefault(getenv(d.name), d.def)); err != nil || *d.dst <= 0 {
			errs = append(errs, fmt.Errorf("%s must be a positive duration", d.name))
		}
	}
	if c.RetryMax < c.RetryBase {
		errs = append(errs, errors.New("ROCKY_RETRY_MAX must be at least ROCKY_RETRY_BASE"))
	}
	return c, errors.Join(errs...)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
