// Package config loads Rocky's runtime configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"
)

type Config struct {
	APIAddr         string
	DatabaseURL     string
	RedisURL        string // optional: Rocky runs correctly without Redis (ADR 0002)
	LogLevel        slog.Level
	LogFormat       string // "text" or "json"
	ShutdownTimeout time.Duration
}

func Load() (Config, error) { return load(os.Getenv) }

func load(getenv func(string) string) (Config, error) {
	c := Config{
		APIAddr:     orDefault(getenv("ROCKY_API_ADDR"), ":8080"),
		DatabaseURL: getenv("ROCKY_DATABASE_URL"),
		RedisURL:    getenv("ROCKY_REDIS_URL"),
		LogFormat:   orDefault(getenv("ROCKY_LOG_FORMAT"), "text"),
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
	return c, errors.Join(errs...)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
