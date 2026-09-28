// Package telemetry sets up Rocky's logging (and, later, metrics and tracing).
package telemetry

import (
	"io"
	"log/slog"
)

func NewLogger(w io.Writer, level slog.Level, format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}
	if format == "json" {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}
