// Command worker runs a Rocky worker with the built-in demo handlers.
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aniket0742/rocky/internal/config"
	"github.com/aniket0742/rocky/internal/storage"
	"github.com/aniket0742/rocky/internal/telemetry"
	"github.com/aniket0742/rocky/internal/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := telemetry.NewLogger(os.Stdout, cfg.LogLevel, cfg.LogFormat)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()

	id := cfg.WorkerID
	if id == "" {
		host, _ := os.Hostname()
		id = host + "-" + strings.ToLower(rand.Text()[:6])
	}
	w, err := worker.New(worker.Config{
		ID:           id,
		Queues:       cfg.WorkerQueues,
		Concurrency:  cfg.WorkerConcurrency,
		PollInterval: cfg.WorkerPollInterval,
	}, storage.New(pool), demoHandlers(), log)
	if err != nil {
		return err
	}
	return w.Run(ctx)
}
