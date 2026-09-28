// Command migrate applies Rocky's database migrations.
//
//	migrate          apply pending migrations
//	migrate status   list migrations and whether each is applied
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aniket0742/rocky/internal/config"
	"github.com/aniket0742/rocky/internal/telemetry"
	"github.com/aniket0742/rocky/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
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

	cmd := "up"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	switch cmd {
	case "up":
		results, err := migrations.Up(ctx, pool)
		if err != nil {
			return err
		}
		for _, r := range results {
			log.Info("migration applied", "version", r.Source.Version, "file", r.Source.Path, "duration", r.Duration)
		}
		log.Info("schema up to date", "applied", len(results))
	case "status":
		statuses, err := migrations.Status(ctx, pool)
		if err != nil {
			return err
		}
		for _, s := range statuses {
			log.Info("migration", "version", s.Source.Version, "file", s.Source.Path, "state", s.State)
		}
	default:
		return fmt.Errorf("unknown command %q (want: up, status)", cmd)
	}
	return nil
}
