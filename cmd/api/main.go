// Command api runs Rocky's HTTP API.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/aniket0742/rocky/internal/api"
	"github.com/aniket0742/rocky/internal/config"
	"github.com/aniket0742/rocky/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
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

	// Connections are lazy: an unreachable dependency is reported by /v1/health,
	// not treated as a startup failure. Only invalid configuration fails here.
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()
	deps := []api.Dependency{{Name: "postgres", Check: pool.Ping, Required: true}}

	redisDep := api.Dependency{Name: "redis"} // optional (ADR 0002)
	if cfg.RedisURL != "" {
		opt, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		rdb := redis.NewClient(opt)
		defer rdb.Close()
		redisDep.Check = func(ctx context.Context) error { return rdb.Ping(ctx).Err() }
	}
	deps = append(deps, redisDep)

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	for _, d := range deps {
		if d.Check != nil {
			if err := d.Check(pingCtx); err != nil {
				log.Warn("dependency unreachable at startup", "dependency", d.Name, "err", err)
			}
		}
	}
	cancel()

	srv := &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           api.NewHandler(log, deps),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("api listening", "addr", cfg.APIAddr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down", "timeout", cfg.ShutdownTimeout)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
