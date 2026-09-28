// Package testdb gives integration tests an isolated, migrated Postgres schema.
package testdb

import (
	"context"
	"crypto/rand"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aniket0742/rocky/migrations"
)

// New returns a pool whose search_path is a fresh schema with all migrations
// applied. The schema is dropped when the test ends. Tests are skipped unless
// ROCKY_TEST_DATABASE_URL is set.
func New(t testing.TB) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("ROCKY_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ROCKY_TEST_DATABASE_URL not set; skipping integration test")
	}
	ctx := context.Background()
	schema := "test_" + strings.ToLower(rand.Text())

	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		conn, err := pgx.Connect(ctx, url)
		if err != nil {
			t.Errorf("cleanup connect: %v", err)
			return
		}
		defer conn.Close(ctx)
		if _, err := conn.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop schema: %v", err)
		}
	})

	if _, err := migrations.Up(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}
