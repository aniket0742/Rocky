// Package migrations holds Rocky's SQL schema, embedded into the binary.
package migrations

import (
	"context"
	"embed"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed *.sql
var files embed.FS

// Up applies pending migrations. A Postgres advisory lock serializes concurrent runs.
func Up(ctx context.Context, pool *pgxpool.Pool) ([]*goose.MigrationResult, error) {
	p, closeDB, err := provider(pool)
	if err != nil {
		return nil, err
	}
	defer closeDB()
	return p.Up(ctx)
}

// Status reports every known migration and whether it has been applied.
func Status(ctx context.Context, pool *pgxpool.Pool) ([]*goose.MigrationStatus, error) {
	p, closeDB, err := provider(pool)
	if err != nil {
		return nil, err
	}
	defer closeDB()
	return p.Status(ctx)
}

func provider(pool *pgxpool.Pool) (*goose.Provider, func() error, error) {
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return nil, nil, err
	}
	db := stdlib.OpenDBFromPool(pool) // closing db leaves the pool open
	p, err := goose.NewProvider(goose.DialectPostgres, db, files, goose.WithSessionLocker(locker))
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	return p, db.Close, nil
}
