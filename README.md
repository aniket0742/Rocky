# Rocky

Reliable background jobs in Go. Postgres-backed job engine with fenced leases, retries, scheduling and a control plane.

> **Status:** Phase 0 (foundation). The job engine is not built yet.

## Guarantees

- **At-least-once execution.** A job may run more than once. Make handlers idempotent.
- **Fenced ownership.** At most one valid lease per job; stale workers can't change a job.
- **Idempotent enqueue.** Same idempotency key, same job.

Rocky does not claim exactly-once. See [ADR 0003](docs/adr/0003-execution-guarantee-and-lease-fencing.md).

## Run

Requires Docker.

```sh
docker compose up --build
curl localhost:8080/v1/health
```

## Develop

Requires Go 1.27.

```sh
go test ./...
docker compose up -d postgres redis   # then set the vars in .env.example
go run ./cmd/api
```

## Layout

| Path | What |
|---|---|
| `cmd/api` | HTTP API binary |
| `internal/api` | Handlers, middleware, error shape |
| `internal/config` | Env config |
| `internal/telemetry` | Logging |
| `docs/adr` | Architecture decisions |

Full spec: [ROCKY_MASTER_SPEC.md](ROCKY_MASTER_SPEC.md).
