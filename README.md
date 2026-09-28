# Rocky

Reliable background jobs in Go. Postgres-backed job engine with fenced leases, retries, scheduling and a control plane.

> **Status:** Phase 2 (reliable execution). Jobs run under heartbeat-renewed leases. Crashed workers' jobs are recovered, failures retry with exponential backoff, and shutdown drains gracefully. See [ADR 0006](docs/adr/0006-lease-renewal-recovery-and-shutdown.md).

## Guarantees

- **At-least-once execution.** A job may run more than once. Make handlers idempotent.
- **Fenced ownership.** At most one valid lease per job; stale workers can't change a job.
- **Idempotent enqueue.** Same idempotency key, same job.

Rocky does not claim exactly-once. See [ADR 0003](docs/adr/0003-execution-guarantee-and-lease-fencing.md) and [ADR 0005](docs/adr/0005-job-state-machine-and-claiming.md).

## Run

Requires Docker.

```sh
docker compose up --build            # postgres, redis, migrate, api, worker

curl -X POST localhost:8080/v1/jobs -d '{"type":"noop","payload":{"hello":"world"}}'
curl localhost:8080/v1/jobs/<id>
curl localhost:8080/v1/jobs/<id>/events   # execution history
```

Demo job types: `noop`, `sleep` (`{"seconds": n}`), `fail` (always fails, then dead-letters).

## API

| Endpoint | |
|---|---|
| `POST /v1/jobs` | Create a job: `type` (required), `queue`, `payload`, `metadata`, `priority`, `max_attempts`, `timeout_seconds`, `idempotency_key` |
| `GET /v1/jobs/{id}` | Current state |
| `GET /v1/jobs/{id}/events` | History, oldest first |
| `GET /v1/health` | Dependency status |

## Develop

Requires Go 1.27.

```sh
docker compose up -d postgres
export ROCKY_TEST_DATABASE_URL=postgres://rocky:rocky@localhost:5432/rocky?sslmode=disable
go test ./...        # integration tests skip if the variable is unset
```

To run binaries outside Docker, set the variables in `.env.example`, then `go run ./cmd/migrate`, `./cmd/api` or `./cmd/worker`.

## Layout

| Path | What |
|---|---|
| `cmd/api`, `cmd/worker`, `cmd/migrate` | Binaries |
| `internal/jobs` | Domain: states, records, enqueue rules |
| `internal/storage` | Postgres: every state transition and its SQL |
| `internal/worker` | Handler registry, claim loop, heartbeats, reaper, shutdown |
| `internal/retry` | Backoff policy |
| `internal/api` | HTTP handlers, middleware, error shape |
| `migrations` | SQL schema (embedded) |
| `test` | End-to-end tests |
| `docs/adr` | Architecture decisions |
