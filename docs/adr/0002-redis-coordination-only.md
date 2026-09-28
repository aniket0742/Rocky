# 0002: Redis is for coordination only

**Status:** Accepted (2026-09-28)

## Context
Some things need to be fast and shared across workers: rate limits, per-queue concurrency, wakeups and live UI signals. Those are not reasons to put job state in Redis.

## Decision
- Redis is used for rate limiting, concurrency coordination, wakeups and live signals.
- Redis holds no job state and is never required for correctness.
- If Redis is unavailable, workers fall back to polling Postgres.

## Consequences
- A Redis outage raises latency and weakens global limits. It does not lose or corrupt jobs.
- Every Redis feature needs a defined fallback, documented in `failure-modes.md`.
- The API reports Redis as `degraded`, not down.
