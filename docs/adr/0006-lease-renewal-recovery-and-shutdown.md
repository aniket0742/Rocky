# 0006: Lease renewal, recovery and shutdown

**Status:** Accepted (2026-09-28)

## Context
Leases must be short, so a crashed worker's jobs come back quickly, yet long enough for long-running jobs. A retry must not hammer a failing dependency. A deploy must not lose or stall jobs.

## Decision
- **Heartbeats:** each worker renews all of its leases in one query every `LeaseTTL/3` (default TTL 30s). Renewal stops at the job's timeout, so a handler that ignores its deadline loses the lease within one TTL. A lease the database refuses to renew is lost: the handler is cancelled and its result discarded.
- **Reaper:** every worker (no coordinator) looks for expired leases every poll interval and expires them through the fenced transition. An expired lease is a failed, retryable attempt.
- **Backoff:** the delay after failed attempt n is `base·2^(n-1)`, ±20% jitter, capped at `max` (defaults 2s and 1h). Errors wrapped with `worker.NonRetryable` dead-letter immediately.
- **Shutdown (spec §9.6):** stop claiming, then give in-flight jobs `ShutdownGrace` to finish (heartbeats continue), then cancel the rest. A cancelled job that returns is recorded as *interrupted by worker shutdown* and retried at once. It counts as an attempt but gets no backoff. Handlers that ignore cancellation for one more TTL are abandoned to lease expiry.

## Consequences
- A crashed worker's jobs are retried about one TTL (plus backoff) after its last heartbeat.
- A job whose handler runs past a lost lease may execute twice, but only one result is recorded (at-least-once, ADR 0003).
- An interruption on a job's last attempt dead-letters it. Deploys should use a grace period longer than typical jobs.
