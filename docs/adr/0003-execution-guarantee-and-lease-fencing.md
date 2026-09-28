# 0003: At-least-once execution with fenced leases

**Status:** Accepted (2026-09-28)

## Context
Crashes, GC pauses and partitions make "exactly once" and "never two workers" impossible to promise honestly. A worker can still be running a handler after its lease has expired.

## Decision
Rocky guarantees:
1. **At-least-once execution.**
2. **Fenced ownership:** at most one valid lease per job at a time.
3. **Idempotency support:** an idempotency key on enqueue. Handlers are responsible for idempotent side effects.

How fencing works:
- Every claim issues a new, unique lease token.
- Heartbeat, completion and failure updates apply only if `lease_token` matches.
- A stale worker's writes are rejected, and the worker abandons the job.
- Lease expiry uses the database clock. An expired lease counts as a failed attempt.

## Consequences
- Stale workers cannot corrupt job state. Duplicate side effects are still possible, and this is documented.
- Rocky never claims exactly-once execution.
