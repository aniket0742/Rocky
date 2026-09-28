# 0005: Job state machine and claiming

**Status:** Accepted (2026-09-28)

## Context
Claims order jobs by `(priority, run_at)`. If future-dated jobs sat in the claimable set, one large batch of delayed high-priority jobs would force every claim to scan past them.

## Decision
- States: `scheduled`, `queued`, `running`, `retrying`, `succeeded`, `dead_letter`, `cancelled`.
- Only `queued` jobs are claimable. A partial index `(queue, priority, run_at, id) WHERE status = 'queued'` makes each claim a short, ordered index scan with `FOR UPDATE SKIP LOCKED`.
- A promoter, run by every worker, moves `scheduled` and `retrying` jobs to `queued` once `run_at <= now()`.
- Every transition is one transaction: a fenced `UPDATE` on `jobs`, then the `attempts` row, then the `job_events` row(s).
- Lease expiry is strict. Worker writes require `lease_expires_at > now()`, and the (Phase 2) reaper requires `<= now()`, so the two can never both succeed.
- CHECK constraints enforce the invariants in the database: a lease exists only while `running`; `finished_at` is set only in terminal states; `attempt <= max_attempts`.

## Consequences
- Claim cost does not grow with the number of delayed jobs.
- Scheduled jobs and retries become claimable up to about 1s after `run_at`.
- Per-job event order equals the real sequence of events, because events are written while the job's row is locked.
