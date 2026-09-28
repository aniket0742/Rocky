# 0004: Scheduler deduplication and missed runs

**Status:** Accepted (2026-09-28)

## Context
Scheduler restarts, or two scheduler instances running at once, must not create duplicate jobs. The behavior after downtime must also be predictable.

## Decision
- Jobs created by a schedule carry a unique `(schedule_id, fire_time)`, enforced by a database constraint. A duplicate insert is a no-op.
- Each schedule has an explicit missed-run policy:
  - `skip`: resume at the next future fire time.
  - `run_once` (default): enqueue only the most recent missed fire time.
  - `catch_up`: enqueue each missed fire time, capped (default 10).
- Fire times are computed in the schedule's timezone, using the database clock.

## Consequences
- Duplicates are prevented by the database, not by leader election, so running more than one scheduler is safe.
- Downtime behavior is set per schedule and can be tested.
