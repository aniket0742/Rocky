# 0001: Postgres is the source of truth and the claim mechanism

**Status:** Accepted (2026-09-28)

## Context
Job state, attempts, events and schedules must survive crashes and change atomically. A separate dispatch queue (Redis, Kafka) would mean writing to two systems for every state change, and those two copies can drift apart.

## Decision
- Postgres stores jobs, attempts, `job_events` and schedules.
- Workers claim jobs with `SELECT ... FOR UPDATE SKIP LOCKED`. The claim, the new lease and its `job_events` row are written in one transaction.

## Consequences
- There is no dual-write and no reconciliation. Every state change is one transaction.
- Claim throughput is bounded by Postgres. We measure it in Phase 7 before optimizing.
