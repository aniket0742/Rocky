-- +goose Up
CREATE TABLE jobs (
    id               uuid        PRIMARY KEY DEFAULT uuidv7(),
    queue            text        NOT NULL DEFAULT 'default',
    type             text        NOT NULL,
    payload          jsonb       NOT NULL DEFAULT '{}',
    metadata         jsonb       NOT NULL DEFAULT '{}',
    priority         smallint    NOT NULL DEFAULT 3,             -- 1 critical, 2 high, 3 normal, 4 low
    status           text        NOT NULL,
    attempt          integer     NOT NULL DEFAULT 0,             -- attempts started so far
    max_attempts     integer     NOT NULL DEFAULT 5,
    timeout_seconds  integer     NOT NULL DEFAULT 300,           -- per attempt
    run_at           timestamptz NOT NULL DEFAULT now(),
    idempotency_key  text,
    lease_token      uuid,
    lease_expires_at timestamptz,
    worker_id        text,                                       -- current or last worker
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,

    CHECK (status IN ('scheduled', 'queued', 'running', 'retrying', 'succeeded', 'dead_letter', 'cancelled')),
    CHECK (priority BETWEEN 1 AND 4),
    CHECK (max_attempts >= 1 AND attempt BETWEEN 0 AND max_attempts),
    CHECK (timeout_seconds > 0),
    CHECK ((status = 'running') = (lease_token IS NOT NULL)),    -- a lease exists iff running
    CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),
    CHECK ((status IN ('succeeded', 'dead_letter', 'cancelled')) = (finished_at IS NOT NULL))
);

CREATE TABLE attempts (
    job_id      uuid        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt     integer     NOT NULL,
    worker_id   text        NOT NULL,
    lease_token uuid        NOT NULL,
    status      text        NOT NULL DEFAULT 'running',
    error       text,
    started_at  timestamptz NOT NULL DEFAULT now(),
    ended_at    timestamptz,
    PRIMARY KEY (job_id, attempt),
    CHECK (status IN ('running', 'succeeded', 'failed', 'lease_expired')),
    CHECK ((status = 'running') = (ended_at IS NULL))
);

CREATE TABLE job_events (
    id         bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id     uuid        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt    integer,                                          -- null for job-level events
    type       text        NOT NULL,
    worker_id  text,
    data       jsonb       NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Claim: next ready job in a queue, by priority then age.
CREATE INDEX jobs_ready_idx ON jobs (queue, priority, run_at, id) WHERE status = 'queued';
-- Promote: scheduled/retrying jobs whose run_at has arrived.
CREATE INDEX jobs_pending_idx ON jobs (run_at) WHERE status IN ('scheduled', 'retrying');
-- Reaper and worker "active jobs". lease_expires_at is deliberately not indexed,
-- so heartbeats don't touch an indexed column and remain HOT-eligible.
CREATE INDEX jobs_running_idx ON jobs (worker_id) WHERE status = 'running';
CREATE UNIQUE INDEX jobs_idempotency_key_idx ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;
-- Timeline: one job's history in order.
CREATE INDEX job_events_job_idx ON job_events (job_id, id);

-- +goose Down
DROP TABLE job_events, attempts, jobs;
