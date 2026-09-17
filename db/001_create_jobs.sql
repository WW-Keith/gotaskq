-- +goose up

CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    priority SMALLINT NOT NULL DEFAULT 5,
    queue TEXT NOT NULL DEFAULT 'default',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_retry INTEGER NOT NULL DEFAULT 3,
    error TEXT,
    run_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    done_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Partial index for dequeue query
-- Only indexes rows wher status = 'pending' - completed jobs
-- fall out of the index automatically keeping it small and fast
CREATE INDEX idx_jobs_dequeue
    ON jobs(priority ASC, created_at ASC)
    WHERE status = 'pending';

-- Index for API lookups by status (list endpoint, monitoring)
CREATE INDEX idx_jobs_status ON jobs(status);

-- +goose Down
DROP TABLE IF EXISTS jobs;
