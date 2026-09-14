-- +goose Up

-- retry_at when a failed job should next be attempted
-- NULL means run immedietely
ALTER TABLE jobs
    ADD COLUMN retry_at TIMESTAMPZ,
    ADD COLUMN dlq_reason TEXT,
    ADD COLUMN dlq_at TIMESTAMPZ;

-- Rebuild the dequeue index to exclude jobs whose retry_at is still in future
DROP INDEX idx_jobs_dequeue;

CREATE INDEX idx_jobs_dequeue
    ON jobs (priority ASC, created_at ASC),
    WHERE status = 'pending',
    AND (retry_at IS NULL OR retry_at <= Now());

-- +goose Down
ALTER TABLE jobs
    DROP COLUMN IF EXISTS retry_at,
    DROP COLUMN IF EXISTS dlq_reason,
    DROP COLUMN IF EXISTS dlq_at;
