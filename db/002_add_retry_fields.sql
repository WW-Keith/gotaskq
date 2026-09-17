-- +goose Up

-- retry_at when a failed job should next be attempted
-- NULL means run immedietely
-- dlq_reason: explains why job was sent to dead letter queue
-- dlq_at: timestamp when job was moved to DLQ
ALTER TABLE jobs
    ADD COLUMN retry_at TIMESTAMPTZ,
    ADD COLUMN dlq_reason TEXT,
    ADD COLUMN dlq_at TIMESTAMPTZ;

    -- NOTE: We intentionally do not modify idx_jobs_dequeue here
    -- Partial indexes practices must be Immutable meaning NOW() cannot be used
    -- dynamically in the Dequeue SQL query instead

--  +goose Down

ALTER TABLE jobs
    DROP COLUMN IF EXISTS retry_at,
    DROP COLUMN IF EXISTS dlq_reason,
    DROP COLUMN IF EXISTS dlq_at;
