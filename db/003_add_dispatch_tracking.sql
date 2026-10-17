-- +goose Up

ALTER TABLE jobs
    ADD COLUMN published_at TIMESTAMPTZ;

CREATE INDEX idx_jobs_scheduled_dispatch
    ON jobs(run_at, retry_at)
    WHERE status = 'pending' AND published_at IS NULL;

-- +goose Down

DROP INDEX IF EXISTS idx_jobs_scheduled_dispatch;
ALTER TABLE jobs
    DROP COLUMN IF EXISTS published_at;
