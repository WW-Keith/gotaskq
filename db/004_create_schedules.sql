-- +goose Up

CREATE TABLE recurring_schedules (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    queue TEXT NOT NULL DEFAULT 'default',
    priority SMALLINT NOT NULL DEFAULT 5,
    max_retry INTEGER NOT NULL DEFAULT 3,
    cron_expr TEXT NOT NULL,
    next_run_at TIMESTAMPTZ NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_recurring_schedules_due
    ON recurring_schedules(next_run_at)
    WHERE enabled = TRUE;

-- +goose Down

DROP TABLE IF EXISTS recurring_schedules;
