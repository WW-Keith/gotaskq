package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/WW-Keith/gotaskq/internal/model"
	"github.com/WW-Keith/gotaskq/internal/schedule"
	_ "github.com/lib/pq"
)

// PostgresStore implements store using PostgreSQL
type PostgresStore struct {
	db *sql.DB
}

//var _Store = (*PostgresStore)(nil)

// NewPostgresStore opens a connection pool and verifies it with a ping
func NewPostgresStore(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	//Connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	//Verify connection is actually reachable
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *PostgresStore) PendingCount(ctx context.Context, queue string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM jobs WHERE status = 'pending' AND queue = $1`,
		queue,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count pending jobs: %w", err)
	}
	return count, nil
}

// DueScheduledJobs returns unpublished pending jobs whose delay or retry time elapsed.
func (s *PostgresStore) DueScheduledJobs(ctx context.Context, now time.Time, limit int) ([]*model.Job, error) {
	const q = `
		SELECT id, queue
		FROM jobs
		WHERE status = 'pending'
		  AND published_at IS NULL
		  AND (run_at IS NULL OR run_at <= $1)
		  AND (retry_at IS NULL OR retry_at <= $1)
		ORDER BY COALESCE(retry_at, run_at, created_at), created_at
		LIMIT $2`

	rows, err := s.db.QueryContext(ctx, q, now.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("query due scheduled jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*model.Job
	for rows.Next() {
		job := &model.Job{}
		if err := rows.Scan(&job.ID, &job.Queue); err != nil {
			return nil, fmt.Errorf("scan due scheduled job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due scheduled jobs: %w", err)
	}
	return jobs, nil
}

func (s *PostgresStore) MarkPublished(ctx context.Context, id string) error {
	const q = `
		UPDATE jobs
		SET published_at = NOW(), updated_at = NOW()
		WHERE id = $1 AND published_at IS NULL`
	res, err := s.db.ExecContext(ctx, q, id)
	if err != nil {
		return fmt.Errorf("mark job published: %w", err)
	}
	return expectOneRow(res, id)
}

func (s *PostgresStore) CreateSchedule(ctx context.Context, recurring *model.Schedule) error {
	if err := recurring.Valid(); err != nil {
		return fmt.Errorf("invalid schedule: %w", err)
	}
	if _, err := schedule.Next(recurring.Cron, time.Now()); err != nil {
		return fmt.Errorf("invalid schedule: %w", err)
	}
	recurring.ID = uuid.NewString()
	recurring.CreatedAt = time.Now().UTC()
	if recurring.Queue == "" {
		recurring.Queue = "default"
	}
	if recurring.MaxRetry == 0 {
		recurring.MaxRetry = 3
	}
	const q = `
			INSERT INTO recurring_schedules
				(id, type, payload, queue, priority, max_retry, cron_expr, next_run_at, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	_, err := s.db.ExecContext(ctx, q,
		recurring.ID, recurring.Type, recurring.Payload, recurring.Queue,
		recurring.Priority, recurring.MaxRetry, recurring.Cron,
		recurring.NextRunAt.UTC(), recurring.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("create schedule: %w", err)
	}
	return nil
}

func (s *PostgresStore) DeleteSchedule(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM recurring_schedules WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete schedule: %w", err)
	}
	return expectOneRow(res, id)
}

// MaterializeDueSchedules creates one job for each due schedule and advances its
// next_run_at atomically, so concurrent scheduler instances cannot duplicate occurrences.
func (s *PostgresStore) MaterializeDueSchedules(ctx context.Context, now time.Time, limit int) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin schedule materialization: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
			SELECT id, type, payload, queue, priority, max_retry, cron_expr, next_run_at
			FROM recurring_schedules
			WHERE enabled = TRUE AND next_run_at <= $1
			ORDER BY next_run_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED`, now.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("query due schedules: %w", err)
	}
	type dueSchedule struct {
		id, jobType, queue, expression string
		payload                        []byte
		priority                       model.Priority
		maxRetry                       int
		nextRunAt                      time.Time
	}
	var due []dueSchedule
	for rows.Next() {
		var item dueSchedule
		if err := rows.Scan(
			&item.id, &item.jobType, &item.payload, &item.queue,
			&item.priority, &item.maxRetry, &item.expression, &item.nextRunAt,
		); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan due schedule: %w", err)
		}
		due = append(due, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("iterate due schedules: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close due schedules: %w", err)
	}

	now = now.UTC()
	for _, item := range due {
		nextRunAt, err := schedule.Next(item.expression, now)
		if err != nil {
			return 0, fmt.Errorf("advance schedule %s: %w", item.id, err)
		}
		_, err = tx.ExecContext(ctx, `
				INSERT INTO jobs
					(id, type, payload, status, priority, queue, max_retry, run_at, created_at, updated_at)
				VALUES ($1, $2, $3, 'pending', $4, $5, $6, $7, $8, $8)`,
			uuid.NewString(), item.jobType, item.payload, item.priority, item.queue,
			item.maxRetry, item.nextRunAt, now)
		if err != nil {
			return 0, fmt.Errorf("create job for schedule %s: %w", item.id, err)
		}
		_, err = tx.ExecContext(ctx, `
				UPDATE recurring_schedules SET next_run_at = $2 WHERE id = $1`,
			item.id, nextRunAt)
		if err != nil {
			return 0, fmt.Errorf("advance schedule %s: %w", item.id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit schedule materialization: %w", err)
	}
	return len(due), nil
}

// Enqueue persists a new job with statusPending
// It sets the ID, Status, CreatedAt, UpdatedAt fields on the job
// before inserting, so the caller gets the full populated struct back
func (s *PostgresStore) Enqueue(ctx context.Context, job *model.Job) error {
	// Validate before touching the database
	if err := job.Valid(); err != nil {
		return fmt.Errorf("invalid job: %w", err)
	}

	// Set fields the caller should not have to worry about
	job.ID = uuid.New().String()
	job.Status = model.StatusPending
	job.CreatedAt = time.Now().UTC()
	job.UpdatedAt = job.CreatedAt

	// Default queue if caller did not set one
	if job.Queue == "" {
		job.Queue = "default"
	}

	// Default max retry if caller didn't set one
	if job.MaxRetry == 0 {
		job.MaxRetry = 3
	}

	const q = `
		INSERT INTO jobs
			(id, type, payload, status, priority, queue,
			max_retry, run_at, created_at, updated_at)
		VALUES
			($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	_, err := s.db.ExecContext(ctx, q,
		job.ID,
		job.Type,
		job.Payload,
		job.Status,
		job.Priority,
		job.Queue,
		job.MaxRetry,
		job.RunAt, // nill is fine postgres stores it as null
		job.CreatedAt,
		job.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("enqueue job: %w", err)
	}
	return nil
}

// Dequeue atomically claims the next available job from the given queue
// Returns nil when the queue is empty - this is not an error
// The job's status is set to running and attempts is incremented
func (s *PostgresStore) Dequeue(ctx context.Context, queue string) (*model.Job, error) {
	const q = `
		UPDATE jobs
		SET    status = 'running',
		       started_at = NOW(),
		       updated_at = NOW(),
		       attempts = attempts + 1
		WHERE  id = (
			SELECT id FROM jobs
			WHERE  status = 'pending'
			AND    queue  = $1
			AND    (run_at IS NULL OR run_at <= NOW())
			AND    (retry_at IS NULL OR retry_at <= NOW())
			ORDER  BY priority ASC, created_at ASC
			LIMIT  1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, type, payload, status, priority, queue,
		          attempts, max_retry, error, run_at,
		          started_at, done_at, created_at, updated_at`

	job := &model.Job{}
	err := s.db.QueryRowContext(ctx, q, queue).Scan(
		&job.ID,
		&job.Type,
		&job.Payload,
		&job.Status,
		&job.Priority,
		&job.Queue,
		&job.Attempts,
		&job.MaxRetry,
		&job.Error,
		&job.RunAt,
		&job.StartedAt,
		&job.DoneAt,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // empty queue, not an error
	}
	if err != nil {
		return nil, fmt.Errorf("dequeue: %w", err)
	}
	return job, nil
}

// Claim atomically transitions one broker-delivered pending job to running.
func (s *PostgresStore) Claim(ctx context.Context, id string) (*model.Job, error) {
	const q = `
		UPDATE jobs
		SET status = 'running',
		    started_at = NOW(),
		    updated_at = NOW(),
		    attempts = attempts + 1
		WHERE id = $1
		  AND status = 'pending'
		  AND (run_at IS NULL OR run_at <= NOW())
		  AND (retry_at IS NULL OR retry_at <= NOW())
		RETURNING id, type, payload, status, priority, queue,
		          attempts, max_retry, error, run_at, retry_at,
		          started_at, done_at, created_at, updated_at`
	job := &model.Job{}
	err := s.db.QueryRowContext(ctx, q, id).Scan(
		&job.ID, &job.Type, &job.Payload, &job.Status, &job.Priority,
		&job.Queue, &job.Attempts, &job.MaxRetry, &job.Error, &job.RunAt,
		&job.RetryAt, &job.StartedAt, &job.DoneAt, &job.CreatedAt, &job.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim job: %w", err)
	}
	return job, nil
}

// Ack marks job as succesfully completed
func (s *PostgresStore) Ack(ctx context.Context, id string) error {
	const q = `
		UPDATE jobs
		SET
			status = 'succeeded',
			done_at = NOW(),
			updated_at = NOW()
		WHERE id = $1`
	res, err := s.db.ExecContext(ctx, q, id)
	if err != nil {
		return fmt.Errorf("ack job: %v", err)
	}
	return expectOneRow(res, id)
}

// Nack marks job as failed with an error message
func (s *PostgresStore) Nack(ctx context.Context, id, errMsg string) error {
	const q = `
		UPDATE jobs
		SET
			status = 'failed',
			error = $2,
			done_at = NOW(),
			updated_at = NOW()
		WHERE id = $1`
	res, err := s.db.ExecContext(ctx, q, id, errMsg)
	if err != nil {
		return fmt.Errorf("nack job: %v", err)
	}
	return expectOneRow(res, id)
}

// GetById retrieves a single job by id
// Returns ErrNotFound if no jobs exists with that ID
func (s *PostgresStore) GetByID(ctx context.Context, id string) (*model.Job, error) {
	const q = `
	SELECT
		id, type, payload, status, priority, queue,
		attempts, max_retry, error, run_at, retry_at,
		started_at, done_at, created_at, updated_at
	FROM jobs
	WHERE id = $1`

	job := &model.Job{}
	err := s.db.QueryRowContext(ctx, q, id).Scan(
		&job.ID,
		&job.Type,
		&job.Payload,
		&job.Status,
		&job.Priority,
		&job.Queue,
		&job.Attempts,
		&job.MaxRetry,
		&job.Error,
		&job.RunAt,
		&job.RetryAt,
		&job.StartedAt,
		&job.DoneAt,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job by ID: %w", err)
	}
	return job, nil
}

// List returns jobs filtered by status
// Pass empty string for all status
func (s *PostgresStore) List(ctx context.Context, status model.Status, limit, offset int) ([]*model.Job, error) {
	//Full implementation comes in later with api handler
	//For now return empty slice so the interface is satisfied
	return []*model.Job{}, nil
}

// Cancel marks pending jobs as cancelled
func (s *PostgresStore) Cancel(ctx context.Context, id string) error {
	const q = `
	UPDATE jobs
	SET
		status = 'cancelled',
		done_at = NOW(),
		updated_at = NOW()
	WHERE id = $1
	AND status = 'pending'`

	res, err := s.db.ExecContext(ctx, q, id)
	if err != nil {
		return fmt.Errorf("cnancel job: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrorCannotCancel
	}
	return nil
}

func expectOneRow(res sql.Result, id string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return nil
}

// ScheduleRetry resets a failed job to pending with a future retry_at.
// Called by the worker when a job fails but has remaining attempts.
func (s *PostgresStore) ScheduleRetry(ctx context.Context, id, errMsg string, retryAt time.Time) error {
	const q = `
		UPDATE jobs
		SET
			status     = 'pending',
			error      = $2,
			retry_at   = $3,
			published_at = NULL,
			updated_at = NOW()
		WHERE id = $1`

	res, err := s.db.ExecContext(ctx, q, id, errMsg, retryAt.UTC())
	if err != nil {
		return fmt.Errorf("schedule retry: %w", err)
	}
	return expectOneRow(res, id)
}

// MarkFailed permanently fails a job and records the DLQ reason.
// Called when a job has exhausted all retry attempts.
func (s *PostgresStore) MarkFailed(ctx context.Context, id, reason string) error {
	const q = `
		UPDATE jobs
		SET
			status     = 'failed',
			dlq_reason = $2,
			dlq_at     = NOW(),
			done_at    = NOW(),
			updated_at = NOW()
		WHERE id = $1`

	res, err := s.db.ExecContext(ctx, q, id, reason)
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	return expectOneRow(res, id)
}
