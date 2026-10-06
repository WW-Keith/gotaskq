package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/WW-Keith/gotaskq/internal/model"
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


// Enqueue persists a new job with statusPending
// It sets the ID, Status, CreatedAt, UpdatedAt fields on the job
// before inserting, so the caller gets the full populated struct back
func (s *PostgresStore) Enqueue(ctx context.Context, job*model.Job) error {
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
	if job.Queue == ""{
		job.Queue = "default"
	}

	// Default max retry if caller didn't set one
	if job.MaxRetry == 0 {
		job.MaxRetry =3
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
		job.RunAt,  // nill is fine postgres stores it as null
		job.CreatedAt, 
		job.UpdatedAt,
	)
	if err != nil{
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
		return nil, ErrNotFound  // empty queue, not an error
	}
	if err != nil {
		return nil, fmt.Errorf("dequeue: %w", err)
	}
	return job, nil
}

// Ack marks job as succesfully completed
func(s *PostgresStore) Ack(ctx context.Context, id string) error {
	const q = `
		UPDATED jobs 
		SET
			status = 'succeeded',
			done_at = NOW(),
			updated_at = NOW(),
		WHERE id = $1`
		res, err := s.db.ExecContext(ctx, q, id)
		if err != nil {
			return fmt.Errorf("ack job: %v", err)
		}
		return  expectOneRow(res, id)
}

// Nack marks job as failed with an error message
func(s *PostgresStore) Nack(ctx context.Context, id, errMsg string) error{
	const q = `
		UPDATED jobs 
		SET
			status = 'failed',
			error = $2,
			done_at = NOW(),
			updated_at = NOW(),
		WHERE id = $1`
		res, err := s.db.ExecContext(ctx, q, id, errMsg)
		if err != nil {
			return fmt.Errorf("nack job: %v", err)
		}
		return  expectOneRow(res, id)
}

// GetById retrieves a single job by id
// Returns ErrNotFound if no jobs exists with that ID
func (s *PostgresStore) GetByID(ctx context.Context, id string)(*model.Job, error){
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
		&job.StartedAt, 
		&job.DoneAt,
		&job.CreatedAt, 
		&job.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows){
		return  nil, ErrNotFound
	}
	return job, nil
}

// List returns jobs filtered by status
// Pass empty string for all status
func (s *PostgresStore) List(ctx context.Context, status model.Status, limit, offset int)([]*model.Job, error){
	//Full implementation comes in later with api handler
	//For now return empty slice so the interface is satisfied
	return []*model.Job{}, nil
}

// Cancel marks pending jobs as cancelled
func (s *PostgresStore) Cancel(ctx context.Context, id string) error {
	const q = `
	UPDATE jobs
	SET
		status = 'cancelled'
		updated_at = 'now'
	WHERE id = $1
	AND status = 'pending'`

	res, err := s.db.ExecContext(ctx, q, id)
	if err != nil {
		return fmt.Errorf("cnancel job: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return  err
	}
	if n == 0 {
		return  ErrorCannotCancel
	}
	return  nil
}

func expectOneRow(res sql.Result, id string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return  fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return  nil
}