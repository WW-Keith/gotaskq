package store

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"time"
)

// PostgresStore implements store using PostgreSQL
type PostgresStore struct {
	db *sql.DB
}

var _Store = (*PostgresStore)(nil)

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

//func (s *PostgresStore) Enqueue(ctx context.Context, job interface{}) error {
//	return fmt.Errorf("not implemented")
//}
