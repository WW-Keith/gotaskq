package db_test

import (
	"database/sql"
	"os"
	"testing"

	_"github.com/lib/pq"
)



func testDSn() string {
	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		return dsn
	}
	return "postgres://gotaskq:gotaskq@127.0.0.1:5432/gotaskq?sslmode=disable"
}

func TestSchema_JobTableExists(t *testing.T){
	db, err := sql.Open("postgres", testDSn())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()


	//Query the information schema - standard SQL, works on any postgres
	var count int
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = 'public'
		AND table_name = 'jobs'
	`).Scan(&count)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("jobs table not found: count = %d", count)
	}
}

func TestSchema_RequiredColumns(t *testing.T){
	db, err := sql.Open("postgres", testDSn())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	required := []string{
		"id", "type","payload", "status", "priority", 
		"queue", "attempts", "max_retry", "error", 
		"run_at", "retry_at", "created_at", "updated_at",
	}
	for _, col := range required {
		var count int
		err := db.QueryRow(`
			SELECT COUNT(*)
			FROM information_schema.columns
			WHERE table_name = 'jobs'
			AND column_name = $1
		`, col).Scan(&count)
		if err != nil{
			t.Fatalf("query for column %q failed: %v", col, err)
		}
		if count == 0 {
			t.Errorf("missing column: %q", col)
		}
	}
}

func TestSchema_DequeueIndexExists(t *testing.T){
	db, err := sql.Open("postgres", testDSn())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	var count int
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM pg_indexes
		WHERE tablename = 'jobs'
		AND indexname = 'idx_jobs_dequeue'
	`).Scan(&count)
	if err != nil{
		t.Fatalf("query failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("idx_jobs_dequeue not found")
	}
}