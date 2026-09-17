package store

import (
	"context"
	"os"
	"testing"
)

func testDSn() string {
	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		return dsn
	}
	return "postgres://gotaskq:gotaskq@localhost:5432/gotaskq?sslmode=disable"
}

func TestNewPostgresStore_Connect(t *testing.T) {
	store, err := NewPostgresStore(testDSn())
	if err != nil {
		t.Fatalf("NewPostgresStore() failed: %v", err)
	}

	ctx := context.Background()
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping() failed after connect: %v", err)
	}
}

func TestNewPostgresStore_BadDSN(t *testing.T) {
	_, err := NewPostgresStore("postgres://bad:cred@localhost:9999/nodb?sslmode=disable")
	if err == nil {
		t.Fatalf("expected error for bad DSN, got nil")
	}
	t.Logf("got expected error: %v", err)
}
