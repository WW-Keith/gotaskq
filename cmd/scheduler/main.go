package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/WW-Keith/gotaskq/internal/broker"
	"github.com/WW-Keith/gotaskq/internal/scheduler"
	"github.com/WW-Keith/gotaskq/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "******localhost:5432/gotaskq?sslmode=disable"
	}
	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	pg, err := store.NewPostgresStore(dsn)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	rb, err := broker.NewRedisBroker(redisAddr)
	if err != nil {
		log.Fatalf("connect to redis: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	scheduler.New(pg, rb, time.Second).Run(ctx)
}
