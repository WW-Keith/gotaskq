package main

import (
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/WW-Keith/gotaskq/internal/api"
	"github.com/WW-Keith/gotaskq/internal/broker"
	"github.com/WW-Keith/gotaskq/internal/observe"
	"github.com/WW-Keith/gotaskq/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://gotaskq:gotaskq@localhost:5432/gotaskq?sslmode=disable"
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

	addr := ":8080"
	slog.Info("server starting", "addr", addr)

	srv := api.NewServer(pg, rb, observe.New())
	if err := http.ListenAndServe(addr, srv); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
