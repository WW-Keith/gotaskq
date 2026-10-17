package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/WW-Keith/gotaskq/internal/broker"
	"github.com/WW-Keith/gotaskq/internal/model"
	"github.com/WW-Keith/gotaskq/internal/observe"
	"github.com/WW-Keith/gotaskq/internal/store"
	"github.com/WW-Keith/gotaskq/internal/worker"
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

	queue := os.Getenv("QUEUE")
	if queue == "" {
		queue = "default"
	}

	// Unique consumer name: hostname + pid so multiple workers
	// in the same consumer group don't collide.
	hostname, _ := os.Hostname()
	consumer := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	pg, err := store.NewPostgresStore(dsn)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}

	rb, err := broker.NewRedisBroker(redisAddr)
	if err != nil {
		log.Fatalf("connect to redis: %v", err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGTERM,
		syscall.SIGINT,
	)
	defer stop()

	// Create the consumer group before reading from it.
	if err := rb.EnsureGroup(ctx, queue); err != nil {
		log.Fatalf("ensure consumer group: %v", err)
	}

	exec := worker.NewExecutor()

	exec.Register("send_email", func(ctx context.Context, job *model.Job) error {
		var payload struct {
			To string `json:"to"`
		}
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return err
		}
		slog.Info("sending email", "to", payload.To, "job_id", job.ID)
		return nil
	})

	exec.Register("resize_image", func(ctx context.Context, job *model.Job) error {
		slog.Info("resizing image", "job_id", job.ID)
		return nil
	})

	metrics := observe.New()
	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9091"
	}
	go func() {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", metrics.Handler())
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			slog.Error("metrics server failed", "addr", metricsAddr, "err", err)
		}
	}()

	w := worker.New(pg, rb, exec, queue, consumer, metrics)
	w.Run(ctx)

	slog.Info("worker exited cleanly")
}
