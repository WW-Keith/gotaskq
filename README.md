# GoTaskQ

A distributed task queue written in Go — built from scratch to understand how production job queues like Sidekiq, BullMQ, and Celery work under the hood.

Jobs are submitted via HTTP, persisted in PostgreSQL, delivered to workers through a Redis Streams broker, and executed with exponential backoff retry, dead-letter queuing, and priority routing. The worker fleet autoscales on Kubernetes based on live queue depth metrics.

---

## Architecture

```
┌─────────────┐     POST /jobs      ┌──────────────────┐
│   Client    │ ──────────────────▶ │   API Server     │
└─────────────┘                     │  (cmd/server)    │
                                    └────────┬─────────┘
                                             │ Enqueue
                                    ┌────────▼─────────┐
                                    │   PostgreSQL     │
                                    │  jobs table      │
                                    └────────┬─────────┘
                                             │ Publish ID
                                    ┌────────▼─────────┐
                                    │  Redis Streams   │
                                    │  gotaskq:default │
                                    └────────┬─────────┘
                                             │ Subscribe
                              ┌──────────────┼──────────────┐
                     ┌────────▼───┐  ┌───────▼────┐  ┌─────▼──────┐
                     │  Worker 1  │  │  Worker 2  │  │  Worker N  │
                     │(cmd/worker)│  │(cmd/worker)│  │(cmd/worker)│
                     └────────────┘  └────────────┘  └────────────┘
                              │
                     ┌────────▼─────────┐
                     │   Scheduler      │
                     │ (cmd/scheduler)  │
                     │ delayed + cron   │
                     └──────────────────┘
```

**Key design decisions:**

- **Broker carries IDs only** — job payloads stay in PostgreSQL, keeping Redis entries small and the store as single source of truth
- **SKIP LOCKED for atomic dequeue** — multiple workers claim jobs concurrently with no application-level locking
- **Interface-driven** — `Store` and `Broker` are interfaces; swap implementations without touching workers or API
- **Graceful shutdown** — SIGTERM drains in-flight jobs before exit; no jobs lost on rolling deploys

---

## Quick Start

**Requirements:** Docker, Docker Compose

```bash
git clone https://github.com/ww-keith/gotaskq.git
cd gotaskq
make up
```

This starts PostgreSQL, runs migrations, and launches the API server, worker,
scheduler, Prometheus, and Grafana.

**Verify:**

```bash
curl http://localhost:8080/health
# {"status":"ok"}
```

**Monitoring:** Open [Grafana](http://localhost:3000) and sign in with
`admin` / `gotaskq`. The provisioned **GoTaskQ / GoTaskQ Overview** dashboard
shows queue depth, active workers, enqueue and completion rates, job duration,
retries, and dead-letter rates. Prometheus is available at
[localhost:9090](http://localhost:9090).

---

## Usage

**Submit a job:**

```bash
curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "send_email",
    "payload": {"to": "user@example.com", "subject": "Hello"},
    "queue": "default"
  }' | jq .
```

```json
{
  "id": "3f7a9b2c-1d4e-4f8a-9c0b-2e3d4f5a6b7c",
  "type": "send_email",
  "status": "pending",
  "queue": "default",
  "priority": 0,
  "attempts": 0,
  "max_retry": 3,
  "created_at": "2024-01-15T14:32:01Z"
}
```

**Check job status:**

```bash
curl -s http://localhost:8080/jobs/3f7a9b2c-1d4e-4f8a-9c0b-2e3d4f5a6b7c | jq .status
# "succeeded"
```

**Cancel a pending job:**

```bash
curl -s -X DELETE http://localhost:8080/jobs/<id> | jq .
```

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Liveness check |
| `GET` | `/metrics` | Prometheus metrics |
| `POST` | `/jobs` | Submit a new job |
| `GET` | `/jobs/:id` | Get job status and details |
| `DELETE` | `/jobs/:id` | Cancel a pending job |
| `POST` | `/schedules` | Create a recurring job schedule |
| `DELETE` | `/schedules/:id` | Delete a recurring schedule |

**POST /jobs request body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | ✓ | Handler name (`send_email`, `resize_image`) |
| `payload` | object | ✓ | Arbitrary JSON passed to the handler |
| `queue` | string | | `critical`, `default`, `low` (default: `default`) |
| `priority` | int | | `0`=critical, `5`=default, `10`=low |
| `max_retry` | int | | Max retry attempts (default: 3) |
| `run_at` | timestamp | | Optional RFC 3339 time to delay execution until |

**POST /schedules** accepts the same job fields plus `cron`, a standard
five-field cron expression. Expressions are evaluated in UTC. For example:

```json
{
  "type": "send_email",
  "payload": {"to": "user@example.com"},
  "queue": "default",
  "cron": "0 9 * * 1-5"
}
```

---

## Development

```bash
# Run all tests
go test ./... -v

# Run tests for a specific package
go test ./internal/store/... -v
go test ./internal/worker/... -v
go test ./internal/api/... -v

# Run migrations
make migrate

# Open a database shell
make db-shell

# Stop the stack
make down
```

### Kubernetes

`deploy/k8s` contains a single-node PostgreSQL and Redis setup, the GoTaskQ
server/worker/scheduler, Prometheus scraping, and an HPA for the default queue.
The HPA uses the Prometheus Adapter to expose `gotaskq_queue_depth` through the
Kubernetes external metrics API.

Build the four images and make them available to your Kubernetes cluster
(for a local cluster, load these tags into its image cache):

```bash
docker build -f Dockerfile.server -t gotaskq-server:latest .
docker build -f Dockerfile.worker -t gotaskq-worker:latest .
docker build -f Dockerfile.scheduler -t gotaskq-scheduler:latest .
docker build -f Dockerfile.migrate -t gotaskq-migrate:latest .
```

Before deploying, replace the sample database password and matching
`DATABASE_URL` in `deploy/k8s/core.yaml`. The example Secret is for local
development only; use your cluster's secret manager for real deployments.

```bash
kubectl apply -k deploy/k8s
kubectl wait -n gotaskq --for=condition=complete job/gotaskq-migrate --timeout=180s

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install prometheus-adapter prometheus-community/prometheus-adapter \
  --namespace gotaskq \
  -f deploy/k8s/prometheus-adapter-values.yaml
kubectl wait --for=condition=Available \
  apiservice/v1beta1.external.metrics.k8s.io --timeout=120s
kubectl apply -f deploy/k8s/hpa.yaml
```

The HPA targets an average of five pending default-queue jobs per worker, with
1–20 replicas. Check scaling with `kubectl -n gotaskq get hpa worker`; the API
is available inside the cluster at `http://server:8080`, or locally with
`kubectl -n gotaskq port-forward svc/server 8080:8080`.
For later schema changes, delete the completed `gotaskq-migrate` Job before
reapplying the manifests so Kubernetes creates a fresh migration Job.

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgres://gotaskq:gotaskq@localhost:5432/gotaskq` | PostgreSQL DSN |
| `QUEUE` | `default` | Queue the worker drains |
| `METRICS_ADDR` | `:9091` | Worker Prometheus metrics listener |

Delayed jobs and retry attempts are published by the scheduler once their
`run_at` or `retry_at` time is due. Recurring schedules are materialized into
jobs transactionally and advanced to their next cron occurrence. Prometheus scrapes the API at
`/metrics` and the worker at port `9091`.

---

## Project Structure

```
gotaskq/
├── cmd/
│   ├── server/       # HTTP API binary
│   ├── worker/       # Job execution binary
│   └── scheduler/    # Delayed + cron job binary
├── internal/
│   ├── model/        # Job, Schedule, Status, Priority types
│   ├── store/        # PostgreSQL persistence and atomic job claims
│   ├── broker/       # Redis Streams transport
│   ├── api/          # HTTP handlers, routing, middleware, metrics
│   ├── scheduler/    # Delayed/retry dispatch and cron materialization
│   ├── schedule/     # UTC cron expression handling
│   ├── queue/        # Priority selection, retry policy, dead-letter queue
│   ├── observe/      # Prometheus instruments
│   └── worker/       # Job execution and graceful shutdown
├── db/               # SQL migration files (goose)
└── deploy/           # Docker Compose, Grafana, and Kubernetes manifests
```

---

## What This Demonstrates

This project was built as a deep-dive into how production task queues work. Each component addresses a real systems problem:

| Component | Problem Solved |
|-----------|---------------|
| `SELECT FOR UPDATE SKIP LOCKED` | Atomic job claiming across concurrent workers without application locks |
| `Store` interface | Testable architecture — unit tests run with a fake store, zero infrastructure |
| Graceful shutdown | SIGTERM drains in-flight jobs; no data loss on rolling deploys |
| Typed `Status` constants | Compiler-enforced state machine — typos caught at build time, not runtime |
| Partial index on `status='pending'` | Dequeue stays fast regardless of how many completed jobs accumulate |

---

## Roadmap

- [x] PostgreSQL-backed job store with SKIP LOCKED dequeue
- [x] HTTP API — submit, fetch, cancel jobs
- [x] Worker process with executor registry and graceful shutdown
- [x] Docker Compose local stack
- [x] Redis Streams broker (v0.2)
- [x] Exponential backoff retry + dead-letter queue (v0.2)
- [x] Priority queues: critical / default / low (v0.2)
- [x] Delayed and recurring cron schedules
- [x] Prometheus metrics endpoint and worker metrics (v0.3)
- [x] Grafana dashboard (v0.3)
- [x] Kubernetes deployment + HPA on queue depth (v0.4)

---

## License

MIT