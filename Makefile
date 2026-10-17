DB_URL=postgres://gotaskq:gotaskq@127.0.0.1:5432/gotaskq?sslmode=disable

COMPOSE ?= podman-compose --parallel 1 -f deploy/docker-compose.yml
POSTGRES_HOST_PORT ?= 5432

.PHONY: migrate migrate-down migrate-status db-shell

migrate-up:
	goose -dir db postgres "$(DB_URL)" up
migrate-down:
	goose -dir db postgres "$(DB_URL)" down

migrate-status:
	goose -dir db postgres "$(DB_URL)" status

db-shell:
	$(COMPOSE) exec postgres psql -U gotaskq -d gotaskq

.PHONY: up down logs

up:
	POSTGRES_HOST_PORT=$(POSTGRES_HOST_PORT) $(COMPOSE) up --build -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

.PHONY: up down logs open-grafana open-prometheus

open-grafana:
	open http://localhost:3000   # admin / gotaskq

open-prometheus:
	open http://localhost:9090