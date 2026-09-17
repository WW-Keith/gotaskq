DB_URL=migrate://gotaskq:gotaskq@localhost:5432/gotaskq?sslmode=disable

.PHONY: migrate migrate-down migrate-status db-shell

migrate:
	goose-dir db postgres "$(DB_URL)" up
migrate-down:
	goose-dir db postgres "$(DB_URL)" down

migrate-status:
	goose-dir db postgres "(DB_URL)" status

db-shell:
	docker exec-it gotaskq-pg psql -U gotaskq -d gotaskq
