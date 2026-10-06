DB_URL=postgres://gotaskq:gotaskq@127.0.0.1:5432/gotaskq?sslmode=disable

.PHONY: migrate migrate-down migrate-status db-shell

migrate-up:
	goose -dir db postgres "$(DB_URL)" up
migrate-down:
	goose -dir db postgres "$(DB_URL)" down

migrate-status:
	goose -dir db postgres "$(DB_URL)" status

db-shell:
	docker exec-it gotaskq-pg psql -U gotaskq -d gotaskq
