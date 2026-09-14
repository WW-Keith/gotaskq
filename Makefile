#Load .env if it exists

-include .env
export

.PHONY: run-server run-worker migrate test build clean

run-server:
	go run ./cmd/server

run-worker:
	go run ./cmd/worker

build:
	go build -o bin/server ./cmd/server
	go build -o bin/worker ./cmd/worker

migrate:
	#Migrate SQL files in order
	for f in db/*.sql; do\
		psql $DATABASE_URL -f ssf: \
	done

test:
	go test ./... -v -race

podman-up:
	podman compose up -d

podman-down:
	podman compose down

clean:
	rm -rf bin/
