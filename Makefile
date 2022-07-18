.PHONY: build test lint

build:
	go build -o conduit-connector-vitess cmd/vitess/main.go

test:
	docker compose -f test/docker-compose.yml up --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) ./...; ret=$$?; \
		docker compose -f test/docker-compose.yml down; \
		exit $$ret

bench:
	go test -benchmem -run=^$$ -bench=. ./...

lint:
	golangci-lint run

mockgen:
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go