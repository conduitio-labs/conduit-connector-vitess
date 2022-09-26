.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-vitess.version=${VERSION}'" -o conduit-connector-vitess cmd/connector/main.go

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
	mockgen -package mock -source source/source.go -destination source/mock/source.go
