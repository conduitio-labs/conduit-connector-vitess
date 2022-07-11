.PHONY: build test lint

build:
	go build -o conduit-connector-vitess cmd/vitess/main.go

test:
	go test $(GOTEST_FLAGS) ./...

lint:
	golangci-lint run

mockgen:
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go