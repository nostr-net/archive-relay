BINARY ?= archive-relay

.PHONY: build run test test-unit test-integration vet fmt clean docker

build:
	go build -o $(BINARY) ./cmd/archive-relay

run: build
	./$(BINARY)

fmt:
	gofmt -s -w .

vet:
	go vet ./...

test: test-unit

test-unit:
	go test ./...

# Integration tests need ClickHouse on $$CH_ADDR (default localhost:9000).
# Excludes internal/crawler (its one test hits a live relay, env-overridable
# via CRAWLER_SOURCE — run it by hand).
test-integration:
	go test -tags=integration -count=1 -timeout 120s ./internal/store/ ./internal/stats/ ./internal/e2e/

clean:
	rm -f $(BINARY)
