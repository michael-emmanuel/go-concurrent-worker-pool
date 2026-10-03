.PHONY: all fmt fmt-check vet test race lint bench examples cover ci

all: fmt-check vet test race

fmt:
	gofmt -w .

fmt-check:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then echo "needs gofmt:"; echo "$$unformatted"; exit 1; fi

vet:
	go vet ./...

test:
	go test -count=1 ./...

race:
	go test -race -count=1 ./...

lint:
	golangci-lint run ./...

bench:
	go test -run '^$$' -bench . -benchmem ./workerpool

examples:
	@for e in basic backpressure retries graceful-shutdown cancellation http-api; do \
		echo "== $$e"; go run ./examples/$$e || exit 1; \
	done

cover:
	go test -coverprofile=coverage.out ./workerpool
	go tool cover -func=coverage.out | tail -1

ci: fmt-check vet test race
