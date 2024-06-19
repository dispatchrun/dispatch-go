.PHONY: clean coroc fmt lint test integration-test clean coroc

fmt:
	go fmt ./...

lint:
	golangci-lint run

test:
	go test ./...

integration-test: clean coroc
	coroc ./dispatchtest/integration
	go run -tags durable ./dispatchtest/integration

clean:
	find . -name '*_durable.go' -delete

coroc:
	@which coroc &>/dev/null \
		|| echo "Installing coroc..." \
		&& go install github.com/dispatchrun/coroutine/compiler/cmd/coroc@latest
