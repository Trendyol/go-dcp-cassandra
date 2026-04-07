GO_TOOLCHAIN ?= go1.25.8
BIN_DIR ?= ./bin

.PHONY: build
build:
	GOTOOLCHAIN=$(GO_TOOLCHAIN) go build -v ./...

.PHONY: tools
tools:
	GOTOOLCHAIN=$(GO_TOOLCHAIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.3.0
	GOTOOLCHAIN=$(GO_TOOLCHAIN) go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@v0.35.0

.PHONY: clean
clean:
	rm -rf $(BIN_DIR)

.PHONY: linter
linter: tools
	fieldalignment -fix ./...
	GOTOOLCHAIN=$(GO_TOOLCHAIN) golangci-lint run -c .golangci.yml --timeout=5m -v --fix

.PHONY: lint
lint: tools
	GOTOOLCHAIN=$(GO_TOOLCHAIN) golangci-lint run -c .golangci.yml --timeout=5m -v

.PHONY: test
test:
	GOTOOLCHAIN=$(GO_TOOLCHAIN) go test -v ./...

.PHONY: bench
bench:
	GOTOOLCHAIN=$(GO_TOOLCHAIN) go test ./... -bench . -benchmem

.PHONY: build-example
build-example:
	mkdir -p $(BIN_DIR)
	GOTOOLCHAIN=$(GO_TOOLCHAIN) go build -o $(BIN_DIR)/example-simple ./example/simple/main.go

.PHONY: compose
compose:
	docker compose up --wait --build --force-recreate --remove-orphans

.PHONY: tidy
tidy:
	GOTOOLCHAIN=$(GO_TOOLCHAIN) go mod tidy
	cd example/simple && GOTOOLCHAIN=$(GO_TOOLCHAIN) go mod tidy && cd ../..
