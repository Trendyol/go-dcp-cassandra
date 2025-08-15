.PHONY: default

default: init

init:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.3.0
	go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@v0.35.0

clean:
	rm -rf ./build

linter:
	fieldalignment -fix ./...
	golangci-lint run -c .golangci.yml --timeout=5m -v --fix

lint:
	golangci-lint run -c .golangci.yml --timeout=5m -v

test:
	go test ./... -bench . -benchmem

compose:
	docker compose up --wait --build --force-recreate --remove-orphans

tidy:
	go mod tidy
	cd example/simple && go mod tidy && cd ../..