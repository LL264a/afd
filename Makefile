# AFD Makefile
# Cross-platform convenience targets. Works on Linux, macOS, and Windows
# (via Git Bash, WSL, or `make` from msys2/chocolatey).

GO          ?= go
BIN_DIR     ?= bin
BIN_NAME    ?= afd
BIN_PATH    := $(BIN_DIR)/$(BIN_NAME)

VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_TIME  ?= $(shell date -u +%FT%TZ)
LDFLAGS     := -s -w -X main.Version=$(VERSION) -X main.Commit=$(COMMIT) -X main.BuildTime=$(BUILD_TIME)

PKG         := ./cmd/afd
TEST_PKGS   := ./...
COVERAGE    := coverage.out

.EXPORT_ALL_VARIABLES:
GOFLAGS = -trimpath

.PHONY: all build run test test-race cover vet fmt lint tidy docker compose-up compose-down clean help

all: build

## help: Show this help
help:
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## build: Build the binary into $(BIN_PATH)
build:
	@mkdir -p $(BIN_DIR)
	$(GO) build -ldflags="$(LDFLAGS)" -o $(BIN_PATH) $(PKG)

## run: Run `serve` against config.yaml in the current directory
run: build
	./$(BIN_PATH) serve -c config.yaml

## test: Run all unit tests
test:
	$(GO) test -count=1 -timeout=300s $(TEST_PKGS)

## test-race: Run tests with the race detector (requires CGO_ENABLED=1)
test-race:
	CGO_ENABLED=1 $(GO) test -race -count=1 -timeout=300s $(TEST_PKGS)

## cover: Run tests with coverage and render an HTML report
cover: $(COVERAGE)
	$(GO) tool cover -html=$(COVERAGE) -o coverage.html
	@echo "open coverage.html"

$(COVERAGE):
	$(GO) test -count=1 -coverprofile=$(COVERAGE) -covermode=atomic $(TEST_PKGS)

## vet: Run go vet
vet:
	$(GO) vet $(TEST_PKGS)

## fmt: Format all sources
fmt:
	$(GO) fmt ./...
	@command -v goimports >/dev/null 2>&1 || $(GO) install golang.org/x/tools/cmd/goimports@latest
	goimports -w .

## lint: gofmt + goimports + go vet
lint: fmt vet
	@if [ "$$(gofmt -l . | wc -l)" -gt 0 ]; then echo "gofmt found issues"; gofmt -l .; exit 1; fi
	@command -v goimports >/dev/null 2>&1 || $(GO) install golang.org/x/tools/cmd/goimports@latest
	@if [ "$$(goimports -l . | wc -l)" -gt 0 ]; then echo "goimports found issues"; goimports -l .; exit 1; fi

## tidy: go mod tidy
tidy:
	$(GO) mod tidy

## docker: Build the Docker image
docker:
	docker build -t afd:$(VERSION) \
	  --build-arg VERSION=$(VERSION) \
	  --build-arg COMMIT=$(COMMIT) \
	  --build-arg BUILD_TIME=$(BUILD_TIME) .

## compose-up: Bring up the dev cluster
compose-up:
	docker compose up -d

## compose-down: Tear down the dev cluster
compose-down:
	docker compose down

## clean: Remove build artifacts
clean:
	rm -rf $(BIN_DIR) $(COVERAGE) coverage.html
