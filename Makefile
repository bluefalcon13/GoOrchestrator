# Binary name and build directory
BINARY_NAME := orchestrator
BUILD_DIR := ./build
DOCKER_IMAGE := go-orchestrator:latest
DOCKERFILE := builder/Dockerfile

# Go toolchain settings
GO ?= go
GOFLAGS ?= -trimpath
LDFLAGS ?= -s -w

.PHONY: all build build-static docker-build test test-coverage clean help

all: build

## build: Compiles the binary using host Go libraries into ./build
build:
	@mkdir -p $(BUILD_DIR)
	@echo "==> Building $(BINARY_NAME) into $(BUILD_DIR)/ using host toolchain..."
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_NAME) ./main.go
	@echo "==> Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

## build-static: Compiles a pure static, CGO-disabled binary into ./build (scratch-ready)
build-static:
	@mkdir -p $(BUILD_DIR)
	@echo "==> Building static $(BINARY_NAME) into $(BUILD_DIR)/..."
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS) -extldflags '-static'" -o $(BUILD_DIR)/$(BINARY_NAME) ./main.go
	@echo "==> Static build complete: $(BUILD_DIR)/$(BINARY_NAME)"

## docker-build: Builds the minimal FROM scratch Docker container
docker-build:
	@echo "==> Building Docker image $(DOCKER_IMAGE) from $(DOCKERFILE)..."
	docker build -f $(DOCKERFILE) -t $(DOCKERIMAGE) .
	@echo "==> Docker image build complete: $(DOCKER_IMAGE)"

## test: Runs all unit and integration tests across all packages
test:
	@echo "==> Running tests..."
	$(GO) test -v -count=1 ./...

## test-coverage: Runs all tests with coverage report output to ./build
test-coverage:
	@mkdir -p $(BUILD_DIR)
	@echo "==> Running tests with coverage..."
	$(GO) test -count=1 -coverprofile=$(BUILD_DIR)/coverage.out ./...
	$(GO) tool cover -func=$(BUILD_DIR)/coverage.out

## clean: Removes build artifacts and coverage files
clean:
	@echo "==> Cleaning build artifacts..."
	@rm -rf $(BUILD_DIR)
	@echo "==> Clean complete."

## help: Prints this help message
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':'

