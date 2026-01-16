# QDHub Makefile
# Build and deployment automation

# Variables
APP_NAME := qdhub
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME)"
GO := go
GOTEST := $(GO) test
GOBUILD := $(GO) build $(LDFLAGS)

# Directories
BIN_DIR := bin
CMD_DIR := cmd

.PHONY: all build clean test lint run help
.PHONY: build-linux build-darwin build-windows build-all
.PHONY: test-unit test-integration test-e2e test-all
.PHONY: deploy-dev deploy-prod
.PHONY: deps tidy fmt vet
.PHONY: install-swag docs

# Default target
all: deps build test-unit

# Help
help:
	@echo "QDHub Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make build          Build for current platform"
	@echo "  make build-all      Build for all platforms"
	@echo "  make test           Run all tests"
	@echo "  make test-unit      Run unit tests"
	@echo "  make test-integration  Run integration tests"
	@echo "  make test-e2e       Run e2e tests"
	@echo "  make run            Run the application"
	@echo "  make lint           Run linters"
	@echo "  make clean          Clean build artifacts"
	@echo "  make deps           Download dependencies"
	@echo "  make install-swag   Install swag tool for API docs"
	@echo "  make docs           Generate Swagger API documentation"
	@echo "  make deploy-dev     Deploy to dev environment"
	@echo "  make deploy-prod    Deploy to production"
	@echo ""

# Dependencies
deps:
	$(GO) mod download

tidy:
	$(GO) mod tidy

# Build targets
build: deps
	$(GOBUILD) -o $(BIN_DIR)/$(APP_NAME) .

build-linux: deps
	GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(BIN_DIR)/$(APP_NAME)-linux-amd64 .
	GOOS=linux GOARCH=arm64 $(GOBUILD) -o $(BIN_DIR)/$(APP_NAME)-linux-arm64 .

build-darwin: deps
	GOOS=darwin GOARCH=amd64 $(GOBUILD) -o $(BIN_DIR)/$(APP_NAME)-darwin-amd64 .
	GOOS=darwin GOARCH=arm64 $(GOBUILD) -o $(BIN_DIR)/$(APP_NAME)-darwin-arm64 .

build-windows: deps
	GOOS=windows GOARCH=amd64 $(GOBUILD) -o $(BIN_DIR)/$(APP_NAME)-windows-amd64.exe .

build-all: build-linux build-darwin build-windows

# Test targets
test: test-unit

test-unit:
	$(GOTEST) -v -race -coverprofile=coverage.out ./internal/... ./pkg/...

test-integration:
	$(GOTEST) -v -tags=integration ./tests/integration/...

test-e2e:
	$(GOTEST) -v -tags=e2e ./tests/e2e/...

test-all: test-unit test-integration test-e2e

# Coverage report
coverage: test-unit
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Code quality
fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

lint: fmt vet
	@echo "Linting completed"

# Run
run: build
	./$(BIN_DIR)/$(APP_NAME) server --config configs/config.yaml

run-dev:
	$(GO) run . -config configs/config.yaml

# Swagger documentation
install-swag:
	@echo "Installing swag tool..."
	@$(GO) install github.com/swaggo/swag/cmd/swag@latest
	@echo "swag tool installed successfully"

docs: install-swag
	@echo "Generating Swagger documentation..."
	@if [ -f ./$(BIN_DIR)/$(APP_NAME) ]; then \
		./$(BIN_DIR)/$(APP_NAME) docs; \
	else \
		$(GO) run . docs; \
	fi

# Clean
clean:
	rm -rf $(BIN_DIR)
	rm -f coverage.out coverage.html

# Database migrations
migrate-up:
	@echo "Running database migrations..."
	@./scripts/migrate.sh up

migrate-down:
	@echo "Rolling back last migration..."
	@./scripts/migrate.sh down

# Deployment
deploy-dev:
	@./scripts/deploy.sh dev

deploy-prod:
	@./scripts/deploy.sh prod

# Docker (future)
docker-build:
	docker build -t $(APP_NAME):$(VERSION) .

docker-run:
	docker run -p 8080:8080 $(APP_NAME):$(VERSION)
