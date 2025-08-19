# Makefile for Tradr Backfiller Relay

# Variables
BINARY_NAME=relay
DOCKER_IMAGE=tradr-backfiller-relay
DOCKER_TAG=latest
GO=go
PROTOC=protoc
PROTO_DIR=proto
CONFIG_FILE=config/config.yaml

# Go build flags
GOFLAGS=-v
LDFLAGS=-ldflags="-w -s"

# Colors for output
COLOR_RESET=\033[0m
COLOR_BOLD=\033[1m
COLOR_GREEN=\033[32m
COLOR_YELLOW=\033[33m

.PHONY: help
help: ## Show this help message
	@echo "$(COLOR_BOLD)Available targets:$(COLOR_RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(COLOR_GREEN)%-20s$(COLOR_RESET) %s\n", $$1, $$2}'

.PHONY: all
all: clean proto build ## Clean, generate protobuf, and build

.PHONY: proto
proto: ## Generate protobuf code
	@echo "$(COLOR_YELLOW)Generating protobuf code...$(COLOR_RESET)"
	$(PROTOC) --go_out=. --go-grpc_out=. $(PROTO_DIR)/ingester.proto
	@echo "$(COLOR_GREEN)✓ Protobuf code generated$(COLOR_RESET)"

.PHONY: build
build: ## Build the binary
	@echo "$(COLOR_YELLOW)Building $(BINARY_NAME)...$(COLOR_RESET)"
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o bin/$(BINARY_NAME) ./cmd/main.go
	@echo "$(COLOR_GREEN)✓ Binary built: bin/$(BINARY_NAME)$(COLOR_RESET)"

.PHONY: run
run: build ## Build and run the service
	@echo "$(COLOR_YELLOW)Running $(BINARY_NAME)...$(COLOR_RESET)"
	./bin/$(BINARY_NAME)

.PHONY: test
test: ## Run tests
	@echo "$(COLOR_YELLOW)Running tests...$(COLOR_RESET)"
	$(GO) test -v -race -cover ./...
	@echo "$(COLOR_GREEN)✓ Tests completed$(COLOR_RESET)"

.PHONY: test-coverage
test-coverage: ## Run tests with coverage report
	@echo "$(COLOR_YELLOW)Running tests with coverage...$(COLOR_RESET)"
	$(GO) test -v -race -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "$(COLOR_GREEN)✓ Coverage report generated: coverage.html$(COLOR_RESET)"

.PHONY: lint
lint: ## Run linter
	@echo "$(COLOR_YELLOW)Running linter...$(COLOR_RESET)"
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed. Install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	fi
	@echo "$(COLOR_GREEN)✓ Linting completed$(COLOR_RESET)"

.PHONY: fmt
fmt: ## Format code
	@echo "$(COLOR_YELLOW)Formatting code...$(COLOR_RESET)"
	$(GO) fmt ./...
	@echo "$(COLOR_GREEN)✓ Code formatted$(COLOR_RESET)"

.PHONY: vet
vet: ## Run go vet
	@echo "$(COLOR_YELLOW)Running go vet...$(COLOR_RESET)"
	$(GO) vet ./...
	@echo "$(COLOR_GREEN)✓ Vet completed$(COLOR_RESET)"

.PHONY: deps
deps: ## Download dependencies
	@echo "$(COLOR_YELLOW)Downloading dependencies...$(COLOR_RESET)"
	$(GO) mod download
	$(GO) mod tidy
	@echo "$(COLOR_GREEN)✓ Dependencies downloaded$(COLOR_RESET)"

.PHONY: update-deps
update-deps: ## Update dependencies
	@echo "$(COLOR_YELLOW)Updating dependencies...$(COLOR_RESET)"
	$(GO) get -u ./...
	$(GO) mod tidy
	@echo "$(COLOR_GREEN)✓ Dependencies updated$(COLOR_RESET)"

.PHONY: docker-build
docker-build: ## Build Docker image
	@echo "$(COLOR_YELLOW)Building Docker image...$(COLOR_RESET)"
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .
	@echo "$(COLOR_GREEN)✓ Docker image built: $(DOCKER_IMAGE):$(DOCKER_TAG)$(COLOR_RESET)"

.PHONY: docker-run
docker-run: ## Run service in Docker
	@echo "$(COLOR_YELLOW)Running Docker container...$(COLOR_RESET)"
	docker run -p 50051:50051 -p 9090:9090 \
		-e DATABASE_DSN="postgres://relay:relay@host.docker.internal:5432/backfiller_relay?sslmode=disable" \
		-e FIREHOSE_HOST="wss://bsky.network" \
		-e INGESTER_ADDRESSES="host.docker.internal:50052" \
		$(DOCKER_IMAGE):$(DOCKER_TAG)

.PHONY: docker-compose-up
docker-compose-up: ## Start services with docker-compose
	@echo "$(COLOR_YELLOW)Starting services with docker-compose...$(COLOR_RESET)"
	docker-compose up -d
	@echo "$(COLOR_GREEN)✓ Services started$(COLOR_RESET)"

.PHONY: docker-compose-down
docker-compose-down: ## Stop services with docker-compose
	@echo "$(COLOR_YELLOW)Stopping services...$(COLOR_RESET)"
	docker-compose down
	@echo "$(COLOR_GREEN)✓ Services stopped$(COLOR_RESET)"

.PHONY: docker-compose-logs
docker-compose-logs: ## Show docker-compose logs
	docker-compose logs -f

.PHONY: clean
clean: ## Clean build artifacts
	@echo "$(COLOR_YELLOW)Cleaning build artifacts...$(COLOR_RESET)"
	rm -rf bin/ coverage.out coverage.html
	@echo "$(COLOR_GREEN)✓ Clean completed$(COLOR_RESET)"

.PHONY: install
install: build ## Install binary to GOPATH/bin
	@echo "$(COLOR_YELLOW)Installing $(BINARY_NAME)...$(COLOR_RESET)"
	$(GO) install ./cmd/main.go
	@echo "$(COLOR_GREEN)✓ Installed to $$GOPATH/bin$(COLOR_RESET)"

.PHONY: proto-deps
proto-deps: ## Install protobuf dependencies
	@echo "$(COLOR_YELLOW)Installing protobuf dependencies...$(COLOR_RESET)"
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo "$(COLOR_GREEN)✓ Protobuf dependencies installed$(COLOR_RESET)"

.PHONY: dev
dev: ## Run service in development mode with hot reload
	@echo "$(COLOR_YELLOW)Running in development mode...$(COLOR_RESET)"
	@if command -v air >/dev/null 2>&1; then \
		air; \
	else \
		echo "Air not installed. Install with: go install github.com/cosmtrek/air@latest"; \
		$(MAKE) run; \
	fi

.PHONY: check
check: fmt vet lint test ## Run all checks (format, vet, lint, test)
	@echo "$(COLOR_GREEN)✓ All checks passed$(COLOR_RESET)"

.PHONY: ci
ci: deps check build ## Run CI pipeline
	@echo "$(COLOR_GREEN)✓ CI pipeline completed$(COLOR_RESET)"

# Database operations
.PHONY: db-migrate
db-migrate: ## Run database migrations
	@echo "$(COLOR_YELLOW)Running database migrations...$(COLOR_RESET)"
	@echo "Migrations are run automatically on startup"
	@echo "$(COLOR_GREEN)✓ Done$(COLOR_RESET)"

.PHONY: db-reset
db-reset: ## Reset database (WARNING: destroys all data)
	@echo "$(COLOR_YELLOW)Resetting database...$(COLOR_RESET)"
	docker-compose down -v
	docker-compose up -d postgres
	@echo "$(COLOR_GREEN)✓ Database reset$(COLOR_RESET)"

# Monitoring
.PHONY: metrics
metrics: ## Show current metrics
	@echo "$(COLOR_YELLOW)Fetching metrics...$(COLOR_RESET)"
	curl -s http://localhost:9090/metrics | grep -E "^relay_"

.PHONY: health
health: ## Check service health
	@echo "$(COLOR_YELLOW)Checking service health...$(COLOR_RESET)"
	@curl -s http://localhost:9090/health && echo " - Metrics: $(COLOR_GREEN)✓$(COLOR_RESET)" || echo " - Metrics: $(COLOR_RED)✗$(COLOR_RESET)"
	@grpcurl -plaintext localhost:50051 ingester.v1.IngesterService/HealthCheck && echo " - gRPC: $(COLOR_GREEN)✓$(COLOR_RESET)" || echo " - gRPC: $(COLOR_RED)✗$(COLOR_RESET)"