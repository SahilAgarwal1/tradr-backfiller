# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository is for developing a **high-throughput Bluesky backfilling and firehose indexing system**. The goal is to create an extremely fast system capable of processing the entire Bluesky network's data efficiently.

### Repository Structure

- **`tradr-backfiller-relay/`** - Our main implementation of the high-performance backfiller/relay system
- **`market/`** - Reference implementation of a high-speed backfiller by another developer (study for optimization techniques)
- **`indigo/`** - Bluesky's reference AT Protocol implementation library (dependency)
- **`atproto/`** - Bluesky's AT Protocol TypeScript reference (for understanding protocol)

### Current Implementation: Tradr Backfiller Relay

A high-performance service that bridges the AT Protocol (Bluesky) firehose with gRPC-based ingester services:
- Consumes real-time events from the AT Protocol firehose
- Handles historical repository backfilling from PostgreSQL queue
- Broadcasts operations to multiple connected ingester clients via gRPC
- Provides comprehensive metrics and monitoring

## Architecture

The relay uses a **server-client architecture** where:
- The relay is a gRPC **SERVER** (port 50051) that accepts connections from ingester clients
- Ingesters are gRPC **CLIENTS** that connect to the relay and receive operations
- Operations flow: AT Protocol Firehose → Relay Server → Multiple Ingester Clients

Key architectural decisions:
- No buffering needed - AT Protocol firehose maintains 24-hour buffer with cursor-based replay
- Backfiller uses PostgreSQL queue for persistence, no additional buffering required
- Server broadcasts operations to all connected clients simultaneously

## Common Commands

### Building and Running
```bash
# Generate protobuf code (required after proto changes)
make proto

# Build the binary
make build

# Run tests
make test

# Run with Docker Compose (includes monitoring stack)
docker-compose -f docker-compose.monitoring.yml up -d

# Restart monitoring stack to reload Grafana dashboards
docker-compose -f docker-compose.monitoring.yml restart grafana

# View logs
docker-compose -f docker-compose.monitoring.yml logs -f relay

# Run test environment with mock ingester
docker-compose -f docker-compose.test.yml up -d
./test/test.sh  # Run test suite
```

### Development
```bash
# Format code
make fmt

# Run linter
make lint

# Run all checks (format, vet, lint, test)
make check

# Check service health
make health

# View current metrics
curl http://localhost:9090/metrics | grep relay_
```

## Key Components

### gRPC Server (`grpc/server.go`)
- Manages connected ingester clients
- Broadcasts operations to all clients
- Handles client lifecycle (connect/disconnect)
- Tracks metrics for connected clients

### Handler (`grpc/handler.go`)
- Implements AT Protocol event handlers
- Converts firehose events to gRPC operations
- Broadcasts via server to all connected clients

### Relay (`relay/relay.go`)
- Main orchestrator connecting firehose to gRPC server
- Manages firehose WebSocket connection
- Handles cursor-based replay for reliability

### Metrics (`metrics/metrics.go`)
- Prometheus metrics collection
- Key metrics:
  - `relay_operations_total`: Total operations by type (create/update/delete)
  - `relay_repos_processed_total`: Total repositories processed
  - `relay_grpc_connection_state`: Health of gRPC connections
  - `relay_backfill_jobs_active`: Active backfill jobs

### Configuration (`config/config.go`)
- YAML or environment variable configuration
- Key settings: database DSN, firehose host, gRPC port

## Protocol Buffers

The gRPC service is defined in `proto/ingester.proto`:
- Server sends `OperationRequest` to clients
- Clients send `OperationResponse` back to server
- Bidirectional streaming for real-time updates

To regenerate after proto changes:
```bash
make proto
```

## Testing

The project includes a comprehensive test environment:
- `docker-compose.test.yml`: Test environment with mock ingester
- `test/test.sh`: Automated test suite
- `test/mock-ingester/client.go`: Mock ingester client for testing

## Monitoring

The project includes full observability:
- Prometheus metrics (port 9090)
- Grafana dashboards (`monitoring/grafana/dashboards/`)
- Dashboard shows:
  - Operations per second (individual record operations)
  - Jobs per minute (entire repository processing)
  - Repos per hour/second
  - Connection health
  - Backfill progress

### Key Metrics to Track
- **repos/sec**: Overall repository processing rate (target: >10)
- **records/sec**: Individual record operations (target: >10,000)  
- **Memory usage**: Should stay under 2GB for sustainable operation
- **Goroutine count**: Should match configured parallelism
- **Records per second**: New metric tracking actual throughput

## Important Notes

1. **No Buffering**: The relay doesn't buffer operations since both data sources (firehose and backfiller) have their own persistence mechanisms
2. **Server Architecture**: The relay is the SERVER, ingesters are CLIENTS (not the reverse)
3. **Metrics Clarification**: 
   - ops/sec = individual record operations (create/update/delete)
   - records/sec = actual record throughput
   - jobs/min = entire repository backfill jobs
4. **Protocol**: Uses AT Protocol (atproto) and Indigo libraries for Bluesky integration

## Dependencies

Key Go dependencies:
- `github.com/bluesky-social/indigo`: AT Protocol implementation (has performance limitations)
- `google.golang.org/grpc`: gRPC framework
- `github.com/prometheus/client_golang`: Metrics
- `gorm.io/gorm`: Database ORM (slower than raw SQL)
- `github.com/rs/zerolog`: Structured logging
- `github.com/jackc/pgx/v5`: PostgreSQL driver (consider for performance)