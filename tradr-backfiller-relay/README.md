# Tradr Backfiller Relay

A high-performance relay service that bridges the AT Protocol firehose with gRPC-based Ingester services, providing reliable data streaming with backfilling capabilities.

## Overview

The Tradr Backfiller Relay acts as an intermediary between the AT Protocol firehose and your data processing pipeline. It:

- **Consumes real-time events** from the AT Protocol firehose
- **Handles historical repository backfilling** for complete data indexing
- **Buffers operations** during backfill to prevent data loss
- **Distributes operations** to multiple Ingester instances via gRPC
- **Provides metrics** for monitoring and observability
- **Manages state** persistently in PostgreSQL

## Architecture

```
AT Protocol Firehose
        ↓
  [Backfiller Relay]
   ├── Real-time events
   ├── Backfill processor
   └── Buffer manager
        ↓
  gRPC Streaming
        ↓
  [Ingester Services]
   ├── Ingester 1
   ├── Ingester 2
   └── Ingester N
        ↓
    AppView DB
```

## Features

- **Real-time Processing**: Streams firehose events with minimal latency
- **Backfilling**: Fetches and processes historical repository data
- **Buffering**: Queues operations during backfills to prevent loss
- **Load Balancing**: Distributes operations across multiple Ingesters
- **Fault Tolerance**: Automatic reconnection and retry logic
- **Observability**: Prometheus metrics and health checks
- **Scalability**: Horizontally scalable with multiple relay instances

## Quick Start

### Prerequisites

- Go 1.21 or later
- PostgreSQL 15 or later
- Protocol Buffers compiler (`protoc`)
- Docker and Docker Compose (optional)

### Installation

1. **Clone the repository**:
```bash
git clone <repository-url>
cd tradr-backfiller-relay
```

2. **Install dependencies**:
```bash
make deps
make proto-deps
```

3. **Generate protobuf code**:
```bash
make proto
```

4. **Build the binary**:
```bash
make build
```

### Running Locally

#### Using Docker Compose (Recommended)

```bash
# Start all services (PostgreSQL, Relay, Prometheus, Grafana)
make docker-compose-up

# View logs
make docker-compose-logs

# Stop services
make docker-compose-down
```

#### Manual Setup

1. **Start PostgreSQL**:
```bash
docker run -d \
  --name relay-postgres \
  -e POSTGRES_USER=relay \
  -e POSTGRES_PASSWORD=relay \
  -e POSTGRES_DB=backfiller_relay \
  -p 5432:5432 \
  postgres:15-alpine
```

2. **Configure the service**:
Edit `config/config.yaml` or set environment variables:
```bash
export DATABASE_DSN="postgres://relay:relay@localhost:5432/backfiller_relay?sslmode=disable"
export FIREHOSE_HOST="wss://bsky.network"
export INGESTER_ADDRESSES="localhost:50052,localhost:50053"
```

3. **Run the relay**:
```bash
make run
```

## Configuration

Configuration can be provided via `config/config.yaml` or environment variables.

### Key Configuration Options

| Config Key | Environment Variable | Description | Default |
|------------|---------------------|-------------|---------|
| `relay.name` | `RELAY_NAME` | Service instance name | `tradr-relay` |
| `firehose.relay_host` | `FIREHOSE_HOST` | AT Protocol relay WebSocket URL | `wss://bsky.network` |
| `firehose.nsid_filter` | `NSID_FILTER` | Filter for record types | `app.bsky.` |
| `backfiller.parallel_backfills` | `PARALLEL_BACKFILLS` | Concurrent backfill jobs | `10` |
| `database.dsn` | `DATABASE_DSN` | PostgreSQL connection string | Required |
| `ingesters` | `INGESTER_ADDRESSES` | Comma-separated Ingester addresses | Required |
| `grpc.port` | `GRPC_PORT` | Management gRPC port | `50051` |
| `metrics.port` | `METRICS_PORT` | Prometheus metrics port | `9090` |

### Full Configuration Example

```yaml
relay:
  name: "production-relay"
  max_retries: 5
  retry_backoff_ms: 2000

firehose:
  relay_host: "wss://bsky.network"
  nsid_filter: "app.bsky."
  max_reconnect_attempts: -1
  reconnect_delay_ms: 5000

backfiller:
  parallel_backfills: 20
  parallel_record_creates: 200
  buffer_size: 50000
  checkpoint_interval: 5000

grpc:
  port: 50051
  max_message_size: 20971520  # 20MB
  keepalive_time_sec: 30

ingesters:
  - "ingester-1.internal:50052"
  - "ingester-2.internal:50052"
  - "ingester-3.internal:50052"

database:
  dsn: "postgres://relay:password@db.internal:5432/relay?sslmode=require"
  max_open_conns: 50
  max_idle_conns: 10

metrics:
  port: 9090
  enable_runtime_metrics: true

log_level: "info"
```

## API Reference

### gRPC Service Definition

The relay implements the `IngesterService` defined in `proto/ingester.proto`:

```protobuf
service IngesterService {
  // Bidirectional streaming for continuous operations
  rpc StreamOperations(stream OperationRequest) returns (stream OperationResponse);
  
  // Single operation ingestion
  rpc Ingest(OperationRequest) returns (OperationResponse);
  
  // Health check endpoint
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}
```

### Metrics Endpoints

- `GET /metrics` - Prometheus metrics
- `GET /health` - Health check endpoint

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `relay_operations_total` | Counter | Total operations by type |
| `relay_operations_sent_total` | Counter | Operations sent to Ingesters |
| `relay_operations_buffered_total` | Counter | Operations buffered |
| `relay_operation_duration_seconds` | Histogram | Operation processing latency |
| `relay_buffered_operations` | Gauge | Current buffered operations |
| `relay_healthy_ingesters` | Gauge | Number of healthy connections |
| `relay_errors_total` | Counter | Errors by type |

## Development

### Running Tests

```bash
# Run all tests
make test

# Run with coverage
make test-coverage

# Run specific package tests
go test -v ./grpc/...
```

### Code Quality

```bash
# Format code
make fmt

# Run linter
make lint

# Run all checks
make check
```

### Building Docker Image

```bash
# Build image
make docker-build

# Run container
make docker-run
```

## Monitoring

### Prometheus

The relay exposes Prometheus metrics on port 9090 by default. Add to your Prometheus configuration:

```yaml
scrape_configs:
  - job_name: 'tradr-relay'
    static_configs:
      - targets: ['relay-host:9090']
```

### Grafana Dashboard

Import the dashboard from `grafana/dashboard.json` for visualization of:
- Operation throughput
- Error rates
- Buffer utilization
- Connection health
- Latency percentiles

### Health Checks

```bash
# Check metrics endpoint
curl http://localhost:9090/health

# Check gRPC health
grpcurl -plaintext localhost:50051 ingester.v1.IngesterService/HealthCheck
```

## Deployment

### Kubernetes

Example deployment configuration:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tradr-relay
spec:
  replicas: 3
  selector:
    matchLabels:
      app: tradr-relay
  template:
    metadata:
      labels:
        app: tradr-relay
    spec:
      containers:
      - name: relay
        image: tradr-backfiller-relay:latest
        env:
        - name: DATABASE_DSN
          valueFrom:
            secretKeyRef:
              name: relay-secrets
              key: database-dsn
        - name: INGESTER_ADDRESSES
          value: "ingester-service:50052"
        ports:
        - containerPort: 50051
          name: grpc
        - containerPort: 9090
          name: metrics
        livenessProbe:
          httpGet:
            path: /health
            port: 9090
        readinessProbe:
          httpGet:
            path: /health
            port: 9090
```

### Docker Swarm

```bash
docker service create \
  --name tradr-relay \
  --replicas 3 \
  --env DATABASE_DSN="..." \
  --env INGESTER_ADDRESSES="..." \
  --publish 50051:50051 \
  --publish 9090:9090 \
  tradr-backfiller-relay:latest
```

## Troubleshooting

### Common Issues

1. **Connection to Ingesters failing**
   - Check network connectivity
   - Verify Ingester addresses are correct
   - Check gRPC port accessibility

2. **High buffer usage**
   - Increase `buffer_size` in configuration
   - Add more Ingester instances
   - Check Ingester processing speed

3. **Database connection errors**
   - Verify PostgreSQL is running
   - Check connection string format
   - Ensure database exists

4. **Firehose disconnections**
   - Check network stability
   - Verify firehose URL is correct
   - Monitor reconnection attempts in logs

### Debug Mode

Enable debug logging:
```bash
export LOG_LEVEL=debug
./bin/relay
```

### Performance Tuning

- **Increase parallel backfills**: Set `PARALLEL_BACKFILLS` higher for faster processing
- **Adjust buffer size**: Increase `buffer_size` for handling bursts
- **Connection pool**: Tune `max_open_conns` based on load
- **Message size**: Adjust `max_message_size` for large records

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run `make check`
6. Submit a pull request

## License

[Add your license information here]

## Support

For issues and questions:
- Open an issue on GitHub
- Check existing issues for solutions
- Review logs with `docker-compose logs relay`

## Acknowledgments

This project builds upon:
- [Indigo](https://github.com/bluesky-social/indigo) - AT Protocol implementation
- [ATProto](https://atproto.com) - AT Protocol specification
- [Bluesky](https://bsky.app) - Social networking service