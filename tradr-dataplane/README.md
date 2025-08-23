# Bsky Data Plane Server

This service runs the Bluesky (bsky) data plane server, which provides the data access layer for the Bluesky AppView.

## Overview

The data plane server is a gRPC service that:
- Manages the PostgreSQL database containing social graph data
- Provides efficient data access for the AppView API
- Handles database migrations
- Processes subscriptions from the PDS (Personal Data Server)

## Configuration

The service is configured via environment variables:

- `BSKY_DATAPLANE_PORT` - Server port (default: 3000)
- `BSKY_DATAPLANE_DB_POSTGRES_URL` - PostgreSQL connection string (required)
- `BSKY_DATAPLANE_DB_POSTGRES_SCHEMA` - Database schema name (default: 'bsky')
- `BSKY_DATAPLANE_DB_MIGRATE` - Run migrations on startup when set to '1'
- `BSKY_DATAPLANE_DB_POOL_SIZE` - Database connection pool size (default: 10)
- `BSKY_DATAPLANE_DB_POOL_MAX_USES` - Max uses per connection (default: Infinity)
- `BSKY_DATAPLANE_DB_POOL_IDLE_TIMEOUT_MS` - Idle timeout in ms (default: 10000)
- `BSKY_DID_PLC_URL` - PLC directory URL (default: 'https://plc.directory')

## Running Locally

```bash
# Set required environment variables
export BSKY_DATAPLANE_DB_POSTGRES_URL="postgresql://pg:password@localhost:5432/postgres"
export BSKY_DATAPLANE_DB_MIGRATE=1

# Run the service
node index.js
```

## Docker

Build the image:
```bash
docker build -t bsky-dataplane .
```

Run with Docker:
```bash
docker run -p 3000:3000 \
  -e BSKY_DATAPLANE_DB_POSTGRES_URL="postgresql://..." \
  -e BSKY_DATAPLANE_DB_MIGRATE=1 \
  bsky-dataplane
```

## Database Migrations

The service can run database migrations on startup when `BSKY_DATAPLANE_DB_MIGRATE=1` is set. 

For production deployments, you may want to run migrations separately:
- Set `BSKY_DATAPLANE_DB_MIGRATE=0` for the main service
- Run migrations as a separate job/init container with `BSKY_DATAPLANE_DB_MIGRATE=1`

## Deployment

This service should be deployed alongside:
- PostgreSQL database
- Redis (optional, for caching)
- PDS (for receiving updates)
- Bsky AppView (which queries this data plane)

The data plane server does not directly serve public API requests - it provides data access for the Bsky AppView service.