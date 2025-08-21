# Run this dockerfile from the tradr-backfiller-relay directory like:
#
#   docker build -t tradr-relay .

### Compile stage
FROM golang:1.25-alpine AS build-env
RUN apk add --no-cache build-base make git

# Set working directory
WORKDIR /dockerbuild

# Copy the local indigo module (for replace directive)
COPY indigo /indigo

# Copy go.mod and go.sum first for better caching  
COPY tradr-backfiller-relay/go.mod tradr-backfiller-relay/go.sum ./

# Download dependencies - this layer will be cached unless go.mod/go.sum change
RUN go mod download

# Copy all relay source code
COPY tradr-backfiller-relay/ .

# Install protoc and Go protobuf plugins
RUN apk add --no-cache protobuf protobuf-dev && \
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Generate protobuf code
RUN protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/ingester.proto

# Build with same flags as indigo services
# timezone data for alpine builds
ENV GOEXPERIMENT=loopvar
RUN go build -tags timetzdata -o /relay ./cmd/main.go

### Run stage
FROM alpine:latest

# Install runtime dependencies (same as indigo services)
RUN apk add --no-cache --update dumb-init ca-certificates runit
ENTRYPOINT ["dumb-init", "--"]

# Set working directory
WORKDIR /

# Create data directory
RUN mkdir -p data/relay

# Copy the binary from builder stage
COPY --from=build-env /relay /relay

# Copy configuration file if needed
COPY --from=build-env /dockerbuild/config/config.yaml /config/config.yaml

# Create non-root user for security
RUN addgroup -g 1000 relay && \
    adduser -D -u 1000 -G relay relay && \
    chown -R relay:relay /data

# Small things to make golang binaries work well under alpine (same as indigo)
ENV GODEBUG=netdns=go
ENV TZ=Etc/UTC

# Set default environment variables
ENV CONFIG_PATH=/config/config.yaml \
    LOG_LEVEL=info

# Switch to non-root user
USER relay

# Expose ports
# 50051 - gRPC port (if still using gRPC)
# 9090  - Prometheus metrics port
EXPOSE 50051 9090

# Run the relay service
CMD ["/relay"]

LABEL org.opencontainers.image.source=https://github.com/yourusername/tradr-backfiller
LABEL org.opencontainers.image.description="Tradr Backfiller Relay"
LABEL org.opencontainers.image.licenses=MIT