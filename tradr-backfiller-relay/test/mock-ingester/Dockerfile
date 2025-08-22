# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /build

# Copy local indigo module first (for replace directive)
COPY indigo /indigo

# Copy go mod files from the main project
COPY tradr-backfiller-relay/go.mod tradr-backfiller-relay/go.sum ./
RUN go mod download

# Copy necessary files
COPY tradr-backfiller-relay/proto/ proto/
COPY tradr-backfiller-relay/test/mock-ingester/client.go ./

# Install protoc and generate proto files
RUN apk add --no-cache protobuf protobuf-dev && \
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Generate proto files
RUN protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/ingester.proto

# Build the mock ingester client
RUN go build -o /mock-ingester ./client.go

# Run stage
FROM alpine:latest

RUN apk add --no-cache ca-certificates

COPY --from=builder /mock-ingester /mock-ingester

ENTRYPOINT ["/mock-ingester"]