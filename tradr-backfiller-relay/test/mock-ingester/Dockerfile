# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy proto files and generate
COPY proto/ proto/
COPY test/mock-ingester/ test/mock-ingester/

# Install protoc
RUN apk add --no-cache protobuf protobuf-dev

# Build the mock ingester
RUN go build -o /mock-ingester ./test/mock-ingester/main.go

# Run stage
FROM alpine:latest

RUN apk add --no-cache ca-certificates

COPY --from=builder /mock-ingester /mock-ingester

ENTRYPOINT ["/mock-ingester"]