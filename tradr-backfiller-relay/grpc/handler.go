package grpc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/rs/zerolog/log"
	pb "tradr-backfiller-relay/proto"
	"tradr-backfiller-relay/metrics"
)

// Handler implements the relay.Handler interface and broadcasts
// backfiller operations to all connected ingester clients
type Handler struct {
	// server handles all client connections
	server *Server
	
	// metrics tracks operation counts, latencies, and errors
	metrics *metrics.Collector
}

// NewHandler creates a new gRPC handler with server
func NewHandler(server *Server, metrics *metrics.Collector) *Handler {
	return &Handler{
		server:  server,
		metrics: metrics,
	}
}

// HandleCreateRecord broadcasts a create operation to all connected clients
func (h *Handler) HandleCreateRecord(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid, seq int64) error {
	// Track timing for metrics
	start := time.Now()
	defer func() {
		h.metrics.RecordOperationLatency("create", time.Since(start))
	}()

	// Increment operation counter
	h.metrics.IncrementOperationCount("create")

	// Validate inputs
	if repo == "" {
		return fmt.Errorf("empty repo DID")
	}
	if path == "" {
		return fmt.Errorf("empty record path")
	}

	// Parse collection from path (e.g., "app.bsky.feed.post/abc123" -> "app.bsky.feed.post")
	collection := path
	if idx := strings.IndexByte(path, '/'); idx > 0 {
		collection = path[:idx]
	}

	// Parse rkey from path (e.g., "app.bsky.feed.post/abc123" -> "abc123")
	rkey := ""
	if idx := strings.IndexByte(path, '/'); idx > 0 {
		rkey = path[idx+1:]
	}
	
	// Build the gRPC request
	req := &pb.OperationRequest{
		Seq:        seq,
		Repo:       repo,
		Rev:        rev,
		Rkey:       rkey,
		Collection: collection,
		Time:       time.Now().Format(time.RFC3339),
		Operation: &pb.OperationRequest_Create{
			Create: &pb.CreateOperation{
				Record: *rec,
				Cid:    cid.String(),
			},
		},
	}

	// Broadcast to all connected clients
	err := h.server.Broadcast(req)
	if err != nil {
		h.metrics.IncrementErrorCount("create", err)
		log.Debug().
			Err(err).
			Str("repo", repo).
			Str("path", path).
			Msg("Failed to broadcast create operation")
		return fmt.Errorf("failed to broadcast create operation: %w", err)
	}

	return nil
}

// HandleUpdateRecord broadcasts an update operation to all connected clients
func (h *Handler) HandleUpdateRecord(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid, seq int64) error {
	// Track timing for metrics
	start := time.Now()
	defer func() {
		h.metrics.RecordOperationLatency("update", time.Since(start))
	}()

	// Increment operation counter
	h.metrics.IncrementOperationCount("update")

	// Validate inputs
	if repo == "" {
		return fmt.Errorf("empty repo DID")
	}
	if path == "" {
		return fmt.Errorf("empty record path")
	}

	// Parse collection from path
	collection := path
	if idx := strings.IndexByte(path, '/'); idx > 0 {
		collection = path[:idx]
	}

	// Parse rkey from path
	rkey := ""
	if idx := strings.IndexByte(path, '/'); idx > 0 {
		rkey = path[idx+1:]
	}

	// Build the gRPC request
	req := &pb.OperationRequest{
		Seq:        seq,
		Repo:       repo,
		Rev:        rev,
		Rkey:       rkey,
		Collection: collection,
		Time:       time.Now().Format(time.RFC3339),
		Operation: &pb.OperationRequest_Update{
			Update: &pb.UpdateOperation{
				Record: *rec,
				Cid:    cid.String(),
			},
		},
	}

	// Broadcast to all connected clients
	err := h.server.Broadcast(req)
	if err != nil {
		h.metrics.IncrementErrorCount("update", err)
		log.Debug().
			Err(err).
			Str("repo", repo).
			Str("path", path).
			Msg("Failed to broadcast update operation")
		return fmt.Errorf("failed to broadcast update operation: %w", err)
	}

	return nil
}

// HandleDeleteRecord broadcasts a delete operation to all connected clients
func (h *Handler) HandleDeleteRecord(ctx context.Context, repo string, rev string, path string, seq int64) error {
	// Track timing for metrics
	start := time.Now()
	defer func() {
		h.metrics.RecordOperationLatency("delete", time.Since(start))
	}()

	// Increment operation counter
	h.metrics.IncrementOperationCount("delete")

	// Validate inputs
	if repo == "" {
		return fmt.Errorf("empty repo DID")
	}
	if path == "" {
		return fmt.Errorf("empty record path")
	}

	// Parse collection from path
	collection := path
	if idx := strings.IndexByte(path, '/'); idx > 0 {
		collection = path[:idx]
	}

	// Parse rkey from path
	rkey := ""
	if idx := strings.IndexByte(path, '/'); idx > 0 {
		rkey = path[idx+1:]
	}

	// Build the gRPC request
	req := &pb.OperationRequest{
		Seq:        seq,
		Repo:       repo,
		Rev:        rev,
		Rkey:       rkey,
		Collection: collection,
		Time:       time.Now().Format(time.RFC3339),
		Operation: &pb.OperationRequest_Delete{
			Delete: &pb.DeleteOperation{},
		},
	}

	// Broadcast to all connected clients
	err := h.server.Broadcast(req)
	if err != nil {
		h.metrics.IncrementErrorCount("delete", err)
		log.Debug().
			Err(err).
			Str("repo", repo).
			Str("path", path).
			Msg("Failed to broadcast delete operation")
		return fmt.Errorf("failed to broadcast delete operation: %w", err)
	}

	return nil
}