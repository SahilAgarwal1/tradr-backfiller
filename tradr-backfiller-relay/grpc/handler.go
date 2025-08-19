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

// Handler implements the relay.Handler interface and converts
// backfiller operations into gRPC requests to send to Ingesters
type Handler struct {
	// stream manages connections to multiple Ingester instances
	stream *StreamManager
	
	// metrics tracks operation counts, latencies, and errors
	metrics *metrics.Collector
}

// NewHandler creates a new gRPC handler
func NewHandler(stream *StreamManager, metrics *metrics.Collector) *Handler {
	return &Handler{
		stream:  stream,
		metrics: metrics,
	}
}

// HandleCreateRecord converts a create operation to a gRPC request and sends it
// This is called by the backfiller when a new record is created
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
	if rec == nil || len(*rec) == 0 {
		return fmt.Errorf("empty record data")
	}
	if cid == nil {
		return fmt.Errorf("nil CID")
	}

	// Parse the path to extract collection and rkey
	// Path format: "collection/rkey" e.g., "app.bsky.feed.post/3k2yihcrp6f2c"
	collection, rkey := h.parsePath(path)

	// Create the gRPC request
	req := &pb.OperationRequest{
		Repo:       repo,
		Rev:        rev,
		Collection: collection,
		Rkey:       rkey,
		Time:       time.Now().UTC().Format(time.RFC3339),
		Seq:        seq,
		Operation: &pb.OperationRequest_Create{
			Create: &pb.CreateOperation{
				Record: *rec,
				Cid:    cid.String(),
			},
		},
	}

	// Log the operation (debug level to avoid spam)
	log.Debug().
		Str("repo", repo).
		Str("path", path).
		Str("rev", rev).
		Str("cid", cid.String()).
		Int("record_size", len(*rec)).
		Msg("Sending create operation")

	// Send the request through the stream manager
	// The stream manager handles load balancing and retries
	err := h.stream.Send(ctx, req)
	if err != nil {
		h.metrics.IncrementErrorCount("create", err)
		return fmt.Errorf("failed to send create operation: %w", err)
	}

	return nil
}

// HandleUpdateRecord converts an update operation to a gRPC request and sends it
// This is called by the backfiller when an existing record is updated
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
	if rec == nil || len(*rec) == 0 {
		return fmt.Errorf("empty record data")
	}
	if cid == nil {
		return fmt.Errorf("nil CID")
	}

	// Parse the path to extract collection and rkey
	collection, rkey := h.parsePath(path)

	// Create the gRPC request
	req := &pb.OperationRequest{
		Repo:       repo,
		Rev:        rev,
		Collection: collection,
		Rkey:       rkey,
		Time:       time.Now().UTC().Format(time.RFC3339),
		Seq:        seq,
		Operation: &pb.OperationRequest_Update{
			Update: &pb.UpdateOperation{
				Record: *rec,
				Cid:    cid.String(),
				// PrevCid could be added here if we track it
			},
		},
	}

	// Log the operation (debug level to avoid spam)
	log.Debug().
		Str("repo", repo).
		Str("path", path).
		Str("rev", rev).
		Str("cid", cid.String()).
		Int("record_size", len(*rec)).
		Msg("Sending update operation")

	// Send the request through the stream manager
	err := h.stream.Send(ctx, req)
	if err != nil {
		h.metrics.IncrementErrorCount("update", err)
		return fmt.Errorf("failed to send update operation: %w", err)
	}

	return nil
}

// HandleDeleteRecord converts a delete operation to a gRPC request and sends it
// This is called by the backfiller when a record is deleted
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

	// Parse the path to extract collection and rkey
	collection, rkey := h.parsePath(path)

	// Create the gRPC request
	req := &pb.OperationRequest{
		Repo:       repo,
		Rev:        rev,
		Collection: collection,
		Rkey:       rkey,
		Time:       time.Now().UTC().Format(time.RFC3339),
		Seq:        seq,
		Operation: &pb.OperationRequest_Delete{
			Delete: &pb.DeleteOperation{
				// PrevCid could be added here if we track it
			},
		},
	}

	// Log the operation (debug level to avoid spam)
	log.Debug().
		Str("repo", repo).
		Str("path", path).
		Str("rev", rev).
		Msg("Sending delete operation")

	// Send the request through the stream manager
	err := h.stream.Send(ctx, req)
	if err != nil {
		h.metrics.IncrementErrorCount("delete", err)
		return fmt.Errorf("failed to send delete operation: %w", err)
	}

	return nil
}

// parsePath splits a record path into collection and rkey
// Path format: "collection/rkey" e.g., "app.bsky.feed.post/3k2yihcrp6f2c"
func (h *Handler) parsePath(path string) (collection string, rkey string) {
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	// If no slash found, treat the whole path as collection
	return path, ""
}

// GetStats returns statistics about the handler's operation
func (h *Handler) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"stream_stats":  h.stream.GetStats(),
		"metrics_stats": h.metrics.GetStats(),
	}
}