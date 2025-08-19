package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	pb "tradr-backfiller-relay/proto"
)

// MockIngesterClient connects to the relay server and receives operations
type MockIngesterClient struct {
	// Counters for tracking operations
	createCount int64
	updateCount int64
	deleteCount int64
	errorCount  int64
	
	// Options
	verbose     bool
	ackEvery    int  // Send ACK every N operations (0 = never ack)
	relayAddr   string
}

// Run connects to the relay and starts receiving operations
func (m *MockIngesterClient) Run(ctx context.Context) error {
	// Connect to relay server
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}
	
	conn, err := grpc.Dial(m.relayAddr, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to relay: %w", err)
	}
	defer conn.Close()
	
	// Create client
	client := pb.NewIngesterServiceClient(conn)
	
	// Check health first
	healthResp, err := client.HealthCheck(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	log.Printf("✅ Connected to relay: %s", healthResp.Status)
	
	// Start streaming
	stream, err := client.StreamOperations(ctx)
	if err != nil {
		return fmt.Errorf("failed to start stream: %w", err)
	}
	
	log.Println("📡 Stream established, waiting for operations...")
	
	// Start goroutine for receiving operations
	go m.receiveOperations(stream)
	
	// Keep connection alive until context is cancelled
	<-ctx.Done()
	return stream.CloseSend()
}

// receiveOperations handles incoming operations from the relay
func (m *MockIngesterClient) receiveOperations(stream pb.IngesterService_StreamOperationsClient) {
	operationCount := int64(0)
	
	for {
		// Receive operation from relay (server sends OperationRequest)
		req, err := stream.Recv()
		if err == io.EOF {
			log.Println("⚪ Stream closed by server")
			return
		}
		if err != nil {
			log.Printf("❌ Error receiving: %v", err)
			atomic.AddInt64(&m.errorCount, 1)
			return
		}
		
		operationCount++
		
		// Count by operation type
		switch req.Operation.(type) {
		case *pb.OperationRequest_Create:
			atomic.AddInt64(&m.createCount, 1)
			if m.verbose {
				log.Printf("➕ CREATE: %s/%s/%s", req.Repo, req.Collection, req.Rkey)
			}
		case *pb.OperationRequest_Update:
			atomic.AddInt64(&m.updateCount, 1)
			if m.verbose {
				log.Printf("✏️ UPDATE: %s/%s/%s", req.Repo, req.Collection, req.Rkey)
			}
		case *pb.OperationRequest_Delete:
			atomic.AddInt64(&m.deleteCount, 1)
			if m.verbose {
				log.Printf("🗑️ DELETE: %s/%s/%s", req.Repo, req.Collection, req.Rkey)
			}
		}
		
		// Send ACK if configured
		if m.ackEvery > 0 && operationCount%int64(m.ackEvery) == 0 {
			ack := &pb.OperationResponse{
				Success: true,
				Seq:     req.Seq,
			}
			if err := stream.Send(ack); err != nil {
				log.Printf("⚠️ Failed to send ACK: %v", err)
			} else if m.verbose {
				log.Printf("✓ ACK sent for seq %d", req.Seq)
			}
		}
	}
}

// PrintStats prints statistics every N seconds
func (m *MockIngesterClient) PrintStats(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	
	var lastCreate, lastUpdate, lastDelete int64
	
	for range ticker.C {
		currentCreate := atomic.LoadInt64(&m.createCount)
		currentUpdate := atomic.LoadInt64(&m.updateCount)
		currentDelete := atomic.LoadInt64(&m.deleteCount)
		currentError := atomic.LoadInt64(&m.errorCount)
		
		createRate := float64(currentCreate-lastCreate) / interval.Seconds()
		updateRate := float64(currentUpdate-lastUpdate) / interval.Seconds()
		deleteRate := float64(currentDelete-lastDelete) / interval.Seconds()
		
		totalRate := createRate + updateRate + deleteRate
		
		log.Printf("📊 Stats: Creates=%d (%.1f/s) Updates=%d (%.1f/s) Deletes=%d (%.1f/s) Errors=%d",
			currentCreate, createRate,
			currentUpdate, updateRate,
			currentDelete, deleteRate,
			currentError)
		
		// Also output JSON for monitoring
		stats := map[string]interface{}{
			"timestamp": time.Now().Unix(),
			"totals": map[string]int64{
				"creates": currentCreate,
				"updates": currentUpdate,
				"deletes": currentDelete,
				"errors":  currentError,
			},
			"rates": map[string]float64{
				"creates_per_sec": createRate,
				"updates_per_sec": updateRate,
				"deletes_per_sec": deleteRate,
				"total_per_sec":   totalRate,
			},
		}
		
		jsonBytes, _ := json.Marshal(stats)
		log.Printf("📈 JSON: %s", string(jsonBytes))
		
		lastCreate = currentCreate
		lastUpdate = currentUpdate
		lastDelete = currentDelete
	}
}

func main() {
	var (
		relayAddr = flag.String("relay", "localhost:50051", "Relay server address")
		stats     = flag.Int("stats", 5, "Print statistics every N seconds (0 to disable)")
		verbose   = flag.Bool("verbose", false, "Enable verbose logging")
		ackEvery  = flag.Int("ack", 100, "Send ACK every N operations (0 = never)")
	)
	flag.Parse()
	
	client := &MockIngesterClient{
		relayAddr: *relayAddr,
		verbose:   *verbose,
		ackEvery:  *ackEvery,
	}
	
	ctx := context.Background()
	
	// Start stats printer if enabled
	if *stats > 0 {
		go client.PrintStats(time.Duration(*stats) * time.Second)
	}
	
	log.Printf("🚀 Mock Ingester Client starting...")
	log.Printf("📍 Connecting to relay at: %s", *relayAddr)
	
	if err := client.Run(ctx); err != nil {
		log.Fatalf("❌ Client failed: %v", err)
	}
}