package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	pb "tradr-backfiller-relay/proto"
)

// MockIngester implements the IngesterService for testing
type MockIngester struct {
	pb.UnimplementedIngesterServiceServer
	
	// Counters for tracking operations
	createCount int64
	updateCount int64
	deleteCount int64
	errorCount  int64
	
	// Options
	verbose     bool
	failRate    float32 // Percentage of operations to fail (for testing error handling)
	delayMs     int     // Artificial delay in milliseconds
}

// StreamOperations handles the bidirectional stream - ULTRA MINIMAL FOR MAX THROUGHPUT
func (m *MockIngester) StreamOperations(stream pb.IngesterService_StreamOperationsServer) error {
	log.Println("📡 New stream connection established")
	
	// Start a goroutine to send periodic ACKs to keep connection alive
	// But don't ACK every message - just periodically to prevent timeout
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		
		for range ticker.C {
			// Send a heartbeat response
			resp := &pb.OperationResponse{
				Success: true,
				Seq:     0, // Special seq for heartbeat
			}
			if err := stream.Send(resp); err != nil {
				return
			}
		}
	}()
	
	// Just receive and count - no responses per operation
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			log.Println("⚪ Stream closed by client")
			return nil
		}
		if err != nil {
			log.Printf("❌ Error receiving: %v", err)
			return err
		}
		
		// Only count for statistics
		switch req.Operation.(type) {
		case *pb.OperationRequest_Create:
			atomic.AddInt64(&m.createCount, 1)
		case *pb.OperationRequest_Update:
			atomic.AddInt64(&m.updateCount, 1)
		case *pb.OperationRequest_Delete:
			atomic.AddInt64(&m.deleteCount, 1)
		}
		
		// NO RESPONSE - just continue to next message
	}
}

// HealthCheck implements the health check RPC
func (m *MockIngester) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{
		Healthy: true,
		Status:  "Mock ingester is running",
		Backlog: 0,
	}, nil
}

// PrintStats prints statistics every N seconds
func (m *MockIngester) PrintStats(interval time.Duration) {
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
		
		log.Printf("📊 Stats: Creates=%d (%.1f/s) Updates=%d (%.1f/s) Deletes=%d (%.1f/s) Errors=%d",
			currentCreate, createRate,
			currentUpdate, updateRate,
			currentDelete, deleteRate,
			currentError)
		
		// Also print as JSON for easier parsing
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
			},
		}
		
		if jsonBytes, err := json.Marshal(stats); err == nil {
			log.Printf("📈 JSON: %s", string(jsonBytes))
		}
		
		lastCreate = currentCreate
		lastUpdate = currentUpdate
		lastDelete = currentDelete
	}
}

func main() {
	var (
		port     = flag.Int("port", 50052, "Port to listen on")
		verbose  = flag.Bool("verbose", false, "Enable verbose logging")
		failRate = flag.Float64("fail-rate", 0, "Percentage of operations to fail (0-1)")
		delayMs  = flag.Int("delay", 0, "Artificial delay in milliseconds")
		statsInterval = flag.Int("stats", 10, "Statistics interval in seconds")
	)
	flag.Parse()
	
	log.Printf("🚀 Starting mock ingester on port %d", *port)
	log.Printf("⚙️  Options: verbose=%v fail-rate=%.2f delay=%dms stats=%ds",
		*verbose, *failRate, *delayMs, *statsInterval)
	
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(10 * 1024 * 1024), // 10MB
		grpc.MaxSendMsgSize(10 * 1024 * 1024), // 10MB
	)
	
	ingester := &MockIngester{
		verbose:  *verbose,
		failRate: float32(*failRate),
		delayMs:  *delayMs,
	}
	
	pb.RegisterIngesterServiceServer(s, ingester)
	
	// Start statistics printer
	if *statsInterval > 0 {
		go ingester.PrintStats(time.Duration(*statsInterval) * time.Second)
	}
	
	log.Printf("✅ Mock ingester ready and listening on port %d", *port)
	
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}