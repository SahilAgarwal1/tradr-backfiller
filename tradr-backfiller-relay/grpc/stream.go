package grpc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	pb "tradr-backfiller-relay/proto"
	"tradr-backfiller-relay/config"
	"tradr-backfiller-relay/metrics"
)

// StreamManager manages gRPC connections and streams to multiple Ingester instances
// It handles load balancing, reconnection, and buffering during disconnections
type StreamManager struct {
	// addresses is the list of Ingester service addresses
	addresses []string
	
	// clients holds the gRPC client connections
	clients []pb.IngesterServiceClient
	
	// connections holds the underlying gRPC connections (for health checking)
	connections []*grpc.ClientConn
	
	// streams holds the active bidirectional streams
	streams []pb.IngesterService_StreamCommitsClient
	
	// mu protects concurrent access to streams and connections
	mu sync.RWMutex
	
	// current is the index for round-robin load balancing
	current uint32
	
	// ctx is the parent context for all operations
	ctx context.Context
	
	// cancel function to stop all operations
	cancel context.CancelFunc
	
	// config holds gRPC configuration
	config config.GRPCConfig
	
	// metrics for tracking operations
	metrics *metrics.Collector
	
	// buffer holds operations during disconnection
	buffer chan *pb.CommitRequest
	
	// healthy tracks which connections are healthy
	healthy []bool
	
	// reconnectChan signals when a reconnection is needed
	reconnectChan chan int
}

// NewStreamManager creates a new stream manager and establishes connections
func NewStreamManager(ctx context.Context, addresses []string, cfg config.GRPCConfig, metrics *metrics.Collector) (*StreamManager, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("no ingester addresses provided")
	}

	// Create a cancellable context
	ctx, cancel := context.WithCancel(ctx)

	sm := &StreamManager{
		addresses:     addresses,
		clients:       make([]pb.IngesterServiceClient, len(addresses)),
		connections:   make([]*grpc.ClientConn, len(addresses)),
		streams:       make([]pb.IngesterService_StreamCommitsClient, len(addresses)),
		healthy:       make([]bool, len(addresses)),
		ctx:          ctx,
		cancel:       cancel,
		config:       cfg,
		metrics:      metrics,
		buffer:       make(chan *pb.CommitRequest, cfg.MaxMessageSize/1024), // Buffer size based on message size
		reconnectChan: make(chan int, len(addresses)),
	}

	// Establish connections to all Ingesters
	for i, addr := range addresses {
		if err := sm.connect(i, addr); err != nil {
			log.Error().
				Err(err).
				Str("address", addr).
				Int("index", i).
				Msg("Failed to connect to Ingester")
			// Continue connecting to other instances
		}
	}

	// Check if we have at least one healthy connection
	hasHealthy := false
	for _, h := range sm.healthy {
		if h {
			hasHealthy = true
			break
		}
	}
	
	if !hasHealthy {
		cancel()
		return nil, fmt.Errorf("failed to establish any healthy connections")
	}

	// Start background goroutines for health checking and reconnection
	go sm.healthCheckLoop()
	go sm.reconnectLoop()
	go sm.bufferProcessor()

	log.Info().
		Int("total", len(addresses)).
		Int("healthy", sm.countHealthy()).
		Msg("Stream manager initialized")

	return sm, nil
}

// connect establishes a connection to a single Ingester
func (sm *StreamManager) connect(index int, address string) error {
	log.Debug().
		Str("address", address).
		Int("index", index).
		Msg("Connecting to Ingester")

	// Configure gRPC dial options
	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(sm.config.MaxMessageSize),
			grpc.MaxCallSendMsgSize(sm.config.MaxMessageSize),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Duration(sm.config.KeepAliveTimeSec) * time.Second,
			Timeout:             time.Duration(sm.config.KeepAliveTimeoutSec) * time.Second,
			PermitWithoutStream: true,
		}),
	}

	// Add TLS configuration if enabled
	if sm.config.EnableTLS {
		// TODO: Load TLS credentials
		log.Warn().Msg("TLS enabled but not implemented yet")
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Establish the connection
	conn, err := grpc.DialContext(sm.ctx, address, dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to dial %s: %w", address, err)
	}

	// Create the client
	client := pb.NewIngesterServiceClient(conn)

	// Test the connection with a health check
	ctx, cancel := context.WithTimeout(sm.ctx, 5*time.Second)
	defer cancel()

	_, err = client.HealthCheck(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		conn.Close()
		return fmt.Errorf("health check failed for %s: %w", address, err)
	}

	// Create a bidirectional stream
	stream, err := client.StreamCommits(sm.ctx)
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to create stream for %s: %w", address, err)
	}

	// Store the connection, client, and stream
	sm.mu.Lock()
	sm.connections[index] = conn
	sm.clients[index] = client
	sm.streams[index] = stream
	sm.healthy[index] = true
	sm.mu.Unlock()

	// Start goroutine to handle responses from this stream
	go sm.handleResponses(index, stream)

	log.Info().
		Str("address", address).
		Int("index", index).
		Msg("Successfully connected to Ingester")

	return nil
}

// Send sends an operation request to one of the connected Ingesters
// It uses round-robin load balancing to distribute requests
func (sm *StreamManager) Send(ctx context.Context, req *pb.CommitRequest) error {
	// Try to send directly first
	if err := sm.sendDirect(ctx, req); err != nil {
		// If direct send fails, buffer the request
		select {
		case sm.buffer <- req:
			log.Debug().Msg("Operation buffered due to connection issues")
			sm.metrics.IncrementBufferedCount()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			return fmt.Errorf("buffer full, dropping operation")
		}
	}
	
	return nil
}

// sendDirect attempts to send a request directly to a healthy stream
func (sm *StreamManager) sendDirect(ctx context.Context, req *pb.CommitRequest) error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Count healthy connections
	healthyCount := sm.countHealthyLocked()
	if healthyCount == 0 {
		return fmt.Errorf("no healthy connections available")
	}

	// Try sending to the next healthy stream (round-robin)
	attempts := 0
	maxAttempts := len(sm.streams)
	
	for attempts < maxAttempts {
		// Get the next index using atomic operation for thread safety
		index := int(atomic.AddUint32(&sm.current, 1) % uint32(len(sm.streams)))
		
		// Check if this connection is healthy
		if !sm.healthy[index] {
			attempts++
			continue
		}

		// Try to send on this stream
		stream := sm.streams[index]
		if stream != nil {
			if err := stream.Send(req); err != nil {
				log.Debug().
					Err(err).
					Int("index", index).
					Msg("Failed to send on stream, marking unhealthy")
				
				// Mark as unhealthy and trigger reconnection
				sm.healthy[index] = false
				select {
				case sm.reconnectChan <- index:
				default:
				}
				
				attempts++
				continue
			}
			
			// Successfully sent
			sm.metrics.IncrementSentCount()
			return nil
		}
		
		attempts++
	}

	return fmt.Errorf("failed to send after %d attempts", attempts)
}

// handleResponses processes responses from an Ingester stream
func (sm *StreamManager) handleResponses(index int, stream pb.IngesterService_StreamCommitsClient) {
	for {
		// Receive response from the stream
		resp, err := stream.Recv()
		if err != nil {
			log.Error().
				Err(err).
				Int("index", index).
				Msg("Error receiving from stream")
			
			// Mark as unhealthy and trigger reconnection
			sm.mu.Lock()
			sm.healthy[index] = false
			sm.mu.Unlock()
			
			select {
			case sm.reconnectChan <- index:
			default:
			}
			
			return
		}

		// Process the response
		if !resp.Success {
			log.Warn().
				Str("error", resp.Error).
				Int("index", index).
				Msg("Operation failed on Ingester")
			sm.metrics.IncrementErrorCount("response", fmt.Errorf(resp.Error))
		} else {
			log.Debug().
				Int64("sequence", resp.GetSequence()).
				Str("processed_at", resp.ProcessedAt).
				Msg("Operation acknowledged")
			sm.metrics.IncrementAcknowledgedCount()
		}
	}
}

// healthCheckLoop periodically checks the health of all connections
func (sm *StreamManager) healthCheckLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			sm.checkHealth()
		}
	}
}

// checkHealth checks the health of all connections
func (sm *StreamManager) checkHealth() {
	sm.mu.RLock()
	connections := make([]*grpc.ClientConn, len(sm.connections))
	copy(connections, sm.connections)
	sm.mu.RUnlock()

	for i, conn := range connections {
		if conn == nil {
			continue
		}

		// Check connection state
		state := conn.GetState()
		healthy := state == connectivity.Ready || state == connectivity.Idle

		sm.mu.Lock()
		oldHealth := sm.healthy[i]
		sm.healthy[i] = healthy
		sm.mu.Unlock()

		// Log state changes
		if oldHealth != healthy {
			log.Info().
				Int("index", i).
				Str("address", sm.addresses[i]).
				Bool("healthy", healthy).
				Str("state", state.String()).
				Msg("Connection health changed")

			// Trigger reconnection if became unhealthy
			if !healthy {
				select {
				case sm.reconnectChan <- i:
				default:
				}
			}
		}
	}
}

// reconnectLoop handles reconnection attempts for failed connections
func (sm *StreamManager) reconnectLoop() {
	reconnectTimer := time.NewTicker(10 * time.Second)
	defer reconnectTimer.Stop()

	pendingReconnects := make(map[int]bool)

	for {
		select {
		case <-sm.ctx.Done():
			return
		
		case index := <-sm.reconnectChan:
			pendingReconnects[index] = true
		
		case <-reconnectTimer.C:
			// Attempt to reconnect all pending connections
			for index := range pendingReconnects {
				go func(i int) {
					log.Info().
						Int("index", i).
						Str("address", sm.addresses[i]).
						Msg("Attempting reconnection")
					
					// Close old connection if exists
					sm.mu.Lock()
					if sm.connections[i] != nil {
						sm.connections[i].Close()
					}
					sm.mu.Unlock()
					
					// Attempt reconnection
					if err := sm.connect(i, sm.addresses[i]); err != nil {
						log.Error().
							Err(err).
							Int("index", i).
							Msg("Reconnection failed")
						// Will retry on next tick
					} else {
						delete(pendingReconnects, i)
					}
				}(index)
			}
		}
	}
}

// bufferProcessor processes buffered operations when connections are restored
func (sm *StreamManager) bufferProcessor() {
	for {
		select {
		case <-sm.ctx.Done():
			return
		
		case req := <-sm.buffer:
			// Keep trying to send buffered request
			for {
				if err := sm.sendDirect(sm.ctx, req); err != nil {
					// Wait a bit before retrying
					time.Sleep(100 * time.Millisecond)
					
					// Check if we should give up
					select {
					case <-sm.ctx.Done():
						return
					default:
						continue
					}
				} else {
					sm.metrics.DecrementBufferedCount()
					break
				}
			}
		}
	}
}

// countHealthy returns the number of healthy connections
func (sm *StreamManager) countHealthy() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.countHealthyLocked()
}

// countHealthyLocked counts healthy connections (must be called with lock held)
func (sm *StreamManager) countHealthyLocked() int {
	count := 0
	for _, h := range sm.healthy {
		if h {
			count++
		}
	}
	return count
}

// GetStats returns statistics about the stream manager
func (sm *StreamManager) GetStats() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	stats := map[string]interface{}{
		"total_connections": len(sm.connections),
		"healthy_connections": sm.countHealthyLocked(),
		"buffer_size": len(sm.buffer),
		"buffer_capacity": cap(sm.buffer),
	}

	// Add per-connection stats
	connectionStats := make([]map[string]interface{}, len(sm.connections))
	for i, conn := range sm.connections {
		connStat := map[string]interface{}{
			"address": sm.addresses[i],
			"healthy": sm.healthy[i],
		}
		if conn != nil {
			connStat["state"] = conn.GetState().String()
		}
		connectionStats[i] = connStat
	}
	stats["connections"] = connectionStats

	return stats
}

// Close closes all connections and stops background goroutines
func (sm *StreamManager) Close() error {
	log.Info().Msg("Closing stream manager")
	
	// Cancel context to stop background goroutines
	sm.cancel()
	
	// Close all connections
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	for i, conn := range sm.connections {
		if conn != nil {
			if err := conn.Close(); err != nil {
				log.Error().
					Err(err).
					Int("index", i).
					Msg("Error closing connection")
			}
		}
	}
	
	// Close buffer channel
	close(sm.buffer)
	
	return nil
}