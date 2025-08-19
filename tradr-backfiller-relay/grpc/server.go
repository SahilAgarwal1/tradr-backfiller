package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	pb "tradr-backfiller-relay/proto"
	"tradr-backfiller-relay/config"
	"tradr-backfiller-relay/metrics"
)

// Client represents a connected ingester client
type Client struct {
	id       string
	stream   pb.IngesterService_StreamOperationsServer
	active   bool
	mu       sync.Mutex
}

// Server implements the gRPC server for the relay
type Server struct {
	pb.UnimplementedIngesterServiceServer
	
	// clients holds all connected ingester clients
	clients map[string]*Client
	mu      sync.RWMutex
	
	// metrics for tracking operations
	metrics *metrics.Collector
	
	// config
	config config.GRPCConfig
	
	// grpc server instance
	grpcServer *grpc.Server
	
	// client counter for generating IDs
	clientCounter uint64
}

// NewServer creates a new gRPC server
func NewServer(cfg config.GRPCConfig, metrics *metrics.Collector) *Server {
	return &Server{
		clients: make(map[string]*Client),
		metrics: metrics,
		config:  cfg,
	}
}

// Start starts the gRPC server
func (s *Server) Start(ctx context.Context, port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", port, err)
	}
	
	// Configure gRPC server options
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(s.config.MaxMessageSize),
		grpc.MaxSendMsgSize(s.config.MaxMessageSize),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 15 * 60 * 1000 * 1000 * 1000, // 15 minutes in nanoseconds
			MaxConnectionAge:  30 * 60 * 1000 * 1000 * 1000, // 30 minutes in nanoseconds
			Time:              60 * 1000 * 1000 * 1000,       // 60 seconds in nanoseconds
			Timeout:           20 * 1000 * 1000 * 1000,       // 20 seconds in nanoseconds
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * 1000 * 1000 * 1000, // 30 seconds in nanoseconds
			PermitWithoutStream: true,
		}),
	}
	
	s.grpcServer = grpc.NewServer(opts...)
	pb.RegisterIngesterServiceServer(s.grpcServer, s)
	
	log.Info().
		Int("port", port).
		Msg("Starting gRPC server")
	
	// Start server in a goroutine
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			log.Error().Err(err).Msg("gRPC server failed")
		}
	}()
	
	// Wait for context cancellation
	<-ctx.Done()
	
	log.Info().Msg("Shutting down gRPC server")
	s.grpcServer.GracefulStop()
	
	return nil
}

// StreamOperations handles bidirectional streaming with ingester clients
// Server sends OperationRequest, client sends OperationResponse
func (s *Server) StreamOperations(stream pb.IngesterService_StreamOperationsServer) error {
	// Get client info from context
	p, ok := peer.FromContext(stream.Context())
	if !ok {
		return status.Error(codes.Internal, "failed to get peer info")
	}
	
	// Generate client ID
	clientID := fmt.Sprintf("client-%d-%s", atomic.AddUint64(&s.clientCounter, 1), p.Addr.String())
	
	// Create and register client
	client := &Client{
		id:     clientID,
		stream: stream,
		active: true,
	}
	
	s.mu.Lock()
	s.clients[clientID] = client
	clientCount := len(s.clients)
	s.mu.Unlock()
	
	// Update metrics
	s.metrics.SetHealthyIngesters(clientCount)
	s.metrics.SetTotalIngesters(clientCount)
	
	log.Info().
		Str("client_id", clientID).
		Str("address", p.Addr.String()).
		Int("total_clients", clientCount).
		Msg("Ingester client connected")
	
	// Handle client disconnection
	defer func() {
		s.mu.Lock()
		delete(s.clients, clientID)
		remainingClients := len(s.clients)
		s.mu.Unlock()
		
		// Update metrics
		s.metrics.SetHealthyIngesters(remainingClients)
		s.metrics.SetTotalIngesters(remainingClients)
		
		log.Info().
			Str("client_id", clientID).
			Int("remaining_clients", remainingClients).
			Msg("Ingester client disconnected")
	}()
	
	// Start a goroutine to handle responses from this client
	go func() {
		for {
			// Receive ACKs (OperationResponse) from client
			resp, err := stream.Recv()
			if err != nil {
				log.Debug().
					Str("client_id", clientID).
					Err(err).
					Msg("Client disconnected or error receiving")
				return
			}
			
			// Process ACK
			if resp.Success {
				s.metrics.IncrementAcknowledgedCount()
				log.Debug().
					Str("client_id", clientID).
					Int64("seq", resp.Seq).
					Msg("Received ACK from client")
			} else {
				log.Warn().
					Str("client_id", clientID).
					Str("error", resp.Error).
					Int64("seq", resp.Seq).
					Msg("Client reported error processing operation")
			}
		}
	}()
	
	// Keep the stream alive by waiting for context cancellation
	<-stream.Context().Done()
	
	return nil
}

// HealthCheck implements the health check RPC
func (s *Server) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	s.mu.RLock()
	clientCount := len(s.clients)
	s.mu.RUnlock()
	
	return &pb.HealthCheckResponse{
		Healthy: true,
		Status:  fmt.Sprintf("Relay server running with %d connected clients", clientCount),
		Backlog: 0,
	}, nil
}

// Broadcast sends an operation to all connected clients
func (s *Server) Broadcast(req *pb.OperationRequest) error {
	s.mu.RLock()
	clients := make([]*Client, 0, len(s.clients))
	for _, client := range s.clients {
		clients = append(clients, client)
	}
	s.mu.RUnlock()
	
	if len(clients) == 0 {
		return fmt.Errorf("no connected clients")
	}
	
	var successCount int
	var lastErr error
	
	// Send to all clients
	for _, client := range clients {
		client.mu.Lock()
		if !client.active {
			client.mu.Unlock()
			continue
		}
		
		// Server sends OperationRequest to client
		err := client.stream.Send(req)
		if err != nil {
			log.Debug().
				Str("client_id", client.id).
				Err(err).
				Msg("Failed to send to client")
			client.active = false
			lastErr = err
		} else {
			successCount++
			s.metrics.IncrementSentCount()
		}
		client.mu.Unlock()
	}
	
	if successCount == 0 && lastErr != nil {
		return fmt.Errorf("failed to send to any client: %w", lastErr)
	}
	
	return nil
}

// GetConnectedClients returns the number of connected clients
func (s *Server) GetConnectedClients() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clients)
}

// GetStats returns server statistics
func (s *Server) GetStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	clientAddresses := make([]string, 0, len(s.clients))
	for _, client := range s.clients {
		clientAddresses = append(clientAddresses, client.id)
	}
	
	return map[string]interface{}{
		"connected_clients": len(s.clients),
		"client_ids":       clientAddresses,
	}
}