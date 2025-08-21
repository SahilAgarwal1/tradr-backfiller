package metrics

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"tradr-backfiller-relay/config"
)

// Collector collects and exposes metrics for Prometheus
type Collector struct {
	// Counters for operations
	operationsTotal   *prometheus.CounterVec   // Total operations by type
	operationsSent    prometheus.Counter       // Successfully sent to Ingester
	operationsAcked   prometheus.Counter       // Acknowledged by Ingester
	
	
	// gRPC message tracking
	grpcMessagesSent  prometheus.Counter       // Total gRPC messages sent
	grpcBytesOut      prometheus.Counter       // Total bytes sent via gRPC
	grpcBytesIn       prometheus.Counter       // Total bytes received via gRPC
	
	// Network I/O tracking
	networkBytesIn    prometheus.Counter       // Total network bytes received
	networkBytesOut   prometheus.Counter       // Total network bytes sent
	
	// Histograms for latencies
	operationLatency *prometheus.HistogramVec  // Latency by operation type
	repoProcessTime  prometheus.Histogram      // Time to process a full repo
	
	// Gauges for current state
	healthyIngesters    prometheus.Gauge       // Number of healthy Ingester connections
	totalIngesters      prometheus.Gauge       // Total number of configured Ingesters
	grpcConnectionState *prometheus.GaugeVec   // State of each gRPC connection (0=down, 1=up)
	
	
	// Error tracking
	errorsTotal     *prometheus.CounterVec     // Errors by type and operation
	
	// Internal counters for rate calculation
	sentCount     uint64
	errorCount    uint64
	lastRateCalc  time.Time
	errorRate     float64
	mu            sync.RWMutex
	
	// Service name for labels
	serviceName string
}

// New creates a new metrics collector
func New(serviceName string) *Collector {
	c := &Collector{
		serviceName:  serviceName,
		lastRateCalc: time.Now(),
		
		// Define operation counters
		operationsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "relay_operations_total",
				Help: "Total number of operations processed by type",
			},
			[]string{"operation", "service"},
		),
		
		operationsSent: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "relay_operations_sent_total",
				Help: "Total number of operations sent to Ingesters",
			},
		),
		
		operationsAcked: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "relay_operations_acknowledged_total",
				Help: "Total number of operations acknowledged by Ingesters",
			},
		),
		
		
		// gRPC message tracking
		grpcMessagesSent: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "relay_grpc_messages_sent_total",
				Help: "Total number of gRPC messages sent to ingesters",
			},
		),
		
		grpcBytesOut: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "relay_grpc_bytes_sent_total",
				Help: "Total bytes sent via gRPC",
			},
		),
		
		grpcBytesIn: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "relay_grpc_bytes_received_total",
				Help: "Total bytes received via gRPC",
			},
		),
		
		// Network I/O tracking
		networkBytesIn: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "relay_network_bytes_in_total",
				Help: "Total network bytes received (firehose + backfill)",
			},
		),
		
		networkBytesOut: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "relay_network_bytes_out_total",
				Help: "Total network bytes sent",
			},
		),
		
		// Define latency histogram
		operationLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "relay_operation_duration_seconds",
				Help:    "Operation processing duration in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation", "service"},
		),
		
		repoProcessTime: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "relay_repo_process_duration_seconds",
				Help:    "Time to fully process a repository",
				Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600},
			},
		),
		
		// Define gauges for gRPC connections
		healthyIngesters: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "relay_healthy_ingesters",
				Help: "Number of healthy Ingester connections",
			},
		),
		
		totalIngesters: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "relay_total_ingesters",
				Help: "Total number of configured Ingester connections",
			},
		),
		
		grpcConnectionState: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "relay_grpc_connection_state",
				Help: "State of each gRPC connection (0=disconnected, 1=connected, 2=connecting)",
			},
			[]string{"address", "index"},
		),
		
		
		// Define error counter
		errorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "relay_errors_total",
				Help: "Total number of errors by type",
			},
			[]string{"operation", "error_type", "service"},
		),
	}
	
	// Register all metrics with Prometheus
	prometheus.MustRegister(
		c.operationsTotal,
		c.operationsSent,
		c.operationsAcked,
		c.grpcMessagesSent,
		c.grpcBytesOut,
		c.grpcBytesIn,
		c.networkBytesIn,
		c.networkBytesOut,
		c.operationLatency,
		c.repoProcessTime,
		c.healthyIngesters,
		c.totalIngesters,
		c.grpcConnectionState,
		c.errorsTotal,
	)
	
	// Start background goroutine for periodic calculations
	go c.calculateRates()
	
	return c
}

// IncrementOperationCount increments the counter for a specific operation type
func (c *Collector) IncrementOperationCount(operation string) {
	c.operationsTotal.WithLabelValues(operation, c.serviceName).Inc()
}

// RecordOperationLatency records the latency for an operation
func (c *Collector) RecordOperationLatency(operation string, duration time.Duration) {
	c.operationLatency.WithLabelValues(operation, c.serviceName).Observe(duration.Seconds())
}

// IncrementSentCount increments the sent counter
func (c *Collector) IncrementSentCount() {
	c.operationsSent.Inc()
	atomic.AddUint64(&c.sentCount, 1)
}

// IncrementAcknowledgedCount increments the acknowledged counter
func (c *Collector) IncrementAcknowledgedCount() {
	c.operationsAcked.Inc()
}

// IncrementErrorCount increments the error counter
func (c *Collector) IncrementErrorCount(operation string, err error) {
	errorType := "unknown"
	if err != nil {
		// Categorize error types
		switch {
		case isConnectionError(err):
			errorType = "connection"
		case isTimeoutError(err):
			errorType = "timeout"
		default:
			errorType = "processing"
		}
	}
	
	c.errorsTotal.WithLabelValues(operation, errorType, c.serviceName).Inc()
	atomic.AddUint64(&c.errorCount, 1)
}

// SetHealthyIngesters sets the number of healthy Ingester connections
func (c *Collector) SetHealthyIngesters(count int) {
	c.healthyIngesters.Set(float64(count))
}

// SetTotalIngesters sets the total number of configured Ingester connections
func (c *Collector) SetTotalIngesters(count int) {
	c.totalIngesters.Set(float64(count))
}

// UpdateConnectionState updates the state of a specific gRPC connection
// state: 0=disconnected, 1=connected, 2=connecting
func (c *Collector) UpdateConnectionState(address string, index int, state float64) {
	c.grpcConnectionState.WithLabelValues(address, fmt.Sprintf("%d", index)).Set(state)
}


// RecordRepoProcessTime records the time it took to process a repo
func (c *Collector) RecordRepoProcessTime(duration time.Duration) {
	c.repoProcessTime.Observe(duration.Seconds())
}

// IncrementGRPCMessagesSent increments the gRPC messages sent counter
func (c *Collector) IncrementGRPCMessagesSent() {
	c.grpcMessagesSent.Inc()
}

// AddGRPCBytesOut adds to the gRPC bytes sent counter
func (c *Collector) AddGRPCBytesOut(bytes int) {
	c.grpcBytesOut.Add(float64(bytes))
}

// AddGRPCBytesIn adds to the gRPC bytes received counter
func (c *Collector) AddGRPCBytesIn(bytes int) {
	c.grpcBytesIn.Add(float64(bytes))
}

// AddNetworkBytesIn adds to the network bytes received counter
func (c *Collector) AddNetworkBytesIn(bytes int) {
	c.networkBytesIn.Add(float64(bytes))
}

// AddNetworkBytesOut adds to the network bytes sent counter
func (c *Collector) AddNetworkBytesOut(bytes int) {
	c.networkBytesOut.Add(float64(bytes))
}

// Removed buffer-related methods:
// - IncrementBufferedCount
// - DecrementBufferedCount  
// - IncrementDroppedCount

// calculateRates periodically calculates error rates and other derived metrics
func (c *Collector) calculateRates() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		now := time.Now()
		duration := now.Sub(c.lastRateCalc).Seconds()
		
		if duration > 0 {
			sent := atomic.LoadUint64(&c.sentCount)
			errors := atomic.LoadUint64(&c.errorCount)
			
			total := sent + errors
			if total > 0 {
				c.mu.Lock()
				c.errorRate = float64(errors) / float64(total)
				c.lastRateCalc = now
				c.mu.Unlock()
				
				// Reset counters for next period
				atomic.StoreUint64(&c.sentCount, 0)
				atomic.StoreUint64(&c.errorCount, 0)
			}
		}
	}
}

// GetStats returns current statistics
func (c *Collector) GetStats() map[string]interface{} {
	c.mu.RLock()
	errorRate := c.errorRate
	c.mu.RUnlock()
	
	return map[string]interface{}{
		"error_rate": errorRate,
	}
}

// StartServer starts the HTTP server for Prometheus metrics endpoint
func StartServer(ctx context.Context, cfg config.MetricsConfig) error {
	addr := fmt.Sprintf(":%d", cfg.Port)
	
	// Create HTTP mux
	mux := http.NewServeMux()
	
	// Add Prometheus metrics handler
	mux.Handle(cfg.Path, promhttp.Handler())
	
	// Add health endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	
	// Create HTTP server
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	
	// Start server in goroutine
	go func() {
		log.Info().
			Str("address", addr).
			Str("path", cfg.Path).
			Msg("Metrics server started")
		
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().
				Err(err).
				Msg("Metrics server error")
		}
	}()
	
	// Wait for context cancellation
	<-ctx.Done()
	
	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	log.Info().Msg("Shutting down metrics server")
	return server.Shutdown(shutdownCtx)
}

// Helper functions for error categorization

func isConnectionError(err error) bool {
	// Check if error is related to connection issues
	// This is simplified - real implementation would check specific error types
	return err != nil && contains(err.Error(), []string{"connection", "dial", "connect"})
}

func isTimeoutError(err error) bool {
	// Check if error is a timeout
	return err != nil && contains(err.Error(), []string{"timeout", "deadline"})
}

func contains(s string, substrs []string) bool {
	for _, substr := range substrs {
		if len(s) >= len(substr) && s[:len(substr)] == substr {
			return true
		}
	}
	return false
}