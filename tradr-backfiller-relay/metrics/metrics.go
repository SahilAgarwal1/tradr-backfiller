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
	operationsTotal   *prometheus.CounterVec   // Total operations by type and source
	operationsSent    prometheus.Counter       // Successfully sent to Ingester
	operationsAcked   prometheus.Counter       // Acknowledged by Ingester
	bytesProcessed    *prometheus.CounterVec   // Total bytes processed by source
	
	// Histograms for latencies
	operationLatency *prometheus.HistogramVec  // Latency by operation type
	
	// Gauges for current state
	healthyIngesters    prometheus.Gauge       // Number of healthy Ingester connections
	totalIngesters      prometheus.Gauge       // Total number of configured Ingesters
	grpcConnectionState *prometheus.GaugeVec   // State of each gRPC connection (0=down, 1=up)
	
	// Backfill metrics
	backfillJobsActive  prometheus.Gauge         // Number of active backfill jobs
	backfillJobsPending prometheus.Gauge         // Number of pending backfill jobs
	backfillJobsFailed  prometheus.Gauge         // Number of failed backfill jobs
	backfillRepoSize    *prometheus.GaugeVec     // Size of repos being backfilled
	
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
				Help: "Total number of operations processed by type and source",
			},
			[]string{"operation", "source", "service"},
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
		
		bytesProcessed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "relay_bytes_processed_total",
				Help: "Total bytes processed by source",
			},
			[]string{"source"},
		),
		
		// Define latency histogram
		operationLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "relay_operation_duration_seconds",
				Help:    "Operation processing duration in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation", "source", "service"},
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
		
		// Define backfill metrics
		backfillJobsActive: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "relay_backfill_jobs_active",
				Help: "Number of backfill jobs currently being processed",
			},
		),
		
		backfillJobsPending: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "relay_backfill_jobs_pending",
				Help: "Number of backfill jobs waiting to be processed",
			},
		),
		
		backfillJobsFailed: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "relay_backfill_jobs_failed",
				Help: "Number of backfill jobs that have failed",
			},
		),
		
		backfillRepoSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "relay_backfill_repo_size",
				Help: "Size of repositories being backfilled (in bytes)",
			},
			[]string{"repo"},
		),
		
		// Define error counter
		errorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "relay_errors_total",
				Help: "Total number of errors by type",
			},
			[]string{"operation", "error_type", "source", "service"},
		),
	}
	
	// Register all metrics with Prometheus
	prometheus.MustRegister(
		c.operationsTotal,
		c.operationsSent,
		c.operationsAcked,
		c.bytesProcessed,
		c.operationLatency,
		c.healthyIngesters,
		c.totalIngesters,
		c.grpcConnectionState,
		c.backfillJobsActive,
		c.backfillJobsPending,
		c.backfillJobsFailed,
		c.backfillRepoSize,
		c.errorsTotal,
	)
	
	// Start background goroutine for periodic calculations
	go c.calculateRates()
	
	return c
}

// IncrementOperationCount increments the counter for a specific operation type and source
func (c *Collector) IncrementOperationCount(operation string, source string) {
	c.operationsTotal.WithLabelValues(operation, source, c.serviceName).Inc()
}

// RecordOperationLatency records the latency for an operation
func (c *Collector) RecordOperationLatency(operation string, source string, duration time.Duration) {
	c.operationLatency.WithLabelValues(operation, source, c.serviceName).Observe(duration.Seconds())
}

// AddBytesProcessed adds to the bytes processed counter
func (c *Collector) AddBytesProcessed(source string, bytes int) {
	c.bytesProcessed.WithLabelValues(source).Add(float64(bytes))
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
func (c *Collector) IncrementErrorCount(operation string, source string, err error) {
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
	
	c.errorsTotal.WithLabelValues(operation, errorType, source, c.serviceName).Inc()
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

// SetBackfillJobsActive sets the number of active backfill jobs
func (c *Collector) SetBackfillJobsActive(count int) {
	c.backfillJobsActive.Set(float64(count))
}

// SetBackfillJobsPending sets the number of pending backfill jobs
func (c *Collector) SetBackfillJobsPending(count int) {
	c.backfillJobsPending.Set(float64(count))
}

// SetBackfillJobsFailed sets the number of failed backfill jobs
func (c *Collector) SetBackfillJobsFailed(count int) {
	c.backfillJobsFailed.Set(float64(count))
}

// SetBackfillRepoSize sets the size of a repo being backfilled
func (c *Collector) SetBackfillRepoSize(repo string, size float64) {
	c.backfillRepoSize.WithLabelValues(repo).Set(size)
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