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
	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog/log"
	"tradr-backfiller-relay/config"
)

// Collector collects and exposes metrics for Prometheus
type Collector struct {
	// Counters for operations
	operationsTotal   *prometheus.CounterVec   // Total operations by type
	operationsSent    prometheus.Counter       // Successfully sent to Ingester
	operationsBuffered prometheus.Counter      // Buffered due to connection issues
	operationsDropped prometheus.Counter       // Dropped due to buffer overflow
	operationsAcked   prometheus.Counter       // Acknowledged by Ingester
	
	// Histograms for latencies
	operationLatency *prometheus.HistogramVec  // Latency by operation type
	
	// Gauges for current state
	bufferedCount    prometheus.Gauge          // Current buffered operations
	healthyIngesters prometheus.Gauge          // Number of healthy Ingester connections
	
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
		
		operationsBuffered: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "relay_operations_buffered_total",
				Help: "Total number of operations buffered",
			},
		),
		
		operationsDropped: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "relay_operations_dropped_total",
				Help: "Total number of operations dropped due to buffer overflow",
			},
		),
		
		operationsAcked: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "relay_operations_acknowledged_total",
				Help: "Total number of operations acknowledged by Ingesters",
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
		
		// Define gauges
		bufferedCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "relay_buffered_operations",
				Help: "Current number of buffered operations",
			},
		),
		
		healthyIngesters: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "relay_healthy_ingesters",
				Help: "Number of healthy Ingester connections",
			},
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
		c.operationsBuffered,
		c.operationsDropped,
		c.operationsAcked,
		c.operationLatency,
		c.bufferedCount,
		c.healthyIngesters,
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

// IncrementBufferedCount increments the buffered counter
func (c *Collector) IncrementBufferedCount() {
	c.operationsBuffered.Inc()
	c.bufferedCount.Inc()
}

// DecrementBufferedCount decrements the buffered counter
func (c *Collector) DecrementBufferedCount() {
	c.bufferedCount.Dec()
}

// IncrementDroppedCount increments the dropped counter
func (c *Collector) IncrementDroppedCount() {
	c.operationsDropped.Inc()
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
		case isBufferError(err):
			errorType = "buffer"
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
	
	// Get current gauge values using dto.Metric
	bufferedMetric := &dto.Metric{}
	c.bufferedCount.Write(bufferedMetric)
	
	healthyMetric := &dto.Metric{}
	c.healthyIngesters.Write(healthyMetric)
	
	return map[string]interface{}{
		"error_rate":           errorRate,
		"buffered_operations":  int64(bufferedMetric.GetGauge().GetValue()),
		"healthy_ingesters":    int64(healthyMetric.GetGauge().GetValue()),
		"buffer_usage":        0.0, // This would be calculated based on buffer capacity
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

func isBufferError(err error) bool {
	// Check if error is related to buffering
	return err != nil && contains(err.Error(), []string{"buffer", "overflow", "full"})
}

func contains(s string, substrs []string) bool {
	for _, substr := range substrs {
		if len(s) >= len(substr) && s[:len(substr)] == substr {
			return true
		}
	}
	return false
}

// prometheusMetricDTO is a helper struct for reading Prometheus metric values
type prometheusMetricDTO struct {
	gauge *prometheusGaugeDTO
}

func (m *prometheusMetricDTO) GetGauge() *prometheusGaugeDTO {
	if m.gauge == nil {
		m.gauge = &prometheusGaugeDTO{}
	}
	return m.gauge
}

func (m *prometheusMetricDTO) Write(pb *prometheusMetricDTO) error {
	// This is a simplified implementation
	// Real implementation would properly serialize the metric
	return nil
}

type prometheusGaugeDTO struct {
	value float64
}

func (g *prometheusGaugeDTO) GetValue() float64 {
	return g.value
}