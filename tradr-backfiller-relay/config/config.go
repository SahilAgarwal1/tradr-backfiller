package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the relay service
type Config struct {
	// Relay configuration for the service itself
	Relay RelayConfig `yaml:"relay"`
	
	// Firehose configuration for subscribing to the AT Protocol firehose
	Firehose FirehoseConfig `yaml:"firehose"`
	
	// Backfiller configuration for processing repos and buffering
	Backfiller BackfillerConfig `yaml:"backfiller"`
	
	// GRPC configuration for server
	GRPC GRPCConfig `yaml:"grpc"`
	
	// Database configuration for state storage
	Database DatabaseConfig `yaml:"database"`
	
	// Metrics configuration for Prometheus endpoint
	Metrics MetricsConfig `yaml:"metrics"`
	
	// LogLevel for the application (debug, info, warn, error)
	LogLevel string `yaml:"log_level"`
}

// RelayConfig contains settings for the relay service itself
type RelayConfig struct {
	// Name is the identifier for this relay instance
	Name string `yaml:"name"`
	
	// MaxRetries for failed operations before giving up
	MaxRetries int `yaml:"max_retries"`
	
	// RetryBackoffMs is the initial backoff in milliseconds for retries
	RetryBackoffMs int `yaml:"retry_backoff_ms"`
}

// FirehoseConfig contains settings for the firehose subscription
type FirehoseConfig struct {
	// RelayHost is the WebSocket URL of the AT Protocol relay
	// e.g., "wss://bsky.network" or "wss://bsky.social"
	RelayHost string `yaml:"relay_host"`
	
	// NSIDFilter limits which record types to process
	// e.g., "app.bsky." to only process Bluesky records
	NSIDFilter string `yaml:"nsid_filter"`
	
	// SequencerURL is the optional sequencer service for cursor management
	SequencerURL string `yaml:"sequencer_url"`
	
	// MaxReconnectAttempts before giving up (-1 for infinite)
	MaxReconnectAttempts int `yaml:"max_reconnect_attempts"`
	
	// ReconnectDelayMs is the delay between reconnection attempts
	ReconnectDelayMs int `yaml:"reconnect_delay_ms"`
}

// BackfillerConfig contains settings for the backfill processor
type BackfillerConfig struct {
	// ParallelBackfills is the number of concurrent repo backfills
	ParallelBackfills int `yaml:"parallel_backfills"`
	
	// ParallelRecordCreates is the number of concurrent record operations per backfill
	ParallelRecordCreates int `yaml:"parallel_record_creates"`
	
	// NSIDFilter for backfill operations (usually same as firehose)
	NSIDFilter string `yaml:"nsid_filter"`
	
	// CheckpointInterval is how often to checkpoint progress (in operations)
	CheckpointInterval int `yaml:"checkpoint_interval"`
	
	// BufferSize is the maximum number of operations to buffer during backfill
	BufferSize int `yaml:"buffer_size"`
	
	// ProcessIntervalMs is how often to check for new backfill jobs
	ProcessIntervalMs int `yaml:"process_interval_ms"`
}

// GRPCConfig contains settings for gRPC server and client
type GRPCConfig struct {
	// Port for the management gRPC server
	Port int `yaml:"port"`
	
	// MaxMessageSize is the maximum message size in bytes (default 10MB)
	MaxMessageSize int `yaml:"max_message_size"`
	
	// MaxConcurrentStreams limits concurrent streams per connection
	MaxConcurrentStreams uint32 `yaml:"max_concurrent_streams"`
	
	// KeepAliveTimeSec for connection health checks
	KeepAliveTimeSec int `yaml:"keepalive_time_sec"`
	
	// KeepAliveTimeoutSec for keepalive response timeout
	KeepAliveTimeoutSec int `yaml:"keepalive_timeout_sec"`
	
	// EnableTLS for secure connections
	EnableTLS bool `yaml:"enable_tls"`
	
	// TLSCertFile path (if TLS enabled)
	TLSCertFile string `yaml:"tls_cert_file"`
	
	// TLSKeyFile path (if TLS enabled)
	TLSKeyFile string `yaml:"tls_key_file"`
}

// DatabaseConfig contains database connection settings
type DatabaseConfig struct {
	// DSN is the database connection string
	// e.g., "postgres://user:pass@localhost:5432/relay?sslmode=disable"
	DSN string `yaml:"dsn"`
	
	// MaxOpenConns is the maximum number of open connections
	MaxOpenConns int `yaml:"max_open_conns"`
	
	// MaxIdleConns is the maximum number of idle connections
	MaxIdleConns int `yaml:"max_idle_conns"`
	
	// ConnMaxLifetimeMin is the maximum connection lifetime in minutes
	ConnMaxLifetimeMin int `yaml:"conn_max_lifetime_min"`
}

// MetricsConfig contains settings for the Prometheus metrics endpoint
type MetricsConfig struct {
	// Port for the HTTP metrics server
	Port int `yaml:"port"`
	
	// Path for the metrics endpoint (default "/metrics")
	Path string `yaml:"path"`
	
	// EnableRuntimeMetrics includes Go runtime metrics
	EnableRuntimeMetrics bool `yaml:"enable_runtime_metrics"`
}

// Load reads configuration from file and environment variables
func Load() (*Config, error) {
	// Determine config file path from environment or use default
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config/config.yaml"
	}

	// Create default configuration
	cfg := &Config{
		Relay: RelayConfig{
			Name:           "tradr-relay",
			MaxRetries:     3,
			RetryBackoffMs: 1000,
		},
		Firehose: FirehoseConfig{
			RelayHost:            "wss://bsky.network",
			NSIDFilter:           "app.bsky.",
			MaxReconnectAttempts: -1,
			ReconnectDelayMs:     5000,
		},
		Backfiller: BackfillerConfig{
			ParallelBackfills:     10,
			ParallelRecordCreates: 100,
			NSIDFilter:           "app.bsky.",
			CheckpointInterval:    1000,
			BufferSize:           10000,
			ProcessIntervalMs:     1000,
		},
		GRPC: GRPCConfig{
			Port:                 50051,
			MaxMessageSize:       10 * 1024 * 1024, // 10MB
			MaxConcurrentStreams: 100,
			KeepAliveTimeSec:     30,
			KeepAliveTimeoutSec:  10,
			EnableTLS:           false,
		},
		Database: DatabaseConfig{
			MaxOpenConns:       25,
			MaxIdleConns:       5,
			ConnMaxLifetimeMin: 5,
		},
		Metrics: MetricsConfig{
			Port:                 9090,
			Path:                 "/metrics",
			EnableRuntimeMetrics: true,
		},
		LogLevel: "info",
	}

	// Try to read config file if it exists
	if _, err := os.Stat(configPath); err == nil {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}

		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
	}

	// Override with environment variables
	overrideFromEnv(cfg)

	// Validate configuration
	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// overrideFromEnv overrides configuration values from environment variables
func overrideFromEnv(cfg *Config) {
	// Relay configuration
	if name := os.Getenv("RELAY_NAME"); name != "" {
		cfg.Relay.Name = name
	}

	// Firehose configuration
	if host := os.Getenv("FIREHOSE_HOST"); host != "" {
		cfg.Firehose.RelayHost = host
	}
	if filter := os.Getenv("NSID_FILTER"); filter != "" {
		cfg.Firehose.NSIDFilter = filter
		cfg.Backfiller.NSIDFilter = filter
	}

	// Database configuration
	if dsn := os.Getenv("DATABASE_DSN"); dsn != "" {
		cfg.Database.DSN = dsn
	}

	// GRPC configuration
	if port := os.Getenv("GRPC_SERVER_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			cfg.GRPC.Port = p
		}
	}

	// Metrics configuration
	if port := os.Getenv("METRICS_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			cfg.Metrics.Port = p
		}
	}

	// Log level
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		cfg.LogLevel = level
	}

	// Backfiller configuration
	if parallel := os.Getenv("PARALLEL_BACKFILLS"); parallel != "" {
		if p, err := strconv.Atoi(parallel); err == nil {
			cfg.Backfiller.ParallelBackfills = p
		}
	}
}

// validate checks that the configuration is valid
func validate(cfg *Config) error {
	// Check required fields
	if cfg.Database.DSN == "" {
		return fmt.Errorf("database DSN is required")
	}

	if cfg.Firehose.RelayHost == "" {
		return fmt.Errorf("firehose relay host is required")
	}

	// Validate numeric ranges
	if cfg.GRPC.Port <= 0 || cfg.GRPC.Port > 65535 {
		return fmt.Errorf("invalid GRPC port: %d", cfg.GRPC.Port)
	}

	if cfg.Metrics.Port <= 0 || cfg.Metrics.Port > 65535 {
		return fmt.Errorf("invalid metrics port: %d", cfg.Metrics.Port)
	}

	if cfg.Backfiller.ParallelBackfills <= 0 {
		return fmt.Errorf("parallel backfills must be positive")
	}

	return nil
}