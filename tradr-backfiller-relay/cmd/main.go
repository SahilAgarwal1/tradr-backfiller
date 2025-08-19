package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"tradr-backfiller-relay/config"
	"tradr-backfiller-relay/grpc"
	"tradr-backfiller-relay/metrics"
	"tradr-backfiller-relay/relay"
)

func main() {
	// Initialize logging
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// Set log level
	if level, err := zerolog.ParseLevel(cfg.LogLevel); err == nil {
		zerolog.SetGlobalLevel(level)
	}

	// Connect to database (similar to search)
	log.Info().Msg("Connecting to database")
	db, err := gorm.Open(postgres.Open(cfg.Database.DSN), &gorm.Config{})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to database")
	}
	
	// Configure connection pool
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get underlying SQL database")
	}
	sqlDB.SetMaxOpenConns(cfg.Database.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.Database.MaxIdleConns)
	
	// Initialize metrics
	metricsCollector := metrics.New(cfg.Relay.Name)
	
	// Initialize gRPC connections to Ingesters
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	streamManager, err := grpc.NewStreamManager(ctx, cfg.Ingesters, cfg.GRPC, metricsCollector)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize stream manager")
	}
	defer streamManager.Close()
	
	// Create gRPC handler
	grpcHandler := grpc.NewHandler(streamManager, metricsCollector)
	
	// Create relay (equivalent to creating Indexer in search)
	r, err := relay.NewRelay(db, grpcHandler, *cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize relay")
	}
	
	// Start metrics server
	go func() {
		if err := metrics.StartServer(ctx, cfg.Metrics); err != nil {
			log.Error().Err(err).Msg("Metrics server error")
		}
	}()
	
	// Handle shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	// Run relay in goroutine so we can handle shutdown
	go func() {
		log.Info().Msg("Starting relay")
		if err := r.RunRelay(ctx); err != nil {
			log.Error().Err(err).Msg("Relay error")
			cancel()
		}
	}()
	
	// Wait for shutdown signal
	select {
	case sig := <-sigChan:
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
	case <-ctx.Done():
		log.Info().Msg("Context cancelled")
	}
	
	// Graceful shutdown
	cancel()
	time.Sleep(2 * time.Second) // Give time for cleanup
	
	log.Info().Msg("Relay shutdown complete")
}