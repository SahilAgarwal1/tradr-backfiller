package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
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
	
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Create gRPC server to accept ingester connections
	grpcServer := grpc.NewServer(cfg.GRPC, metricsCollector)
	
	// Create handler that uses the server to broadcast
	grpcHandler := grpc.NewHandler(grpcServer, metricsCollector)
	
	// Create relay (equivalent to creating Indexer in search)
	r, err := relay.NewRelay(db, grpcHandler, *cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize relay")
	}
	
	// Start gRPC server in background
	go func() {
		port := cfg.GRPC.Port
		if port == 0 {
			port = 50051 // default gRPC port
		}
		if err := grpcServer.Start(ctx, port); err != nil {
			log.Error().Err(err).Msg("gRPC server error")
		}
	}()
	
	// Start metrics server
	go func() {
		if err := metrics.StartServer(ctx, cfg.Metrics); err != nil {
			log.Error().Err(err).Msg("Metrics server error")
		}
	}()
	
	// Start pprof server for profiling
	go func() {
		pprofPort := 6060
		log.Info().Int("port", pprofPort).Msg("Starting pprof server for profiling")
		log.Info().Msg("Available endpoints:")
		log.Info().Msg("  http://localhost:6060/debug/pprof/          - pprof index")
		log.Info().Msg("  http://localhost:6060/debug/pprof/profile   - CPU profile")
		log.Info().Msg("  http://localhost:6060/debug/pprof/heap      - heap profile")
		log.Info().Msg("  http://localhost:6060/debug/pprof/goroutine - goroutine profile")
		log.Info().Msg("  http://localhost:6060/debug/pprof/block     - block profile")
		log.Info().Msg("  http://localhost:6060/debug/pprof/mutex     - mutex profile")
		if err := http.ListenAndServe(fmt.Sprintf(":%d", pprofPort), nil); err != nil {
			log.Error().Err(err).Msg("pprof server error")
		}
	}()
	
	// Handle shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	// Start relay in a goroutine
	errChan := make(chan error, 1)
	go func() {
		log.Info().Msg("Starting relay")
		errChan <- r.RunRelay(ctx)
	}()
	
	// Wait for shutdown signal or error
	select {
	case sig := <-sigChan:
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
	case err := <-errChan:
		if err != nil {
			log.Error().Err(err).Msg("Relay error")
		}
	}
	
	// Graceful shutdown
	log.Info().Msg("Shutting down...")
	cancel()
	
	// Give services time to clean up
	time.Sleep(2 * time.Second)
	
	log.Info().Msg("Relay shutdown complete")
}