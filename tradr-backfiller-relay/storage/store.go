package storage

import (
	"fmt"
	"time"

	"github.com/bluesky-social/indigo/backfill"
	"github.com/rs/zerolog/log"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"tradr-backfiller-relay/config"
)

// Store wraps the indigo backfill.Store with our database connection
type Store struct {
	// db is the underlying GORM database connection
	db *gorm.DB
	
	// backfillStore is the indigo backfill store implementation
	// We reuse the GormStore from indigo which handles all the
	// job state management, buffering, and retry logic
	backfillStore backfill.Store
}

// NewStore creates a new storage instance with database connection
func NewStore(cfg config.DatabaseConfig) (*Store, error) {
	log.Info().Str("dsn", maskDSN(cfg.DSN)).Msg("Connecting to database")

	// Configure GORM with appropriate logging
	gormConfig := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent), // We'll use zerolog instead
		NowFunc: func() time.Time {
			return time.Now().UTC()
		},
	}

	// Open database connection
	db, err := gorm.Open(postgres.Open(cfg.DSN), gormConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Get underlying SQL database to configure connection pool
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get underlying SQL database: %w", err)
	}

	// Configure connection pool settings
	if cfg.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetimeMin > 0 {
		sqlDB.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetimeMin) * time.Minute)
	}

	// Test the connection
	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Run migrations to create necessary tables
	if err := runMigrations(db); err != nil {
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	// Create the indigo backfill store
	// This reuses all the existing logic for job management,
	// buffering, state tracking, and retry handling
	// The GormStore will handle its own table (gorm_db_jobs) internally
	backfillStore := backfill.NewGormstore(db)

	log.Info().Msg("Database connection established and migrations complete")

	return &Store{
		db:            db,
		backfillStore: backfillStore,
	}, nil
}

// GetBackfillStore returns the underlying backfill store for use with the backfiller
func (s *Store) GetBackfillStore() backfill.Store {
	return s.backfillStore
}

// GetDB returns the underlying GORM database (useful for custom queries)
func (s *Store) GetDB() *gorm.DB {
	return s.db
}

// Close closes the database connection
func (s *Store) Close() error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying SQL database: %w", err)
	}
	
	log.Info().Msg("Closing database connection")
	return sqlDB.Close()
}

// runMigrations creates or updates the database schema
func runMigrations(db *gorm.DB) error {
	log.Info().Msg("Running database migrations")

	// Run auto-migration for backfill package tables
	// The GormDBJob is the table used by the backfill.Gormstore
	// It tracks job state, retries, and other metadata
	if err := db.AutoMigrate(&backfill.GormDBJob{}); err != nil {
		return fmt.Errorf("failed to migrate backfill tables: %w", err)
	}

	// The backfill.Gormstore handles buffered operations in memory
	// so we don't need a separate buffered_ops table
	// The GormDBJob table already tracks:
	// - Repo (DID)
	// - State (enqueued, in_progress, complete, failed)
	// - Rev (current revision)
	// - RetryCount and RetryAfter for retry logic

	log.Info().Msg("Database migrations complete")
	return nil
}

// maskDSN masks the password in a DSN for logging
func maskDSN(dsn string) string {
	// Simple masking - in production, use a proper DSN parser
	if idx := len("postgres://"); len(dsn) > idx {
		if atIdx := idx + len("user:"); len(dsn) > atIdx {
			// Find the @ symbol after the password
			for i := atIdx; i < len(dsn); i++ {
				if dsn[i] == '@' {
					return dsn[:atIdx] + "****" + dsn[i:]
				}
			}
		}
	}
	return dsn
}

// Stats returns statistics about the storage state
type Stats struct {
	TotalJobs       int64
	EnqueuedJobs    int64
	InProgressJobs  int64
	CompletedJobs   int64
	FailedJobs      int64
}

// GetStats returns current statistics about the storage state
func (s *Store) GetStats() (*Stats, error) {
	stats := &Stats{}

	// Count jobs by state from the GormDBJob table
	// The backfill package uses "gorm_db_jobs" as the table name
	s.db.Model(&backfill.GormDBJob{}).Count(&stats.TotalJobs)
	s.db.Model(&backfill.GormDBJob{}).Where("state = ?", "enqueued").Count(&stats.EnqueuedJobs)
	s.db.Model(&backfill.GormDBJob{}).Where("state = ?", "in_progress").Count(&stats.InProgressJobs)
	s.db.Model(&backfill.GormDBJob{}).Where("state = ?", "complete").Count(&stats.CompletedJobs)
	s.db.Model(&backfill.GormDBJob{}).Where("state LIKE ?", "failed%").Count(&stats.FailedJobs)

	return stats, nil
}