package relay

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"

	"tradr-backfiller-relay/config"
	"tradr-backfiller-relay/metrics"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/backfill"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/autoscaling"
	"github.com/carlmjohnson/versioninfo"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
)

// Relay is the main relay service (equivalent to Indexer in search)
// It handles firehose subscription, backfilling, and forwarding to gRPC
type Relay struct {
	// Core components (same as search/Indexer)
	db        *gorm.DB
	relayHost string

	// Backfill components (same as search/Indexer)
	bfs *backfill.Gormstore
	bf  *backfill.Backfiller

	// Our handler for forwarding operations
	handler Handler

	// Metrics collector
	metrics *metrics.Collector
}

// LastSeq tracks cursor position (same as search)
type LastSeq struct {
	ID  uint `gorm:"primarykey"`
	Seq int64
}

// NewRelay creates a new relay instance (equivalent to NewIndexer in search)
func NewRelay(db *gorm.DB, handler Handler, cfg config.Config, metricsCollector *metrics.Collector) (*Relay, error) {
	log.Info().Msg("Initializing relay")

	// Run migrations (same as search)
	db.AutoMigrate(&LastSeq{})
	db.AutoMigrate(&backfill.GormDBJob{})

	// Create backfill store (same as search)
	bfstore := backfill.NewGormstore(db)

	// Set up backfill options optimized for streaming
	opts := backfill.DefaultBackfillOptions()
	opts.ParallelBackfills = cfg.Backfiller.ParallelBackfills
	opts.ParallelRecordCreates = cfg.Backfiller.ParallelRecordCreates
	opts.NSIDFilter = cfg.Backfiller.NSIDFilter
	opts.SyncRequestsPerSecond = 100 // Increase from default 2 to allow 100 repos/sec

	// Log the configuration for monitoring
	log.Info().
		Int("parallel_backfills", opts.ParallelBackfills).
		Int("parallel_record_creates", opts.ParallelRecordCreates).
		Int("sync_requests_per_second", opts.SyncRequestsPerSecond).
		Msg("Backfiller configuration (using streaming)")

	// Create relay instance
	r := &Relay{
		db:        db,
		relayHost: cfg.Firehose.RelayHost,
		bfs:       bfstore,
		handler:   handler,
		metrics:   metricsCollector,
	}

	// Create backfiller with our handlers (same pattern as search)
	bf := backfill.NewBackfiller(
		"relay",
		bfstore,
		r.handleCreateOrUpdate, // These methods will forward to gRPC
		r.handleCreateOrUpdate,
		r.handleDelete,
		opts,
	)

	r.bf = bf
	return r, nil
}

// RunRelay starts the relay (following same pattern as search/indexing.go)
func (r *Relay) RunRelay(ctx context.Context) error {
	// Get last cursor
	cursor, err := r.getLastCursor()
	if err != nil {
		return fmt.Errorf("get last cursor: %w", err)
	}

	// Load and start backfill jobs
	if err := r.bfs.LoadJobs(ctx); err != nil {
		return fmt.Errorf("loading backfill jobs: %w", err)
	}
	go r.bf.Start()

	// Connect to firehose
	u, err := url.Parse(r.relayHost)
	if err != nil {
		return fmt.Errorf("invalid relay host: %w", err)
	}
	u.Path = "xrpc/com.atproto.sync.subscribeRepos"
	if cursor != 0 {
		u.RawQuery = fmt.Sprintf("cursor=%d", cursor)
	}

	con, _, err := websocket.DefaultDialer.Dial(u.String(), http.Header{
		"User-Agent": []string{fmt.Sprintf("relay/%s", versioninfo.Short())},
	})
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	// Handle events (same as search)
	callbacks := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
			// Save cursor periodically (same as search)
			defer func() {
				if evt.Seq%100 == 0 {
					if err := r.updateLastCursor(evt.Seq); err != nil {
						log.Error().Err(err).Msg("Failed to persist cursor")
					}
				}
			}()

			// Handle tooBig events (same as search)
			if evt.TooBig {
				if evt.Since != nil {
					log.Warn().Msg("Skipping non-genesis tooBig event")
					return nil
				}
				// For tooBig genesis, enqueue backfill
				return r.bfs.EnqueueJob(ctx, evt.Repo)
			}

			// Pass to backfiller (same as search)
			if err := r.bf.HandleEvent(ctx, evt); err != nil {
				log.Error().Err(err).Msg("Failed to handle event")
			}
			return nil
		},
	}

	// Use autoscaling scheduler (same as search)
	scheduler := autoscaling.NewScheduler(
		autoscaling.DefaultAutoscaleSettings(),
		r.relayHost,
		callbacks.EventHandler,
	)

	// Create a logger for the stream handler
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", "stream")

	return events.HandleRepoStream(ctx, con, scheduler, logger)
}

// handleCreateOrUpdate forwards create/update operations to handler
// Optimized for streaming - data is already in memory, no lookups needed
func (r *Relay) handleCreateOrUpdate(ctx context.Context, repo string, rev string, path string, recB *[]byte, rcid *cid.Cid) error {
	// Track metrics
	if r.metrics != nil {
		r.metrics.IncrementRecordsProcessed()
		r.metrics.IncrementOperationCount("create")
	}

	// With streaming, the record data (*recB) is already loaded
	// No need for blockstore lookups or additional fetching
	// This is a major performance improvement over the old approach

	// TEMPORARILY DISABLED: Commenting out gRPC call to measure throughput
	// When re-enabled, this will be very efficient since data is already in memory
	// return r.handler.HandleCreateRecord(ctx, repo, rev, path, recB, rcid, 0)
	return nil
}

// handleDelete forwards delete operations to handler
// Optimized for streaming
func (r *Relay) handleDelete(ctx context.Context, repo string, rev string, path string) error {
	// Track metrics
	if r.metrics != nil {
		r.metrics.IncrementRecordsProcessed()
		r.metrics.IncrementOperationCount("delete")
	}

	// Note: seq is 0 for backfilled data (not from firehose)

	// TEMPORARILY DISABLED: Commenting out gRPC call to measure throughput
	// return r.handler.HandleDeleteRecord(ctx, repo, rev, path, 0)
	return nil
}

// getLastCursor gets the last processed sequence (same as search)
func (r *Relay) getLastCursor() (int64, error) {
	var lastSeq LastSeq
	if err := r.db.Find(&lastSeq).Error; err != nil {
		return 0, err
	}
	if lastSeq.ID == 0 {
		return 0, r.db.Create(&lastSeq).Error
	}
	return lastSeq.Seq, nil
}

// updateLastCursor saves cursor position (same as search)
func (r *Relay) updateLastCursor(seq int64) error {
	return r.db.Model(LastSeq{}).Where("id = 1").Update("seq", seq).Error
}
