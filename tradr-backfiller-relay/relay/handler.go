package relay

import (
	"context"
	"github.com/ipfs/go-cid"
)

// Handler interface defines the methods needed to handle backfill operations
type Handler interface {
	HandleCreateRecord(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid, seq int64) error
	HandleUpdateRecord(ctx context.Context, repo string, rev string, path string, rec *[]byte, cid *cid.Cid, seq int64) error
	HandleDeleteRecord(ctx context.Context, repo string, rev string, path string, seq int64) error
}