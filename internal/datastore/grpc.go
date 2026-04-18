package datastore

import (
	"crypto/rand"
	"encoding/base64"
	"sync"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// GRPCServer implements datastorepb.DatastoreServer. It is the canonical logic
// and state owner for the Datastore module; the HTTP Server delegates all
// request handling to it.
type GRPCServer struct {
	datastorepb.UnimplementedDatastoreServer
	store *storage.Store
	txMu  sync.Mutex
	txns  map[string]txEntry
}

func newGRPCServer(store *storage.Store) *GRPCServer {
	return &GRPCServer{store: store, txns: make(map[string]txEntry)}
}

func newTxID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

// resolveReadOptions extracts the effective snapshot time and transaction IDs
// from ReadOptions. Used identically by Lookup and RunQuery.
//
// Returns:
//   - readAt: non-nil when reads should use a historical snapshot
//   - activeTxID: non-empty for a read-write transaction (for OCC read tracking)
//   - newTxIDForResp: non-empty when a new transaction was started inline
func (g *GRPCServer) resolveReadOptions(ro *datastorepb.ReadOptions) (readAt *time.Time, activeTxID, newTxIDForResp string) {
	if ro == nil {
		return
	}
	if rt := ro.GetReadTime(); rt != nil {
		t := rt.AsTime()
		readAt = &t
		return
	}
	if tx := ro.GetTransaction(); len(tx) > 0 {
		txID := string(tx)
		g.txMu.Lock()
		entry, ok := g.txns[txID]
		g.txMu.Unlock()
		if ok && entry.readOnly && entry.readTime != nil {
			t := entry.readTime.AsTime()
			readAt = &t
		} else if ok && !entry.readOnly {
			activeTxID = txID
		}
		return
	}
	if newTxOpts := ro.GetNewTransaction(); newTxOpts != nil {
		var entry txEntry
		if newTxOpts.GetReadOnly() != nil {
			entry.readOnly = true
			if rt := newTxOpts.GetReadOnly().GetReadTime(); rt != nil {
				entry.readTime = rt
			} else {
				entry.readTime = timestamppb.Now()
			}
			t := entry.readTime.AsTime()
			readAt = &t
		}
		newTxIDForResp = newTxID()
		g.txMu.Lock()
		g.txns[newTxIDForResp] = entry
		g.txMu.Unlock()
	}
	return
}
