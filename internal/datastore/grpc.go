package datastore

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"sync"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

const (
	transactionMaxAge       = 270 * time.Second
	transactionIdle         = 60 * time.Second
	transactionReadKeyBytes = 10 << 20
)

// GRPCServer owns Datastore request handling and transaction state.
type GRPCServer struct {
	datastorepb.UnimplementedDatastoreServer
	store      *storage.Store
	indexes    *IndexManager
	txMu       sync.Mutex
	txns       map[string]txEntry
	querySlots chan struct{}
}

func newGRPCServer(store *storage.Store, indexes *IndexManager) *GRPCServer {
	return newGRPCServerWithOptions(store, indexes, Options{})
}

// Options controls bounded query execution resources.
type Options struct {
	QueryConcurrency int
}

func newGRPCServerWithOptions(store *storage.Store, indexes *IndexManager, options Options) *GRPCServer {
	if indexes == nil {
		indexes = NewIndexManager(store)
	}
	concurrency := options.QueryConcurrency
	if concurrency == 0 {
		concurrency = storage.DefaultQueryConcurrency
	}
	return &GRPCServer{store: store, indexes: indexes, txns: make(map[string]txEntry), querySlots: make(chan struct{}, concurrency)}
}

func (g *GRPCServer) acquireQuerySlot(ctx context.Context) (func(), error) {
	select {
	case g.querySlots <- struct{}{}:
		return func() { <-g.querySlots }, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (g *GRPCServer) addTransaction(id string, entry txEntry) {
	if entry.readTime == nil {
		entry.readTime = timestamppb.New(g.store.ReadTime())
	}
	now := time.Now()
	entry.created = now
	entry.lastUsed = now
	g.txMu.Lock()
	g.txns[id] = entry
	g.txMu.Unlock()
	time.AfterFunc(transactionIdle, func() { g.expireTransaction(id) })
}

func (g *GRPCServer) expireTransaction(id string) {
	g.txMu.Lock()
	entry, ok := g.txns[id]
	if !ok {
		g.txMu.Unlock()
		return
	}
	now := time.Now()
	deadline := entry.created.Add(transactionMaxAge)
	if idleDeadline := entry.lastUsed.Add(transactionIdle); idleDeadline.Before(deadline) {
		deadline = idleDeadline
	}
	if !now.Before(deadline) {
		delete(g.txns, id)
		g.txMu.Unlock()
		return
	}
	g.txMu.Unlock()
	time.AfterFunc(time.Until(deadline), func() { g.expireTransaction(id) })
}

func (g *GRPCServer) expireTransactions(now time.Time) {
	g.txMu.Lock()
	for id, entry := range g.txns {
		if now.Sub(entry.created) >= transactionMaxAge || now.Sub(entry.lastUsed) >= transactionIdle {
			delete(g.txns, id)
		}
	}
	g.txMu.Unlock()
}

func newTxID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

// resolveReadOptions selects a snapshot and resolves explicit or inline transactions.
func (g *GRPCServer) resolveReadOptions(ro *datastorepb.ReadOptions, project, database string) (readAt *time.Time, activeTxID, newTxIDForResp string, err error) {
	if ro == nil {
		return
	}
	if rt := ro.GetReadTime(); rt != nil {
		t := rt.AsTime()
		if err = validateReadTime(t); err != nil {
			return
		}
		readAt = &t
		return
	}
	if tx := ro.GetTransaction(); len(tx) > 0 {
		txID := string(tx)
		g.txMu.Lock()
		entry, ok := g.txns[txID]
		if ok {
			now := time.Now()
			if now.Sub(entry.created) >= transactionMaxAge || now.Sub(entry.lastUsed) >= transactionIdle {
				delete(g.txns, txID)
				ok = false
			} else {
				entry.lastUsed = now
				g.txns[txID] = entry
			}
		}
		g.txMu.Unlock()
		if !ok {
			err = status.Error(codes.InvalidArgument, "the referenced transaction has expired or is no longer valid")
			return
		}
		if entry.project != project || entry.database != database {
			err = status.Error(codes.InvalidArgument, "transaction belongs to another project or database")
			return
		}
		if entry.readTime != nil {
			t := entry.readTime.AsTime()
			if err = validateReadTime(t); err != nil {
				return
			}
			readAt = &t
		}
		if !entry.readOnly {
			activeTxID = txID
		}
		return
	}
	if newTxOpts := ro.GetNewTransaction(); newTxOpts != nil {
		entry := txEntry{project: project, database: database, readTime: timestamppb.New(g.store.ReadTime())}
		if newTxOpts.GetReadOnly() != nil {
			entry.readOnly = true
			if rt := newTxOpts.GetReadOnly().GetReadTime(); rt != nil {
				entry.readTime = rt
			} else {
				entry.readTime = timestamppb.Now()
			}
			t := entry.readTime.AsTime()
			if err = validateReadTime(t); err != nil {
				return
			}
			readAt = &t
		}
		newTxIDForResp = newTxID()
		t := entry.readTime.AsTime()
		readAt = &t
		if !entry.readOnly {
			activeTxID = newTxIDForResp
		}
		g.addTransaction(newTxIDForResp, entry)
	}
	return
}

func validateReadTime(readTime time.Time) error {
	now := time.Now()
	if readTime.After(now) {
		return status.Error(codes.InvalidArgument, "read_time must not be in the future")
	}
	if readTime.Before(now.Add(-time.Hour)) {
		return status.Error(codes.FailedPrecondition, "read_time is outside the one-hour retention window")
	}
	return nil
}

func (g *GRPCServer) recordTransactionReads(id string, reads map[txReadKey]int64) error {
	g.txMu.Lock()
	defer g.txMu.Unlock()
	entry, ok := g.txns[id]
	if !ok {
		return status.Error(codes.InvalidArgument, "the referenced transaction has expired or is no longer valid")
	}
	if entry.reads == nil {
		entry.reads = make(map[txReadKey]int64)
	}
	for key, version := range reads {
		if _, exists := entry.reads[key]; !exists {
			entryBytes := len(key.project) + len(key.database) + len(key.namespace) + len(key.path)
			if entry.readBytes+entryBytes > transactionReadKeyBytes {
				return status.Error(codes.ResourceExhausted, "transaction read keys exceed Hearthstore's 10 MiB logical-key budget")
			}
			entry.readBytes += entryBytes
		}
		if _, exists := entry.reads[key]; !exists {
			entry.reads[key] = version
		}
	}
	g.txns[id] = entry
	return nil
}

func (g *GRPCServer) recordQueryScope(id string, scope storage.QueryScope) error {
	g.txMu.Lock()
	defer g.txMu.Unlock()
	entry, ok := g.txns[id]
	if !ok {
		return status.Error(codes.InvalidArgument, "transaction is no longer valid")
	}
	if entry.queries == nil {
		entry.queries = make(map[storage.QueryScope]struct{})
	}
	if _, exists := entry.queries[scope]; !exists {
		cost := len(scope.Project) + len(scope.Database) + len(scope.Namespace) + len(scope.Kind) + 64
		if entry.readBytes+cost > transactionReadKeyBytes {
			return status.Error(codes.ResourceExhausted, "transaction query scopes exceed memory budget")
		}
		entry.readBytes += cost
		entry.queries[scope] = struct{}{}
	}
	g.txns[id] = entry
	return nil
}
