package server

import (
	"sync"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// Server implements firestorepb.FirestoreServer backed by a disk Store.
type Server struct {
	firestorepb.UnimplementedFirestoreServer
	store *storage.Store

	txMu sync.Mutex
	txns map[string]txEntry // active transaction IDs

	// Global registry of active watch streams and their targets.
	// Used by the tokenStale path to forward supplementary targets (e.g. ratings) to a
	// new WebChannel session that the SDK creates concurrently with sending those targets
	// to the old session.
	streamsMu sync.Mutex
	streams   map[string]map[int32]*watchTarget // streamID → targetID → watchTarget
}

type txEntry struct {
	readOnly bool
	readTime *timestamppb.Timestamp // non-nil for read-time snapshot transactions
}

func New(store *storage.Store) *Server {
	return &Server{
		store:   store,
		txns:    make(map[string]txEntry),
		streams: make(map[string]map[int32]*watchTarget),
	}
}
