package storage

import (
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PinReadTime prevents GC from advancing past an active operation's snapshot.
// The caller must release the lease when the operation finishes.
func (s *Store) PinReadTime(snapshot time.Time) (func(), error) {
	if snapshot.UnixNano() < 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid snapshot time")
	}
	ts := uint64(snapshot.UnixNano())
	s.snapshotMu.Lock()
	if ts < s.discardTs {
		s.snapshotMu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "snapshot has been discarded")
	}
	if s.snapshots == nil {
		s.snapshots = make(map[uint64]int)
	}
	s.snapshots[ts]++
	s.snapshotMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.snapshotMu.Lock()
			s.snapshots[ts]--
			if s.snapshots[ts] == 0 {
				delete(s.snapshots, ts)
			}
			s.snapshotMu.Unlock()
		})
	}, nil
}

func (s *Store) advanceDiscardTime(cutoff time.Time) {
	if cutoff.UnixNano() <= 0 {
		return
	}
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	ts := uint64(cutoff.UnixNano())
	for active := range s.snapshots {
		ts = min(ts, active)
	}
	if ts > s.discardTs {
		s.db.SetDiscardTs(ts)
		s.discardTs = ts
	}
}
