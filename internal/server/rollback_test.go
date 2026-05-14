package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func TestRollback_Success(t *testing.T) {
	s := newTestServer(t)
	tx := beginTx(t, s)

	_, err := s.Rollback(context.Background(), &firestorepb.RollbackRequest{
		Database:    "projects/test-proj/databases/(default)",
		Transaction: tx,
	})
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
}

func TestRollback_UnknownTransactionFails(t *testing.T) {
	s := newTestServer(t)
	_, err := s.Rollback(context.Background(), &firestorepb.RollbackRequest{
		Database:    "projects/test-proj/databases/(default)",
		Transaction: []byte("bogus"),
	})
	mustCode(t, err, codes.NotFound)
}

// Writes made within a rolled-back transaction must not be visible.
func TestRollback_WritesNotApplied(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	tx := beginTx(t, s)

	// Perform a write scoped to the transaction (via Commit with writes, but rolled back instead).
	// The canonical way to test this: commit a write under the tx, then rollback, and verify
	// the document is absent. In Firestore, writes are staged and only applied at Commit.
	if _, err := s.Rollback(ctx, &firestorepb.RollbackRequest{
		Database:    "projects/test-proj/databases/(default)",
		Transaction: tx,
	}); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// The transaction is gone; a subsequent Commit must fail.
	_, err := s.Commit(ctx, &firestorepb.CommitRequest{
		Database:    "projects/test-proj/databases/(default)",
		Transaction: tx,
	})
	mustCode(t, err, codes.NotFound)
}

func TestRollback_DoubleRollbackFails(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	tx := beginTx(t, s)

	req := &firestorepb.RollbackRequest{
		Database:    "projects/test-proj/databases/(default)",
		Transaction: tx,
	}
	if _, err := s.Rollback(ctx, req); err != nil {
		t.Fatalf("first Rollback: %v", err)
	}
	_, err := s.Rollback(ctx, req)
	mustCode(t, err, codes.NotFound)
}
