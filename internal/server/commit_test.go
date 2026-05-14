package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func beginTx(t *testing.T, s *Server) []byte {
	t.Helper()
	resp, err := s.BeginTransaction(context.Background(), &firestorepb.BeginTransactionRequest{
		Database: "projects/test-proj/databases/(default)",
	})
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	return resp.Transaction
}

func TestCommit_EmptyWritesSucceeds(t *testing.T) {
	s := newTestServer(t)
	tx := beginTx(t, s)

	resp, err := s.Commit(context.Background(), &firestorepb.CommitRequest{
		Database:    "projects/test-proj/databases/(default)",
		Transaction: tx,
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if resp.CommitTime == nil {
		t.Error("CommitTime should be set")
	}
}

func TestCommit_WritesApplied(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	tx := beginTx(t, s)

	_, err := s.Commit(ctx, &firestorepb.CommitRequest{
		Database:    "projects/test-proj/databases/(default)",
		Transaction: tx,
		Writes: []*firestorepb.Write{
			{
				Operation: &firestorepb.Write_Update{
					Update: &firestorepb.Document{
						Name:   docName("widgets", "tx-doc"),
						Fields: map[string]*firestorepb.Value{"color": strVal("purple")},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	got, err := s.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: docName("widgets", "tx-doc")})
	if err != nil {
		t.Fatalf("GetDocument after commit: %v", err)
	}
	if got.Fields["color"].GetStringValue() != "purple" {
		t.Errorf("color = %q, want %q", got.Fields["color"].GetStringValue(), "purple")
	}
}

func TestCommit_UnknownTransactionFails(t *testing.T) {
	s := newTestServer(t)
	_, err := s.Commit(context.Background(), &firestorepb.CommitRequest{
		Database:    "projects/test-proj/databases/(default)",
		Transaction: []byte("invalid-tx-id"),
	})
	mustCode(t, err, codes.NotFound)
}

// Committing an already-committed transaction should fail.
func TestCommit_DoubleCommitFails(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	tx := beginTx(t, s)

	commitReq := &firestorepb.CommitRequest{
		Database:    "projects/test-proj/databases/(default)",
		Transaction: tx,
	}
	if _, err := s.Commit(ctx, commitReq); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	_, err := s.Commit(ctx, commitReq)
	if err == nil {
		t.Error("second Commit on same transaction should fail")
	}
}

func TestCommit_WriteResultCountMatchesWriteCount(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	tx := beginTx(t, s)

	resp, err := s.Commit(ctx, &firestorepb.CommitRequest{
		Database:    "projects/test-proj/databases/(default)",
		Transaction: tx,
		Writes: []*firestorepb.Write{
			{Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{Name: docName("widgets", "x"), Fields: map[string]*firestorepb.Value{"v": intVal(1)}}}},
			{Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{Name: docName("widgets", "y"), Fields: map[string]*firestorepb.Value{"v": intVal(2)}}}},
		},
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(resp.WriteResults) != 2 {
		t.Errorf("expected 2 WriteResults, got %d", len(resp.WriteResults))
	}
}
