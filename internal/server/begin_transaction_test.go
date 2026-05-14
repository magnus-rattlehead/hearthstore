package server

import (
	"context"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func TestBeginTransaction_ReturnsTransactionID(t *testing.T) {
	s := newTestServer(t)
	resp, err := s.BeginTransaction(context.Background(), &firestorepb.BeginTransactionRequest{
		Database: "projects/test-proj/databases/(default)",
	})
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if len(resp.Transaction) == 0 {
		t.Error("expected non-empty transaction ID")
	}
}

func TestBeginTransaction_ReadOnly(t *testing.T) {
	s := newTestServer(t)
	resp, err := s.BeginTransaction(context.Background(), &firestorepb.BeginTransactionRequest{
		Database: "projects/test-proj/databases/(default)",
		Options: &firestorepb.TransactionOptions{
			Mode: &firestorepb.TransactionOptions_ReadOnly_{
				ReadOnly: &firestorepb.TransactionOptions_ReadOnly{},
			},
		},
	})
	if err != nil {
		t.Fatalf("BeginTransaction (read-only): %v", err)
	}
	if len(resp.Transaction) == 0 {
		t.Error("expected non-empty transaction ID for read-only transaction")
	}
}

func TestBeginTransaction_ReadWrite(t *testing.T) {
	s := newTestServer(t)
	resp, err := s.BeginTransaction(context.Background(), &firestorepb.BeginTransactionRequest{
		Database: "projects/test-proj/databases/(default)",
		Options: &firestorepb.TransactionOptions{
			Mode: &firestorepb.TransactionOptions_ReadWrite_{
				ReadWrite: &firestorepb.TransactionOptions_ReadWrite{},
			},
		},
	})
	if err != nil {
		t.Fatalf("BeginTransaction (read-write): %v", err)
	}
	if len(resp.Transaction) == 0 {
		t.Error("expected non-empty transaction ID for read-write transaction")
	}
}

func TestBeginTransaction_EachCallReturnsDistinctID(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	req := &firestorepb.BeginTransactionRequest{Database: "projects/test-proj/databases/(default)"}

	r1, err := s.BeginTransaction(ctx, req)
	if err != nil {
		t.Fatalf("BeginTransaction 1: %v", err)
	}
	r2, err := s.BeginTransaction(ctx, req)
	if err != nil {
		t.Fatalf("BeginTransaction 2: %v", err)
	}
	if string(r1.Transaction) == string(r2.Transaction) {
		t.Error("concurrent transactions must have distinct IDs")
	}
}
