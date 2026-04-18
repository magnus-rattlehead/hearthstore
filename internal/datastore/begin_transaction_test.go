package datastore

import (
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

func TestBeginTransaction_DistinctIDs(t *testing.T) {
	s := newTestDsServer(t)

	var r1, r2 datastorepb.BeginTransactionResponse
	mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &r1)
	mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &r2)

	if string(r1.Transaction) == string(r2.Transaction) {
		t.Errorf("expected distinct tx IDs, both were %q", r1.Transaction)
	}
}

func TestBeginTransaction_ReadOnly(t *testing.T) {
	s := newTestDsServer(t)

	var resp datastorepb.BeginTransactionResponse
	mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{
		ProjectId: testProject,
		TransactionOptions: &datastorepb.TransactionOptions{
			Mode: &datastorepb.TransactionOptions_ReadOnly_{
				ReadOnly: &datastorepb.TransactionOptions_ReadOnly{},
			},
		},
	}, &resp)

	if len(resp.Transaction) == 0 {
		t.Error("expected non-empty transaction ID")
	}
	s.grpc.txMu.Lock()
	entry, ok := s.grpc.txns[string(resp.Transaction)]
	s.grpc.txMu.Unlock()
	if !ok {
		t.Error("transaction not stored")
	}
	if !entry.readOnly {
		t.Error("expected readOnly=true")
	}
}

func TestBeginTransaction_ReadWrite(t *testing.T) {
	s := newTestDsServer(t)

	var resp datastorepb.BeginTransactionResponse
	mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{
		ProjectId: testProject,
		TransactionOptions: &datastorepb.TransactionOptions{
			Mode: &datastorepb.TransactionOptions_ReadWrite_{
				ReadWrite: &datastorepb.TransactionOptions_ReadWrite{},
			},
		},
	}, &resp)

	s.grpc.txMu.Lock()
	entry, ok := s.grpc.txns[string(resp.Transaction)]
	s.grpc.txMu.Unlock()
	if !ok {
		t.Error("transaction not stored")
	}
	if entry.readOnly {
		t.Error("expected readOnly=false for read-write tx")
	}
}
