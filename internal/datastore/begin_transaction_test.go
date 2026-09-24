package datastore

import (
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testTransactionExpiryBoundsAbandonedState(t *testing.T) {
	g := newGRPCServer(nil, nil)
	now := time.Now()
	g.txns["expired"] = txEntry{created: now.Add(-transactionMaxAge), lastUsed: now}
	g.txns["idle"] = txEntry{created: now, lastUsed: now.Add(-transactionIdle)}
	g.expireTransactions(now)
	if len(g.txns) != 0 {
		t.Fatalf("transactions retained after expiry: %d", len(g.txns))
	}
	_, _, _, err := g.resolveReadOptions(&datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_Transaction{Transaction: []byte("expired")}}, testProject, defaultDatabase)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expired transaction error = %v, want InvalidArgument", err)
	}
}

func testBeginTransactionDistinctIDs(t *testing.T, s *Server) {

	var r1, r2 datastorepb.BeginTransactionResponse
	mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &r1)
	mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &r2)

	if string(r1.Transaction) == string(r2.Transaction) {
		t.Errorf("expected distinct tx IDs, both were %q", r1.Transaction)
	}
}

func TestTransactions(t *testing.T) {
	t.Run("expiry", testTransactionExpiryBoundsAbandonedState)
	t.Run("distinct_ids", func(t *testing.T) {
		testBeginTransactionDistinctIDs(t, newTestDsServer(t))
	})
	for _, tc := range []struct {
		name     string
		options  *datastorepb.TransactionOptions
		readOnly bool
	}{
		{"read_only", &datastorepb.TransactionOptions{Mode: &datastorepb.TransactionOptions_ReadOnly_{ReadOnly: &datastorepb.TransactionOptions_ReadOnly{}}}, true},
		{"read_write", &datastorepb.TransactionOptions{Mode: &datastorepb.TransactionOptions_ReadWrite_{ReadWrite: &datastorepb.TransactionOptions_ReadWrite{}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestDsServer(t)
			var resp datastorepb.BeginTransactionResponse
			mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{
				ProjectId: testProject, TransactionOptions: tc.options,
			}, &resp)
			if len(resp.Transaction) == 0 {
				t.Fatal("expected non-empty transaction ID")
			}
			s.grpc.txMu.Lock()
			entry, ok := s.grpc.txns[string(resp.Transaction)]
			s.grpc.txMu.Unlock()
			if !ok || entry.readOnly != tc.readOnly {
				t.Fatalf("stored=%t readOnly=%t, want stored=true readOnly=%t", ok, entry.readOnly, tc.readOnly)
			}
		})
	}
}
