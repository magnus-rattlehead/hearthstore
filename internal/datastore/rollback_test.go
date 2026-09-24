package datastore

import (
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

func testRollbackUnknownTx(t *testing.T, s *Server) {

	r := doPost(t, s, projectURL("rollback"), &datastorepb.RollbackRequest{
		ProjectId:   testProject,
		Transaction: []byte("no-such-tx"),
	}, nil)
	if r.StatusCode != 404 {
		t.Errorf("want 404 for unknown tx, got %d", r.StatusCode)
	}
}

func testRollbackDoubleRollback(t *testing.T, s *Server) {

	var begin datastorepb.BeginTransactionResponse
	mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &begin)

	rbReq := &datastorepb.RollbackRequest{
		ProjectId:   testProject,
		Transaction: begin.Transaction,
	}
	r1 := doPost(t, s, projectURL("rollback"), rbReq, nil)
	if r1.StatusCode != 200 {
		t.Fatalf("first rollback: %d", r1.StatusCode)
	}
	s.grpc.txMu.Lock()
	_, exists := s.grpc.txns[string(begin.Transaction)]
	s.grpc.txMu.Unlock()
	if exists {
		t.Error("transaction still in map after rollback")
	}
	r2 := doPost(t, s, projectURL("rollback"), rbReq, nil)
	if r2.StatusCode != 404 {
		t.Errorf("second rollback: want 404, got %d", r2.StatusCode)
	}
}

func TestRollback(t *testing.T) {
	t.Run("lifecycle", func(t *testing.T) {
		testRollbackDoubleRollback(t, newTestDsServer(t))
	})
	t.Run("unknown_transaction", func(t *testing.T) {
		testRollbackUnknownTx(t, newTestDsServer(t))
	})
}
