package datastore

import (
	"net/http"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

func TestLookup(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, s *Server)
	}{
		{
			name: "found",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_found", "w1")
				upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"color": dsStr("red")}))
				var resp datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &resp)
				if len(resp.Found) != 1 {
					t.Fatalf("want 1 found, got %d", len(resp.Found))
				}
				if len(resp.Missing) != 0 {
					t.Fatalf("want 0 missing, got %d", len(resp.Missing))
				}
				if got := resp.Found[0].Entity.Properties["color"].GetStringValue(); got != "red" {
					t.Errorf("color = %q, want %q", got, "red")
				}
			},
		},
		{
			name: "missing",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_missing", "nonexistent")
				var resp datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &resp)
				if len(resp.Missing) != 1 {
					t.Fatalf("want 1 missing, got %d", len(resp.Missing))
				}
				if len(resp.Found) != 0 {
					t.Fatalf("want 0 found, got %d", len(resp.Found))
				}
			},
		},
		{
			name: "mixed",
			run: func(t *testing.T, s *Server) {
				k1 := dsKey("K_mixed", "w1")
				k2 := dsKey("K_mixed", "w2")
				upsertEntity(t, s, dsEntity(k1, map[string]*datastorepb.Value{"x": dsInt(1)}))
				var resp datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{k1, k2}}, &resp)
				if len(resp.Found) != 1 || len(resp.Missing) != 1 {
					t.Errorf("want found=1 missing=1, got found=%d missing=%d", len(resp.Found), len(resp.Missing))
				}
			},
		},
		{
			name: "dedup",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_dedup", "w1")
				upsertEntity(t, s, dsEntity(key, nil))
				var resp datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{
					ProjectId: testProject,
					Keys:      []*datastorepb.Key{key, key, key},
				}, &resp)
				if len(resp.Found) != 1 {
					t.Errorf("want 1 found (dedup), got %d", len(resp.Found))
				}
			},
		},
		{
			// Lookup inside a read-write transaction records the entity version in the
			// read set; a subsequent commit that sees a version bump returns 409.
			name: "in_tx_records_version",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_in_tx_records_version", "e1")
				upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"v": dsInt(1)}))

				var begin datastorepb.BeginTransactionResponse
				mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &begin)

				// Lookup within the transaction - records version in read set.
				var lr datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{
					ProjectId:   testProject,
					Keys:        []*datastorepb.Key{key},
					ReadOptions: &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_Transaction{Transaction: begin.Transaction}},
				}, &lr)
				if len(lr.Found) != 1 {
					t.Fatalf("in_tx_records_version: entity not found in tx lookup")
				}

				// Another writer updates the entity outside this transaction.
				upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"v": dsInt(2)}))

				// Now commit the original transaction - OCC must detect the conflict.
				r := doPost(t, s, projectURL("commit"), &datastorepb.CommitRequest{
					ProjectId:           testProject,
					TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: begin.Transaction},
					Mutations: []*datastorepb.Mutation{
						{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(key, map[string]*datastorepb.Value{"v": dsInt(99)})}},
					},
				}, nil)
				if r.StatusCode != http.StatusConflict {
					t.Errorf("in_tx_records_version: want 409 after out-of-band write, got %d", r.StatusCode)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestDsServer(t)
			tc.run(t, s)
		})
	}
}
