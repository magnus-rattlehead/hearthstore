package datastore

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

func TestCommit(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, s *Server)
	}{
		{
			name: "insert",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_insert", "w1")
				entity := dsEntity(key, map[string]*datastorepb.Value{"color": dsStr("blue")})
				var resp datastorepb.CommitResponse
				mustPost(t, s, "commit", &datastorepb.CommitRequest{
					ProjectId: testProject,
					Mutations: []*datastorepb.Mutation{
						{Operation: &datastorepb.Mutation_Insert{Insert: entity}},
					},
				}, &resp)
				if len(resp.MutationResults) != 1 {
					t.Fatalf("want 1 result, got %d", len(resp.MutationResults))
				}
				var lr datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &lr)
				if len(lr.Found) != 1 {
					t.Fatalf("entity not found after insert")
				}
			},
		},
		{
			name: "insert_already_exists",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_insert_already_exists", "w1")
				upsertEntity(t, s, dsEntity(key, nil))
				r := doPost(t, s, projectURL("commit"), &datastorepb.CommitRequest{
					ProjectId: testProject,
					Mutations: []*datastorepb.Mutation{
						{Operation: &datastorepb.Mutation_Insert{Insert: dsEntity(key, nil)}},
					},
				}, nil)
				if r.StatusCode != http.StatusConflict {
					t.Errorf("want 409 for duplicate insert, got %d", r.StatusCode)
				}
			},
		},
		{
			name: "update",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_update", "w1")
				upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"x": dsInt(1)}))
				mustPost(t, s, "commit", &datastorepb.CommitRequest{
					ProjectId: testProject,
					Mutations: []*datastorepb.Mutation{
						{Operation: &datastorepb.Mutation_Update{Update: dsEntity(key, map[string]*datastorepb.Value{"x": dsInt(2)})}},
					},
				}, nil)
				var lr datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &lr)
				if lr.Found[0].Entity.Properties["x"].GetIntegerValue() != 2 {
					t.Error("property not updated")
				}
			},
		},
		{
			name: "upsert",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_upsert", "w1")
				upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"v": dsInt(10)}))
				upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"v": dsInt(20)}))
				var lr datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &lr)
				if lr.Found[0].Entity.Properties["v"].GetIntegerValue() != 20 {
					t.Error("upsert did not overwrite")
				}
			},
		},
		{
			name: "delete",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_delete", "w1")
				upsertEntity(t, s, dsEntity(key, nil))
				mustPost(t, s, "commit", &datastorepb.CommitRequest{
					ProjectId: testProject,
					Mutations: []*datastorepb.Mutation{
						{Operation: &datastorepb.Mutation_Delete{Delete: key}},
					},
				}, nil)
				var lr datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &lr)
				if len(lr.Missing) != 1 {
					t.Error("entity still exists after delete")
				}
			},
		},
		{
			name: "transactional",
			run: func(t *testing.T, s *Server) {
				var begin datastorepb.BeginTransactionResponse
				mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &begin)
				key := dsKey("K_transactional", "tx1")
				mustPost(t, s, "commit", &datastorepb.CommitRequest{
					ProjectId:           testProject,
					TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: begin.Transaction},
					Mutations: []*datastorepb.Mutation{
						{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(key, map[string]*datastorepb.Value{"x": dsInt(99)})}},
					},
				}, nil)
				s.grpc.txMu.Lock()
				_, exists := s.grpc.txns[string(begin.Transaction)]
				s.grpc.txMu.Unlock()
				if exists {
					t.Error("transaction still in map after commit")
				}
			},
		},
		{
			name: "transactional_unknown_tx",
			run: func(t *testing.T, s *Server) {
				r := doPost(t, s, projectURL("commit"), &datastorepb.CommitRequest{
					ProjectId:           testProject,
					TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: []byte("bogus-tx")},
				}, nil)
				if r.StatusCode != http.StatusNotFound {
					t.Errorf("want 404 for unknown tx, got %d", r.StatusCode)
				}
			},
		},
		{
			name: "property_transform_set_to_server_value",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_ptssv", "w1")
				upsertEntity(t, s, dsEntity(key, nil))
				mustPost(t, s, "commit", &datastorepb.CommitRequest{
					ProjectId: testProject,
					Mutations: []*datastorepb.Mutation{{
						Operation: &datastorepb.Mutation_Update{Update: dsEntity(key, nil)},
						PropertyTransforms: []*datastorepb.PropertyTransform{{
							Property: "updatedAt",
							TransformType: &datastorepb.PropertyTransform_SetToServerValue{
								SetToServerValue: datastorepb.PropertyTransform_REQUEST_TIME,
							},
						}},
					}},
				}, nil)
				var lr datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &lr)
				if lr.Found[0].Entity.Properties["updatedAt"] == nil {
					t.Error("updatedAt not set by transform")
				}
			},
		},
		{
			name: "property_transform_increment",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_ptinc", "counter")
				upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"n": dsInt(5)}))
				mustPost(t, s, "commit", &datastorepb.CommitRequest{
					ProjectId: testProject,
					Mutations: []*datastorepb.Mutation{{
						Operation: &datastorepb.Mutation_Update{Update: dsEntity(key, map[string]*datastorepb.Value{"n": dsInt(5)})},
						PropertyTransforms: []*datastorepb.PropertyTransform{{
							Property:      "n",
							TransformType: &datastorepb.PropertyTransform_Increment{Increment: dsInt(3)},
						}},
					}},
				}, nil)
				var lr datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &lr)
				if n := lr.Found[0].Entity.Properties["n"].GetIntegerValue(); n != 8 {
					t.Errorf("n = %d, want 8", n)
				}
			},
		},
		{
			name: "property_transform_append_missing_elements",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_ptame", "arr")
				upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{
					"tags": dsArray(dsStr("a"), dsStr("b")),
				}))
				mustPost(t, s, "commit", &datastorepb.CommitRequest{
					ProjectId: testProject,
					Mutations: []*datastorepb.Mutation{{
						Operation: &datastorepb.Mutation_Update{Update: dsEntity(key, map[string]*datastorepb.Value{
							"tags": dsArray(dsStr("a"), dsStr("b")),
						})},
						PropertyTransforms: []*datastorepb.PropertyTransform{{
							Property: "tags",
							TransformType: &datastorepb.PropertyTransform_AppendMissingElements{
								AppendMissingElements: &datastorepb.ArrayValue{
									Values: []*datastorepb.Value{dsStr("b"), dsStr("c")},
								},
							},
						}},
					}},
				}, nil)
				var lr datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &lr)
				tags := lr.Found[0].Entity.Properties["tags"].GetArrayValue().GetValues()
				if len(tags) != 3 {
					t.Errorf("want 3 tags (a, b, c), got %d", len(tags))
				}
			},
		},
		{
			name: "base_version_conflict",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_bvc", "cv")
				upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"v": dsInt(1)}))
				var lr datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &lr)
				ver := lr.Found[0].Version
				var resp datastorepb.CommitResponse
				mustPost(t, s, "commit", &datastorepb.CommitRequest{
					ProjectId: testProject,
					Mutations: []*datastorepb.Mutation{{
						Operation:                 &datastorepb.Mutation_Upsert{Upsert: dsEntity(key, map[string]*datastorepb.Value{"v": dsInt(2)})},
						ConflictDetectionStrategy: &datastorepb.Mutation_BaseVersion{BaseVersion: ver + 999},
					}},
				}, &resp)
				if !resp.MutationResults[0].ConflictDetected {
					t.Error("expected conflict_detected=true for stale base_version")
				}
			},
		},
		{
			name: "insert_incomplete_key",
			run: func(t *testing.T, s *Server) {
				incompleteKey := &datastorepb.Key{
					PartitionId: &datastorepb.PartitionId{ProjectId: testProject},
					Path:        []*datastorepb.Key_PathElement{{Kind: "K_iik"}},
				}
				var resp datastorepb.CommitResponse
				mustPost(t, s, "commit", &datastorepb.CommitRequest{
					ProjectId: testProject,
					Mutations: []*datastorepb.Mutation{
						{Operation: &datastorepb.Mutation_Insert{Insert: dsEntity(incompleteKey, map[string]*datastorepb.Value{"color": dsStr("red")})}},
					},
				}, &resp)
				if len(resp.MutationResults) != 1 {
					t.Fatalf("want 1 mutation result, got %d", len(resp.MutationResults))
				}
				mr := resp.MutationResults[0]
				if mr.Key == nil {
					t.Fatal("MutationResult.Key should be non-nil for incomplete key insert")
				}
				path := mr.Key.Path
				if len(path) == 0 || path[len(path)-1].GetId() == 0 {
					t.Errorf("allocated key should have non-zero numeric ID")
				}
			},
		},
		{
			name: "upsert_incomplete_key",
			run: func(t *testing.T, s *Server) {
				// Send the exact JSON body shape the Python Datastore client sends:
				// incomplete key with explicit namespaceId:"" and excludeFromIndexes on props.
				rawBody := `{
					"mode":"NON_TRANSACTIONAL",
					"mutations":[{"upsert":{
						"key":{
							"partitionId":{"projectId":"` + testProject + `","namespaceId":""},
							"path":[{"kind":"K_uik"}]
						},
						"properties":{
							"timezone":{"excludeFromIndexes":true,"stringValue":"US/Pacific"},
							"status":{"excludeFromIndexes":false,"stringValue":"active"}
						}
					}}]
				}`
				hr := httptest.NewRequest(http.MethodPost, projectURL("commit"), strings.NewReader(rawBody))
				hr.Header.Set("Content-Type", "application/json")
				rw := httptest.NewRecorder()
				s.Handler().ServeHTTP(rw, hr)
				if rw.Code != 200 {
					t.Fatalf("status %d: %s", rw.Code, rw.Body.String())
				}
				t.Logf("response JSON: %s", rw.Body.String())
				var resp datastorepb.CommitResponse
				if err := pjsonUnmarshal.Unmarshal(rw.Body.Bytes(), &resp); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				if len(resp.MutationResults) != 1 {
					t.Fatalf("want 1 mutation result, got %d", len(resp.MutationResults))
				}
				mr := resp.MutationResults[0]
				if mr.Key == nil {
					t.Fatal("MutationResult.Key should be non-nil for incomplete key upsert")
				}
				path := mr.Key.Path
				if len(path) == 0 || path[len(path)-1].GetId() == 0 {
					t.Errorf("allocated key should have non-zero numeric ID, got path: %v", path)
				}
			},
		},
		{
			name: "commit_time_returned",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_ct", "ct1")
				var resp datastorepb.CommitResponse
				mustPost(t, s, "commit", &datastorepb.CommitRequest{
					ProjectId: testProject,
					Mutations: []*datastorepb.Mutation{
						{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(key, nil)}},
					},
				}, &resp)
				if resp.CommitTime == nil {
					t.Error("CommitTime should be populated in CommitResponse")
				}
			},
		},
		{
			name: "read_only_transaction_rejects_writes",
			run: func(t *testing.T, s *Server) {
				var beginResp datastorepb.BeginTransactionResponse
				mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{
					ProjectId: testProject,
					TransactionOptions: &datastorepb.TransactionOptions{
						Mode: &datastorepb.TransactionOptions_ReadOnly_{
							ReadOnly: &datastorepb.TransactionOptions_ReadOnly{},
						},
					},
				}, &beginResp)
				key := dsKey("K_rotx", "w1")
				r := doPost(t, s, projectURL("commit"), &datastorepb.CommitRequest{
					ProjectId:           testProject,
					TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: beginResp.Transaction},
					Mutations: []*datastorepb.Mutation{
						{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(key, nil)}},
					},
				}, nil)
				if r.StatusCode != http.StatusPreconditionFailed {
					t.Errorf("want 412 for commit on read-only tx, got %d", r.StatusCode)
				}
			},
		},
		{
			// Two goroutines begin read-write transactions, both read the same entity,
			// the second commits first, and then the first commit must return HTTP 409 (Aborted).
			name: "occ_concurrent",
			run: func(t *testing.T, s *Server) {
				key := dsKey("K_occ_concurrent", "shared")
				upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"v": dsInt(1)}))

				var tx1, tx2 datastorepb.BeginTransactionResponse
				mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &tx1)
				mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &tx2)

				// Both transactions read the same entity (records version in read set).
				lookupInTx := func(txID []byte) {
					var lr datastorepb.LookupResponse
					mustPost(t, s, "lookup", &datastorepb.LookupRequest{
						ProjectId:   testProject,
						Keys:        []*datastorepb.Key{key},
						ReadOptions: &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_Transaction{Transaction: txID}},
					}, &lr)
				}
				lookupInTx(tx1.Transaction)
				lookupInTx(tx2.Transaction)

				// tx2 commits first - succeeds.
				mustPost(t, s, "commit", &datastorepb.CommitRequest{
					ProjectId:           testProject,
					TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: tx2.Transaction},
					Mutations: []*datastorepb.Mutation{
						{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(key, map[string]*datastorepb.Value{"v": dsInt(2)})}},
					},
				}, nil)

				// tx1 commits after tx2 - entity version changed; must be rejected.
				r := doPost(t, s, projectURL("commit"), &datastorepb.CommitRequest{
					ProjectId:           testProject,
					TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: tx1.Transaction},
					Mutations: []*datastorepb.Mutation{
						{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(key, map[string]*datastorepb.Value{"v": dsInt(3)})}},
					},
				}, nil)
				if r.StatusCode != http.StatusConflict {
					t.Errorf("occ_concurrent: want 409 (conflict), got %d", r.StatusCode)
				}
			},
		},
		{
			// Two goroutines read different entities - no OCC conflict expected.
			name: "occ_no_conflict",
			run: func(t *testing.T, s *Server) {
				k1 := dsKey("K_occ_no_conflict", "e1")
				k2 := dsKey("K_occ_no_conflict", "e2")
				upsertEntity(t, s, dsEntity(k1, map[string]*datastorepb.Value{"v": dsInt(1)}))
				upsertEntity(t, s, dsEntity(k2, map[string]*datastorepb.Value{"v": dsInt(2)}))

				var tx1, tx2 datastorepb.BeginTransactionResponse
				mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &tx1)
				mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &tx2)

				// tx1 reads e1; tx2 reads e2.
				var lr1 datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{
					ProjectId:   testProject,
					Keys:        []*datastorepb.Key{k1},
					ReadOptions: &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_Transaction{Transaction: tx1.Transaction}},
				}, &lr1)
				var lr2 datastorepb.LookupResponse
				mustPost(t, s, "lookup", &datastorepb.LookupRequest{
					ProjectId:   testProject,
					Keys:        []*datastorepb.Key{k2},
					ReadOptions: &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_Transaction{Transaction: tx2.Transaction}},
				}, &lr2)

				var wg sync.WaitGroup
				var err1, err2 int
				wg.Add(2)
				go func() {
					defer wg.Done()
					r := doPost(t, s, projectURL("commit"), &datastorepb.CommitRequest{
						ProjectId:           testProject,
						TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: tx1.Transaction},
						Mutations: []*datastorepb.Mutation{
							{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(k1, map[string]*datastorepb.Value{"v": dsInt(10)})}},
						},
					}, nil)
					err1 = r.StatusCode
				}()
				go func() {
					defer wg.Done()
					r := doPost(t, s, projectURL("commit"), &datastorepb.CommitRequest{
						ProjectId:           testProject,
						TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: tx2.Transaction},
						Mutations: []*datastorepb.Mutation{
							{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(k2, map[string]*datastorepb.Value{"v": dsInt(20)})}},
						},
					}, nil)
					err2 = r.StatusCode
				}()
				wg.Wait()

				if err1 != http.StatusOK {
					t.Errorf("occ_no_conflict: tx1 want 200, got %d", err1)
				}
				if err2 != http.StatusOK {
					t.Errorf("occ_no_conflict: tx2 want 200, got %d", err2)
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
