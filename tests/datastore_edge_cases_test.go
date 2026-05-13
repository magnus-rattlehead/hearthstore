//go:build integration

package tests

// Edge-case integration tests for the Cloud Datastore path covering:
// ancestor queries, lookup deduplication, baseVersion conflict detection,
// create-after-delete, AVG aggregation, delete idempotency, and ReserveIds.

import (
	"net/http"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// dsChildKey builds a two-level key: parent Kind/name -> child Kind/name.
func dsChildKey(parentKind, parentName, childKind, childName string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: testProject},
		Path: []*datastorepb.Key_PathElement{
			{Kind: parentKind, IdType: &datastorepb.Key_PathElement_Name{Name: parentName}},
			{Kind: childKind, IdType: &datastorepb.Key_PathElement_Name{Name: childName}},
		},
	}
}

func TestDSEdge_AncestorQuery(t *testing.T) {
	s := newDSTestServer(t)

	// Seed: two group parents, each with two member children.
	parentA := dsNameKey("AncGroup", "groupA")
	parentB := dsNameKey("AncGroup", "groupB")
	childA1 := dsChildKey("AncGroup", "groupA", "AncMember", "m1")
	childA2 := dsChildKey("AncGroup", "groupA", "AncMember", "m2")
	childB1 := dsChildKey("AncGroup", "groupB", "AncMember", "m3")

	upsert(s, parentA, map[string]*datastorepb.Value{"name": dsStrVal("Group A")})
	upsert(s, parentB, map[string]*datastorepb.Value{"name": dsStrVal("Group B")})
	upsert(s, childA1, map[string]*datastorepb.Value{"name": dsStrVal("Member 1")})
	upsert(s, childA2, map[string]*datastorepb.Value{"name": dsStrVal("Member 2")})
	upsert(s, childB1, map[string]*datastorepb.Value{"name": dsStrVal("Member 3")})

	// Query for AncMember entities that are descendants of groupA.
	var resp datastorepb.RunQueryResponse
	if code := s.post("runQuery", &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunQueryRequest_Query{
			Query: &datastorepb.Query{
				Kind: []*datastorepb.KindExpression{{Name: "AncMember"}},
				Filter: &datastorepb.Filter{
					FilterType: &datastorepb.Filter_PropertyFilter{
						PropertyFilter: &datastorepb.PropertyFilter{
							Property: &datastorepb.PropertyReference{Name: "__key__"},
							Op:       datastorepb.PropertyFilter_HAS_ANCESTOR,
							Value: &datastorepb.Value{
								ValueType: &datastorepb.Value_KeyValue{KeyValue: parentA},
							},
						},
					},
				},
			},
		},
	}, &resp); code != http.StatusOK {
		t.Fatalf("runQuery: HTTP %d", code)
	}

	if got := len(resp.Batch.EntityResults); got != 2 {
		t.Errorf("HAS_ANCESTOR groupA: want 2 results, got %d", got)
	}
}

func TestDSEdge_Lookup_Dedup(t *testing.T) {
	s := newDSTestServer(t)

	key := dsNameKey("DedupKind", "d1")
	upsert(s, key, map[string]*datastorepb.Value{"x": dsIntVal(1)})

	// Send the same key twice in one Lookup request.
	// Note: the Datastore API spec does not define deduplication behaviour for
	// duplicate keys; this test documents our implementation's choice to
	// deduplicate, returning exactly one Found entry per unique key.
	var resp datastorepb.LookupResponse
	if code := s.post("lookup", &datastorepb.LookupRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{key, key},
	}, &resp); code != http.StatusOK {
		t.Fatalf("lookup: HTTP %d", code)
	}

	if len(resp.Found) != 1 {
		t.Errorf("lookup dedup: want 1 Found entry, got %d", len(resp.Found))
	}
}

func TestDSEdge_BaseVersion_Conflict(t *testing.T) {
	s := newDSTestServer(t)

	key := dsNameKey("VerKind", "v1")

	// Initial upsert - capture the version.
	var commitResp datastorepb.CommitResponse
	if code := s.post("commit", &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{
				Upsert: &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"v": dsIntVal(1)}},
			},
		}},
	}, &commitResp); code != http.StatusOK {
		t.Fatalf("initial upsert: HTTP %d", code)
	}
	version := commitResp.MutationResults[0].Version

	// Upsert with wrong baseVersion (current + 999) -> should conflict.
	var conflictResp datastorepb.CommitResponse
	if code := s.post("commit", &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{
				Upsert: &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"v": dsIntVal(99)}},
			},
			ConflictDetectionStrategy: &datastorepb.Mutation_BaseVersion{BaseVersion: version + 999},
		}},
	}, &conflictResp); code != http.StatusOK {
		t.Fatalf("conflict upsert: HTTP %d", code)
	}
	if !conflictResp.MutationResults[0].ConflictDetected {
		t.Error("expected ConflictDetected=true for wrong baseVersion, got false")
	}

	// Verify entity is unchanged (still v=1).
	var lookup datastorepb.LookupResponse
	s.post("lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &lookup) //nolint:errcheck
	if v := lookup.Found[0].Entity.Properties["v"].GetIntegerValue(); v != 1 {
		t.Errorf("entity after conflict: v = %d, want 1 (unchanged)", v)
	}
}

func TestDSEdge_BaseVersion_Match(t *testing.T) {
	s := newDSTestServer(t)

	key := dsNameKey("VerKind2", "v2")

	var resp1 datastorepb.CommitResponse
	s.post("commit", &datastorepb.CommitRequest{ //nolint:errcheck
		ProjectId: testProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{
				Upsert: &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"v": dsIntVal(1)}},
			},
		}},
	}, &resp1)
	version := resp1.MutationResults[0].Version

	// Upsert with correct baseVersion -> no conflict.
	var resp2 datastorepb.CommitResponse
	if code := s.post("commit", &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{
				Upsert: &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"v": dsIntVal(2)}},
			},
			ConflictDetectionStrategy: &datastorepb.Mutation_BaseVersion{BaseVersion: version},
		}},
	}, &resp2); code != http.StatusOK {
		t.Fatalf("matched-version upsert: HTTP %d", code)
	}
	if resp2.MutationResults[0].ConflictDetected {
		t.Error("expected ConflictDetected=false for correct baseVersion, got true")
	}
}

func TestDSEdge_CreateAfterDelete(t *testing.T) {
	s := newDSTestServer(t)

	key := dsNameKey("CADKind", "cad1")

	// Insert the entity.
	if code := s.post("commit", &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Insert{
				Insert: &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"v": dsIntVal(1)}},
			},
		}},
	}, &datastorepb.CommitResponse{}); code != http.StatusOK {
		t.Fatalf("insert: HTTP %d", code)
	}

	// Delete it.
	if code := s.post("commit", &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Delete{Delete: key},
		}},
	}, &datastorepb.CommitResponse{}); code != http.StatusOK {
		t.Fatalf("delete: HTTP %d", code)
	}

	// Insert the same key again - should succeed (not ALREADY_EXISTS).
	if code := s.post("commit", &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Insert{
				Insert: &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"v": dsIntVal(2)}},
			},
		}},
	}, &datastorepb.CommitResponse{}); code != http.StatusOK {
		t.Errorf("re-insert after delete: want HTTP 200, got %d", code)
	}
}

func TestDSEdge_AVG_Aggregation(t *testing.T) {
	s := newDSTestServer(t)

	for id, score := range map[string]int64{"e1": 2, "e2": 4, "e3": 6} {
		upsert(s, dsNameKey("AvgKind", id), map[string]*datastorepb.Value{"score": dsIntVal(score)})
	}

	var resp datastorepb.RunAggregationQueryResponse
	if code := s.post("runAggregationQuery", &datastorepb.RunAggregationQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{
			AggregationQuery: &datastorepb.AggregationQuery{
				QueryType: &datastorepb.AggregationQuery_NestedQuery{
					NestedQuery: &datastorepb.Query{
						Kind: []*datastorepb.KindExpression{{Name: "AvgKind"}},
					},
				},
				Aggregations: []*datastorepb.AggregationQuery_Aggregation{{
					Alias: "avg_score",
					Operator: &datastorepb.AggregationQuery_Aggregation_Avg_{
						Avg: &datastorepb.AggregationQuery_Aggregation_Avg{
							Property: &datastorepb.PropertyReference{Name: "score"},
						},
					},
				}},
			},
		},
	}, &resp); code != http.StatusOK {
		t.Fatalf("runAggregationQuery: HTTP %d", code)
	}

	results := resp.Batch.AggregationResults
	if len(results) == 0 {
		t.Fatal("no aggregation results")
	}
	avg := results[0].AggregateProperties["avg_score"].GetDoubleValue()
	if avg != 4.0 {
		t.Errorf("AVG(score) = %v, want 4.0", avg)
	}
}

func TestDSEdge_Delete_NonExistent_Idempotent(t *testing.T) {
	s := newDSTestServer(t)

	key := dsNameKey("DelKind", "never-existed")
	// Deleting a key that was never inserted should succeed (idempotent).
	if code := s.post("commit", &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Delete{Delete: key},
		}},
	}, &datastorepb.CommitResponse{}); code != http.StatusOK {
		t.Errorf("delete non-existent: want HTTP 200, got %d", code)
	}
}

func TestDSEdge_ReserveIds_NoOp(t *testing.T) {
	s := newDSTestServer(t)

	key := dsNameKey("ResKind", "r1")
	var resp datastorepb.ReserveIdsResponse
	if code := s.post("reserveIds", &datastorepb.ReserveIdsRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{key},
	}, &resp); code != http.StatusOK {
		t.Errorf("reserveIds: want HTTP 200, got %d", code)
	}
}
