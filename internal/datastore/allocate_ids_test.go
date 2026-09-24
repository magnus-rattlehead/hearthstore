package datastore

import (
	"context"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// incompleteKey creates a key with no ID or name on the last element.
func incompleteKey(kind string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: testProject},
		Path: []*datastorepb.Key_PathElement{
			{Kind: kind},
		},
	}
}

func testInsertIncompleteKeySkipsExistingAllocatedID(t *testing.T) {
	probe := newTestDsServer(t)
	var allocated datastorepb.AllocateIdsResponse
	mustPost(t, probe, "allocateIds", &datastorepb.AllocateIdsRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{incompleteKey("PlanBalanceChange")},
	}, &allocated)
	occupiedID := allocated.Keys[0].Path[0].GetId()

	s := newTestDsServer(t)
	upsertEntity(t, s, dsEntity(dsKeyID("PlanBalanceChange", occupiedID), nil))
	resp, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Insert{
				Insert: dsEntity(incompleteKey("PlanBalanceChange"), nil),
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.MutationResults[0].GetKey().GetPath()[0].GetId(); got == occupiedID || got == 0 {
		t.Fatalf("allocated ID = %d, occupied ID = %d", got, occupiedID)
	}
}

func testAllocateIDsScatteredAndReserved(t *testing.T) {
	s := newTestDsServer(t)

	ids := make([]int64, 5)
	seen := make(map[int64]bool)
	for i := range ids {
		var resp datastorepb.AllocateIdsResponse
		mustPost(t, s, "allocateIds", &datastorepb.AllocateIdsRequest{
			ProjectId: testProject,
			Keys:      []*datastorepb.Key{incompleteKey("Widget")},
		}, &resp)
		ids[i] = resp.Keys[0].Path[0].GetId()
		if ids[i] == 0 || seen[ids[i]] {
			t.Fatalf("allocated zero or duplicate ID: %v", ids[:i+1])
		}
		seen[ids[i]] = true
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] == ids[i-1]+1 {
			t.Fatalf("IDs are sequential rather than scattered: %v", ids)
		}
	}

	reserved := newTestDsServer(t)
	mustPost(t, reserved, "reserveIds", &datastorepb.ReserveIdsRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{dsKeyID("Widget", ids[0])},
	}, &datastorepb.ReserveIdsResponse{})
	var resp datastorepb.AllocateIdsResponse
	mustPost(t, reserved, "allocateIds", &datastorepb.AllocateIdsRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{incompleteKey("Widget")},
	}, &resp)
	if got := resp.Keys[0].Path[0].GetId(); got == ids[0] {
		t.Fatalf("reserved ID %d was allocated", got)
	}
}

func testAllocateIDsPerKind(t *testing.T) {
	s := newTestDsServer(t)

	var wa, ga datastorepb.AllocateIdsResponse
	mustPost(t, s, "allocateIds", &datastorepb.AllocateIdsRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{incompleteKey("Widget")},
	}, &wa)
	mustPost(t, s, "allocateIds", &datastorepb.AllocateIdsRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{incompleteKey("Gadget")},
	}, &ga)

	wid := wa.Keys[0].Path[0].GetId()
	gid := ga.Keys[0].Path[0].GetId()
	if wid == 0 || gid == 0 || wid != gid {
		t.Errorf("each kind should have an independent allocator: Widget=%d Gadget=%d", wid, gid)
	}
}

func testAllocateIDsCompleteKeyPassthrough(t *testing.T) {
	s := newTestDsServer(t)

	key := dsKeyID("Widget", 42)
	var resp datastorepb.AllocateIdsResponse
	mustPost(t, s, "allocateIds", &datastorepb.AllocateIdsRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{key},
	}, &resp)

	if len(resp.Keys) != 1 || resp.Keys[0].Path[0].GetId() != 42 {
		t.Errorf("complete key should pass through unchanged, got %v", resp.Keys)
	}
}

func TestAllocateIDs(t *testing.T) {
	t.Run("occupied_id", testInsertIncompleteKeySkipsExistingAllocatedID)
	t.Run("unique_scattered_reserved", testAllocateIDsScatteredAndReserved)
	t.Run("per_kind", testAllocateIDsPerKind)
	t.Run("complete_key", testAllocateIDsCompleteKeyPassthrough)
}
