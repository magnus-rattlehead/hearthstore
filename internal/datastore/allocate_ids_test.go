package datastore

import (
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

func TestAllocateIds_Unique(t *testing.T) {
	s := newTestDsServer(t)

	var r1, r2 datastorepb.AllocateIdsResponse
	mustPost(t, s, "allocateIds", &datastorepb.AllocateIdsRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{incompleteKey("Widget")},
	}, &r1)
	mustPost(t, s, "allocateIds", &datastorepb.AllocateIdsRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{incompleteKey("Widget")},
	}, &r2)

	id1 := r1.Keys[0].Path[0].GetId()
	id2 := r2.Keys[0].Path[0].GetId()
	if id1 == id2 {
		t.Errorf("expected distinct IDs, both got %d", id1)
	}
	if id1 == 0 || id2 == 0 {
		t.Errorf("IDs should be non-zero, got %d %d", id1, id2)
	}
}

func TestAllocateIds_Monotonic(t *testing.T) {
	s := newTestDsServer(t)

	ids := make([]int64, 5)
	for i := range ids {
		var resp datastorepb.AllocateIdsResponse
		mustPost(t, s, "allocateIds", &datastorepb.AllocateIdsRequest{
			ProjectId: testProject,
			Keys:      []*datastorepb.Key{incompleteKey("Widget")},
		}, &resp)
		ids[i] = resp.Keys[0].Path[0].GetId()
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Errorf("IDs not monotonically increasing: %v", ids)
			break
		}
	}
}

func TestAllocateIds_PerKind(t *testing.T) {
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
	// Both should start at 1 (independent sequences per kind).
	if wid != 1 || gid != 1 {
		t.Errorf("each kind should start at 1: Widget=%d Gadget=%d", wid, gid)
	}
}

func TestAllocateIds_CompleteKeyPassthrough(t *testing.T) {
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
