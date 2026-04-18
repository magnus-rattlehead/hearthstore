package datastore

import (
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// dsKeyNs builds a named key in a specific namespace.
func dsKeyNs(namespace, kind, name string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{
			ProjectId:   testProject,
			NamespaceId: namespace,
		},
		Path: []*datastorepb.Key_PathElement{
			{Kind: kind, IdType: &datastorepb.Key_PathElement_Name{Name: name}},
		},
	}
}

// runQueryNs executes a RunQuery scoped to the given namespace.
func runQueryNs(t *testing.T, s *Server, namespace string, q *datastorepb.Query) *datastorepb.RunQueryResponse {
	t.Helper()
	var resp datastorepb.RunQueryResponse
	mustPost(t, s, "runQuery", &datastorepb.RunQueryRequest{
		ProjectId:   testProject,
		PartitionId: &datastorepb.PartitionId{NamespaceId: namespace},
		QueryType:   &datastorepb.RunQueryRequest_Query{Query: q},
	}, &resp)
	return &resp
}

// TestNamespace_QueryIsolation verifies that entities written to namespace "ns-a"
// are not visible when querying from namespace "ns-b" or the default namespace.
func TestNamespace_QueryIsolation(t *testing.T) {
	s := newTestDsServer(t)

	// Insert a Widget in namespace "ns-a".
	upsertEntity(t, s, dsEntity(
		dsKeyNs("ns-a", "Widget", "w1"),
		map[string]*datastorepb.Value{"color": dsStr("red")},
	))

	// Query Widgets in "ns-b" — must return zero results.
	respB := runQueryNs(t, s, "ns-b", &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "Widget"}},
	})
	if n := len(respB.Batch.EntityResults); n != 0 {
		t.Errorf("ns-b query: want 0 results, got %d", n)
	}

	// Query Widgets in default namespace ("") — must return zero results.
	respDefault := runQueryNs(t, s, "", &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "Widget"}},
	})
	if n := len(respDefault.Batch.EntityResults); n != 0 {
		t.Errorf("default ns query: want 0 results, got %d", n)
	}

	// Query Widgets in "ns-a" — must return the one entity.
	respA := runQueryNs(t, s, "ns-a", &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "Widget"}},
	})
	if n := len(respA.Batch.EntityResults); n != 1 {
		t.Errorf("ns-a query: want 1 result, got %d", n)
	}
}

// TestNamespace_LookupIsolation verifies that a key in "ns-a" is not found
// when the lookup key uses "ns-b".
func TestNamespace_LookupIsolation(t *testing.T) {
	s := newTestDsServer(t)

	upsertEntity(t, s, dsEntity(
		dsKeyNs("ns-a", "Widget", "w1"),
		map[string]*datastorepb.Value{"x": dsInt(1)},
	))

	// Lookup with "ns-b" key — should be in Missing.
	var lr datastorepb.LookupResponse
	mustPost(t, s, "lookup", &datastorepb.LookupRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{dsKeyNs("ns-b", "Widget", "w1")},
	}, &lr)
	if len(lr.Found) != 0 {
		t.Errorf("ns-b lookup: want 0 found, got %d", len(lr.Found))
	}
	if len(lr.Missing) != 1 {
		t.Errorf("ns-b lookup: want 1 missing, got %d", len(lr.Missing))
	}

	// Lookup with "ns-a" key — should be found.
	var lr2 datastorepb.LookupResponse
	mustPost(t, s, "lookup", &datastorepb.LookupRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{dsKeyNs("ns-a", "Widget", "w1")},
	}, &lr2)
	if len(lr2.Found) != 1 {
		t.Errorf("ns-a lookup: want 1 found, got %d", len(lr2.Found))
	}
}

// TestNamespace_SameKindMultipleNamespaces verifies that two entities with the
// same kind and name but different namespaces coexist independently.
func TestNamespace_SameKindMultipleNamespaces(t *testing.T) {
	s := newTestDsServer(t)

	upsertEntity(t, s, dsEntity(
		dsKeyNs("ns-a", "Widget", "w1"),
		map[string]*datastorepb.Value{"v": dsInt(10)},
	))
	upsertEntity(t, s, dsEntity(
		dsKeyNs("ns-b", "Widget", "w1"),
		map[string]*datastorepb.Value{"v": dsInt(20)},
	))

	// Each namespace has exactly one Widget.
	for _, tc := range []struct {
		ns       string
		wantVal  int64
	}{
		{"ns-a", 10},
		{"ns-b", 20},
	} {
		resp := runQueryNs(t, s, tc.ns, &datastorepb.Query{
			Kind: []*datastorepb.KindExpression{{Name: "Widget"}},
		})
		if n := len(resp.Batch.EntityResults); n != 1 {
			t.Errorf("ns=%q: want 1 entity, got %d", tc.ns, n)
			continue
		}
		got := resp.Batch.EntityResults[0].Entity.Properties["v"].GetIntegerValue()
		if got != tc.wantVal {
			t.Errorf("ns=%q: want v=%d, got %d", tc.ns, tc.wantVal, got)
		}
	}
}

// TestNamespace_FilterIsolation verifies that filter pushdown (ds_field_index)
// respects namespace boundaries — an equality filter in "ns-b" must not match
// entities indexed under "ns-a".
func TestNamespace_FilterIsolation(t *testing.T) {
	s := newTestDsServer(t)

	// Write "red" widgets in ns-a only.
	for _, name := range []string{"w1", "w2"} {
		upsertEntity(t, s, dsEntity(
			dsKeyNs("ns-a", "Widget", name),
			map[string]*datastorepb.Value{"color": dsStr("red")},
		))
	}

	// Query with equality filter in ns-b — must return 0.
	resp := runQueryNs(t, s, "ns-b", &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "Widget"}},
		Filter: &datastorepb.Filter{
			FilterType: &datastorepb.Filter_PropertyFilter{
				PropertyFilter: &datastorepb.PropertyFilter{
					Property: &datastorepb.PropertyReference{Name: "color"},
					Op:       datastorepb.PropertyFilter_EQUAL,
					Value:    dsStr("red"),
				},
			},
		},
	})
	if n := len(resp.Batch.EntityResults); n != 0 {
		t.Errorf("ns-b filter query: want 0, got %d", n)
	}

	// Same filter in ns-a — must return 2.
	resp2 := runQueryNs(t, s, "ns-a", &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "Widget"}},
		Filter: &datastorepb.Filter{
			FilterType: &datastorepb.Filter_PropertyFilter{
				PropertyFilter: &datastorepb.PropertyFilter{
					Property: &datastorepb.PropertyReference{Name: "color"},
					Op:       datastorepb.PropertyFilter_EQUAL,
					Value:    dsStr("red"),
				},
			},
		},
	})
	if n := len(resp2.Batch.EntityResults); n != 2 {
		t.Errorf("ns-a filter query: want 2, got %d", n)
	}
}
