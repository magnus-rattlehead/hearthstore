package server

import (
	"context"
	"fmt"
	"math"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	latlng "google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// seed creates documents in the given collection with the provided fields maps.
func seedDocs(t *testing.T, s *Server, collection string, docs map[string]map[string]*firestorepb.Value) {
	t.Helper()
	ctx := context.Background()
	for id, fields := range docs {
		_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       collectionParent(),
			CollectionId: collection,
			DocumentId:   id,
			Document:     &firestorepb.Document{Fields: fields},
		})
		if err != nil {
			t.Fatalf("seed CreateDocument %s: %v", id, err)
		}
	}
}

func runQuery(t *testing.T, s *Server, q *firestorepb.StructuredQuery) []*firestorepb.Document {
	t.Helper()
	stream := &fakeRunQueryStream{fakeServerStream: newFakeStream()}
	err := s.RunQuery(&firestorepb.RunQueryRequest{
		Parent: collectionParent(),
		QueryType: &firestorepb.RunQueryRequest_StructuredQuery{
			StructuredQuery: q,
		},
	}, stream)
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	var docs []*firestorepb.Document
	for _, r := range stream.sent {
		if r.Document != nil {
			docs = append(docs, r.Document)
		}
	}
	return docs
}

func TestRunQuery_EmptyCollection(t *testing.T) {
	s := newTestServer(t)
	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
	})
	if len(docs) != 0 {
		t.Errorf("expected 0 results, got %d", len(docs))
	}
}

func TestRunQuery_NoFilter_ReturnsAll(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"color": strVal("red")},
		"b": {"color": strVal("blue")},
		"c": {"color": strVal("green")},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
	})
	if len(docs) != 3 {
		t.Errorf("expected 3 results, got %d", len(docs))
	}
}

func TestRunQuery_EqualityFilter(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"red1":  {"color": strVal("red")},
		"red2":  {"color": strVal("red")},
		"blue1": {"color": strVal("blue")},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		Where: fieldFilter("color", firestorepb.StructuredQuery_FieldFilter_EQUAL, strVal("red")),
	})
	if len(docs) != 2 {
		t.Errorf("expected 2 red widgets, got %d", len(docs))
	}
}

func TestRunQuery_InequalityFilter(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"count": intVal(1)},
		"b": {"count": intVal(5)},
		"c": {"count": intVal(10)},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		Where: fieldFilter("count", firestorepb.StructuredQuery_FieldFilter_GREATER_THAN, intVal(3)),
	})
	if len(docs) != 2 {
		t.Errorf("expected 2 docs with count > 3, got %d", len(docs))
	}
}

func TestRunQuery_RangeFilter(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"count": intVal(1)},
		"b": {"count": intVal(5)},
		"c": {"count": intVal(9)},
		"d": {"count": intVal(15)},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		Where: &firestorepb.StructuredQuery_Filter{
			FilterType: &firestorepb.StructuredQuery_Filter_CompositeFilter{
				CompositeFilter: &firestorepb.StructuredQuery_CompositeFilter{
					Op: firestorepb.StructuredQuery_CompositeFilter_AND,
					Filters: []*firestorepb.StructuredQuery_Filter{
						fieldFilter("count", firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL, intVal(5)),
						fieldFilter("count", firestorepb.StructuredQuery_FieldFilter_LESS_THAN, intVal(15)),
					},
				},
			},
		},
	})
	if len(docs) != 2 {
		t.Errorf("expected 2 docs with 5 <= count < 15, got %d", len(docs))
	}
}

func TestRunQuery_MultipleEqualityFilters(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"match":   {"color": strVal("red"), "active": boolVal(true)},
		"no-col":  {"color": strVal("blue"), "active": boolVal(true)},
		"no-flag": {"color": strVal("red"), "active": boolVal(false)},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		Where: &firestorepb.StructuredQuery_Filter{
			FilterType: &firestorepb.StructuredQuery_Filter_CompositeFilter{
				CompositeFilter: &firestorepb.StructuredQuery_CompositeFilter{
					Op: firestorepb.StructuredQuery_CompositeFilter_AND,
					Filters: []*firestorepb.StructuredQuery_Filter{
						fieldFilter("color", firestorepb.StructuredQuery_FieldFilter_EQUAL, strVal("red")),
						fieldFilter("active", firestorepb.StructuredQuery_FieldFilter_EQUAL, boolVal(true)),
					},
				},
			},
		},
	})
	if len(docs) != 1 {
		t.Errorf("expected 1 match, got %d", len(docs))
	}
	if docs[0].Name != docName("widgets", "match") {
		t.Errorf("wrong document: %s", docs[0].Name)
	}
}

func TestRunQuery_InFilter(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"color": strVal("red")},
		"b": {"color": strVal("blue")},
		"c": {"color": strVal("green")},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		Where: &firestorepb.StructuredQuery_Filter{
			FilterType: &firestorepb.StructuredQuery_Filter_FieldFilter{
				FieldFilter: &firestorepb.StructuredQuery_FieldFilter{
					Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "color"},
					Op:    firestorepb.StructuredQuery_FieldFilter_IN,
					Value: &firestorepb.Value{
						ValueType: &firestorepb.Value_ArrayValue{
							ArrayValue: &firestorepb.ArrayValue{
								Values: []*firestorepb.Value{strVal("red"), strVal("green")},
							},
						},
					},
				},
			},
		},
	})
	if len(docs) != 2 {
		t.Errorf("expected 2 docs with color IN [red, green], got %d", len(docs))
	}
}

func TestRunQuery_Limit(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"x": intVal(1)},
		"b": {"x": intVal(2)},
		"c": {"x": intVal(3)},
		"d": {"x": intVal(4)},
		"e": {"x": intVal(5)},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		Limit: wrapperspb.Int32(3),
	})
	if len(docs) != 3 {
		t.Errorf("expected 3 results (limit=3), got %d", len(docs))
	}
}

func TestRunQuery_Offset(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"x": intVal(1)},
		"b": {"x": intVal(2)},
		"c": {"x": intVal(3)},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:   []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		Offset: 2,
	})
	if len(docs) != 1 {
		t.Errorf("expected 1 result after offset=2, got %d", len(docs))
	}
}

func TestRunQuery_OrderByAscending(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"c": {"rank": intVal(3)},
		"a": {"rank": intVal(1)},
		"b": {"rank": intVal(2)},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		OrderBy: []*firestorepb.StructuredQuery_Order{
			{
				Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: "rank"},
				Direction: firestorepb.StructuredQuery_ASCENDING,
			},
		},
	})
	if len(docs) != 3 {
		t.Fatalf("expected 3 results, got %d", len(docs))
	}
	ranks := []int64{
		docs[0].Fields["rank"].GetIntegerValue(),
		docs[1].Fields["rank"].GetIntegerValue(),
		docs[2].Fields["rank"].GetIntegerValue(),
	}
	if ranks[0] != 1 || ranks[1] != 2 || ranks[2] != 3 {
		t.Errorf("ascending order wrong: %v", ranks)
	}
}

func TestRunQuery_OrderByDescending(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"rank": intVal(1)},
		"b": {"rank": intVal(2)},
		"c": {"rank": intVal(3)},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		OrderBy: []*firestorepb.StructuredQuery_Order{
			{
				Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: "rank"},
				Direction: firestorepb.StructuredQuery_DESCENDING,
			},
		},
	})
	if len(docs) != 3 {
		t.Fatalf("expected 3 results, got %d", len(docs))
	}
	if docs[0].Fields["rank"].GetIntegerValue() != 3 {
		t.Errorf("first result rank = %d, want 3", docs[0].Fields["rank"].GetIntegerValue())
	}
}

func TestRunQuery_OnlyMatchesTargetCollection(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Documents in two different collections.
	for _, col := range []string{"widgets", "gadgets"} {
		_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       collectionParent(),
			CollectionId: col,
			DocumentId:   "item",
			Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"x": intVal(1)}},
		})
		if err != nil {
			t.Fatalf("CreateDocument %s/item: %v", col, err)
		}
	}

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
	})
	if len(docs) != 1 {
		t.Errorf("expected 1 result from 'widgets' only, got %d", len(docs))
	}
}

func TestRunQuery_StartAt_Inclusive(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"score": intVal(10)},
		"b": {"score": intVal(20)},
		"c": {"score": intVal(30)},
	})

	orderBy := []*firestorepb.StructuredQuery_Order{{
		Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: "score"},
		Direction: firestorepb.StructuredQuery_ASCENDING,
	}}

	// StartAt score=20, Before=true (inclusive: score >= 20).
	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		OrderBy: orderBy,
		StartAt: &firestorepb.Cursor{Values: []*firestorepb.Value{intVal(20)}, Before: true},
	})
	if len(docs) != 2 {
		t.Errorf("StartAt(20, inclusive): want 2 docs (20,30), got %d", len(docs))
	}
}

func TestRunQuery_StartAt_Exclusive(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"score": intVal(10)},
		"b": {"score": intVal(20)},
		"c": {"score": intVal(30)},
	})

	orderBy := []*firestorepb.StructuredQuery_Order{{
		Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: "score"},
		Direction: firestorepb.StructuredQuery_ASCENDING,
	}}

	// StartAt score=20, Before=false (exclusive: score > 20).
	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		OrderBy: orderBy,
		StartAt: &firestorepb.Cursor{Values: []*firestorepb.Value{intVal(20)}, Before: false},
	})
	if len(docs) != 1 {
		t.Errorf("StartAt(20, exclusive): want 1 doc (30), got %d", len(docs))
	}
	if len(docs) == 1 && docs[0].Fields["score"].GetIntegerValue() != 30 {
		t.Errorf("want score=30, got %d", docs[0].Fields["score"].GetIntegerValue())
	}
}

func TestRunQuery_EndAt_Inclusive(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"score": intVal(10)},
		"b": {"score": intVal(20)},
		"c": {"score": intVal(30)},
	})

	orderBy := []*firestorepb.StructuredQuery_Order{{
		Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: "score"},
		Direction: firestorepb.StructuredQuery_ASCENDING,
	}}

	// EndAt score=20, Before=false (inclusive: score <= 20).
	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		OrderBy: orderBy,
		EndAt:   &firestorepb.Cursor{Values: []*firestorepb.Value{intVal(20)}, Before: false},
	})
	if len(docs) != 2 {
		t.Errorf("EndAt(20, inclusive): want 2 docs (10,20), got %d", len(docs))
	}
}

func TestRunQuery_EndAt_Exclusive(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"score": intVal(10)},
		"b": {"score": intVal(20)},
		"c": {"score": intVal(30)},
	})

	orderBy := []*firestorepb.StructuredQuery_Order{{
		Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: "score"},
		Direction: firestorepb.StructuredQuery_ASCENDING,
	}}

	// EndAt score=20, Before=true (exclusive: score < 20).
	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		OrderBy: orderBy,
		EndAt:   &firestorepb.Cursor{Values: []*firestorepb.Value{intVal(20)}, Before: true},
	})
	if len(docs) != 1 {
		t.Errorf("EndAt(20, exclusive): want 1 doc (10), got %d", len(docs))
	}
	if len(docs) == 1 && docs[0].Fields["score"].GetIntegerValue() != 10 {
		t.Errorf("want score=10, got %d", docs[0].Fields["score"].GetIntegerValue())
	}
}

func TestRunQuery_StartAtEndAt_Range(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"score": intVal(10)},
		"b": {"score": intVal(20)},
		"c": {"score": intVal(30)},
		"d": {"score": intVal(40)},
	})

	orderBy := []*firestorepb.StructuredQuery_Order{{
		Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: "score"},
		Direction: firestorepb.StructuredQuery_ASCENDING,
	}}

	// score in [20, 30] inclusive.
	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		OrderBy: orderBy,
		StartAt: &firestorepb.Cursor{Values: []*firestorepb.Value{intVal(20)}, Before: true},
		EndAt:   &firestorepb.Cursor{Values: []*firestorepb.Value{intVal(30)}, Before: false},
	})
	if len(docs) != 2 {
		t.Errorf("range [20,30]: want 2 docs, got %d", len(docs))
	}
}

// Ensure wrapperspb import is used (it's already imported in this file).
var _ = wrapperspb.Int32(0)

// ── additional filter tests ────────────────────────────────────────────────

func nullVal() *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_NullValue{}}
}

func arrayOf(vals ...*firestorepb.Value) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_ArrayValue{
		ArrayValue: &firestorepb.ArrayValue{Values: vals},
	}}
}

func orFilter(filters ...*firestorepb.StructuredQuery_Filter) *firestorepb.StructuredQuery_Filter {
	return &firestorepb.StructuredQuery_Filter{
		FilterType: &firestorepb.StructuredQuery_Filter_CompositeFilter{
			CompositeFilter: &firestorepb.StructuredQuery_CompositeFilter{
				Op:      firestorepb.StructuredQuery_CompositeFilter_OR,
				Filters: filters,
			},
		},
	}
}

func TestRunQuery_ArrayContains(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"tags": arrayOf(strVal("go"), strVal("sql"))},
		"b": {"tags": arrayOf(strVal("python"), strVal("sql"))},
		"c": {"tags": arrayOf(strVal("rust"))},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		Where: fieldFilter("tags", firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS, strVal("sql")),
	})
	if len(docs) != 2 {
		t.Errorf("ARRAY_CONTAINS 'sql': want 2 docs, got %d", len(docs))
	}
}

func TestRunQuery_IsNull(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// One doc with x=null, one with x=1, one with no x field.
	for _, tc := range []struct {
		id     string
		fields map[string]*firestorepb.Value
	}{
		{"null-x", map[string]*firestorepb.Value{"x": nullVal()}},
		{"int-x", map[string]*firestorepb.Value{"x": intVal(1)}},
		{"no-x", map[string]*firestorepb.Value{"y": intVal(1)}},
	} {
		if _, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       collectionParent(),
			CollectionId: "things",
			DocumentId:   tc.id,
			Document:     &firestorepb.Document{Fields: tc.fields},
		}); err != nil {
			t.Fatalf("seed %s: %v", tc.id, err)
		}
	}

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: &firestorepb.StructuredQuery_Filter{
			FilterType: &firestorepb.StructuredQuery_Filter_UnaryFilter{
				UnaryFilter: &firestorepb.StructuredQuery_UnaryFilter{
					OperandType: &firestorepb.StructuredQuery_UnaryFilter_Field{
						Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "x"},
					},
					Op: firestorepb.StructuredQuery_UnaryFilter_IS_NULL,
				},
			},
		},
	})
	if len(docs) != 1 {
		t.Errorf("IS_NULL: want 1 doc (null-x), got %d", len(docs))
	}
}

func TestRunQuery_IsNotNull(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	for _, tc := range []struct {
		id     string
		fields map[string]*firestorepb.Value
	}{
		{"null-x", map[string]*firestorepb.Value{"x": nullVal()}},
		{"int-x", map[string]*firestorepb.Value{"x": intVal(1)}},
		{"str-x", map[string]*firestorepb.Value{"x": strVal("hello")}},
	} {
		if _, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       collectionParent(),
			CollectionId: "things",
			DocumentId:   tc.id,
			Document:     &firestorepb.Document{Fields: tc.fields},
		}); err != nil {
			t.Fatalf("seed %s: %v", tc.id, err)
		}
	}

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: &firestorepb.StructuredQuery_Filter{
			FilterType: &firestorepb.StructuredQuery_Filter_UnaryFilter{
				UnaryFilter: &firestorepb.StructuredQuery_UnaryFilter{
					OperandType: &firestorepb.StructuredQuery_UnaryFilter_Field{
						Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "x"},
					},
					Op: firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NULL,
				},
			},
		},
	})
	// null-x has value_null=1, so IS_NOT_NULL excludes it. int-x and str-x don't have value_null rows.
	if len(docs) != 2 {
		t.Errorf("IS_NOT_NULL: want 2 docs (int-x, str-x), got %d", len(docs))
	}
}

func TestRunQuery_NotInFilter(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"red":   {"color": strVal("red")},
		"blue":  {"color": strVal("blue")},
		"green": {"color": strVal("green")},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		Where: fieldFilter("color", firestorepb.StructuredQuery_FieldFilter_NOT_IN,
			arrayOf(strVal("red"), strVal("blue"))),
	})
	if len(docs) != 1 {
		t.Errorf("NOT_IN [red,blue]: want 1 doc (green), got %d", len(docs))
	}
	if len(docs) == 1 && docs[0].Fields["color"].GetStringValue() != "green" {
		t.Errorf("NOT_IN result should be green, got %s", docs[0].Fields["color"].GetStringValue())
	}
}

func TestRunQuery_OR_CompositeFilter(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"red":   {"color": strVal("red")},
		"blue":  {"color": strVal("blue")},
		"green": {"color": strVal("green")},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		Where: orFilter(
			fieldFilter("color", firestorepb.StructuredQuery_FieldFilter_EQUAL, strVal("red")),
			fieldFilter("color", firestorepb.StructuredQuery_FieldFilter_EQUAL, strVal("blue")),
		),
	})
	if len(docs) != 2 {
		t.Errorf("OR filter (red OR blue): want 2, got %d", len(docs))
	}
}

func TestRunQuery_AllDescendants(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Create widgets in different parent paths (simulating sub-collections).
	parents := []string{
		collectionParent(),
		fmt.Sprintf("projects/%s/databases/%s/documents/companies/acme", testProject, testDB),
		fmt.Sprintf("projects/%s/databases/%s/documents/companies/beta", testProject, testDB),
	}
	for i, parent := range parents {
		if _, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       parent,
			CollectionId: "widgets",
			DocumentId:   fmt.Sprintf("w%d", i),
			Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"x": intVal(int64(i))}},
		}); err != nil {
			t.Fatalf("seed w%d: %v", i, err)
		}
	}

	// Collection group query — allDescendants=true.
	stream := &fakeRunQueryStream{fakeServerStream: newFakeStream()}
	err := s.RunQuery(&firestorepb.RunQueryRequest{
		Parent: collectionParent(),
		QueryType: &firestorepb.RunQueryRequest_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{
					{CollectionId: "widgets", AllDescendants: true},
				},
			},
		},
	}, stream)
	if err != nil {
		t.Fatalf("RunQuery (allDescendants): %v", err)
	}
	var docs []*firestorepb.Document
	for _, r := range stream.sent {
		if r.Document != nil {
			docs = append(docs, r.Document)
		}
	}
	if len(docs) != 3 {
		t.Errorf("allDescendants: want 3 widgets across all parents, got %d", len(docs))
	}
}

func TestRunQuery_NestedFieldFilter(t *testing.T) {
	s := newTestServer(t)
	// Documents with a nested "address" map containing a "city" field.
	mapVal := func(city string) *firestorepb.Value {
		return &firestorepb.Value{
			ValueType: &firestorepb.Value_MapValue{
				MapValue: &firestorepb.MapValue{
					Fields: map[string]*firestorepb.Value{"city": strVal(city)},
				},
			},
		}
	}
	seedDocs(t, s, "people", map[string]map[string]*firestorepb.Value{
		"alice": {"address": mapVal("NYC")},
		"bob":   {"address": mapVal("LA")},
		"carol": {"address": mapVal("NYC")},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "people"}},
		Where: fieldFilter("address.city", firestorepb.StructuredQuery_FieldFilter_EQUAL, strVal("NYC")),
	})
	if len(docs) != 2 {
		t.Errorf("nested field filter: want 2 docs (NYC), got %d", len(docs))
	}
}

// TestRunQuery_NotEqual_MissingFieldExcluded verifies that documents which do not have
// the filtered field are excluded from NOT_EQUAL results (matches Firestore semantics).
func TestRunQuery_NotEqual_MissingFieldExcluded(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "items", map[string]map[string]*firestorepb.Value{
		"has-x-42":   {"x": intVal(42)},
		"has-x-99":   {"x": intVal(99)},
		"missing-x":  {"y": intVal(1)}, // no "x" field — must be excluded
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "items"}},
		Where: fieldFilter("x", firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL, intVal(42)),
	})
	// Only has-x-99 should match; missing-x must be excluded.
	if len(docs) != 1 {
		t.Errorf("NOT_EQUAL: want 1 doc (has-x-99), got %d", len(docs))
	}
	if len(docs) == 1 && docs[0].Fields["x"].GetIntegerValue() != 99 {
		t.Errorf("NOT_EQUAL result should have x=99, got x=%d", docs[0].Fields["x"].GetIntegerValue())
	}
}

// TestRunQuery_IsNotNull_MissingFieldExcluded verifies that documents without the field
// are excluded from IS_NOT_NULL results.
func TestRunQuery_IsNotNull_MissingFieldExcluded(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()
	for _, tc := range []struct {
		id     string
		fields map[string]*firestorepb.Value
	}{
		{"has-null", map[string]*firestorepb.Value{"x": nullVal()}},
		{"has-int", map[string]*firestorepb.Value{"x": intVal(5)}},
		{"missing", map[string]*firestorepb.Value{"y": intVal(1)}}, // no "x" — must be excluded
	} {
		if _, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       collectionParent(),
			CollectionId: "items",
			DocumentId:   tc.id,
			Document:     &firestorepb.Document{Fields: tc.fields},
		}); err != nil {
			t.Fatalf("seed %s: %v", tc.id, err)
		}
	}

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "items"}},
		Where: &firestorepb.StructuredQuery_Filter{
			FilterType: &firestorepb.StructuredQuery_Filter_UnaryFilter{
				UnaryFilter: &firestorepb.StructuredQuery_UnaryFilter{
					OperandType: &firestorepb.StructuredQuery_UnaryFilter_Field{
						Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "x"},
					},
					Op: firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NULL,
				},
			},
		},
	})
	// Only has-int should match; has-null has value_null=1 (excluded), missing has no row (excluded).
	if len(docs) != 1 {
		t.Errorf("IS_NOT_NULL (missing field excluded): want 1 doc (has-int), got %d", len(docs))
	}
}

// TestRunQuery_IsNotNull_EmptyArrayMatches verifies that a document with an empty array
// field is included in IS_NOT_NULL results (the field exists and is not null).
func TestRunQuery_IsNotNull_EmptyArrayMatches(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	for _, tc := range []struct {
		id     string
		fields map[string]*firestorepb.Value
	}{
		{"empty-arr", map[string]*firestorepb.Value{"tags": {ValueType: &firestorepb.Value_ArrayValue{ArrayValue: &firestorepb.ArrayValue{}}}}},
		{"no-field", map[string]*firestorepb.Value{"other": intVal(1)}},
	} {
		if _, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       collectionParent(),
			CollectionId: "things",
			DocumentId:   tc.id,
			Document:     &firestorepb.Document{Fields: tc.fields},
		}); err != nil {
			t.Fatalf("seed %s: %v", tc.id, err)
		}
	}

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: &firestorepb.StructuredQuery_Filter{
			FilterType: &firestorepb.StructuredQuery_Filter_UnaryFilter{
				UnaryFilter: &firestorepb.StructuredQuery_UnaryFilter{
					OperandType: &firestorepb.StructuredQuery_UnaryFilter_Field{
						Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "tags"},
					},
					Op: firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NULL,
				},
			},
		},
	})
	// empty-arr has tags=[] which IS_NOT_NULL; no-field has no tags field so excluded.
	if len(docs) != 1 {
		t.Errorf("IS_NOT_NULL (empty array): want 1 doc (empty-arr), got %d", len(docs))
	}
}

// TestRunQuery_MultiFieldOrderCursor tests ORDER BY two fields with a StartAt cursor.
func TestRunQuery_MultiFieldOrderCursor(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "items", map[string]map[string]*firestorepb.Value{
		"a1": {"cat": strVal("a"), "rank": intVal(1)},
		"a2": {"cat": strVal("a"), "rank": intVal(2)},
		"b1": {"cat": strVal("b"), "rank": intVal(1)},
		"b2": {"cat": strVal("b"), "rank": intVal(2)},
	})

	// ORDER BY cat ASC, rank ASC; StartAt (inclusive) at ("a", 2).
	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "items"}},
		OrderBy: []*firestorepb.StructuredQuery_Order{
			{Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "cat"}, Direction: firestorepb.StructuredQuery_ASCENDING},
			{Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "rank"}, Direction: firestorepb.StructuredQuery_ASCENDING},
		},
		StartAt: &firestorepb.Cursor{
			Values: []*firestorepb.Value{strVal("a"), intVal(2)},
			Before: true, // inclusive
		},
	})
	// Expect: a2, b1, b2 (a1 is before the cursor).
	if len(docs) != 3 {
		t.Errorf("multi-field cursor StartAt: want 3 docs, got %d", len(docs))
	}
	if len(docs) > 0 && docs[0].Fields["cat"].GetStringValue() != "a" {
		t.Errorf("first doc should be category a, got %s", docs[0].Fields["cat"].GetStringValue())
	}
	if len(docs) > 0 && docs[0].Fields["rank"].GetIntegerValue() != 2 {
		t.Errorf("first doc should be rank 2, got %d", docs[0].Fields["rank"].GetIntegerValue())
	}
}

// TestRunQuery_MixedOps creates documents, updates one, deletes another, then queries
// to verify only the correct live state is returned.
func TestRunQuery_MixedOps(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	create := func(id string, score int64) {
		t.Helper()
		if _, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent: collectionParent(), CollectionId: "players", DocumentId: id,
			Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"score": intVal(score)}},
		}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	create("alice", 10)
	create("bob", 20)
	create("carol", 30)

	// Update alice's score.
	if _, err := s.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   fmt.Sprintf("projects/%s/databases/%s/documents/players/alice", testProject, testDB),
			Fields: map[string]*firestorepb.Value{"score": intVal(50)},
		},
	}); err != nil {
		t.Fatalf("update alice: %v", err)
	}

	// Delete bob.
	if _, err := s.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{
		Name: fmt.Sprintf("projects/%s/databases/%s/documents/players/bob", testProject, testDB),
	}); err != nil {
		t.Fatalf("delete bob: %v", err)
	}

	// Query all players with score >= 20 — should return only carol (score=30) and alice (score=50).
	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "players"}},
		Where: fieldFilter("score", firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL, intVal(20)),
	})
	if len(docs) != 2 {
		t.Errorf("mixed ops query: want 2 docs (alice+carol), got %d", len(docs))
	}
	// Verify alice's updated score is reflected.
	found := false
	for _, d := range docs {
		if d.Fields["score"].GetIntegerValue() == 50 {
			found = true
		}
	}
	if !found {
		t.Error("alice's updated score=50 should appear in results")
	}
}

// ── compareValues unit tests ───────────────────────────────────────────────

func nanVal() *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_DoubleValue{DoubleValue: math.NaN()}}
}

func doubleVal(f float64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_DoubleValue{DoubleValue: f}}
}

func bytesVal(b []byte) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_BytesValue{BytesValue: b}}
}

func refVal(path string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_ReferenceValue{ReferenceValue: path}}
}

func geoVal(lat, lon float64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_GeoPointValue{
		GeoPointValue: &latlng.LatLng{Latitude: lat, Longitude: lon},
	}}
}

func arrayVal(vs ...*firestorepb.Value) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_ArrayValue{
		ArrayValue: &firestorepb.ArrayValue{Values: vs},
	}}
}

func mapVal(fields map[string]*firestorepb.Value) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_MapValue{
		MapValue: &firestorepb.MapValue{Fields: fields},
	}}
}

func TestCompareValues_NaN_LessThanNegInf(t *testing.T) {
	if got := compareValues(nanVal(), doubleVal(math.Inf(-1))); got != -1 {
		t.Errorf("NaN vs -Inf: want -1, got %d", got)
	}
}

func TestCompareValues_NaN_LessThanZero(t *testing.T) {
	if got := compareValues(nanVal(), intVal(0)); got != -1 {
		t.Errorf("NaN vs 0: want -1, got %d", got)
	}
}

func TestCompareValues_NaN_EqualNaN(t *testing.T) {
	if got := compareValues(nanVal(), nanVal()); got != 0 {
		t.Errorf("NaN vs NaN: want 0, got %d", got)
	}
}

func TestCompareValues_NaN_GreaterThanNonNaN(t *testing.T) {
	// non-NaN > NaN
	if got := compareValues(intVal(0), nanVal()); got != 1 {
		t.Errorf("0 vs NaN: want 1, got %d", got)
	}
}

func TestCompareValues_Bytes(t *testing.T) {
	a := bytesVal([]byte{0x01, 0x02})
	b := bytesVal([]byte{0x01, 0x03})
	if got := compareValues(a, b); got != -1 {
		t.Errorf("[1,2] vs [1,3]: want -1, got %d", got)
	}
	if got := compareValues(b, a); got != 1 {
		t.Errorf("[1,3] vs [1,2]: want 1, got %d", got)
	}
	if got := compareValues(a, a); got != 0 {
		t.Errorf("[1,2] vs [1,2]: want 0, got %d", got)
	}
}

func TestCompareValues_Reference(t *testing.T) {
	a := refVal("projects/p/databases/d/documents/col/aaa")
	b := refVal("projects/p/databases/d/documents/col/bbb")
	if got := compareValues(a, b); got != -1 {
		t.Errorf("ref aaa vs bbb: want -1, got %d", got)
	}
	if got := compareValues(b, a); got != 1 {
		t.Errorf("ref bbb vs aaa: want 1, got %d", got)
	}
	if got := compareValues(a, a); got != 0 {
		t.Errorf("ref aaa vs aaa: want 0, got %d", got)
	}
}

func TestCompareValues_GeoPoint_LatitudeFirst(t *testing.T) {
	north := geoVal(40.0, -74.0)
	south := geoVal(30.0, -74.0)
	if got := compareValues(south, north); got != -1 {
		t.Errorf("lat 30 vs 40: want -1, got %d", got)
	}
}

func TestCompareValues_GeoPoint_LongitudeSecond(t *testing.T) {
	west := geoVal(40.0, -75.0)
	east := geoVal(40.0, -74.0)
	if got := compareValues(west, east); got != -1 {
		t.Errorf("lon -75 vs -74: want -1, got %d", got)
	}
}

func TestCompareValues_Array_ElementByElement(t *testing.T) {
	a := arrayVal(intVal(1), intVal(2))
	b := arrayVal(intVal(1), intVal(3))
	if got := compareValues(a, b); got != -1 {
		t.Errorf("[1,2] vs [1,3]: want -1, got %d", got)
	}
}

func TestCompareValues_Array_LengthTiebreak(t *testing.T) {
	short := arrayVal(intVal(1))
	long := arrayVal(intVal(1), intVal(2))
	if got := compareValues(short, long); got != -1 {
		t.Errorf("[1] vs [1,2]: want -1, got %d", got)
	}
}

func TestCompareValues_Map_SortedKeys(t *testing.T) {
	// {a:1, b:1} vs {a:1, b:2}
	a := mapVal(map[string]*firestorepb.Value{"a": intVal(1), "b": intVal(1)})
	b := mapVal(map[string]*firestorepb.Value{"a": intVal(1), "b": intVal(2)})
	if got := compareValues(a, b); got != -1 {
		t.Errorf("map b:1 vs b:2: want -1, got %d", got)
	}
}

func TestOrderBy_NaN_SortsBefore(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "nums", map[string]map[string]*firestorepb.Value{
		"nan":  {"v": nanVal()},
		"zero": {"v": intVal(0)},
		"neg":  {"v": doubleVal(math.Inf(-1))},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "nums"}},
		OrderBy: []*firestorepb.StructuredQuery_Order{
			{Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "v"}, Direction: firestorepb.StructuredQuery_ASCENDING},
		},
	})
	if len(docs) != 3 {
		t.Fatalf("want 3 docs, got %d", len(docs))
	}
	// NaN < -Inf < 0 in Firestore ordering.
	if !math.IsNaN(docs[0].Fields["v"].GetDoubleValue()) {
		t.Errorf("first doc should be NaN, got %v", docs[0].Fields["v"])
	}
	if !math.IsInf(docs[1].Fields["v"].GetDoubleValue(), -1) {
		t.Errorf("second doc should be -Inf, got %v", docs[1].Fields["v"])
	}
}

func TestFilter_GtOnBytes(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "blobs", map[string]map[string]*firestorepb.Value{
		"a": {"data": bytesVal([]byte{0x01})},
		"b": {"data": bytesVal([]byte{0x02})},
		"c": {"data": bytesVal([]byte{0x03})},
	})

	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "blobs"}},
		Where: fieldFilter("data", firestorepb.StructuredQuery_FieldFilter_GREATER_THAN, bytesVal([]byte{0x01})),
	})
	if len(docs) != 2 {
		t.Errorf("want 2 docs (b,c) with data > 0x01, got %d", len(docs))
	}
}

func TestRunQuery_GeopointFilterEqual(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "places", map[string]map[string]*firestorepb.Value{
		"sf":  {"loc": geoVal(37.7749, -122.4194)},
		"nyc": {"loc": geoVal(40.7128, -74.0060)},
	})
	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "places"}},
		Where: fieldFilter("loc", firestorepb.StructuredQuery_FieldFilter_EQUAL, geoVal(37.7749, -122.4194)),
	})
	if len(docs) != 1 {
		t.Errorf("geopoint EQUAL: want 1 doc (sf), got %d", len(docs))
	}
}

func TestRunQuery_MapValFilterEqual(t *testing.T) {
	s := newTestServer(t)
	m1 := mapVal(map[string]*firestorepb.Value{"x": intVal(1), "y": intVal(2)})
	m2 := mapVal(map[string]*firestorepb.Value{"x": intVal(3), "y": intVal(4)})
	seedDocs(t, s, "shapes", map[string]map[string]*firestorepb.Value{
		"a": {"coord": m1},
		"b": {"coord": m2},
	})
	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "shapes"}},
		Where: fieldFilter("coord", firestorepb.StructuredQuery_FieldFilter_EQUAL, mapVal(map[string]*firestorepb.Value{"x": intVal(1), "y": intVal(2)})),
	})
	if len(docs) != 1 {
		t.Errorf("mapval EQUAL: want 1 doc (a), got %d", len(docs))
	}
}

func TestRunQuery_ArrayExactMatch(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "lists", map[string]map[string]*firestorepb.Value{
		"a": {"tags": arrayVal(strVal("x"), strVal("y"))},
		"b": {"tags": arrayVal(strVal("p"), strVal("q"))},
	})
	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "lists"}},
		Where: fieldFilter("tags", firestorepb.StructuredQuery_FieldFilter_EQUAL, arrayVal(strVal("x"), strVal("y"))),
	})
	if len(docs) != 1 {
		t.Errorf("array EQUAL: want 1 doc (a), got %d", len(docs))
	}
}

func TestRunQuery_RefFilterEqual(t *testing.T) {
	s := newTestServer(t)
	ref1 := docName("things", "t1")
	ref2 := docName("things", "t2")
	seedDocs(t, s, "items", map[string]map[string]*firestorepb.Value{
		"a": {"ref": refVal(ref1)},
		"b": {"ref": refVal(ref2)},
	})
	docs := runQuery(t, s, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "items"}},
		Where: fieldFilter("ref", firestorepb.StructuredQuery_FieldFilter_EQUAL, refVal(ref1)),
	})
	if len(docs) != 1 {
		t.Errorf("ref EQUAL: want 1 doc (a), got %d", len(docs))
	}
}

func TestRunQuery_SnapshotReadAtTime(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Create "before" and capture its exact create_time as the snapshot boundary.
	beforeDoc, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent: collectionParent(), CollectionId: "snap", DocumentId: "before",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"v": intVal(1)}},
	})
	if err != nil {
		t.Fatalf("create 'before': %v", err)
	}
	// Use "before"'s update_time as the snapshot — "after" will be created strictly later.
	snapTime := beforeDoc.UpdateTime

	// Create "after" at a later time.
	if _, err = s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent: collectionParent(), CollectionId: "snap", DocumentId: "after",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"v": intVal(2)}},
	}); err != nil {
		t.Fatalf("create 'after': %v", err)
	}

	// Query at snapshot time — should only see "before".
	stream := &fakeRunQueryStream{fakeServerStream: newFakeStream()}
	if err := s.RunQuery(&firestorepb.RunQueryRequest{
		Parent: collectionParent(),
		ConsistencySelector: &firestorepb.RunQueryRequest_ReadTime{ReadTime: snapTime},
		QueryType: &firestorepb.RunQueryRequest_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "snap"}},
			},
		},
	}, stream); err != nil {
		t.Fatalf("RunQuery with readTime: %v", err)
	}
	var docs []*firestorepb.Document
	for _, r := range stream.sent {
		if r.Document != nil {
			docs = append(docs, r.Document)
		}
	}
	if len(docs) != 1 {
		t.Errorf("snapshot_read: want 1 doc at snapshot, got %d", len(docs))
	}
	if len(docs) == 1 && docs[0].Name != docName("snap", "before") {
		t.Errorf("snapshot_read: want 'before', got %q", docs[0].Name)
	}
}
