package query_test

import (
	"strings"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/magnus-rattlehead/hearthstore/internal/query"
)

// ── value helpers ──────────────────────────────────────────────────────────

func strVal(s string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: s}}
}

func intVal(i int64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: i}}
}

func dblVal(f float64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_DoubleValue{DoubleValue: f}}
}

func boolVal(b bool) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_BooleanValue{BooleanValue: b}}
}

func nullVal() *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_NullValue{}}
}

func arrayOf(vals ...*firestorepb.Value) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_ArrayValue{
		ArrayValue: &firestorepb.ArrayValue{Values: vals},
	}}
}

// ── query helpers ─────────────────────────────────────────────────────────

func fieldFilter(path string, op firestorepb.StructuredQuery_FieldFilter_Operator, val *firestorepb.Value) *firestorepb.StructuredQuery_Filter {
	return &firestorepb.StructuredQuery_Filter{
		FilterType: &firestorepb.StructuredQuery_Filter_FieldFilter{
			FieldFilter: &firestorepb.StructuredQuery_FieldFilter{
				Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: path},
				Op:    op,
				Value: val,
			},
		},
	}
}

func unaryFilter(path string, op firestorepb.StructuredQuery_UnaryFilter_Operator) *firestorepb.StructuredQuery_Filter {
	return &firestorepb.StructuredQuery_Filter{
		FilterType: &firestorepb.StructuredQuery_Filter_UnaryFilter{
			UnaryFilter: &firestorepb.StructuredQuery_UnaryFilter{
				OperandType: &firestorepb.StructuredQuery_UnaryFilter_Field{
					Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: path},
				},
				Op: op,
			},
		},
	}
}

func andFilter(filters ...*firestorepb.StructuredQuery_Filter) *firestorepb.StructuredQuery_Filter {
	return &firestorepb.StructuredQuery_Filter{
		FilterType: &firestorepb.StructuredQuery_Filter_CompositeFilter{
			CompositeFilter: &firestorepb.StructuredQuery_CompositeFilter{
				Op:      firestorepb.StructuredQuery_CompositeFilter_AND,
				Filters: filters,
			},
		},
	}
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

func order(field string, dir firestorepb.StructuredQuery_Direction) *firestorepb.StructuredQuery_Order {
	return &firestorepb.StructuredQuery_Order{
		Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: field},
		Direction: dir,
	}
}

// ── build helper ──────────────────────────────────────────────────────────

func build(t *testing.T, q *firestorepb.StructuredQuery, allDescendants bool) *query.Result {
	t.Helper()
	r, err := query.Build("proj", "(default)", "parent/path", allDescendants, q)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return r
}

// assertContains checks that the SQL contains all substrings.
func assertContains(t *testing.T, sql string, subs ...string) {
	t.Helper()
	for _, sub := range subs {
		if !strings.Contains(sql, sub) {
			t.Errorf("SQL missing %q\nGot: %s", sub, sql)
		}
	}
}

func assertNotContains(t *testing.T, sql string, subs ...string) {
	t.Helper()
	for _, sub := range subs {
		if strings.Contains(sql, sub) {
			t.Errorf("SQL should not contain %q\nGot: %s", sub, sql)
		}
	}
}

// ── tests ─────────────────────────────────────────────────────────────────

func TestBuild_NoFilter(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
	}, false)

	assertContains(t, r.SQL,
		"SELECT d.data",
		"FROM documents d",
		"d.project=?", "d.database=?", "d.parent_path=?", "d.collection=?", "d.deleted=0",
	)
	assertNotContains(t, r.SQL, "EXISTS", "ORDER BY", "LIMIT")

	// Args: project, database, parent_path, collection
	if len(r.Args) < 4 {
		t.Errorf("want at least 4 args, got %d", len(r.Args))
	}
}

func TestBuild_EqualityFilter_String(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
		Where: fieldFilter("color", firestorepb.StructuredQuery_FieldFilter_EQUAL, strVal("red")),
	}, false)

	assertContains(t, r.SQL, "EXISTS", "field_path=?", "value_string=?")
	// Args must include "color" and "red"
	if !argsContain(r.Args, "color") || !argsContain(r.Args, "red") {
		t.Errorf("args missing field_path or value; args=%v", r.Args)
	}
}

func TestBuild_EqualityFilter_Int(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: fieldFilter("count", firestorepb.StructuredQuery_FieldFilter_EQUAL, intVal(5)),
	}, false)

	assertContains(t, r.SQL, "EXISTS", "field_path=?", "value_int=?")
	if !argsContain(r.Args, "count") || !argsContain(r.Args, int64(5)) {
		t.Errorf("args missing field_path or value; args=%v", r.Args)
	}
}

func TestBuild_InequalityFilter_GT(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: fieldFilter("count", firestorepb.StructuredQuery_FieldFilter_GREATER_THAN, intVal(3)),
	}, false)

	// Numeric inequality emits both int and double conditions.
	assertContains(t, r.SQL, "EXISTS", "value_int >", "value_double >")
}

func TestBuild_InequalityFilter_LTE(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: fieldFilter("price", firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL, dblVal(9.99)),
	}, false)

	assertContains(t, r.SQL, "value_int <=", "value_double <=")
}

func TestBuild_RangeFilter_AND(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: andFilter(
			fieldFilter("count", firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL, intVal(5)),
			fieldFilter("count", firestorepb.StructuredQuery_FieldFilter_LESS_THAN, intVal(15)),
		),
	}, false)

	// Two separate EXISTS clauses joined with AND
	if strings.Count(r.SQL, "EXISTS") < 2 {
		t.Errorf("expected 2 EXISTS clauses for AND filter, got SQL: %s", r.SQL)
	}
	assertContains(t, r.SQL, "AND")
}

func TestBuild_NotEqualFilter(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: fieldFilter("color", firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL, strVal("red")),
	}, false)

	assertContains(t, r.SQL, "NOT EXISTS")
}

func TestBuild_InFilter(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: fieldFilter("color", firestorepb.StructuredQuery_FieldFilter_IN,
			arrayOf(strVal("red"), strVal("blue"))),
	}, false)

	assertContains(t, r.SQL, "EXISTS", "IN (", "value_string")
	// 2 placeholders for the IN list
	if strings.Count(r.SQL, "?") < 2 {
		t.Errorf("expected at least 2 ? placeholders for IN; got: %s", r.SQL)
	}
}

func TestBuild_NOT_IN_Filter(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: fieldFilter("color", firestorepb.StructuredQuery_FieldFilter_NOT_IN,
			arrayOf(strVal("red"), strVal("blue"))),
	}, false)

	assertContains(t, r.SQL, "NOT EXISTS", "IN (")
}

func TestBuild_ARRAY_CONTAINS(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: fieldFilter("tags", firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS, strVal("go")),
	}, false)

	// ARRAY_CONTAINS is same as EQUAL since each array element is a separate row.
	assertContains(t, r.SQL, "EXISTS", "field_path=?", "value_string=?")
	if !argsContain(r.Args, "go") {
		t.Errorf("args missing 'go'; args=%v", r.Args)
	}
}

func TestBuild_ARRAY_CONTAINS_ANY(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: fieldFilter("tags", firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY,
			arrayOf(strVal("go"), strVal("rust"))),
	}, false)

	assertContains(t, r.SQL, "EXISTS", "IN (")
}

func TestBuild_IS_NULL(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: unaryFilter("x", firestorepb.StructuredQuery_UnaryFilter_IS_NULL),
	}, false)

	assertContains(t, r.SQL, "EXISTS", "value_null=1")
}

func TestBuild_IS_NOT_NULL(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: unaryFilter("x", firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NULL),
	}, false)

	// IS_NOT_NULL requires the field to exist (EXISTS, not NOT EXISTS) with a non-null value row.
	assertContains(t, r.SQL, "EXISTS", "value_null IS NULL")
}

func TestBuild_IS_NAN_NeedsGoFilter(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: unaryFilter("x", firestorepb.StructuredQuery_UnaryFilter_IS_NAN),
	}, false)

	if !r.NeedsGoFilter {
		t.Error("IS_NAN should set NeedsGoFilter=true")
	}
}

func TestBuild_IS_NOT_NAN_NeedsGoFilter(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: unaryFilter("x", firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NAN),
	}, false)

	if !r.NeedsGoFilter {
		t.Error("IS_NOT_NAN should set NeedsGoFilter=true")
	}
}

func TestBuild_OrderByASC(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		OrderBy: []*firestorepb.StructuredQuery_Order{order("rank", firestorepb.StructuredQuery_ASCENDING)},
	}, false)

	assertContains(t, r.SQL, "LEFT JOIN field_index", "ORDER BY", "ASC")
	assertNotContains(t, r.SQL, "DESC")
}

func TestBuild_OrderByDESC(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		OrderBy: []*firestorepb.StructuredQuery_Order{order("rank", firestorepb.StructuredQuery_DESCENDING)},
	}, false)

	assertContains(t, r.SQL, "ORDER BY", "DESC")
}

func TestBuild_LimitOffset(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:   []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Limit:  wrapperspb.Int32(10),
		Offset: 5,
	}, false)

	assertContains(t, r.SQL, "LIMIT ?", "OFFSET ?")
	if !argsContain(r.Args, int64(10)) || !argsContain(r.Args, int64(5)) {
		t.Errorf("LIMIT/OFFSET args wrong; args=%v", r.Args)
	}
}

func TestBuild_StartAt_SingleField_Inclusive(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		OrderBy: []*firestorepb.StructuredQuery_Order{order("score", firestorepb.StructuredQuery_ASCENDING)},
		StartAt: &firestorepb.Cursor{Values: []*firestorepb.Value{intVal(20)}, Before: true},
	}, false)

	// StartAt inclusive (Before=true) → >=
	assertContains(t, r.SQL, ">=")
}

func TestBuild_StartAt_SingleField_Exclusive(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		OrderBy: []*firestorepb.StructuredQuery_Order{order("score", firestorepb.StructuredQuery_ASCENDING)},
		StartAt: &firestorepb.Cursor{Values: []*firestorepb.Value{intVal(20)}, Before: false},
	}, false)

	// StartAt exclusive (Before=false) → >
	assertContains(t, r.SQL, " > ?")
	assertNotContains(t, r.SQL, ">=")
}

func TestBuild_EndAt_SingleField_Inclusive(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		OrderBy: []*firestorepb.StructuredQuery_Order{order("score", firestorepb.StructuredQuery_ASCENDING)},
		EndAt:   &firestorepb.Cursor{Values: []*firestorepb.Value{intVal(30)}, Before: false},
	}, false)

	assertContains(t, r.SQL, "<=")
}

func TestBuild_EndAt_SingleField_Exclusive(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		OrderBy: []*firestorepb.StructuredQuery_Order{order("score", firestorepb.StructuredQuery_ASCENDING)},
		EndAt:   &firestorepb.Cursor{Values: []*firestorepb.Value{intVal(30)}, Before: true},
	}, false)

	// EndAt exclusive (Before=true) → <
	assertContains(t, r.SQL, " < ?")
	assertNotContains(t, r.SQL, "<=")
}

func TestBuild_AllDescendants(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{
			{CollectionId: "widgets", AllDescendants: true},
		},
	}, true)

	// Collection group: no parent_path constraint
	assertNotContains(t, r.SQL, "d.parent_path=?")
	assertContains(t, r.SQL, "d.collection=?")
}

func TestBuild_NameOrderBy(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		OrderBy: []*firestorepb.StructuredQuery_Order{order("__name__", firestorepb.StructuredQuery_ASCENDING)},
	}, false)

	// __name__ → d.path, no field_index JOIN for it
	assertContains(t, r.SQL, "d.path")
	// Should not have a LEFT JOIN for __name__ since it's the doc path
	if strings.Count(r.SQL, "LEFT JOIN") != 0 {
		t.Errorf("__name__ ORDER BY should not produce a field_index JOIN; SQL: %s", r.SQL)
	}
}

func TestBuild_OR_CompositeFilter(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		Where: orFilter(
			fieldFilter("color", firestorepb.StructuredQuery_FieldFilter_EQUAL, strVal("red")),
			fieldFilter("color", firestorepb.StructuredQuery_FieldFilter_EQUAL, strVal("blue")),
		),
	}, false)

	assertContains(t, r.SQL, "OR")
	if strings.Count(r.SQL, "EXISTS") < 2 {
		t.Errorf("OR filter should produce 2 EXISTS clauses; SQL: %s", r.SQL)
	}
}

func TestBuild_MultiField_OrderBy(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		OrderBy: []*firestorepb.StructuredQuery_Order{
			order("score", firestorepb.StructuredQuery_ASCENDING),
			order("name", firestorepb.StructuredQuery_ASCENDING),
		},
	}, false)

	// Two LEFT JOINs for two ORDER BY fields
	if strings.Count(r.SQL, "LEFT JOIN") != 2 {
		t.Errorf("expected 2 LEFT JOINs for 2 ORDER BY fields; SQL: %s", r.SQL)
	}
}

func TestBuild_MultiField_Cursor(t *testing.T) {
	r := build(t, &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "things"}},
		OrderBy: []*firestorepb.StructuredQuery_Order{
			order("score", firestorepb.StructuredQuery_ASCENDING),
			order("name", firestorepb.StructuredQuery_ASCENDING),
		},
		StartAt: &firestorepb.Cursor{
			Values: []*firestorepb.Value{intVal(10), strVal("alice")},
			Before: true, // inclusive
		},
	}, false)

	// Multi-field cursor expands to row-comparison: (f1>v1) OR (f1=v1 AND f2>=v2)
	assertContains(t, r.SQL, "OR")
}

// argsContain checks if args contains the given value.
func argsContain(args []any, val any) bool {
	for _, a := range args {
		if a == val {
			return true
		}
	}
	return false
}

// Ensure wrapperspb is used.
var _ = wrapperspb.Int32(0)
