package server

import (
	"math"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func runAggregation(t *testing.T, s *Server, q *firestorepb.StructuredAggregationQuery) []*firestorepb.RunAggregationQueryResponse {
	t.Helper()
	stream := &fakeAggregationStream{fakeServerStream: newFakeStream()}
	err := s.RunAggregationQuery(&firestorepb.RunAggregationQueryRequest{
		Parent: collectionParent(),
		QueryType: &firestorepb.RunAggregationQueryRequest_StructuredAggregationQuery{
			StructuredAggregationQuery: q,
		},
	}, stream)
	if err != nil {
		t.Fatalf("RunAggregationQuery: %v", err)
	}
	return stream.sent
}

func TestRunAggregationQuery_CountEmpty(t *testing.T) {
	s := newTestServer(t)
	results := runAggregation(t, s, &firestorepb.StructuredAggregationQuery{
		QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
			},
		},
		Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{
			{
				Alias: "count",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Count_{
					Count: &firestorepb.StructuredAggregationQuery_Aggregation_Count{},
				},
			},
		},
	})
	if len(results) == 0 {
		t.Fatal("expected at least one aggregation result")
	}
	val := results[0].Result.AggregateFields["count"]
	if val.GetIntegerValue() != 0 {
		t.Errorf("count = %d, want 0", val.GetIntegerValue())
	}
}

func TestRunAggregationQuery_CountAll(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"x": intVal(1)},
		"b": {"x": intVal(2)},
		"c": {"x": intVal(3)},
	})

	results := runAggregation(t, s, &firestorepb.StructuredAggregationQuery{
		QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
			},
		},
		Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{
			{
				Alias: "total",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Count_{
					Count: &firestorepb.StructuredAggregationQuery_Aggregation_Count{},
				},
			},
		},
	})
	if len(results) == 0 {
		t.Fatal("expected aggregation result")
	}
	val := results[0].Result.AggregateFields["total"]
	if val.GetIntegerValue() != 3 {
		t.Errorf("count = %d, want 3", val.GetIntegerValue())
	}
}

func TestRunAggregationQuery_CountWithFilter(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"active": boolVal(true)},
		"b": {"active": boolVal(true)},
		"c": {"active": boolVal(false)},
	})

	results := runAggregation(t, s, &firestorepb.StructuredAggregationQuery{
		QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
				Where: fieldFilter("active", firestorepb.StructuredQuery_FieldFilter_EQUAL, boolVal(true)),
			},
		},
		Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{
			{
				Alias: "active_count",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Count_{
					Count: &firestorepb.StructuredAggregationQuery_Aggregation_Count{},
				},
			},
		},
	})
	if len(results) == 0 {
		t.Fatal("expected aggregation result")
	}
	val := results[0].Result.AggregateFields["active_count"]
	if val.GetIntegerValue() != 2 {
		t.Errorf("active_count = %d, want 2", val.GetIntegerValue())
	}
}

func TestRunAggregationQuery_Sum(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"score": intVal(10)},
		"b": {"score": intVal(20)},
		"c": {"score": intVal(30)},
	})

	results := runAggregation(t, s, &firestorepb.StructuredAggregationQuery{
		QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
			},
		},
		Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{
			{
				Alias: "total",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Sum_{
					Sum: &firestorepb.StructuredAggregationQuery_Aggregation_Sum{
						Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "score"},
					},
				},
			},
		},
	})
	if len(results) == 0 {
		t.Fatal("expected result")
	}
	if total := results[0].Result.AggregateFields["total"].GetIntegerValue(); total != 60 {
		t.Errorf("sum = %d, want 60", total)
	}
}

func TestRunAggregationQuery_Avg(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"score": intVal(10)},
		"b": {"score": intVal(20)},
		"c": {"score": intVal(30)},
	})

	results := runAggregation(t, s, &firestorepb.StructuredAggregationQuery{
		QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
			},
		},
		Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{
			{
				Alias: "avg",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Avg_{
					Avg: &firestorepb.StructuredAggregationQuery_Aggregation_Avg{
						Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "score"},
					},
				},
			},
		},
	})
	if len(results) == 0 {
		t.Fatal("expected result")
	}
	if avg := results[0].Result.AggregateFields["avg"].GetDoubleValue(); avg != 20.0 {
		t.Errorf("avg = %f, want 20.0", avg)
	}
}

func TestRunAggregationQuery_AvgEmptyReturnsNull(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"other": intVal(1)},
	})

	results := runAggregation(t, s, &firestorepb.StructuredAggregationQuery{
		QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
			},
		},
		Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{
			{
				Alias: "avg",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Avg_{
					Avg: &firestorepb.StructuredAggregationQuery_Aggregation_Avg{
						Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "missing"},
					},
				},
			},
		},
	})
	if len(results) == 0 {
		t.Fatal("expected result")
	}
	v := results[0].Result.AggregateFields["avg"]
	if _, ok := v.ValueType.(*firestorepb.Value_NullValue); !ok {
		t.Errorf("avg of missing field should be null, got %T", v.ValueType)
	}
}

// TestRunAggregationQuery_CountUpTo verifies that count respects the up_to limit.
func TestRunAggregationQuery_CountUpTo(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "widgets", map[string]map[string]*firestorepb.Value{
		"a": {"x": intVal(1)},
		"b": {"x": intVal(2)},
		"c": {"x": intVal(3)},
		"d": {"x": intVal(4)},
		"e": {"x": intVal(5)},
	})

	results := runAggregation(t, s, &firestorepb.StructuredAggregationQuery{
		QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "widgets"}},
			},
		},
		Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{
			{
				Alias: "count",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Count_{
					Count: &firestorepb.StructuredAggregationQuery_Aggregation_Count{
						UpTo: &wrapperspb.Int64Value{Value: 3},
					},
				},
			},
		},
	})
	if len(results) == 0 {
		t.Fatal("expected aggregation result")
	}
	if n := results[0].Result.AggregateFields["count"].GetIntegerValue(); n != 3 {
		t.Errorf("count with up_to=3: want 3, got %d", n)
	}
}

// TestRunAggregationQuery_MultipleAggregations verifies count+sum+avg can be computed
// in one request (single streaming pass, no docs slice).
func TestRunAggregationQuery_MultipleAggregations(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "items", map[string]map[string]*firestorepb.Value{
		"p": {"score": intVal(10)},
		"q": {"score": intVal(30)},
	})

	results := runAggregation(t, s, &firestorepb.StructuredAggregationQuery{
		QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "items"}},
			},
		},
		Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{
			{
				Alias: "n",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Count_{
					Count: &firestorepb.StructuredAggregationQuery_Aggregation_Count{},
				},
			},
			{
				Alias: "total",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Sum_{
					Sum: &firestorepb.StructuredAggregationQuery_Aggregation_Sum{
						Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "score"},
					},
				},
			},
			{
				Alias: "mean",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Avg_{
					Avg: &firestorepb.StructuredAggregationQuery_Aggregation_Avg{
						Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "score"},
					},
				},
			},
		},
	})
	if len(results) == 0 {
		t.Fatal("expected aggregation result")
	}
	fields := results[0].Result.AggregateFields
	if n := fields["n"].GetIntegerValue(); n != 2 {
		t.Errorf("count = %d, want 2", n)
	}
	if total := fields["total"].GetIntegerValue(); total != 40 {
		t.Errorf("sum = %d, want 40", total)
	}
	if mean := fields["mean"].GetDoubleValue(); mean != 20.0 {
		t.Errorf("avg = %f, want 20.0", mean)
	}
}

// TestRunAggregationQuery_SumMixedIntDouble verifies that sum promotes to double
// when documents store the field as different numeric types.
func TestRunAggregationQuery_SumMixedIntDouble(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "items", map[string]map[string]*firestorepb.Value{
		"int-doc":    {"val": intVal(10)},
		"double-doc": {"val": &firestorepb.Value{ValueType: &firestorepb.Value_DoubleValue{DoubleValue: 0.5}}},
	})

	results := runAggregation(t, s, &firestorepb.StructuredAggregationQuery{
		QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "items"}},
			},
		},
		Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{
			{
				Alias: "total",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Sum_{
					Sum: &firestorepb.StructuredAggregationQuery_Aggregation_Sum{
						Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "val"},
					},
				},
			},
		},
	})
	if len(results) == 0 {
		t.Fatal("expected aggregation result")
	}
	v := results[0].Result.AggregateFields["total"]
	// Mixed int+double → result must be a double value.
	dv, ok := v.ValueType.(*firestorepb.Value_DoubleValue)
	if !ok {
		t.Fatalf("mixed int+double sum should be DoubleValue, got %T", v.ValueType)
	}
	if dv.DoubleValue != 10.5 {
		t.Errorf("sum = %f, want 10.5", dv.DoubleValue)
	}
}

func TestRunAggregationQuery_SumWithNaN(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "items", map[string]map[string]*firestorepb.Value{
		"a": {"val": intVal(5)},
		"b": {"val": &firestorepb.Value{ValueType: &firestorepb.Value_DoubleValue{DoubleValue: math.NaN()}}},
	})
	results := runAggregation(t, s, &firestorepb.StructuredAggregationQuery{
		QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "items"}},
			},
		},
		Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{
			{
				Alias: "total",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Sum_{
					Sum: &firestorepb.StructuredAggregationQuery_Aggregation_Sum{
						Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "val"},
					},
				},
			},
		},
	})
	if len(results) == 0 {
		t.Fatal("expected aggregation result")
	}
	v := results[0].Result.AggregateFields["total"]
	dv, ok := v.ValueType.(*firestorepb.Value_DoubleValue)
	if !ok {
		t.Fatalf("sum with NaN should be DoubleValue, got %T", v.ValueType)
	}
	if !math.IsNaN(dv.DoubleValue) {
		t.Errorf("sum with NaN field should be NaN, got %f", dv.DoubleValue)
	}
}

func TestRunAggregationQuery_AvgWithNaN(t *testing.T) {
	s := newTestServer(t)
	seedDocs(t, s, "items", map[string]map[string]*firestorepb.Value{
		"a": {"val": doubleVal(1.0)},
		"b": {"val": &firestorepb.Value{ValueType: &firestorepb.Value_DoubleValue{DoubleValue: math.NaN()}}},
	})
	results := runAggregation(t, s, &firestorepb.StructuredAggregationQuery{
		QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "items"}},
			},
		},
		Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{
			{
				Alias: "avg",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Avg_{
					Avg: &firestorepb.StructuredAggregationQuery_Aggregation_Avg{
						Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "val"},
					},
				},
			},
		},
	})
	if len(results) == 0 {
		t.Fatal("expected aggregation result")
	}
	v := results[0].Result.AggregateFields["avg"]
	dv, ok := v.ValueType.(*firestorepb.Value_DoubleValue)
	if !ok {
		t.Fatalf("avg with NaN should be DoubleValue (NaN), got %T", v.ValueType)
	}
	if !math.IsNaN(dv.DoubleValue) {
		t.Errorf("avg with NaN field should be NaN, got %f", dv.DoubleValue)
	}
}
