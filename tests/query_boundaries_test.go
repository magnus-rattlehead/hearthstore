//go:build integration

package tests

// Integration tests for query cursor boundary semantics (startAt/startAfter,
// endAt/endBefore), unary IS_NAN / IS_NOT_NAN filters, and empty-collection
// aggregation behaviour.

import (
	"context"
	"math"
	"testing"

	"cloud.google.com/go/firestore"
	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// seedScores creates docs a1(score=1), a2(score=2), a3(score=3) in collection col.
func seedScores(t *testing.T, col *firestore.CollectionRef, ctx context.Context) {
	t.Helper()
	for id, score := range map[string]int{"a1": 1, "a2": 2, "a3": 3} {
		if _, err := col.Doc(id).Set(ctx, map[string]any{"score": score}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
}

// scoreCounts returns the distinct score values from the given docs.
func scoreValues(docs []*firestore.DocumentSnapshot) []int64 {
	vals := make([]int64, 0, len(docs))
	for _, d := range docs {
		v, _ := d.DataAt("score")
		vals = append(vals, v.(int64))
	}
	return vals
}

func TestBoundary_StartAt_Inclusive(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	col := client.Collection("bound-start-at")
	seedScores(t, col, ctx)

	docs, err := col.OrderBy("score", firestore.Asc).StartAt(2).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	vals := scoreValues(docs)
	// StartAt(2) is inclusive: should return score=2 AND score=3.
	if len(docs) != 2 {
		t.Fatalf("StartAt(2): want 2 docs, got %d (scores: %v)", len(docs), vals)
	}
	if vals[0] != 2 || vals[1] != 3 {
		t.Errorf("StartAt(2): want scores [2 3], got %v", vals)
	}
}

func TestBoundary_StartAfter_Exclusive(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	col := client.Collection("bound-start-after")
	seedScores(t, col, ctx)

	docs, err := col.OrderBy("score", firestore.Asc).StartAfter(2).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	vals := scoreValues(docs)
	// StartAfter(2) is exclusive: should return only score=3.
	if len(docs) != 1 {
		t.Fatalf("StartAfter(2): want 1 doc, got %d (scores: %v)", len(docs), vals)
	}
	if vals[0] != 3 {
		t.Errorf("StartAfter(2): want score 3, got %d", vals[0])
	}
}

func TestBoundary_EndAt_Inclusive(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	col := client.Collection("bound-end-at")
	seedScores(t, col, ctx)

	docs, err := col.OrderBy("score", firestore.Asc).EndAt(2).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	vals := scoreValues(docs)
	// EndAt(2) is inclusive: should return score=1 AND score=2.
	if len(docs) != 2 {
		t.Fatalf("EndAt(2): want 2 docs, got %d (scores: %v)", len(docs), vals)
	}
	if vals[0] != 1 || vals[1] != 2 {
		t.Errorf("EndAt(2): want scores [1 2], got %v", vals)
	}
}

func TestBoundary_EndBefore_Exclusive(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	col := client.Collection("bound-end-before")
	seedScores(t, col, ctx)

	docs, err := col.OrderBy("score", firestore.Asc).EndBefore(2).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	vals := scoreValues(docs)
	// EndBefore(2) is exclusive: should return only score=1.
	if len(docs) != 1 {
		t.Fatalf("EndBefore(2): want 1 doc, got %d (scores: %v)", len(docs), vals)
	}
	if vals[0] != 1 {
		t.Errorf("EndBefore(2): want score 1, got %d", vals[0])
	}
}

func TestBoundary_StartAt_EndAt_Range(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	col := client.Collection("bound-range")
	seedScores(t, col, ctx)

	docs, err := col.OrderBy("score", firestore.Asc).StartAt(1).EndAt(2).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	vals := scoreValues(docs)
	// StartAt(1).EndAt(2): inclusive on both ends -> [1, 2].
	if len(docs) != 2 {
		t.Fatalf("StartAt(1).EndAt(2): want 2 docs, got %d (scores: %v)", len(docs), vals)
	}
	if vals[0] != 1 || vals[1] != 2 {
		t.Errorf("StartAt(1).EndAt(2): want [1 2], got %v", vals)
	}
}

func TestBoundary_IsNaN(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	col := client.Collection("bound-nan")

	if _, err := col.Doc("nan-doc").Set(ctx, map[string]any{"score": math.NaN()}); err != nil {
		t.Fatalf("seed nan: %v", err)
	}
	if _, err := col.Doc("real-doc").Set(ctx, map[string]any{"score": 1.0}); err != nil {
		t.Fatalf("seed real: %v", err)
	}

	// The Firestore SDK converts Where("score", "==", NaN) to IS_NAN unary filter.
	docs, err := col.Where("score", "==", math.NaN()).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("IS NAN query: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("IS NAN: want 1 doc, got %d", len(docs))
	}
	if docs[0].Ref.ID != "nan-doc" {
		t.Errorf("IS NAN: want nan-doc, got %s", docs[0].Ref.ID)
	}
}

func TestBoundary_IsNotNaN(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	col := client.Collection("bound-not-nan")

	if _, err := col.Doc("nan-doc").Set(ctx, map[string]any{"score": math.NaN()}); err != nil {
		t.Fatalf("seed nan: %v", err)
	}
	if _, err := col.Doc("real-doc").Set(ctx, map[string]any{"score": 1.0}); err != nil {
		t.Fatalf("seed real: %v", err)
	}

	// The Firestore SDK converts Where("score", "!=", NaN) to IS_NOT_NAN unary filter.
	docs, err := col.Where("score", "!=", math.NaN()).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("IS NOT NAN query: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("IS NOT NAN: want 1 doc, got %d", len(docs))
	}
	if docs[0].Ref.ID != "real-doc" {
		t.Errorf("IS NOT NAN: want real-doc, got %s", docs[0].Ref.ID)
	}
}

func TestBoundary_Aggregation_Count_Empty(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("bound-agg-empty-count")
	result, err := col.NewAggregationQuery().WithCount("n").Get(ctx)
	if err != nil {
		t.Fatalf("aggregation count on empty: %v", err)
	}
	if n := aggInt(result["n"]); n != 0 {
		t.Errorf("COUNT on empty collection = %d, want 0", n)
	}
}

func TestBoundary_Aggregation_Sum_Empty(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("bound-agg-empty-sum")
	result, err := col.NewAggregationQuery().WithSum("score", "s").Get(ctx)
	if err != nil {
		t.Fatalf("aggregation sum on empty: %v", err)
	}
	if s := aggInt(result["s"]); s != 0 {
		t.Errorf("SUM on empty collection = %d, want 0", s)
	}
}

func TestBoundary_Aggregation_Avg_Empty(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("bound-agg-empty-avg")
	result, err := col.NewAggregationQuery().WithAvg("score", "a").Get(ctx)
	if err != nil {
		t.Fatalf("aggregation avg on empty: %v", err)
	}
	// AVG on empty collection should return null.
	// The SDK may represent this as Go nil or as a *firestorepb.Value with NullValue type.
	switch v := result["a"].(type) {
	case nil:
		// OK - explicit nil means null
	case *firestorepb.Value:
		if _, isNull := v.GetValueType().(*firestorepb.Value_NullValue); !isNull {
			t.Errorf("AVG on empty collection = %v, want null", v)
		}
	default:
		t.Errorf("AVG on empty collection = %v (%T), want null", result["a"], result["a"])
	}
}
