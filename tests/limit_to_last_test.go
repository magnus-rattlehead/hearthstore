//go:build integration

package tests

import (
	"context"
	"testing"

	"cloud.google.com/go/firestore"
)

// TestLimitToLast_Basic seeds 5 docs with scores 1-5 and expects the last 3
// (scores 3, 4, 5) in ascending order.
func TestLimitToLast_Basic(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	col := client.Collection("ltl_basic")

	for i := 1; i <= 5; i++ {
		if _, err := col.NewDoc().Set(ctx, map[string]any{"score": i}); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	docs, err := col.OrderBy("score", firestore.Asc).LimitToLast(3).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("LimitToLast query: %v", err)
	}

	if len(docs) != 3 {
		t.Fatalf("want 3 docs, got %d", len(docs))
	}
	want := []int64{3, 4, 5}
	for i, doc := range docs {
		got := doc.Data()["score"].(int64)
		if got != want[i] {
			t.Errorf("doc[%d]: want score=%d, got score=%d", i, want[i], got)
		}
	}
}

// TestLimitToLast_WithFilter seeds 10 docs (alternating active/inactive) and
// expects the last 2 active docs in ascending score order.
func TestLimitToLast_WithFilter(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	col := client.Collection("ltl_filter")

	for i := 1; i <= 10; i++ {
		active := i%2 == 0
		if _, err := col.NewDoc().Set(ctx, map[string]any{"score": i, "active": active}); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	docs, err := col.Where("active", "==", true).OrderBy("score", firestore.Asc).LimitToLast(2).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("LimitToLast query: %v", err)
	}

	if len(docs) != 2 {
		t.Fatalf("want 2 docs, got %d", len(docs))
	}
	// Active docs have scores 2,4,6,8,10 - last 2 are 8 and 10.
	want := []int64{8, 10}
	for i, doc := range docs {
		got := doc.Data()["score"].(int64)
		if got != want[i] {
			t.Errorf("doc[%d]: want score=%d, got score=%d", i, want[i], got)
		}
	}
}

// TestLimitToLast_MultiOrderBy seeds docs with (name, rank) pairs and verifies
// that LimitToLast respects multi-field ordering.
func TestLimitToLast_MultiOrderBy(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	col := client.Collection("ltl_multi")

	seeds := []map[string]any{
		{"name": "alice", "rank": 1},
		{"name": "alice", "rank": 3},
		{"name": "bob", "rank": 2},
		{"name": "bob", "rank": 4},
		{"name": "carol", "rank": 1},
	}
	for _, s := range seeds {
		if _, err := col.NewDoc().Set(ctx, s); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	docs, err := col.OrderBy("name", firestore.Asc).OrderBy("rank", firestore.Asc).LimitToLast(2).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("LimitToLast query: %v", err)
	}

	if len(docs) != 2 {
		t.Fatalf("want 2 docs, got %d", len(docs))
	}
	// Sorted: alice/1, alice/3, bob/2, bob/4, carol/1 - last 2 are bob/4, carol/1.
	type pair struct{ name string; rank int64 }
	want := []pair{{"bob", 4}, {"carol", 1}}
	for i, doc := range docs {
		data := doc.Data()
		gotName := data["name"].(string)
		gotRank := data["rank"].(int64)
		if gotName != want[i].name || gotRank != want[i].rank {
			t.Errorf("doc[%d]: want {%s,%d}, got {%s,%d}", i, want[i].name, want[i].rank, gotName, gotRank)
		}
	}
}

// TestLimitToLast_NoExplicitOrder verifies that LimitToLast without an explicit
// ORDER BY (SDK uses document ID) returns the correct last 2 docs.
func TestLimitToLast_NoExplicitOrder(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()
	col := client.Collection("ltl_norder")

	// Use fixed IDs so we know the expected document ID order.
	ids := []string{"aaa", "bbb", "ccc", "ddd", "eee"}
	for _, id := range ids {
		if _, err := col.Doc(id).Set(ctx, map[string]any{"v": 1}); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	docs, err := col.OrderBy(firestore.DocumentID, firestore.Asc).LimitToLast(2).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("LimitToLast query: %v", err)
	}

	if len(docs) != 2 {
		t.Fatalf("want 2 docs, got %d", len(docs))
	}
	want := []string{"ddd", "eee"}
	for i, doc := range docs {
		if doc.Ref.ID != want[i] {
			t.Errorf("doc[%d]: want ID=%s, got ID=%s", i, want[i], doc.Ref.ID)
		}
	}
}
