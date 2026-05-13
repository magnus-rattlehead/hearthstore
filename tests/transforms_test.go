//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
)

// TestTransform_ServerTimestamp verifies that firestore.ServerTimestamp is replaced
// with a server-side timestamp on write.
func TestTransform_ServerTimestamp(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("logs").Doc("e1")
	before := time.Now().Add(-time.Second)

	if _, err := ref.Set(ctx, map[string]any{
		"msg":       "hello",
		"createdAt": firestore.ServerTimestamp,
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	snap, err := ref.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	raw, err := snap.DataAt("createdAt")
	if err != nil {
		t.Fatalf("DataAt createdAt: %v", err)
	}
	ts, ok := raw.(time.Time)
	if !ok {
		t.Fatalf("createdAt should be time.Time, got %T", raw)
	}
	if ts.Before(before) {
		t.Errorf("createdAt %v is before test start %v", ts, before)
	}
}

// TestTransform_ServerTimestamp_Update verifies ServerTimestamp works in an Update call.
func TestTransform_ServerTimestamp_Update(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("logs").Doc("e2")
	if _, err := ref.Set(ctx, map[string]any{"n": 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	before := time.Now().Add(-time.Second)
	if _, err := ref.Update(ctx, []firestore.Update{
		{Path: "updatedAt", Value: firestore.ServerTimestamp},
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	snap, _ := ref.Get(ctx)
	raw, err := snap.DataAt("updatedAt")
	if err != nil {
		t.Fatalf("DataAt updatedAt: %v", err)
	}
	ts := raw.(time.Time)
	if ts.Before(before) {
		t.Errorf("updatedAt %v is before test start %v", ts, before)
	}
}

// TestTransform_ArrayUnion verifies firestore.ArrayUnion adds missing elements without duplicates.
func TestTransform_ArrayUnion(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("posts").Doc("p1")
	if _, err := ref.Set(ctx, map[string]any{"tags": []string{"go", "grpc"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Union with "grpc" (duplicate) and "sqlite" (new).
	if _, err := ref.Update(ctx, []firestore.Update{
		{Path: "tags", Value: firestore.ArrayUnion("grpc", "sqlite")},
	}); err != nil {
		t.Fatalf("Update (ArrayUnion): %v", err)
	}

	snap, _ := ref.Get(ctx)
	raw, _ := snap.DataAt("tags")
	tags, ok := raw.([]any)
	if !ok {
		t.Fatalf("tags should be []any, got %T", raw)
	}
	if len(tags) != 3 {
		t.Errorf("want 3 tags (go, grpc, sqlite), got %d: %v", len(tags), tags)
	}
}

// TestTransform_ArrayUnion_EmptyBase verifies ArrayUnion works when the field doesn't exist yet.
func TestTransform_ArrayUnion_EmptyBase(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("posts").Doc("p2")
	if _, err := ref.Set(ctx, map[string]any{"x": 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if _, err := ref.Update(ctx, []firestore.Update{
		{Path: "tags", Value: firestore.ArrayUnion("a", "b")},
	}); err != nil {
		t.Fatalf("Update (ArrayUnion on missing field): %v", err)
	}

	snap, _ := ref.Get(ctx)
	raw, _ := snap.DataAt("tags")
	tags := raw.([]any)
	if len(tags) != 2 {
		t.Errorf("want 2 tags, got %d: %v", len(tags), tags)
	}
}

// TestTransform_ArrayRemove verifies firestore.ArrayRemove removes matching elements.
func TestTransform_ArrayRemove(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("posts").Doc("p3")
	if _, err := ref.Set(ctx, map[string]any{"tags": []string{"go", "grpc", "sqlite"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if _, err := ref.Update(ctx, []firestore.Update{
		{Path: "tags", Value: firestore.ArrayRemove("grpc", "sqlite")},
	}); err != nil {
		t.Fatalf("Update (ArrayRemove): %v", err)
	}

	snap, _ := ref.Get(ctx)
	raw, _ := snap.DataAt("tags")
	tags := raw.([]any)
	if len(tags) != 1 || tags[0] != "go" {
		t.Errorf("want [go], got %v", tags)
	}
}

// TestTransform_ArrayRemove_NonExistentElements verifies that removing elements not in
// the array is a no-op (no error, existing elements preserved).
func TestTransform_ArrayRemove_NonExistentElements(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("posts").Doc("p4")
	if _, err := ref.Set(ctx, map[string]any{"tags": []string{"go"}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if _, err := ref.Update(ctx, []firestore.Update{
		{Path: "tags", Value: firestore.ArrayRemove("missing")},
	}); err != nil {
		t.Fatalf("Update (ArrayRemove non-existent): %v", err)
	}

	snap, _ := ref.Get(ctx)
	raw, _ := snap.DataAt("tags")
	tags := raw.([]any)
	if len(tags) != 1 || tags[0] != "go" {
		t.Errorf("want [go] unchanged, got %v", tags)
	}
}

// TestTransform_Increment_Int verifies integer increment.
func TestTransform_Increment_Int(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("counters").Doc("c1")
	if _, err := ref.Set(ctx, map[string]any{"n": 5}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if _, err := ref.Update(ctx, []firestore.Update{
		{Path: "n", Value: firestore.Increment(3)},
	}); err != nil {
		t.Fatalf("Update (Increment): %v", err)
	}

	snap, _ := ref.Get(ctx)
	n, _ := snap.DataAt("n")
	if n != int64(8) {
		t.Errorf("n = %v (type %T), want 8", n, n)
	}
}

// TestTransform_Increment_FromMissing verifies Increment on a missing field starts from 0.
func TestTransform_Increment_FromMissing(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("counters").Doc("c2")
	if _, err := ref.Set(ctx, map[string]any{"other": "x"}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if _, err := ref.Update(ctx, []firestore.Update{
		{Path: "count", Value: firestore.Increment(10)},
	}); err != nil {
		t.Fatalf("Update (Increment on missing): %v", err)
	}

	snap, _ := ref.Get(ctx)
	count, _ := snap.DataAt("count")
	if count != int64(10) {
		t.Errorf("count = %v, want 10", count)
	}
}

// TestTransform_Increment_Float verifies float increment promotes the result to float.
func TestTransform_Increment_Float(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("counters").Doc("c3")
	if _, err := ref.Set(ctx, map[string]any{"score": 1.5}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if _, err := ref.Update(ctx, []firestore.Update{
		{Path: "score", Value: firestore.Increment(0.5)},
	}); err != nil {
		t.Fatalf("Update (Increment float): %v", err)
	}

	snap, _ := ref.Get(ctx)
	score, _ := snap.DataAt("score")
	if score != float64(2.0) {
		t.Errorf("score = %v (type %T), want 2.0", score, score)
	}
}

// TestTransform_AllInOneBatch verifies that multiple transforms can be applied in a
// single WriteBatch commit.
func TestTransform_AllInOneBatch(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("mixed").Doc("m1")
	if _, err := ref.Set(ctx, map[string]any{
		"count": int64(0),
		"tags":  []string{"a"},
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	batch := client.Batch()
	batch.Update(ref, []firestore.Update{
		{Path: "count", Value: firestore.Increment(1)},
		{Path: "tags", Value: firestore.ArrayUnion("b")},
		{Path: "lastSeen", Value: firestore.ServerTimestamp},
	})
	if _, err := batch.Commit(ctx); err != nil {
		t.Fatalf("Batch.Commit: %v", err)
	}

	snap, _ := ref.Get(ctx)

	if count, _ := snap.DataAt("count"); count != int64(1) {
		t.Errorf("count = %v, want 1", count)
	}

	raw, _ := snap.DataAt("tags")
	tags := raw.([]any)
	if len(tags) != 2 {
		t.Errorf("tags len = %d, want 2: %v", len(tags), tags)
	}

	if _, err := snap.DataAt("lastSeen"); err != nil {
		t.Errorf("lastSeen should exist after ServerTimestamp: %v", err)
	}
}

// TestTransform_NestedField_Increment verifies that Increment works on a nested field path.
func TestTransform_NestedField_Increment(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("counters").Doc("nested1")
	if _, err := ref.Set(ctx, map[string]any{
		"metadata": map[string]any{"count": int64(5)},
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if _, err := ref.Update(ctx, []firestore.Update{
		{Path: "metadata.count", Value: firestore.Increment(3)},
	}); err != nil {
		t.Fatalf("Update (nested Increment): %v", err)
	}

	snap, err := ref.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	n, err := snap.DataAt("metadata.count")
	if err != nil {
		t.Fatalf("DataAt metadata.count: %v", err)
	}
	if n != int64(8) {
		t.Errorf("metadata.count = %v (type %T), want 8", n, n)
	}
}

// TestTransform_ServerTimestamp_InTransaction verifies ServerTimestamp and Increment
// both work correctly inside a RunTransaction.
func TestTransform_ServerTimestamp_InTransaction(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("logs").Doc("tx1")
	if _, err := ref.Set(ctx, map[string]any{"n": int64(1)}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	before := time.Now().Add(-time.Second)
	if err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		return tx.Update(ref, []firestore.Update{
			{Path: "updatedAt", Value: firestore.ServerTimestamp},
			{Path: "n", Value: firestore.Increment(1)},
		})
	}); err != nil {
		t.Fatalf("RunTransaction: %v", err)
	}

	snap, _ := ref.Get(ctx)
	raw, err := snap.DataAt("updatedAt")
	if err != nil {
		t.Fatalf("DataAt updatedAt: %v", err)
	}
	ts := raw.(time.Time)
	if ts.Before(before) {
		t.Errorf("updatedAt %v is before test start %v", ts, before)
	}
	if n, _ := snap.DataAt("n"); n != int64(2) {
		t.Errorf("n = %v, want 2", n)
	}
}

// TestTransform_ArrayUnion_IntValues verifies ArrayUnion works with integer elements
// (relies on compareValues correctly comparing integer values).
func TestTransform_ArrayUnion_IntValues(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("posts").Doc("int-union")
	if _, err := ref.Set(ctx, map[string]any{"ids": []int64{1, 2, 3}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Union with 2 (duplicate) and 4 (new).
	if _, err := ref.Update(ctx, []firestore.Update{
		{Path: "ids", Value: firestore.ArrayUnion(int64(2), int64(4))},
	}); err != nil {
		t.Fatalf("Update (ArrayUnion int): %v", err)
	}

	snap, _ := ref.Get(ctx)
	raw, _ := snap.DataAt("ids")
	ids := raw.([]any)
	if len(ids) != 4 {
		t.Errorf("want 4 ids [1,2,3,4], got %d: %v", len(ids), ids)
	}
}

// TestTransform_ArrayRemove_IntValues verifies ArrayRemove works with integer elements.
func TestTransform_ArrayRemove_IntValues(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("posts").Doc("int-remove")
	if _, err := ref.Set(ctx, map[string]any{"ids": []int64{1, 2, 3}}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if _, err := ref.Update(ctx, []firestore.Update{
		{Path: "ids", Value: firestore.ArrayRemove(int64(2))},
	}); err != nil {
		t.Fatalf("Update (ArrayRemove int): %v", err)
	}

	snap, _ := ref.Get(ctx)
	raw, _ := snap.DataAt("ids")
	ids := raw.([]any)
	if len(ids) != 2 {
		t.Errorf("want [1, 3], got %v", ids)
	}
}

// TestTransform_Increment_IntDeltaOnFloat verifies that an integer delta applied to a
// float field produces a float result.
func TestTransform_Increment_IntDeltaOnFloat(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("counters").Doc("intfloat")
	if _, err := ref.Set(ctx, map[string]any{"score": 1.5}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if _, err := ref.Update(ctx, []firestore.Update{
		{Path: "score", Value: firestore.Increment(2)},
	}); err != nil {
		t.Fatalf("Update (int delta on float): %v", err)
	}

	snap, _ := ref.Get(ctx)
	score, _ := snap.DataAt("score")
	if score != float64(3.5) {
		t.Errorf("score = %v (type %T), want 3.5", score, score)
	}
}

// TestTransform_EmptyArray_IsNotNull verifies that a document with an empty array
// field is returned by IS_NOT_NULL queries (empty array != null).
func TestTransform_EmptyArray_IsNotNull(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("things")
	col.Doc("empty-arr").Set(ctx, map[string]any{"tags": []string{}})
	col.Doc("no-field").Set(ctx, map[string]any{"other": 1})
	col.Doc("null-field").Set(ctx, map[string]any{"tags": nil})

	// IS_NOT_NULL should match only the empty-array doc (tags exists and is not null).
	docs, err := col.Where("tags", "!=", nil).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("IS_NOT_NULL (empty array): want 1 doc, got %d", len(docs))
	}
	if len(docs) == 1 {
		id := docs[0].Ref.ID
		if id != "empty-arr" {
			t.Errorf("expected empty-arr, got %s", id)
		}
	}
}
