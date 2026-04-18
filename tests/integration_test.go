//go:build integration

// Package tests contains end-to-end tests that use the official Firestore Go client
// against a hearthstore server started in-process. These tests exercise the full
// gRPC transport layer — serialization, proto encoding, client middleware — exactly
// as a real backend caller would experience it.
//
// Run with:
//
//	go test -tags integration ./tests/...
package tests

import (
	"context"
	"net"
	"sort"
	"testing"

	"cloud.google.com/go/firestore"
	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"github.com/magnus-rattlehead/hearthstore/internal/server"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// aggInt extracts an integer from an AggregationResult field (the client returns *firestorepb.Value).
func aggInt(v any) int64 {
	if pv, ok := v.(*firestorepb.Value); ok {
		return pv.GetIntegerValue()
	}
	if n, ok := v.(int64); ok {
		return n
	}
	return -1
}

// aggFloat64 extracts a float64 from an AggregationResult field.
func aggFloat64(v any) float64 {
	if pv, ok := v.(*firestorepb.Value); ok {
		return pv.GetDoubleValue()
	}
	if f, ok := v.(float64); ok {
		return f
	}
	return -1
}

const testProject = "test-proj"

// newClient starts a hearthstore gRPC server on a random port and returns a
// *firestore.Client connected to it. Both are cleaned up when the test ends.
func newClient(t *testing.T) *firestore.Client {
	t.Helper()
	ctx := context.Background()

	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	grpcSrv := grpc.NewServer()
	firestorepb.RegisterFirestoreServer(grpcSrv, server.New(store))
	reflection.Register(grpcSrv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(grpcSrv.Stop)

	addr := lis.Addr().String()
	client, err := firestore.NewClient(ctx, testProject,
		option.WithEndpoint(addr),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("firestore.NewClient: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// ── basic CRUD ─────────────────────────────────────────────────────────────

func TestIntegration_SetAndGet(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("users").Doc("alice")
	if _, err := ref.Set(ctx, map[string]any{"name": "Alice", "score": 42}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	snap, err := ref.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if name, _ := snap.DataAt("name"); name != "Alice" {
		t.Errorf("name = %v, want Alice", name)
	}
	if score, _ := snap.DataAt("score"); score != int64(42) {
		t.Errorf("score = %v, want 42", score)
	}
}

func TestIntegration_Delete(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("items").Doc("x")
	if _, err := ref.Set(ctx, map[string]any{"v": 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := ref.Delete(ctx); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := ref.Get(ctx); err == nil {
		t.Error("Get after Delete: expected error (NotFound), got nil")
	}
}

func TestIntegration_Update(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("items").Doc("y")
	if _, err := ref.Set(ctx, map[string]any{"a": 1, "b": 2}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := ref.Update(ctx, []firestore.Update{{Path: "a", Value: 99}}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	snap, err := ref.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if a, _ := snap.DataAt("a"); a != int64(99) {
		t.Errorf("a = %v, want 99 after update", a)
	}
	// b should be unchanged
	if b, _ := snap.DataAt("b"); b != int64(2) {
		t.Errorf("b = %v, want 2 (unchanged)", b)
	}
}

// ── queries ────────────────────────────────────────────────────────────────

func TestIntegration_Query_Equality(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("products")
	for id, price := range map[string]int{"a": 10, "b": 20, "c": 10} {
		if _, err := col.Doc(id).Set(ctx, map[string]any{"price": price}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	docs, err := col.Where("price", "==", 10).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("price==10: want 2 docs, got %d", len(docs))
	}
}

func TestIntegration_Query_OrderByLimit(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("scores")
	for id, v := range map[string]int{"p1": 30, "p2": 10, "p3": 20} {
		if _, err := col.Doc(id).Set(ctx, map[string]any{"v": v}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	docs, err := col.OrderBy("v", firestore.Asc).Limit(2).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("want 2 docs, got %d", len(docs))
	}
	if v, _ := docs[0].DataAt("v"); v != int64(10) {
		t.Errorf("first doc v = %v, want 10", v)
	}
	if v, _ := docs[1].DataAt("v"); v != int64(20) {
		t.Errorf("second doc v = %v, want 20", v)
	}
}

func TestIntegration_Query_InFilter(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("things")
	for id, color := range map[string]string{"r": "red", "g": "green", "b": "blue"} {
		if _, err := col.Doc(id).Set(ctx, map[string]any{"color": color}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	docs, err := col.Where("color", "in", []string{"red", "blue"}).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("color in [red,blue]: want 2, got %d", len(docs))
	}
}

func TestIntegration_Query_ArrayContains(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("posts")
	col.Doc("a").Set(ctx, map[string]any{"tags": []string{"go", "grpc"}})
	col.Doc("b").Set(ctx, map[string]any{"tags": []string{"python", "grpc"}})
	col.Doc("c").Set(ctx, map[string]any{"tags": []string{"java"}})

	docs, err := col.Where("tags", "array-contains", "grpc").Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("array-contains grpc: want 2, got %d", len(docs))
	}
}

func TestIntegration_Query_NestedField(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("people")
	for id, city := range map[string]string{"alice": "NYC", "bob": "LA", "carol": "NYC"} {
		col.Doc(id).Set(ctx, map[string]any{"address": map[string]any{"city": city}})
	}

	docs, err := col.Where("address.city", "==", "NYC").Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("address.city==NYC: want 2, got %d", len(docs))
	}
}

// ── transactions ───────────────────────────────────────────────────────────

func TestIntegration_Transaction_Commit(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("bank").Doc("account")
	if _, err := ref.Set(ctx, map[string]any{"balance": 100}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		snap, err := tx.Get(ref)
		if err != nil {
			return err
		}
		bal, _ := snap.DataAt("balance")
		return tx.Update(ref, []firestore.Update{{Path: "balance", Value: bal.(int64) + 50}})
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}

	snap, _ := ref.Get(ctx)
	if bal, _ := snap.DataAt("balance"); bal != int64(150) {
		t.Errorf("balance after tx = %v, want 150", bal)
	}
}

func TestIntegration_Transaction_Rollback(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("bank").Doc("acct2")
	if _, err := ref.Set(ctx, map[string]any{"balance": 200}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Transaction that deliberately returns an error (forces rollback).
	_ = client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		tx.Update(ref, []firestore.Update{{Path: "balance", Value: 0}})
		return context.Canceled // abort
	})

	snap, _ := ref.Get(ctx)
	if bal, _ := snap.DataAt("balance"); bal != int64(200) {
		t.Errorf("balance after rollback = %v, want 200 (unchanged)", bal)
	}
}

// ── aggregation ────────────────────────────────────────────────────────────

func TestIntegration_AggregationCount(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("events")
	for i := 0; i < 5; i++ {
		col.NewDoc().Set(ctx, map[string]any{"i": i})
	}

	result, err := col.NewAggregationQuery().WithCount("n").Get(ctx)
	if err != nil {
		t.Fatalf("aggregation: %v", err)
	}
	if n := aggInt(result["n"]); n != 5 {
		t.Errorf("count = %v, want 5", result["n"])
	}
}

func TestIntegration_AggregationAvg(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("avg-scores")
	// avg(10, 20, 30, 40) = 25.0
	for id, score := range map[string]int{"s0": 10, "s1": 20, "s2": 30, "s3": 40} {
		if _, err := col.Doc(id).Set(ctx, map[string]any{"score": score}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	result, err := col.NewAggregationQuery().WithAvg("score", "avg").Get(ctx)
	if err != nil {
		t.Fatalf("aggregation avg: %v", err)
	}
	if avg := aggFloat64(result["avg"]); avg != 25.0 {
		t.Errorf("avg(score) = %v, want 25.0", avg)
	}
}

// ── mixed operations ───────────────────────────────────────────────────────

// TestIntegration_MixedOps verifies that the state seen by a query reflects a
// sequence of writes, updates, and deletes — the core correctness property.
func TestIntegration_MixedOps(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("players")
	col.Doc("alice").Set(ctx, map[string]any{"score": 10})
	col.Doc("bob").Set(ctx, map[string]any{"score": 20})
	col.Doc("carol").Set(ctx, map[string]any{"score": 30})

	// Update alice.
	col.Doc("alice").Update(ctx, []firestore.Update{{Path: "score", Value: 50}})
	// Delete bob.
	col.Doc("bob").Delete(ctx)

	// Query: score >= 20 → expect alice (50) and carol (30) only.
	docs, err := col.Where("score", ">=", 20).OrderBy("score", firestore.Asc).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("want 2 docs, got %d", len(docs))
	}

	var scores []int64
	for _, d := range docs {
		v, _ := d.DataAt("score")
		scores = append(scores, v.(int64))
	}
	sort.Slice(scores, func(i, j int) bool { return scores[i] < scores[j] })
	if scores[0] != 30 || scores[1] != 50 {
		t.Errorf("scores = %v, want [30 50]", scores)
	}
}
