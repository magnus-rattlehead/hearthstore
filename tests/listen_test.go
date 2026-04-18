//go:build integration

package tests

// Integration tests for the Listen (real-time watch) RPC using the official
// Firestore Go SDK. These exercise CollectionRef.Snapshots() and
// DocumentRef.Snapshots() — the Go SDK equivalents of JS onSnapshot() —
// end-to-end against a live hearthstore gRPC server.
//
// Each test subscribes before writing so the initial snapshot is consumed
// first; subsequent writes are then reflected as delta snapshots. This mirrors
// the canonical Firebase pattern and verifies that the Listen RPC fires the
// correct TargetChange events, change types (Added/Modified/Removed), and that
// multiple independent streams are isolated from each other.

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
)

// listenCtx returns a context that times out after d, suitable for blocking
// iter.Next() calls. Tests fail immediately if the deadline passes before the
// expected snapshot arrives.
func listenCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// TestListen_InitialSnapshot_EmptyCollection verifies that subscribing to an
// empty collection delivers an initial snapshot with 0 documents and a non-zero
// ReadTime, confirming the server sent TARGET_CHANGE(CURRENT).
func TestListen_InitialSnapshot_EmptyCollection(t *testing.T) {
	client := newClient(t)
	ctx := listenCtx(t, 5*time.Second)

	iter := client.Collection("listen-empty").Snapshots(ctx)
	t.Cleanup(iter.Stop)

	snap, err := iter.Next()
	if err != nil {
		t.Fatalf("initial snapshot: %v", err)
	}
	if snap.Size != 0 {
		t.Errorf("want 0 docs in empty collection, got %d", snap.Size)
	}
	if snap.ReadTime.IsZero() {
		t.Error("initial snapshot ReadTime should be set")
	}
}

// TestListen_DocumentAdded verifies that writing a document after subscribing
// delivers a delta snapshot whose Changes contains a DocumentAdded entry.
func TestListen_DocumentAdded(t *testing.T) {
	client := newClient(t)
	ctx := listenCtx(t, 10*time.Second)

	col := client.Collection("listen-added")
	iter := col.Snapshots(ctx)
	t.Cleanup(iter.Stop)

	// Consume initial (empty) snapshot.
	initial, err := iter.Next()
	if err != nil {
		t.Fatalf("initial snapshot: %v", err)
	}
	if initial.Size != 0 {
		t.Fatalf("collection should be empty initially, got %d docs", initial.Size)
	}

	// Write a document.
	if _, err := col.Doc("msg1").Set(context.Background(), map[string]any{"text": "hello"}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Delta snapshot must contain the new document.
	delta, err := iter.Next()
	if err != nil {
		t.Fatalf("delta snapshot after write: %v", err)
	}
	if delta.Size != 1 {
		t.Fatalf("want 1 doc after write, got %d", delta.Size)
	}
	if len(delta.Changes) != 1 || delta.Changes[0].Kind != firestore.DocumentAdded {
		t.Fatalf("want 1 DocumentAdded change, got %v", delta.Changes)
	}
	if text, _ := delta.Changes[0].Doc.DataAt("text"); text != "hello" {
		t.Errorf("text = %v, want hello", text)
	}
}

// TestListen_DocumentModified verifies that updating a document delivers a
// delta snapshot with DocumentModified and the new field value.
func TestListen_DocumentModified(t *testing.T) {
	client := newClient(t)
	ctx := listenCtx(t, 10*time.Second)

	col := client.Collection("listen-modified")

	// Seed a document before subscribing so it appears in the initial snapshot.
	if _, err := col.Doc("d1").Set(context.Background(), map[string]any{"v": 1}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	iter := col.Snapshots(ctx)
	t.Cleanup(iter.Stop)

	initial, err := iter.Next()
	if err != nil {
		t.Fatalf("initial snapshot: %v", err)
	}
	if initial.Size != 1 {
		t.Fatalf("want 1 doc in initial snapshot, got %d", initial.Size)
	}

	// Update the document.
	if _, err := col.Doc("d1").Set(context.Background(), map[string]any{"v": 99}); err != nil {
		t.Fatalf("update Set: %v", err)
	}

	delta, err := iter.Next()
	if err != nil {
		t.Fatalf("delta snapshot after update: %v", err)
	}
	if delta.Size != 1 {
		t.Fatalf("want 1 doc after update, got %d", delta.Size)
	}
	if len(delta.Changes) != 1 || delta.Changes[0].Kind != firestore.DocumentModified {
		t.Fatalf("want 1 DocumentModified change, got %v", delta.Changes)
	}
	if v, _ := delta.Changes[0].Doc.DataAt("v"); v != int64(99) {
		t.Errorf("v = %v, want 99", v)
	}
}

// TestListen_DocumentDeleted verifies that deleting a document delivers a
// delta snapshot with 0 documents and a DocumentRemoved change.
func TestListen_DocumentDeleted(t *testing.T) {
	client := newClient(t)
	ctx := listenCtx(t, 10*time.Second)

	col := client.Collection("listen-deleted")

	if _, err := col.Doc("d1").Set(context.Background(), map[string]any{"v": 1}); err != nil {
		t.Fatalf("seed Set: %v", err)
	}

	iter := col.Snapshots(ctx)
	t.Cleanup(iter.Stop)

	initial, err := iter.Next()
	if err != nil {
		t.Fatalf("initial snapshot: %v", err)
	}
	if initial.Size != 1 {
		t.Fatalf("want 1 doc in initial snapshot, got %d", initial.Size)
	}

	// Delete the document.
	if _, err := col.Doc("d1").Delete(context.Background()); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	delta, err := iter.Next()
	if err != nil {
		t.Fatalf("delta snapshot after delete: %v", err)
	}
	if delta.Size != 0 {
		t.Errorf("want 0 docs after delete, got %d", delta.Size)
	}
	if len(delta.Changes) != 1 || delta.Changes[0].Kind != firestore.DocumentRemoved {
		t.Errorf("want 1 DocumentRemoved change, got %v", delta.Changes)
	}
}

// TestListen_DocumentRef_Snapshots verifies DocumentRef.Snapshots(): the first
// call returns a snapshot with Exists()=false; after writing the doc, the next
// snapshot has Exists()=true with the correct fields.
func TestListen_DocumentRef_Snapshots(t *testing.T) {
	client := newClient(t)
	ctx := listenCtx(t, 10*time.Second)

	ref := client.Collection("listen-docref").Doc("watch1")
	iter := ref.Snapshots(ctx)
	t.Cleanup(iter.Stop)

	// Initial: document does not exist.
	initial, err := iter.Next()
	if err != nil {
		t.Fatalf("initial snapshot: %v", err)
	}
	if initial.Exists() {
		t.Error("document should not exist initially")
	}

	// Write the document.
	if _, err := ref.Set(context.Background(), map[string]any{"name": "Alice"}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Next snapshot: document exists.
	delta, err := iter.Next()
	if err != nil {
		t.Fatalf("snapshot after write: %v", err)
	}
	if !delta.Exists() {
		t.Fatal("document should exist after write")
	}
	if name, _ := delta.DataAt("name"); name != "Alice" {
		t.Errorf("name = %v, want Alice", name)
	}
}

// TestListen_MultipleTargets_IndependentStreams verifies that two concurrent
// Snapshots() iterators on different collections each only receive updates for
// their own collection.
func TestListen_MultipleTargets_IndependentStreams(t *testing.T) {
	client := newClient(t)
	ctx := listenCtx(t, 10*time.Second)

	colA := client.Collection("listen-multi-a")
	colB := client.Collection("listen-multi-b")

	iterA := colA.Snapshots(ctx)
	t.Cleanup(iterA.Stop)
	iterB := colB.Snapshots(ctx)
	t.Cleanup(iterB.Stop)

	// Consume both initial (empty) snapshots.
	if _, err := iterA.Next(); err != nil {
		t.Fatalf("iterA initial: %v", err)
	}
	if _, err := iterB.Next(); err != nil {
		t.Fatalf("iterB initial: %v", err)
	}

	// Write only to collection A.
	if _, err := colA.Doc("a1").Set(context.Background(), map[string]any{"x": 1}); err != nil {
		t.Fatalf("Set colA: %v", err)
	}

	// iterA receives the update.
	snapA, err := iterA.Next()
	if err != nil {
		t.Fatalf("iterA delta: %v", err)
	}
	if snapA.Size != 1 {
		t.Errorf("iterA: want 1 doc, got %d", snapA.Size)
	}

	// Write only to collection B.
	if _, err := colB.Doc("b1").Set(context.Background(), map[string]any{"y": 2}); err != nil {
		t.Fatalf("Set colB: %v", err)
	}

	// iterB receives the update.
	snapB, err := iterB.Next()
	if err != nil {
		t.Fatalf("iterB delta: %v", err)
	}
	if snapB.Size != 1 {
		t.Errorf("iterB: want 1 doc, got %d", snapB.Size)
	}

	// Verify streams are truly independent: colA still has exactly 1 doc.
	aAll, err := colA.Documents(context.Background()).GetAll()
	if err != nil {
		t.Fatalf("colA list: %v", err)
	}
	if len(aAll) != 1 {
		t.Errorf("colA should have only a1, got %d docs", len(aAll))
	}
}
