//go:build integration

package tests

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

func TestList_ListDocuments_Basic(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("list-widgets")
	seed := map[string]string{"w1": "Alpha", "w2": "Beta", "w3": "Gamma"}
	for id, name := range seed {
		if _, err := col.Doc(id).Set(ctx, map[string]any{"name": name}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	docs, err := col.Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("Documents.GetAll: %v", err)
	}
	if len(docs) != 3 {
		t.Fatalf("want 3 docs, got %d", len(docs))
	}
	// Verify values round-trip correctly.
	got := make(map[string]string, 3)
	for _, d := range docs {
		name, _ := d.DataAt("name")
		got[d.Ref.ID] = name.(string)
	}
	for id, wantName := range seed {
		if got[id] != wantName {
			t.Errorf("doc %s: name = %q, want %q", id, got[id], wantName)
		}
	}
}

func TestList_ListDocuments_Empty(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	docs, err := client.Collection("list-never-written").Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("Documents.GetAll on empty collection: %v", err)
	}
	if len(docs) != 0 {
		t.Errorf("want 0 docs, got %d", len(docs))
	}
}

func TestList_ListDocuments_MultipleDocsCountedCorrectly(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("list-counted")
	for i := 0; i < 10; i++ {
		if _, err := col.Doc(fmt.Sprintf("d%02d", i)).Set(ctx, map[string]any{"i": i}); err != nil {
			t.Fatalf("seed d%02d: %v", i, err)
		}
	}

	docs, err := col.Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(docs) != 10 {
		t.Errorf("want 10 docs, got %d", len(docs))
	}
}

func TestList_ListDocuments_FieldMask(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("list-masked")
	if _, err := col.Doc("d1").Set(ctx, map[string]any{"name": "X", "secret": "hidden", "score": int64(100)}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	docs, err := col.Select("name", "score").Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("Select query: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("want 1 doc, got %d", len(docs))
	}
	snap := docs[0]
	if _, err := snap.DataAt("secret"); err == nil {
		t.Error("'secret' field should be absent after Select projection")
	}
	if name, _ := snap.DataAt("name"); name != "X" {
		t.Errorf("name = %v, want X", name)
	}
	if score, _ := snap.DataAt("score"); score != int64(100) {
		t.Errorf("score = %v, want 100", score)
	}
}

func TestList_ListCollectionIds_Basic(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	for _, col := range []string{"lc-apples", "lc-oranges", "lc-pears"} {
		if _, err := client.Collection(col).Doc("d1").Set(ctx, map[string]any{"x": 1}); err != nil {
			t.Fatalf("seed %s: %v", col, err)
		}
	}

	iter := client.Collections(ctx)
	got := make(map[string]bool)
	for {
		ref, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("Collections.Next: %v", err)
		}
		got[ref.ID] = true
	}

	for _, want := range []string{"lc-apples", "lc-oranges", "lc-pears"} {
		if !got[want] {
			t.Errorf("missing collection ID %q", want)
		}
	}
}

func TestList_ListCollectionIds_Subcollections(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	// Create a document in a subcollection nested under cities/london.
	if _, err := client.Doc("lc-cities/london/boroughs/westminster").Set(ctx, map[string]any{"pop": 250000}); err != nil {
		t.Fatalf("seed subcollection: %v", err)
	}

	iter := client.Doc("lc-cities/london").Collections(ctx)
	var found bool
	for {
		ref, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("Collections.Next: %v", err)
		}
		if ref.ID == "boroughs" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'boroughs' in subcollection IDs of cities/london")
	}
}

func TestList_ListCollectionIds_Empty(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	iter := client.Collections(ctx)
	_, err := iter.Next()
	if err != iterator.Done {
		t.Errorf("expected iterator.Done on empty database, got %v", err)
	}
}

// Ensure the Firestore SDK's Query.Documents iterator
// correctly returns all docs ordered by document ID.
func TestList_ListDocuments_OrderedByDocumentID(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("list-ordered")
	for _, id := range []string{"c", "a", "b"} {
		if _, err := col.Doc(id).Set(ctx, map[string]any{"v": id}); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}

	docs, err := col.OrderBy(firestore.DocumentID, firestore.Asc).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("GetAll ordered: %v", err)
	}
	if len(docs) != 3 {
		t.Fatalf("want 3, got %d", len(docs))
	}
	order := []string{docs[0].Ref.ID, docs[1].Ref.ID, docs[2].Ref.ID}
	want := []string{"a", "b", "c"}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("order[%d] = %q, want %q", i, order[i], want[i])
		}
	}
}
