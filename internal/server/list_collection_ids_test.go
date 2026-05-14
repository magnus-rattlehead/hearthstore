package server

import (
	"context"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func TestListCollectionIds_Empty(t *testing.T) {
	s := newTestServer(t)
	resp, err := s.ListCollectionIds(context.Background(), &firestorepb.ListCollectionIdsRequest{
		Parent: docName("things", "thing-1"),
	})
	if err != nil {
		t.Fatalf("ListCollectionIds: %v", err)
	}
	if len(resp.CollectionIds) != 0 {
		t.Errorf("expected 0 collection IDs, got %d", len(resp.CollectionIds))
	}
}

func TestListCollectionIds_ReturnsSubcollections(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Create documents in two subcollections under things/parent.
	// Parent is the document path; CollectionId identifies the subcollection.
	for _, sub := range []string{"parts", "tags"} {
		_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       docName("things", "parent"),
			CollectionId: sub,
			DocumentId:   "item-1",
			Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"x": intVal(1)}},
		})
		if err != nil {
			t.Fatalf("CreateDocument %s: %v", sub, err)
		}
	}

	resp, err := s.ListCollectionIds(ctx, &firestorepb.ListCollectionIdsRequest{
		Parent: docName("things", "parent"),
	})
	if err != nil {
		t.Fatalf("ListCollectionIds: %v", err)
	}
	if len(resp.CollectionIds) != 2 {
		t.Errorf("expected 2 collection IDs, got %d: %v", len(resp.CollectionIds), resp.CollectionIds)
	}

	ids := map[string]bool{}
	for _, id := range resp.CollectionIds {
		ids[id] = true
	}
	for _, want := range []string{"parts", "tags"} {
		if !ids[want] {
			t.Errorf("missing collection ID %q", want)
		}
	}
}

func TestListCollectionIds_DoesNotReturnRootCollections(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Root collection "widgets" should not appear when listing a different document's subcollections.
	_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
		DocumentId:   "w1",
		Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"x": intVal(1)}},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	resp, err := s.ListCollectionIds(ctx, &firestorepb.ListCollectionIdsRequest{
		Parent: docName("things", "unrelated"),
	})
	if err != nil {
		t.Fatalf("ListCollectionIds: %v", err)
	}
	if len(resp.CollectionIds) != 0 {
		t.Errorf("expected 0 IDs for unrelated doc, got %v", resp.CollectionIds)
	}
}
