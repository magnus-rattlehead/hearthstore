package server

import (
	"context"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func TestListDocuments_EmptyCollection(t *testing.T) {
	s := newTestServer(t)
	resp, err := s.ListDocuments(context.Background(), &firestorepb.ListDocumentsRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
	})
	if err != nil {
		t.Fatalf("ListDocuments: %v", err)
	}
	if len(resp.Documents) != 0 {
		t.Errorf("expected 0 documents, got %d", len(resp.Documents))
	}
}

func TestListDocuments_ReturnsAllDocuments(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	for _, id := range []string{"a", "b", "c"} {
		_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       collectionParent(),
			CollectionId: "widgets",
			DocumentId:   id,
			Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"x": intVal(1)}},
		})
		if err != nil {
			t.Fatalf("CreateDocument %s: %v", id, err)
		}
	}

	resp, err := s.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
	})
	if err != nil {
		t.Fatalf("ListDocuments: %v", err)
	}
	if len(resp.Documents) != 3 {
		t.Errorf("expected 3 documents, got %d", len(resp.Documents))
	}
}

func TestListDocuments_PageSize(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	for _, id := range []string{"a", "b", "c", "d", "e"} {
		_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       collectionParent(),
			CollectionId: "widgets",
			DocumentId:   id,
			Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"x": intVal(1)}},
		})
		if err != nil {
			t.Fatalf("CreateDocument %s: %v", id, err)
		}
	}

	resp, err := s.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
		PageSize:     3,
	})
	if err != nil {
		t.Fatalf("ListDocuments with page_size=3: %v", err)
	}
	if len(resp.Documents) != 3 {
		t.Errorf("expected 3 documents (page_size=3), got %d", len(resp.Documents))
	}
	if resp.NextPageToken == "" {
		t.Error("expected NextPageToken for remaining results")
	}
}

func TestListDocuments_Pagination(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	for _, id := range []string{"a", "b", "c", "d"} {
		_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       collectionParent(),
			CollectionId: "widgets",
			DocumentId:   id,
			Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"x": intVal(1)}},
		})
		if err != nil {
			t.Fatalf("CreateDocument %s: %v", id, err)
		}
	}

	page1, err := s.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
		PageSize:     2,
	})
	if err != nil {
		t.Fatalf("ListDocuments page 1: %v", err)
	}

	page2, err := s.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{
		Parent:        collectionParent(),
		CollectionId:  "widgets",
		PageSize:      2,
		PageToken:     page1.NextPageToken,
	})
	if err != nil {
		t.Fatalf("ListDocuments page 2: %v", err)
	}

	total := len(page1.Documents) + len(page2.Documents)
	if total != 4 {
		t.Errorf("paginated total = %d, want 4", total)
	}
	if page2.NextPageToken != "" {
		t.Error("NextPageToken should be empty on last page")
	}
}

func TestListDocuments_DoesNotReturnDeletedDocuments(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	for _, id := range []string{"keep", "delete-me"} {
		_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       collectionParent(),
			CollectionId: "widgets",
			DocumentId:   id,
			Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"x": intVal(1)}},
		})
		if err != nil {
			t.Fatalf("CreateDocument %s: %v", id, err)
		}
	}

	if _, err := s.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: docName("widgets", "delete-me")}); err != nil {
		t.Fatalf("DeleteDocument: %v", err)
	}

	resp, err := s.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
	})
	if err != nil {
		t.Fatalf("ListDocuments: %v", err)
	}
	if len(resp.Documents) != 1 {
		t.Errorf("expected 1 document after deletion, got %d", len(resp.Documents))
	}
	if resp.Documents[0].Name != docName("widgets", "keep") {
		t.Errorf("wrong document returned: %s", resp.Documents[0].Name)
	}
}
