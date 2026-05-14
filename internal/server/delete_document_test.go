package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func TestDeleteDocument_Success(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
		DocumentId:   "w1",
		Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"foo": strVal("bar")}},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	_, err = s.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{
		Name: docName("widgets", "w1"),
	})
	if err != nil {
		t.Fatalf("DeleteDocument: %v", err)
	}

	// Document should no longer be accessible.
	_, err = s.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: docName("widgets", "w1")})
	mustCode(t, err, codes.NotFound)
}

// Firestore delete is idempotent: deleting a nonexistent document succeeds.
func TestDeleteDocument_Idempotent(t *testing.T) {
	s := newTestServer(t)
	_, err := s.DeleteDocument(context.Background(), &firestorepb.DeleteDocumentRequest{
		Name: docName("widgets", "nonexistent"),
	})
	if err != nil {
		t.Errorf("DeleteDocument on nonexistent doc should succeed, got: %v", err)
	}
}

// Delete with a must-exist precondition fails when the document is absent.
func TestDeleteDocument_PreconditionExists_Fails(t *testing.T) {
	s := newTestServer(t)
	_, err := s.DeleteDocument(context.Background(), &firestorepb.DeleteDocumentRequest{
		Name: docName("widgets", "nonexistent"),
		CurrentDocument: &firestorepb.Precondition{
			ConditionType: &firestorepb.Precondition_Exists{Exists: true},
		},
	})
	mustCode(t, err, codes.NotFound)
}

func TestDeleteDocument_RemovesFromCollection(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	for _, id := range []string{"w1", "w2", "w3"} {
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

	if _, err := s.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: docName("widgets", "w2")}); err != nil {
		t.Fatalf("DeleteDocument: %v", err)
	}

	stream := &fakeRunQueryStream{fakeServerStream: newFakeStream()}
	err := s.RunQuery(&firestorepb.RunQueryRequest{
		Parent: collectionParent(),
		QueryType: &firestorepb.RunQueryRequest_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{
					{CollectionId: "widgets"},
				},
			},
		},
	}, stream)
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	for _, r := range stream.sent {
		if r.Document != nil && r.Document.Name == docName("widgets", "w2") {
			t.Error("deleted document w2 still appears in query results")
		}
	}
}
