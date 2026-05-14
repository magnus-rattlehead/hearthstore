package server

import (
	"context"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func TestBatchGetDocuments_AllFound(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	for _, id := range []string{"a", "b", "c"} {
		_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       collectionParent(),
			CollectionId: "widgets",
			DocumentId:   id,
			Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"id": strVal(id)}},
		})
		if err != nil {
			t.Fatalf("CreateDocument %s: %v", id, err)
		}
	}

	stream := &fakeBatchGetStream{fakeServerStream: newFakeStream()}
	err := s.BatchGetDocuments(&firestorepb.BatchGetDocumentsRequest{
		Database: "projects/test-proj/databases/(default)",
		Documents: []string{
			docName("widgets", "a"),
			docName("widgets", "b"),
			docName("widgets", "c"),
		},
	}, stream)
	if err != nil {
		t.Fatalf("BatchGetDocuments: %v", err)
	}
	if len(stream.sent) != 3 {
		t.Errorf("expected 3 responses, got %d", len(stream.sent))
	}
	for _, r := range stream.sent {
		if r.GetFound() == nil {
			t.Errorf("expected all documents found, got missing: %v", r.GetMissing())
		}
	}
}

func TestBatchGetDocuments_MixedFoundAndMissing(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
		DocumentId:   "exists",
		Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"x": intVal(1)}},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	stream := &fakeBatchGetStream{fakeServerStream: newFakeStream()}
	err = s.BatchGetDocuments(&firestorepb.BatchGetDocumentsRequest{
		Database: "projects/test-proj/databases/(default)",
		Documents: []string{
			docName("widgets", "exists"),
			docName("widgets", "missing"),
		},
	}, stream)
	if err != nil {
		t.Fatalf("BatchGetDocuments: %v", err)
	}
	if len(stream.sent) != 2 {
		t.Errorf("expected 2 responses, got %d", len(stream.sent))
	}

	found, missing := 0, 0
	for _, r := range stream.sent {
		if r.GetFound() != nil {
			found++
		} else {
			missing++
		}
	}
	if found != 1 || missing != 1 {
		t.Errorf("found=%d missing=%d, want found=1 missing=1", found, missing)
	}
}

func TestBatchGetDocuments_DeduplicatesRequests(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
		DocumentId:   "w1",
		Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"x": intVal(1)}},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	// Requesting the same document twice should yield one response per unique name.
	stream := &fakeBatchGetStream{fakeServerStream: newFakeStream()}
	err = s.BatchGetDocuments(&firestorepb.BatchGetDocumentsRequest{
		Database:  "projects/test-proj/databases/(default)",
		Documents: []string{docName("widgets", "w1"), docName("widgets", "w1")},
	}, stream)
	if err != nil {
		t.Fatalf("BatchGetDocuments: %v", err)
	}
	if len(stream.sent) != 1 {
		t.Errorf("expected 1 response for duplicate request, got %d", len(stream.sent))
	}
}
