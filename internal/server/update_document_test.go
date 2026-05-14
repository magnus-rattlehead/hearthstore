package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func TestUpdateDocument_Success(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
		DocumentId:   "w1",
		Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"color": strVal("red")}},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	updated, err := s.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   docName("widgets", "w1"),
			Fields: map[string]*firestorepb.Value{"color": strVal("blue"), "count": intVal(3)},
		},
	})
	if err != nil {
		t.Fatalf("UpdateDocument: %v", err)
	}
	if updated.Fields["color"].GetStringValue() != "blue" {
		t.Errorf("color = %q, want %q", updated.Fields["color"].GetStringValue(), "blue")
	}
}

// UpdateDocument without an update mask is an upsert: it creates the document if absent.
func TestUpdateDocument_NoMask_UpsertCreatesDocument(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	doc, err := s.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   docName("widgets", "new-doc"),
			Fields: map[string]*firestorepb.Value{"color": strVal("red")},
		},
	})
	if err != nil {
		t.Fatalf("UpdateDocument (upsert): %v", err)
	}
	if doc.Fields["color"].GetStringValue() != "red" {
		t.Errorf("color = %q, want %q", doc.Fields["color"].GetStringValue(), "red")
	}
}

// UpdateDocument with an update mask requires the document to exist.
func TestUpdateDocument_WithMask_NotFound(t *testing.T) {
	s := newTestServer(t)
	_, err := s.UpdateDocument(context.Background(), &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   docName("widgets", "missing"),
			Fields: map[string]*firestorepb.Value{"color": strVal("red")},
		},
		UpdateMask: &firestorepb.DocumentMask{FieldPaths: []string{"color"}},
	})
	mustCode(t, err, codes.NotFound)
}

// UpdateDocument with an update mask should only overwrite the specified fields,
// leaving other existing fields intact.
func TestUpdateDocument_WithUpdateMask(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
		DocumentId:   "w2",
		Document: &firestorepb.Document{
			Fields: map[string]*firestorepb.Value{
				"color": strVal("red"),
				"size":  strVal("large"),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	_, err = s.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   docName("widgets", "w2"),
			Fields: map[string]*firestorepb.Value{"color": strVal("green")},
		},
		UpdateMask: &firestorepb.DocumentMask{FieldPaths: []string{"color"}},
	})
	if err != nil {
		t.Fatalf("UpdateDocument with mask: %v", err)
	}

	got, err := s.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: docName("widgets", "w2")})
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if got.Fields["color"].GetStringValue() != "green" {
		t.Errorf("color = %q, want %q", got.Fields["color"].GetStringValue(), "green")
	}
	// 'size' was not in the update mask; it must survive.
	if got.Fields["size"].GetStringValue() != "large" {
		t.Errorf("size = %q, want %q (should be preserved by mask)", got.Fields["size"].GetStringValue(), "large")
	}
}

func TestUpdateDocument_UpdateTimeAdvances(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	created, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
		DocumentId:   "w3",
		Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"x": intVal(1)}},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	updated, err := s.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   docName("widgets", "w3"),
			Fields: map[string]*firestorepb.Value{"x": intVal(2)},
		},
	})
	if err != nil {
		t.Fatalf("UpdateDocument: %v", err)
	}
	if !updated.UpdateTime.AsTime().After(created.UpdateTime.AsTime()) {
		t.Error("UpdateTime should advance after an update")
	}
}
