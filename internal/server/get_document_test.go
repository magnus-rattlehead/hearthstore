package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func TestGetDocument_NotFound(t *testing.T) {
	s := newTestServer(t)
	_, err := s.GetDocument(context.Background(), &firestorepb.GetDocumentRequest{
		Name: docName("widgets", "nonexistent"),
	})
	mustCode(t, err, codes.NotFound)
}

func TestGetDocument_ReturnsDocument(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
		DocumentId:   "w1",
		Document: &firestorepb.Document{
			Fields: map[string]*firestorepb.Value{
				"color": strVal("red"),
				"count": intVal(42),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	got, err := s.GetDocument(ctx, &firestorepb.GetDocumentRequest{
		Name: docName("widgets", "w1"),
	})
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if got.Fields["color"].GetStringValue() != "red" {
		t.Errorf("color = %q, want %q", got.Fields["color"].GetStringValue(), "red")
	}
	if got.Fields["count"].GetIntegerValue() != 42 {
		t.Errorf("count = %d, want 42", got.Fields["count"].GetIntegerValue())
	}
}

func TestGetDocument_ReflectsUpdate(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
		DocumentId:   "w2",
		Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"color": strVal("blue")}},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	_, err = s.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   docName("widgets", "w2"),
			Fields: map[string]*firestorepb.Value{"color": strVal("green")},
		},
	})
	if err != nil {
		t.Fatalf("UpdateDocument: %v", err)
	}

	got, err := s.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: docName("widgets", "w2")})
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if got.Fields["color"].GetStringValue() != "green" {
		t.Errorf("color = %q, want %q", got.Fields["color"].GetStringValue(), "green")
	}
}

func TestGetDocument_WithFieldMask(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
		DocumentId:   "w3",
		Document: &firestorepb.Document{
			Fields: map[string]*firestorepb.Value{
				"color": strVal("red"),
				"size":  strVal("large"),
				"count": intVal(7),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	got, err := s.GetDocument(ctx, &firestorepb.GetDocumentRequest{
		Name: docName("widgets", "w3"),
		Mask: &firestorepb.DocumentMask{FieldPaths: []string{"color"}},
	})
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if _, ok := got.Fields["color"]; !ok {
		t.Error("expected field 'color' in masked response")
	}
	if _, ok := got.Fields["size"]; ok {
		t.Error("field 'size' should be excluded by mask")
	}
	if _, ok := got.Fields["count"]; ok {
		t.Error("field 'count' should be excluded by mask")
	}
}
