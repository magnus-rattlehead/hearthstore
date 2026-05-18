package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func TestBatchWrite_CreateAndUpdate(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	// Seed a document to update.
	_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
		DocumentId:   "existing",
		Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"color": strVal("red")}},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	resp, err := s.BatchWrite(ctx, &firestorepb.BatchWriteRequest{
		Database: "projects/test-proj/databases/(default)",
		Writes: []*firestorepb.Write{
			{
				Operation: &firestorepb.Write_Update{
					Update: &firestorepb.Document{
						Name:   docName("widgets", "new"),
						Fields: map[string]*firestorepb.Value{"color": strVal("blue")},
					},
				},
			},
			{
				Operation: &firestorepb.Write_Update{
					Update: &firestorepb.Document{
						Name:   docName("widgets", "existing"),
						Fields: map[string]*firestorepb.Value{"color": strVal("green")},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("BatchWrite: %v", err)
	}
	if len(resp.WriteResults) != 2 {
		t.Errorf("expected 2 WriteResults, got %d", len(resp.WriteResults))
	}
	for i, wr := range resp.WriteResults {
		if wr.UpdateTime == nil {
			t.Errorf("WriteResults[%d].UpdateTime should be set", i)
		}
	}
}

func TestBatchWrite_DeleteOperation(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	_, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       collectionParent(),
		CollectionId: "widgets",
		DocumentId:   "to-delete",
		Document:     &firestorepb.Document{Fields: map[string]*firestorepb.Value{"x": intVal(1)}},
	})
	if err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	_, err = s.BatchWrite(ctx, &firestorepb.BatchWriteRequest{
		Database: "projects/test-proj/databases/(default)",
		Writes: []*firestorepb.Write{
			{Operation: &firestorepb.Write_Delete{Delete: docName("widgets", "to-delete")}},
		},
	})
	if err != nil {
		t.Fatalf("BatchWrite delete: %v", err)
	}

	_, err = s.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: docName("widgets", "to-delete")})
	mustCode(t, err, codes.NotFound)
}

// A failed write in a batch should not block other writes; individual status
// is reported per-write in WriteStatus, not as an RPC-level error.
func TestBatchWrite_PartialFailureReportedPerWrite(t *testing.T) {
	s := newTestServer(t)
	ctx := context.Background()

	resp, err := s.BatchWrite(ctx, &firestorepb.BatchWriteRequest{
		Database: "projects/test-proj/databases/(default)",
		Writes: []*firestorepb.Write{
			// Valid upsert.
			{
				Operation: &firestorepb.Write_Update{
					Update: &firestorepb.Document{
						Name:   docName("widgets", "ok"),
						Fields: map[string]*firestorepb.Value{"x": intVal(1)},
					},
				},
			},
			// Update with must-exist precondition on a nonexistent doc - should fail.
			{
				CurrentDocument: &firestorepb.Precondition{
					ConditionType: &firestorepb.Precondition_Exists{Exists: true},
				},
				Operation: &firestorepb.Write_Update{
					Update: &firestorepb.Document{
						Name:   docName("widgets", "ghost"),
						Fields: map[string]*firestorepb.Value{"x": intVal(2)},
					},
				},
			},
		},
	})
	// BatchWrite itself must not return an error.
	if err != nil {
		t.Fatalf("BatchWrite: %v", err)
	}
	if len(resp.WriteResults) != 2 {
		t.Fatalf("expected 2 WriteResults, got %d", len(resp.WriteResults))
	}
	// First write succeeded.
	if resp.Status[0] != nil && resp.Status[0].Code != 0 {
		t.Errorf("write[0] should succeed, got status %v", resp.Status[0])
	}
	// Second write failed with NOT_FOUND.
	if resp.Status[1] == nil || codes.Code(resp.Status[1].Code) != codes.NotFound {
		t.Errorf("write[1] should fail with NotFound, got %v", resp.Status[1])
	}
}
