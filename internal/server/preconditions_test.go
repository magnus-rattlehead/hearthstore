package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPrecondition_ExistsTrue_OnMissingDoc(t *testing.T) {
	s := newTestServer(t)
	name := docName("things", "p1")

	_, err := s.UpdateDocument(context.Background(), &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   name,
			Fields: map[string]*firestorepb.Value{"x": intVal(1)},
		},
		CurrentDocument: &firestorepb.Precondition{
			ConditionType: &firestorepb.Precondition_Exists{Exists: true},
		},
	})
	mustCode(t, err, codes.NotFound)
}

func TestPrecondition_ExistsFalse_OnExistingDoc(t *testing.T) {
	s := newTestServer(t)
	name := docName("things", "p2")
	seedDoc(t, s, name, map[string]*firestorepb.Value{"x": intVal(1)})

	_, err := s.UpdateDocument(context.Background(), &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   name,
			Fields: map[string]*firestorepb.Value{"x": intVal(2)},
		},
		CurrentDocument: &firestorepb.Precondition{
			ConditionType: &firestorepb.Precondition_Exists{Exists: false},
		},
	})
	mustCode(t, err, codes.FailedPrecondition)
}

func TestPrecondition_UpdateTime_Mismatch(t *testing.T) {
	s := newTestServer(t)
	name := docName("things", "p3")
	seedDoc(t, s, name, map[string]*firestorepb.Value{"x": intVal(1)})

	_, err := s.UpdateDocument(context.Background(), &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   name,
			Fields: map[string]*firestorepb.Value{"x": intVal(99)},
		},
		CurrentDocument: &firestorepb.Precondition{
			ConditionType: &firestorepb.Precondition_UpdateTime{
				UpdateTime: timestamppb.New(timestamppb.Now().AsTime().Add(-999)),
			},
		},
	})
	mustCode(t, err, codes.FailedPrecondition)
}

func TestPrecondition_Delete_ExistsTrue_OnMissing(t *testing.T) {
	s := newTestServer(t)

	_, err := s.DeleteDocument(context.Background(), &firestorepb.DeleteDocumentRequest{
		Name: docName("things", "p4"),
		CurrentDocument: &firestorepb.Precondition{
			ConditionType: &firestorepb.Precondition_Exists{Exists: true},
		},
	})
	mustCode(t, err, codes.NotFound)
}

func TestPrecondition_UpdateTime_Match(t *testing.T) {
	s := newTestServer(t)
	name := docName("things", "p5")
	seedDoc(t, s, name, map[string]*firestorepb.Value{"x": intVal(1)})

	// Get the actual update time.
	doc, err := s.GetDocument(context.Background(), &firestorepb.GetDocumentRequest{Name: name})
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}

	_, err = s.UpdateDocument(context.Background(), &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   name,
			Fields: map[string]*firestorepb.Value{"x": intVal(99)},
		},
		CurrentDocument: &firestorepb.Precondition{
			ConditionType: &firestorepb.Precondition_UpdateTime{
				UpdateTime: doc.UpdateTime,
			},
		},
	})
	if err != nil {
		t.Errorf("precondition update_time match should succeed: %v", err)
	}
}
