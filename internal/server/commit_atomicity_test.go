package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// TestCommitAtomic verifies that if write N in a Commit batch fails, all prior
// writes in the same batch are rolled back (all-or-nothing semantics).
func TestCommitAtomic_RollsBackOnFailure(t *testing.T) {
	s := newTestServer(t)

	// Seed doc1 — will be deleted by write #1.
	name1 := docName("things", "atomic1")
	name2 := docName("things", "atomic2")
	seedDoc(t, s, name1, map[string]*firestorepb.Value{"x": intVal(1)})
	// name2 does NOT exist — write #2 will fail with exists=false precondition.

	// Write #1: delete name1 (succeeds on its own)
	// Write #2: update name2 with precondition exists=true (fails — doc missing)
	_, err := s.Commit(context.Background(), &firestorepb.CommitRequest{
		Database: "projects/" + testProject + "/databases/" + testDB,
		Writes: []*firestorepb.Write{
			{
				Operation: &firestorepb.Write_Delete{Delete: name1},
			},
			{
				Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
					Name:   name2,
					Fields: map[string]*firestorepb.Value{"y": intVal(2)},
				}},
				CurrentDocument: &firestorepb.Precondition{
					ConditionType: &firestorepb.Precondition_Exists{Exists: true},
				},
			},
		},
	})

	// The commit should fail (write 2 failed).
	if err == nil {
		t.Fatal("Commit should have failed due to write #2 precondition")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("want NotFound, got %v", got)
	}

	// Write #1 (delete name1) must have been rolled back — name1 should still exist.
	_, getErr := s.GetDocument(context.Background(), &firestorepb.GetDocumentRequest{Name: name1})
	if getErr != nil {
		t.Errorf("name1 should still exist after rollback, got: %v", getErr)
	}
}
