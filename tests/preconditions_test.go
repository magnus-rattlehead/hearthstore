//go:build integration

package tests

// Integration tests for write preconditions (exists, update_time) and
// read-only transaction enforcement across both Firestore and Datastore paths.

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	"cloud.google.com/go/firestore"
)

// -- Firestore preconditions ------------------------------------------------

func TestPrecondition_Create_Success(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("precond").Doc("new-doc")
	if _, err := ref.Create(ctx, map[string]any{"x": 1}); err != nil {
		t.Fatalf("Create on new doc: %v", err)
	}
}

func TestPrecondition_Create_AlreadyExists(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("precond").Doc("existing")
	if _, err := ref.Set(ctx, map[string]any{"x": 1}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err := ref.Create(ctx, map[string]any{"x": 2})
	if err == nil {
		t.Fatal("Create on existing doc: expected error, got nil")
	}
	if code := status.Code(err); code != codes.AlreadyExists && code != codes.FailedPrecondition {
		t.Errorf("Create on existing doc: want AlreadyExists or FailedPrecondition, got %v", code)
	}
}

func TestPrecondition_Update_ExistsTrue_Missing(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	// Update sends exists=true precondition implicitly.
	ref := client.Collection("precond").Doc("ghost")
	_, err := ref.Update(ctx, []firestore.Update{{Path: "x", Value: 1}})
	if err == nil {
		t.Fatal("Update on missing doc: expected error, got nil")
	}
	code := status.Code(err)
	if code != codes.NotFound && code != codes.FailedPrecondition {
		t.Errorf("Update on missing doc: want NotFound or FailedPrecondition, got %v", code)
	}
}

func TestPrecondition_Commit_ExistsFalse_Present(t *testing.T) {
	s := newFSRESTServer(t)
	dbPath := fmt.Sprintf("projects/%s/databases/(default)", testProject)

	// Create the document first.
	docURL := s.docURL("precond-ef", "existing")
	s.patch(docURL, &firestorepb.UpdateDocumentRequest{ //nolint:errcheck
		Document: fsDoc(docURL, map[string]*firestorepb.Value{"v": fsIntVal(1)}),
	}, nil)

	// Commit a Write with exists=false precondition - should fail because doc exists.
	code := s.post(s.actionURL("commit"), &firestorepb.CommitRequest{
		Database: dbPath,
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Update{
				Update: fsDoc(s.docName("precond-ef", "existing"), map[string]*firestorepb.Value{"v": fsIntVal(99)}),
			},
			CurrentDocument: &firestorepb.Precondition{
				ConditionType: &firestorepb.Precondition_Exists{Exists: false},
			},
		}},
	}, nil)
	if code == http.StatusOK {
		t.Error("commit with exists=false on existing doc: expected error, got HTTP 200")
	}
}

func TestPrecondition_Commit_UpdateTime_Match(t *testing.T) {
	s := newFSRESTServer(t)
	dbPath := fmt.Sprintf("projects/%s/databases/(default)", testProject)

	// Create the document and capture its update_time from a subsequent GET.
	docURL := s.docURL("precond-ut", "timedoc")
	s.patch(docURL, &firestorepb.UpdateDocumentRequest{ //nolint:errcheck
		Document: fsDoc(docURL, map[string]*firestorepb.Value{"v": fsIntVal(1)}),
	}, nil)

	var got firestorepb.Document
	if code := s.get(docURL, &got); code != http.StatusOK {
		t.Fatalf("GET: HTTP %d", code)
	}
	updateTime := got.UpdateTime

	// Commit with the correct update_time - should succeed.
	code := s.post(s.actionURL("commit"), &firestorepb.CommitRequest{
		Database: dbPath,
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Update{
				Update: fsDoc(s.docName("precond-ut", "timedoc"), map[string]*firestorepb.Value{"v": fsIntVal(2)}),
			},
			CurrentDocument: &firestorepb.Precondition{
				ConditionType: &firestorepb.Precondition_UpdateTime{UpdateTime: updateTime},
			},
		}},
	}, nil)
	if code != http.StatusOK {
		t.Errorf("commit with matching update_time: want HTTP 200, got %d", code)
	}
}

func TestPrecondition_Commit_UpdateTime_Mismatch(t *testing.T) {
	s := newFSRESTServer(t)
	dbPath := fmt.Sprintf("projects/%s/databases/(default)", testProject)

	docURL := s.docURL("precond-utm", "timedoc2")
	s.patch(docURL, &firestorepb.UpdateDocumentRequest{ //nolint:errcheck
		Document: fsDoc(docURL, map[string]*firestorepb.Value{"v": fsIntVal(1)}),
	}, nil)

	// Commit with a deliberately wrong update_time.
	wrongTime := timestamppb.New(time.Now().Add(-24 * time.Hour))
	code := s.post(s.actionURL("commit"), &firestorepb.CommitRequest{
		Database: dbPath,
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Update{
				Update: fsDoc(s.docName("precond-utm", "timedoc2"), map[string]*firestorepb.Value{"v": fsIntVal(99)}),
			},
			CurrentDocument: &firestorepb.Precondition{
				ConditionType: &firestorepb.Precondition_UpdateTime{UpdateTime: wrongTime},
			},
		}},
	}, nil)
	if code == http.StatusOK {
		t.Error("commit with wrong update_time: expected error, got HTTP 200")
	}
}

func TestPrecondition_Delete_ExistsTrue_Missing(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("precond-del").Doc("ghost")
	_, err := ref.Delete(ctx, firestore.Exists)
	if err == nil {
		t.Fatal("Delete with Exists precondition on missing doc: expected error, got nil")
	}
	code := status.Code(err)
	if code != codes.NotFound && code != codes.FailedPrecondition {
		t.Errorf("want NotFound or FailedPrecondition, got %v", code)
	}
}

// TestPrecondition_Transaction_ReadOnly_Write_Fails verifies that a write
// committed inside a read-only transaction is rejected by the server.
// Uses the raw REST API to bypass any client-side SDK checks.
func TestPrecondition_Transaction_ReadOnly_Write_Fails(t *testing.T) {
	s := newFSRESTServer(t)
	dbPath := fmt.Sprintf("projects/%s/databases/(default)", testProject)

	// Begin a read-only transaction.
	var beginResp firestorepb.BeginTransactionResponse
	if code := s.post(s.actionURL("beginTransaction"), &firestorepb.BeginTransactionRequest{
		Database: dbPath,
		Options: &firestorepb.TransactionOptions{
			Mode: &firestorepb.TransactionOptions_ReadOnly_{
				ReadOnly: &firestorepb.TransactionOptions_ReadOnly{},
			},
		},
	}, &beginResp); code != http.StatusOK {
		t.Fatalf("beginTransaction: HTTP %d", code)
	}

	// Attempt to commit a write under the read-only transaction.
	docName := s.docName("precond-ro", "doc1")
	code := s.post(s.actionURL("commit"), &firestorepb.CommitRequest{
		Database:    dbPath,
		Transaction: beginResp.Transaction,
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Update{
				Update: fsDoc(docName, map[string]*firestorepb.Value{"x": fsIntVal(1)}),
			},
		}},
	}, nil)
	if code == http.StatusOK {
		t.Error("commit write in read-only transaction: expected error, got HTTP 200")
	}
}

func TestPrecondition_Commit_UnknownTransaction(t *testing.T) {
	s := newFSRESTServer(t)
	dbPath := fmt.Sprintf("projects/%s/databases/(default)", testProject)

	code := s.post(s.actionURL("commit"), &firestorepb.CommitRequest{
		Database:    dbPath,
		Transaction: []byte("bogus-tx-id"),
		Writes:      []*firestorepb.Write{},
	}, nil)
	if code != http.StatusNotFound {
		t.Errorf("commit with unknown tx: want HTTP 404, got %d", code)
	}
}

// -- Datastore preconditions ------------------------------------------------

// TestDSPrecondition_Transaction_ReadOnly_Write_Fails verifies the Datastore
// server rejects writes in a read-only transaction (raw HTTP path).
func TestDSPrecondition_Transaction_ReadOnly_Write_Fails(t *testing.T) {
	s := newDSTestServer(t)

	var beginResp datastorepb.BeginTransactionResponse
	if code := s.post("beginTransaction", &datastorepb.BeginTransactionRequest{
		ProjectId: testProject,
		TransactionOptions: &datastorepb.TransactionOptions{
			Mode: &datastorepb.TransactionOptions_ReadOnly_{
				ReadOnly: &datastorepb.TransactionOptions_ReadOnly{},
			},
		},
	}, &beginResp); code != http.StatusOK {
		t.Fatalf("beginTransaction: HTTP %d", code)
	}

	key := dsNameKey("ROCheck", "doc1")
	code := s.post("commit", &datastorepb.CommitRequest{
		ProjectId:           testProject,
		Mode:                datastorepb.CommitRequest_TRANSACTIONAL,
		TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: beginResp.Transaction},
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{
				Upsert: &datastorepb.Entity{
					Key:        key,
					Properties: map[string]*datastorepb.Value{"x": dsIntVal(1)},
				},
			},
		}},
	}, nil)
	if code == http.StatusOK {
		t.Error("commit write in read-only DS transaction: expected error, got HTTP 200")
	}
}
