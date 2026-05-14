package server

import (
	"context"
	"io"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeWriteStreamFull is a fakeWriteStream that closes after all messages are consumed.
type fakeWriteStreamFull struct {
	fakeServerStream
	sent []*firestorepb.WriteResponse
	recv []*firestorepb.WriteRequest
	pos  int
}

func (f *fakeWriteStreamFull) Send(r *firestorepb.WriteResponse) error {
	f.sent = append(f.sent, r)
	return nil
}

func (f *fakeWriteStreamFull) Recv() (*firestorepb.WriteRequest, error) {
	if f.pos >= len(f.recv) {
		return nil, io.EOF
	}
	r := f.recv[f.pos]
	f.pos++
	return r, nil
}

func (f *fakeWriteStreamFull) Context() context.Context { return context.Background() }

func TestWrite_Handshake(t *testing.T) {
	s := newTestServer(t)
	db := "projects/" + testProject + "/databases/" + testDB

	stream := &fakeWriteStreamFull{
		fakeServerStream: newFakeStream(),
		recv: []*firestorepb.WriteRequest{
			{Database: db}, // handshake: no writes
		},
	}

	if err := s.Write(stream); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if len(stream.sent) != 1 {
		t.Fatalf("want 1 response (handshake), got %d", len(stream.sent))
	}
	resp := stream.sent[0]
	if resp.StreamId == "" {
		t.Error("want non-empty StreamId")
	}
	if len(resp.StreamToken) == 0 {
		t.Error("want non-empty StreamToken")
	}
	if resp.CommitTime == nil {
		t.Error("want CommitTime set")
	}
}

func TestWrite_WriteAndVerify(t *testing.T) {
	s := newTestServer(t)
	db := "projects/" + testProject + "/databases/" + testDB
	docName := docName("things", "w1")

	stream := &fakeWriteStreamFull{
		fakeServerStream: newFakeStream(),
		recv: []*firestorepb.WriteRequest{
			{Database: db}, // handshake
			{
				// batch write
				Writes: []*firestorepb.Write{
					{
						Operation: &firestorepb.Write_Update{
							Update: &firestorepb.Document{
								Name:   docName,
								Fields: map[string]*firestorepb.Value{"x": intVal(42)},
							},
						},
					},
				},
			},
		},
	}

	if err := s.Write(stream); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if len(stream.sent) != 2 {
		t.Fatalf("want 2 responses (handshake + write), got %d", len(stream.sent))
	}

	writeResp := stream.sent[1]
	if len(writeResp.WriteResults) != 1 {
		t.Fatalf("want 1 WriteResult, got %d", len(writeResp.WriteResults))
	}
	if writeResp.WriteResults[0].UpdateTime == nil {
		t.Error("want UpdateTime in WriteResult")
	}

	// Verify the document was actually stored.
	doc, err := s.store.GetDoc(testProject, testDB, "things/w1")
	if err != nil {
		t.Fatalf("GetDoc: %v", err)
	}
	if doc.Fields["x"].GetIntegerValue() != 42 {
		t.Errorf("x = %v, want 42", doc.Fields["x"])
	}
}

// TestWrite_PreconditionFailed verifies that a Write with an exists:false precondition
// for a document that already exists closes the stream with FAILED_PRECONDITION.
// The real Firebase server does not return a WriteResult for a precondition failure;
// it closes the stream with an error. The SDK's handleWriteStreamClose sees an empty
// write pipeline (any preceding batches were already acknowledged) and restarts cleanly.
func TestWrite_PreconditionFailed(t *testing.T) {
	s := newTestServer(t)
	db := "projects/" + testProject + "/databases/" + testDB
	name := docName("messages", "m1")

	// Pre-create the document.
	seedDoc(t, s, name, map[string]*firestorepb.Value{"text": strVal("hello")})

	stream := &fakeWriteStreamFull{
		fakeServerStream: newFakeStream(),
		recv: []*firestorepb.WriteRequest{
			{Database: db}, // handshake
			{
				Writes: []*firestorepb.Write{
					{
						Operation: &firestorepb.Write_Update{
							Update: &firestorepb.Document{
								Name:   name,
								Fields: map[string]*firestorepb.Value{"text": strVal("hello")},
							},
						},
						CurrentDocument: &firestorepb.Precondition{
							ConditionType: &firestorepb.Precondition_Exists{Exists: false},
						},
					},
				},
			},
		},
	}

	err := s.Write(stream)
	if err == nil {
		t.Fatal("Write should return FailedPrecondition for exists:false on existing doc, got nil")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("want FailedPrecondition, got %v", got)
	}
	// Only the handshake response should have been sent (no WriteResult for a failed precondition).
	if len(stream.sent) != 1 {
		t.Errorf("want 1 response (handshake only), got %d", len(stream.sent))
	}
}

// TestWrite_SetMerge_NonExistentDoc verifies that setDoc({field: val}, {merge: true})
// on a document that doesn't exist yet succeeds and creates the document.
// This was failing with NotFound before the fix; rowy hits this when creating
// the _rowy_/settings document on a fresh database.
func TestWrite_SetMerge_NonExistentDoc(t *testing.T) {
	s := newTestServer(t)
	db := "projects/" + testProject + "/databases/" + testDB
	name := docName("_rowy_", "settings")

	stream := &fakeWriteStreamFull{
		fakeServerStream: newFakeStream(),
		recv: []*firestorepb.WriteRequest{
			{Database: db}, // handshake
			{
				Writes: []*firestorepb.Write{
					{
						Operation: &firestorepb.Write_Update{
							Update: &firestorepb.Document{
								Name:   name,
								Fields: map[string]*firestorepb.Value{"version": intVal(1)},
							},
						},
						UpdateMask: &firestorepb.DocumentMask{FieldPaths: []string{"version"}},
						// No CurrentDocument precondition → set with merge, must create if absent.
					},
				},
			},
		},
	}

	if err := s.Write(stream); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if len(stream.sent) != 2 {
		t.Fatalf("want 2 responses (handshake + write), got %d", len(stream.sent))
	}
	if len(stream.sent[1].WriteResults) != 1 {
		t.Fatalf("want 1 WriteResult, got %d", len(stream.sent[1].WriteResults))
	}

	doc, err := s.store.GetDoc(testProject, testDB, "_rowy_/settings")
	if err != nil {
		t.Fatalf("GetDoc after merge-write: %v", err)
	}
	if doc.Fields["version"].GetIntegerValue() != 1 {
		t.Errorf("version = %v, want 1", doc.Fields["version"])
	}
}

func TestWrite_MultipleBatches(t *testing.T) {
	s := newTestServer(t)
	db := "projects/" + testProject + "/databases/" + testDB

	stream := &fakeWriteStreamFull{
		fakeServerStream: newFakeStream(),
		recv: []*firestorepb.WriteRequest{
			{Database: db},
			{
				Writes: []*firestorepb.Write{
					{Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
						Name: docName("things", "a"), Fields: map[string]*firestorepb.Value{"n": intVal(1)},
					}}},
				},
			},
			{
				Writes: []*firestorepb.Write{
					{Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
						Name: docName("things", "b"), Fields: map[string]*firestorepb.Value{"n": intVal(2)},
					}}},
				},
			},
		},
	}

	if err := s.Write(stream); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// handshake + 2 batches = 3 responses
	if len(stream.sent) != 3 {
		t.Errorf("want 3 responses, got %d", len(stream.sent))
	}

	// Stream tokens should change between batches.
	t1 := string(stream.sent[1].StreamToken)
	t2 := string(stream.sent[2].StreamToken)
	if t1 == t2 {
		t.Error("stream tokens should differ between batches")
	}
}
