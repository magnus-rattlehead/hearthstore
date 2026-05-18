package server

import (
	"context"
	"io"
	"testing"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/magnus-rattlehead/hearthstore/internal/config"
)

// controlledListenStream is a listen stream that reads from a channel
// (so the test can feed requests dynamically) and captures sent responses.
type controlledListenStream struct {
	fakeServerStream
	ctx    context.Context
	cancel context.CancelFunc
	reqCh  chan *firestorepb.ListenRequest
	sent   []*firestorepb.ListenResponse
}

func newControlledListenStream() *controlledListenStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &controlledListenStream{
		fakeServerStream: newFakeStream(),
		ctx:              ctx,
		cancel:           cancel,
		reqCh:            make(chan *firestorepb.ListenRequest, 8),
	}
}

func (f *controlledListenStream) Context() context.Context { return f.ctx }

func (f *controlledListenStream) Send(r *firestorepb.ListenResponse) error {
	f.sent = append(f.sent, r)
	return nil
}

func (f *controlledListenStream) Recv() (*firestorepb.ListenRequest, error) {
	select {
	case req, ok := <-f.reqCh:
		if !ok {
			return nil, io.EOF
		}
		return req, nil
	case <-f.ctx.Done():
		return nil, io.EOF
	}
}

// sendReq feeds a request into the stream.
func (f *controlledListenStream) sendReq(req *firestorepb.ListenRequest) {
	f.reqCh <- req
}

// close signals EOF to the listen loop.
func (f *controlledListenStream) close() {
	close(f.reqCh)
}

// waitForNResponses blocks until at least n responses have been sent, or times out.
func (f *controlledListenStream) waitForNResponses(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(f.sent) >= n {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func makeAddTargetReq(db, collection string, targetID int32) *firestorepb.ListenRequest {
	parent := db + "/documents"
	return &firestorepb.ListenRequest{
		Database: db,
		TargetChange: &firestorepb.ListenRequest_AddTarget{
			AddTarget: &firestorepb.Target{
				TargetId: targetID,
				TargetType: &firestorepb.Target_Query{
					Query: &firestorepb.Target_QueryTarget{
						Parent: parent,
						QueryType: &firestorepb.Target_QueryTarget_StructuredQuery{
							StructuredQuery: &firestorepb.StructuredQuery{
								From: []*firestorepb.StructuredQuery_CollectionSelector{
									{CollectionId: collection},
								},
							},
						},
					},
				},
			},
		},
	}
}

func makeAddTargetReqWithToken(db, collection string, targetID int32, token []byte) *firestorepb.ListenRequest {
	parent := db + "/documents"
	return &firestorepb.ListenRequest{
		Database: db,
		TargetChange: &firestorepb.ListenRequest_AddTarget{
			AddTarget: &firestorepb.Target{
				TargetId: targetID,
				ResumeType: &firestorepb.Target_ResumeToken{
					ResumeToken: token,
				},
				TargetType: &firestorepb.Target_Query{
					Query: &firestorepb.Target_QueryTarget{
						Parent: parent,
						QueryType: &firestorepb.Target_QueryTarget_StructuredQuery{
							StructuredQuery: &firestorepb.StructuredQuery{
								From: []*firestorepb.StructuredQuery_CollectionSelector{
									{CollectionId: collection},
								},
							},
						},
					},
				},
			},
		},
	}
}

func makeAddTargetReqWithReadTime(db, collection string, targetID int32, readTime *timestamppb.Timestamp) *firestorepb.ListenRequest {
	parent := db + "/documents"
	return &firestorepb.ListenRequest{
		Database: db,
		TargetChange: &firestorepb.ListenRequest_AddTarget{
			AddTarget: &firestorepb.Target{
				TargetId: targetID,
				ResumeType: &firestorepb.Target_ReadTime{
					ReadTime: readTime,
				},
				TargetType: &firestorepb.Target_Query{
					Query: &firestorepb.Target_QueryTarget{
						Parent: parent,
						QueryType: &firestorepb.Target_QueryTarget_StructuredQuery{
							StructuredQuery: &firestorepb.StructuredQuery{
								From: []*firestorepb.StructuredQuery_CollectionSelector{
									{CollectionId: collection},
								},
							},
						},
					},
				},
			},
		},
	}
}

func makeAddTargetReqOnce(db, collection string, targetID int32) *firestorepb.ListenRequest {
	parent := db + "/documents"
	return &firestorepb.ListenRequest{
		Database: db,
		TargetChange: &firestorepb.ListenRequest_AddTarget{
			AddTarget: &firestorepb.Target{
				TargetId: targetID,
				Once:     true,
				TargetType: &firestorepb.Target_Query{
					Query: &firestorepb.Target_QueryTarget{
						Parent: parent,
						QueryType: &firestorepb.Target_QueryTarget_StructuredQuery{
							StructuredQuery: &firestorepb.StructuredQuery{
								From: []*firestorepb.StructuredQuery_CollectionSelector{
									{CollectionId: collection},
								},
							},
						},
					},
				},
			},
		},
	}
}

// makeDocumentsTargetReq builds an AddTarget request for an explicit set of document paths.
func makeDocumentsTargetReq(db string, docPaths []string, targetID int32) *firestorepb.ListenRequest {
	var names []string
	for _, p := range docPaths {
		names = append(names, db+"/documents/"+p)
	}
	return &firestorepb.ListenRequest{
		Database: db,
		TargetChange: &firestorepb.ListenRequest_AddTarget{
			AddTarget: &firestorepb.Target{
				TargetId: targetID,
				TargetType: &firestorepb.Target_Documents{
					Documents: &firestorepb.Target_DocumentsTarget{
						Documents: names,
					},
				},
			},
		},
	}
}

// makeDocumentsTargetReqWithToken is like makeDocumentsTargetReq but includes a resume token.
func makeDocumentsTargetReqWithToken(db string, docPaths []string, targetID int32, token []byte) *firestorepb.ListenRequest {
	var names []string
	for _, p := range docPaths {
		names = append(names, db+"/documents/"+p)
	}
	return &firestorepb.ListenRequest{
		Database: db,
		TargetChange: &firestorepb.ListenRequest_AddTarget{
			AddTarget: &firestorepb.Target{
				TargetId: targetID,
				ResumeType: &firestorepb.Target_ResumeToken{
					ResumeToken: token,
				},
				TargetType: &firestorepb.Target_Documents{
					Documents: &firestorepb.Target_DocumentsTarget{
						Documents: names,
					},
				},
			},
		},
	}
}

// captureResumeToken scans sent responses for a TargetChange_CURRENT and returns its ResumeToken.
func captureResumeToken(sent []*firestorepb.ListenResponse) []byte {
	for _, msg := range sent {
		if tc, ok := msg.ResponseType.(*firestorepb.ListenResponse_TargetChange); ok {
			if tc.TargetChange.TargetChangeType == firestorepb.TargetChange_CURRENT {
				return tc.TargetChange.ResumeToken
			}
		}
	}
	return nil
}

func removeTargetReq(db string, targetID int32) *firestorepb.ListenRequest {
	return &firestorepb.ListenRequest{
		Database: db,
		TargetChange: &firestorepb.ListenRequest_RemoveTarget{
			RemoveTarget: targetID,
		},
	}
}

const testDB2 = "projects/" + testProject + "/databases/" + testDB

func TestListen_InitialSnapshot(t *testing.T) {
	s := newTestServer(t)

	// Create a document before the listen starts.
	_, err := s.store.InsertDoc(testProject, testDB, "things", "", "things/doc1",
		&firestorepb.Document{
			Name:   docName("things", "doc1"),
			Fields: map[string]*firestorepb.Value{"x": intVal(1)},
		})
	if err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	stream := newControlledListenStream()
	done := make(chan error, 1)
	go func() {
		done <- s.Listen(stream)
	}()

	stream.sendReq(makeAddTargetReq(testDB2, "things", 1))

	// Wait for: ADD ack + DocumentChange + CURRENT + NO_CHANGE = at least 4 messages.
	if !stream.waitForNResponses(4, 2*time.Second) {
		t.Fatalf("timeout waiting for initial snapshot responses, got %d", len(stream.sent))
	}

	stream.close()
	if err := <-done; err != nil {
		t.Errorf("Listen returned error: %v", err)
	}

	// Verify message types in order.
	findTargetChangeType := func(want firestorepb.TargetChange_TargetChangeType) bool {
		for _, msg := range stream.sent {
			if tc, ok := msg.ResponseType.(*firestorepb.ListenResponse_TargetChange); ok {
				if tc.TargetChange.TargetChangeType == want {
					return true
				}
			}
		}
		return false
	}
	if !findTargetChangeType(firestorepb.TargetChange_ADD) {
		t.Error("want TargetChange_ADD in responses")
	}
	if !findTargetChangeType(firestorepb.TargetChange_CURRENT) {
		t.Error("want TargetChange_CURRENT in responses")
	}

	// Verify we got a DocumentChange for doc1.
	foundDoc := false
	for _, msg := range stream.sent {
		if dc, ok := msg.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			if dc.DocumentChange.Document.Name == docName("things", "doc1") {
				foundDoc = true
			}
		}
	}
	if !foundDoc {
		t.Error("want DocumentChange for doc1 in initial snapshot")
	}
}

func TestListen_RealTimeDocumentChange(t *testing.T) {
	s := newTestServer(t)

	stream := newControlledListenStream()
	done := make(chan error, 1)
	go func() {
		done <- s.Listen(stream)
	}()

	stream.sendReq(makeAddTargetReq(testDB2, "things", 1))

	// Wait for initial snapshot to complete (ADD + CURRENT + NO_CHANGE at minimum).
	if !stream.waitForNResponses(3, 2*time.Second) {
		t.Fatalf("timeout waiting for initial snapshot, got %d", len(stream.sent))
	}
	baseline := len(stream.sent)

	// Now write a new document - should trigger a DocumentChange.
	_, err := s.store.UpsertDoc(testProject, testDB, "things", "", "things/live1",
		&firestorepb.Document{
			Name:   docName("things", "live1"),
			Fields: map[string]*firestorepb.Value{"y": intVal(99)},
		})
	if err != nil {
		t.Fatalf("UpsertDoc: %v", err)
	}

	// Wait for the new event to arrive.
	if !stream.waitForNResponses(baseline+1, 2*time.Second) {
		t.Fatalf("timeout waiting for real-time event, got %d after baseline %d", len(stream.sent), baseline)
	}

	stream.close()
	<-done

	// Check that we received a DocumentChange for live1.
	foundLive := false
	for _, msg := range stream.sent[baseline:] {
		if dc, ok := msg.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			if dc.DocumentChange.Document.Name == docName("things", "live1") {
				foundLive = true
			}
		}
	}
	if !foundLive {
		t.Error("want DocumentChange for live1 after real-time write")
	}
}

func TestListen_DocumentDelete(t *testing.T) {
	s := newTestServer(t)

	// Pre-create a document.
	_, err := s.store.InsertDoc(testProject, testDB, "things", "", "things/del1",
		&firestorepb.Document{
			Name:   docName("things", "del1"),
			Fields: map[string]*firestorepb.Value{"z": intVal(1)},
		})
	if err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	stream := newControlledListenStream()
	done := make(chan error, 1)
	go func() {
		done <- s.Listen(stream)
	}()

	stream.sendReq(makeAddTargetReq(testDB2, "things", 1))

	// Wait for initial snapshot.
	if !stream.waitForNResponses(4, 2*time.Second) {
		t.Fatalf("timeout waiting for initial snapshot, got %d", len(stream.sent))
	}
	baseline := len(stream.sent)

	// Delete the document.
	if err := s.store.DeleteDoc(testProject, testDB, "things/del1"); err != nil {
		t.Fatalf("DeleteDoc: %v", err)
	}

	// Wait for DocumentDelete event.
	if !stream.waitForNResponses(baseline+1, 2*time.Second) {
		t.Fatalf("timeout waiting for delete event")
	}

	stream.close()
	<-done

	foundDelete := false
	for _, msg := range stream.sent[baseline:] {
		if dd, ok := msg.ResponseType.(*firestorepb.ListenResponse_DocumentDelete); ok {
			if dd.DocumentDelete.Document == docName("things", "del1") {
				foundDelete = true
			}
		}
	}
	if !foundDelete {
		t.Error("want DocumentDelete for del1 after delete")
	}
}

func TestListen_RemoveTarget(t *testing.T) {
	s := newTestServer(t)

	stream := newControlledListenStream()
	done := make(chan error, 1)
	go func() {
		done <- s.Listen(stream)
	}()

	stream.sendReq(makeAddTargetReq(testDB2, "things", 1))
	if !stream.waitForNResponses(3, 2*time.Second) {
		t.Fatalf("timeout waiting for initial snapshot")
	}

	// Remove the target.
	stream.sendReq(removeTargetReq(testDB2, 1))

	// Wait for the REMOVE TargetChange.
	if !stream.waitForNResponses(len(stream.sent)+1, 2*time.Second) {
		t.Fatalf("timeout waiting for REMOVE ack")
	}

	stream.close()
	<-done

	foundRemove := false
	for _, msg := range stream.sent {
		if tc, ok := msg.ResponseType.(*firestorepb.ListenResponse_TargetChange); ok {
			if tc.TargetChange.TargetChangeType == firestorepb.TargetChange_REMOVE {
				foundRemove = true
			}
		}
	}
	if !foundRemove {
		t.Error("want TargetChange_REMOVE after RemoveTarget request")
	}
}

// TestListen_OnceFlag_TargetAutoRemoved verifies that a target with Once=true is
// automatically removed after the initial snapshot is delivered (CURRENT sent),
// and a REMOVE TargetChange is sent by the server.
func TestListen_OnceFlag_TargetAutoRemoved(t *testing.T) {
	s := newTestServer(t)

	if _, err := s.store.InsertDoc(testProject, testDB, "once", "", "once/doc1",
		&firestorepb.Document{
			Name:   docName("once", "doc1"),
			Fields: map[string]*firestorepb.Value{"v": intVal(1)},
		}); err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	stream := newControlledListenStream()
	done := make(chan error, 1)
	go func() { done <- s.Listen(stream) }()

	stream.sendReq(makeAddTargetReqOnce(testDB2, "once", 1))

	// Expect: ADD + DocumentChange + CURRENT + NO_CHANGE + REMOVE = 5 messages.
	if !stream.waitForNResponses(5, 2*time.Second) {
		t.Fatalf("timeout waiting for once responses, got %d", len(stream.sent))
	}

	// Verify REMOVE is sent.
	foundRemove := false
	for _, msg := range stream.sent {
		if tc, ok := msg.ResponseType.(*firestorepb.ListenResponse_TargetChange); ok {
			if tc.TargetChange.TargetChangeType == firestorepb.TargetChange_REMOVE {
				foundRemove = true
			}
		}
	}
	if !foundRemove {
		t.Error("want TargetChange_REMOVE after once=true snapshot delivery")
	}

	// After REMOVE, subsequent writes should NOT trigger further messages for this target.
	baseline := len(stream.sent)
	if _, err := s.store.InsertDoc(testProject, testDB, "once", "", "once/doc2",
		&firestorepb.Document{
			Name:   docName("once", "doc2"),
			Fields: map[string]*firestorepb.Value{"v": intVal(2)},
		}); err != nil {
		t.Fatalf("InsertDoc doc2: %v", err)
	}

	// Give time for any spurious event.
	time.Sleep(50 * time.Millisecond)
	if len(stream.sent) != baseline {
		t.Errorf("no events expected after once target removed, got %d extra", len(stream.sent)-baseline)
	}

	stream.close()
	<-done
}

// TestListen_ResumeToken_DeltaOnly verifies that reconnecting on the same Listen stream
// with a resume token sends only documents written after the token, not docs that existed
// before it. Uses same-stream reconnect (two AddTargets in a single Listen call).
func TestListen_ResumeToken_DeltaOnly(t *testing.T) {
	s := newTestServer(t)

	// Insert doc1 before we capture a token.
	if _, err := s.store.InsertDoc(testProject, testDB, "tok", "", "tok/doc1",
		&firestorepb.Document{
			Name:   docName("tok", "doc1"),
			Fields: map[string]*firestorepb.Value{"v": intVal(1)},
		}); err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	// Single stream for both AddTargets (same streamID -> delta path).
	stream := newControlledListenStream()
	done := make(chan error, 1)
	go func() { done <- s.Listen(stream) }()

	// First AddTarget: initial snapshot.
	stream.sendReq(makeAddTargetReq(testDB2, "tok", 1))
	// ADD + DocumentChange(doc1) + CURRENT + NO_CHANGE = 4 messages.
	if !stream.waitForNResponses(4, 2*time.Second) {
		t.Fatalf("timeout waiting for initial snapshot, got %d", len(stream.sent))
	}
	token := captureResumeToken(stream.sent)
	if token == nil {
		t.Fatal("no CURRENT resume token found")
	}

	// Remove the target (keep stream alive), then insert doc2.
	stream.sendReq(removeTargetReq(testDB2, 1))
	if !stream.waitForNResponses(5, 2*time.Second) { // +REMOVE
		t.Fatalf("timeout waiting for REMOVE ack, got %d", len(stream.sent))
	}

	if _, err := s.store.InsertDoc(testProject, testDB, "tok", "", "tok/doc2",
		&firestorepb.Document{
			Name:   docName("tok", "doc2"),
			Fields: map[string]*firestorepb.Value{"v": intVal(2)},
		}); err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	// Reconnect on the same stream with the token from step 1.
	stream.sendReq(makeAddTargetReqWithToken(testDB2, "tok", 1, token))
	// ADD + DocumentChange(doc2) + CURRENT + NO_CHANGE = 4 more messages (total 9).
	if !stream.waitForNResponses(9, 2*time.Second) {
		t.Fatalf("timeout waiting for reconnect responses, got %d", len(stream.sent))
	}

	stream.close()
	<-done

	// Inspect only the messages after the second AddTarget (index 5 onwards).
	reconnectMsgs := stream.sent[5:]
	foundDoc1, foundDoc2 := false, false
	for _, msg := range reconnectMsgs {
		if dc, ok := msg.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			switch dc.DocumentChange.Document.Name {
			case docName("tok", "doc1"):
				foundDoc1 = true
			case docName("tok", "doc2"):
				foundDoc2 = true
			}
		}
	}
	if foundDoc1 {
		t.Error("doc1 should NOT be re-sent on same-stream resume (existed before token)")
	}
	if !foundDoc2 {
		t.Error("doc2 should be sent (written after token)")
	}
}

// TestListen_ResumeToken_DeletedDoc verifies that a doc deleted after the token is
// reported as DocumentDelete on same-stream reconnect.
func TestListen_ResumeToken_DeletedDoc(t *testing.T) {
	s := newTestServer(t)

	if _, err := s.store.InsertDoc(testProject, testDB, "del", "", "del/doc1",
		&firestorepb.Document{
			Name:   docName("del", "doc1"),
			Fields: map[string]*firestorepb.Value{"v": intVal(1)},
		}); err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	stream := newControlledListenStream()
	done := make(chan error, 1)
	go func() { done <- s.Listen(stream) }()

	stream.sendReq(makeAddTargetReq(testDB2, "del", 1))
	if !stream.waitForNResponses(4, 2*time.Second) {
		t.Fatalf("timeout, got %d", len(stream.sent))
	}
	token := captureResumeToken(stream.sent)
	if token == nil {
		t.Fatal("no CURRENT token")
	}

	// Remove target, then delete doc1, then reconnect.
	stream.sendReq(removeTargetReq(testDB2, 1))
	if !stream.waitForNResponses(5, 2*time.Second) {
		t.Fatalf("timeout waiting for REMOVE, got %d", len(stream.sent))
	}

	if err := s.store.DeleteDoc(testProject, testDB, "del/doc1"); err != nil {
		t.Fatalf("DeleteDoc: %v", err)
	}

	stream.sendReq(makeAddTargetReqWithToken(testDB2, "del", 1, token))
	// ADD + DocumentDelete + CURRENT + NO_CHANGE = 4 more (total 9).
	if !stream.waitForNResponses(9, 2*time.Second) {
		t.Fatalf("timeout, got %d", len(stream.sent))
	}

	stream.close()
	<-done

	foundDelete := false
	for _, msg := range stream.sent[5:] {
		if dd, ok := msg.ResponseType.(*firestorepb.ListenResponse_DocumentDelete); ok {
			if dd.DocumentDelete.Document == docName("del", "doc1") {
				foundDelete = true
			}
		}
	}
	if !foundDelete {
		t.Error("want DocumentDelete for doc1 deleted after token")
	}
}

// TestListen_ReadTime_DeltaOnly verifies that AddTarget with read_time behaves like
// a resume token: only docs written after the read_time are delivered.
func TestListen_ReadTime_DeltaOnly(t *testing.T) {
	s := newTestServer(t)

	// Insert doc1 before the readTime.
	if _, err := s.store.InsertDoc(testProject, testDB, "rt", "", "rt/doc1",
		&firestorepb.Document{
			Name:   docName("rt", "doc1"),
			Fields: map[string]*firestorepb.Value{"v": intVal(1)},
		}); err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	readTime := timestamppb.Now()
	time.Sleep(2 * time.Millisecond) // ensure doc2's update_time > readTime

	// Insert doc2 after readTime.
	if _, err := s.store.InsertDoc(testProject, testDB, "rt", "", "rt/doc2",
		&firestorepb.Document{
			Name:   docName("rt", "doc2"),
			Fields: map[string]*firestorepb.Value{"v": intVal(2)},
		}); err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	stream := newControlledListenStream()
	done := make(chan error, 1)
	go func() { done <- s.Listen(stream) }()

	stream.sendReq(makeAddTargetReqWithReadTime(testDB2, "rt", 1, readTime))
	// ADD + DocumentChange(doc2) + CURRENT + NO_CHANGE
	if !stream.waitForNResponses(4, 2*time.Second) {
		t.Fatalf("timeout, got %d", len(stream.sent))
	}
	stream.close()
	<-done

	foundDoc1, foundDoc2 := false, false
	for _, msg := range stream.sent {
		if dc, ok := msg.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			switch dc.DocumentChange.Document.Name {
			case docName("rt", "doc1"):
				foundDoc1 = true
			case docName("rt", "doc2"):
				foundDoc2 = true
			}
		}
	}
	if foundDoc1 {
		t.Error("doc1 should NOT be sent (existed before read_time)")
	}
	if !foundDoc2 {
		t.Error("doc2 should be sent (written after read_time)")
	}
}

// TestListen_Query_StaleToken_FullSnapshot verifies that a resume token from a different
// Listen stream triggers a full snapshot (without RESET) for a query target.
// RESET is intentionally not sent: sending it disrupts concurrent WebChannel sessions
// that share the same logical watch (e.g. ratings loaded on Session A while Session B
// for the restaurant doc sends RESET, causing the SDK to discard Session A's state).
func TestListen_Query_StaleToken_FullSnapshot(t *testing.T) {
	s := newTestServer(t)

	if _, err := s.store.InsertDoc(testProject, testDB, "stale", "", "stale/doc1",
		&firestorepb.Document{
			Name:   docName("stale", "doc1"),
			Fields: map[string]*firestorepb.Value{"v": intVal(1)},
		}); err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	// Stream 1: capture token.
	stream1 := newControlledListenStream()
	done1 := make(chan error, 1)
	go func() { done1 <- s.Listen(stream1) }()

	stream1.sendReq(makeAddTargetReq(testDB2, "stale", 1))
	if !stream1.waitForNResponses(4, 2*time.Second) {
		t.Fatalf("stream1: timeout, got %d", len(stream1.sent))
	}
	token := captureResumeToken(stream1.sent)
	if token == nil {
		t.Fatal("no token from stream1")
	}
	stream1.close()
	<-done1

	// Stream 2 (different streamID): reconnect with stream1's token -> full snapshot, no RESET.
	stream2 := newControlledListenStream()
	done2 := make(chan error, 1)
	go func() { done2 <- s.Listen(stream2) }()

	stream2.sendReq(makeAddTargetReqWithToken(testDB2, "stale", 1, token))
	// ADD + DocumentChange(doc1) + CURRENT + NO_CHANGE = 4 messages (no RESET).
	if !stream2.waitForNResponses(4, 2*time.Second) {
		t.Fatalf("stream2: timeout, got %d", len(stream2.sent))
	}
	stream2.close()
	<-done2

	// Verify RESET is NOT present.
	for _, msg := range stream2.sent {
		if tc, ok := msg.ResponseType.(*firestorepb.ListenResponse_TargetChange); ok {
			if tc.TargetChange.TargetChangeType == firestorepb.TargetChange_RESET {
				t.Error("got unexpected TargetChange_RESET for cross-stream token on query target")
			}
		}
	}

	// Full snapshot: doc1 must be present.
	foundDoc1 := false
	for _, msg := range stream2.sent {
		if dc, ok := msg.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			if dc.DocumentChange.Document.Name == docName("stale", "doc1") {
				foundDoc1 = true
			}
		}
	}
	if !foundDoc1 {
		t.Error("want doc1 in full snapshot for cross-stream token on query target")
	}
}

// TestListen_Query_DiffToken_SameStream verifies that a same-stream reconnect on a query
// target sends only documents changed after the token (delta, not full snapshot).
func TestListen_Query_DiffToken_SameStream(t *testing.T) {
	s := newTestServer(t)

	if _, err := s.store.InsertDoc(testProject, testDB, "qdiff", "", "qdiff/doc1",
		&firestorepb.Document{
			Name:   docName("qdiff", "doc1"),
			Fields: map[string]*firestorepb.Value{"v": intVal(1)},
		}); err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	stream := newControlledListenStream()
	done := make(chan error, 1)
	go func() { done <- s.Listen(stream) }()

	stream.sendReq(makeAddTargetReq(testDB2, "qdiff", 1))
	if !stream.waitForNResponses(4, 2*time.Second) {
		t.Fatalf("timeout waiting for initial snapshot, got %d", len(stream.sent))
	}
	token := captureResumeToken(stream.sent)
	if token == nil {
		t.Fatal("no token")
	}

	// Remove then insert doc2.
	stream.sendReq(removeTargetReq(testDB2, 1))
	if !stream.waitForNResponses(5, 2*time.Second) {
		t.Fatalf("timeout REMOVE, got %d", len(stream.sent))
	}

	if _, err := s.store.InsertDoc(testProject, testDB, "qdiff", "", "qdiff/doc2",
		&firestorepb.Document{
			Name:   docName("qdiff", "doc2"),
			Fields: map[string]*firestorepb.Value{"v": intVal(2)},
		}); err != nil {
		t.Fatalf("InsertDoc doc2: %v", err)
	}

	stream.sendReq(makeAddTargetReqWithToken(testDB2, "qdiff", 1, token))
	// ADD + DocumentChange(doc2) + CURRENT + NO_CHANGE = 4 more (total 9).
	if !stream.waitForNResponses(9, 2*time.Second) {
		t.Fatalf("timeout reconnect, got %d", len(stream.sent))
	}

	stream.close()
	<-done

	reconnectMsgs := stream.sent[5:]
	foundDoc1, foundDoc2 := false, false
	for _, msg := range reconnectMsgs {
		if dc, ok := msg.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			switch dc.DocumentChange.Document.Name {
			case docName("qdiff", "doc1"):
				foundDoc1 = true
			case docName("qdiff", "doc2"):
				foundDoc2 = true
			}
		}
	}
	if foundDoc1 {
		t.Error("doc1 should NOT be re-sent on same-stream delta")
	}
	if !foundDoc2 {
		t.Error("doc2 should appear in same-stream delta")
	}
}

// TestListen_Documents_InitialSnapshot verifies that a Documents target with no token
// sends a full snapshot of the watched documents.
func TestListen_Documents_InitialSnapshot(t *testing.T) {
	s := newTestServer(t)

	if _, err := s.store.InsertDoc(testProject, testDB, "docs", "", "docs/d1",
		&firestorepb.Document{
			Name:   docName("docs", "d1"),
			Fields: map[string]*firestorepb.Value{"v": intVal(1)},
		}); err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	stream := newControlledListenStream()
	done := make(chan error, 1)
	go func() { done <- s.Listen(stream) }()

	stream.sendReq(makeDocumentsTargetReq(testDB2, []string{"docs/d1"}, 1))
	// ADD + DocumentChange(d1) + CURRENT + NO_CHANGE = 4.
	if !stream.waitForNResponses(4, 2*time.Second) {
		t.Fatalf("timeout, got %d", len(stream.sent))
	}
	stream.close()
	<-done

	foundD1 := false
	for _, msg := range stream.sent {
		if dc, ok := msg.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			if dc.DocumentChange.Document.Name == docName("docs", "d1") {
				foundD1 = true
			}
		}
	}
	if !foundD1 {
		t.Error("want DocumentChange for d1 in initial Documents snapshot")
	}
}

// TestListen_Documents_DiffToken_NoChange verifies that a same-stream reconnect on a
// Documents target sends nothing when the watched doc is unchanged.
func TestListen_Documents_DiffToken_NoChange(t *testing.T) {
	s := newTestServer(t)

	if _, err := s.store.InsertDoc(testProject, testDB, "dnc", "", "dnc/d1",
		&firestorepb.Document{
			Name:   docName("dnc", "d1"),
			Fields: map[string]*firestorepb.Value{"v": intVal(1)},
		}); err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	stream := newControlledListenStream()
	done := make(chan error, 1)
	go func() { done <- s.Listen(stream) }()

	stream.sendReq(makeDocumentsTargetReq(testDB2, []string{"dnc/d1"}, 1))
	if !stream.waitForNResponses(4, 2*time.Second) {
		t.Fatalf("timeout initial snapshot, got %d", len(stream.sent))
	}
	token := captureResumeToken(stream.sent)
	if token == nil {
		t.Fatal("no token")
	}

	// Remove and reconnect without any intervening write.
	stream.sendReq(removeTargetReq(testDB2, 1))
	if !stream.waitForNResponses(5, 2*time.Second) {
		t.Fatalf("timeout REMOVE, got %d", len(stream.sent))
	}

	stream.sendReq(makeDocumentsTargetReqWithToken(testDB2, []string{"dnc/d1"}, 1, token))
	// ADD + CURRENT + NO_CHANGE = 3 more (no DocumentChange since doc unchanged).
	if !stream.waitForNResponses(8, 2*time.Second) {
		t.Fatalf("timeout reconnect, got %d", len(stream.sent))
	}

	stream.close()
	<-done

	// No DocumentChange in reconnect messages (indices 5+).
	for _, msg := range stream.sent[5:] {
		if _, ok := msg.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			t.Error("unexpected DocumentChange on reconnect when doc unchanged")
		}
	}
}

// TestListen_Documents_DiffToken_DocUpdated verifies that a same-stream reconnect on a
// Documents target sends only the updated doc.
func TestListen_Documents_DiffToken_DocUpdated(t *testing.T) {
	s := newTestServer(t)

	if _, err := s.store.InsertDoc(testProject, testDB, "dup", "", "dup/d1",
		&firestorepb.Document{
			Name:   docName("dup", "d1"),
			Fields: map[string]*firestorepb.Value{"v": intVal(1)},
		}); err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	stream := newControlledListenStream()
	done := make(chan error, 1)
	go func() { done <- s.Listen(stream) }()

	stream.sendReq(makeDocumentsTargetReq(testDB2, []string{"dup/d1"}, 1))
	if !stream.waitForNResponses(4, 2*time.Second) {
		t.Fatalf("timeout initial, got %d", len(stream.sent))
	}
	token := captureResumeToken(stream.sent)
	if token == nil {
		t.Fatal("no token")
	}

	stream.sendReq(removeTargetReq(testDB2, 1))
	if !stream.waitForNResponses(5, 2*time.Second) {
		t.Fatalf("timeout REMOVE, got %d", len(stream.sent))
	}

	// Update d1 after the token.
	if _, err := s.store.UpsertDoc(testProject, testDB, "dup", "", "dup/d1",
		&firestorepb.Document{
			Name:   docName("dup", "d1"),
			Fields: map[string]*firestorepb.Value{"v": intVal(99)},
		}); err != nil {
		t.Fatalf("UpsertDoc: %v", err)
	}

	stream.sendReq(makeDocumentsTargetReqWithToken(testDB2, []string{"dup/d1"}, 1, token))
	// ADD + DocumentChange(d1 updated) + CURRENT + NO_CHANGE = 4 more (total 9).
	if !stream.waitForNResponses(9, 2*time.Second) {
		t.Fatalf("timeout reconnect, got %d", len(stream.sent))
	}

	stream.close()
	<-done

	foundUpdated := false
	for _, msg := range stream.sent[5:] {
		if dc, ok := msg.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			if dc.DocumentChange.Document.Name == docName("dup", "d1") {
				foundUpdated = true
			}
		}
	}
	if !foundUpdated {
		t.Error("want DocumentChange for updated d1 in Documents diff")
	}
}

// TestListen_Documents_DiffToken_DocDeleted verifies that a same-stream reconnect on a
// Documents target sends DocumentDelete for a doc deleted after the token.
func TestListen_Documents_DiffToken_DocDeleted(t *testing.T) {
	s := newTestServer(t)

	if _, err := s.store.InsertDoc(testProject, testDB, "ddel", "", "ddel/d1",
		&firestorepb.Document{
			Name:   docName("ddel", "d1"),
			Fields: map[string]*firestorepb.Value{"v": intVal(1)},
		}); err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	stream := newControlledListenStream()
	done := make(chan error, 1)
	go func() { done <- s.Listen(stream) }()

	stream.sendReq(makeDocumentsTargetReq(testDB2, []string{"ddel/d1"}, 1))
	if !stream.waitForNResponses(4, 2*time.Second) {
		t.Fatalf("timeout initial, got %d", len(stream.sent))
	}
	token := captureResumeToken(stream.sent)
	if token == nil {
		t.Fatal("no token")
	}

	stream.sendReq(removeTargetReq(testDB2, 1))
	if !stream.waitForNResponses(5, 2*time.Second) {
		t.Fatalf("timeout REMOVE, got %d", len(stream.sent))
	}

	if err := s.store.DeleteDoc(testProject, testDB, "ddel/d1"); err != nil {
		t.Fatalf("DeleteDoc: %v", err)
	}

	stream.sendReq(makeDocumentsTargetReqWithToken(testDB2, []string{"ddel/d1"}, 1, token))
	// ADD + DocumentDelete + CURRENT + NO_CHANGE = 4 more (total 9).
	if !stream.waitForNResponses(9, 2*time.Second) {
		t.Fatalf("timeout reconnect, got %d", len(stream.sent))
	}

	stream.close()
	<-done

	foundDelete := false
	for _, msg := range stream.sent[5:] {
		if dd, ok := msg.ResponseType.(*firestorepb.ListenResponse_DocumentDelete); ok {
			if dd.DocumentDelete.Document == docName("ddel", "d1") {
				foundDelete = true
			}
		}
	}
	if !foundDelete {
		t.Error("want DocumentDelete for d1 deleted after token in Documents diff")
	}
}

// TestListen_Documents_StaleToken_FullSnapshot verifies that a resume token from a
// different Listen stream triggers a full snapshot (without RESET) for a Documents target.
// RESET is intentionally not sent - see TestListen_Query_StaleToken_FullSnapshot for rationale.
// TestListen_Documents_StaleToken_FullSnapshot verifies that a resume token from a
// different Listen stream triggers a full snapshot (without RESET) for a Documents target.
func TestListen_Documents_StaleToken_FullSnapshot(t *testing.T) {
	s := newTestServer(t)

	if _, err := s.store.InsertDoc(testProject, testDB, "dstale", "", "dstale/d1",
		&firestorepb.Document{
			Name:   docName("dstale", "d1"),
			Fields: map[string]*firestorepb.Value{"v": intVal(1)},
		}); err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	// Stream 1: capture token.
	stream1 := newControlledListenStream()
	done1 := make(chan error, 1)
	go func() { done1 <- s.Listen(stream1) }()

	stream1.sendReq(makeDocumentsTargetReq(testDB2, []string{"dstale/d1"}, 1))
	if !stream1.waitForNResponses(4, 2*time.Second) {
		t.Fatalf("stream1 timeout, got %d", len(stream1.sent))
	}
	token := captureResumeToken(stream1.sent)
	if token == nil {
		t.Fatal("no token from stream1")
	}
	stream1.close()
	<-done1

	// Stream 2 (different streamID): reconnect with stream1's token -> full snapshot, no RESET.
	stream2 := newControlledListenStream()
	done2 := make(chan error, 1)
	go func() { done2 <- s.Listen(stream2) }()

	stream2.sendReq(makeDocumentsTargetReqWithToken(testDB2, []string{"dstale/d1"}, 1, token))
	// ADD + DocumentChange(d1) + CURRENT + NO_CHANGE = 4 messages (no RESET).
	if !stream2.waitForNResponses(4, 2*time.Second) {
		t.Fatalf("stream2 timeout, got %d", len(stream2.sent))
	}
	stream2.close()
	<-done2

	// Verify RESET is NOT present.
	for _, msg := range stream2.sent {
		if tc, ok := msg.ResponseType.(*firestorepb.ListenResponse_TargetChange); ok {
			if tc.TargetChange.TargetChangeType == firestorepb.TargetChange_RESET {
				t.Error("got unexpected TargetChange_RESET for cross-stream token on Documents target")
			}
		}
	}

	foundD1 := false
	for _, msg := range stream2.sent {
		if dc, ok := msg.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			if dc.DocumentChange.Document.Name == docName("dstale", "d1") {
				foundD1 = true
			}
		}
	}
	if !foundD1 {
		t.Error("want d1 in full snapshot for cross-stream token on Documents target")
	}
}

// TestListen_Heartbeat verifies that a global NO_CHANGE (targetIds=[]) is sent
// periodically even when there are no document changes.
func TestListen_Heartbeat(t *testing.T) {
	// Override the heartbeat interval to something short enough to test without
	// waiting 30 real seconds.
	old := config.HeartbeatInterval
	config.HeartbeatInterval = 100 * time.Millisecond
	defer func() { config.HeartbeatInterval = old }()

	s := newTestServer(t)

	stream := newControlledListenStream()
	done := make(chan error, 1)
	go func() { done <- s.Listen(stream) }()

	stream.sendReq(makeAddTargetReq(testDB2, "hb", 1))
	// Wait for initial snapshot + CURRENT + NO_CHANGE (4 messages), then a heartbeat.
	if !stream.waitForNResponses(5, 2*time.Second) {
		t.Fatalf("timeout waiting for heartbeat, got %d responses", len(stream.sent))
	}
	stream.close()
	<-done

	// The heartbeat is a NO_CHANGE with empty TargetIds.
	foundHeartbeat := false
	for _, msg := range stream.sent[4:] { // skip initial 4
		if tc, ok := msg.ResponseType.(*firestorepb.ListenResponse_TargetChange); ok {
			if tc.TargetChange.TargetChangeType == firestorepb.TargetChange_NO_CHANGE &&
				len(tc.TargetChange.TargetIds) == 0 &&
				tc.TargetChange.ReadTime != nil {
				foundHeartbeat = true
			}
		}
	}
	if !foundHeartbeat {
		t.Error("want periodic NO_CHANGE heartbeat (empty targetIds, readTime set)")
	}
}
