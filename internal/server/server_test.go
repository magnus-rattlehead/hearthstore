package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

const (
	testProject = "test-proj"
	testDB      = "(default)"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return New(store)
}

// docName returns the full Firestore resource name for a document.
func docName(collection, id string) string {
	return fmt.Sprintf("projects/%s/databases/%s/documents/%s/%s", testProject, testDB, collection, id)
}

// collectionParent returns the parent path for a root-level collection.
func collectionParent() string {
	return fmt.Sprintf("projects/%s/databases/%s/documents", testProject, testDB)
}

// strVal builds a Firestore string Value.
func strVal(s string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: s}}
}

// intVal builds a Firestore integer Value.
func intVal(i int64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: i}}
}

// boolVal builds a Firestore boolean Value.
func boolVal(b bool) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_BooleanValue{BooleanValue: b}}
}

// tsVal builds a Firestore timestamp Value.
func tsVal(ts time.Time) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_TimestampValue{
		TimestampValue: timestamppb.New(ts),
	}}
}

// mustCode asserts the gRPC status code of err.
func mustCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Errorf("status code = %v, want %v (err: %v)", got, want, err)
	}
}

// fieldFilter builds a StructuredQuery_Filter for a single field equality.
func fieldFilter(field string, op firestorepb.StructuredQuery_FieldFilter_Operator, val *firestorepb.Value) *firestorepb.StructuredQuery_Filter {
	return &firestorepb.StructuredQuery_Filter{
		FilterType: &firestorepb.StructuredQuery_Filter_FieldFilter{
			FieldFilter: &firestorepb.StructuredQuery_FieldFilter{
				Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: field},
				Op:    op,
				Value: val,
			},
		},
	}
}

// --- Fake stream implementations for server-streaming RPCs ---

type fakeServerStream struct {
	ctx context.Context
}

func newFakeStream() fakeServerStream {
	return fakeServerStream{ctx: context.Background()}
}

func (f fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (f fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (f fakeServerStream) SetTrailer(metadata.MD)       {}
func (f fakeServerStream) Context() context.Context     { return f.ctx }
func (f fakeServerStream) SendMsg(any) error            { return nil }
func (f fakeServerStream) RecvMsg(any) error            { return nil }

type fakeBatchGetStream struct {
	fakeServerStream
	sent []*firestorepb.BatchGetDocumentsResponse
}

func (f *fakeBatchGetStream) Send(r *firestorepb.BatchGetDocumentsResponse) error {
	f.sent = append(f.sent, r)
	return nil
}

type fakeRunQueryStream struct {
	fakeServerStream
	sent []*firestorepb.RunQueryResponse
}

func (f *fakeRunQueryStream) Send(r *firestorepb.RunQueryResponse) error {
	f.sent = append(f.sent, r)
	return nil
}

type fakeAggregationStream struct {
	fakeServerStream
	sent []*firestorepb.RunAggregationQueryResponse
}

func (f *fakeAggregationStream) Send(r *firestorepb.RunAggregationQueryResponse) error {
	f.sent = append(f.sent, r)
	return nil
}

type fakeWriteStream struct {
	fakeServerStream
	sent []*firestorepb.WriteResponse
	recv []*firestorepb.WriteRequest
	pos  int
}

func (f *fakeWriteStream) Send(r *firestorepb.WriteResponse) error {
	f.sent = append(f.sent, r)
	return nil
}

func (f *fakeWriteStream) Recv() (*firestorepb.WriteRequest, error) {
	if f.pos >= len(f.recv) {
		return nil, fmt.Errorf("no more messages")
	}
	r := f.recv[f.pos]
	f.pos++
	return r, nil
}

type fakeListenStream struct {
	fakeServerStream
	sent []*firestorepb.ListenResponse
	recv []*firestorepb.ListenRequest
	pos  int
}

func (f *fakeListenStream) Send(r *firestorepb.ListenResponse) error {
	f.sent = append(f.sent, r)
	return nil
}

func (f *fakeListenStream) Recv() (*firestorepb.ListenRequest, error) {
	if f.pos >= len(f.recv) {
		return nil, fmt.Errorf("no more messages")
	}
	r := f.recv[f.pos]
	f.pos++
	return r, nil
}
