package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	adminpb "cloud.google.com/go/datastore/admin/apiv1/adminpb"
	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type cancelWhenVisible struct {
	context.Context
	cancel  context.CancelFunc
	visible func() bool
}

func TestFailureResponses(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		code      int
		canonical string
	}{
		{"cancel", context.Canceled, 499, "CANCELLED"},
		{"deadline", context.DeadlineExceeded, 504, "DEADLINE_EXCEEDED"},
		{"unavailable", status.Error(codes.Unavailable, "try later"), 503, "UNAVAILABLE"},
		{"capacity", status.Error(codes.ResourceExhausted, "capacity"), 429, "RESOURCE_EXHAUSTED"},
		{"precondition", status.Error(codes.FailedPrecondition, "recover first"), 400, "FAILED_PRECONDITION"},
		{"permission", status.Error(codes.PermissionDenied, "denied"), 403, "PERMISSION_DENIED"},
		{"authentication", status.Error(codes.Unauthenticated, "credentials"), 401, "UNAUTHENTICATED"},
		{"range", status.Error(codes.OutOfRange, "range"), 400, "OUT_OF_RANGE"},
		{"disk", errors.New("write /private/database: I/O error"), 500, "INTERNAL"},
		{"rolled back import", &storage.ImportError{Outcome: "rolled back", Err: errors.New("write /private/database: I/O error")}, 500, "INTERNAL"},
		{"uncertain import", &storage.ImportError{Outcome: "unresolved", Err: errors.Join(status.Error(codes.InvalidArgument, "source failed"), errors.New("rollback failed"))}, 500, "INTERNAL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			writeGrpcErr(w, tc.err)
			var body errResp
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if w.Code != tc.code || body.Error.Status != tc.canonical || strings.Contains(body.Error.Message, "/private/database") {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if tc.name == "rolled back import" && !strings.Contains(body.Error.Message, "rolled back") {
				t.Fatalf("known rollback was not reported: %s", body.Error.Message)
			}
		})
	}
}

func TestRecoveryRequiredRejectsEmptyAndCachedRequests(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := New(store)
	err = store.DsImportAtomic(context.Background(), testProject, defaultDatabase, func(func(*datastorepb.Entity) error) error {
		// Closing the disposable DB makes rollback fail through the real I/O path.
		return errors.Join(errors.New("import source failed"), store.Close())
	})
	if err == nil {
		t.Fatal("expected import failure")
	}
	for name, run := range map[string]func() error{
		"admin": func() error {
			_, err := NewAdminServer(store, nil).ListIndexes(context.Background(), &adminpb.ListIndexesRequest{ProjectId: testProject})
			return err
		},
		"begin": func() error {
			_, err := s.grpc.BeginTransaction(context.Background(), &datastorepb.BeginTransactionRequest{ProjectId: testProject})
			return err
		},
		"empty lookup": func() error {
			_, err := s.grpc.Lookup(context.Background(), &datastorepb.LookupRequest{ProjectId: testProject})
			return err
		},
		"explain": func() error {
			_, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Recovery"}}}}, ExplainOptions: &datastorepb.ExplainOptions{}})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("request after failed recovery: %v", err)
			}
		})
	}
	// Real transports must preserve the recovery-required error too.
	httpServer := httptest.NewServer(s.Handler())
	defer httpServer.Close()
	response, err := httpServer.Client().Post(httpServer.URL+"/v1/projects/"+testProject+":beginTransaction", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			t.Error(err)
		}
	}()
	var body errResp
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Status != "FAILED_PRECONDITION" || response.StatusCode != http.StatusBadRequest {
		t.Fatalf("REST recovery error: status=%d body=%+v", response.StatusCode, body)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	datastorepb.RegisterDatastoreServer(server, s.grpc)
	defer server.Stop()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	}()
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := connection.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = datastorepb.NewDatastoreClient(connection).BeginTransaction(ctx, &datastorepb.BeginTransactionRequest{ProjectId: testProject})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("gRPC recovery error: %v", err)
	}
}

type brokenResponseWriter struct {
	header http.Header
	writes int
}

func (w *brokenResponseWriter) Header() http.Header { return w.header }
func (w *brokenResponseWriter) WriteHeader(int)     {}
func (w *brokenResponseWriter) Write([]byte) (int, error) {
	w.writes++
	return 0, errors.New("connection lost")
}

func TestCommitResponseFailurePreservesWrite(t *testing.T) {
	s := newTestDsServer(t)
	w := &brokenResponseWriter{header: make(http.Header)}
	r := httptest.NewRequest(http.MethodPost, "/v1/projects/"+testProject+":commit", strings.NewReader(`{"mode":"NON_TRANSACTIONAL","mutations":[{"upsert":{"key":{"path":[{"kind":"ResponseFailure","name":"one"}]},"properties":{"value":{"integerValue":"7"}}}}]}`))
	s.Handler().ServeHTTP(w, r)
	if w.writes != 1 {
		t.Fatalf("response attempted %d writes", w.writes)
	}
	got, err := s.grpc.Lookup(context.Background(), &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{dsKey("ResponseFailure", "one")}})
	if err != nil || len(got.GetFound()) != 1 || got.Found[0].Entity.Properties["value"].GetIntegerValue() != 7 {
		t.Fatalf("committed write not retained: found=%d err=%v", len(got.GetFound()), err)
	}
}

func (c cancelWhenVisible) Err() error {
	if c.visible() {
		c.cancel()
	}
	return c.Context.Err()
}

func TestSingleUseCommitDoesNotSplitTransaction(t *testing.T) {
	s := newTestDsServer(t)
	req := &datastorepb.CommitRequest{ProjectId: testProject, Mode: datastorepb.CommitRequest_TRANSACTIONAL,
		TransactionSelector: &datastorepb.CommitRequest_SingleUseTransaction{SingleUseTransaction: &datastorepb.TransactionOptions{Mode: &datastorepb.TransactionOptions_ReadWrite_{ReadWrite: &datastorepb.TransactionOptions_ReadWrite{}}}},
	}
	for i := range 32 {
		values := make([]*datastorepb.Value, 128)
		for j := range values {
			values[j] = dsStr(fmt.Sprintf("%04d%s", j, strings.Repeat("x", 900)))
		}
		req.Mutations = append(req.Mutations, &datastorepb.Mutation{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(dsKey("AtomicFanout", fmt.Sprint(i)), map[string]*datastorepb.Value{"values": dsArray(values...)})}})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, _, _, _, firstPath := keyComponents(dsKey("AtomicFanout", "0"))
	ctx = cancelWhenVisible{Context: ctx, cancel: cancel, visible: func() bool {
		_, _, err := s.grpc.store.DsGet(testProject, defaultDatabase, "", firstPath)
		return err == nil
	}}
	response, err := s.grpc.Commit(ctx, req)
	if status.Code(err) != codes.ResourceExhausted {
		t.Errorf("oversized atomic commit response=%v err=%v, want storage limit", response, err)
	}
	keys := make([]*datastorepb.Key, 32)
	for i := range keys {
		keys[i] = dsKey("AtomicFanout", fmt.Sprint(i))
	}
	lookup, err := s.grpc.Lookup(context.Background(), &datastorepb.LookupRequest{ProjectId: testProject, Keys: keys})
	if err != nil || len(lookup.GetFound()) != 0 || len(lookup.GetMissing()) != len(keys) {
		t.Fatalf("failed transaction left partial data: found=%d missing=%d err=%v", len(lookup.GetFound()), len(lookup.GetMissing()), err)
	}
}
