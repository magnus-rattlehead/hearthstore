//go:build integration

package tests

// Integration tests for the Firestore v1 REST API using net/http + protojson.
// Each test spins up a fresh httptest.Server backed by a temp SQLite store and
// exercises the REST API directly, covering the HTTP transport path that the
// gRPC-only SDK tests do not.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/magnus-rattlehead/hearthstore/internal/server"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

var (
	fsPjsonM = protojson.MarshalOptions{EmitUnpopulated: false}
	fsPjsonU = protojson.UnmarshalOptions{DiscardUnknown: true}
)

// fsRESTServer wraps an httptest.Server and provides typed helpers.
type fsRESTServer struct {
	t      *testing.T
	ts     *httptest.Server
	client *http.Client
}

func newFSRESTServer(t *testing.T) *fsRESTServer {
	t.Helper()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	ts := httptest.NewServer(server.New(store).RESTHandler())
	t.Cleanup(ts.Close)
	return &fsRESTServer{t: t, ts: ts, client: ts.Client()}
}

// docURL builds the document URL: /v1/projects/{testProject}/databases/(default)/documents/{collection}/{doc}
func (s *fsRESTServer) docURL(collection, doc string) string {
	return fmt.Sprintf("%s/v1/projects/%s/databases/(default)/documents/%s/%s",
		s.ts.URL, testProject, collection, doc)
}

// docName returns the Firestore resource name for use in proto messages (no host/scheme).
func (s *fsRESTServer) docName(collection, doc string) string {
	return fmt.Sprintf("projects/%s/databases/(default)/documents/%s/%s", testProject, collection, doc)
}

// actionURL builds an action URL: /v1/projects/{testProject}/databases/(default)/documents:{method}
func (s *fsRESTServer) actionURL(method string) string {
	return fmt.Sprintf("%s/v1/projects/%s/databases/(default)/documents:%s",
		s.ts.URL, testProject, method)
}

// patch sends a PATCH request with the given proto message body and unmarshals the response.
func (s *fsRESTServer) patch(url string, req proto.Message, resp proto.Message) int {
	s.t.Helper()
	return s.do(http.MethodPatch, url, req, resp)
}

// get sends a GET request and unmarshals the response.
func (s *fsRESTServer) get(url string, resp proto.Message) int {
	s.t.Helper()
	return s.do(http.MethodGet, url, nil, resp)
}

// del sends a DELETE request.
func (s *fsRESTServer) del(url string) int {
	s.t.Helper()
	return s.do(http.MethodDelete, url, nil, nil)
}

// post sends a POST request with the given proto message body and unmarshals the response.
func (s *fsRESTServer) post(url string, req proto.Message, resp proto.Message) int {
	s.t.Helper()
	return s.do(http.MethodPost, url, req, resp)
}

func (s *fsRESTServer) do(method, url string, reqMsg, respMsg proto.Message) int {
	s.t.Helper()
	var body io.Reader
	if reqMsg != nil {
		b, err := fsPjsonM.Marshal(reqMsg)
		if err != nil {
			s.t.Fatalf("marshal request: %v", err)
		}
		body = bytes.NewReader(b)
	}
	httpReq, _ := http.NewRequestWithContext(context.Background(), method, url, body)
	if body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	httpResp, err := s.client.Do(httpReq)
	if err != nil {
		s.t.Fatalf("%s %s: %v", method, url, err)
	}
	defer httpResp.Body.Close()
	raw, _ := io.ReadAll(httpResp.Body)
	if httpResp.StatusCode == http.StatusOK && respMsg != nil {
		if err := fsPjsonU.Unmarshal(raw, respMsg); err != nil {
			s.t.Fatalf("unmarshal response from %s %s: %v\nbody: %s", method, url, err, raw)
		}
	}
	return httpResp.StatusCode
}

// fsDoc builds a Document proto with string/int fields for testing.
func fsDoc(name string, fields map[string]*firestorepb.Value) *firestorepb.Document {
	return &firestorepb.Document{Name: name, Fields: fields}
}

func fsStrVal(s string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: s}}
}

func fsIntVal(i int64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: i}}
}

// ── basic CRUD ─────────────────────────────────────────────────────────────

func TestFSREST_SetAndGet(t *testing.T) {
	s := newFSRESTServer(t)

	docURL := s.docURL("users", "alice")

	// PATCH = UpdateDocument (full replace / upsert without mask).
	patchReq := &firestorepb.UpdateDocumentRequest{
		Document: fsDoc(docURL, map[string]*firestorepb.Value{
			"name":  fsStrVal("Alice"),
			"score": fsIntVal(42),
		}),
	}
	if code := s.patch(docURL, patchReq, nil); code != http.StatusOK {
		t.Fatalf("PATCH: HTTP %d", code)
	}

	var got firestorepb.Document
	if code := s.get(docURL, &got); code != http.StatusOK {
		t.Fatalf("GET: HTTP %d", code)
	}
	if got.Fields["name"].GetStringValue() != "Alice" {
		t.Errorf("name = %q, want Alice", got.Fields["name"].GetStringValue())
	}
	if got.Fields["score"].GetIntegerValue() != 42 {
		t.Errorf("score = %d, want 42", got.Fields["score"].GetIntegerValue())
	}
}

func TestFSREST_Delete(t *testing.T) {
	s := newFSRESTServer(t)

	docURL := s.docURL("items", "toDelete")

	// Create doc.
	s.patch(docURL, &firestorepb.UpdateDocumentRequest{ //nolint:errcheck
		Document: fsDoc(docURL, map[string]*firestorepb.Value{"v": fsIntVal(1)}),
	}, nil)

	// Delete it.
	if code := s.del(docURL); code != http.StatusOK {
		t.Fatalf("DELETE: HTTP %d", code)
	}

	// GET should return 404.
	if code := s.get(docURL, nil); code != http.StatusNotFound {
		t.Errorf("GET after DELETE: want 404, got %d", code)
	}
}

func TestFSREST_BatchGet(t *testing.T) {
	s := newFSRESTServer(t)

	urlA := s.docURL("batch", "a")
	urlB := s.docURL("batch", "b")

	s.patch(urlA, &firestorepb.UpdateDocumentRequest{ //nolint:errcheck
		Document: fsDoc(urlA, map[string]*firestorepb.Value{"n": fsIntVal(1)}),
	}, nil)
	s.patch(urlB, &firestorepb.UpdateDocumentRequest{ //nolint:errcheck
		Document: fsDoc(urlB, map[string]*firestorepb.Value{"n": fsIntVal(2)}),
	}, nil)

	// Call batchGet via the REST action endpoint.
	// The response is a JSON array of BatchGetDocumentsResponse messages.
	// We read and unmarshal manually since it's an array.
	batchURL := s.actionURL("batchGet")
	dbPath := fmt.Sprintf("projects/%s/databases/(default)", testProject)
	reqBody, _ := fsPjsonM.Marshal(&firestorepb.BatchGetDocumentsRequest{
		Database:  dbPath,
		Documents: []string{s.docName("batch", "a"), s.docName("batch", "b")},
	})

	httpReq, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, batchURL, bytes.NewReader(reqBody))
	httpReq.Header.Set("Content-Type", "application/json")
	httpResp, err := s.client.Do(httpReq)
	if err != nil {
		t.Fatalf("batchGet POST: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(httpResp.Body)
		t.Fatalf("batchGet: HTTP %d, body: %s", httpResp.StatusCode, body)
	}
	raw, _ := io.ReadAll(httpResp.Body)

	// Parse JSON array.
	var found int
	// Count "found" keys in the JSON array (simple text scan suffices here).
	for i := 0; i < len(raw)-5; i++ {
		if string(raw[i:i+6]) == `"name"` {
			found++
		}
	}
	if found < 2 {
		t.Errorf("batchGet: want at least 2 documents, raw = %s", raw)
	}
}

func TestFSREST_RunQuery(t *testing.T) {
	s := newFSRESTServer(t)

	for id, score := range map[string]int64{"p1": 10, "p2": 20, "p3": 10} {
		url := s.docURL("products", id)
		s.patch(url, &firestorepb.UpdateDocumentRequest{ //nolint:errcheck
			Document: fsDoc(url, map[string]*firestorepb.Value{"price": fsIntVal(score)}),
		}, nil)
	}

	qURL := s.actionURL("runQuery")
	reqBody, _ := fsPjsonM.Marshal(&firestorepb.RunQueryRequest{
		Parent: fmt.Sprintf("projects/%s/databases/(default)/documents", testProject),
		QueryType: &firestorepb.RunQueryRequest_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "products"}},
				Where: &firestorepb.StructuredQuery_Filter{
					FilterType: &firestorepb.StructuredQuery_Filter_FieldFilter{
						FieldFilter: &firestorepb.StructuredQuery_FieldFilter{
							Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "price"},
							Op:    firestorepb.StructuredQuery_FieldFilter_EQUAL,
							Value: fsIntVal(10),
						},
					},
				},
			},
		},
	})

	httpReq, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, qURL, bytes.NewReader(reqBody))
	httpReq.Header.Set("Content-Type", "application/json")
	httpResp, err := s.client.Do(httpReq)
	if err != nil {
		t.Fatalf("runQuery POST: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(httpResp.Body)
		t.Fatalf("runQuery: HTTP %d, body: %s", httpResp.StatusCode, body)
	}
	raw, _ := io.ReadAll(httpResp.Body)

	// Count document results in the JSON array.
	var docCount int
	for i := 0; i < len(raw)-9; i++ {
		if string(raw[i:i+10]) == `"document"` {
			docCount++
		}
	}
	if docCount != 2 {
		t.Errorf("runQuery price==10: want 2 docs, got %d; raw = %s", docCount, raw)
	}
}

func TestFSREST_Transaction_Commit(t *testing.T) {
	s := newFSRESTServer(t)

	docURL := s.docURL("bank", "account")
	s.patch(docURL, &firestorepb.UpdateDocumentRequest{ //nolint:errcheck
		Document: fsDoc(docURL, map[string]*firestorepb.Value{"balance": fsIntVal(100)}),
	}, nil)

	// Begin transaction.
	var beginResp firestorepb.BeginTransactionResponse
	if code := s.post(s.actionURL("beginTransaction"), &firestorepb.BeginTransactionRequest{}, &beginResp); code != http.StatusOK {
		t.Fatalf("beginTransaction: HTTP %d", code)
	}
	txID := beginResp.Transaction

	// Read in transaction.
	var current firestorepb.Document
	if code := s.get(docURL+"?transaction="+string(txID), &current); code != http.StatusOK {
		if code2 := s.get(docURL, &current); code2 != http.StatusOK {
			t.Fatalf("GET: HTTP %d", code2)
		}
	}
	balance := current.Fields["balance"].GetIntegerValue()

	// Commit with new balance (use Firestore resource name, not HTTP URL).
	dbPath := fmt.Sprintf("projects/%s/databases/(default)", testProject)
	newDoc := fsDoc(s.docName("bank", "account"), map[string]*firestorepb.Value{"balance": fsIntVal(balance + 50)})
	commitReq := &firestorepb.CommitRequest{
		Database:    dbPath,
		Transaction: txID,
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Update{Update: newDoc},
		}},
	}
	if code := s.post(s.actionURL("commit"), commitReq, &firestorepb.CommitResponse{}); code != http.StatusOK {
		t.Fatalf("commit: HTTP %d", code)
	}

	var verify firestorepb.Document
	s.get(docURL, &verify) //nolint:errcheck
	if got := verify.Fields["balance"].GetIntegerValue(); got != 150 {
		t.Errorf("balance after commit = %d, want 150", got)
	}
}

func TestFSREST_Transaction_Rollback(t *testing.T) {
	s := newFSRESTServer(t)

	docURL := s.docURL("bank", "acct2")
	s.patch(docURL, &firestorepb.UpdateDocumentRequest{ //nolint:errcheck
		Document: fsDoc(docURL, map[string]*firestorepb.Value{"balance": fsIntVal(200)}),
	}, nil)

	var beginResp firestorepb.BeginTransactionResponse
	s.post(s.actionURL("beginTransaction"), &firestorepb.BeginTransactionRequest{}, &beginResp) //nolint:errcheck

	dbPath := fmt.Sprintf("projects/%s/databases/(default)", testProject)
	if code := s.post(s.actionURL("rollback"), &firestorepb.RollbackRequest{
		Database:    dbPath,
		Transaction: beginResp.Transaction,
	}, nil); code != http.StatusOK {
		t.Fatalf("rollback: HTTP %d", code)
	}

	var verify firestorepb.Document
	s.get(docURL, &verify) //nolint:errcheck
	if got := verify.Fields["balance"].GetIntegerValue(); got != 200 {
		t.Errorf("balance after rollback = %d, want 200 (unchanged)", got)
	}
}

func TestFSREST_Commit_Batch(t *testing.T) {
	s := newFSRESTServer(t)

	urlA := s.docURL("batch-write", "a")
	urlB := s.docURL("batch-write", "b")
	urlC := s.docURL("batch-write", "c")

	dbPath := fmt.Sprintf("projects/%s/databases/(default)", testProject)
	commitReq := &firestorepb.CommitRequest{
		Database: dbPath,
		Writes: []*firestorepb.Write{
			{Operation: &firestorepb.Write_Update{Update: fsDoc(s.docName("batch-write", "a"), map[string]*firestorepb.Value{"v": fsIntVal(1)})}},
			{Operation: &firestorepb.Write_Update{Update: fsDoc(s.docName("batch-write", "b"), map[string]*firestorepb.Value{"v": fsIntVal(2)})}},
			{Operation: &firestorepb.Write_Update{Update: fsDoc(s.docName("batch-write", "c"), map[string]*firestorepb.Value{"v": fsIntVal(3)})}},
		},
	}
	if code := s.post(s.actionURL("commit"), commitReq, &firestorepb.CommitResponse{}); code != http.StatusOK {
		t.Fatalf("commit batch: HTTP %d", code)
	}

	for _, tc := range []struct {
		url  string
		want int64
	}{
		{urlA, 1},
		{urlB, 2},
		{urlC, 3},
	} {
		var doc firestorepb.Document
		if code := s.get(tc.url, &doc); code != http.StatusOK {
			t.Errorf("GET %s: HTTP %d", tc.url, code)
			continue
		}
		if got := doc.Fields["v"].GetIntegerValue(); got != tc.want {
			t.Errorf("v = %d, want %d", got, tc.want)
		}
	}
}
