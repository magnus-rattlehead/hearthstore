//go:build integration

package tests

// Integration tests for the Cloud Datastore REST API exercised over a real TCP
// connection. The Cloud Datastore Go SDK (v1.22+) is gRPC-only and cannot be
// used against hearthstore's HTTP REST server without a gRPC layer; these tests
// use net/http + protojson instead, which still covers the full HTTP transport
// path - TCP I/O, JSON serialisation, content-type negotiation, and error
// mapping - that the in-process unit tests (httptest.Recorder) do not.
//
// Run with:
//
//	go test -tags integration ./tests/...

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	dspkg "github.com/magnus-rattlehead/hearthstore/internal/datastore"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// dsTestServer wraps a live httptest.Server and provides typed helpers for each
// Datastore API method.
type dsTestServer struct {
	t      *testing.T
	ts     *httptest.Server
	client *http.Client
}

func newDSTestServer(t *testing.T) *dsTestServer {
	t.Helper()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	ts := httptest.NewServer(dspkg.New(store).Handler())
	t.Cleanup(ts.Close)
	return &dsTestServer{t: t, ts: ts, client: ts.Client()}
}

var (
	dsPjsonM = protojson.MarshalOptions{EmitUnpopulated: false}
	dsPjsonU = protojson.UnmarshalOptions{DiscardUnknown: true}
)

// post marshals req as proto JSON, POSTs to /v1/projects/{testProject}:{method},
// and unmarshals the response into resp. Returns the HTTP status code.
func (s *dsTestServer) post(method string, req, resp proto.Message) int {
	s.t.Helper()
	body, err := dsPjsonM.Marshal(req)
	if err != nil {
		s.t.Fatalf("marshal %s request: %v", method, err)
	}
	url := fmt.Sprintf("%s/v1/projects/%s:%s", s.ts.URL, testProject, method)
	httpReq, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	httpResp, err := s.client.Do(httpReq)
	if err != nil {
		s.t.Fatalf("POST %s: %v", method, err)
	}
	defer httpResp.Body.Close()
	raw, _ := io.ReadAll(httpResp.Body)
	if httpResp.StatusCode == http.StatusOK && resp != nil {
		if err := dsPjsonU.Unmarshal(raw, resp); err != nil {
			s.t.Fatalf("unmarshal %s response: %v\nbody: %s", method, err, raw)
		}
	}
	return httpResp.StatusCode
}

// errorStatus POSTs req and returns the "error.status" string from the response.
func (s *dsTestServer) errorStatus(method string, req proto.Message) string {
	s.t.Helper()
	body, _ := dsPjsonM.Marshal(req)
	url := fmt.Sprintf("%s/v1/projects/%s:%s", s.ts.URL, testProject, method)
	httpReq, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	httpResp, _ := s.client.Do(httpReq)
	defer httpResp.Body.Close()
	raw, _ := io.ReadAll(httpResp.Body)
	var e struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	json.Unmarshal(raw, &e) //nolint:errcheck
	return e.Error.Status
}

// -- helpers ----------------------------------------------------------------

func dsNameKey(kind, name string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: testProject},
		Path:        []*datastorepb.Key_PathElement{{Kind: kind, IdType: &datastorepb.Key_PathElement_Name{Name: name}}},
	}
}

func dsIncompleteKey(kind string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: testProject},
		Path:        []*datastorepb.Key_PathElement{{Kind: kind}},
	}
}

func dsStrVal(s string) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: s}}
}

func dsIntVal(i int64) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: i}}
}

func upsert(s *dsTestServer, key *datastorepb.Key, props map[string]*datastorepb.Value) {
	s.t.Helper()
	req := &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{
				Upsert: &datastorepb.Entity{Key: key, Properties: props},
			},
		}},
	}
	if code := s.post("commit", req, &datastorepb.CommitResponse{}); code != http.StatusOK {
		s.t.Fatalf("upsert commit: HTTP %d", code)
	}
}

// -- basic CRUD -------------------------------------------------------------

func TestDS_UpsertAndLookup(t *testing.T) {
	s := newDSTestServer(t)

	key := dsNameKey("User", "alice")
	upsert(s, key, map[string]*datastorepb.Value{
		"name":  dsStrVal("Alice"),
		"score": dsIntVal(42),
	})

	var resp datastorepb.LookupResponse
	if code := s.post("lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &resp); code != http.StatusOK {
		t.Fatalf("lookup: HTTP %d", code)
	}
	if len(resp.Found) != 1 {
		t.Fatalf("found = %d, want 1", len(resp.Found))
	}
	props := resp.Found[0].Entity.Properties
	if props["name"].GetStringValue() != "Alice" {
		t.Errorf("name = %q, want Alice", props["name"].GetStringValue())
	}
	if props["score"].GetIntegerValue() != 42 {
		t.Errorf("score = %d, want 42", props["score"].GetIntegerValue())
	}
}

func TestDS_Lookup_Missing(t *testing.T) {
	s := newDSTestServer(t)

	var resp datastorepb.LookupResponse
	if code := s.post("lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{dsNameKey("Ghost", "nobody")}}, &resp); code != http.StatusOK {
		t.Fatalf("lookup: HTTP %d", code)
	}
	if len(resp.Missing) != 1 || len(resp.Found) != 0 {
		t.Errorf("missing=%d found=%d, want missing=1 found=0", len(resp.Missing), len(resp.Found))
	}
}

func TestDS_Delete(t *testing.T) {
	s := newDSTestServer(t)

	key := dsNameKey("Item", "toDelete")
	upsert(s, key, map[string]*datastorepb.Value{"v": dsIntVal(1)})

	s.post("commit", &datastorepb.CommitRequest{ //nolint:errcheck
		ProjectId: testProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{Delete: key}}},
	}, &datastorepb.CommitResponse{})

	var lookResp datastorepb.LookupResponse
	s.post("lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &lookResp) //nolint:errcheck
	if len(lookResp.Missing) != 1 {
		t.Errorf("after delete: missing=%d, want 1", len(lookResp.Missing))
	}
}

func TestDS_Insert_Conflict(t *testing.T) {
	s := newDSTestServer(t)

	key := dsNameKey("Dup", "d1")
	upsert(s, key, map[string]*datastorepb.Value{"v": dsIntVal(1)})

	status := s.errorStatus("commit", &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Insert{
				Insert: &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"v": dsIntVal(2)}},
			},
		}},
	})
	if status != "ALREADY_EXISTS" {
		t.Errorf("insert duplicate: status = %q, want ALREADY_EXISTS", status)
	}
}

// -- queries ----------------------------------------------------------------

func TestDS_Query_Equality(t *testing.T) {
	s := newDSTestServer(t)

	for name, score := range map[string]int64{"p1": 10, "p2": 20, "p3": 10} {
		upsert(s, dsNameKey("Player", name), map[string]*datastorepb.Value{"score": dsIntVal(score)})
	}

	var resp datastorepb.RunQueryResponse
	if code := s.post("runQuery", &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunQueryRequest_Query{
			Query: &datastorepb.Query{
				Kind: []*datastorepb.KindExpression{{Name: "Player"}},
				Filter: &datastorepb.Filter{
					FilterType: &datastorepb.Filter_PropertyFilter{
						PropertyFilter: &datastorepb.PropertyFilter{
							Property: &datastorepb.PropertyReference{Name: "score"},
							Op:       datastorepb.PropertyFilter_EQUAL,
							Value:    dsIntVal(10),
						},
					},
				},
			},
		},
	}, &resp); code != http.StatusOK {
		t.Fatalf("runQuery: HTTP %d", code)
	}
	if got := len(resp.Batch.EntityResults); got != 2 {
		t.Errorf("score==10: want 2 results, got %d", got)
	}
}

func TestDS_Query_OrderByLimit(t *testing.T) {
	s := newDSTestServer(t)

	for name, score := range map[string]int64{"r1": 30, "r2": 10, "r3": 20} {
		upsert(s, dsNameKey("Run", name), map[string]*datastorepb.Value{"score": dsIntVal(score)})
	}

	var resp datastorepb.RunQueryResponse
	if code := s.post("runQuery", &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunQueryRequest_Query{
			Query: &datastorepb.Query{
				Kind:  []*datastorepb.KindExpression{{Name: "Run"}},
				Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "score"}, Direction: datastorepb.PropertyOrder_ASCENDING}},
				Limit: wrapperspb.Int32(2),
			},
		},
	}, &resp); code != http.StatusOK {
		t.Fatalf("runQuery: HTTP %d", code)
	}
	results := resp.Batch.EntityResults
	if len(results) != 2 {
		t.Fatalf("want 2, got %d", len(results))
	}
	if results[0].Entity.Properties["score"].GetIntegerValue() != 10 {
		t.Errorf("first score = %d, want 10", results[0].Entity.Properties["score"].GetIntegerValue())
	}
	if results[1].Entity.Properties["score"].GetIntegerValue() != 20 {
		t.Errorf("second score = %d, want 20", results[1].Entity.Properties["score"].GetIntegerValue())
	}
}

func TestDS_Query_CursorPagination(t *testing.T) {
	s := newDSTestServer(t)

	for i := int64(1); i <= 5; i++ {
		upsert(s, dsNameKey("Paged", fmt.Sprintf("e%d", i)), map[string]*datastorepb.Value{"n": dsIntVal(i)})
	}

	// Page 1: first 3 items.
	var resp1 datastorepb.RunQueryResponse
	if code := s.post("runQuery", &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunQueryRequest_Query{
			Query: &datastorepb.Query{
				Kind:  []*datastorepb.KindExpression{{Name: "Paged"}},
				Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "n"}, Direction: datastorepb.PropertyOrder_ASCENDING}},
				Limit: wrapperspb.Int32(3),
			},
		},
	}, &resp1); code != http.StatusOK {
		t.Fatalf("page1: HTTP %d", code)
	}
	if len(resp1.Batch.EntityResults) != 3 {
		t.Fatalf("page1: want 3, got %d", len(resp1.Batch.EntityResults))
	}

	// Page 2: start from cursor.
	var resp2 datastorepb.RunQueryResponse
	if code := s.post("runQuery", &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunQueryRequest_Query{
			Query: &datastorepb.Query{
				Kind:        []*datastorepb.KindExpression{{Name: "Paged"}},
				Order:       []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "n"}, Direction: datastorepb.PropertyOrder_ASCENDING}},
				StartCursor: resp1.Batch.EndCursor,
			},
		},
	}, &resp2); code != http.StatusOK {
		t.Fatalf("page2: HTTP %d", code)
	}
	if len(resp2.Batch.EntityResults) != 2 {
		t.Errorf("page2: want 2, got %d", len(resp2.Batch.EntityResults))
	}
}

// -- aggregation ------------------------------------------------------------

func TestDS_Count(t *testing.T) {
	s := newDSTestServer(t)

	for i := int64(1); i <= 4; i++ {
		upsert(s, dsNameKey("Widget", fmt.Sprintf("w%d", i)), map[string]*datastorepb.Value{"n": dsIntVal(i)})
	}

	var resp datastorepb.RunAggregationQueryResponse
	if code := s.post("runAggregationQuery", &datastorepb.RunAggregationQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{
			AggregationQuery: &datastorepb.AggregationQuery{
				QueryType: &datastorepb.AggregationQuery_NestedQuery{
					NestedQuery: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Widget"}}},
				},
				Aggregations: []*datastorepb.AggregationQuery_Aggregation{{
					Operator: &datastorepb.AggregationQuery_Aggregation_Count_{
						Count: &datastorepb.AggregationQuery_Aggregation_Count{},
					},
					Alias: "n",
				}},
			},
		},
	}, &resp); code != http.StatusOK {
		t.Fatalf("runAggregationQuery: HTTP %d", code)
	}
	if len(resp.Batch.AggregationResults) == 0 {
		t.Fatal("no aggregation results")
	}
	if got := resp.Batch.AggregationResults[0].AggregateProperties["n"].GetIntegerValue(); got != 4 {
		t.Errorf("count = %d, want 4", got)
	}
}

func TestDS_Sum(t *testing.T) {
	s := newDSTestServer(t)

	for name, v := range map[string]int64{"s1": 10, "s2": 20, "s3": 30} {
		upsert(s, dsNameKey("Summed", name), map[string]*datastorepb.Value{"v": dsIntVal(v)})
	}

	var resp datastorepb.RunAggregationQueryResponse
	if code := s.post("runAggregationQuery", &datastorepb.RunAggregationQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{
			AggregationQuery: &datastorepb.AggregationQuery{
				QueryType: &datastorepb.AggregationQuery_NestedQuery{
					NestedQuery: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Summed"}}},
				},
				Aggregations: []*datastorepb.AggregationQuery_Aggregation{{
					Operator: &datastorepb.AggregationQuery_Aggregation_Sum_{
						Sum: &datastorepb.AggregationQuery_Aggregation_Sum{Property: &datastorepb.PropertyReference{Name: "v"}},
					},
					Alias: "total",
				}},
			},
		},
	}, &resp); code != http.StatusOK {
		t.Fatalf("runAggregationQuery: HTTP %d", code)
	}
	if got := resp.Batch.AggregationResults[0].AggregateProperties["total"].GetIntegerValue(); got != 60 {
		t.Errorf("sum = %d, want 60", got)
	}
}

// -- transactions -----------------------------------------------------------

func TestDS_Transaction_Commit(t *testing.T) {
	s := newDSTestServer(t)

	key := dsNameKey("Account", "acc1")
	upsert(s, key, map[string]*datastorepb.Value{"balance": dsIntVal(100)})

	// Begin transaction.
	var beginResp datastorepb.BeginTransactionResponse
	if code := s.post("beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &beginResp); code != http.StatusOK {
		t.Fatalf("beginTransaction: HTTP %d", code)
	}
	txID := beginResp.Transaction

	// Lookup inside transaction.
	var lookResp datastorepb.LookupResponse
	s.post("lookup", &datastorepb.LookupRequest{ //nolint:errcheck
		ProjectId:   testProject,
		Keys:        []*datastorepb.Key{key},
		ReadOptions: &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_Transaction{Transaction: txID}},
	}, &lookResp)
	balance := lookResp.Found[0].Entity.Properties["balance"].GetIntegerValue()

	// Commit with updated balance.
	if code := s.post("commit", &datastorepb.CommitRequest{
		ProjectId:           testProject,
		Mode:                datastorepb.CommitRequest_TRANSACTIONAL,
		TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: txID},
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{
				Upsert: &datastorepb.Entity{
					Key:        key,
					Properties: map[string]*datastorepb.Value{"balance": dsIntVal(balance + 50)},
				},
			},
		}},
	}, &datastorepb.CommitResponse{}); code != http.StatusOK {
		t.Fatalf("commit: HTTP %d", code)
	}

	var verifyResp datastorepb.LookupResponse
	s.post("lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &verifyResp) //nolint:errcheck
	if got := verifyResp.Found[0].Entity.Properties["balance"].GetIntegerValue(); got != 150 {
		t.Errorf("balance = %d, want 150", got)
	}
}

func TestDS_Transaction_Rollback(t *testing.T) {
	s := newDSTestServer(t)

	key := dsNameKey("Account", "acc2")
	upsert(s, key, map[string]*datastorepb.Value{"balance": dsIntVal(200)})

	var beginResp datastorepb.BeginTransactionResponse
	s.post("beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &beginResp) //nolint:errcheck

	s.post("rollback", &datastorepb.RollbackRequest{ProjectId: testProject, Transaction: beginResp.Transaction}, &datastorepb.RollbackResponse{}) //nolint:errcheck

	var verifyResp datastorepb.LookupResponse
	s.post("lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &verifyResp) //nolint:errcheck
	if got := verifyResp.Found[0].Entity.Properties["balance"].GetIntegerValue(); got != 200 {
		t.Errorf("balance after rollback = %d, want 200", got)
	}
}

// -- auto-allocated IDs -----------------------------------------------------

func TestDS_AllocateIds(t *testing.T) {
	s := newDSTestServer(t)

	var resp datastorepb.AllocateIdsResponse
	if code := s.post("allocateIds", &datastorepb.AllocateIdsRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{dsIncompleteKey("Thing"), dsIncompleteKey("Thing")},
	}, &resp); code != http.StatusOK {
		t.Fatalf("allocateIds: HTTP %d", code)
	}
	if len(resp.Keys) != 2 {
		t.Fatalf("want 2 keys, got %d", len(resp.Keys))
	}
	id0 := resp.Keys[0].Path[0].GetId()
	id1 := resp.Keys[1].Path[0].GetId()
	if id0 == 0 || id1 == 0 {
		t.Errorf("allocated IDs must be non-zero, got %d and %d", id0, id1)
	}
	if id0 == id1 {
		t.Errorf("allocated IDs must be distinct, both = %d", id0)
	}
}

func TestDS_AllocateIds_ThenInsert(t *testing.T) {
	s := newDSTestServer(t)

	var allocResp datastorepb.AllocateIdsResponse
	s.post("allocateIds", &datastorepb.AllocateIdsRequest{ //nolint:errcheck
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{dsIncompleteKey("Gadget")},
	}, &allocResp)
	allocated := allocResp.Keys[0]

	upsert(s, allocated, map[string]*datastorepb.Value{"name": dsStrVal("widget")})

	var lookResp datastorepb.LookupResponse
	s.post("lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{allocated}}, &lookResp) //nolint:errcheck
	if len(lookResp.Found) != 1 {
		t.Errorf("found = %d, want 1 after insert with allocated ID", len(lookResp.Found))
	}
}

// -- mixed operations -------------------------------------------------------

func TestDS_MixedOps(t *testing.T) {
	s := newDSTestServer(t)

	keyA := dsNameKey("Actor", "alice")
	keyB := dsNameKey("Actor", "bob")
	keyC := dsNameKey("Actor", "carol")

	upsert(s, keyA, map[string]*datastorepb.Value{"score": dsIntVal(10)})
	upsert(s, keyB, map[string]*datastorepb.Value{"score": dsIntVal(20)})
	upsert(s, keyC, map[string]*datastorepb.Value{"score": dsIntVal(30)})

	// Bump alice to 50.
	upsert(s, keyA, map[string]*datastorepb.Value{"score": dsIntVal(50)})

	// Delete bob.
	s.post("commit", &datastorepb.CommitRequest{ //nolint:errcheck
		ProjectId: testProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{Delete: keyB}}},
	}, &datastorepb.CommitResponse{})

	// Query score >= 20 -> alice (50) and carol (30), ordered ascending.
	var resp datastorepb.RunQueryResponse
	if code := s.post("runQuery", &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunQueryRequest_Query{
			Query: &datastorepb.Query{
				Kind: []*datastorepb.KindExpression{{Name: "Actor"}},
				Filter: &datastorepb.Filter{
					FilterType: &datastorepb.Filter_PropertyFilter{
						PropertyFilter: &datastorepb.PropertyFilter{
							Property: &datastorepb.PropertyReference{Name: "score"},
							Op:       datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL,
							Value:    dsIntVal(20),
						},
					},
				},
				Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "score"}, Direction: datastorepb.PropertyOrder_ASCENDING}},
			},
		},
	}, &resp); code != http.StatusOK {
		t.Fatalf("runQuery: HTTP %d", code)
	}
	if len(resp.Batch.EntityResults) != 2 {
		t.Fatalf("want 2, got %d", len(resp.Batch.EntityResults))
	}
	s0 := resp.Batch.EntityResults[0].Entity.Properties["score"].GetIntegerValue()
	s1 := resp.Batch.EntityResults[1].Entity.Properties["score"].GetIntegerValue()
	if s0 != 30 || s1 != 50 {
		t.Errorf("scores = [%d %d], want [30 50]", s0, s1)
	}
}

// -- snapshot read ---------------------------------------------------------

func TestDS_SnapshotRead(t *testing.T) {
	s := newDSTestServer(t)

	key := dsNameKey("Event", "e1")
	upsert(s, key, map[string]*datastorepb.Value{"v": dsIntVal(1)})

	// Capture a snapshot timestamp after the first write.
	snapTime := timestamppb.Now()

	// Write a second entity after the snapshot.
	upsert(s, dsNameKey("Event", "e2"), map[string]*datastorepb.Value{"v": dsIntVal(2)})

	// Query at snapshot time - should only see e1.
	var resp datastorepb.RunQueryResponse
	if code := s.post("runQuery", &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		ReadOptions: &datastorepb.ReadOptions{
			ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: snapTime},
		},
		QueryType: &datastorepb.RunQueryRequest_Query{
			Query: &datastorepb.Query{
				Kind: []*datastorepb.KindExpression{{Name: "Event"}},
			},
		},
	}, &resp); code != http.StatusOK {
		t.Fatalf("runQuery at readTime: HTTP %d", code)
	}
	if n := len(resp.Batch.EntityResults); n != 1 {
		t.Errorf("snapshot read: want 1 entity, got %d", n)
	}
}

// -- OCC concurrent transaction --------------------------------------------

func TestDS_OCC_ConcurrentTransaction(t *testing.T) {
	s := newDSTestServer(t)

	key := dsNameKey("Counter", "c1")
	upsert(s, key, map[string]*datastorepb.Value{"n": dsIntVal(0)})

	// tx1 and tx2 both begin transactions.
	var begin1, begin2 datastorepb.BeginTransactionResponse
	if code := s.post("beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &begin1); code != http.StatusOK {
		t.Fatalf("beginTransaction tx1: HTTP %d", code)
	}
	if code := s.post("beginTransaction", &datastorepb.BeginTransactionRequest{ProjectId: testProject}, &begin2); code != http.StatusOK {
		t.Fatalf("beginTransaction tx2: HTTP %d", code)
	}

	// Both transactions read the same entity.
	lookupInTx := func(txID []byte) {
		var lr datastorepb.LookupResponse
		s.post("lookup", &datastorepb.LookupRequest{ //nolint:errcheck
			ProjectId:   testProject,
			Keys:        []*datastorepb.Key{key},
			ReadOptions: &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_Transaction{Transaction: txID}},
		}, &lr)
	}
	lookupInTx(begin1.Transaction)
	lookupInTx(begin2.Transaction)

	// tx2 commits first - succeeds.
	if code := s.post("commit", &datastorepb.CommitRequest{
		ProjectId:           testProject,
		Mode:                datastorepb.CommitRequest_TRANSACTIONAL,
		TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: begin2.Transaction},
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{
				Upsert: &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"n": dsIntVal(1)}},
			},
		}},
	}, &datastorepb.CommitResponse{}); code != http.StatusOK {
		t.Fatalf("tx2 commit: HTTP %d (should succeed)", code)
	}

	// tx1 commits - must fail with 409 (Aborted/OCC conflict).
	code := s.post("commit", &datastorepb.CommitRequest{
		ProjectId:           testProject,
		Mode:                datastorepb.CommitRequest_TRANSACTIONAL,
		TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: begin1.Transaction},
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{
				Upsert: &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"n": dsIntVal(2)}},
			},
		}},
	}, &datastorepb.CommitResponse{})
	if code != http.StatusConflict {
		t.Errorf("tx1 commit after concurrent write: want 409, got %d", code)
	}
}
