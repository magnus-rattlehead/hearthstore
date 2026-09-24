//go:build integration

package tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	dspkg "github.com/magnus-rattlehead/hearthstore/internal/datastore"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

const testProject = "test-proj"

func TestDatastoreRESTWorkflow(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	server := httptest.NewServer(dspkg.New(store).Handler())
	t.Cleanup(server.Close)
	key := restNameKey("RESTSmoke", "entity")

	postREST(t, server, "commit", &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{
			Upsert: &datastorepb.Entity{
				Key: key,
				Properties: map[string]*datastorepb.Value{
					"score": {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 42}},
				},
			},
		}}},
	}, &datastorepb.CommitResponse{})

	var lookup datastorepb.LookupResponse
	postREST(t, server, "lookup", &datastorepb.LookupRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{key},
	}, &lookup)
	if len(lookup.Found) != 1 || lookup.Found[0].Entity.Properties["score"].GetIntegerValue() != 42 {
		t.Fatalf("lookup response = %v, want one entity with score 42", &lookup)
	}

	var query datastorepb.RunQueryResponse
	postREST(t, server, "runQuery", &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
			Kind: []*datastorepb.KindExpression{{Name: "RESTSmoke"}},
			Filter: &datastorepb.Filter{FilterType: &datastorepb.Filter_PropertyFilter{
				PropertyFilter: &datastorepb.PropertyFilter{
					Property: &datastorepb.PropertyReference{Name: "score"},
					Op:       datastorepb.PropertyFilter_EQUAL,
					Value:    &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 42}},
				},
			}},
		}},
	}, &query)
	if got := len(query.Batch.EntityResults); got != 1 {
		t.Fatalf("query results = %d, want 1", got)
	}

	postREST(t, server, "commit", &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{Delete: key}}},
	}, &datastorepb.CommitResponse{})

	lookup.Reset()
	postREST(t, server, "lookup", &datastorepb.LookupRequest{
		ProjectId: testProject,
		Keys:      []*datastorepb.Key{key},
	}, &lookup)
	if len(lookup.Missing) != 1 {
		t.Fatalf("missing results = %d, want 1 after delete", len(lookup.Missing))
	}
}

func postREST(t *testing.T, server *httptest.Server, method string, request, response proto.Message) {
	t.Helper()
	body, err := protojson.Marshal(request)
	if err != nil {
		t.Fatalf("marshal %s request: %v", method, err)
	}
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		fmt.Sprintf("%s/v1/projects/%s:%s", server.URL, testProject, method),
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("create %s request: %v", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", method, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s response: %v", method, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s: status %d, body: %s", method, resp.StatusCode, raw)
	}
	if response != nil {
		if err := protojson.Unmarshal(raw, response); err != nil {
			t.Fatalf("unmarshal %s response: %v\nbody: %s", method, err, raw)
		}
	}
}

func restNameKey(kind, name string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: testProject},
		Path: []*datastorepb.Key_PathElement{{
			Kind:   kind,
			IdType: &datastorepb.Key_PathElement_Name{Name: name},
		}},
	}
}
