package datastore

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	latlng "google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// newTestDsServer creates a Server backed by an in-memory (temp dir) SQLite store.
func newTestDsServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	t.Cleanup(func() {
		store.Close()
		os.RemoveAll(dir)
	})
	return New(store)
}

const testProject = "test-proj"

// dsKey builds a named key with a single kind+name path element.
func dsKey(kind, name string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: testProject},
		Path: []*datastorepb.Key_PathElement{
			{Kind: kind, IdType: &datastorepb.Key_PathElement_Name{Name: name}},
		},
	}
}

// dsKeyID builds a key with a numeric ID.
func dsKeyID(kind string, id int64) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: testProject},
		Path: []*datastorepb.Key_PathElement{
			{Kind: kind, IdType: &datastorepb.Key_PathElement_Id{Id: id}},
		},
	}
}

// dsAncestorKey builds a multi-element key from alternating kind/id-or-name pairs.
// e.g. dsAncestorKey("Parent", "p1", "Child", "c1")
func dsAncestorKey(elems ...interface{}) *datastorepb.Key {
	if len(elems)%2 != 0 {
		panic("dsAncestorKey: need even number of args (kind, id/name pairs)")
	}
	var path []*datastorepb.Key_PathElement
	for i := 0; i < len(elems); i += 2 {
		kind := elems[i].(string)
		pe := &datastorepb.Key_PathElement{Kind: kind}
		switch v := elems[i+1].(type) {
		case string:
			pe.IdType = &datastorepb.Key_PathElement_Name{Name: v}
		case int64:
			pe.IdType = &datastorepb.Key_PathElement_Id{Id: v}
		case int:
			pe.IdType = &datastorepb.Key_PathElement_Id{Id: int64(v)}
		}
		path = append(path, pe)
	}
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: testProject},
		Path:        path,
	}
}

// dsEntity builds an entity with the given key and properties.
func dsEntity(key *datastorepb.Key, props map[string]*datastorepb.Value) *datastorepb.Entity {
	return &datastorepb.Entity{Key: key, Properties: props}
}

func dsStr(s string) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: s}}
}

func dsInt(i int64) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: i}}
}

func dsBool(b bool) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_BooleanValue{BooleanValue: b}}
}

func dsDouble(f float64) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: f}}
}

func dsGeo(lat, lng float64) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_GeoPointValue{
		GeoPointValue: &latlng.LatLng{Latitude: lat, Longitude: lng},
	}}
}

func dsTimestamp(ts time.Time) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_TimestampValue{
		TimestampValue: timestamppb.New(ts),
	}}
}

func dsBlob(b []byte) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_BlobValue{BlobValue: b}}
}

func dsKeyVal(key *datastorepb.Key) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: key}}
}

func dsArray(vals ...*datastorepb.Value) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_ArrayValue{
		ArrayValue: &datastorepb.ArrayValue{Values: vals},
	}}
}

func dsEntityVal(props map[string]*datastorepb.Value) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{
		EntityValue: &datastorepb.Entity{Properties: props},
	}}
}

func dsNull() *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_NullValue{}}
}

// doPost calls the server's HTTP handler with a POST request and unmarshals the response.
func doPost(t *testing.T, s *Server, path string, reqMsg proto.Message, respMsg proto.Message) *http.Response {
	t.Helper()
	var body []byte
	if reqMsg != nil {
		var err error
		body, err = pjsonMarshal.Marshal(reqMsg)
		if err != nil {
			t.Fatalf("doPost marshal: %v", err)
		}
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	resp := w.Result()
	if respMsg != nil && resp.StatusCode == 200 {
		if err := pjsonUnmarshal.Unmarshal(w.Body.Bytes(), respMsg); err != nil {
			t.Fatalf("doPost unmarshal (status %d): %v\nbody: %s", resp.StatusCode, err, w.Body.Bytes())
		}
	}
	return resp
}

// projectURL builds the /v1/projects/{project}:{method} URL.
func projectURL(method string) string {
	return "/v1/projects/" + testProject + ":" + method
}

// mustPost asserts status 200 and unmarshals the response.
func mustPost(t *testing.T, s *Server, method string, req proto.Message, resp proto.Message) {
	t.Helper()
	var body []byte
	if req != nil {
		var err error
		body, err = pjsonMarshal.Marshal(req)
		if err != nil {
			t.Fatalf("mustPost marshal: %v", err)
		}
	}
	hr := httptest.NewRequest(http.MethodPost, projectURL(method), bytes.NewReader(body))
	hr.Header.Set("Content-Type", "application/json")
	rw := httptest.NewRecorder()
	s.Handler().ServeHTTP(rw, hr)
	if rw.Code != 200 {
		t.Fatalf("POST %s: status %d, body: %s", method, rw.Code, rw.Body.Bytes())
	}
	if resp != nil {
		if err := pjsonUnmarshal.Unmarshal(rw.Body.Bytes(), resp); err != nil {
			t.Fatalf("mustPost unmarshal: %v\nbody: %s", err, rw.Body.Bytes())
		}
	}
}

// upsertEntity is a test helper that upserts a single entity.
func upsertEntity(t *testing.T, s *Server, entity *datastorepb.Entity) {
	t.Helper()
	req := &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mutations: []*datastorepb.Mutation{
			{Operation: &datastorepb.Mutation_Upsert{Upsert: entity}},
		},
	}
	var resp datastorepb.CommitResponse
	mustPost(t, s, "commit", req, &resp)
}
