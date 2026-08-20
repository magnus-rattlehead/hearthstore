package datastore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func TestRunQueryBuildsAndUsesCompositeIndex(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	generated := filepath.Join(dir, "index.generated.yaml")
	server := NewWithIndexManager(store, NewIndexManager(store, generated))
	seedKind(t, server, "Invoice", []seedRow{
		{name: "old", props: map[string]*datastorepb.Value{"plan": dsStr("p1"), "state": dsStr("open"), "created_date": dsInt(1)}},
		{name: "new", props: map[string]*datastorepb.Value{"plan": dsStr("p1"), "state": dsStr("open"), "created_date": dsInt(2)}},
		{name: "other", props: map[string]*datastorepb.Value{"plan": dsStr("p2"), "state": dsStr("open"), "created_date": dsInt(3)}},
	})
	query := &datastorepb.Query{
		Kind:   []*datastorepb.KindExpression{{Name: "Invoice"}},
		Filter: andFilter(propFilter("plan", datastorepb.PropertyFilter_EQUAL, dsStr("p1")), propFilter("state", datastorepb.PropertyFilter_EQUAL, dsStr("open"))),
		Order:  []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "created_date"}, Direction: datastorepb.PropertyOrder_DESCENDING}},
	}
	first := runQuery(t, server, query)
	if len(first.Batch.EntityResults) != 2 {
		t.Fatalf("first query returned %d", len(first.Batch.EntityResults))
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		indexes, err := store.ListDsCompositeIndexes(testProject)
		if err != nil {
			t.Fatal(err)
		}
		if len(indexes) == 1 && indexes[0].State == storage.DsIndexReady {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("index did not become ready: %+v", indexes)
		}
		time.Sleep(10 * time.Millisecond)
	}
	second := runQuery(t, server, query)
	if got := second.Batch.EntityResults[0].Entity.Key.Path[0].GetName(); got != "new" {
		t.Fatalf("first result=%q, want new", got)
	}
	if _, err := os.Stat(generated); err != nil {
		t.Fatalf("generated index config: %v", err)
	}
}

func TestCompositeIndexCursorPagination(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manager := NewIndexManager(store, filepath.Join(dir, "index.generated.yaml"))
	server := NewWithIndexManager(store, manager)
	seedKind(t, server, "Invoice", []seedRow{
		{name: "old", props: map[string]*datastorepb.Value{"plan": dsStr("p1"), "state": dsStr("open"), "created_date": dsInt(1)}},
		{name: "new", props: map[string]*datastorepb.Value{"plan": dsStr("p1"), "state": dsStr("open"), "created_date": dsInt(2)}},
	})
	query := &datastorepb.Query{
		Kind:   []*datastorepb.KindExpression{{Name: "Invoice"}},
		Filter: andFilter(propFilter("plan", datastorepb.PropertyFilter_EQUAL, dsStr("p1")), propFilter("state", datastorepb.PropertyFilter_EQUAL, dsStr("open"))),
		Order:  []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "created_date"}, Direction: datastorepb.PropertyOrder_DESCENDING}},
		Limit:  wrapperspb.Int32(1),
	}
	definition, _ := queryIndexDefinition(query, false)
	definition.Project = testProject
	if _, _, err := store.EnsureDsCompositeIndex(context.Background(), definition, true); err != nil {
		t.Fatal(err)
	}

	first := runQuery(t, server, query)
	cursor, ok := decodeCursorFull(first.Batch.EndCursor)
	if !ok || cursor.V != 2 || cursor.I == "" || len(cursor.K) == 0 {
		t.Fatalf("cursor=%+v ok=%v", cursor, ok)
	}
	query.StartCursor = first.Batch.EndCursor
	second := runQuery(t, server, query)
	if got := second.Batch.EntityResults[0].Entity.Key.Path[0].GetName(); got != "old" {
		t.Fatalf("second page=%q", got)
	}

	query.StartCursor = []byte("VXNlclByb2ZpbGUvb2xk")
	_, err = server.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: query}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid cursor error=%v", err)
	}
}
