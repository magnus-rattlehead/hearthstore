package datastore

import (
	"context"
	"reflect"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestPropertyMetadataAncestorAndTypes(t *testing.T) {
	s := newTestDsServer(t)
	values := dsArray(dsNull(), dsInt(1), dsTimestamp(time.Unix(1, 0)), dsDouble(2), dsBool(true), dsBlob([]byte("x")), dsStr("x"), dsKeyVal(dsKey("Other", "one")), dsGeo(1, 2))
	upsertEntity(t, s, dsEntity(dsKey("MetadataTypes", "one"), map[string]*datastorepb.Value{"mixed": values, "nested": dsEntityVal(map[string]*datastorepb.Value{"leaf": dsBool(true)})}))
	upsertEntity(t, s, dsEntity(dsKey("OtherMetadataTypes", "one"), map[string]*datastorepb.Value{"foreign": dsInt(1)}))
	ancestor := dsKey("__kind__", "MetadataTypes")
	response := runQueryNs(t, s, "", &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "__property__"}}, Filter: propFilter("__key__", datastorepb.PropertyFilter_HAS_ANCESTOR, dsKeyVal(ancestor))})
	got := map[string][]string{}
	for _, row := range response.Batch.EntityResults {
		if len(row.Entity.Key.Path) != 2 || row.Entity.Key.Path[0].GetName() != "MetadataTypes" {
			t.Fatalf("wrong ancestor: %v", row.Entity.Key)
		}
		name := row.Entity.Key.Path[1].GetName()
		for _, value := range row.Entity.Properties["property_representation"].GetArrayValue().GetValues() {
			got[name] = append(got[name], value.GetStringValue())
		}
	}
	want := map[string][]string{"mixed": {"BOOLEAN", "DOUBLE", "INT64", "NULL", "POINT", "REFERENCE", "STRING"}, "nested": {"STRING"}, "nested.leaf": {"BOOLEAN"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("metadata=%v, want %v", got, want)
	}
}

func TestMetadataQueryContract(t *testing.T) {
	s := newTestDsServer(t)
	for _, kind := range []string{"__namespace__", "__kind__", "__property__"} {
		for _, q := range []*datastorepb.Query{
			{Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "__key__"}, Direction: datastorepb.PropertyOrder_DESCENDING}}},
			{Filter: propFilter("property_representation", datastorepb.PropertyFilter_EQUAL, dsStr("STRING"))},
		} {
			q.Kind = []*datastorepb.KindExpression{{Name: kind}}
			_, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("%s metadata query=%v: %v", kind, q, err)
			}
		}
		_, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL, Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(dsKey(kind, "forbidden"), nil)}}}})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("write reserved metadata kind %s: %v", kind, err)
		}
	}
}

func TestMetadataNamespaceAndRepresentations(t *testing.T) {
	s := newTestDsServer(t)
	excluded := dsStr("not indexed")
	excluded.ExcludeFromIndexes = true
	upsertEntity(t, s, dsEntity(dsKeyNs("", "DefaultOnly", "one"), map[string]*datastorepb.Value{"secret": dsInt(1)}))
	first := dsKeyNs("tenant", "TenantOnly", "one")
	second := dsKeyNs("tenant", "TenantOnly", "two")
	upsertEntity(t, s, dsEntity(first, map[string]*datastorepb.Value{"x": dsArray(dsInt(1), dsInt(1), dsStr("a")), "hidden": excluded}))
	upsertEntity(t, s, dsEntity(second, map[string]*datastorepb.Value{"x": dsInt(2), "y": dsStr("b")}))
	t.Run("kind_isolation", func(t *testing.T) {
		response := runQueryNs(t, s, "tenant", &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "__kind__"}}})
		rows := response.Batch.EntityResults
		if len(rows) != 1 || rows[0].Entity.Key.Path[0].GetName() != "TenantOnly" || rows[0].Entity.Key.PartitionId.NamespaceId != "tenant" {
			t.Fatalf("kind metadata = %v", rows)
		}
	})
	read := func(t *testing.T, options *datastorepb.ReadOptions) map[string][]string {
		t.Helper()
		out := map[string][]string{}
		var cursor []byte
		for page := 0; page < 5; page++ {
			q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "__property__"}}, Limit: wrapperspb.Int32(1), StartCursor: cursor}
			response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, PartitionId: &datastorepb.PartitionId{NamespaceId: "tenant"}, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}, ReadOptions: options})
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range response.Batch.EntityResults {
				key := row.Entity.Key
				if key.PartitionId.NamespaceId != "tenant" || len(key.Path) != 2 || key.Path[0].GetName() != "TenantOnly" {
					t.Fatalf("property key = %v", key)
				}
				var reps []string
				for _, value := range row.Entity.Properties["property_representation"].GetArrayValue().GetValues() {
					reps = append(reps, value.GetStringValue())
				}
				out[key.Path[1].GetName()] = reps
			}
			if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
				return out
			}
			cursor = response.Batch.EndCursor
		}
		t.Fatal("metadata pagination failed to finish")
		return nil
	}
	t.Run("property_lifecycle", func(t *testing.T) {
		want := map[string][]string{"x": {"INT64", "STRING"}, "y": {"STRING"}}
		if got := read(t, nil); !reflect.DeepEqual(got, want) {
			t.Fatalf("properties = %v, want %v", got, want)
		}
		tx, err := s.grpc.BeginTransaction(context.Background(), &datastorepb.BeginTransactionRequest{ProjectId: testProject, TransactionOptions: &datastorepb.TransactionOptions{Mode: &datastorepb.TransactionOptions_ReadOnly_{ReadOnly: &datastorepb.TransactionOptions_ReadOnly{}}}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := s.grpc.Rollback(context.Background(), &datastorepb.RollbackRequest{ProjectId: testProject, Transaction: tx.Transaction}); err != nil {
				t.Error(err)
			}
		}()
		if _, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{Delete: first}}}}); err != nil {
			t.Fatal(err)
		}
		if got := read(t, nil); !reflect.DeepEqual(got, map[string][]string{"x": {"INT64"}, "y": {"STRING"}}) {
			t.Fatalf("properties after delete = %v", got)
		}
		if got := read(t, &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_Transaction{Transaction: tx.Transaction}}); !reflect.DeepEqual(got, want) {
			t.Fatalf("snapshot properties = %v, want %v", got, want)
		}
		upsertEntity(t, s, dsEntity(second, map[string]*datastorepb.Value{"y": dsStr("updated")}))
		if got := read(t, nil); !reflect.DeepEqual(got, map[string][]string{"y": {"STRING"}}) {
			t.Fatalf("properties after replacement = %v", got)
		}
	})
}

// dsKeyNs builds a named key in a specific namespace.
func dsKeyNs(namespace, kind, name string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{
			ProjectId:   testProject,
			NamespaceId: namespace,
		},
		Path: []*datastorepb.Key_PathElement{
			{Kind: kind, IdType: &datastorepb.Key_PathElement_Name{Name: name}},
		},
	}
}

// runQueryNs executes a RunQuery scoped to the given namespace.
func runQueryNs(t *testing.T, s *Server, namespace string, q *datastorepb.Query) *datastorepb.RunQueryResponse {
	t.Helper()
	var resp datastorepb.RunQueryResponse
	mustPost(t, s, "runQuery", &datastorepb.RunQueryRequest{
		ProjectId:   testProject,
		PartitionId: &datastorepb.PartitionId{NamespaceId: namespace},
		QueryType:   &datastorepb.RunQueryRequest_Query{Query: q},
	}, &resp)
	return &resp
}

func TestNamespaceIsolation(t *testing.T) {
	type row struct {
		namespace, name string
		value           int64
	}
	for _, tc := range []struct {
		name string
		rows []row
	}{
		{"single_namespace", []row{{"ns-a", "w1", 10}}},
		{"same_key", []row{{"ns-a", "w1", 10}, {"ns-b", "w1", 20}}},
		{"multiple_matches", []row{{"ns-a", "w1", 10}, {"ns-a", "w2", 20}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestDsServer(t)
			for _, row := range tc.rows {
				upsertEntity(t, s, dsEntity(dsKeyNs(row.namespace, "Widget", row.name),
					map[string]*datastorepb.Value{"v": dsInt(row.value), "color": dsStr("red")}))
			}
			for _, namespace := range []string{"ns-a", "ns-b", ""} {
				t.Run("namespace="+namespace, func(t *testing.T) {
					want := map[string]int64{}
					for _, row := range tc.rows {
						if row.namespace == namespace {
							want[row.name] = row.value
						}
					}
					for _, filtered := range []bool{false, true} {
						q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Widget"}}}
						if filtered {
							q.Filter = propFilter("color", datastorepb.PropertyFilter_EQUAL, dsStr("red"))
						}
						response := runQueryNs(t, s, namespace, q)
						got := map[string]int64{}
						for _, result := range response.Batch.EntityResults {
							key := result.Entity.Key
							if key.GetPartitionId().GetNamespaceId() != namespace {
								t.Fatalf("filtered=%t: wrong namespace: %v", filtered, key)
							}
							got[key.Path[0].GetName()] = result.Entity.Properties["v"].GetIntegerValue()
						}
						if len(response.Batch.EntityResults) != len(want) || !reflect.DeepEqual(got, want) {
							t.Fatalf("filtered=%t: results=%v, want %v", filtered, response.Batch.EntityResults, want)
						}
					}
					key := dsKeyNs(namespace, "Widget", "w1")
					var lookup datastorepb.LookupResponse
					mustPost(t, s, "lookup", &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}}, &lookup)
					if value, exists := want["w1"]; exists {
						if len(lookup.Found) != 1 || len(lookup.Missing) != 0 || lookup.Found[0].Entity.Properties["v"].GetIntegerValue() != value {
							t.Fatalf("lookup=%v, want value %d", &lookup, value)
						}
					} else if len(lookup.Found) != 0 || len(lookup.Missing) != 1 {
						t.Fatalf("lookup=%v, want one missing key", &lookup)
					}
				})
			}
		})
	}
}
