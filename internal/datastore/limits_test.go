package datastore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestCompatibilityUnindexedValueBoundaries(t *testing.T) {
	s := newTestDsServer(t)
	for _, shape := range []string{"string", "blob", "utf8", "array", "embedded"} {
		for _, size := range []int{1_000_001, 1_048_487, 1_048_488} {
			t.Run(fmt.Sprintf("%s/%d", shape, size), func(t *testing.T) {
				v := dsStr(strings.Repeat("x", size))
				if shape == "utf8" {
					v = dsStr(strings.Repeat("é", size/2) + strings.Repeat("x", size%2))
				}
				if shape == "blob" {
					v = &datastorepb.Value{ValueType: &datastorepb.Value_BlobValue{BlobValue: bytes.Repeat([]byte{1}, size)}}
				}
				v.ExcludeFromIndexes = true
				if shape == "array" {
					v = dsArray(v)
				}
				if shape == "embedded" {
					v.ExcludeFromIndexes = false
					v = dsEntityVal(map[string]*datastorepb.Value{"y": v})
					v.ExcludeFromIndexes = true
				}
				key := &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: "p"}, Path: []*datastorepb.Key_PathElement{{Kind: "K", IdType: &datastorepb.Key_PathElement_Name{Name: "k"}}}}
				entity := dsEntity(key, map[string]*datastorepb.Value{"x": v})
				if proto.Size(entity) > maxEntityBytes {
					t.Fatal("fixture exceeds entity limit")
				}
				_, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: "p", Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL, Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: entity}}}})
				want := codes.OK
				if size > 1_048_487 {
					want = codes.InvalidArgument
				}
				if status.Code(err) != want {
					t.Errorf("code=%v want=%v: %v", status.Code(err), want, err)
				}
			})
		}
	}
	for _, container := range []bool{false, true} {
		v := dsArray(dsInt(1))
		v.ExcludeFromIndexes = container
		v.GetArrayValue().Values[0].ExcludeFromIndexes = !container
		_, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL, Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(dsKey("ArrayFlags", "a"), map[string]*datastorepb.Value{"x": v})}}}})
		want := codes.OK
		if container {
			want = codes.InvalidArgument
		}
		if status.Code(err) != want {
			t.Errorf("container=%t error=%v want=%v", container, err, want)
		}
	}
	t.Run("legacy_vector_exclusion_preserved", func(t *testing.T) {
		// Java dispatches meaning 31 before ordinary array validation.
		v := dsArray(&datastorepb.Value{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: 1.5}})
		v.Meaning, v.ExcludeFromIndexes = 31, true
		_, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL, Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(dsKey("ArrayFlags", "vector"), map[string]*datastorepb.Value{"x": v})}}}})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestBoundaryValueAndKeyContract(t *testing.T) {
	s := newTestDsServer(t)
	base := dsEntity(dsKey("BoundaryValues", "row"), map[string]*datastorepb.Value{"x": dsInt(1)})
	cases := []struct {
		name   string
		change func(*datastorepb.Entity)
		valid  bool
	}{
		{"unset", func(e *datastorepb.Entity) { e.Properties["x"] = &datastorepb.Value{} }, false},
		{"nil", func(e *datastorepb.Entity) { e.Properties["x"] = nil }, false},
		{"array_unset", func(e *datastorepb.Entity) { e.Properties["x"] = dsArray(&datastorepb.Value{}) }, false},
		{"entity_unset", func(e *datastorepb.Entity) { e.Properties["x"] = dsEntityVal(map[string]*datastorepb.Value{"y": {}}) }, false},
		{"empty_name", func(e *datastorepb.Entity) { e.Properties[""] = dsInt(1) }, false},
		{"reserved_name", func(e *datastorepb.Entity) { e.Properties["__reserved__"] = dsInt(1) }, false},
		{"projection_meaning", func(e *datastorepb.Entity) { e.Properties["x"].Meaning = 18 }, false},
		{"nested_projection_meaning", func(e *datastorepb.Entity) {
			v := dsInt(1)
			v.Meaning = 18
			e.Properties["x"] = dsArray(dsEntityVal(map[string]*datastorepb.Value{"y": v}))
		}, false},
		{"reserved_kind", func(e *datastorepb.Entity) { e.Key.Path[0].Kind = "__reserved__" }, false},
		{"reserved_key_name", func(e *datastorepb.Entity) {
			e.Key.Path[0].IdType = &datastorepb.Key_PathElement_Name{Name: "__reserved__"}
		}, false},
		{"invalid_name_utf8", func(e *datastorepb.Entity) { e.Properties[string([]byte{255})] = dsInt(1) }, false},
		{"invalid_string_utf8", func(e *datastorepb.Entity) { e.Properties["x"] = dsStr(string([]byte{255})) }, false},
		{"nested_array", func(e *datastorepb.Entity) { e.Properties["x"] = dsArray(dsArray(dsInt(1))) }, false},
		{"empty_array", func(e *datastorepb.Entity) { e.Properties["x"] = dsArray() }, true},
		{"mixed_array", func(e *datastorepb.Entity) {
			e.Properties["x"] = dsArray(dsInt(1), dsStr("a"), &datastorepb.Value{ValueType: &datastorepb.Value_NullValue{}})
		}, true},
		{"array_entity", func(e *datastorepb.Entity) {
			e.Properties["x"] = dsArray(dsEntityVal(map[string]*datastorepb.Value{"y": dsArray(dsInt(1))}))
		}, true},
		{"zero_id", func(e *datastorepb.Entity) { e.Key.Path[0].IdType = &datastorepb.Key_PathElement_Id{} }, false},
		{"negative_id", func(e *datastorepb.Entity) { e.Key.Path[0].IdType = &datastorepb.Key_PathElement_Id{Id: -1} }, true},
	}
	for _, size := range []int{1499, 1500, 1501} {
		for _, target := range []string{"property", "array_property", "kind", "key_name", "string"} {
			cases = append(cases, struct {
				name   string
				change func(*datastorepb.Entity)
				valid  bool
			}{fmt.Sprintf("%s_%d", target, size), func(e *datastorepb.Entity) {
				name := strings.Repeat("x", size)
				switch target {
				case "property":
					e.Properties = map[string]*datastorepb.Value{name: dsInt(1)}
				case "array_property":
					e.Properties = map[string]*datastorepb.Value{name: dsArray(dsInt(1))}
				case "kind":
					e.Key.Path[0].Kind = name
				case "key_name":
					e.Key.Path[0].IdType = &datastorepb.Key_PathElement_Name{Name: name}
				case "string":
					e.Properties["x"] = dsStr(name)
				}
			}, size <= 1500})
		}
	}
	for _, depth := range []int{99, 100, 101} {
		cases = append(cases, struct {
			name   string
			change func(*datastorepb.Entity)
			valid  bool
		}{fmt.Sprintf("key_depth_%d", depth), func(e *datastorepb.Entity) {
			e.Key.Path = nil
			for range depth {
				e.Key.Path = append(e.Key.Path, &datastorepb.Key_PathElement{Kind: "K", IdType: &datastorepb.Key_PathElement_Id{Id: 1}})
			}
		}, depth <= 100})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := proto.Clone(base).(*datastorepb.Entity)
			tc.change(e)
			marker := dsEntity(dsKey("BoundaryAtomic", tc.name), nil)
			_, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL, Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: marker}}, {Operation: &datastorepb.Mutation_Upsert{Upsert: e}}}})
			if tc.valid {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("error=%v want InvalidArgument", err)
			}
			response, lookupErr := s.grpc.Lookup(context.Background(), &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{marker.Key}})
			if lookupErr != nil || len(response.GetFound()) != 0 {
				t.Fatalf("rejected batch wrote marker: %v %v", response, lookupErr)
			}
		})
	}
}

func TestEntityNestingBoundary(t *testing.T) {
	for _, depth := range []int{19, 20, 21} {
		properties := map[string]*datastorepb.Value{"leaf": dsInt(1)}
		for range depth {
			properties = map[string]*datastorepb.Value{"n": dsEntityVal(properties)}
		}
		err := validateEntityLimits(dsEntity(dsKey("DepthBoundary", "one"), properties))
		if depth <= 20 && err != nil || depth > 20 && status.Code(err) != codes.InvalidArgument {
			t.Errorf("depth=%d error=%v", depth, err)
		}
	}
}

func TestIndexedValueCountExcludesContainers(t *testing.T) {
	values := make([]*datastorepb.Value, maxIndexedValues)
	for i := range values {
		values[i] = dsInt(int64(i))
	}
	entity := dsEntity(dsKey("LimitBoundary", "one"), map[string]*datastorepb.Value{"nested": {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"values": dsArray(values...)}}}}})
	if err := validateEntityLimits(entity); err != nil {
		t.Fatalf("20,000 indexed scalars: %v", err)
	}
	entity.Properties["extra"] = dsInt(1)
	if err := validateEntityLimits(entity); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("20,001 indexed scalars: %v", err)
	}
}

// Datastore query documentation forbids projecting equality-filtered properties.
// IN is a disjunction of equalities; the prohibition also applies under OR.
func TestProjectionFilterContract(t *testing.T) {
	s := newTestDsServer(t)
	t.Run("duplicate_projection", func(t *testing.T) {
		q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "ProjectionContract"}}, Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "x"}}, {Property: &datastorepb.PropertyReference{Name: "x"}}}}
		_, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("duplicate projection: %v, want InvalidArgument", err)
		}
	})
	for _, tc := range []struct {
		name   string
		filter *datastorepb.Filter
		want   codes.Code
	}{
		{"equal", propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)), codes.InvalidArgument},
		{"in", propFilter("x", datastorepb.PropertyFilter_IN, dsArray(dsInt(1), dsInt(2))), codes.InvalidArgument},
		{"or_equal", orFilter(propFilter("y", datastorepb.PropertyFilter_EQUAL, dsInt(2)), propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1))), codes.InvalidArgument},
		{"other_equal", propFilter("y", datastorepb.PropertyFilter_EQUAL, dsInt(1)), codes.OK},
		{"range", propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(1)), codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "ProjectionContract"}}, Filter: tc.filter, Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "x"}}}}
			for _, explain := range []*datastorepb.ExplainOptions{nil, {}} {
				_, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}, ExplainOptions: explain})
				if status.Code(err) != tc.want {
					t.Fatalf("query explain=%v: %v, want %s", explain, err, tc.want)
				}
				_, err = s.grpc.RunAggregationQuery(context.Background(), &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: q}, Aggregations: []*datastorepb.AggregationQuery_Aggregation{countAgg("n")}}}, ExplainOptions: explain})
				if status.Code(err) != tc.want {
					t.Fatalf("aggregation explain=%v: %v, want %s", explain, err, tc.want)
				}
			}
		})
	}
}

func TestQueryAdmissionCancellationTransports(t *testing.T) {
	for _, transport := range []string{"REST", "gRPC"} {
		t.Run(transport, func(t *testing.T) {
			s := newTestDsServer(t)
			s.grpc.querySlots = make(chan struct{}, 1)
			seedKind(t, s, "Admission", []seedRow{{"one", map[string]*datastorepb.Value{"n": dsInt(1)}}})
			entered, finished := make(chan struct{}, 2), make(chan struct{}, 2)
			g := grpc.NewServer()
			datastorepb.RegisterDatastoreServer(g, s.grpc)
			t.Cleanup(g.Stop)
			rest := s.Handler()
			server := httptest.NewServer(h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				entered <- struct{}{}
				defer func() { finished <- struct{}{} }()
				if strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
					g.ServeHTTP(w, r)
				} else {
					rest.ServeHTTP(w, r)
				}
			}), &http2.Server{}))
			t.Cleanup(server.Close)
			connection, err := grpc.NewClient(strings.TrimPrefix(server.URL, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := connection.Close(); err != nil {
					t.Error(err)
				}
			})
			request := &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Admission"}}}}}
			encoded, err := protojson.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			invoke := func(ctx context.Context) (*datastorepb.RunQueryResponse, error) {
				if transport == "gRPC" {
					return datastorepb.NewDatastoreClient(connection).RunQuery(ctx, request)
				}
				r, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/projects/"+testProject+":runQuery", bytes.NewReader(encoded))
				if err != nil {
					return nil, err
				}
				r.Header.Set("Content-Type", "application/json")
				response, err := http.DefaultClient.Do(r)
				if err != nil {
					return nil, err
				}
				body, readErr := io.ReadAll(response.Body)
				if err := errors.Join(readErr, response.Body.Close()); err != nil {
					return nil, err
				}
				if response.StatusCode != http.StatusOK {
					return nil, fmt.Errorf("REST status %d: %s", response.StatusCode, body)
				}
				var result datastorepb.RunQueryResponse
				return &result, protojson.Unmarshal(body, &result)
			}
			ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			s.grpc.querySlots <- struct{}{} // A different query owns the only slot.
			queued, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := invoke(queued); done <- err }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("request did not reach live server")
			}
			cancel()
			select {
			case err := <-done:
				if transport == "gRPC" && status.Code(err) != codes.Canceled || transport == "REST" && !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled request error=%v", err)
				}
			case <-ctx.Done():
				t.Fatal("canceled client remained blocked")
			}
			select {
			case <-finished:
			case <-ctx.Done():
				t.Fatal("canceled server handler remained blocked")
			}
			if len(s.grpc.querySlots) != 1 {
				t.Fatal("canceled waiter released another query's slot")
			}
			<-s.grpc.querySlots
			response, err := invoke(ctx)
			if err != nil || len(response.GetBatch().GetEntityResults()) != 1 || response.Batch.EntityResults[0].Entity.Properties["n"].GetIntegerValue() != 1 {
				t.Fatalf("following query response=%v error=%v", response, err)
			}
		})
	}
}

func TestQueryPreparationHonorsCancellation(t *testing.T) {
	filter := propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1))
	for range 400 {
		filter = andFilter(filter)
	}
	_, _, _, err := prepareQueryExecution(&cancelAfterFirstCheck{}, &datastorepb.Query{Filter: filter}, "")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("preparation error=%v, want cancellation", err)
	}
}

type workCancelContext struct {
	context.Context
	work   *storage.QueryWork
	kind   storage.WorkKind
	after  uint64
	cancel context.CancelFunc
}

func (c *workCancelContext) Err() error {
	if c.work.Snapshot()[c.kind] >= c.after {
		c.cancel()
	}
	return c.Context.Err()
}

func TestProjectionSortStopsComparingAfterCancellation(t *testing.T) {
	ctx, work := storage.WithQueryWork(context.Background(), 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx = &workCancelContext{Context: ctx, work: work, kind: storage.WorkComparisons, after: 1, cancel: cancel}
	var values []*datastorepb.Value
	for i := 255; i >= 0; i-- {
		values = append(values, dsInt(int64(i)))
	}
	err := visitProjectionSelection(ctx, dsEntity(dsKey("SortCancel", "one"), map[string]*datastorepb.Value{"x": dsArray(values...)}), []string{"x"}, func(*datastorepb.Entity, []int, bool) bool { t.Fatal("emitted canceled projection"); return false })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if got := work.Snapshot()[storage.WorkComparisons]; got != 1 {
		t.Fatalf("continued comparing after cancellation: %d", got)
	}
}

func TestQueryWorkYieldsPreserveCorrelatedProjection(t *testing.T) {
	s := newTestDsServer(t)
	const kind = "WorkProjection"
	seedKind(t, s, kind, []seedRow{{"one", map[string]*datastorepb.Value{"x": dsArray(dsInt(1), dsInt(4)), "y": dsArray(dsInt(2), dsInt(5))}}})
	q, condition, err := prepareQueryCondition(&datastorepb.Query{
		Kind:       []*datastorepb.KindExpression{{Name: kind}},
		Filter:     orFilter(andFilter(propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(3)), propFilter("y", datastorepb.PropertyFilter_GREATER_THAN, dsInt(4))), andFilter(propFilter("x", datastorepb.PropertyFilter_LESS_THAN, dsInt(2)), propFilter("y", datastorepb.PropertyFilter_LESS_THAN, dsInt(3)))),
		Order:      []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}}, {Property: &datastorepb.PropertyReference{Name: "y"}}},
		Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "x"}}, {Property: &datastorepb.PropertyReference{Name: "y"}}},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	condition.branches = nil // Exercise the exact correlated executor, not DNF.
	var previous []uint64
	for _, quantum := range []uint64{1024, 1} {
		ctx := context.WithValue(context.Background(), queryConditionContextKey{}, preparedQueryCondition{condition: condition})
		ctx, work := storage.WithQueryWork(ctx, quantum)
		response, err := s.grpc.RunQuery(ctx, &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
		if err != nil {
			t.Fatal(err)
		}
		var got [][2]int64
		for _, row := range response.Batch.EntityResults {
			got = append(got, [2]int64{row.Entity.Properties["x"].GetIntegerValue(), row.Entity.Properties["y"].GetIntegerValue()})
		}
		if !slices.Equal(got, [][2]int64{{1, 2}, {4, 5}}) {
			t.Fatalf("quantum=%d tuples=%v", quantum, got)
		}
		stats := work.Snapshot()
		// Four candidate tuples (two rejected), then two scalar output projections.
		if stats[storage.WorkAttempts] == 0 || stats[storage.WorkComparisons] == 0 || stats[storage.WorkDecodedBytes] == 0 || stats[storage.WorkProjectionTuples] != 6 {
			t.Fatalf("unaccounted work: %v", stats)
		}
		if quantum == 1 && stats[storage.WorkYields] != stats[storage.WorkAttempts] {
			t.Fatalf("not every checkpoint yielded: %v", stats)
		}
		if previous != nil && !slices.Equal(previous, stats[:storage.WorkYields]) {
			t.Fatalf("yield replayed work: %v / %v", previous, stats)
		}
		previous = append([]uint64(nil), stats[:storage.WorkYields]...)
	}
	t.Run("zero_results_still_count", func(t *testing.T) {
		q, condition, err := prepareQueryCondition(&datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}, Filter: propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(99))}, "")
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.WithValue(context.Background(), queryConditionContextKey{}, preparedQueryCondition{condition: condition})
		ctx, work := storage.WithQueryWork(ctx, 1)
		response, err := s.grpc.RunQuery(ctx, &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
		if err != nil {
			t.Fatal(err)
		}
		stats := work.Snapshot()
		if len(response.Batch.EntityResults) != 0 || stats[storage.WorkIndexEntries] != 1 || stats[storage.WorkDecodedBytes] == 0 || stats[storage.WorkAttempts] == 0 {
			t.Fatalf("rows=%d work=%v", len(response.Batch.EntityResults), stats)
		}
	})
	t.Run("covering_projection_decode", func(t *testing.T) {
		ctx, work := storage.WithQueryWork(context.Background(), 1)
		q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}, Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}}}, Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "x"}}}}
		response, err := s.grpc.RunQuery(ctx, &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
		if err != nil {
			t.Fatal(err)
		}
		var got []int64
		for _, row := range response.Batch.EntityResults {
			got = append(got, row.Entity.Properties["x"].GetIntegerValue())
		}
		if !slices.Equal(got, []int64{1, 4}) || work.Snapshot()[storage.WorkDecodedBytes] == 0 {
			t.Fatalf("tuples=%v work=%v", got, work.Snapshot())
		}
	})
}

func TestAggregationWorkPersistsAcrossResponsePages(t *testing.T) {
	s := newTestDsServer(t)
	s.grpc.querySlots = make(chan struct{}, 1)
	if err := s.grpc.store.ConfigureExactQueries(2<<20, 1); err != nil {
		t.Fatal(err)
	}
	bulkSeedLongPaths(t, s, 2000)
	ctx, work := storage.WithQueryWork(context.Background(), 1)
	response, err := s.grpc.RunAggregationQuery(ctx, &datastorepb.RunAggregationQueryRequest{
		ProjectId: testProject, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true},
		QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{
			QueryType:    &datastorepb.AggregationQuery_NestedQuery{NestedQuery: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Widget"}}}},
			Aggregations: []*datastorepb.AggregationQuery_Aggregation{countAgg("count"), sumAgg("sum", "score")},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	properties := response.Batch.AggregationResults[0].AggregateProperties
	if properties["count"].GetIntegerValue() != 2000 || properties["sum"].GetIntegerValue() != 1_999_000 {
		t.Fatalf("aggregate=%v", properties)
	}
	// Long keys remain in covering rows and their cursors, forcing internal
	// pages even though unreferenced source properties are no longer fetched.
	stats := work.Snapshot()
	if stats[storage.WorkDecodedBytes] < 2000*1000 || stats[storage.WorkAttempts] == 0 {
		t.Fatalf("work reset between pages: %v", stats)
	}
	for _, counter := range []struct {
		name  string
		value uint64
	}{{"work_attempts", stats[storage.WorkAttempts]}, {"work_decoded_bytes", stats[storage.WorkDecodedBytes]}} {
		if got := response.ExplainMetrics.ExecutionStats.DebugStats.Fields[counter.name].GetStringValue(); got != strconv.FormatUint(counter.value, 10) {
			t.Fatalf("%s=%q want %d", counter.name, got, counter.value)
		}
	}
	if len(s.grpc.querySlots) != 0 {
		t.Fatal("aggregation leaked admission")
	}
}

func TestCanceledQueryReleasesScratchAndAdmission(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := store.ConfigureExactQueries(2<<20, 1); err != nil {
		t.Fatal(err)
	}
	s := NewWithOptions(store, NewIndexManager(store), Options{QueryConcurrency: 1})
	var values []*datastorepb.Value
	for i := range 100 {
		values = append(values, dsInt(int64(i)))
	}
	seedKind(t, s, "CancelWork", []seedRow{{"one", map[string]*datastorepb.Value{"x": dsArray(values...)}}})
	q, condition, err := prepareQueryCondition(&datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "CancelWork"}}, Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}}}}, "")
	if err != nil {
		t.Fatal(err)
	}
	base := context.WithValue(context.Background(), queryConditionContextKey{}, preparedQueryCondition{condition: condition})
	ctx, work := storage.WithQueryWork(base, 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx = &workCancelContext{Context: ctx, work: work, kind: storage.WorkAttempts, after: 32, cancel: cancel}
	req := &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}}
	response, err := s.grpc.RunQuery(ctx, req)
	if !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
		t.Fatalf("cancel error=%v", err)
	}
	if response != nil || len(s.grpc.querySlots) != 0 {
		t.Fatal("canceled query returned partial success or leaked admission")
	}
	files, err := os.ReadDir(filepath.Join(root, "scratch"))
	if err != nil || len(files) != 0 {
		t.Fatalf("query scratch remains: %v, %v", files, err)
	}
	response, err = s.grpc.RunQuery(base, req)
	if err != nil || len(response.GetBatch().GetEntityResults()) != 1 {
		t.Fatalf("following query failed: %v, %v", response, err)
	}
}

func TestAggregationChecksCancellationBeforeAccumulatingPage(t *testing.T) {
	s := newTestDsServer(t)
	seedKind(t, s, "CancelAggregate", []seedRow{{"one", map[string]*datastorepb.Value{"n": dsInt(1)}}})
	ctx, work := storage.WithQueryWork(context.Background(), 1024)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx = &workCancelContext{Context: ctx, work: work, kind: storage.WorkDecodedBytes, after: 1, cancel: cancel}
	response, err := s.grpc.RunAggregationQuery(ctx, &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "CancelAggregate"}}}}, Aggregations: []*datastorepb.AggregationQuery_Aggregation{sumAgg("sum", "n")}}}})
	if !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
		t.Fatalf("error=%v response=%v", err, response)
	}
	if response != nil {
		t.Fatal("canceled aggregation returned partial success")
	}
}

func TestValidateCommitLimitsRejectsOversizedDatastoreWrites(t *testing.T) {
	entity := &datastorepb.Entity{
		Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "Thing", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}},
		Properties: map[string]*datastorepb.Value{
			"indexed": {ValueType: &datastorepb.Value_StringValue{StringValue: strings.Repeat("x", maxIndexedValueBytes+1)}},
		},
	}
	req := &datastorepb.CommitRequest{Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: entity}}}}
	if err := validateCommitLimits(req); err == nil {
		t.Fatal("expected an oversized indexed value to be rejected")
	}
}

func TestTransactionReadBudgetChargesExactLogicalKeyBytes(t *testing.T) {
	server := &GRPCServer{txns: map[string]txEntry{"tx": {}}}
	first := txReadKey{path: strings.Repeat("a", 6<<20)}
	if err := server.recordTransactionReads("tx", map[txReadKey]int64{first: 1}); err != nil {
		t.Fatal(err)
	}
	if got := server.txns["tx"].readBytes; got != len(first.path) {
		t.Fatalf("charged bytes=%d, want %d", got, len(first.path))
	}
	second := txReadKey{path: strings.Repeat("b", 5<<20)}
	if err := server.recordTransactionReads("tx", map[txReadKey]int64{second: 1}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("second read error=%v, want ResourceExhausted", err)
	}
}
