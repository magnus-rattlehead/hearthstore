package datastore

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

type seedRow struct {
	name  string
	props map[string]*datastorepb.Value
}

func seedKind(t *testing.T, s *Server, kind string, rows []seedRow) {
	t.Helper()
	for _, row := range rows {
		upsertEntity(t, s, dsEntity(dsKey(kind, row.name), row.props))
	}
}

func propFilter(prop string, op datastorepb.PropertyFilter_Operator, val *datastorepb.Value) *datastorepb.Filter {
	return &datastorepb.Filter{
		FilterType: &datastorepb.Filter_PropertyFilter{
			PropertyFilter: &datastorepb.PropertyFilter{
				Property: &datastorepb.PropertyReference{Name: prop},
				Op:       op,
				Value:    val,
			},
		},
	}
}

func andFilter(filters ...*datastorepb.Filter) *datastorepb.Filter {
	return &datastorepb.Filter{
		FilterType: &datastorepb.Filter_CompositeFilter{
			CompositeFilter: &datastorepb.CompositeFilter{
				Op:      datastorepb.CompositeFilter_AND,
				Filters: filters,
			},
		},
	}
}

func orFilter(filters ...*datastorepb.Filter) *datastorepb.Filter {
	return &datastorepb.Filter{
		FilterType: &datastorepb.Filter_CompositeFilter{
			CompositeFilter: &datastorepb.CompositeFilter{
				Op:      datastorepb.CompositeFilter_OR,
				Filters: filters,
			},
		},
	}
}

// runQuery executes a RunQuery against the test server.
func runQuery(t *testing.T, s *Server, q *datastorepb.Query) *datastorepb.RunQueryResponse {
	t.Helper()
	var resp datastorepb.RunQueryResponse
	mustPost(t, s, "runQuery", &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunQueryRequest_Query{Query: q},
	}, &resp)
	return &resp
}

// qKind sets q.Kind and runs the query.
func qKind(t *testing.T, s *Server, kind string, q *datastorepb.Query) *datastorepb.RunQueryResponse {
	t.Helper()
	if q == nil {
		q = &datastorepb.Query{}
	}
	q.Kind = []*datastorepb.KindExpression{{Name: kind}}
	return runQuery(t, s, q)
}

func TestRunQuery(t *testing.T) {
	s := newTestDsServer(t)

	tests := []struct {
		name   string
		seed   []seedRow
		filter *datastorepb.Filter
		order  []*datastorepb.PropertyOrder
		proj   []*datastorepb.Projection
		limit  int32
		offset int32
		wantN  int
		// run overrides the above fields for multi-step or non-count assertions.
		run func(t *testing.T, s *Server, kind string)
	}{
		{
			name: "kind_only",
			seed: []seedRow{
				{"w1", map[string]*datastorepb.Value{"c": dsStr("red")}},
				{"w2", map[string]*datastorepb.Value{"c": dsStr("blue")}},
				{"w3", map[string]*datastorepb.Value{"c": dsStr("green")}},
			},
			wantN: 3,
		},
		{
			name: "eq_string",
			seed: []seedRow{
				{"w1", map[string]*datastorepb.Value{"color": dsStr("red")}},
				{"w2", map[string]*datastorepb.Value{"color": dsStr("blue")}},
				{"w3", map[string]*datastorepb.Value{"color": dsStr("red")}},
			},
			filter: propFilter("color", datastorepb.PropertyFilter_EQUAL, dsStr("red")),
			wantN:  2,
		},
		{
			name: "eq_int",
			seed: []seedRow{
				{"e1", map[string]*datastorepb.Value{"v": dsInt(1)}},
				{"e2", map[string]*datastorepb.Value{"v": dsInt(2)}},
				{"e3", map[string]*datastorepb.Value{"v": dsInt(1)}},
			},
			filter: propFilter("v", datastorepb.PropertyFilter_EQUAL, dsInt(1)),
			wantN:  2,
		},
		{
			name: "eq_bool",
			seed: []seedRow{
				{"t1", map[string]*datastorepb.Value{"active": dsBool(true)}},
				{"t2", map[string]*datastorepb.Value{"active": dsBool(false)}},
				{"t3", map[string]*datastorepb.Value{"active": dsBool(true)}},
			},
			filter: propFilter("active", datastorepb.PropertyFilter_EQUAL, dsBool(true)),
			wantN:  2,
		},
		{
			name: "eq_null",
			seed: []seedRow{
				{"n1", map[string]*datastorepb.Value{"x": dsNull()}},
				{"n2", map[string]*datastorepb.Value{"x": dsStr("val")}},
			},
			filter: propFilter("x", datastorepb.PropertyFilter_EQUAL, dsNull()),
			wantN:  1,
		},
		{
			name: "eq_timestamp",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"ts": dsTimestamp(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))}},
				{"b", map[string]*datastorepb.Value{"ts": dsTimestamp(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))}},
			},
			filter: propFilter("ts", datastorepb.PropertyFilter_EQUAL,
				dsTimestamp(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))),
			wantN: 1,
		},
		{
			name: "eq_blob",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"data": dsBlob([]byte("hello"))}},
				{"b", map[string]*datastorepb.Value{"data": dsBlob([]byte("world"))}},
			},
			filter: propFilter("data", datastorepb.PropertyFilter_EQUAL, dsBlob([]byte("hello"))),
			wantN:  1,
		},
		{
			name: "eq_geopoint",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"loc": dsGeo(37.7749, -122.4194)}},
				{"b", map[string]*datastorepb.Value{"loc": dsGeo(40.7128, -74.0060)}},
			},
			filter: propFilter("loc", datastorepb.PropertyFilter_EQUAL, dsGeo(37.7749, -122.4194)),
			wantN:  1,
		},
		{
			name: "eq_entityval",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"emb": dsEntityVal(map[string]*datastorepb.Value{"name": dsStr("foo")})}},
				{"b", map[string]*datastorepb.Value{"emb": dsEntityVal(map[string]*datastorepb.Value{"name": dsStr("bar")})}},
			},
			filter: propFilter("emb", datastorepb.PropertyFilter_EQUAL,
				dsEntityVal(map[string]*datastorepb.Value{"name": dsStr("foo")})),
			wantN: 1,
		},
		{
			name: "eq_array_whole",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"tags": dsArray(dsStr("x"), dsStr("y"))}},
				{"b", map[string]*datastorepb.Value{"tags": dsArray(dsStr("p"), dsStr("q"))}},
			},
			filter: propFilter("tags", datastorepb.PropertyFilter_EQUAL, dsArray(dsStr("x"), dsStr("y"))),
			wantN:  1,
		},
		{
			name: "eq_keyval",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"ref": dsKeyVal(dsKey("Thing", "t1"))}},
				{"b", map[string]*datastorepb.Value{"ref": dsKeyVal(dsKey("Thing", "t2"))}},
			},
			filter: propFilter("ref", datastorepb.PropertyFilter_EQUAL, dsKeyVal(dsKey("Thing", "t1"))),
			wantN:  1,
		},
		{
			name: "gt_int",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"score": dsInt(10)}},
				{"b", map[string]*datastorepb.Value{"score": dsInt(20)}},
				{"c", map[string]*datastorepb.Value{"score": dsInt(30)}},
			},
			filter: propFilter("score", datastorepb.PropertyFilter_GREATER_THAN, dsInt(15)),
			wantN:  2,
		},
		{
			name: "lt_int",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"score": dsInt(10)}},
				{"b", map[string]*datastorepb.Value{"score": dsInt(20)}},
				{"c", map[string]*datastorepb.Value{"score": dsInt(30)}},
			},
			filter: propFilter("score", datastorepb.PropertyFilter_LESS_THAN, dsInt(20)),
			wantN:  1,
		},
		{
			name: "gte_int",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"score": dsInt(10)}},
				{"b", map[string]*datastorepb.Value{"score": dsInt(20)}},
				{"c", map[string]*datastorepb.Value{"score": dsInt(30)}},
			},
			filter: propFilter("score", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(20)),
			wantN:  2,
		},
		{
			name: "lte_int",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"score": dsInt(10)}},
				{"b", map[string]*datastorepb.Value{"score": dsInt(20)}},
				{"c", map[string]*datastorepb.Value{"score": dsInt(30)}},
			},
			filter: propFilter("score", datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL, dsInt(20)),
			wantN:  2,
		},
		{
			name: "neq_string",
			seed: []seedRow{
				{"w1", map[string]*datastorepb.Value{"color": dsStr("red")}},
				{"w2", map[string]*datastorepb.Value{"color": dsStr("blue")}},
				{"w3", map[string]*datastorepb.Value{"color": dsStr("red")}},
			},
			filter: propFilter("color", datastorepb.PropertyFilter_NOT_EQUAL, dsStr("red")),
			wantN:  1,
		},
		{
			name: "in_string",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"color": dsStr("red")}},
				{"b", map[string]*datastorepb.Value{"color": dsStr("blue")}},
				{"c", map[string]*datastorepb.Value{"color": dsStr("green")}},
			},
			filter: propFilter("color", datastorepb.PropertyFilter_IN,
				dsArray(dsStr("red"), dsStr("green"))),
			wantN: 2,
		},
		{
			name: "not_in_string",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"color": dsStr("red")}},
				{"b", map[string]*datastorepb.Value{"color": dsStr("blue")}},
				{"c", map[string]*datastorepb.Value{"color": dsStr("green")}},
			},
			filter: propFilter("color", datastorepb.PropertyFilter_NOT_IN,
				dsArray(dsStr("red"), dsStr("green"))),
			wantN: 1,
		},
		{
			name: "has_ancestor",
			run: func(t *testing.T, s *Server, kind string) {
				parentKind := "Par_" + kind
				parent := dsAncestorKey(parentKind, "p1")
				child := dsAncestorKey(parentKind, "p1", kind, "c1")
				grandchild := dsAncestorKey(parentKind, "p1", kind, "c1", kind+"GC", "g1")
				orphan := dsKey(kind, "orphan")

				upsertEntity(t, s, dsEntity(child, map[string]*datastorepb.Value{"x": dsInt(1)}))
				upsertEntity(t, s, dsEntity(grandchild, map[string]*datastorepb.Value{"x": dsInt(2)}))
				upsertEntity(t, s, dsEntity(orphan, map[string]*datastorepb.Value{"x": dsInt(3)}))

				resp := qKind(t, s, kind, &datastorepb.Query{
					Filter: &datastorepb.Filter{
						FilterType: &datastorepb.Filter_PropertyFilter{
							PropertyFilter: &datastorepb.PropertyFilter{
								Property: &datastorepb.PropertyReference{Name: "__key__"},
								Op:       datastorepb.PropertyFilter_HAS_ANCESTOR,
								Value:    &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: parent}},
							},
						},
					},
				})
				if n := len(resp.Batch.EntityResults); n != 1 {
					t.Errorf("has_ancestor: want 1 child, got %d", n)
				}
			},
		},
		{
			name: "array_contains_elem",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"tags": dsArray(dsStr("x"), dsStr("y"), dsStr("z"))}},
				{"b", map[string]*datastorepb.Value{"tags": dsArray(dsStr("p"), dsStr("q"))}},
			},
			filter: propFilter("tags", datastorepb.PropertyFilter_EQUAL, dsStr("y")),
			wantN:  1,
		},
		{
			name: "and_composite",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"color": dsStr("red"), "score": dsInt(10)}},
				{"b", map[string]*datastorepb.Value{"color": dsStr("red"), "score": dsInt(30)}},
				{"c", map[string]*datastorepb.Value{"color": dsStr("blue"), "score": dsInt(30)}},
			},
			filter: andFilter(
				propFilter("color", datastorepb.PropertyFilter_EQUAL, dsStr("red")),
				propFilter("score", datastorepb.PropertyFilter_GREATER_THAN, dsInt(20)),
			),
			wantN: 1,
		},
		{
			name: "or_composite",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"color": dsStr("red"), "score": dsInt(10)}},
				{"b", map[string]*datastorepb.Value{"color": dsStr("blue"), "score": dsInt(20)}},
				{"c", map[string]*datastorepb.Value{"color": dsStr("red"), "score": dsInt(30)}},
			},
			filter: orFilter(
				propFilter("color", datastorepb.PropertyFilter_EQUAL, dsStr("blue")),
				propFilter("score", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(30)),
			),
			wantN: 2,
		},
		{
			name: "order_desc",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"score": dsInt(10)}},
					{"b", map[string]*datastorepb.Value{"score": dsInt(30)}},
					{"c", map[string]*datastorepb.Value{"score": dsInt(20)}},
				})
				resp := qKind(t, s, kind, &datastorepb.Query{
					Order: []*datastorepb.PropertyOrder{
						{Property: &datastorepb.PropertyReference{Name: "score"}, Direction: datastorepb.PropertyOrder_DESCENDING},
					},
				})
				results := resp.Batch.EntityResults
				if len(results) != 3 {
					t.Fatalf("order_desc: want 3 results, got %d", len(results))
				}
				s0 := results[0].Entity.Properties["score"].GetIntegerValue()
				s1 := results[1].Entity.Properties["score"].GetIntegerValue()
				s2 := results[2].Entity.Properties["score"].GetIntegerValue()
				if s0 < s1 || s1 < s2 {
					t.Errorf("order_desc: not sorted descending: %d %d %d", s0, s1, s2)
				}
			},
		},
		{
			name: "order_multi",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"color": dsStr("red"), "score": dsInt(20)}},
					{"b", map[string]*datastorepb.Value{"color": dsStr("blue"), "score": dsInt(10)}},
					{"c", map[string]*datastorepb.Value{"color": dsStr("red"), "score": dsInt(10)}},
				})
				resp := qKind(t, s, kind, &datastorepb.Query{
					Order: []*datastorepb.PropertyOrder{
						{Property: &datastorepb.PropertyReference{Name: "color"}, Direction: datastorepb.PropertyOrder_ASCENDING},
						{Property: &datastorepb.PropertyReference{Name: "score"}, Direction: datastorepb.PropertyOrder_ASCENDING},
					},
				})
				results := resp.Batch.EntityResults
				if len(results) != 3 {
					t.Fatalf("order_multi: want 3 results, got %d", len(results))
				}
				// Expect: blue/10, red/10, red/20.
				want := []struct {
					color string
					score int64
				}{
					{"blue", 10}, {"red", 10}, {"red", 20},
				}
				for i, w := range want {
					got := results[i].Entity.Properties
					if got["color"].GetStringValue() != w.color || got["score"].GetIntegerValue() != w.score {
						t.Errorf("order_multi[%d]: want %s/%d, got %s/%d",
							i, w.color, w.score,
							got["color"].GetStringValue(), got["score"].GetIntegerValue())
					}
				}
			},
		},
		{
			name: "limit_basic",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"v": dsInt(1)}},
					{"b", map[string]*datastorepb.Value{"v": dsInt(2)}},
					{"c", map[string]*datastorepb.Value{"v": dsInt(3)}},
				})
				resp := qKind(t, s, kind, &datastorepb.Query{Limit: wrapperspb.Int32(2)})
				if n := len(resp.Batch.EntityResults); n != 2 {
					t.Errorf("limit_basic: want 2, got %d", n)
				}
				if resp.Batch.MoreResults != datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT {
					t.Errorf("limit_basic: want MORE_RESULTS_AFTER_LIMIT, got %v", resp.Batch.MoreResults)
				}
			},
		},
		{
			name: "offset_basic",
			seed: []seedRow{
				{"a", map[string]*datastorepb.Value{"v": dsInt(1)}},
				{"b", map[string]*datastorepb.Value{"v": dsInt(2)}},
				{"c", map[string]*datastorepb.Value{"v": dsInt(3)}},
			},
			offset: 2,
			wantN:  1,
		},
		{
			name: "cursor_pagination",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"v": dsInt(1)}},
					{"b", map[string]*datastorepb.Value{"v": dsInt(2)}},
					{"c", map[string]*datastorepb.Value{"v": dsInt(3)}},
				})
				r1 := qKind(t, s, kind, &datastorepb.Query{Limit: wrapperspb.Int32(2)})
				if n := len(r1.Batch.EntityResults); n != 2 {
					t.Fatalf("cursor_pagination page1: want 2, got %d", n)
				}
				r2 := qKind(t, s, kind, &datastorepb.Query{
					Limit:       wrapperspb.Int32(2),
					StartCursor: r1.Batch.EndCursor,
				})
				if n := len(r2.Batch.EntityResults); n != 1 {
					t.Errorf("cursor_pagination page2: want 1, got %d", n)
				}
			},
		},
		{
			name: "keys_only",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"score": dsInt(1)}},
					{"b", map[string]*datastorepb.Value{"score": dsInt(2)}},
				})
				resp := qKind(t, s, kind, &datastorepb.Query{
					Projection: []*datastorepb.Projection{
						{Property: &datastorepb.PropertyReference{Name: "__key__"}},
					},
				})
				for _, er := range resp.Batch.EntityResults {
					if len(er.Entity.Properties) > 0 {
						t.Error("keys_only: result should have no properties")
					}
					if er.Entity.Key == nil {
						t.Error("keys_only: result should have a key")
					}
				}
			},
		},
		{
			name: "projection",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"color": dsStr("red"), "score": dsInt(10)}},
					{"b", map[string]*datastorepb.Value{"color": dsStr("blue"), "score": dsInt(20)}},
				})
				resp := qKind(t, s, kind, &datastorepb.Query{
					Projection: []*datastorepb.Projection{
						{Property: &datastorepb.PropertyReference{Name: "color"}},
					},
				})
				for _, er := range resp.Batch.EntityResults {
					if _, ok := er.Entity.Properties["score"]; ok {
						t.Error("projection: result should not include 'score'")
					}
					if _, ok := er.Entity.Properties["color"]; !ok {
						t.Error("projection: result should include 'color'")
					}
				}
			},
		},
		{
			name: "snapshot_read",
			run: func(t *testing.T, s *Server, kind string) {
				upsertEntity(t, s, dsEntity(dsKey(kind, "before"), map[string]*datastorepb.Value{"v": dsInt(1)}))
				snapTime := time.Now()
				upsertEntity(t, s, dsEntity(dsKey(kind, "after"), map[string]*datastorepb.Value{"v": dsInt(2)}))

				var resp datastorepb.RunQueryResponse
				mustPost(t, s, "runQuery", &datastorepb.RunQueryRequest{
					ProjectId: testProject,
					ReadOptions: &datastorepb.ReadOptions{
						ConsistencyType: &datastorepb.ReadOptions_ReadTime{
							ReadTime: dsTimestamp(snapTime).GetTimestampValue(),
						},
					},
					QueryType: &datastorepb.RunQueryRequest_Query{
						Query: &datastorepb.Query{
							Kind: []*datastorepb.KindExpression{{Name: kind}},
						},
					},
				}, &resp)
				if n := len(resp.Batch.EntityResults); n != 1 {
					t.Errorf("snapshot_read: want 1 result at snapshot, got %d", n)
				}
				if n := len(resp.Batch.EntityResults); n == 1 {
					if name := resp.Batch.EntityResults[0].Entity.Key.Path[0].GetName(); name != "before" {
						t.Errorf("snapshot_read: want entity 'before', got %q", name)
					}
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kind := "K_" + tc.name
			if tc.run != nil {
				tc.run(t, s, kind)
				return
			}
			seedKind(t, s, kind, tc.seed)
			q := &datastorepb.Query{
				Filter:     tc.filter,
				Order:      tc.order,
				Projection: tc.proj,
				Offset:     tc.offset,
			}
			if tc.limit > 0 {
				q.Limit = wrapperspb.Int32(tc.limit)
			}
			resp := qKind(t, s, kind, q)
			if got := len(resp.Batch.EntityResults); got != tc.wantN {
				t.Errorf("want %d results, got %d", tc.wantN, got)
			}
		})
	}
}
