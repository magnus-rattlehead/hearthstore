package datastore

import (
	"encoding/base64"
	"fmt"
	"strings"
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
			name: "cursor_large_pages",
			run: func(t *testing.T, s *Server, kind string) {
				rows := make([]seedRow, 200)
				for i := range rows {
					rows[i] = seedRow{
						fmt.Sprintf("e%04d", i),
						map[string]*datastorepb.Value{"v": dsInt(int64(i))},
					}
				}
				seedKind(t, s, kind, rows)

				seen := make(map[string]bool, 200)
				var cursor []byte
				for page := 0; page < 4; page++ {
					resp := qKind(t, s, kind, &datastorepb.Query{
						Limit:       wrapperspb.Int32(50),
						StartCursor: cursor,
					})
					if n := len(resp.Batch.EntityResults); n != 50 {
						t.Errorf("page %d: want 50, got %d", page, n)
						return
					}
					for _, er := range resp.Batch.EntityResults {
						name := er.Entity.Key.Path[len(er.Entity.Key.Path)-1].GetName()
						if seen[name] {
							t.Errorf("duplicate entity %q on page %d", name, page)
						}
						seen[name] = true
					}
					cursor = resp.Batch.EndCursor
				}
				// 5th page must be empty with NO_MORE_RESULTS
				resp := qKind(t, s, kind, &datastorepb.Query{
					Limit:       wrapperspb.Int32(50),
					StartCursor: cursor,
				})
				if n := len(resp.Batch.EntityResults); n != 0 {
					t.Errorf("page5: want 0, got %d", n)
				}
				if resp.Batch.MoreResults != datastorepb.QueryResultBatch_NO_MORE_RESULTS {
					t.Errorf("page5: want NO_MORE_RESULTS, got %v", resp.Batch.MoreResults)
				}
				if len(seen) != 200 {
					t.Errorf("total distinct entities: want 200, got %d", len(seen))
				}
			},
		},
		{
			name: "cursor_with_eq_filter",
			run: func(t *testing.T, s *Server, kind string) {
				rows := make([]seedRow, 20)
				for i := range rows {
					status := "inactive"
					if i < 10 {
						status = "active"
					}
					rows[i] = seedRow{
						fmt.Sprintf("s%04d", i),
						map[string]*datastorepb.Value{"status": dsStr(status)},
					}
				}
				seedKind(t, s, kind, rows)

				filter := propFilter("status", datastorepb.PropertyFilter_EQUAL, dsStr("active"))
				seen := make(map[string]bool, 10)
				var cursor []byte
				for {
					resp := qKind(t, s, kind, &datastorepb.Query{
						Filter:      filter,
						Limit:       wrapperspb.Int32(3),
						StartCursor: cursor,
					})
					for _, er := range resp.Batch.EntityResults {
						name := er.Entity.Key.Path[len(er.Entity.Key.Path)-1].GetName()
						if seen[name] {
							t.Errorf("duplicate entity %q", name)
						}
						seen[name] = true
						if er.Entity.Properties["status"].GetStringValue() != "active" {
							t.Errorf("got non-active entity %q", name)
						}
					}
					if resp.Batch.MoreResults != datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT {
						break
					}
					cursor = resp.Batch.EndCursor
				}
				if len(seen) != 10 {
					t.Errorf("want 10 active entities, got %d", len(seen))
				}
			},
		},
		{
			name: "cursor_order_asc",
			run: func(t *testing.T, s *Server, kind string) {
				// Seed in reverse order to verify SQL sorts correctly
				rows := make([]seedRow, 20)
				for i := range rows {
					rows[i] = seedRow{
						fmt.Sprintf("o%04d", 19-i),
						map[string]*datastorepb.Value{"score": dsInt(int64(20 - i))},
					}
				}
				seedKind(t, s, kind, rows)

				order := []*datastorepb.PropertyOrder{
					{Property: &datastorepb.PropertyReference{Name: "score"}, Direction: datastorepb.PropertyOrder_ASCENDING},
				}
				var prevScore int64 = -1
				var cursor []byte
				total := 0
				for {
					resp := qKind(t, s, kind, &datastorepb.Query{
						Order:       order,
						Limit:       wrapperspb.Int32(5),
						StartCursor: cursor,
					})
					for _, er := range resp.Batch.EntityResults {
						score := er.Entity.Properties["score"].GetIntegerValue()
						if score <= prevScore {
							t.Errorf("order violated: score %d after %d (asc)", score, prevScore)
						}
						prevScore = score
						total++
					}
					if resp.Batch.MoreResults != datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT {
						break
					}
					cursor = resp.Batch.EndCursor
				}
				if total != 20 {
					t.Errorf("want 20 total, got %d", total)
				}
			},
		},
		{
			name: "cursor_order_desc",
			run: func(t *testing.T, s *Server, kind string) {
				rows := make([]seedRow, 20)
				for i := range rows {
					rows[i] = seedRow{
						fmt.Sprintf("d%04d", i),
						map[string]*datastorepb.Value{"score": dsInt(int64(i + 1))},
					}
				}
				seedKind(t, s, kind, rows)

				order := []*datastorepb.PropertyOrder{
					{Property: &datastorepb.PropertyReference{Name: "score"}, Direction: datastorepb.PropertyOrder_DESCENDING},
				}
				prevScore := int64(21) // sentinel: above max score
				var cursor []byte
				total := 0
				for {
					resp := qKind(t, s, kind, &datastorepb.Query{
						Order:       order,
						Limit:       wrapperspb.Int32(5),
						StartCursor: cursor,
					})
					for _, er := range resp.Batch.EntityResults {
						score := er.Entity.Properties["score"].GetIntegerValue()
						if score >= prevScore {
							t.Errorf("order violated: score %d after %d (desc)", score, prevScore)
						}
						prevScore = score
						total++
					}
					if resp.Batch.MoreResults != datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT {
						break
					}
					cursor = resp.Batch.EndCursor
				}
				if total != 20 {
					t.Errorf("want 20 total, got %d", total)
				}
			},
		},
		{
			name: "cursor_order_multi",
			run: func(t *testing.T, s *Server, kind string) {
				// 10 "a" entities + 10 "b" entities, each with scores 1–10
				var rows []seedRow
				for i := 1; i <= 10; i++ {
					rows = append(rows, seedRow{
						fmt.Sprintf("ma%04d", i),
						map[string]*datastorepb.Value{"cat": dsStr("a"), "score": dsInt(int64(i))},
					})
					rows = append(rows, seedRow{
						fmt.Sprintf("mb%04d", i),
						map[string]*datastorepb.Value{"cat": dsStr("b"), "score": dsInt(int64(i))},
					})
				}
				seedKind(t, s, kind, rows)

				// ORDER BY cat ASC, score DESC → a/10, a/9, ..., a/1, b/10, ..., b/1
				order := []*datastorepb.PropertyOrder{
					{Property: &datastorepb.PropertyReference{Name: "cat"}, Direction: datastorepb.PropertyOrder_ASCENDING},
					{Property: &datastorepb.PropertyReference{Name: "score"}, Direction: datastorepb.PropertyOrder_DESCENDING},
				}
				prevCat := ""
				prevScore := int64(11) // sentinel
				var cursor []byte
				total := 0
				for {
					resp := qKind(t, s, kind, &datastorepb.Query{
						Order:       order,
						Limit:       wrapperspb.Int32(5),
						StartCursor: cursor,
					})
					for _, er := range resp.Batch.EntityResults {
						cat := er.Entity.Properties["cat"].GetStringValue()
						score := er.Entity.Properties["score"].GetIntegerValue()
						if cat < prevCat || (cat == prevCat && score >= prevScore) {
							t.Errorf("order violated: %s/%d after %s/%d", cat, score, prevCat, prevScore)
						}
						if cat != prevCat {
							prevScore = 11 // reset when category advances
						}
						prevCat = cat
						prevScore = score
						total++
					}
					if resp.Batch.MoreResults != datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT {
						break
					}
					cursor = resp.Batch.EndCursor
				}
				if total != 20 {
					t.Errorf("want 20 total, got %d", total)
				}
			},
		},
		{
			name: "cursor_exact_boundary",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"v": dsInt(1)}},
					{"b", map[string]*datastorepb.Value{"v": dsInt(2)}},
					{"c", map[string]*datastorepb.Value{"v": dsInt(3)}},
					{"d", map[string]*datastorepb.Value{"v": dsInt(4)}},
					{"e", map[string]*datastorepb.Value{"v": dsInt(5)}},
					{"f", map[string]*datastorepb.Value{"v": dsInt(6)}},
					{"g", map[string]*datastorepb.Value{"v": dsInt(7)}},
					{"h", map[string]*datastorepb.Value{"v": dsInt(8)}},
					{"i", map[string]*datastorepb.Value{"v": dsInt(9)}},
					{"j", map[string]*datastorepb.Value{"v": dsInt(10)}},
				})
				r1 := qKind(t, s, kind, &datastorepb.Query{Limit: wrapperspb.Int32(5)})
				if n := len(r1.Batch.EntityResults); n != 5 {
					t.Fatalf("page1: want 5, got %d", n)
				}
				if r1.Batch.MoreResults != datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT {
					t.Errorf("page1: want MORE_RESULTS_AFTER_LIMIT, got %v", r1.Batch.MoreResults)
				}
				r2 := qKind(t, s, kind, &datastorepb.Query{
					Limit:       wrapperspb.Int32(5),
					StartCursor: r1.Batch.EndCursor,
				})
				if n := len(r2.Batch.EntityResults); n != 5 {
					t.Fatalf("page2: want 5, got %d", n)
				}
				if r2.Batch.MoreResults != datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT {
					t.Errorf("page2: want MORE_RESULTS_AFTER_LIMIT, got %v", r2.Batch.MoreResults)
				}
				r3 := qKind(t, s, kind, &datastorepb.Query{
					Limit:       wrapperspb.Int32(5),
					StartCursor: r2.Batch.EndCursor,
				})
				if n := len(r3.Batch.EntityResults); n != 0 {
					t.Errorf("page3: want 0, got %d", n)
				}
				if r3.Batch.MoreResults != datastorepb.QueryResultBatch_NO_MORE_RESULTS {
					t.Errorf("page3: want NO_MORE_RESULTS, got %v", r3.Batch.MoreResults)
				}
			},
		},
		{
			name: "order_by_key_asc",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"charlie", map[string]*datastorepb.Value{"v": dsInt(3)}},
					{"alpha", map[string]*datastorepb.Value{"v": dsInt(1)}},
					{"bravo", map[string]*datastorepb.Value{"v": dsInt(2)}},
					{"delta", map[string]*datastorepb.Value{"v": dsInt(4)}},
					{"echo", map[string]*datastorepb.Value{"v": dsInt(5)}},
				})
				r1 := qKind(t, s, kind, &datastorepb.Query{
					Order: []*datastorepb.PropertyOrder{{
						Property:  &datastorepb.PropertyReference{Name: "__key__"},
						Direction: datastorepb.PropertyOrder_ASCENDING,
					}},
					Limit: wrapperspb.Int32(3),
				})
				if n := len(r1.Batch.EntityResults); n != 3 {
					t.Fatalf("page1: want 3, got %d", n)
				}
				names1 := [3]string{
					r1.Batch.EntityResults[0].Entity.Key.Path[0].GetName(),
					r1.Batch.EntityResults[1].Entity.Key.Path[0].GetName(),
					r1.Batch.EntityResults[2].Entity.Key.Path[0].GetName(),
				}
				if names1 != [3]string{"alpha", "bravo", "charlie"} {
					t.Errorf("page1 order: got %v", names1)
				}
				r2 := qKind(t, s, kind, &datastorepb.Query{
					Order: []*datastorepb.PropertyOrder{{
						Property:  &datastorepb.PropertyReference{Name: "__key__"},
						Direction: datastorepb.PropertyOrder_ASCENDING,
					}},
					Limit:       wrapperspb.Int32(3),
					StartCursor: r1.Batch.EndCursor,
				})
				if n := len(r2.Batch.EntityResults); n != 2 {
					t.Fatalf("page2: want 2, got %d", n)
				}
				names2 := [2]string{
					r2.Batch.EntityResults[0].Entity.Key.Path[0].GetName(),
					r2.Batch.EntityResults[1].Entity.Key.Path[0].GetName(),
				}
				if names2 != [2]string{"delta", "echo"} {
					t.Errorf("page2 order: got %v", names2)
				}
			},
		},
		{
			name: "order_by_key_desc",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"alpha", map[string]*datastorepb.Value{"v": dsInt(1)}},
					{"bravo", map[string]*datastorepb.Value{"v": dsInt(2)}},
					{"charlie", map[string]*datastorepb.Value{"v": dsInt(3)}},
					{"delta", map[string]*datastorepb.Value{"v": dsInt(4)}},
					{"echo", map[string]*datastorepb.Value{"v": dsInt(5)}},
				})
				r1 := qKind(t, s, kind, &datastorepb.Query{
					Order: []*datastorepb.PropertyOrder{{
						Property:  &datastorepb.PropertyReference{Name: "__key__"},
						Direction: datastorepb.PropertyOrder_DESCENDING,
					}},
					Limit: wrapperspb.Int32(3),
				})
				if n := len(r1.Batch.EntityResults); n != 3 {
					t.Fatalf("page1: want 3, got %d", n)
				}
				names1 := [3]string{
					r1.Batch.EntityResults[0].Entity.Key.Path[0].GetName(),
					r1.Batch.EntityResults[1].Entity.Key.Path[0].GetName(),
					r1.Batch.EntityResults[2].Entity.Key.Path[0].GetName(),
				}
				if names1 != [3]string{"echo", "delta", "charlie"} {
					t.Errorf("page1 order: got %v", names1)
				}
				r2 := qKind(t, s, kind, &datastorepb.Query{
					Order: []*datastorepb.PropertyOrder{{
						Property:  &datastorepb.PropertyReference{Name: "__key__"},
						Direction: datastorepb.PropertyOrder_DESCENDING,
					}},
					Limit:       wrapperspb.Int32(3),
					StartCursor: r1.Batch.EndCursor,
				})
				if n := len(r2.Batch.EntityResults); n != 2 {
					t.Fatalf("page2: want 2, got %d", n)
				}
				names2 := [2]string{
					r2.Batch.EntityResults[0].Entity.Key.Path[0].GetName(),
					r2.Batch.EntityResults[1].Entity.Key.Path[0].GetName(),
				}
				if names2 != [2]string{"bravo", "alpha"} {
					t.Errorf("page2 order: got %v", names2)
				}
			},
		},
		{
			name: "desc_null_sort_cursor",
			run: func(t *testing.T, s *Server, kind string) {
				// Entities with explicit null score sort last in DESC; paginate through them.
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"score": dsInt(10)}},
					{"b", map[string]*datastorepb.Value{"score": dsInt(5)}},
					{"c", map[string]*datastorepb.Value{"score": dsNull()}},
					{"d", map[string]*datastorepb.Value{"score": dsNull()}},
					{"e", map[string]*datastorepb.Value{"score": dsNull()}},
				})
				order := []*datastorepb.PropertyOrder{{
					Property:  &datastorepb.PropertyReference{Name: "score"},
					Direction: datastorepb.PropertyOrder_DESCENDING,
				}}
				seen := map[string]bool{}
				cursor := []byte(nil)
				for page := 0; page < 10; page++ {
					q := &datastorepb.Query{Order: order, Limit: wrapperspb.Int32(2), StartCursor: cursor}
					r := qKind(t, s, kind, q)
					for _, er := range r.Batch.EntityResults {
						name := er.Entity.Key.Path[0].GetName()
						if seen[name] {
							t.Errorf("duplicate entity %q on page %d", name, page+1)
						}
						seen[name] = true
					}
					if r.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
						break
					}
					cursor = r.Batch.EndCursor
				}
				if len(seen) != 5 {
					t.Errorf("desc_null_sort_cursor: want 5 distinct entities, got %d: %v", len(seen), seen)
				}
			},
		},
		{
			// Simulate the REST/JSON transport double-encoding: pjsonMarshal encodes the
			// EndCursor bytes field as StdB64; the Python gcloud.rest client then applies
			// _cursor_to_urlsafe and sends the result directly (no _cursor_from_urlsafe).
			// pjsonUnmarshal URL-safe-decodes that, giving StdB64(URLSafeB64(JSON)) bytes
			// as StartCursor. Without the double-decode fix, decodeCursorFull would treat
			// URLSafeB64(JSON) as a plain path, and the keyset condition would return 0 rows.
			name: "rest_double_encoded_cursor",
			run: func(t *testing.T, s *Server, kind string) {
				rows := make([]seedRow, 30)
				for i := range rows {
					rows[i] = seedRow{
						fmt.Sprintf("e%04d", i),
						map[string]*datastorepb.Value{"score": dsInt(int64(i + 1))},
					}
				}
				seedKind(t, s, kind, rows)

				order := []*datastorepb.PropertyOrder{
					{Property: &datastorepb.PropertyReference{Name: "score"}, Direction: datastorepb.PropertyOrder_DESCENDING},
				}
				seen := make(map[string]bool, 30)
				var cursor []byte
				for page := 0; page < 10; page++ {
					resp := qKind(t, s, kind, &datastorepb.Query{
						Order:       order,
						Limit:       wrapperspb.Int32(10),
						StartCursor: cursor,
					})
					for _, er := range resp.Batch.EntityResults {
						name := er.Entity.Key.Path[len(er.Entity.Key.Path)-1].GetName()
						if seen[name] {
							t.Errorf("duplicate entity %q on page %d", name, page+1)
						}
						seen[name] = true
					}
					if resp.Batch.MoreResults != datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT {
						break
					}
					// Simulate REST double-encoding: pjsonMarshal encodes EndCursor bytes as
					// StdB64, Python applies urlsafe substitution, pjsonUnmarshal URL-safe-decodes.
					// Net result received by decodeCursorFull: StdB64(original EndCursor bytes).
					raw := resp.Batch.EndCursor
					stdEncoded := base64.StdEncoding.EncodeToString(raw)
					urlSafe := strings.NewReplacer("+", "-", "/", "_").Replace(stdEncoded)
					decoded, err := base64.URLEncoding.DecodeString(urlSafe)
					if err != nil {
						t.Fatalf("simulated REST re-encode failed: %v", err)
					}
					cursor = decoded
				}
				if len(seen) != 30 {
					t.Errorf("rest_double_encoded_cursor: want 30 entities, got %d", len(seen))
				}
			},
		},
		{
			name: "multi_database_isolation",
			run: func(t *testing.T, s *Server, kind string) {
				const dbA = "(default)"
				const dbB = "mydb"
				// Seed one entity in each database.
				for _, db := range []string{dbA, dbB} {
					db := db
					key := &datastorepb.Key{
						PartitionId: &datastorepb.PartitionId{ProjectId: testProject, DatabaseId: db},
						Path:        []*datastorepb.Key_PathElement{{Kind: kind, IdType: &datastorepb.Key_PathElement_Name{Name: "e1"}}},
					}
					mustPost(t, s, "commit", &datastorepb.CommitRequest{
						ProjectId:  testProject,
						DatabaseId: db,
						Mutations: []*datastorepb.Mutation{
							{Operation: &datastorepb.Mutation_Upsert{Upsert: &datastorepb.Entity{
								Key:        key,
								Properties: map[string]*datastorepb.Value{"db": dsStr(db)},
							}}},
						},
					}, &datastorepb.CommitResponse{})
				}
				// Query each database; each should return exactly one entity.
				for _, db := range []string{dbA, dbB} {
					var resp datastorepb.RunQueryResponse
					mustPost(t, s, "runQuery", &datastorepb.RunQueryRequest{
						ProjectId:  testProject,
						DatabaseId: db,
						QueryType: &datastorepb.RunQueryRequest_Query{
							Query: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}},
						},
					}, &resp)
					if n := len(resp.Batch.EntityResults); n != 1 {
						t.Errorf("db=%q: want 1 entity, got %d", db, n)
						continue
					}
					gotDB := resp.Batch.EntityResults[0].Entity.Properties["db"].GetStringValue()
					if gotDB != db {
						t.Errorf("db=%q: entity has db=%q", db, gotDB)
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
