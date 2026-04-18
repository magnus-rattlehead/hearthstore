package datastore

import (
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

func countAgg(alias string) *datastorepb.AggregationQuery_Aggregation {
	return &datastorepb.AggregationQuery_Aggregation{
		Alias: alias,
		Operator: &datastorepb.AggregationQuery_Aggregation_Count_{
			Count: &datastorepb.AggregationQuery_Aggregation_Count{},
		},
	}
}

func sumAgg(alias, prop string) *datastorepb.AggregationQuery_Aggregation {
	return &datastorepb.AggregationQuery_Aggregation{
		Alias: alias,
		Operator: &datastorepb.AggregationQuery_Aggregation_Sum_{
			Sum: &datastorepb.AggregationQuery_Aggregation_Sum{
				Property: &datastorepb.PropertyReference{Name: prop},
			},
		},
	}
}

func avgAgg(alias, prop string) *datastorepb.AggregationQuery_Aggregation {
	return &datastorepb.AggregationQuery_Aggregation{
		Alias: alias,
		Operator: &datastorepb.AggregationQuery_Aggregation_Avg_{
			Avg: &datastorepb.AggregationQuery_Aggregation_Avg{
				Property: &datastorepb.PropertyReference{Name: prop},
			},
		},
	}
}

func runAggKind(t *testing.T, s *Server, kind string, filter *datastorepb.Filter, aggs []*datastorepb.AggregationQuery_Aggregation) *datastorepb.RunAggregationQueryResponse {
	t.Helper()
	q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}, Filter: filter}
	var resp datastorepb.RunAggregationQueryResponse
	mustPost(t, s, "runAggregationQuery", &datastorepb.RunAggregationQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{
			AggregationQuery: &datastorepb.AggregationQuery{
				QueryType:    &datastorepb.AggregationQuery_NestedQuery{NestedQuery: q},
				Aggregations: aggs,
			},
		},
	}, &resp)
	return &resp
}

func TestAggregation(t *testing.T) {
	s := newTestDsServer(t)

	tests := []struct {
		name string
		run  func(t *testing.T, s *Server, kind string)
	}{
		{
			name: "count_empty",
			run: func(t *testing.T, s *Server, kind string) {
				resp := runAggKind(t, s, kind, nil, []*datastorepb.AggregationQuery_Aggregation{countAgg("n")})
				if n := resp.Batch.AggregationResults[0].AggregateProperties["n"].GetIntegerValue(); n != 0 {
					t.Errorf("want 0, got %d", n)
				}
			},
		},
		{
			name: "count_all",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"score": dsInt(10)}},
					{"b", map[string]*datastorepb.Value{"score": dsInt(20)}},
					{"c", map[string]*datastorepb.Value{"score": dsInt(30)}},
				})
				resp := runAggKind(t, s, kind, nil, []*datastorepb.AggregationQuery_Aggregation{countAgg("n")})
				if n := resp.Batch.AggregationResults[0].AggregateProperties["n"].GetIntegerValue(); n != 3 {
					t.Errorf("want 3, got %d", n)
				}
			},
		},
		{
			name: "count_with_eq_filter",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"color": dsStr("red")}},
					{"b", map[string]*datastorepb.Value{"color": dsStr("blue")}},
					{"c", map[string]*datastorepb.Value{"color": dsStr("red")}},
				})
				resp := runAggKind(t, s, kind,
					propFilter("color", datastorepb.PropertyFilter_EQUAL, dsStr("red")),
					[]*datastorepb.AggregationQuery_Aggregation{countAgg("n")},
				)
				if n := resp.Batch.AggregationResults[0].AggregateProperties["n"].GetIntegerValue(); n != 2 {
					t.Errorf("want 2 red, got %d", n)
				}
			},
		},
		{
			name: "count_limit",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"v": dsInt(1)}},
					{"b", map[string]*datastorepb.Value{"v": dsInt(2)}},
					{"c", map[string]*datastorepb.Value{"v": dsInt(3)}},
					{"d", map[string]*datastorepb.Value{"v": dsInt(4)}},
					{"e", map[string]*datastorepb.Value{"v": dsInt(5)}},
				})
				var resp datastorepb.RunAggregationQueryResponse
				mustPost(t, s, "runAggregationQuery", &datastorepb.RunAggregationQueryRequest{
					ProjectId: testProject,
					QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{
						AggregationQuery: &datastorepb.AggregationQuery{
							QueryType: &datastorepb.AggregationQuery_NestedQuery{
								NestedQuery: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}},
							},
							Aggregations: []*datastorepb.AggregationQuery_Aggregation{{
								Alias: "n",
								Operator: &datastorepb.AggregationQuery_Aggregation_Count_{
									Count: &datastorepb.AggregationQuery_Aggregation_Count{UpTo: wrapperspb.Int64(3)},
								},
							}},
						},
					},
				}, &resp)
				if n := resp.Batch.AggregationResults[0].AggregateProperties["n"].GetIntegerValue(); n != 3 {
					t.Errorf("count_limit: want 3, got %d", n)
				}
			},
		},
		{
			name: "sum_int",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"score": dsInt(10)}},
					{"b", map[string]*datastorepb.Value{"score": dsInt(20)}},
					{"c", map[string]*datastorepb.Value{"score": dsInt(30)}},
				})
				resp := runAggKind(t, s, kind, nil, []*datastorepb.AggregationQuery_Aggregation{sumAgg("total", "score")})
				if total := resp.Batch.AggregationResults[0].AggregateProperties["total"].GetIntegerValue(); total != 60 {
					t.Errorf("sum: want 60, got %d", total)
				}
			},
		},
		{
			name: "avg_int",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"score": dsInt(10)}},
					{"b", map[string]*datastorepb.Value{"score": dsInt(20)}},
					{"c", map[string]*datastorepb.Value{"score": dsInt(30)}},
				})
				resp := runAggKind(t, s, kind, nil, []*datastorepb.AggregationQuery_Aggregation{avgAgg("avg", "score")})
				if avg := resp.Batch.AggregationResults[0].AggregateProperties["avg"].GetDoubleValue(); avg != 20.0 {
					t.Errorf("avg: want 20.0, got %f", avg)
				}
			},
		},
		{
			name: "avg_missing_field",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"score": dsInt(10)}},
					{"b", map[string]*datastorepb.Value{"score": dsInt(20)}},
				})
				resp := runAggKind(t, s, kind, nil, []*datastorepb.AggregationQuery_Aggregation{avgAgg("avg", "nonexistent")})
				v := resp.Batch.AggregationResults[0].AggregateProperties["avg"]
				if _, ok := v.ValueType.(*datastorepb.Value_NullValue); !ok {
					t.Errorf("avg of missing property: want null, got %v", v)
				}
			},
		},
		{
			name: "multiple_agg_same_req",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"score": dsInt(10)}},
					{"b", map[string]*datastorepb.Value{"score": dsInt(20)}},
					{"c", map[string]*datastorepb.Value{"score": dsInt(30)}},
				})
				resp := runAggKind(t, s, kind, nil, []*datastorepb.AggregationQuery_Aggregation{
					countAgg("n"),
					sumAgg("total", "score"),
				})
				props := resp.Batch.AggregationResults[0].AggregateProperties
				if n := props["n"].GetIntegerValue(); n != 3 {
					t.Errorf("multiple_agg count: want 3, got %d", n)
				}
				if total := props["total"].GetIntegerValue(); total != 60 {
					t.Errorf("multiple_agg sum: want 60, got %d", total)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kind := "K_" + tc.name
			tc.run(t, s, kind)
		})
	}
}
