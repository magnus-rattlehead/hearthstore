package datastore

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/protobuf/types/known/timestamppb"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func (g *GRPCServer) RunAggregationQuery(ctx context.Context, req *datastorepb.RunAggregationQueryRequest) (*datastorepb.RunAggregationQueryResponse, error) {
	if req.ProjectId == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id is required")
	}
	database := req.DatabaseId
	if database == "" {
		database = defaultDatabase
	}
	namespace := req.PartitionId.GetNamespaceId()

	aq, ok := req.QueryType.(*datastorepb.RunAggregationQueryRequest_AggregationQuery)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "only structured aggregation queries are supported")
	}
	ag := aq.AggregationQuery

	sq, ok := ag.QueryType.(*datastorepb.AggregationQuery_NestedQuery)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "nested_query is required")
	}
	q := sq.NestedQuery

	// Handle ExplainOptions: analyze=false (or unset) → plan only, no execution.
	explainOpts := req.GetExplainOptions()
	if explainOpts != nil && !explainOpts.Analyze {
		plan := buildAggregationQueryPlan(ag, q)
		return &datastorepb.RunAggregationQueryResponse{
			Batch: &datastorepb.AggregationResultBatch{
				AggregationResults: nil,
				MoreResults:        datastorepb.QueryResultBatch_NO_MORE_RESULTS,
				ReadTime:           timestamppb.Now(),
			},
			ExplainMetrics: &datastorepb.ExplainMetrics{
				PlanSummary: plan,
			},
		}, nil
	}

	// Validate aggregation operators: __key__ is not supported for Sum/Avg.
	for _, agg := range ag.Aggregations {
		switch op := agg.Operator.(type) {
		case *datastorepb.AggregationQuery_Aggregation_Sum_:
			if op.Sum.GetProperty().GetName() == "__key__" {
				return nil, status.Error(codes.InvalidArgument, "Aggregations are not supported for the property: __key__")
			}
		case *datastorepb.AggregationQuery_Aggregation_Avg_:
			if op.Avg.GetProperty().GetName() == "__key__" {
				return nil, status.Error(codes.InvalidArgument, "Aggregations are not supported for the property: __key__")
			}
		}
	}

	// Resolve snapshot time from ReadOptions or read-only transaction.
	var readAt *time.Time
	if ro := req.GetReadOptions(); ro != nil {
		if rt := ro.GetReadTime(); rt != nil {
			t := rt.AsTime()
			readAt = &t
		} else if tx := ro.GetTransaction(); len(tx) > 0 {
			txID := string(tx)
			g.txMu.Lock()
			entry, ok := g.txns[txID]
			g.txMu.Unlock()
			if ok && entry.readOnly && entry.readTime != nil {
				t := entry.readTime.AsTime()
				readAt = &t
			}
		}
	}

	var kind, ancestorPath string
	if len(q.Kind) > 0 {
		kind = q.Kind[0].Name
	}
	if q.Filter != nil {
		ancestorPath = extractAncestorPath(q.Filter, req.ProjectId, database, namespace)
	}

	// Build SQL filter clause for pushdown (live reads only).
	var aggFilterSQL string
	var aggFilterArgs []any
	var aggNeedsGoFilter bool
	if q.Filter != nil && readAt == nil {
		aggFilterSQL, aggFilterArgs, aggNeedsGoFilter = buildDsWhereClause(req.ProjectId, database, namespace, kind, q.Filter)
		if aggFilterSQL == "" {
			aggNeedsGoFilter = true
		}
	}

	start := time.Now()
	var rows []*storage.DsEntityRow
	var err error
	if readAt != nil {
		rows, err = g.store.DsQueryKindAsOf(req.ProjectId, database, namespace, kind, ancestorPath, *readAt)
	} else {
		rows, err = g.store.DsQueryKind(req.ProjectId, database, namespace, kind, ancestorPath, aggFilterSQL, aggFilterArgs)
	}
	if err != nil {
		return nil, err
	}

	if q.Filter != nil && (readAt != nil || aggNeedsGoFilter) {
		filtered := rows[:0]
		for _, row := range rows {
			if matchesFilter(row.Entity, q.Filter) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}

	// When a limit is present with no ORDER BY, sort by the aggregation field
	// ascending before truncating. This matches Google Cloud Datastore emulator
	// behavior where limit picks the entities with the lowest field values.
	if q.Limit != nil && len(q.Order) == 0 {
		if prop := aggSortField(ag.Aggregations); prop != "" {
			sort.SliceStable(rows, func(i, j int) bool {
				vi := getProp(rows[i].Entity, prop)
				vj := getProp(rows[j].Entity, prop)
				return compareValues(vi, vj) < 0
			})
		}
	}

	if q.Limit != nil && int(q.Limit.Value) < len(rows) {
		rows = rows[:q.Limit.Value]
	}

	aggProps := make(map[string]*datastorepb.Value, len(ag.Aggregations))
	for i, agg := range ag.Aggregations {
		alias := agg.Alias
		if alias == "" {
			alias = fmt.Sprintf("property_%d", i+1)
		}
		switch op := agg.Operator.(type) {

		case *datastorepb.AggregationQuery_Aggregation_Count_:
			upTo := op.Count.GetUpTo().GetValue()
			n := int64(len(rows))
			if upTo > 0 && n > upTo {
				n = upTo
			}
			aggProps[alias] = &datastorepb.Value{
				ValueType: &datastorepb.Value_IntegerValue{IntegerValue: n},
			}

		case *datastorepb.AggregationQuery_Aggregation_Sum_:
			prop := op.Sum.GetProperty().GetName()
			var sum float64
			allInt := true
			hasAny := false
			for _, row := range rows {
				v := getProp(row.Entity, prop)
				if v == nil || !isDsNumeric(v) {
					continue
				}
				hasAny = true
				if _, isInt := v.ValueType.(*datastorepb.Value_IntegerValue); !isInt {
					allInt = false
				}
				sum += dsNumericFloat(v)
			}
			if !hasAny {
				aggProps[alias] = &datastorepb.Value{
					ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 0},
				}
			} else if allInt && sum >= math.MinInt64 && sum <= math.MaxInt64 {
				// All-integer sum that fits in int64: return as integer.
				aggProps[alias] = &datastorepb.Value{
					ValueType: &datastorepb.Value_IntegerValue{IntegerValue: int64(sum)},
				}
			} else {
				// Any float operand or overflow: return as double.
				aggProps[alias] = &datastorepb.Value{
					ValueType: &datastorepb.Value_DoubleValue{DoubleValue: sum},
				}
			}

		case *datastorepb.AggregationQuery_Aggregation_Avg_:
			prop := op.Avg.GetProperty().GetName()
			var sum float64
			count := 0
			for _, row := range rows {
				v := getProp(row.Entity, prop)
				if v == nil || !isDsNumeric(v) {
					continue
				}
				sum += dsNumericFloat(v)
				count++
			}
			if count == 0 || math.IsNaN(sum/float64(count)) {
				aggProps[alias] = &datastorepb.Value{
					ValueType: &datastorepb.Value_NullValue{},
				}
			} else {
				aggProps[alias] = &datastorepb.Value{
					ValueType: &datastorepb.Value_DoubleValue{DoubleValue: sum / float64(count)},
				}
			}
		}
	}

	resp := &datastorepb.RunAggregationQueryResponse{
		Batch: &datastorepb.AggregationResultBatch{
			AggregationResults: []*datastorepb.AggregationResult{
				{AggregateProperties: aggProps},
			},
			MoreResults: datastorepb.QueryResultBatch_NO_MORE_RESULTS,
			ReadTime:    timestamppb.Now(),
		},
	}
	if explainOpts != nil && explainOpts.Analyze {
		resp.ExplainMetrics = &datastorepb.ExplainMetrics{
			PlanSummary:    buildAggregationQueryPlan(ag, q),
			ExecutionStats: buildAggregationQueryExecutionStats(len(rows), time.Since(start)),
		}
	}
	return resp, nil
}

// aggSortField returns the single property name used by all sum/avg aggregations,
// or "" if aggregations use different or no properties (e.g. count-only queries).
func aggSortField(aggs []*datastorepb.AggregationQuery_Aggregation) string {
	var field string
	for _, agg := range aggs {
		var prop string
		switch op := agg.Operator.(type) {
		case *datastorepb.AggregationQuery_Aggregation_Sum_:
			prop = op.Sum.GetProperty().GetName()
		case *datastorepb.AggregationQuery_Aggregation_Avg_:
			prop = op.Avg.GetProperty().GetName()
		default:
			// count or unknown — no field to sort by
			return ""
		}
		if prop == "" {
			return ""
		}
		if field == "" {
			field = prop
		} else if field != prop {
			return "" // multiple different fields
		}
	}
	return field
}

func (s *Server) handleRunAggregationQuery(w http.ResponseWriter, r *http.Request, project string) {
	var req datastorepb.RunAggregationQueryRequest
	if !readProtoJSON(w, r.Body, &req) {
		return
	}
	if req.ProjectId == "" {
		req.ProjectId = project
	}
	resp, err := s.grpc.RunAggregationQuery(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	writeProtoJSON(w, resp)
}
