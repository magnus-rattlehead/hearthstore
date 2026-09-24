package datastore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (g *GRPCServer) RunAggregationQuery(ctx context.Context, req *datastorepb.RunAggregationQueryRequest) (out *datastorepb.RunAggregationQueryResponse, resultErr error) {
	defer func() { resultErr = rpcError(resultErr) }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.store.CheckAvailable(); err != nil {
		return nil, err
	}
	ctx, work := storage.WithQueryWork(ctx, 0)
	release, err := g.acquireQuerySlot(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	response, err := g.runAggregationQuery(ctx, req)
	if err == nil {
		addQueryWorkStats(response.GetExplainMetrics(), work)
	}
	return response, err
}

func (g *GRPCServer) runAggregationQuery(ctx context.Context, req *datastorepb.RunAggregationQueryRequest) (response *datastorepb.RunAggregationQueryResponse, resultErr error) {
	work := storage.QueryWorkFromContext(ctx)
	if req.GetProjectId() == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id is required")
	}
	database := req.DatabaseId
	if database == "" {
		database = defaultDatabase
	}
	aq, ok := req.QueryType.(*datastorepb.RunAggregationQueryRequest_AggregationQuery)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "only structured aggregation queries are supported")
	}
	ag := aq.AggregationQuery
	if ag == nil {
		return nil, status.Error(codes.InvalidArgument, "aggregation_query is required")
	}

	sq, ok := ag.QueryType.(*datastorepb.AggregationQuery_NestedQuery)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "nested_query is required")
	}
	q := sq.NestedQuery
	if q == nil {
		return nil, status.Error(codes.InvalidArgument, "nested_query is required")
	}
	ctx = context.WithValue(ctx, aggregationEntriesKey{}, true)
	ctx, q, condition, err := prepareQueryExecution(ctx, q, req.PartitionId.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	if len(ag.Aggregations) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one aggregation is required")
	}
	aliases := make(map[string]bool, len(ag.Aggregations))
	projected := make(map[string]bool, len(q.Projection))
	for _, field := range q.Projection {
		projected[field.GetProperty().GetName()] = true
	}
	for _, agg := range ag.Aggregations {
		if agg == nil || agg.Operator == nil {
			return nil, status.Error(codes.InvalidArgument, "aggregation operator is required")
		}
		if agg.Alias != "" {
			if aliases[agg.Alias] {
				return nil, status.Error(codes.InvalidArgument, "aggregation aliases must be unique")
			}
			aliases[agg.Alias] = true
		}
		var property string
		switch op := agg.Operator.(type) {
		case *datastorepb.AggregationQuery_Aggregation_Count_:
			if op.Count == nil || op.Count.UpTo != nil && op.Count.UpTo.Value < 0 {
				return nil, status.Error(codes.InvalidArgument, "count up_to must be non-negative")
			}
		case *datastorepb.AggregationQuery_Aggregation_Sum_:
			property = op.Sum.GetProperty().GetName()
			if op.Sum.GetProperty().GetName() == "" {
				return nil, status.Error(codes.InvalidArgument, "sum property is required")
			}
		case *datastorepb.AggregationQuery_Aggregation_Avg_:
			property = op.Avg.GetProperty().GetName()
			if op.Avg.GetProperty().GetName() == "" {
				return nil, status.Error(codes.InvalidArgument, "average property is required")
			}
		}
		if property == "__key__" {
			return nil, status.Error(codes.InvalidArgument, "Aggregations are not supported for the property: __key__")
		}
		if property != "" && len(projected) > 0 && !projected[property] {
			return nil, status.Error(codes.InvalidArgument, "aggregation property must be included in the projection")
		}
	}
	plannedQuery := aggregationEntryQuery(q, condition, ag.Aggregations)
	ctx = context.WithValue(ctx, aggregationEntriesKey{}, len(plannedQuery.Projection) > 0 || len(plannedQuery.StartCursor) > 0 || len(plannedQuery.EndCursor) > 0)
	if err := g.aggregationQueryCursors(req.ProjectId, database, req.PartitionId.GetNamespaceId(), q, plannedQuery); err != nil {
		return nil, err
	}
	var snapshot querySnapshot
	if aggregationEntries(ctx) && (req.ReadOptions == nil || req.ReadOptions.GetConsistencyType() == nil) && req.ReadOptions.GetNewTransaction() == nil && len(q.StartCursor) == 0 && len(q.EndCursor) == 0 {
		snapshot.aggregation = &aggregationAccess{}
	}
	defer func() {
		if err := snapshot.close(); err != nil {
			response = nil
			resultErr = errors.Join(resultErr, fmt.Errorf("close aggregation execution: %w", err))
		}
	}()

	// Handle ExplainOptions: analyze=false (or unset) -> plan only, no execution.
	explainOpts := req.GetExplainOptions()
	if explainOpts != nil && !explainOpts.Analyze {
		queryPlan, err := g.runQueryWithSnapshot(ctx, &datastorepb.RunQueryRequest{ProjectId: req.ProjectId, DatabaseId: database, PartitionId: req.PartitionId, ReadOptions: req.ReadOptions, QueryType: &datastorepb.RunQueryRequest_Query{Query: plannedQuery}, ExplainOptions: explainOpts}, &snapshot)
		if err != nil {
			return nil, err
		}
		return &datastorepb.RunAggregationQueryResponse{
			Batch: &datastorepb.AggregationResultBatch{
				AggregationResults: nil,
				MoreResults:        datastorepb.QueryResultBatch_NO_MORE_RESULTS,
				ReadTime:           timestamppb.Now(),
			},
			ExplainMetrics: &datastorepb.ExplainMetrics{
				PlanSummary: queryPlan.ExplainMetrics.PlanSummary,
			},
		}, nil
	}

	start := time.Now()
	countOnly := len(ag.Aggregations) == 1
	if countOnly {
		_, countOnly = ag.Aggregations[0].Operator.(*datastorepb.AggregationQuery_Aggregation_Count_)
	}
	countOnly = countOnly && len(plannedQuery.Projection) == 0
	if countOnly {
		plannedQuery.Offset = math.MaxInt32
		plannedQuery.Limit = wrapperspb.Int32(0)
	}
	type aggregateState struct {
		sum   aggregationSum
		count int64
	}
	states := make([]aggregateState, len(ag.Aggregations))
	var uniqueRows, skippedRows int64
	var documentsScanned, indexEntriesScanned int64
	actualPlan := &datastorepb.PlanSummary{}
	var transaction []byte
	remainingOffset := plannedQuery.Offset
	remainingLimit := int32(-1)
	if plannedQuery.Limit != nil {
		remainingLimit = plannedQuery.Limit.Value
	}
	var queryResponse *datastorepb.RunQueryResponse
	readOptions := req.ReadOptions
	for {
		queryResponse, err = g.runQueryWithSnapshot(ctx, &datastorepb.RunQueryRequest{
			ProjectId:      req.ProjectId,
			DatabaseId:     database,
			PartitionId:    req.PartitionId,
			ReadOptions:    readOptions,
			QueryType:      &datastorepb.RunQueryRequest_Query{Query: plannedQuery},
			ExplainOptions: &datastorepb.ExplainOptions{Analyze: true},
		}, &snapshot)
		if err != nil {
			return nil, err
		}
		if metrics := queryResponse.ExplainMetrics; metrics != nil {
			for _, entry := range metrics.GetPlanSummary().GetIndexesUsed() {
				found := false
				for _, previous := range actualPlan.IndexesUsed {
					found = found || proto.Equal(previous, entry)
				}
				if !found {
					actualPlan.IndexesUsed = append(actualPlan.IndexesUsed, entry)
				}
			}
			for _, counter := range []struct {
				name  string
				total *int64
			}{{"documents_scanned", &documentsScanned}, {"index_entries_scanned", &indexEntriesScanned}} {
				value := metrics.GetExecutionStats().GetDebugStats().GetFields()[counter.name].GetStringValue()
				if value == "" {
					continue
				}
				n, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return nil, status.Errorf(codes.Internal, "invalid query counter %s", counter.name)
				}
				*counter.total += n
			}
		}
		if len(queryResponse.Transaction) > 0 {
			transaction = queryResponse.Transaction
			readOptions = &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_Transaction{Transaction: transaction}}
		}
		if readOptions == nil || readOptions.GetConsistencyType() == nil {
			readOptions = &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: queryResponse.Batch.ReadTime}}
		}
		skippedRows += int64(queryResponse.Batch.SkippedResults)
		for _, result := range queryResponse.GetBatch().GetEntityResults() {
			if err := work.Checkpoint(ctx); err != nil {
				return nil, err
			}
			uniqueRows++
			for i, agg := range ag.Aggregations {
				var property string
				switch op := agg.Operator.(type) {
				case *datastorepb.AggregationQuery_Aggregation_Sum_:
					property = op.Sum.GetProperty().GetName()
				case *datastorepb.AggregationQuery_Aggregation_Avg_:
					property = op.Avg.GetProperty().GetName()
				default:
					continue
				}
				value := getProp(result.Entity, property)
				state := &states[i]
				if state.sum.add(value) {
					state.count++
				}
			}
		}
		remainingOffset -= queryResponse.GetBatch().GetSkippedResults()
		if remainingOffset < 0 {
			remainingOffset = 0
		}
		if remainingLimit >= 0 {
			remainingLimit -= int32(len(queryResponse.GetBatch().GetEntityResults()))
		}
		if queryResponse.GetBatch().GetMoreResults() != datastorepb.QueryResultBatch_NOT_FINISHED || !countOnly && remainingLimit == 0 {
			break
		}
		plannedQuery.StartCursor = queryResponse.GetBatch().GetEndCursor()
		plannedQuery.Offset = remainingOffset
		if remainingLimit >= 0 {
			plannedQuery.Limit = wrapperspb.Int32(remainingLimit)
		}
	}
	countedRows := int64(-1)
	if countOnly {
		countedRows = skippedRows - int64(q.Offset)
		if countedRows < 0 {
			countedRows = 0
		}
		if q.Limit != nil && countedRows > int64(q.Limit.Value) {
			countedRows = int64(q.Limit.Value)
		}
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
			n := uniqueRows
			if countedRows >= 0 {
				n = countedRows
			}
			if op.Count.UpTo != nil && n > upTo {
				n = upTo
			}
			aggProps[alias] = &datastorepb.Value{
				ValueType: &datastorepb.Value_IntegerValue{IntegerValue: n},
			}

		case *datastorepb.AggregationQuery_Aggregation_Sum_:
			state := states[i]
			if integer, ok := state.sum.integerValue(); ok {
				// All-integer sum that fits in int64: return as integer.
				aggProps[alias] = &datastorepb.Value{
					ValueType: &datastorepb.Value_IntegerValue{IntegerValue: integer},
				}
			} else {
				// Any float operand or overflow: return as double.
				aggProps[alias] = &datastorepb.Value{
					ValueType: &datastorepb.Value_DoubleValue{DoubleValue: state.sum.doubleValue()},
				}
			}

		case *datastorepb.AggregationQuery_Aggregation_Avg_:
			state := states[i]
			if state.count == 0 {
				aggProps[alias] = &datastorepb.Value{
					ValueType: &datastorepb.Value_NullValue{},
				}
			} else {
				aggProps[alias] = &datastorepb.Value{
					ValueType: &datastorepb.Value_DoubleValue{DoubleValue: state.sum.doubleValue() / float64(state.count)},
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
			ReadTime:    queryResponse.GetBatch().GetReadTime(),
		},
		Transaction: transaction,
	}
	if explainOpts != nil && explainOpts.Analyze {
		resp.ExplainMetrics = &datastorepb.ExplainMetrics{
			PlanSummary:    actualPlan,
			ExecutionStats: buildRunQueryExecutionStatsWithScans(1, int(documentsScanned), indexEntriesScanned, time.Since(start)),
		}
	}
	return resp, nil
}

func (s *Server) handleRunAggregationQuery(w http.ResponseWriter, r *http.Request, project string) {
	var req datastorepb.RunAggregationQueryRequest
	if !readProtoJSON(w, r.Body, &req) {
		return
	}
	if req.ProjectId == "" {
		req.ProjectId = project
	}
	start := time.Now()
	SetHTTPDetails(r.Context(), DSAggregationQueryDetails(&req))
	resp, err := s.grpc.RunAggregationQuery(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	MergeHTTPDetails(r.Context(), DSAggregationResponseDetails(resp, time.Since(start)))
	writeProtoJSON(w, resp)
}
