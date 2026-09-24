package datastore

import (
	"context"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func (g *GRPCServer) countQueryFallback(ctx context.Context, project, database, namespace, kind, ancestor string, condition *compiledCondition, readAt *time.Time) (int64, int64, error) {
	var count int64
	matcher := newQueryMatcher(ctx, condition, "")
	visit := func(row *storage.DsEntityRow) error {
		if matcher.accept(row.Entity) {
			count++
		}
		return matcher.err
	}
	var scanned int64
	var err error
	if isMetadataKind(kind) {
		snapshot := g.store.ReadTime()
		if readAt != nil {
			snapshot = *readAt
		}
		scanned, err = g.store.DsVisitMetadataAsOf(ctx, snapshot, project, database, namespace, kind, visit)
	} else if readAt == nil {
		scanned, err = g.store.DsVisitKindEntities(ctx, project, database, namespace, kind, ancestor, visit)
	} else {
		scanned, err = g.store.DsVisitKindEntitiesAsOf(ctx, *readAt, project, database, namespace, kind, ancestor, visit)
	}
	if err != nil {
		return 0, scanned, err
	}
	return count, scanned, nil
}

type queryCount struct {
	matches, documents, scanned int64
	indexID                     string
}

// countQueryResponse is shared by index, union, and fallback count shortcuts.
// resolveQueryRead excludes transactions from the count-only path.
func countQueryResponse(prepared *preparedRunQuery, read *queryRead, count queryCount) *datastorepb.RunQueryResponse {
	response := &datastorepb.RunQueryResponse{Batch: &datastorepb.QueryResultBatch{
		SkippedResults:   int32(min(count.matches, int64(prepared.query.Offset))),
		MoreResults:      datastorepb.QueryResultBatch_NO_MORE_RESULTS,
		EntityResultType: datastorepb.EntityResult_FULL,
		ReadTime:         read.responseReadTime,
	}}
	if options := prepared.request.GetExplainOptions(); options != nil && options.Analyze {
		response.ExplainMetrics = &datastorepb.ExplainMetrics{
			PlanSummary:    buildCompositeRunQueryPlan(prepared.query, count.indexID),
			ExecutionStats: buildRunQueryExecutionStatsWithScans(0, int(count.documents), count.scanned, time.Since(read.started)),
		}
	}
	return response
}
