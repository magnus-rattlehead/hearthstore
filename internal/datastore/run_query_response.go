package datastore

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type queryPage struct {
	rows                                    []*storage.DsEntityRow
	batchNotFinished, moreResultsAfterLimit bool
	skippedResults                          int32
	skippedCursorRow                        *storage.DsEntityRow
}

// queryResultGroups retains complete row boundaries for response-size trimming.
type queryResultGroups struct {
	results   []*datastorepb.EntityResult
	ends      []int
	cursors   [][]byte
	rowBytes  []int
	bytes     int
	endCursor []byte
}

func buildQueryResponse(ctx context.Context, prepared *preparedRunQuery, read *queryRead, access *queryAccess, scan *queryScan) (*datastorepb.RunQueryResponse, error) {
	page, err := shapeQueryRows(ctx, prepared, read, access, scan)
	if err != nil {
		return nil, err
	}
	encodeRowCursor, err := queryRowCursorEncoder(ctx, prepared, access, scan, page)
	if err != nil {
		return nil, err
	}
	groups := serializeQueryRows(prepared, page, encodeRowCursor)
	keysOnly := isKeysOnly(prepared.query)
	projFields := projectionFields(prepared.query)
	moreResults := datastorepb.QueryResultBatch_NO_MORE_RESULTS
	if page.moreResultsAfterLimit {
		moreResults = datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT
	} else if page.batchNotFinished {
		moreResults = datastorepb.QueryResultBatch_NOT_FINISHED
	}

	entityResultType := datastorepb.EntityResult_FULL
	if keysOnly {
		entityResultType = datastorepb.EntityResult_KEY_ONLY
	} else if len(projFields) > 0 {
		entityResultType = datastorepb.EntityResult_PROJECTION
	}

	resp := &datastorepb.RunQueryResponse{
		Batch: &datastorepb.QueryResultBatch{
			EntityResults:    groups.results,
			SkippedResults:   page.skippedResults,
			EndCursor:        groups.endCursor,
			MoreResults:      moreResults,
			EntityResultType: entityResultType,
			ReadTime:         read.responseReadTime,
		},
	}
	if read.newTxIDForResp != "" {
		resp.Transaction = []byte(read.newTxIDForResp)
	}

	resp.ExplainMetrics = queryResponseMetrics(prepared, read, access, scan, len(groups.results))
	if err := trimQueryResponse(resp, groups); err != nil {
		return nil, err
	}
	return resp, nil
}

func shapeQueryRows(ctx context.Context, prepared *preparedRunQuery, read *queryRead, access *queryAccess, scan *queryScan) (*queryPage, error) {
	page := &queryPage{rows: scan.rows, batchNotFinished: scan.sourceMore}
	if prepared.query.Filter != nil && !prepared.entryQuery {
		var filtered []*storage.DsEntityRow
		for _, row := range page.rows {
			if prepared.matcher.accept(row.Entity) {
				filtered = append(filtered, row)
			}
		}
		page.rows = filtered
		if prepared.matcher.err != nil {
			return nil, prepared.matcher.err
		}
	}

	if !scan.usedComposite {
		slices.SortStableFunc(page.rows, func(a, b *storage.DsEntityRow) int { return compareQueryRows(prepared.query, a, b) })
	}

	if fields := projectionFields(prepared.query); len(fields) > 0 {
		var stopped bool
		var err error
		page.rows, stopped, err = projectRunQueryRows(ctx, page.rows, fields, prepared.startCursor)
		if err != nil {
			return nil, err
		}
		if stopped {
			page.batchNotFinished = true
		}
	}
	if prepared.endBound != nil {
		cut := len(page.rows)
		for i, row := range page.rows {
			comparison := bytes.Compare(row.IndexKey, prepared.endBound.K)
			if scan.fallback {
				comparison = strings.Compare(fallbackRecordTuple(string(row.IndexKey)), fallbackRecordTuple(string(prepared.endBound.K)))
			}
			if !scan.fallback && strings.HasPrefix(scan.indexID, "builtin:") && access.builtinReverse {
				comparison = -comparison
			}
			if comparison > 0 || comparison == 0 && (prepared.endBound.Before || prepared.endBound.O > 0 && (row.ProjectionOffset == 0 || row.ProjectionOffset > prepared.endBound.O)) {
				cut = i
				page.batchNotFinished = false
				break
			}
		}
		page.rows = page.rows[:cut]
	}

	// distinct_on keeps the first row for each projected value tuple.
	if len(prepared.query.DistinctOn) > 0 {
		seen := make(map[string]bool)
		if prepared.startCursor != nil && prepared.startCursor.D != "" {
			seen[prepared.startCursor.D] = true
		}
		var distinct []*storage.DsEntityRow
		for _, row := range page.rows {
			key := distinctKey(row.Entity, prepared.query.DistinctOn)
			if !seen[key] {
				seen[key] = true
				distinct = append(distinct, row)
			}
		}
		page.rows = distinct
	}

	if !scan.usedComposite && len(prepared.query.StartCursor) > 0 {
		startPath := decodeCursor(prepared.query.StartCursor)
		if startPath != "" {
			// Generic cursors resume after the matching sorted row.
			cutIdx := 0
			for i, row := range page.rows {
				if row.Path == startPath {
					cutIdx = i + 1
					break
				}
			}
			page.rows = page.rows[cutIdx:]
		}
	}

	if prepared.query.Offset > 0 {
		if int(prepared.query.Offset) >= len(page.rows) {
			page.skippedResults = int32(len(page.rows))
			if len(page.rows) > 0 {
				page.skippedCursorRow = page.rows[len(page.rows)-1]
			}
			page.rows = nil
		} else {
			page.skippedResults = prepared.query.Offset
			page.skippedCursorRow = page.rows[prepared.query.Offset-1]
			page.rows = page.rows[prepared.query.Offset:]
		}
	}
	if read.hasLimit && read.limit == 0 {
		page.rows = nil
	} else if read.batchLimit > 0 && read.batchLimit < len(page.rows) {
		if !read.hasLimit || read.limit > read.batchLimit {
			page.batchNotFinished = true
		} else {
			page.moreResultsAfterLimit = true
		}
		page.rows = page.rows[:read.batchLimit]
	}

	return page, nil
}

func projectRunQueryRows(ctx context.Context, rows []*storage.DsEntityRow, fields []string, startCursor *storage.CursorPayload) ([]*storage.DsEntityRow, bool, error) {
	var projected []*storage.DsEntityRow
	projectedBytes := 0
	stopped := false
	for _, row := range rows {
		offset := 0
		projectionErr := visitProjection(ctx, row.Entity, fields, func(entity *datastorepb.Entity, last bool) bool {
			offset++
			if startCursor != nil && row.Path == startCursor.P && offset <= startCursor.O {
				return true
			}
			copyRow := *row
			copyRow.Entity = entity
			copyRow.ProjectionOffset = offset
			if last {
				copyRow.ProjectionOffset = 0
			}
			projectedBytes += proto.Size(entity)
			if projectedBytes > maxQueryResponseBytes && len(projected) > 0 {
				stopped = true
				return false
			}
			projected = append(projected, &copyRow)
			return true
		})
		if projectionErr != nil {
			return nil, false, projectionErr
		}
		if stopped {
			break
		}
	}

	return projected, stopped, nil
}

func queryRowCursorEncoder(ctx context.Context, prepared *preparedRunQuery, access *queryAccess, scan *queryScan, page *queryPage) (func(*storage.DsEntityRow) []byte, error) {
	cursorOrdering := newConditionOrdering(prepared.query, projectionFields(prepared.query), prepared.condition)
	if len(prepared.query.Order) == 0 && access.builtinOK && access.builtinProperty != "__key__" {
		// A range scan has an implicit order even without an order clause.
		// Equality/IN prefix slots are not cursor postfix dimensions.
		for _, node := range prepared.condition.nodes {
			f := node.property
			if f == nil || f.Property.Name != access.builtinProperty {
				continue
			}
			switch f.Op {
			case datastorepb.PropertyFilter_LESS_THAN, datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL,
				datastorepb.PropertyFilter_GREATER_THAN, datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL,
				datastorepb.PropertyFilter_NOT_EQUAL, datastorepb.PropertyFilter_NOT_IN:
				ordered := &datastorepb.Query{Order: []*datastorepb.PropertyOrder{{Property: f.Property, Direction: datastorepb.PropertyOrder_ASCENDING}}}
				cursorOrdering = newConditionOrdering(ordered, projectionFields(prepared.query), prepared.condition)
			}
		}
	}
	if len(projectionFields(prepared.query)) > 0 {
		// These scalar tuples already qualified against the source entity;
		// non-projected filter fields need not survive in the response.
		cursorOrdering = newFallbackOrdering(&datastorepb.Query{Order: prepared.query.Order}, projectionFields(prepared.query))
	}
	logicalBounds := make(map[*storage.DsEntityRow][]byte, len(page.rows)+1)
	for _, row := range append(append([]*storage.DsEntityRow(nil), page.rows...), page.skippedCursorRow) {
		if row == nil {
			continue
		}
		if scan.fallback {
			logicalBounds[row] = []byte(strings.SplitN(string(row.IndexKey), "|", 2)[0])
			continue
		}
		bound, err := cursorOrdering.key(ctx, row.Entity)
		if err != nil {
			return nil, err
		}
		logicalBounds[row] = bound
	}
	return func(row *storage.DsEntityRow) []byte {
		distinct := ""
		if len(prepared.query.DistinctOn) > 0 {
			distinct = distinctKey(row.Entity, prepared.query.DistinctOn)
		}
		if scan.usedComposite {
			return encodeCursorFull(storage.CursorPayload{V: 4, P: row.Path, I: scan.indexID, G: scan.generation, K: row.IndexKey, O: row.ProjectionOffset, H: prepared.fingerprint, D: distinct, B: logicalBounds[row]})
		}
		return encodeCursorFull(storage.CursorPayload{V: 4, P: row.Path, H: prepared.fingerprint, O: row.ProjectionOffset, D: distinct, B: logicalBounds[row]})
	}, nil
}

func serializeQueryRows(prepared *preparedRunQuery, page *queryPage, encodeRowCursor func(*storage.DsEntityRow) []byte) *queryResultGroups {
	groups := &queryResultGroups{}
	keysOnly := isKeysOnly(prepared.query)
	groups.results = make([]*datastorepb.EntityResult, 0, len(page.rows))
	groups.ends = make([]int, 0, len(page.rows))
	groups.cursors = make([][]byte, 0, len(page.rows))
	groups.bytes = 0
	groups.rowBytes = make([]int, 0, len(page.rows))
	if page.skippedCursorRow != nil {
		groups.endCursor = encodeRowCursor(page.skippedCursorRow)
	}
	for _, row := range page.rows {
		rowCursor := encodeRowCursor(row)
		e := row.Entity
		var rowResults []*datastorepb.EntityResult
		if keysOnly {
			rowResults = append(rowResults, &datastorepb.EntityResult{
				Entity:     &datastorepb.Entity{Key: e.Key},
				Version:    row.Version,
				CreateTime: row.CreateTime,
				UpdateTime: row.UpdateTime,
				Cursor:     rowCursor,
			})
		} else {
			rowResults = append(rowResults, &datastorepb.EntityResult{
				Entity:     e,
				Version:    row.Version,
				CreateTime: row.CreateTime,
				UpdateTime: row.UpdateTime,
				Cursor:     rowCursor,
			})
		}
		previousResults := len(groups.results)
		groups.results = append(groups.results, rowResults...)
		rowBytes := 0
		for _, result := range rowResults {
			rowBytes += messageFieldSize(2, proto.Size(result))
		}
		if previousResults > 0 && groups.bytes+rowBytes+messageFieldSize(4, len(rowCursor)) > maxQueryResponseBytes {
			groups.results = groups.results[:previousResults]
			page.batchNotFinished = true
			page.moreResultsAfterLimit = false
			break
		}
		groups.bytes += rowBytes
		groups.rowBytes = append(groups.rowBytes, rowBytes)
		groups.ends = append(groups.ends, len(groups.results))
		groups.cursors = append(groups.cursors, rowCursor)
		groups.endCursor = rowCursor
	}

	return groups
}

func queryResponseMetrics(prepared *preparedRunQuery, read *queryRead, access *queryAccess, scan *queryScan, resultCount int) *datastorepb.ExplainMetrics {
	if opts := prepared.request.GetExplainOptions(); opts == nil || !opts.Analyze {
		return nil
	}

	plan := buildRunQueryPlan(prepared.query)
	stats := buildRunQueryExecutionStats(resultCount, time.Since(read.started))
	if scan.indexID != "" {
		plan = buildCompositeRunQueryPlan(prepared.query, scan.indexID)
		if access.index != nil && scan.indexID == access.index.ID {
			plan = buildPhysicalPlan(prepared.query, access.index, "composite", "READY")
		}
		if isMetadataKind(prepared.kind) {
			plan = buildPhysicalPlan(prepared.query, nil, "catalog", "READY")
		}
		stats = buildRunQueryExecutionStatsWithScans(resultCount, int(read.stats.Documents-read.initialReads), scan.entriesScanned, time.Since(read.started))
	}
	if scan.usedEqualityUnion {
		plan = buildEqualityUnionPlan(prepared.query, access.equalityBranches)
	}
	if scan.usedCandidates {
		plan = buildCandidateORPlan(prepared.query, access.candidateBranches)
	} else if len(access.candidateBranches) > 0 {
		probe := buildCandidateORPlan(prepared.query, access.candidateBranches)
		plan.IndexesUsed = append(plan.IndexesUsed, probe.IndexesUsed[1:]...)
	}
	return &datastorepb.ExplainMetrics{
		PlanSummary:    plan,
		ExecutionStats: stats,
	}

}

func trimQueryResponse(resp *datastorepb.RunQueryResponse, groups *queryResultGroups) error {
	for queryResponseSize(resp, groups.bytes) > maxQueryResponseBytes && len(groups.ends) > 1 {
		groups.bytes -= groups.rowBytes[len(groups.rowBytes)-1]
		groups.rowBytes = groups.rowBytes[:len(groups.rowBytes)-1]
		groups.ends = groups.ends[:len(groups.ends)-1]
		groups.cursors = groups.cursors[:len(groups.cursors)-1]
		groups.results = groups.results[:groups.ends[len(groups.ends)-1]]
		groups.endCursor = groups.cursors[len(groups.cursors)-1]
		resp.Batch.EntityResults = groups.results
		resp.Batch.EndCursor = groups.endCursor
		resp.Batch.MoreResults = datastorepb.QueryResultBatch_NOT_FINISHED
	}
	if stats := resp.GetExplainMetrics().GetExecutionStats(); stats != nil {
		stats.ResultsReturned = int64(len(groups.results))
	}
	if proto.Size(resp) > maxQueryResponseBytes {
		return status.Error(codes.ResourceExhausted, "single query result exceeds the 4 MiB response limit")
	}

	return nil
}

func (g *GRPCServer) recordQueryResultReads(prepared *preparedRunQuery, txID string, results []*datastorepb.EntityResult) error {

	reads := make(map[txReadKey]int64, len(results))
	for _, result := range results {
		project, resultDatabase, resultNamespace, _, _, path := keyComponents(result.Entity.GetKey())
		if project == "" {
			project = prepared.request.ProjectId
		}
		if resultDatabase == "" {
			resultDatabase = prepared.database
		}
		reads[txReadKey{project, resultDatabase, resultNamespace, path}] = result.Version
	}
	if err := g.recordTransactionReads(txID, reads); err != nil {
		return err
	}

	return nil
}
