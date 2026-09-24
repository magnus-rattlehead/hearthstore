package datastore

import (
	"bytes"
	"context"
	"math"
	"strings"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// preparedRunQuery holds the normalized request and its cursor coordinate system.
type preparedRunQuery struct {
	request                                 *datastorepb.RunQueryRequest
	query                                   *datastorepb.Query
	condition                               *compiledCondition
	matcher                                 *queryMatcher
	database, namespace, kind, ancestorPath string
	fingerprint                             []byte
	entryQuery                              bool
	startCursor, endBound                   *storage.CursorPayload
}

// queryRead retains the read boundary and accounting for one response page.
type queryRead struct {
	readAt                     *time.Time
	activeTxID, newTxIDForResp string
	responseReadTime           *timestamppb.Timestamp
	hasLimit                   bool
	limit, batchLimit          int
	countOnly                  bool
	stats                      *storage.QueryReadStats
	initialReads               int64
	started                    time.Time
}

// queryAccess describes the eligible access paths before a scan is selected.
type queryAccess struct {
	declinePlanning                           bool
	builtinProperty                           string
	builtinReverse, builtinOK, builtinUnionOK bool
	branches, candidateBranches               []builtinUnionBranch
	equalityBranches                          []*datastorepb.PropertyFilter
	index                                     *storage.DsCompositeIndex
}

func (g *GRPCServer) prepareRunQuery(ctx context.Context, req *datastorepb.RunQueryRequest, owned *querySnapshot) (context.Context, *preparedRunQuery, error) {
	if req.GetProjectId() == "" {
		return ctx, nil, status.Error(codes.InvalidArgument, "project_id is required")
	}
	database := req.DatabaseId
	if database == "" {
		database = defaultDatabase
	}
	namespace := req.PartitionId.GetNamespaceId()

	sq, ok := req.QueryType.(*datastorepb.RunQueryRequest_Query)
	if !ok {
		return ctx, nil, status.Error(codes.Unimplemented, "only structured queries are supported (not GQL)")
	}
	q := sq.Query
	if q == nil {
		return ctx, nil, status.Error(codes.InvalidArgument, "query is required")
	}
	ctx, q, condition, err := prepareQueryExecution(ctx, q, namespace)
	if err != nil {
		return ctx, nil, err
	}
	entryQuery := aggregationEntries(ctx)
	if !entryQuery {
		q = queryWithEffectiveOrder(q)
	}
	matcher := newQueryMatcher(ctx, condition, "")
	if len(q.Kind) > 0 && q.Kind[0].Name == "__namespace__" {
		namespace = "" // The Java emulator serves metadata in the default namespace.
	}
	fingerprint := queryFingerprint(req.ProjectId, database, namespace, q)
	if owned != nil && owned.fingerprint != nil && !bytes.Equal(owned.fingerprint, fingerprint) {
		return ctx, nil, status.Error(codes.InvalidArgument, "internal continuation belongs to another query")
	}
	callerQuery := q
	if !entryQuery {
		q = normalizedQueryOrder(q, condition, projectionFields(q), false)
	}
	startCursor, err := g.queryCursor(q.StartCursor, fingerprint, req.ProjectId, database, namespace, callerQuery, q, entryQuery)
	if err != nil {
		return ctx, nil, err
	}
	endBound, err := g.queryCursor(q.EndCursor, fingerprint, req.ProjectId, database, namespace, callerQuery, q, entryQuery)
	if err != nil {
		return ctx, nil, err
	}
	if !entryQuery && (startCursor != nil && startCursor.I == "fallback" || endBound != nil && endBound.I == "fallback") {
		// Both ends must use the same coordinate system, including a pair
		// with one reversed boundary and one ordinary physical-index cursor.
		for _, cursor := range []*storage.CursorPayload{startCursor, endBound} {
			if cursor != nil {
				cursor.I, cursor.G, cursor.O = "fallback", 0, 0
				cursor.K = []byte(string(cursor.B) + "|" + cursor.P + "|")
			}
		}
	}

	return ctx, &preparedRunQuery{
		request: req, query: q, condition: condition, matcher: matcher,
		database: database, namespace: namespace, fingerprint: fingerprint,
		entryQuery: entryQuery, startCursor: startCursor, endBound: endBound,
	}, nil
}

func (g *GRPCServer) explainPreparedRunQuery(ctx context.Context, prepared *preparedRunQuery, owned *querySnapshot) (*datastorepb.RunQueryResponse, error) {
	var err error
	var plan *datastorepb.PlanSummary
	if prepared.entryQuery && owned != nil && owned.aggregation != nil {
		access := owned.aggregation
		err = access.prepare(ctx, g.indexes, prepared.request.ProjectId, prepared.query, prepared.condition, g.store.ReadTime())
		switch {
		case access.index != nil:
			plan = buildPhysicalPlan(prepared.query, access.index, "composite", "READY")
		case access.builtin != "":
			plan = buildCompositeRunQueryPlan(prepared.query, "builtin:"+access.builtin)
		default:
			plan = buildCompositeRunQueryPlan(prepared.query, "fallback")
		}
	} else {
		plan, err = g.explainQueryPlan(ctx, prepared.request, prepared.query, prepared.database, prepared.namespace, prepared.startCursor, prepared.condition)
	}
	if err != nil {
		return nil, err
	}
	return &datastorepb.RunQueryResponse{
		Batch: &datastorepb.QueryResultBatch{
			EntityResults: nil,
			MoreResults:   datastorepb.QueryResultBatch_NO_MORE_RESULTS,
			ReadTime:      timestamppb.Now(),
		},
		ExplainMetrics: &datastorepb.ExplainMetrics{
			PlanSummary: plan,
		},
	}, nil
}

func (g *GRPCServer) resolveQueryRead(prepared *preparedRunQuery, owned *querySnapshot, read *queryRead) error {
	var err error
	if len(prepared.query.Kind) > 0 {
		prepared.kind = prepared.query.Kind[0].Name
	}
	if prepared.query.Filter != nil {
		prepared.ancestorPath = extractAncestorPath(prepared.query.Filter, prepared.request.ProjectId, prepared.database, prepared.namespace)
	}

	if owned != nil && owned.release != nil && (prepared.request.ReadOptions == nil || prepared.request.ReadOptions.GetReadTime() != nil) {
		// An internal continuation already owns this validated snapshot. It may
		// legitimately outlive the window for starting a new historical read.
		read.readAt = &owned.time
	} else {
		read.readAt, read.activeTxID, read.newTxIDForResp, err = g.resolveReadOptions(prepared.request.GetReadOptions(), prepared.request.ProjectId, prepared.database)
	}
	if err != nil {
		return err
	}
	if read.activeTxID != "" {
		scope := storage.QueryScope{Project: prepared.request.ProjectId, Database: prepared.database, Namespace: prepared.namespace, Kind: prepared.kind}
		if isMetadataKind(prepared.kind) {
			scope.Kind = ""
		}
		if prepared.kind == "__namespace__" {
			scope.AllNamespaces = true
		}
		if err := g.recordQueryScope(read.activeTxID, scope); err != nil {
			return err
		}
	}
	read.responseReadTime = timestamppb.Now()
	if read.readAt != nil {
		read.responseReadTime = timestamppb.New(*read.readAt)
	}

	// limit=0 means "return no entities" (used by count() to read skipped_results only).
	read.hasLimit = prepared.query.Limit != nil
	read.limit = 0
	if read.hasLimit {
		read.limit = int(prepared.query.Limit.Value)
	}
	read.batchLimit = math.MaxInt32
	if read.hasLimit && read.limit < read.batchLimit {
		read.batchLimit = read.limit
	}
	read.countOnly = prepared.kind != "" && read.activeTxID == "" && read.newTxIDForResp == "" &&
		read.hasLimit && read.limit == 0 && prepared.query.Offset == math.MaxInt32 && len(prepared.query.Projection) == 0 &&
		len(prepared.query.DistinctOn) == 0 && len(prepared.query.StartCursor) == 0 && len(prepared.query.EndCursor) == 0

	return nil
}

func (g *GRPCServer) prepareQueryAccess(ctx context.Context, prepared *preparedRunQuery, read *queryRead, access *queryAccess) error {
	var err error
	access.declinePlanning = !prepared.condition.accessPlanningFits(queryOptimizerAllowance(ctx)) && (prepared.startCursor == nil || prepared.startCursor.I == "fallback")
	access.declinePlanning = access.declinePlanning || prepared.entryQuery || prepared.endBound != nil && prepared.endBound.I == "fallback"
	access.declinePlanning = access.declinePlanning || hiddenProjectionOrder(prepared.query)
	if !access.declinePlanning {
		access.builtinProperty, access.builtinReverse, access.builtinOK = builtinQueryAccess(prepared.query)
		access.branches, access.builtinUnionOK = builtinUnionAccess(prepared.query)
		if !read.countOnly {
			access.equalityBranches = equalityUnionAccess(prepared.query, prepared.condition, queryOptimizerAllowance(ctx), prepared.startCursor)
		}
	}
	if !access.declinePlanning && !read.countOnly && !prepared.entryQuery && len(access.equalityBranches) == 0 {
		access.candidateBranches = candidateORAccess(prepared.query, prepared.condition, queryOptimizerAllowance(ctx), prepared.startCursor, prepared.endBound)
	}
	pinnedCompositeIndex := ""
	if prepared.startCursor != nil && prepared.startCursor.I != "" && prepared.startCursor.I != "fallback" && !strings.HasPrefix(prepared.startCursor.I, "builtin:") {
		pinnedCompositeIndex = prepared.startCursor.I
	}
	if !access.declinePlanning && compositeOrderSupported(prepared.query) && len(access.equalityBranches) == 0 && len(access.candidateBranches) == 0 && !prepared.condition.needsEntityWitnesses() && !access.builtinUnionOK && (!access.builtinOK || pinnedCompositeIndex != "") {
		access.index, err = g.indexes.PrepareQuery(ctx, prepared.request.ProjectId, prepared.query, prepared.ancestorPath != "", pinnedCompositeIndex)
		if err != nil {
			return err
		}
	}
	return nil
}
