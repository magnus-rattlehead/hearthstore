package datastore

import (
	"context"
	"log/slog"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type queryScan struct {
	rows                                        []*storage.DsEntityRow
	sourceMore, usedIndex, usedComposite        bool
	indexID                                     string
	generation, entriesScanned, probeScanned    int64
	usedEqualityUnion, usedCandidates, fallback bool
}

// scanRunQuery returns a complete response for count shortcuts. Otherwise, scan
// supplies the rows and access metadata used to build the response page.
func (g *GRPCServer) scanRunQuery(ctx context.Context, prepared *preparedRunQuery, read *queryRead, access *queryAccess, scan *queryScan, owned *querySnapshot) (*datastorepb.RunQueryResponse, error) {
	scan.fallback = access.declinePlanning || prepared.condition.needsEntityWitnesses() ||
		access.index == nil && !access.builtinOK && !access.builtinUnionOK && len(access.equalityBranches) == 0 ||
		prepared.kind == "" || isMetadataKind(prepared.kind) ||
		access.builtinUnionOK && (len(builtinAccessOrder(prepared.query.Order)) > 1 || len(projectionFields(prepared.query)) > 0) ||
		prepared.startCursor != nil && prepared.startCursor.I == "fallback" ||
		access.index != nil && (access.index.ReadySince > read.readAt.UnixNano() || !storage.CompositeBoundsSupported(*access.index, prepared.query.Filter))

	if len(access.candidateBranches) > 0 {
		if err := g.scanQueryCandidates(ctx, prepared, read, access, scan); err != nil {
			return nil, err
		}
	}
	if scan.fallback && !scan.usedIndex {
		if response, err := g.scanQueryFallback(ctx, prepared, read, scan, owned); err != nil || response != nil {
			return response, err
		}
	}
	if len(access.equalityBranches) > 0 && !scan.usedIndex {
		if err := g.scanQueryEqualityUnion(ctx, prepared, read, access, scan); err != nil {
			return nil, err
		}
	}
	if (access.builtinUnionOK || access.builtinOK && prepared.condition.needsCandidateFiltering(prepared.query.Order)) && !scan.usedIndex && !read.countOnly {
		if err := g.scanQueryFilteredBuiltin(ctx, prepared, read, access, scan); err != nil {
			return nil, err
		}
	}
	if access.builtinUnionOK && !scan.usedIndex {
		if response, err := g.scanQueryBuiltinUnion(ctx, prepared, read, access, scan); err != nil || response != nil {
			return response, err
		}
	}
	if !scan.usedIndex {
		if response, err := g.scanQueryComposite(ctx, prepared, read, access, scan); err != nil || response != nil {
			return response, err
		}
	}
	if !scan.usedIndex && access.builtinOK {
		if response, err := g.scanQueryBuiltin(ctx, prepared, read, access, scan); err != nil || response != nil {
			return response, err
		}
	}
	scan.entriesScanned += scan.probeScanned
	if !scan.usedIndex {
		return nil, status.Error(codes.FailedPrecondition, "query has no bounded index access path")
	}
	return nil, nil
}

func (g *GRPCServer) scanQueryCandidates(ctx context.Context, prepared *preparedRunQuery, read *queryRead, access *queryAccess, scan *queryScan) error {
	var err error

	if prepared.endBound != nil {
		scan.fallback = true
	}
	var candidates []string
	var complete bool
	candidates, scan.probeScanned, complete, err = g.probeORPaths(ctx, *read.readAt, prepared.request.ProjectId, prepared.database, prepared.namespace, prepared.kind, access.candidateBranches, queryProbeAllowance(ctx, prepared.query))
	if err != nil {
		return err
	}
	if complete {
		scan.rows, scan.sourceMore, err = g.candidateQueryRows(ctx, *read.readAt, prepared.request.ProjectId, prepared.database, prepared.namespace, prepared.kind, prepared.query, prepared.condition, prepared.startCursor, read.batchLimit+int(prepared.query.Offset)+1, candidates)
		if err != nil {
			return err
		}
		scan.usedIndex, scan.usedComposite, scan.usedCandidates, scan.fallback = true, true, true, true
		scan.indexID = "fallback"
	}

	return nil
}

func (g *GRPCServer) scanQueryFallback(ctx context.Context, prepared *preparedRunQuery, read *queryRead, scan *queryScan, owned *querySnapshot) (*datastorepb.RunQueryResponse, error) {
	var err error

	if read.countOnly {
		count, scanned, err := g.countQueryFallback(ctx, prepared.request.ProjectId, prepared.database, prepared.namespace, prepared.kind, prepared.ancestorPath, prepared.condition, read.readAt)
		if err != nil {
			return nil, err
		}
		return countQueryResponse(prepared, read, queryCount{matches: count, documents: read.stats.Documents, scanned: scanned, indexID: "fallback"}), nil
	}
	candidateLimit := read.batchLimit + int(prepared.query.Offset) + 1
	if owned != nil && prepared.entryQuery {
		if owned.fallback == nil {
			owned.fallback, scan.entriesScanned, err = g.prepareFallbackRows(ctx, *read.readAt, prepared.request.ProjectId, prepared.database, prepared.namespace, prepared.kind, prepared.ancestorPath, prepared.query, prepared.condition, prepared.startCursor, nil)
			owned.readStats, owned.fingerprint = read.stats, prepared.fingerprint
		}
		if err == nil {
			scan.rows, scan.sourceMore, err = owned.fallback.page(ctx, candidateLimit)
		}
	} else {
		scan.rows, scan.entriesScanned, scan.sourceMore, err = g.fallbackQueryRows(ctx, *read.readAt, prepared.request.ProjectId, prepared.database, prepared.namespace, prepared.kind, prepared.ancestorPath, prepared.query, prepared.condition, prepared.startCursor, candidateLimit)
	}
	if err != nil {
		return nil, err
	}
	scan.usedIndex, scan.usedComposite = true, true
	scan.indexID = "fallback"

	return nil, nil
}

func (g *GRPCServer) scanQueryEqualityUnion(ctx context.Context, prepared *preparedRunQuery, read *queryRead, access *queryAccess, scan *queryScan) error {
	var err error

	scan.rows, scan.entriesScanned, scan.sourceMore, scan.usedEqualityUnion, err = g.store.DsQueryEqualityUnionAsOf(ctx, *read.readAt, prepared.request.ProjectId, prepared.database, prepared.namespace, prepared.kind, access.equalityBranches, prepared.startCursor, prepared.endBound, read.batchLimit+int(prepared.query.Offset)+1, isKeysOnly(prepared.query), prepared.matcher.accept)
	if err == nil {
		err = prepared.matcher.err
	}
	if err != nil {
		return err
	}
	scan.usedIndex, scan.usedComposite = true, true
	scan.indexID, scan.generation = "builtin:__key__", 1
	accessPath := "builtin:__key__"
	if scan.usedEqualityUnion {
		accessPath = "builtin:union"
	}
	MergeHTTPDetails(ctx, map[string]any{"access_path": accessPath, "index_entries_scanned": scan.entriesScanned})

	return nil
}

func (g *GRPCServer) scanQueryFilteredBuiltin(ctx context.Context, prepared *preparedRunQuery, read *queryRead, access *queryAccess, scan *queryScan) error {
	property, reverse := "__key__", false
	if len(prepared.query.Order) > 0 {
		property = prepared.query.Order[0].Property.GetName()
		reverse = prepared.query.Order[0].Direction == datastorepb.PropertyOrder_DESCENDING
	}
	candidateLimit := read.batchLimit + int(prepared.query.Offset) + 1
	if len(prepared.query.DistinctOn) > 0 || len(projectionFields(prepared.query)) > 0 {
		candidateLimit = 0
	}
	selectionMatcher := newQueryMatcher(ctx, prepared.condition, property)
	var scanFilter *datastorepb.Filter
	if access.builtinOK && access.builtinProperty == property && !prepared.condition.needsCandidateFiltering(prepared.query.Order) {
		scanFilter = prepared.query.Filter
		// Keep membership branches on the existing complete ordering scan:
		// their array witnesses need not be the selected ordering value.
		for _, node := range prepared.condition.nodes {
			if f := node.property; f != nil && f.Op == datastorepb.PropertyFilter_IN {
				scanFilter = nil
				break
			}
		}
	}
	page, err := g.store.DsQueryBuiltin(ctx, storage.BuiltinQuery{
		Project: prepared.request.ProjectId, Database: prepared.database, Namespace: prepared.namespace,
		Kind: prepared.kind, Property: property, Ancestor: prepared.ancestorPath,
		ReadTime: read.readAt, Filter: scanFilter, Reverse: reverse,
		Cursor: prepared.startCursor, Limit: candidateLimit, Accept: prepared.matcher.accept,
		AcceptCandidate: func(entity *datastorepb.Entity, canonical bool, candidate *datastorepb.Value) bool {
			return selectionMatcher.acceptSelection(entity, canonical, property, candidate)
		},
	})
	scan.rows, scan.entriesScanned, scan.sourceMore = page.Rows, page.Scanned, page.More
	if err == nil {
		err = selectionMatcher.err
	}
	if err == nil {
		err = prepared.matcher.err
	}
	if err != nil {
		return err
	}
	scan.usedIndex, scan.usedComposite = true, true
	access.builtinReverse = reverse
	scan.indexID, scan.generation = "builtin:"+property, 1

	return nil
}

func (g *GRPCServer) scanQueryBuiltinUnion(ctx context.Context, prepared *preparedRunQuery, read *queryRead, access *queryAccess, scan *queryScan) (*datastorepb.RunQueryResponse, error) {
	var err error

	unionPaths, accumulatorErr := g.store.NewPathAccumulator(ctx)
	if accumulatorErr != nil {
		return nil, accumulatorErr
	}
	defer unionPaths.Close()
	for _, branch := range access.branches {
		var scanned int64
		if read.readAt == nil {
			scanned, err = g.store.DsVisitBuiltinPaths(ctx, prepared.request.ProjectId, prepared.database, prepared.namespace, prepared.kind, branch.property, prepared.ancestorPath, branch.filter, unionPaths.Add)
		} else {
			scanned, err = g.store.DsVisitBuiltinPathsAsOf(ctx, *read.readAt, prepared.request.ProjectId, prepared.database, prepared.namespace, prepared.kind, branch.property, prepared.ancestorPath, branch.filter, unionPaths.Add)
		}
		if err != nil {
			return nil, err
		}
		scan.entriesScanned += scanned
	}
	scan.usedIndex = true
	MergeHTTPDetails(ctx, map[string]any{"access_path": "builtin:union", "index_entries_scanned": scan.entriesScanned})
	slog.Debug("Datastore built-in index union", "project", prepared.request.ProjectId, "kind", prepared.kind, "branches", len(access.branches), "index_entries_scanned", scan.entriesScanned, "candidates", len(scan.rows))
	if read.countOnly {
		count, err := unionPaths.Count()
		if err != nil {
			return nil, err
		}
		return countQueryResponse(prepared, read, queryCount{matches: count, scanned: scan.entriesScanned, indexID: "builtin:union"}), nil
	}

	return nil, nil
}

func (g *GRPCServer) scanQueryComposite(ctx context.Context, prepared *preparedRunQuery, read *queryRead, access *queryAccess, scan *queryScan) (*datastorepb.RunQueryResponse, error) {
	if access.index == nil && prepared.startCursor != nil && prepared.startCursor.I != "" && (!access.builtinOK || prepared.startCursor.I != "builtin:"+access.builtinProperty) {
		return nil, status.Error(codes.InvalidArgument, "invalid cursor")
	}
	if access.index != nil && (prepared.startCursor == nil || prepared.startCursor.I != "") {
		if prepared.startCursor != nil && (prepared.startCursor.I != access.index.ID || prepared.startCursor.G != access.index.ActiveGeneration || len(prepared.startCursor.K) == 0) {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		equality, _ := equalityFilterValues(prepared.query.Filter)
		prefix := storage.DsCompositePrefix(*access.index, equality)
		if read.countOnly {
			var count, scanned int64
			var supported bool
			var countErr error
			if read.readAt == nil {
				count, scanned, supported, countErr = g.store.DsCountComposite(ctx, prepared.request.ProjectId, prepared.database, prepared.namespace, access.index.ID, prepared.ancestorPath, prepared.query.Filter)
			} else {
				count, scanned, supported, countErr = g.store.DsCountCompositeAsOf(ctx, *read.readAt, prepared.request.ProjectId, prepared.database, prepared.namespace, access.index.ID, prepared.ancestorPath, prepared.query.Filter)
			}
			if countErr != nil {
				return nil, countErr
			}
			if supported {
				MergeHTTPDetails(ctx, map[string]any{"index_id": access.index.ID, "index_entries_scanned": scanned})
				slog.Debug("Datastore composite index count", "project", prepared.request.ProjectId, "kind", prepared.kind, "index", access.index.ID, "index_entries_scanned", scanned, "count", count)
				return countQueryResponse(prepared, read, queryCount{matches: count, scanned: scanned, indexID: access.index.ID}), nil
			}
			count, fallbackScanned, fallbackErr := g.countQueryFallback(ctx, prepared.request.ProjectId, prepared.database, prepared.namespace, prepared.kind, prepared.ancestorPath, prepared.condition, read.readAt)
			if fallbackErr != nil {
				return nil, fallbackErr
			}
			return countQueryResponse(prepared, read, queryCount{matches: count, documents: read.stats.Documents, scanned: fallbackScanned, indexID: "fallback"}), nil
		}
		candidateLimit := 0
		if read.batchLimit > 0 {
			candidateLimit = read.batchLimit + int(prepared.query.Offset) + 1
			if len(prepared.query.DistinctOn) > 0 || len(projectionFields(prepared.query)) > 0 {
				candidateLimit = 0
			}
		}
		var scanned int64
		coveredRange := false
		if !prepared.entryQuery && rangeKeysAccess(prepared.query, access.index, "") {
			probe, err := g.store.DsProbeCompositeAsOf(ctx, storage.CompositeProbe{
				ReadTime:  *read.readAt,
				Project:   prepared.request.ProjectId,
				Database:  prepared.database,
				Namespace: prepared.namespace,
				IndexID:   access.index.ID,
				Filter:    prepared.query.Filter,
				Cursor:    prepared.startCursor,
				Allowance: queryProbeAllowance(ctx, prepared.query),
			})
			scan.rows, coveredRange = probe.Rows, probe.Complete
			scan.probeScanned += probe.Visited
			if err != nil {
				return nil, err
			}
			if coveredRange {
				scan.rows, scan.sourceMore = coveringRangePage(scan.rows, prepared.startCursor, false, candidateLimit, prepared.matcher.accept)
			}
		}
		if !coveredRange {
			query := storage.CompositeQuery{
				Project: prepared.request.ProjectId, Database: prepared.database, Namespace: prepared.namespace,
				IndexID: access.index.ID, Ancestor: prepared.ancestorPath,
				ReadTime: read.readAt, Prefix: prefix, Filter: prepared.query.Filter,
				Cursor: prepared.startCursor, Limit: candidateLimit,
			}
			if len(projectionFields(prepared.query)) > 0 || !prepared.entryQuery && compositeEqualityKeysCovering(prepared.query, access.index) {
				query.Projection = true
				query.Limit = read.batchLimit + int(prepared.query.Offset) + 1
				query.Accept = queryTuplePredicate(prepared.query, prepared.startCursor, prepared.matcher.accept)
				if prepared.entryQuery && query.Cursor == nil {
					query.Cursor = &storage.CursorPayload{G: access.index.ActiveGeneration}
				}
			} else if read.readAt != nil {
				query.Accept = prepared.matcher.accept
			}
			page, err := g.store.DsQueryComposite(ctx, query)
			if err != nil {
				return nil, err
			}
			scan.rows, scanned, scan.sourceMore = page.Rows, page.Scanned, page.More
		}
		if prepared.matcher.err != nil {
			return nil, prepared.matcher.err
		}
		scan.indexID = access.index.ID
		scan.generation = access.index.ActiveGeneration
		scan.entriesScanned = scanned
		MergeHTTPDetails(ctx, map[string]any{"index_id": access.index.ID, "index_entries_scanned": scanned})
		slog.Debug("Datastore composite index scan", "project", prepared.request.ProjectId, "kind", prepared.kind, "index", access.index.ID, "entries_scanned", scanned, "candidates", len(scan.rows))
		scan.usedComposite = true
		scan.usedIndex = true
	}

	return nil, nil
}

func (g *GRPCServer) scanQueryBuiltin(ctx context.Context, prepared *preparedRunQuery, read *queryRead, access *queryAccess, scan *queryScan) (*datastorepb.RunQueryResponse, error) {

	indexID := "builtin:" + access.builtinProperty
	if prepared.startCursor != nil && prepared.startCursor.I != "" && (prepared.startCursor.I != indexID || len(prepared.startCursor.K) == 0) {
		return nil, status.Error(codes.InvalidArgument, "invalid cursor")
	}
	if read.countOnly {
		var count, scanned int64
		var countErr error
		if read.readAt == nil {
			count, scanned, countErr = g.store.DsCountBuiltin(ctx, prepared.request.ProjectId, prepared.database, prepared.namespace, prepared.kind, access.builtinProperty, prepared.ancestorPath, prepared.query.Filter)
		} else {
			count, scanned, countErr = g.store.DsCountBuiltinAsOf(ctx, *read.readAt, prepared.request.ProjectId, prepared.database, prepared.namespace, prepared.kind, access.builtinProperty, prepared.ancestorPath, prepared.query.Filter)
		}
		if countErr != nil {
			return nil, countErr
		}
		MergeHTTPDetails(ctx, map[string]any{"access_path": indexID, "index_entries_scanned": scanned})
		slog.Debug("Datastore built-in index count", "project", prepared.request.ProjectId, "kind", prepared.kind, "property", access.builtinProperty, "index_entries_scanned", scanned, "count", count)
		return countQueryResponse(prepared, read, queryCount{matches: count, scanned: scanned, indexID: indexID}), nil
	}
	candidateLimit := 0
	if read.batchLimit > 0 {
		candidateLimit = read.batchLimit + int(prepared.query.Offset) + 1
		if len(prepared.query.DistinctOn) > 0 || len(projectionFields(prepared.query)) > 0 {
			candidateLimit = 0
		}
	}
	var scanned int64
	// A single equality has one persisted entry per entity, including arrays
	// and dotted-path collisions, so keys-only reads need no source entity.
	equality := prepared.query.Filter.GetPropertyFilter()
	coveringKeys := isKeysOnly(prepared.query) && (access.builtinProperty == "__key__" || equality.GetOp() == datastorepb.PropertyFilter_EQUAL && equality.GetProperty().GetName() == access.builtinProperty)
	coveredRange := false
	if !prepared.entryQuery && rangeKeysAccess(prepared.query, nil, access.builtinProperty) {
		probe, err := g.store.DsProbeBuiltinAsOf(ctx, storage.BuiltinProbe{
			ReadTime:  *read.readAt,
			Project:   prepared.request.ProjectId,
			Database:  prepared.database,
			Namespace: prepared.namespace,
			Kind:      prepared.kind,
			Property:  access.builtinProperty,
			Filter:    prepared.query.Filter,
			Reverse:   access.builtinReverse,
			Cursor:    prepared.startCursor,
			Allowance: queryProbeAllowance(ctx, prepared.query),
		})
		scan.rows, coveredRange = probe.Rows, probe.Complete
		scan.probeScanned += probe.Visited
		if err != nil {
			return nil, err
		}
		if coveredRange {
			scan.rows, scan.sourceMore = coveringRangePage(scan.rows, prepared.startCursor, access.builtinReverse, candidateLimit, prepared.matcher.accept)
		}
	}
	if !coveredRange {
		query := storage.BuiltinQuery{
			Project: prepared.request.ProjectId, Database: prepared.database, Namespace: prepared.namespace,
			Kind: prepared.kind, Property: access.builtinProperty, Ancestor: prepared.ancestorPath,
			ReadTime: read.readAt, Filter: prepared.query.Filter, Reverse: access.builtinReverse,
			Cursor: prepared.startCursor, Limit: candidateLimit,
		}
		if len(projectionFields(prepared.query)) > 0 || coveringKeys {
			query.Projection = true
			query.Limit = read.batchLimit + int(prepared.query.Offset) + 1
			query.Accept = queryTuplePredicate(prepared.query, prepared.startCursor, prepared.matcher.accept)
		}
		page, err := g.store.DsQueryBuiltin(ctx, query)
		if err != nil {
			return nil, err
		}
		scan.rows, scanned, scan.sourceMore = page.Rows, page.Scanned, page.More
	}
	if prepared.matcher.err != nil {
		return nil, prepared.matcher.err
	}
	scan.indexID = indexID
	scan.generation = 1
	scan.entriesScanned = scanned
	MergeHTTPDetails(ctx, map[string]any{
		"access_path":           indexID,
		"index_entries_scanned": scanned,
	})
	slog.Debug("Datastore built-in index scan", "project", prepared.request.ProjectId, "kind", prepared.kind, "property", access.builtinProperty, "index_entries_scanned", scanned, "candidates", len(scan.rows))
	scan.usedComposite = true
	scan.usedIndex = true

	return nil, nil
}
