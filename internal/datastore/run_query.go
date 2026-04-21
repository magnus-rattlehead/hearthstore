package datastore

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// RunQuery executes a structured query.
func (g *GRPCServer) RunQuery(ctx context.Context, req *datastorepb.RunQueryRequest) (*datastorepb.RunQueryResponse, error) {
	if req.ProjectId == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id is required")
	}
	database := req.DatabaseId
	if database == "" {
		database = defaultDatabase
	}
	namespace := req.PartitionId.GetNamespaceId()

	sq, ok := req.QueryType.(*datastorepb.RunQueryRequest_Query)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "only structured queries are supported (not GQL)")
	}
	q := sq.Query
	if q == nil {
		return nil, status.Error(codes.InvalidArgument, "query is required")
	}

	// Handle ExplainOptions: analyze=false (or unset) → plan only, no execution.
	explainOpts := req.GetExplainOptions()
	if explainOpts != nil && !explainOpts.Analyze {
		plan := buildRunQueryPlan(q)
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

	var kind, ancestorPath string
	if len(q.Kind) > 0 {
		kind = q.Kind[0].Name
	}
	if q.Filter != nil {
		ancestorPath = extractAncestorPath(q.Filter, req.ProjectId, database, namespace)
	}

	readAt, activeTxID, newTxIDForResp := g.resolveReadOptions(req.GetReadOptions())

	// Build SQL filter clause for pushdown into DsQueryKind.
	var filterSQL string
	var filterArgs []any
	var needsGoFilter bool
	if q.Filter != nil && readAt == nil {
		filterSQL, filterArgs, needsGoFilter = buildDsWhereClause(req.ProjectId, database, namespace, kind, q.Filter)
		if filterSQL == "" {
			needsGoFilter = true // no SQL clause generated; Go must filter
		}
	}

	// limit=0 means "return no entities" (used by count() to read skipped_results only).
	hasLimit := q.Limit != nil
	limit := 0
	if hasLimit {
		limit = int(q.Limit.Value)
	}

	// useSQL: push ORDER BY, keyset cursor, and LIMIT into SQLite.
	// Conditions: live read (no snapshot), no Go-side filter needed, no DISTINCT,
	// a positive LIMIT, and no OFFSET (offset is not pushed to SQL).
	useSQL := readAt == nil && !needsGoFilter &&
		len(q.DistinctOn) == 0 && hasLimit && limit > 0 && q.Offset == 0

	start := time.Now()
	var rows []*storage.DsEntityRow
	var filledSorts []storage.DsSortSpec

	if useSQL {
		sortSpecs := buildSortSpecs(q.Order)
		cp := decodeCursorPayload(q.StartCursor)
		var sqlErr error
		filledSorts, rows, sqlErr = g.store.DsQueryKindLimited(
			req.ProjectId, database, namespace, kind, ancestorPath,
			filterSQL, filterArgs, sortSpecs, cp, limit,
		)
		if errors.Is(sqlErr, storage.ErrColumnNotDetected) {
			useSQL = false // fall through to Go-side path
		} else if sqlErr != nil {
			return nil, sqlErr
		}
	}

	if !useSQL {
		var fetchErr error
		if readAt != nil {
			rows, fetchErr = g.store.DsQueryKindAsOf(req.ProjectId, database, namespace, kind, ancestorPath, *readAt)
		} else {
			rows, fetchErr = g.store.DsQueryKind(req.ProjectId, database, namespace, kind, ancestorPath, filterSQL, filterArgs)
		}
		if fetchErr != nil {
			return nil, fetchErr
		}
	}

	// Apply Go-side filter when: snapshot read (no pushdown), or pushdown was partial/absent.
	if q.Filter != nil && (readAt != nil || needsGoFilter) {
		var filtered []*storage.DsEntityRow
		for _, row := range rows {
			if matchesFilter(row.Entity, q.Filter) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}

	// Record entity versions into the transaction read set for OCC conflict detection.
	if activeTxID != "" {
		g.txMu.Lock()
		if entry, ok := g.txns[activeTxID]; ok && !entry.readOnly {
			if entry.reads == nil {
				entry.reads = make(map[txReadKey]int64)
			}
			for _, row := range rows {
				entry.reads[txReadKey{req.ProjectId, database, namespace, row.Path}] = row.Version
			}
			g.txns[activeTxID] = entry
		}
		g.txMu.Unlock()
	}

	if !useSQL {
		if len(q.Order) > 0 {
			sort.SliceStable(rows, func(i, j int) bool {
				for _, ord := range q.Order {
					prop := ord.Property.GetName()
					vi := getProp(rows[i].Entity, prop)
					vj := getProp(rows[j].Entity, prop)
					cmp := compareValues(vi, vj)
					if cmp == 0 {
						continue
					}
					if ord.Direction == datastorepb.PropertyOrder_DESCENDING {
						return cmp > 0
					}
					return cmp < 0
				}
				return false
			})
		} else if hasKeyFilter(q.Filter) {
			// When filtering by __key__ (IN, NOT_IN, EQUAL), results are returned in full-path key order
			// (parent before child), consistent with Datastore's multi-key lookup semantics.
			sort.SliceStable(rows, func(i, j int) bool {
				return rows[i].Path < rows[j].Path
			})
		} else {
			// Default sort for kind queries: ascending by entity name (last path segment),
			// matching Datastore's kind-index ordering where entities sort by own key name.
			sort.SliceStable(rows, func(i, j int) bool {
				return entityName(rows[i].Path) < entityName(rows[j].Path)
			})
		}

		// Apply distinct_on (groupBy) deduplication: keep first entity per unique field combination.
		if len(q.DistinctOn) > 0 {
			seen := make(map[string]bool)
			var distinct []*storage.DsEntityRow
			for _, row := range rows {
				key := distinctKey(row.Entity, q.DistinctOn)
				if !seen[key] {
					seen[key] = true
					distinct = append(distinct, row)
				}
			}
			rows = distinct
		}

		if len(q.StartCursor) > 0 {
			startPath := decodeCursor(q.StartCursor)
			if startPath != "" {
				// Find the cursor entity in the sorted result set and return everything after it.
				cutIdx := 0
				for i, row := range rows {
					if row.Path == startPath {
						cutIdx = i + 1
						break
					}
				}
				rows = rows[cutIdx:]
			}
		}
	}

	var skippedResults int32
	if !useSQL {
		if q.Offset > 0 {
			if int(q.Offset) >= len(rows) {
				skippedResults = int32(len(rows))
				rows = nil
			} else {
				skippedResults = q.Offset
				rows = rows[q.Offset:]
			}
		}
		if hasLimit && limit == 0 {
			rows = nil
		} else if limit > 0 && limit < len(rows) {
			rows = rows[:limit]
		}
	}

	keysOnly := isKeysOnly(q)
	projFields := projectionFields(q)

	results := make([]*datastorepb.EntityResult, 0, len(rows))
	var endCursor []byte
	for _, row := range rows {
		var rowCursor []byte
		if useSQL {
			rowCursor = buildCursor(row.Path, filledSorts, row)
		} else {
			rowCursor = encodeCursor(row.Path)
		}
		e := row.Entity
		if keysOnly {
			results = append(results, &datastorepb.EntityResult{
				Entity:     &datastorepb.Entity{Key: e.Key},
				Version:    row.Version,
				CreateTime: row.CreateTime,
				UpdateTime: row.UpdateTime,
				Cursor:     rowCursor,
			})
		} else if len(projFields) > 0 {
			// Projection: expand array-valued projected fields into multiple rows.
			for _, pe := range expandProjection(e, projFields) {
				results = append(results, &datastorepb.EntityResult{
					Entity:     pe,
					Version:    row.Version,
					CreateTime: row.CreateTime,
					UpdateTime: row.UpdateTime,
					Cursor:     rowCursor,
				})
			}
		} else {
			results = append(results, &datastorepb.EntityResult{
				Entity:     e,
				Version:    row.Version,
				CreateTime: row.CreateTime,
				UpdateTime: row.UpdateTime,
				Cursor:     rowCursor,
			})
		}
		endCursor = rowCursor
	}

	moreResults := datastorepb.QueryResultBatch_NO_MORE_RESULTS
	if limit > 0 && len(rows) == limit {
		// MORE_RESULTS_AFTER_LIMIT tells the client that the limit was reached
		// and there may be additional results, but the client should stop here.
		// The nodejs-datastore client (and other SDKs) treat this as a terminal
		// signal for the current page; use a start_cursor on a new query to paginate.
		moreResults = datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT
	}

	entityResultType := datastorepb.EntityResult_FULL
	if keysOnly {
		entityResultType = datastorepb.EntityResult_KEY_ONLY
	} else if len(projFields) > 0 {
		entityResultType = datastorepb.EntityResult_PROJECTION
	}

	resp := &datastorepb.RunQueryResponse{
		Batch: &datastorepb.QueryResultBatch{
			EntityResults:    results,
			SkippedResults:   skippedResults,
			EndCursor:        endCursor,
			MoreResults:      moreResults,
			EntityResultType: entityResultType,
			ReadTime:         timestamppb.Now(),
		},
	}
	if newTxIDForResp != "" {
		resp.Transaction = []byte(newTxIDForResp)
	}
	// analyze=true: include plan summary + execution stats.
	if explainOpts != nil && explainOpts.Analyze {
		resp.ExplainMetrics = &datastorepb.ExplainMetrics{
			PlanSummary:    buildRunQueryPlan(q),
			ExecutionStats: buildRunQueryExecutionStats(len(results), time.Since(start)),
		}
	}
	return resp, nil
}

func (s *Server) handleRunQuery(w http.ResponseWriter, r *http.Request, project string) {
	var req datastorepb.RunQueryRequest
	if !readProtoJSON(w, r.Body, &req) {
		return
	}
	if req.ProjectId == "" {
		req.ProjectId = project
	}
	resp, err := s.grpc.RunQuery(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	writeProtoJSON(w, resp)
}


func matchesFilter(entity *datastorepb.Entity, f *datastorepb.Filter) bool {
	if f == nil {
		return true
	}
	switch ft := f.FilterType.(type) {
	case *datastorepb.Filter_PropertyFilter:
		return matchesPropertyFilter(entity, ft.PropertyFilter)
	case *datastorepb.Filter_CompositeFilter:
		cf := ft.CompositeFilter
		switch cf.Op {
		case datastorepb.CompositeFilter_AND:
			for _, sub := range cf.Filters {
				if !matchesFilter(entity, sub) {
					return false
				}
			}
			return true
		case datastorepb.CompositeFilter_OR:
			for _, sub := range cf.Filters {
				if matchesFilter(entity, sub) {
					return true
				}
			}
			return false
		}
	}
	return true
}

// evalScalarOp tests whether compareValues(a, b) satisfies op for scalar (non-array) values.
// IN and NOT_IN are handled separately because their filter value is an array of candidates.
func evalScalarOp(a, b *datastorepb.Value, op datastorepb.PropertyFilter_Operator) bool {
	switch op {
	case datastorepb.PropertyFilter_EQUAL:
		return compareValues(a, b) == 0
	case datastorepb.PropertyFilter_NOT_EQUAL:
		return compareValues(a, b) != 0
	case datastorepb.PropertyFilter_LESS_THAN:
		return compareValues(a, b) < 0
	case datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL:
		return compareValues(a, b) <= 0
	case datastorepb.PropertyFilter_GREATER_THAN:
		return compareValues(a, b) > 0
	case datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
		return compareValues(a, b) >= 0
	}
	return false
}

func matchesPropertyFilter(entity *datastorepb.Entity, f *datastorepb.PropertyFilter) bool {
	if f.Property.GetName() == "__key__" {
		return matchesKeyFilter(entity, f)
	}
	val := getProp(entity, f.Property.GetName())
	filterVal := f.Value

	// If the entity property is an array, Datastore semantics say the filter matches
	// if any element of the array satisfies the condition (for scalar comparison ops).
	if av, ok := val.GetValueType().(*datastorepb.Value_ArrayValue); ok && av.ArrayValue != nil {
		elems := av.ArrayValue.Values
		switch f.Op {
		case datastorepb.PropertyFilter_IN:
			filterAv := filterVal.GetArrayValue()
			if filterAv == nil {
				return false
			}
			for _, elem := range elems {
				for _, fe := range filterAv.Values {
					if compareValues(elem, fe) == 0 {
						return true
					}
				}
			}
			return false
		case datastorepb.PropertyFilter_NOT_IN:
			filterAv := filterVal.GetArrayValue()
			if filterAv == nil {
				return true
			}
			for _, elem := range elems {
				for _, fe := range filterAv.Values {
					if compareValues(elem, fe) == 0 {
						return false
					}
				}
			}
			return true
		case datastorepb.PropertyFilter_HAS_ANCESTOR:
			return true
		default:
			for _, elem := range elems {
				if evalScalarOp(elem, filterVal, f.Op) {
					return true
				}
			}
			return false
		}
	}

	switch f.Op {
	case datastorepb.PropertyFilter_IN:
		av := filterVal.GetArrayValue()
		if av == nil {
			return false
		}
		for _, elem := range av.Values {
			if compareValues(val, elem) == 0 {
				return true
			}
		}
		return false
	case datastorepb.PropertyFilter_NOT_IN:
		av := filterVal.GetArrayValue()
		if av == nil {
			return true
		}
		for _, elem := range av.Values {
			if compareValues(val, elem) == 0 {
				return false
			}
		}
		return true
	case datastorepb.PropertyFilter_HAS_ANCESTOR:
		return true
	default:
		return evalScalarOp(val, filterVal, f.Op)
	}
}

func matchesKeyFilter(entity *datastorepb.Entity, f *datastorepb.PropertyFilter) bool {
	_, _, _, _, _, ep := keyComponents(entity.Key)

	switch f.Op {
	case datastorepb.PropertyFilter_HAS_ANCESTOR:
		return true
	case datastorepb.PropertyFilter_EQUAL:
		filterKey := f.Value.GetKeyValue()
		if filterKey == nil {
			return false
		}
		_, _, _, _, _, fp := keyComponents(filterKey)
		return ep == fp
	case datastorepb.PropertyFilter_NOT_EQUAL:
		filterKey := f.Value.GetKeyValue()
		if filterKey == nil {
			return true
		}
		_, _, _, _, _, fp := keyComponents(filterKey)
		return ep != fp
	case datastorepb.PropertyFilter_IN:
		av := f.Value.GetArrayValue()
		if av == nil {
			return false
		}
		for _, elem := range av.Values {
			filterKey := elem.GetKeyValue()
			if filterKey == nil {
				continue
			}
			_, _, _, _, _, fp := keyComponents(filterKey)
			if ep == fp {
				return true
			}
		}
		return false
	case datastorepb.PropertyFilter_NOT_IN:
		av := f.Value.GetArrayValue()
		if av == nil {
			return true
		}
		for _, elem := range av.Values {
			filterKey := elem.GetKeyValue()
			if filterKey == nil {
				continue
			}
			_, _, _, _, _, fp := keyComponents(filterKey)
			if ep == fp {
				return false
			}
		}
		return true
	}
	return true
}

func extractAncestorPath(f *datastorepb.Filter, project, database, namespace string) string {
	if f == nil {
		return ""
	}
	switch ft := f.FilterType.(type) {
	case *datastorepb.Filter_PropertyFilter:
		pf := ft.PropertyFilter
		if pf.Op == datastorepb.PropertyFilter_HAS_ANCESTOR && pf.Property.GetName() == "__key__" {
			ancestorKey := pf.Value.GetKeyValue()
			if ancestorKey == nil {
				return ""
			}
			_, _, _, _, _, path := keyComponents(ancestorKey)
			return path
		}
	case *datastorepb.Filter_CompositeFilter:
		for _, sub := range ft.CompositeFilter.Filters {
			if p := extractAncestorPath(sub, project, database, namespace); p != "" {
				return p
			}
		}
	}
	return ""
}

func getProp(entity *datastorepb.Entity, name string) *datastorepb.Value {
	if entity == nil || entity.Properties == nil {
		return nil
	}
	return entity.Properties[name]
}

func isKeysOnly(q *datastorepb.Query) bool {
	return len(q.Projection) == 1 && q.Projection[0].Property.GetName() == "__key__"
}

func projectionFields(q *datastorepb.Query) []string {
	if len(q.Projection) == 0 {
		return nil
	}
	names := make([]string, 0, len(q.Projection))
	for _, p := range q.Projection {
		n := p.Property.GetName()
		if n != "__key__" {
			names = append(names, n)
		}
	}
	return names
}

func projectEntity(entity *datastorepb.Entity, fields []string) *datastorepb.Entity {
	keep := make(map[string]bool, len(fields))
	for _, f := range fields {
		keep[f] = true
	}
	props := make(map[string]*datastorepb.Value, len(fields))
	for k, v := range entity.Properties {
		if keep[k] {
			props[k] = v
		}
	}
	return &datastorepb.Entity{Key: entity.Key, Properties: props}
}

// expandProjection returns one projected entity per array element for any array-valued
// projected field. Non-array fields are copied as-is. Only the first array field is expanded.
func expandProjection(entity *datastorepb.Entity, fields []string) []*datastorepb.Entity {
	for _, f := range fields {
		v := getProp(entity, f)
		if v == nil {
			continue
		}
		av, ok := v.ValueType.(*datastorepb.Value_ArrayValue)
		if !ok || av.ArrayValue == nil || len(av.ArrayValue.Values) == 0 {
			continue
		}
		// Expand: one row per array element.
		var result []*datastorepb.Entity
		for _, elem := range av.ArrayValue.Values {
			props := make(map[string]*datastorepb.Value, len(fields))
			for _, f2 := range fields {
				if f2 == f {
					props[f2] = elem
				} else if v2 := getProp(entity, f2); v2 != nil {
					props[f2] = v2
				}
			}
			result = append(result, &datastorepb.Entity{Key: entity.Key, Properties: props})
		}
		return result
	}
	// No array expansion needed: return a single projected entity.
	return []*datastorepb.Entity{projectEntity(entity, fields)}
}

// entityName returns the last path segment (entity name or ID) from a storage path.
func entityName(path string) string {
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[i+1:]
	}
	return path
}

// hasKeyFilter reports whether the filter tree contains a non-ancestor __key__ property filter
// (e.g. __key__ IN, __key__ =, __key__ NOT_IN). These queries use full-path key ordering.
func hasKeyFilter(f *datastorepb.Filter) bool {
	if f == nil {
		return false
	}
	switch ft := f.FilterType.(type) {
	case *datastorepb.Filter_PropertyFilter:
		pf := ft.PropertyFilter
		if pf.Property.GetName() == "__key__" && pf.Op != datastorepb.PropertyFilter_HAS_ANCESTOR {
			return true
		}
	case *datastorepb.Filter_CompositeFilter:
		for _, sub := range ft.CompositeFilter.Filters {
			if hasKeyFilter(sub) {
				return true
			}
		}
	}
	return false
}



// dsTimeLayout must match storage.timeLayout for lexicographic timestamp comparisons.
const dsTimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// dsInSubquery builds the preamble for a field-index IN-subquery filter.
// Drives from idx_ds_field (project, database, namespace, kind, field_path, …)
// instead of the old correlated EXISTS approach, which lets idx_ds_doc_join be dropped.
// prefix ends just before the closing ')'; callers append the value condition and ')'.
func dsInSubquery(project, database, namespace, kind string) (prefix string, baseArgs []any) {
	return `d.path IN (SELECT doc_path FROM ds_field_index ` +
		`WHERE project=? AND database=? AND namespace=? AND kind=? AND field_path=?`,
		[]any{project, database, namespace, kind}
}

func dsNotInSubquery(project, database, namespace, kind string) (prefix string, baseArgs []any) {
	return `d.path NOT IN (SELECT doc_path FROM ds_field_index ` +
		`WHERE project=? AND database=? AND namespace=? AND kind=? AND field_path=?`,
		[]any{project, database, namespace, kind}
}

// dsValueColumn maps a datastorepb.Value to the ds_field_index column name and
// a SQL-compatible value for that column. Returns ok=false for types handled
// separately (GeoPoint, EntityValue) or unsupported (ArrayValue).
func dsValueColumn(v *datastorepb.Value) (col string, sqlVal any, ok bool) {
	if v == nil {
		return "", nil, false
	}
	switch vt := v.GetValueType().(type) {
	case *datastorepb.Value_StringValue:
		return "value_string", vt.StringValue, true
	case *datastorepb.Value_IntegerValue:
		return "value_int", vt.IntegerValue, true
	case *datastorepb.Value_DoubleValue:
		return "value_double", vt.DoubleValue, true
	case *datastorepb.Value_BooleanValue:
		b := int64(0)
		if vt.BooleanValue {
			b = 1
		}
		return "value_bool", b, true
	case *datastorepb.Value_NullValue:
		return "value_null", int64(1), true
	case *datastorepb.Value_BlobValue:
		return "value_bytes", vt.BlobValue, true
	case *datastorepb.Value_TimestampValue:
		s := vt.TimestampValue.AsTime().UTC().Format(dsTimeLayout)
		return "value_string", s, true
	case *datastorepb.Value_KeyValue:
		_, _, _, _, _, path := keyComponents(vt.KeyValue)
		return "value_ref", path, true
	}
	return "", nil, false
}

// buildDsWhereClause translates a Datastore filter into a SQL WHERE fragment and
// args for correlated EXISTS subqueries against table alias "d" (ds_documents).
//
// needsGoFilter=true when any sub-filter cannot be fully represented in SQL
// (GeoPoint, EntityValue, NOT_EQUAL, NOT_IN).
// Callers should run matchesFilter on the SQL-reduced result set in that case.
//
// __key__ HAS_ANCESTOR is already handled via ancestorPath in DsQueryKind, so it
// is skipped here. Other __key__ filters emit direct conditions on d.path.
func buildDsWhereClause(project, database, namespace, kind string, f *datastorepb.Filter) (clause string, args []any, needsGoFilter bool) {
	if f == nil {
		return "", nil, false
	}
	switch ft := f.FilterType.(type) {
	case *datastorepb.Filter_PropertyFilter:
		return buildDsPropClause(project, database, namespace, kind, ft.PropertyFilter)
	case *datastorepb.Filter_CompositeFilter:
		cf := ft.CompositeFilter
		var parts []string
		for _, sub := range cf.Filters {
			sc, sa, sg := buildDsWhereClause(project, database, namespace, kind, sub)
			if sg {
				needsGoFilter = true
			}
			if sc != "" {
				parts = append(parts, sc)
				args = append(args, sa...)
			}
		}
		if len(parts) == 0 {
			return "", args, needsGoFilter
		}
		switch cf.Op {
		case datastorepb.CompositeFilter_AND:
			clause = strings.Join(parts, " AND ")
		case datastorepb.CompositeFilter_OR:
			if len(parts) == 1 {
				clause = parts[0]
			} else {
				clause = "(" + strings.Join(parts, " OR ") + ")"
			}
		default:
			return "", nil, true
		}
		return clause, args, needsGoFilter
	}
	return "", nil, false
}

func buildDsPropClause(project, database, namespace, kind string, pf *datastorepb.PropertyFilter) (clause string, args []any, needsGoFilter bool) {
	prop := pf.Property.GetName()
	if prop == "__key__" {
		return buildDsKeyClause(pf)
	}
	if pf.Op == datastorepb.PropertyFilter_HAS_ANCESTOR {
		return "", nil, false // already handled by ancestorPath
	}

	inPfx, inBase := dsInSubquery(project, database, namespace, kind)
	notInPfx, notInBase := dsNotInSubquery(project, database, namespace, kind)

	// GeoPoint EQUAL: two-column IN-subquery on (value_lat, value_lng).
	if gp, ok := pf.Value.ValueType.(*datastorepb.Value_GeoPointValue); ok {
		if pf.Op == datastorepb.PropertyFilter_EQUAL && gp.GeoPointValue != nil {
			lat := gp.GeoPointValue.Latitude
			lng := gp.GeoPointValue.Longitude
			return fmt.Sprintf("%s AND value_lat=? AND value_lng=?)", inPfx),
				append(append(inBase, prop), lat, lng), false
		}
		return "", nil, true
	}

	// EntityValue EQUAL: canonical proto bytes in value_bytes.
	if ev, ok := pf.Value.ValueType.(*datastorepb.Value_EntityValue); ok {
		if pf.Op == datastorepb.PropertyFilter_EQUAL {
			opts := proto.MarshalOptions{Deterministic: true}
			b, _ := opts.Marshal(ev.EntityValue)
			return fmt.Sprintf("%s AND value_bytes=?)", inPfx),
				append(append(inBase, prop), b), false
		}
		return "", nil, true
	}

	// ArrayValue EQUAL: canonical proto bytes in value_bytes.
	if av, ok := pf.Value.ValueType.(*datastorepb.Value_ArrayValue); ok {
		if pf.Op == datastorepb.PropertyFilter_EQUAL {
			opts := proto.MarshalOptions{Deterministic: true}
			b, _ := opts.Marshal(av.ArrayValue)
			return fmt.Sprintf("%s AND value_bytes=?)", inPfx),
				append(append(inBase, prop), b), false
		}
		return "", nil, true
	}

	col, sqlVal, ok := dsValueColumn(pf.Value)
	if !ok {
		return "", nil, true
	}

	switch pf.Op {
	case datastorepb.PropertyFilter_EQUAL:
		return fmt.Sprintf("%s AND %s=?)", inPfx, col),
			append(append(inBase, prop), sqlVal), false
	case datastorepb.PropertyFilter_NOT_EQUAL:
		return fmt.Sprintf("%s AND %s=?)", notInPfx, col),
			append(append(notInBase, prop), sqlVal), true
	case datastorepb.PropertyFilter_LESS_THAN:
		return fmt.Sprintf("%s AND %s<?)", inPfx, col),
			append(append(inBase, prop), sqlVal), false
	case datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL:
		return fmt.Sprintf("%s AND %s<=?)", inPfx, col),
			append(append(inBase, prop), sqlVal), false
	case datastorepb.PropertyFilter_GREATER_THAN:
		return fmt.Sprintf("%s AND %s>?)", inPfx, col),
			append(append(inBase, prop), sqlVal), false
	case datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
		return fmt.Sprintf("%s AND %s>=?)", inPfx, col),
			append(append(inBase, prop), sqlVal), false
	case datastorepb.PropertyFilter_IN:
		return buildDsInNotInClause(project, database, namespace, kind, prop, pf.Value, false)
	case datastorepb.PropertyFilter_NOT_IN:
		return buildDsInNotInClause(project, database, namespace, kind, prop, pf.Value, true)
	}
	return "", nil, true
}

func buildDsKeyClause(pf *datastorepb.PropertyFilter) (clause string, args []any, needsGoFilter bool) {
	switch pf.Op {
	case datastorepb.PropertyFilter_HAS_ANCESTOR:
		return "", nil, false
	case datastorepb.PropertyFilter_EQUAL:
		k := pf.Value.GetKeyValue()
		if k == nil {
			return "", nil, true
		}
		_, _, _, _, _, path := keyComponents(k)
		return "d.path=?", []any{path}, false
	case datastorepb.PropertyFilter_NOT_EQUAL:
		k := pf.Value.GetKeyValue()
		if k == nil {
			return "", nil, true
		}
		_, _, _, _, _, path := keyComponents(k)
		return "d.path!=?", []any{path}, false
	case datastorepb.PropertyFilter_IN:
		av := pf.Value.GetArrayValue()
		if av == nil || len(av.Values) == 0 {
			return "1=0", nil, false
		}
		var pArgs []any
		for _, elem := range av.Values {
			k := elem.GetKeyValue()
			if k == nil {
				continue
			}
			_, _, _, _, _, path := keyComponents(k)
			pArgs = append(pArgs, path)
		}
		if len(pArgs) == 0 {
			return "1=0", nil, false
		}
		ph := strings.Repeat(",?", len(pArgs))[1:]
		return "d.path IN (" + ph + ")", pArgs, false
	case datastorepb.PropertyFilter_NOT_IN:
		av := pf.Value.GetArrayValue()
		if av == nil || len(av.Values) == 0 {
			return "", nil, false
		}
		var pArgs []any
		for _, elem := range av.Values {
			k := elem.GetKeyValue()
			if k == nil {
				continue
			}
			_, _, _, _, _, path := keyComponents(k)
			pArgs = append(pArgs, path)
		}
		if len(pArgs) == 0 {
			return "", nil, false
		}
		ph := strings.Repeat(",?", len(pArgs))[1:]
		return "d.path NOT IN (" + ph + ")", pArgs, false
	}
	return "", nil, true
}

// buildDsInNotInClause builds the IN-subquery clause for IN and NOT_IN filters.
func buildDsInNotInClause(project, database, namespace, kind, prop string, filterVal *datastorepb.Value, negate bool) (clause string, args []any, needsGoFilter bool) {
	av := filterVal.GetArrayValue()
	if av == nil || len(av.Values) == 0 {
		if negate {
			return "", nil, false
		}
		return "1=0", nil, false
	}
	var col string
	var vals []any
	for _, elem := range av.Values {
		c, v, ok := dsValueColumn(elem)
		if !ok {
			return "", nil, true
		}
		if col == "" {
			col = c
		} else if col != c {
			return "", nil, true // mixed types → Go fallback
		}
		vals = append(vals, v)
	}
	if len(vals) == 0 {
		if negate {
			return "", nil, false
		}
		return "1=0", nil, false
	}
	ph := strings.Repeat(",?", len(vals))[1:]
	pfx, base := dsInSubquery(project, database, namespace, kind)
	if negate {
		pfx, base = dsNotInSubquery(project, database, namespace, kind)
	}
	clause = fmt.Sprintf("%s AND %s IN (%s))", pfx, col, ph)
	args = append(append(base, prop), vals...)
	return clause, args, negate
}

// buildSortSpecs converts q.Order into DsSortSpec entries for SQL ORDER BY.
// Returns nil when there are no explicit sort orders (uses SQL default d.path ASC).
// __key__ gets Col="__path__" so it is handled via d.path without a field index join.
func buildSortSpecs(orders []*datastorepb.PropertyOrder) []storage.DsSortSpec {
	if len(orders) == 0 {
		return nil
	}
	specs := make([]storage.DsSortSpec, len(orders))
	for i, ord := range orders {
		col := ""
		if ord.Property.GetName() == "__key__" {
			col = "__path__"
		}
		specs[i] = storage.DsSortSpec{
			FieldPath: ord.Property.GetName(),
			Col:       col,
			Desc:      ord.Direction == datastorepb.PropertyOrder_DESCENDING,
		}
	}
	return specs
}

// decodeCursorPayload decodes a start-cursor byte slice to a *CursorPayload.
// Returns nil when the cursor is empty or cannot be decoded.
func decodeCursorPayload(b []byte) *storage.CursorPayload {
	if len(b) == 0 {
		return nil
	}
	cp, ok := decodeCursorFull(b)
	if !ok {
		return nil
	}
	return &cp
}

// distinctKey returns a string key representing the distinct_on field values of an entity.
func distinctKey(entity *datastorepb.Entity, distinctOn []*datastorepb.PropertyReference) string {
	parts := make([]string, 0, len(distinctOn))
	for _, ref := range distinctOn {
		v := getProp(entity, ref.GetName())
		parts = append(parts, fmt.Sprintf("%v", v))
	}
	return strings.Join(parts, "|")
}
