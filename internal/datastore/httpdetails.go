package datastore

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// httpDetailsKey is the context key for the HTTP details slot.
type httpDetailsKey struct{}

// HTTPDetailsSlot is a mutable holder written by handlers, read by the logging middleware.
type HTTPDetailsSlot struct{ V map[string]any }

// WithHTTPDetails returns a new context carrying an empty slot and the slot itself.
func WithHTTPDetails(ctx context.Context) (context.Context, *HTTPDetailsSlot) {
	s := &HTTPDetailsSlot{}
	return context.WithValue(ctx, httpDetailsKey{}, s), s
}

// SetHTTPDetails stores details in the slot attached to ctx (no-op if no slot).
func SetHTTPDetails(ctx context.Context, d map[string]any) {
	if s, ok := ctx.Value(httpDetailsKey{}).(*HTTPDetailsSlot); ok {
		s.V = d
	}
}

// MergeHTTPDetails merges d into the existing slot map, adding/overwriting keys.
// No-op if no slot is attached to ctx or d is nil.
func MergeHTTPDetails(ctx context.Context, d map[string]any) {
	if len(d) == 0 {
		return
	}
	if s, ok := ctx.Value(httpDetailsKey{}).(*HTTPDetailsSlot); ok {
		if s.V == nil {
			s.V = make(map[string]any, len(d))
		}
		for k, v := range d {
			s.V[k] = v
		}
	}
}

// ---- request builders ----

// DSLookupDetails extracts panel details from a LookupRequest.
func DSLookupDetails(req *datastorepb.LookupRequest) map[string]any {
	keys := req.GetKeys()
	d := map[string]any{"keys": len(keys)}
	if kinds := dsKeyKinds(keys); len(kinds) > 0 {
		d["kinds"] = kinds
	}
	if paths := dsKeyPaths(keys, 5); len(paths) > 0 {
		d["paths"] = paths
	}
	// Namespace is carried per-key; extract from the first key.
	if len(keys) > 0 {
		if ns := keys[0].GetPartitionId().GetNamespaceId(); ns != "" {
			d["namespace"] = ns
		}
	}
	if db := req.GetDatabaseId(); db != "" {
		d["database"] = db
	}
	dsReadOptionDetails(req.GetReadOptions(), d)
	if pm := req.GetPropertyMask().GetPaths(); len(pm) > 0 {
		d["property_mask"] = strings.Join(pm, ", ")
	}
	return d
}

// DSQueryDetails extracts panel details from a RunQueryRequest.
func DSQueryDetails(req *datastorepb.RunQueryRequest) map[string]any {
	if req == nil {
		return nil
	}
	if gql, ok := req.QueryType.(*datastorepb.RunQueryRequest_GqlQuery); ok && gql.GqlQuery != nil {
		d := map[string]any{"gql": gql.GqlQuery.QueryString}
		if ns := req.GetPartitionId().GetNamespaceId(); ns != "" {
			d["namespace"] = ns
		}
		if db := req.GetDatabaseId(); db != "" {
			d["database"] = db
		}
		dsReadOptionDetails(req.GetReadOptions(), d)
		return d
	}
	qt, ok := req.QueryType.(*datastorepb.RunQueryRequest_Query)
	if !ok || qt.Query == nil {
		return nil
	}
	q := qt.Query
	d := map[string]any{}
	if len(q.Kind) > 0 {
		kinds := make([]string, len(q.Kind))
		for i, k := range q.Kind {
			kinds[i] = k.Name
		}
		d["kind"] = strings.Join(kinds, ", ")
	}
	if q.Filter != nil {
		d["filter"] = filterSummary(q.Filter)
	}
	if len(q.Order) > 0 {
		orders := make([]string, 0, len(q.Order))
		for _, o := range q.Order {
			s := o.Property.Name
			if o.Direction == datastorepb.PropertyOrder_DESCENDING {
				s += " desc"
			}
			orders = append(orders, s)
		}
		d["order_by"] = strings.Join(orders, ", ")
	}
	if q.Limit != nil {
		d["limit"] = q.Limit.Value
	}
	if q.Offset > 0 {
		d["offset"] = q.Offset
	}
	if len(q.StartCursor) > 0 {
		d["start_cursor"] = true
	}
	if isKeysOnly(q) {
		d["keys_only"] = true
	} else if proj := projectionFields(q); len(proj) > 0 {
		d["projection"] = strings.Join(proj, ", ")
	}
	if len(q.DistinctOn) > 0 {
		names := make([]string, len(q.DistinctOn))
		for i, ref := range q.DistinctOn {
			names[i] = ref.GetName()
		}
		d["distinct_on"] = strings.Join(names, ", ")
	}
	if ns := req.GetPartitionId().GetNamespaceId(); ns != "" {
		d["namespace"] = ns
	}
	if db := req.GetDatabaseId(); db != "" {
		d["database"] = db
	}
	dsReadOptionDetails(req.GetReadOptions(), d)
	if eo := req.GetExplainOptions(); eo != nil {
		if eo.Analyze {
			d["analyze"] = "true"
		} else {
			d["analyze"] = "plan_only"
		}
	}
	return d
}

// DSAggregationQueryDetails extracts panel details from a RunAggregationQueryRequest.
func DSAggregationQueryDetails(req *datastorepb.RunAggregationQueryRequest) map[string]any {
	var d map[string]any
	if aq, ok := req.QueryType.(*datastorepb.RunAggregationQueryRequest_AggregationQuery); ok {
		ag := aq.AggregationQuery
		if nq, ok2 := ag.QueryType.(*datastorepb.AggregationQuery_NestedQuery); ok2 && nq.NestedQuery != nil {
			// Borrow query detail extraction by constructing a minimal RunQueryRequest.
			fakeReq := &datastorepb.RunQueryRequest{
				ProjectId:   req.ProjectId,
				DatabaseId:  req.DatabaseId,
				PartitionId: req.PartitionId,
				ReadOptions: req.ReadOptions,
				QueryType:   &datastorepb.RunQueryRequest_Query{Query: nq.NestedQuery},
			}
			d = DSQueryDetails(fakeReq)
		}
		if d == nil {
			d = map[string]any{}
		}
		var ops []string
		for _, a := range ag.Aggregations {
			var op string
			switch aop := a.Operator.(type) {
			case *datastorepb.AggregationQuery_Aggregation_Count_:
				op = "count"
			case *datastorepb.AggregationQuery_Aggregation_Sum_:
				op = "sum(" + aop.Sum.GetProperty().GetName() + ")"
			case *datastorepb.AggregationQuery_Aggregation_Avg_:
				op = "avg(" + aop.Avg.GetProperty().GetName() + ")"
			}
			if a.Alias != "" {
				op += " as " + a.Alias
			}
			if op != "" {
				ops = append(ops, op)
			}
		}
		if len(ops) > 0 {
			d["aggregations"] = strings.Join(ops, ", ")
			d["aggregation_count"] = len(ag.Aggregations)
		}
	}
	if d == nil {
		d = map[string]any{}
	}
	return d
}

// DSMutationDetails extracts panel details from a CommitRequest.
func DSMutationDetails(req *datastorepb.CommitRequest) map[string]any {
	mutations := req.GetMutations()
	d := map[string]any{"mutations": len(mutations)}
	var inserts, updates, upserts, deletes, transforms, propertyMasks int
	kindSet := map[string]struct{}{}
	transformOps := map[string]struct{}{}
	var paths []string

	for _, m := range mutations {
		for _, pt := range m.PropertyTransforms {
			transforms++
			switch pt.TransformType.(type) {
			case *datastorepb.PropertyTransform_SetToServerValue:
				transformOps["set_server_value"] = struct{}{}
			case *datastorepb.PropertyTransform_Increment:
				transformOps["increment"] = struct{}{}
			case *datastorepb.PropertyTransform_Maximum:
				transformOps["maximum"] = struct{}{}
			case *datastorepb.PropertyTransform_Minimum:
				transformOps["minimum"] = struct{}{}
			case *datastorepb.PropertyTransform_AppendMissingElements:
				transformOps["append_missing"] = struct{}{}
			case *datastorepb.PropertyTransform_RemoveAllFromArray:
				transformOps["remove_from_array"] = struct{}{}
			}
		}
		if m.GetPropertyMask() != nil {
			propertyMasks++
		}
		switch op := m.Operation.(type) {
		case *datastorepb.Mutation_Insert:
			inserts++
			if k := entityKind(op.Insert); k != "" {
				kindSet[k] = struct{}{}
			}
			if len(paths) < 5 {
				if seg := entityLastSeg(op.Insert.GetKey()); seg != "" {
					paths = append(paths, seg)
				}
			}
		case *datastorepb.Mutation_Update:
			updates++
			if k := entityKind(op.Update); k != "" {
				kindSet[k] = struct{}{}
			}
			if len(paths) < 5 {
				if seg := entityLastSeg(op.Update.GetKey()); seg != "" {
					paths = append(paths, seg)
				}
			}
		case *datastorepb.Mutation_Upsert:
			upserts++
			if k := entityKind(op.Upsert); k != "" {
				kindSet[k] = struct{}{}
			}
			if len(paths) < 5 {
				if seg := entityLastSeg(op.Upsert.GetKey()); seg != "" {
					paths = append(paths, seg)
				}
			}
		case *datastorepb.Mutation_Delete:
			deletes++
			if dp := op.Delete.GetPath(); len(dp) > 0 {
				last := dp[len(dp)-1]
				if last.GetKind() != "" {
					kindSet[last.GetKind()] = struct{}{}
				}
				if len(paths) < 5 {
					switch id := last.GetIdType().(type) {
					case *datastorepb.Key_PathElement_Name:
						paths = append(paths, id.Name)
					case *datastorepb.Key_PathElement_Id:
						paths = append(paths, fmt.Sprintf("%d", id.Id))
					}
				}
			}
		}
	}

	if inserts > 0 {
		d["inserts"] = inserts
	}
	if updates > 0 {
		d["updates"] = updates
	}
	if upserts > 0 {
		d["upserts"] = upserts
	}
	if deletes > 0 {
		d["deletes"] = deletes
	}
	if len(kindSet) > 0 {
		d["kinds"] = sortedKeys(kindSet)
	}
	if len(paths) > 0 {
		d["paths"] = paths
	}
	if transforms > 0 {
		d["transforms"] = transforms
		if len(transformOps) > 0 {
			d["transform_ops"] = sortedKeys(transformOps)
		}
	}
	if propertyMasks > 0 {
		d["property_masks"] = propertyMasks
	}
	switch req.GetMode() {
	case datastorepb.CommitRequest_NON_TRANSACTIONAL:
		d["mode"] = "NON_TRANSACTIONAL"
	case datastorepb.CommitRequest_TRANSACTIONAL:
		d["mode"] = "TRANSACTIONAL"
	}
	if db := req.GetDatabaseId(); db != "" {
		d["database"] = db
	}
	if txn := req.GetTransaction(); len(txn) > 0 {
		d["txn_id_prefix"] = hexPrefix(txn)
	}
	return d
}

// DSBeginTxDetails extracts panel details from a BeginTransactionRequest.
func DSBeginTxDetails(req *datastorepb.BeginTransactionRequest) map[string]any {
	opts := req.GetTransactionOptions()
	d := map[string]any{}
	if opts != nil {
		switch m := opts.Mode.(type) {
		case *datastorepb.TransactionOptions_ReadOnly_:
			d["mode"] = "read_only"
			if rt := m.ReadOnly.GetReadTime(); rt != nil {
				d["read_time_at"] = rt.AsTime().UTC().Format("2006-01-02T15:04:05Z")
			}
		case *datastorepb.TransactionOptions_ReadWrite_:
			d["mode"] = "read_write"
			if pt := m.ReadWrite.GetPreviousTransaction(); len(pt) > 0 {
				d["previous_transaction"] = hexPrefix(pt)
			}
		}
	}
	if db := req.GetDatabaseId(); db != "" {
		d["database"] = db
	}
	return d
}

// DSAllocateIdsDetails extracts panel details from an AllocateIdsRequest.
func DSAllocateIdsDetails(req *datastorepb.AllocateIdsRequest) map[string]any {
	keys := req.GetKeys()
	d := map[string]any{"keys": len(keys)}
	if kinds := dsKeyKinds(keys); len(kinds) > 0 {
		d["kinds"] = kinds
	}
	// Namespace lives in each key's PartitionId.
	if len(keys) > 0 {
		if ns := keys[0].GetPartitionId().GetNamespaceId(); ns != "" {
			d["namespace"] = ns
		}
	}
	if db := req.GetDatabaseId(); db != "" {
		d["database"] = db
	}
	// Collect distinct parent path summaries (ancestors of incomplete keys).
	parentSet := map[string]struct{}{}
	for _, k := range keys {
		path := k.GetPath()
		if len(path) <= 1 {
			continue
		}
		segs := make([]string, 0, len(path)-1)
		for _, el := range path[:len(path)-1] {
			switch id := el.GetIdType().(type) {
			case *datastorepb.Key_PathElement_Name:
				segs = append(segs, el.Kind+"/"+id.Name)
			case *datastorepb.Key_PathElement_Id:
				segs = append(segs, fmt.Sprintf("%s/%d", el.Kind, id.Id))
			}
		}
		if len(segs) > 0 {
			parentSet[strings.Join(segs, "/")] = struct{}{}
		}
	}
	if len(parentSet) > 0 {
		parents := sortedKeys(parentSet)
		if len(parents) > 5 {
			parents = parents[:5]
		}
		d["parent_paths"] = parents
	}
	return d
}

// DSReserveIdsDetails extracts panel details from a ReserveIdsRequest.
func DSReserveIdsDetails(req *datastorepb.ReserveIdsRequest) map[string]any {
	keys := req.GetKeys()
	d := map[string]any{"keys": len(keys)}
	if kinds := dsKeyKinds(keys); len(kinds) > 0 {
		d["kinds"] = kinds
	}
	if db := req.GetDatabaseId(); db != "" {
		d["database"] = db
	}
	return d
}

// DSRollbackDetails extracts panel details from a RollbackRequest.
func DSRollbackDetails(req *datastorepb.RollbackRequest) map[string]any {
	d := map[string]any{}
	if txn := req.GetTransaction(); len(txn) > 0 {
		d["transaction"] = true
		d["txn_id_prefix"] = hexPrefix(txn)
	}
	if db := req.GetDatabaseId(); db != "" {
		d["database"] = db
	}
	return d
}

// ---- response builders ----

// DSLookupResponseDetails extracts response-side panel fields from a LookupResponse.
func DSLookupResponseDetails(resp *datastorepb.LookupResponse, elapsed time.Duration) map[string]any {
	d := map[string]any{
		"found":      len(resp.Found),
		"missing":    len(resp.Missing),
		"elapsed_ms": elapsed.Milliseconds(),
	}
	if len(resp.Deferred) > 0 {
		d["deferred"] = len(resp.Deferred)
	}
	if len(resp.Transaction) > 0 {
		d["txn_returned"] = true
	}
	if resp.ReadTime != nil {
		d["read_time"] = resp.ReadTime.AsTime().UTC().Format("2006-01-02T15:04:05Z")
	}
	return d
}

// DSCommitResponseDetails extracts response-side panel fields from a CommitResponse.
func DSCommitResponseDetails(resp *datastorepb.CommitResponse, elapsed time.Duration) map[string]any {
	d := map[string]any{
		"results":    len(resp.MutationResults),
		"elapsed_ms": elapsed.Milliseconds(),
	}
	var conflicts, allocated int
	var allocPaths []string
	for _, mr := range resp.MutationResults {
		if mr.ConflictDetected {
			conflicts++
		}
		if mr.Key != nil {
			allocated++
			if len(allocPaths) < 5 {
				_, _, _, _, _, path := keyComponents(mr.Key)
				if path != "" {
					allocPaths = append(allocPaths, path)
				}
			}
		}
	}
	if conflicts > 0 {
		d["conflicts"] = conflicts
	}
	if allocated > 0 {
		d["allocated_keys"] = allocated
		if len(allocPaths) > 0 {
			d["allocated_paths"] = allocPaths
		}
	}
	if resp.CommitTime != nil {
		d["commit_time"] = resp.CommitTime.AsTime().UTC().Format("2006-01-02T15:04:05Z")
	}
	return d
}

// DSQueryResponseDetails extracts response-side panel fields from a RunQueryResponse.
func DSQueryResponseDetails(resp *datastorepb.RunQueryResponse, elapsed time.Duration) map[string]any {
	batch := resp.GetBatch()
	d := map[string]any{
		"results":      len(batch.GetEntityResults()),
		"more_results": moreResultsName(batch.GetMoreResults()),
		"elapsed_ms":   elapsed.Milliseconds(),
	}
	if batch.GetSkippedResults() > 0 {
		d["skipped_results"] = batch.GetSkippedResults()
	}
	if len(batch.GetEndCursor()) > 0 {
		d["end_cursor"] = true
	}
	if batch.GetReadTime() != nil {
		d["read_time"] = batch.GetReadTime().AsTime().UTC().Format("2006-01-02T15:04:05Z")
	}
	if len(resp.Transaction) > 0 {
		d["txn_returned"] = true
	}
	if em := resp.ExplainMetrics; em != nil {
		if es := em.ExecutionStats; es != nil {
			d["scanned"] = es.ResultsReturned
			if es.ExecutionDuration != nil {
				d["execution_duration_ms"] = es.ExecutionDuration.AsDuration().Milliseconds()
			}
		}
	}
	return d
}

// DSAggregationResponseDetails extracts response-side panel fields from a RunAggregationQueryResponse.
func DSAggregationResponseDetails(resp *datastorepb.RunAggregationQueryResponse, elapsed time.Duration) map[string]any {
	d := map[string]any{"elapsed_ms": elapsed.Milliseconds()}
	batch := resp.GetBatch()
	if batch != nil && len(batch.AggregationResults) > 0 {
		for alias, v := range batch.AggregationResults[0].AggregateProperties {
			d["agg_"+alias] = valueSummary(v)
		}
	}
	if batch != nil && batch.ReadTime != nil {
		d["read_time"] = batch.ReadTime.AsTime().UTC().Format("2006-01-02T15:04:05Z")
	}
	return d
}

// DSBeginTxResponseDetails extracts response-side panel fields from a BeginTransactionResponse.
func DSBeginTxResponseDetails(resp *datastorepb.BeginTransactionResponse) map[string]any {
	d := map[string]any{}
	if len(resp.Transaction) > 0 {
		d["txn_returned"] = true
		d["txn_id_prefix"] = hexPrefix(resp.Transaction)
	}
	return d
}

// DSAllocateIdsResponseDetails extracts response-side panel fields from an AllocateIdsResponse.
func DSAllocateIdsResponseDetails(resp *datastorepb.AllocateIdsResponse) map[string]any {
	keys := resp.GetKeys()
	d := map[string]any{"allocated": len(keys)}
	var ids []string
	for i, k := range keys {
		if i >= 5 {
			break
		}
		path := k.GetPath()
		if len(path) == 0 {
			continue
		}
		last := path[len(path)-1]
		switch id := last.GetIdType().(type) {
		case *datastorepb.Key_PathElement_Name:
			ids = append(ids, id.Name)
		case *datastorepb.Key_PathElement_Id:
			ids = append(ids, fmt.Sprintf("%d", id.Id))
		}
	}
	if len(ids) > 0 {
		d["allocated_ids"] = ids
	}
	return d
}

// ---- filter summary helpers ----

func filterSummary(f *datastorepb.Filter) string {
	if f == nil {
		return ""
	}
	switch ft := f.FilterType.(type) {
	case *datastorepb.Filter_PropertyFilter:
		return propFilterSummary(ft.PropertyFilter)
	case *datastorepb.Filter_CompositeFilter:
		cf := ft.CompositeFilter
		parts := make([]string, 0, len(cf.Filters))
		for _, sub := range cf.Filters {
			if s := filterSummary(sub); s != "" {
				parts = append(parts, s)
			}
		}
		op := " AND "
		if cf.Op == datastorepb.CompositeFilter_OR {
			op = " OR "
		}
		joined := strings.Join(parts, op)
		if len(parts) > 1 {
			return "(" + joined + ")"
		}
		return joined
	}
	return ""
}

func propFilterSummary(pf *datastorepb.PropertyFilter) string {
	prop := pf.Property.GetName()
	switch pf.Op {
	case datastorepb.PropertyFilter_EQUAL:
		return prop + " = " + valueSummary(pf.Value)
	case datastorepb.PropertyFilter_NOT_EQUAL:
		return prop + " != " + valueSummary(pf.Value)
	case datastorepb.PropertyFilter_LESS_THAN:
		return prop + " < " + valueSummary(pf.Value)
	case datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL:
		return prop + " <= " + valueSummary(pf.Value)
	case datastorepb.PropertyFilter_GREATER_THAN:
		return prop + " > " + valueSummary(pf.Value)
	case datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
		return prop + " >= " + valueSummary(pf.Value)
	case datastorepb.PropertyFilter_IN:
		return prop + " IN " + valueSummary(pf.Value)
	case datastorepb.PropertyFilter_NOT_IN:
		return prop + " NOT IN " + valueSummary(pf.Value)
	case datastorepb.PropertyFilter_HAS_ANCESTOR:
		return prop + " HAS_ANCESTOR " + valueSummary(pf.Value)
	}
	return prop
}

func valueSummary(v *datastorepb.Value) string {
	if v == nil {
		return "null"
	}
	switch vt := v.GetValueType().(type) {
	case *datastorepb.Value_StringValue:
		s := vt.StringValue
		if len(s) > 40 {
			s = s[:37] + "..."
		}
		return fmt.Sprintf("%q", s)
	case *datastorepb.Value_IntegerValue:
		return fmt.Sprintf("%d", vt.IntegerValue)
	case *datastorepb.Value_DoubleValue:
		return fmt.Sprintf("%g", vt.DoubleValue)
	case *datastorepb.Value_BooleanValue:
		if vt.BooleanValue {
			return "true"
		}
		return "false"
	case *datastorepb.Value_NullValue:
		return "null"
	case *datastorepb.Value_TimestampValue:
		return vt.TimestampValue.AsTime().UTC().Format("2006-01-02T15:04:05Z")
	case *datastorepb.Value_KeyValue:
		_, _, _, _, _, path := keyComponents(vt.KeyValue)
		if len(path) > 40 {
			path = "..." + path[len(path)-37:]
		}
		return path
	case *datastorepb.Value_BlobValue:
		return fmt.Sprintf("<bytes:%d>", len(vt.BlobValue))
	case *datastorepb.Value_ArrayValue:
		if av := vt.ArrayValue; av != nil {
			elems := make([]string, 0, len(av.Values))
			for i, elem := range av.Values {
				if i >= 5 {
					elems = append(elems, fmt.Sprintf("+%d more", len(av.Values)-5))
					break
				}
				elems = append(elems, valueSummary(elem))
			}
			return "[" + strings.Join(elems, ", ") + "]"
		}
		return "[]"
	}
	return "?"
}

// ---- internal helpers ----

// dsReadOptionDetails appends read-option fields to d.
func dsReadOptionDetails(ro *datastorepb.ReadOptions, d map[string]any) {
	if ro == nil {
		return
	}
	if rt := ro.GetReadTime(); rt != nil {
		d["read_time_at"] = rt.AsTime().UTC().Format("2006-01-02T15:04:05Z")
		return
	}
	if tx := ro.GetTransaction(); len(tx) > 0 {
		d["request_txn"] = hexPrefix(tx)
		return
	}
	if ro.GetNewTransaction() != nil {
		d["new_transaction"] = true
	}
}

func moreResultsName(mr datastorepb.QueryResultBatch_MoreResultsType) string {
	switch mr {
	case datastorepb.QueryResultBatch_NO_MORE_RESULTS:
		return "no_more"
	case datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT:
		return "more_after_limit"
	case datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_CURSOR:
		return "more_after_cursor"
	case datastorepb.QueryResultBatch_NOT_FINISHED:
		return "not_finished"
	default:
		return "unknown"
	}
}

// hexPrefix returns the first 8 hex chars of b (4 bytes).
func hexPrefix(b []byte) string {
	n := 4
	if len(b) < n {
		n = len(b)
	}
	return hex.EncodeToString(b[:n])
}

// dsKeyPaths returns "Kind/id" summaries for up to limit keys.
func dsKeyPaths(keys []*datastorepb.Key, limit int) []string {
	var out []string
	for _, k := range keys {
		if len(out) >= limit {
			break
		}
		path := k.GetPath()
		if len(path) == 0 {
			continue
		}
		last := path[len(path)-1]
		switch id := last.GetIdType().(type) {
		case *datastorepb.Key_PathElement_Name:
			out = append(out, last.Kind+"/"+id.Name)
		case *datastorepb.Key_PathElement_Id:
			out = append(out, fmt.Sprintf("%s/%d", last.Kind, id.Id))
		default:
			out = append(out, last.Kind+"/<auto>")
		}
	}
	return out
}

// entityLastSeg returns the entity name or id from a Key's last path element.
func entityLastSeg(k *datastorepb.Key) string {
	path := k.GetPath()
	if len(path) == 0 {
		return ""
	}
	last := path[len(path)-1]
	switch id := last.GetIdType().(type) {
	case *datastorepb.Key_PathElement_Name:
		return id.Name
	case *datastorepb.Key_PathElement_Id:
		return fmt.Sprintf("%d", id.Id)
	}
	return "<auto>"
}

// dsKeyKinds returns a sorted, deduplicated list of kinds from a key slice.
func dsKeyKinds(keys []*datastorepb.Key) []string {
	set := map[string]struct{}{}
	for _, k := range keys {
		if path := k.GetPath(); len(path) > 0 {
			if kind := path[len(path)-1].GetKind(); kind != "" {
				set[kind] = struct{}{}
			}
		}
	}
	return sortedKeys(set)
}

func entityKind(e *datastorepb.Entity) string {
	if e == nil {
		return ""
	}
	path := e.GetKey().GetPath()
	if len(path) == 0 {
		return ""
	}
	return path[len(path)-1].GetKind()
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
