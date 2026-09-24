package datastore

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func buildPhysicalPlan(q *datastorepb.Query, index *storage.DsCompositeIndex, access, state string) *datastorepb.PlanSummary {
	var properties []string
	if index != nil {
		for _, property := range index.Properties {
			direction := "ASC"
			if property.Desc {
				direction = "DESC"
			}
			properties = append(properties, property.Name+" "+direction)
		}
	} else if strings.HasPrefix(access, "builtin:") && access != "builtin:union" {
		property := strings.TrimPrefix(access, "builtin:")
		direction := "ASC"
		_, reverse, _ := builtinQueryAccess(q)
		if reverse {
			direction = "DESC"
		}
		properties = append(properties, property+" "+direction)
	} else {
		for _, order := range q.GetOrder() {
			direction := "ASC"
			if order.Direction == datastorepb.PropertyOrder_DESCENDING {
				direction = "DESC"
			}
			properties = append(properties, order.Property.GetName()+" "+direction)
		}
	}
	if len(properties) == 0 || !strings.HasPrefix(properties[len(properties)-1], "__key__ ") {
		tie := "__key__ ASC"
		if strings.HasPrefix(access, "builtin:") && access != "builtin:union" {
			if _, reverse, _ := builtinQueryAccess(q); reverse {
				tie = "__key__ DESC"
			}
		}
		properties = append(properties, tie)
	}
	scope := "Kind"
	if len(q.GetKind()) == 0 {
		scope = "Partition"
	}
	if hasAncestorFilter(q.GetFilter()) {
		scope = "Ancestor"
	}
	fields := map[string]*structpb.Value{
		"access_path": structpb.NewStringValue(access),
		"state":       structpb.NewStringValue(state),
		"query_scope": structpb.NewStringValue(scope),
		"properties":  structpb.NewStringValue("(" + strings.Join(properties, ", ") + ")"),
	}
	if index != nil {
		fields["index_id"] = structpb.NewStringValue(index.ID)
	}
	return &datastorepb.PlanSummary{IndexesUsed: []*structpb.Struct{{Fields: fields}}}
}

func buildCandidateORPlan(q *datastorepb.Query, branches []builtinUnionBranch) *datastorepb.PlanSummary {
	plan := buildPhysicalPlan(q, nil, "disk_sort", "READY")
	for _, branch := range branches {
		part := buildPhysicalPlan(q, nil, "builtin:"+branch.property, "READY")
		part.IndexesUsed[0].Fields["operation"] = structpb.NewStringValue("candidate_probe")
		plan.IndexesUsed = append(plan.IndexesUsed, part.IndexesUsed...)
	}
	return plan
}

func buildRunQueryPlan(q *datastorepb.Query) *datastorepb.PlanSummary {
	if property, _, ok := builtinQueryAccess(q); ok {
		return buildPhysicalPlan(q, nil, "builtin:"+property, "READY")
	}
	return buildPhysicalPlan(q, nil, "disk_sort", "READY")
}

func buildEqualityUnionPlan(q *datastorepb.Query, branches []*datastorepb.PropertyFilter) *datastorepb.PlanSummary {
	plan := &datastorepb.PlanSummary{}
	for _, branch := range branches {
		index := buildPhysicalPlan(q, nil, "builtin:"+branch.Property.GetName(), "READY").IndexesUsed[0]
		index.Fields["access_path"] = structpb.NewStringValue("builtin:union")
		plan.IndexesUsed = append(plan.IndexesUsed, index)
	}
	if !isKeysOnly(q) {
		plan.IndexesUsed = append(plan.IndexesUsed, buildPhysicalPlan(q, nil, "builtin:__key__", "READY").IndexesUsed...)
	}
	return plan
}

func buildCompositeRunQueryPlan(q *datastorepb.Query, indexID string) *datastorepb.PlanSummary {
	if strings.HasPrefix(indexID, "builtin:") {
		return buildPhysicalPlan(q, nil, indexID, "READY")
	}
	if indexID == "fallback" {
		return buildPhysicalPlan(q, nil, "disk_sort", "READY")
	}
	index, _ := queryIndexDefinition(q, hasAncestorFilter(q.Filter))
	index.ID = indexID
	return buildPhysicalPlan(q, &index, "composite", "READY")
}

// explainQueryPlan shares index selection with execution but never creates/builds an index.
func (g *GRPCServer) explainQueryPlan(ctx context.Context, req *datastorepb.RunQueryRequest, q *datastorepb.Query, database, namespace string, cursor *storage.CursorPayload, condition *compiledCondition) (*datastorepb.PlanSummary, error) {
	kind := ""
	if len(q.Kind) > 0 {
		kind = q.Kind[0].Name
	}
	if aggregationEntries(ctx) || hiddenProjectionOrder(q) || !condition.accessPlanningFits(queryOptimizerAllowance(ctx)) && (cursor == nil || cursor.I == "fallback") || condition.needsEntityWitnesses() || kind == "" || isMetadataKind(kind) {
		access := "disk_sort"
		if isMetadataKind(kind) {
			access = "catalog"
		}
		return buildPhysicalPlan(q, nil, access, "READY"), nil
	}
	if branches := equalityUnionAccess(q, condition, queryOptimizerAllowance(ctx), cursor); len(branches) > 0 {
		return buildEqualityUnionPlan(q, branches), nil
	}
	var end *storage.CursorPayload
	if len(q.EndCursor) > 0 {
		decoded, ok := decodeCursorFull(q.EndCursor)
		if ok {
			end = &decoded
		}
	}
	if branches := candidateORAccess(q, condition, queryOptimizerAllowance(ctx), cursor, end); len(branches) > 0 {
		plan := buildCandidateORPlan(q, branches)
		for _, index := range plan.IndexesUsed {
			index.Fields["state"] = structpb.NewStringValue("ADAPTIVE")
		}
		return plan, nil
	}
	if _, union := builtinUnionAccess(q); union {
		if len(builtinAccessOrder(q.Order)) > 1 || len(projectionFields(q)) > 0 {
			return buildPhysicalPlan(q, nil, "disk_sort", "READY"), nil
		}
		property := "__key__"
		if len(q.Order) > 0 {
			property = q.Order[0].Property.GetName()
		}
		return buildPhysicalPlan(q, nil, "builtin:"+property, "READY"), nil
	}
	pinned := ""
	if cursor != nil {
		if cursor.I == "fallback" {
			return buildPhysicalPlan(q, nil, "disk_sort", "READY"), nil
		}
		if !strings.HasPrefix(cursor.I, "builtin:") {
			pinned = cursor.I
		}
	}
	if property, _, ok := builtinQueryAccess(q); ok && pinned == "" {
		return buildPhysicalPlan(q, nil, "builtin:"+property, "READY"), nil
	}
	if !compositeOrderSupported(q) {
		return buildPhysicalPlan(q, nil, "disk_sort", "READY"), nil
	}
	ancestor := extractAncestorPath(q.Filter, req.ProjectId, database, namespace) != ""
	index, err := g.indexes.selectQueryIndex(ctx, req.ProjectId, q, ancestor, pinned)
	if err != nil {
		return nil, err
	}
	if index == nil || !storage.CompositeBoundsSupported(*index, q.Filter) {
		return buildPhysicalPlan(q, nil, "disk_sort", "READY"), nil
	}
	if readTime := req.ReadOptions.GetReadTime(); readTime != nil && index.ReadySince > readTime.AsTime().UnixNano() {
		return buildPhysicalPlan(q, nil, "disk_sort", "READY"), nil
	}
	state := index.State
	if state == "" {
		state = "REQUIRED"
	}
	return buildPhysicalPlan(q, index, "composite", state), nil
}

func hasAncestorFilter(filter *datastorepb.Filter) bool {
	if property := filter.GetPropertyFilter(); property != nil {
		return property.Op == datastorepb.PropertyFilter_HAS_ANCESTOR
	}
	for _, child := range filter.GetCompositeFilter().GetFilters() {
		if hasAncestorFilter(child) {
			return true
		}
	}
	return false
}

func buildRunQueryExecutionStats(n int, elapsed time.Duration) *datastorepb.ExecutionStats {
	return buildRunQueryExecutionStatsWithScans(n, 0, 0, elapsed)
}

// Billing is not emulated. Debug counters describe actual Hearthstore work.
func buildRunQueryExecutionStatsWithScans(n, documentsScanned int, indexEntriesScanned int64, elapsed time.Duration) *datastorepb.ExecutionStats {
	return &datastorepb.ExecutionStats{
		ResultsReturned:   int64(n),
		ExecutionDuration: durationpb.New(elapsed),
		DebugStats: &structpb.Struct{Fields: map[string]*structpb.Value{
			"documents_scanned":     structpb.NewStringValue(fmt.Sprint(documentsScanned)),
			"index_entries_scanned": structpb.NewStringValue(fmt.Sprint(indexEntriesScanned)),
			"billing_supported":     structpb.NewBoolValue(false),
		}},
	}
}

// addQueryWorkStats runs once at the public RPC boundary. Aggregation pages do
// not sum cumulative snapshots (which would double-count earlier pages).
func addQueryWorkStats(metrics *datastorepb.ExplainMetrics, work *storage.QueryWork) {
	stats := metrics.GetExecutionStats()
	if stats == nil {
		return
	} // Plan-only explain does not execute work.
	if stats.DebugStats == nil {
		stats.DebugStats = &structpb.Struct{}
	}
	if stats.DebugStats.Fields == nil {
		stats.DebugStats.Fields = make(map[string]*structpb.Value)
	}
	counts := work.Snapshot()
	for kind, name := range map[storage.WorkKind]string{
		storage.WorkAttempts:          "work_attempts",
		storage.WorkComparisons:       "work_value_comparisons",
		storage.WorkIndexEntries:      "work_entries_visited",
		storage.WorkDecodedBytes:      "work_decoded_bytes",
		storage.WorkProjectionTuples:  "work_projection_tuples",
		storage.WorkScratchReadBytes:  "work_scratch_read_bytes",
		storage.WorkScratchWriteBytes: "work_scratch_write_bytes",
		storage.WorkYields:            "work_yields",
	} {
		stats.DebugStats.Fields[name] = structpb.NewStringValue(strconv.FormatUint(counts[kind], 10))
	}
}
