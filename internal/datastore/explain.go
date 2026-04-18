package datastore

import (
	"fmt"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// buildRunQueryPlan builds a PlanSummary for a RunQuery request.
// For RunQuery, the implicit ordering is always by __name__, so we return
// '(__name__ ASC)' with query_scope 'Collection group'.
func buildRunQueryPlan(q *datastorepb.Query) *datastorepb.PlanSummary {
	props := "(__name__ ASC)"
	if len(q.GetOrder()) > 0 {
		var parts string
		for i, ord := range q.Order {
			if i > 0 {
				parts += ", "
			}
			dir := "ASC"
			if ord.Direction == datastorepb.PropertyOrder_DESCENDING {
				dir = "DESC"
			}
			parts += ord.Property.GetName() + " " + dir
		}
		props = "(" + parts + ", __name__ ASC)"
	}
	entry, _ := structpb.NewStruct(map[string]interface{}{
		"query_scope": "Collection group",
		"properties":  props,
	})
	return &datastorepb.PlanSummary{IndexesUsed: []*structpb.Struct{entry}}
}

// buildAggregationQueryPlan builds a PlanSummary for a RunAggregationQuery request.
// Derives index from aggregation field names; scope is 'Includes ancestors' if there
// is a HAS_ANCESTOR filter in the nested query.
func buildAggregationQueryPlan(ag *datastorepb.AggregationQuery, nestedQuery *datastorepb.Query) *datastorepb.PlanSummary {
	// Derive property list from aggregation operators.
	props := "(__name__ ASC)"
	if len(ag.GetAggregations()) > 0 {
		agg := ag.Aggregations[0]
		var field string
		switch op := agg.Operator.(type) {
		case *datastorepb.AggregationQuery_Aggregation_Sum_:
			field = op.Sum.GetProperty().GetName()
		case *datastorepb.AggregationQuery_Aggregation_Avg_:
			field = op.Avg.GetProperty().GetName()
		}
		if field != "" {
			props = fmt.Sprintf("(%s ASC, __name__ ASC)", field)
		}
	}

	scope := "Collection group"
	if hasAncestorFilter(nestedQuery.GetFilter()) {
		scope = "Includes ancestors"
	}

	entry, _ := structpb.NewStruct(map[string]interface{}{
		"query_scope": scope,
		"properties":  props,
	})
	return &datastorepb.PlanSummary{IndexesUsed: []*structpb.Struct{entry}}
}

func hasAncestorFilter(f *datastorepb.Filter) bool {
	if f == nil {
		return false
	}
	switch ft := f.FilterType.(type) {
	case *datastorepb.Filter_PropertyFilter:
		return ft.PropertyFilter.Op == datastorepb.PropertyFilter_HAS_ANCESTOR
	case *datastorepb.Filter_CompositeFilter:
		for _, sub := range ft.CompositeFilter.Filters {
			if hasAncestorFilter(sub) {
				return true
			}
		}
	}
	return false
}

// buildRunQueryExecutionStats builds execution stats for an analyzed RunQuery.
func buildRunQueryExecutionStats(n int, elapsed time.Duration) *datastorepb.ExecutionStats {
	ns := int32(elapsed.Nanoseconds() % 1e9)
	secs := int64(elapsed.Seconds())
	nStr := fmt.Sprintf("%d", n)
	debugStats, _ := structpb.NewStruct(map[string]interface{}{
		"documents_scanned":    nStr,
		"index_entries_scanned": nStr,
		"billing_details": map[string]interface{}{
			"documents_billable":    nStr,
			"index_entries_billable": "0",
			"min_query_cost":        "0",
			"small_ops":             "0",
		},
	})
	return &datastorepb.ExecutionStats{
		ResultsReturned:   int64(n),
		ReadOperations:    int64(n),
		ExecutionDuration: &durationpb.Duration{Seconds: secs, Nanos: ns},
		DebugStats:        debugStats,
	}
}

// buildAggregationQueryExecutionStats builds execution stats for an analyzed aggregation query.
// For aggregation queries: resultsReturned=1, readOperations=1, documents_scanned=0,
// index_entries_scanned=rowCount (rows scanned to compute aggregation).
func buildAggregationQueryExecutionStats(rowCount int, elapsed time.Duration) *datastorepb.ExecutionStats {
	ns := int32(elapsed.Nanoseconds() % 1e9)
	secs := int64(elapsed.Seconds())
	rowStr := fmt.Sprintf("%d", rowCount)
	debugStats, _ := structpb.NewStruct(map[string]interface{}{
		"documents_scanned":    "0",
		"index_entries_scanned": rowStr,
		"billing_details": map[string]interface{}{
			"documents_billable":    "0",
			"index_entries_billable": rowStr,
			"min_query_cost":        "0",
			"small_ops":             "0",
		},
	})
	return &datastorepb.ExecutionStats{
		ResultsReturned:   1,
		ReadOperations:    1,
		ExecutionDuration: &durationpb.Duration{Seconds: secs, Nanos: ns},
		DebugStats:        debugStats,
	}
}
