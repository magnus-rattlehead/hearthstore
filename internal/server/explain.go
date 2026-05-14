package server

import (
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// buildPlanSummary constructs a PlanSummary reflecting the effective ORDER BY fields.
// Mirrors what a Firestore server would return: one index entry describing the
// fields and directions the query would use.
func buildPlanSummary(effectiveOrders []*firestorepb.StructuredQuery_Order) *firestorepb.PlanSummary {
	var parts []string
	for _, ord := range effectiveOrders {
		dir := "ASC"
		if ord.Direction == firestorepb.StructuredQuery_DESCENDING {
			dir = "DESC"
		}
		parts = append(parts, ord.Field.GetFieldPath()+" "+dir)
	}
	props := "(__name__ ASC)"
	if len(parts) > 0 {
		props = "(" + strings.Join(parts, ", ") + ")"
	}
	entry, _ := structpb.NewStruct(map[string]interface{}{
		"query_scope": "Collection",
		"properties":  props,
	})
	return &firestorepb.PlanSummary{IndexesUsed: []*structpb.Struct{entry}}
}

// buildExplainMetrics constructs ExplainMetrics for analyze=true responses.
func buildExplainMetrics(plan *firestorepb.PlanSummary, n int, elapsed time.Duration) *firestorepb.ExplainMetrics {
	debugStats, _ := structpb.NewStruct(map[string]interface{}{
		"documents_scanned": float64(n),
	})
	// Firestore bills at least 1 read operation even when 0 docs match,
	// because the index scan itself is a read.
	readOps := int64(n)
	if readOps < 1 {
		readOps = 1
	}
	return &firestorepb.ExplainMetrics{
		PlanSummary: plan,
		ExecutionStats: &firestorepb.ExecutionStats{
			ResultsReturned:   int64(n),
			ReadOperations:    readOps,
			ExecutionDuration: durationpb.New(elapsed),
			DebugStats:        debugStats,
		},
	}
}
