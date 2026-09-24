package storage

import (
	"context"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// QueryReadStats counts entity record reads, including rejected and prefetched rows.
// A query owns this collector and executes its storage iterators sequentially.
type QueryReadStats struct{ Documents int64 }
type queryReadStatsKey struct{}

// WithQueryReadStats starts a collector independent of any enclosing query.
func WithQueryReadStats(ctx context.Context) (context.Context, *QueryReadStats) {
	stats := &QueryReadStats{}
	return context.WithValue(ctx, queryReadStatsKey{}, stats), stats
}

func getDSQueryTxn(ctx context.Context, tx *Txn, project, database, namespace, path string) (dsRecord, *datastorepb.Entity, error) {
	if stats, ok := ctx.Value(queryReadStatsKey{}).(*QueryReadStats); ok {
		stats.Documents++
	}
	record, entity, err := getDSTxn(tx, project, database, namespace, path)
	QueryWorkFromContext(ctx).Charge(WorkDecodedBytes, uint64(len(record.Data)))
	return record, entity, err
}
