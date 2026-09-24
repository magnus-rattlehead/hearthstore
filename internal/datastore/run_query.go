package datastore

import (
	"context"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

const (
	maxQueryResponseBytes = 4 << 20
)

// RunQuery executes a structured query.
func (g *GRPCServer) RunQuery(ctx context.Context, req *datastorepb.RunQueryRequest) (out *datastorepb.RunQueryResponse, resultErr error) {
	defer func() { resultErr = rpcError(resultErr) }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.store.CheckAvailable(); err != nil {
		return nil, err
	}
	ctx, work := storage.WithQueryWork(ctx, 0)
	release, err := g.acquireQuerySlot(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	response, err := g.runQuery(ctx, req)
	if err == nil {
		// Convert only at the public boundary. Predicates, ordering, and cursor
		// construction must still see timestamps, not integer filter operands.
		if response.GetBatch().GetEntityResultType() == datastorepb.EntityResult_PROJECTION {
			for _, row := range response.Batch.EntityResults {
				for name, value := range row.Entity.Properties {
					if err := work.Checkpoint(ctx); err != nil {
						return nil, err
					}
					if stamp := value.GetTimestampValue(); stamp != nil {
						converted := proto.Clone(value).(*datastorepb.Value)
						converted.ValueType = &datastorepb.Value_IntegerValue{IntegerValue: stamp.Seconds*1_000_000 + int64(stamp.Nanos)/1_000}
						row.Entity.Properties[name] = converted
					}
				}
			}
		}
		addQueryWorkStats(response.GetExplainMetrics(), work)
	}
	return response, err
}

func (g *GRPCServer) runQuery(ctx context.Context, req *datastorepb.RunQueryRequest) (*datastorepb.RunQueryResponse, error) {
	return g.runQueryWithSnapshot(ctx, req, nil)
}

// querySnapshot transfers the first page's live pin to its multi-page caller.
// The caller releases it after all pages, with no unpinned handoff interval.
type querySnapshot struct {
	time        time.Time
	release     func()
	fallback    *fallbackRows
	readStats   *storage.QueryReadStats
	fingerprint []byte
	aggregation *aggregationAccess
}

func (s *querySnapshot) close() error {
	err := s.fallback.close()
	s.fallback, s.readStats, s.fingerprint = nil, nil, nil
	s.aggregation = nil
	if s.release != nil {
		s.release()
		s.release = nil
	}
	return err
}

func (g *GRPCServer) runQueryWithSnapshot(ctx context.Context, req *datastorepb.RunQueryRequest, owned *querySnapshot) (*datastorepb.RunQueryResponse, error) {
	ctx, stats := storage.WithQueryReadStats(ctx)
	if owned != nil && owned.readStats != nil {
		stats = owned.readStats // The suspended stream retains its first context.
	}
	read := &queryRead{stats: stats, initialReads: stats.Documents}
	ctx, prepared, err := g.prepareRunQuery(ctx, req, owned)
	if err != nil {
		return nil, err
	}
	if opts := req.GetExplainOptions(); opts != nil && !opts.Analyze {
		return g.explainPreparedRunQuery(ctx, prepared, owned)
	}
	if err := g.resolveQueryRead(prepared, owned, read); err != nil {
		return nil, err
	}
	read.started = time.Now()
	access := &queryAccess{}
	if err := g.prepareQueryAccess(ctx, prepared, read, access); err != nil {
		return nil, err
	}
	if read.readAt == nil {
		snapshot := g.store.ReadTime()
		read.readAt = &snapshot
		read.responseReadTime = timestamppb.New(snapshot)
	}
	releaseSnapshot, err := g.store.PinReadTime(*read.readAt)
	if err != nil {
		return nil, err
	}
	if owned != nil && owned.release == nil {
		owned.time, owned.release = *read.readAt, releaseSnapshot
	} else {
		defer releaseSnapshot()
	}
	if prepared.entryQuery && owned != nil && owned.aggregation != nil {
		aggregation := owned.aggregation
		if err := aggregation.prepare(ctx, g.indexes, prepared.request.ProjectId, prepared.query, prepared.condition, *read.readAt); err != nil {
			return nil, err
		}
		access.index, access.builtinProperty = aggregation.index, aggregation.builtin
		access.builtinOK = access.builtinProperty != ""
		access.declinePlanning = access.index == nil && !access.builtinOK
		owned.fingerprint = prepared.fingerprint
	}
	scan := &queryScan{}
	if response, err := g.scanRunQuery(ctx, prepared, read, access, scan, owned); err != nil || response != nil {
		return response, err
	}
	resp, err := buildQueryResponse(ctx, prepared, read, access, scan)
	if err != nil {
		return nil, err
	}
	// Only returned entities become transaction dependencies; prefetched rows do not.
	if read.activeTxID != "" {
		if err := g.recordQueryResultReads(prepared, read.activeTxID, resp.Batch.EntityResults); err != nil {
			return nil, err
		}
	}
	if owned != nil && owned.fallback != nil && len(resp.Batch.EndCursor) > 0 {
		cursor, ok := decodeCursorFull(resp.Batch.EndCursor)
		if !ok {
			return nil, status.Error(codes.Internal, "invalid internal continuation cursor")
		}
		owned.fallback.acknowledge(cursor.K)
	}
	return resp, nil
}
