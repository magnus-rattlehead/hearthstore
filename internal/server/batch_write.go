package server

import (
	"context"
	"database/sql"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func (s *Server) BatchWrite(ctx context.Context, req *firestorepb.BatchWriteRequest) (*firestorepb.BatchWriteResponse, error) {
	writeResults := make([]*firestorepb.WriteResult, len(req.Writes))
	writeStatus := make([]*statuspb.Status, len(req.Writes))

	// Fast path: all simple upserts → single tx, one WAL sync instead of N.
	if rows, project, database, ok := s.collectSimpleUpsertWrites(req.Writes); ok {
		acc := storage.NewFsCommitAccumulator()
		var docs map[string]*firestorepb.Document
		err := s.store.RunInTx(func(tx *sql.Tx) error {
			var e error
			docs, e = s.store.UpsertDocsManyTx(tx, project, database, rows, acc)
			if e != nil {
				return e
			}
			return acc.Flush(tx)
		})
		if err != nil {
			st, _ := status.FromError(err)
			proto := st.Proto()
			for i := range req.Writes {
				writeResults[i] = &firestorepb.WriteResult{UpdateTime: timestamppb.Now()}
				writeStatus[i] = proto
			}
		} else {
			s.store.NotifyBatch(acc.Events())
			for i, r := range rows {
				writeResults[i] = &firestorepb.WriteResult{UpdateTime: docs[r.Path].UpdateTime}
				writeStatus[i] = &statuspb.Status{Code: 0}
			}
		}
		return &firestorepb.BatchWriteResponse{WriteResults: writeResults, Status: writeStatus}, nil
	}

	// Slow path: per-write (preserves per-write fault isolation for deletes/preconditions).
	for i, w := range req.Writes {
		wr, err := s.applyWrite(w)
		if err != nil {
			st, _ := status.FromError(err)
			writeResults[i] = &firestorepb.WriteResult{UpdateTime: timestamppb.Now()}
			writeStatus[i] = st.Proto()
		} else {
			writeResults[i] = wr
			writeStatus[i] = &statuspb.Status{Code: 0}
		}
	}

	return &firestorepb.BatchWriteResponse{
		WriteResults: writeResults,
		Status:       writeStatus,
	}, nil
}
