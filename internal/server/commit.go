package server

import (
	"context"
	"database/sql"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func (s *Server) Commit(ctx context.Context, req *firestorepb.CommitRequest) (*firestorepb.CommitResponse, error) {
	if len(req.Transaction) > 0 {
		txID := string(req.Transaction)
		s.txMu.Lock()
		entry, ok := s.txns[txID]
		if ok {
			delete(s.txns, txID)
		}
		s.txMu.Unlock()
		if !ok {
			return nil, status.Errorf(codes.NotFound, "transaction not found: %s", txID)
		}
		if entry.readOnly && len(req.Writes) > 0 {
			return nil, status.Errorf(codes.FailedPrecondition, "read-only transaction cannot contain writes")
		}
	}

	var results []*firestorepb.WriteResult
	acc := storage.NewFsCommitAccumulator()

	if err := s.store.RunInTx(func(tx *sql.Tx) error {
		results = make([]*firestorepb.WriteResult, 0, len(req.Writes))

		// Fast path: all writes are simple full-replace upserts with no preconditions,
		// masks, or transforms. Batched into one INSERT and one bulk field-index flush.
		if rows, project, database, ok := s.collectSimpleUpsertWrites(req.Writes); ok {
			docs, err := s.store.UpsertDocsManyTx(tx, project, database, rows, acc)
			if err != nil {
				return err
			}
			for _, r := range rows {
				results = append(results, &firestorepb.WriteResult{UpdateTime: docs[r.Path].UpdateTime})
			}
			return acc.Flush(tx)
		}

		// Slow path: writes with preconditions, update masks, transforms, or deletes.
		for _, w := range req.Writes {
			wr, err := s.applyWriteTxAcc(tx, w, acc)
			if err != nil {
				return err
			}
			results = append(results, wr)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Notify subscribers after the transaction has committed (both fast and slow paths).
	s.store.NotifyBatch(acc.Events())

	return &firestorepb.CommitResponse{
		WriteResults: results,
		CommitTime:   timestamppb.Now(),
	}, nil
}

// collectSimpleUpsertWrites returns the bulk-upsert rows for the fast path iff every
// write is a full-replace Write_Update with no precondition, mask, or transforms,
// and all writes target the same project/database.
// Returns (rows, project, database, ok); ok is false if any write needs the slow path.
func (s *Server) collectSimpleUpsertWrites(writes []*firestorepb.Write) ([]storage.FsUpsertDocRow, string, string, bool) {
	if len(writes) == 0 {
		return nil, "", "", true
	}
	rows := make([]storage.FsUpsertDocRow, 0, len(writes))
	var project, database string
	for _, w := range writes {
		op, ok := w.Operation.(*firestorepb.Write_Update)
		if !ok {
			return nil, "", "", false // Delete or Transform
		}
		if w.CurrentDocument != nil {
			return nil, "", "", false // precondition
		}
		if w.UpdateMask != nil && len(w.UpdateMask.FieldPaths) > 0 {
			return nil, "", "", false // merge (partial update)
		}
		if len(w.UpdateTransforms) > 0 {
			return nil, "", "", false // field transforms
		}

		proj, db, path, err := parseName(op.Update.Name)
		if err != nil {
			return nil, "", "", false
		}
		if err := checkDocID(path); err != nil {
			return nil, "", "", false
		}
		if project == "" {
			project, database = proj, db
		} else if proj != project || db != database {
			return nil, "", "", false // mixed projects/databases
		}
		collection, parentPath, _ := splitDocPath(path)
		rows = append(rows, storage.FsUpsertDocRow{
			Collection: collection,
			ParentPath: parentPath,
			Path:       path,
			Doc: &firestorepb.Document{
				Name:   buildDocName(project, database, path),
				Fields: op.Update.GetFields(),
			},
		})
	}
	return rows, project, database, true
}
