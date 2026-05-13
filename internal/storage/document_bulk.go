package storage

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// batchInsertFIBulk inserts field-index rows where each row carries its own
// project/database/collection_path/doc_path metadata.
// Used by FsCommitAccumulator.Flush to write all field-index rows in one batched operation.
func batchInsertFIBulk(exec dbExec, rows []fsFiBulkRow) error {
	if len(rows) == 0 {
		return nil
	}
	const batchSize = 500
	const prefix = `INSERT INTO field_index` +
		` (project, database, collection_path, doc_path, field_path,` +
		`  value_string, value_int, value_double, value_bool, value_null, value_ref, value_bytes, in_array)` +
		` VALUES `
	const placeholder = `(?,?,?,?,?,?,?,?,?,?,?,?,?)`

	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]
		var sb strings.Builder
		sb.WriteString(prefix)
		args := make([]any, 0, len(batch)*13)
		for i, br := range batch {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(placeholder)
			inArrayInt := 0
			if br.row.inArray {
				inArrayInt = 1
			}
			args = append(args,
				br.project, br.database, br.collectionPath, br.docPath, br.row.fieldPath,
				br.row.vals.str, br.row.vals.i, br.row.vals.f, br.row.vals.b,
				br.row.vals.null, br.row.vals.ref, nilIfEmpty(br.row.vals.bytes),
				inArrayInt,
			)
		}
		if _, err := exec.Exec(sb.String(), args...); err != nil {
			return fmt.Errorf("field_index bulk insert: %w", err)
		}
	}
	return nil
}

// FsUpsertDocRow describes one document in a bulk Firestore upsert.
// Collection, ParentPath, and Path are derived from the document name.
// Doc.Name and Doc.Fields must be set; CreateTime/UpdateTime are overwritten by UpsertDocsManyTx.
type FsUpsertDocRow struct {
	Collection, ParentPath, Path string
	Doc                          *firestorepb.Document
}

type fsFiDeleteKey struct{ project, database, docPath string }

type fsFiBulkRow struct {
	project, database, collectionPath, docPath string
	row                                        fiPendingRow
}

type fsChangeRow struct {
	project, database, path, collection, parentPath string
	changeTime                                      string
	deleted                                         int
	data                                            []byte
}

// FsCommitAccumulator collects ChangeEvents from a Firestore Commit or BatchWrite
// for post-transaction fan-out. It also holds pending bulk field-index / change-log
// rows for future slow-path writes (Stage 3+).
//
// Usage:
//
//	acc := storage.NewFsCommitAccumulator()
//	docs, err := store.UpsertDocsManyTx(tx, project, db, rows, acc)
//	if err := acc.Flush(tx); err != nil { return err }  // no-op for fast path; used by slow path
//	// after RunInTx succeeds:
//	store.NotifyBatch(acc.Events())
type FsCommitAccumulator struct {
	changes   []fsChangeRow
	fiDeletes []fsFiDeleteKey
	fiInserts []fsFiBulkRow
	events    []ChangeEvent
}

// NewFsCommitAccumulator returns an empty accumulator.
func NewFsCommitAccumulator() *FsCommitAccumulator { return &FsCommitAccumulator{} }

// Events returns the accumulated ChangeEvents. Drain after a successful transaction
// commit and pass to NotifyBatch.
func (a *FsCommitAccumulator) Events() []ChangeEvent { return a.events }

// Flush writes any pending bulk effects accumulated by slow-path writes (Stage 3+).
// No-op for pure fast-path commits where UpsertDocsManyTx handled all SQL inline.
// Must be called inside the write transaction, before Commit.
func (a *FsCommitAccumulator) Flush(exec dbExec) error {
	if err := a.flushFiDeletes(exec); err != nil {
		return err
	}
	if err := a.flushFiInserts(exec); err != nil {
		return err
	}
	return a.flushChanges(exec)
}

func (a *FsCommitAccumulator) flushFiDeletes(exec dbExec) error {
	if len(a.fiDeletes) == 0 {
		return nil
	}
	type dbKey struct{ project, database string }
	groups := make(map[dbKey][]string, len(a.fiDeletes))
	for _, d := range a.fiDeletes {
		k := dbKey{d.project, d.database}
		groups[k] = append(groups[k], d.docPath)
	}
	const chunkSize = 900
	for k, paths := range groups {
		for start := 0; start < len(paths); start += chunkSize {
			end := start + chunkSize
			if end > len(paths) {
				end = len(paths)
			}
			chunk := paths[start:end]
			ph := strings.Repeat("?,", len(chunk))
			ph = ph[:len(ph)-1]
			args := make([]any, 0, 2+len(chunk))
			args = append(args, k.project, k.database)
			for _, p := range chunk {
				args = append(args, p)
			}
			q := `DELETE FROM field_index WHERE project=? AND database=? AND doc_path IN (` + ph + `)`
			if _, err := exec.Exec(q, args...); err != nil {
				return fmt.Errorf("field_index bulk delete: %w", err)
			}
		}
	}
	return nil
}

func (a *FsCommitAccumulator) flushFiInserts(exec dbExec) error {
	return batchInsertFIBulk(exec, a.fiInserts)
}

func (a *FsCommitAccumulator) flushChanges(exec dbExec) error {
	if len(a.changes) == 0 {
		return nil
	}
	const batchSize = 500
	const prefix = `INSERT INTO document_changes ` +
		`(project, database, path, collection, parent_path, change_time, deleted, data) VALUES `
	const placeholder = `(?,?,?,?,?,?,?,?)`

	eventIdx := 0
	for start := 0; start < len(a.changes); start += batchSize {
		end := start + batchSize
		if end > len(a.changes) {
			end = len(a.changes)
		}
		batch := a.changes[start:end]

		var sb strings.Builder
		sb.WriteString(prefix)
		args := make([]any, 0, len(batch)*8)
		for i, c := range batch {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(placeholder)
			args = append(args, c.project, c.database, c.path, c.collection, c.parentPath, c.changeTime, c.deleted, c.data)
		}

		// Use Exec + LastInsertId to avoid rows.Next() overhead.
		// SQLite AUTOINCREMENT assigns consecutive seq values for a single multi-row INSERT,
		// so firstSeq = lastInsertId - batchLen + 1.
		res, err := exec.Exec(sb.String(), args...)
		if err != nil {
			return fmt.Errorf("document_changes bulk insert: %w", err)
		}
		lastID, err := res.LastInsertId()
		if err != nil {
			return fmt.Errorf("document_changes last insert id: %w", err)
		}
		firstSeq := lastID - int64(len(batch)) + 1
		for i := range batch {
			if eventIdx < len(a.events) {
				a.events[eventIdx].Seq = firstSeq + int64(i)
				eventIdx++
			}
		}
	}
	return nil
}

// UpsertDocsManyTx upserts multiple Firestore documents using per-row cached SQL for the
// documents table, deferring field-index and change-log work to acc for bulk flushing.
//
// All rows must belong to the same project and database.
// Returns a map from document path to the committed document (with CreateTime and UpdateTime set).
func (s *Store) UpsertDocsManyTx(tx *sql.Tx, project, database string, rows []FsUpsertDocRow, acc *FsCommitAccumulator) (map[string]*firestorepb.Document, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	now := monotonicNow()

	result := make(map[string]*firestorepb.Document, len(rows))
	for _, r := range rows {
		// Preserve create_time for existing active documents (cached 1-row SELECT).
		createTime := now
		var existingCreateStr string
		err := tx.QueryRow(
			`SELECT create_time FROM documents WHERE project=? AND database=? AND path=? AND deleted=0`,
			project, database, r.Path,
		).Scan(&existingCreateStr)
		if err == nil {
			if t, e := time.Parse(timeLayout, existingCreateStr); e == nil {
				createTime = timestamppb.New(t)
			}
		}

		d := cloneDoc(r.Doc)
		d.CreateTime = createTime
		d.UpdateTime = now

		// Persist document + field-index using cached single-row SQL (same SQL text -> cache hit).
		if err := saveDocExec(tx, project, database, r.Collection, r.ParentPath, r.Path, d); err != nil {
			return nil, fmt.Errorf("upsert doc %s: %w", r.Path, err)
		}

		// Append change-log and collect seq for the ChangeEvent.
		data, _ := proto.Marshal(d)
		seq, err := appendChangeExec(tx, project, database, r.Path, r.Collection, r.ParentPath, false, data, now.AsTime())
		if err != nil {
			return nil, fmt.Errorf("append change %s: %w", r.Path, err)
		}

		acc.events = append(acc.events, ChangeEvent{Name: d.Name, Doc: d, Seq: seq, Project: project, Database: database, Collection: r.Collection, ParentPath: r.ParentPath})
		result[r.Path] = d
	}

	return result, nil
}

// NotifyBatch fans out a slice of ChangeEvents to all active subscribers.
// Call this after a successful transaction commit (never inside the tx).
func (s *Store) NotifyBatch(events []ChangeEvent) {
	for _, ev := range events {
		s.notify(ev)
	}
}
