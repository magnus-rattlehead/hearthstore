package storage

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// timeLayout uses fixed 9-decimal RFC3339; time.RFC3339Nano drops trailing zeros,
// which breaks SQLite lexicographic comparison ('.' < 'Z').
const timeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// monotonicNow returns a timestamp strictly greater (by ≥1ms) than the last,
// so sequential writes within the same millisecond get distinct update times.
var (
	monoMu     sync.Mutex
	lastTimeMs int64
)

func monotonicNow() *timestamppb.Timestamp {
	monoMu.Lock()
	defer monoMu.Unlock()
	nowMs := time.Now().UnixMilli()
	if nowMs <= lastTimeMs {
		nowMs = lastTimeMs + 1
	}
	lastTimeMs = nowMs
	return timestamppb.New(time.UnixMilli(nowMs))
}

// ChangeEvent is emitted to subscribers whose scope matches the written document.
type ChangeEvent struct {
	Name       string                // full Firestore resource name
	Doc        *firestorepb.Document // non-nil on write; nil on delete
	Deleted    bool
	Seq        int64  // change log sequence number; always > 0
	Project    string // populated by all write paths for sharded fan-out
	Database   string
	Collection string
	ParentPath string
}

// SubScope describes the watch scope for a subscriber shard.
// AllDescendants=true → collection-group listener (any parent depth).
// AllDescendants=false → single-collection listener at ParentPath.
type SubScope struct {
	Project, Database, ParentPath, Collection string
	AllDescendants                            bool
}

type subEntry struct {
	ch     chan ChangeEvent
	scopes []SubScope
}

type subScopeKey struct{ project, database, parentPath, collection string }
type subCollGKey struct{ project, database, collection string }

// Subscribe registers a new change listener with no initial scope filter.
// Call UpdateScopes to route events to this subscriber.
// The caller must call Unsubscribe(id) when done.
func (s *Store) Subscribe() (uint64, <-chan ChangeEvent) {
	ch := make(chan ChangeEvent, 64)
	s.subMu.Lock()
	s.nextID++
	id := s.nextID
	s.subEntries[id] = &subEntry{ch: ch}
	s.subMu.Unlock()
	return id, ch
}

// UpdateScopes replaces the scope filter for subscriber id.
// Only events whose (project, database, collection, parentPath) match one of the
// given scopes will be delivered to this subscriber's channel.
func (s *Store) UpdateScopes(id uint64, scopes []SubScope) {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	entry, ok := s.subEntries[id]
	if !ok {
		return
	}
	s.removeSubFromShards(id, entry.scopes)
	entry.scopes = scopes
	ch := entry.ch
	for _, sc := range scopes {
		if sc.AllDescendants {
			k := subCollGKey{sc.Project, sc.Database, sc.Collection}
			if s.byCollG[k] == nil {
				s.byCollG[k] = make(map[uint64]chan ChangeEvent)
			}
			s.byCollG[k][id] = ch
		} else {
			k := subScopeKey{sc.Project, sc.Database, sc.ParentPath, sc.Collection}
			if s.byScope[k] == nil {
				s.byScope[k] = make(map[uint64]chan ChangeEvent)
			}
			s.byScope[k][id] = ch
		}
	}
}

func (s *Store) removeSubFromShards(id uint64, scopes []SubScope) {
	for _, sc := range scopes {
		if sc.AllDescendants {
			k := subCollGKey{sc.Project, sc.Database, sc.Collection}
			delete(s.byCollG[k], id)
			if len(s.byCollG[k]) == 0 {
				delete(s.byCollG, k)
			}
		} else {
			k := subScopeKey{sc.Project, sc.Database, sc.ParentPath, sc.Collection}
			delete(s.byScope[k], id)
			if len(s.byScope[k]) == 0 {
				delete(s.byScope, k)
			}
		}
	}
}

// Unsubscribe removes and closes the subscriber channel.
func (s *Store) Unsubscribe(id uint64) {
	s.subMu.Lock()
	entry, ok := s.subEntries[id]
	if ok {
		s.removeSubFromShards(id, entry.scopes)
		delete(s.subEntries, id)
	}
	s.subMu.Unlock()
	if ok {
		close(entry.ch)
	}
}

// notify sends a ChangeEvent only to subscribers whose scope matches the event.
func (s *Store) notify(ev ChangeEvent) {
	s.subMu.RLock()
	defer s.subMu.RUnlock()
	send := func(shard map[uint64]chan ChangeEvent) {
		for _, ch := range shard {
			select {
			case ch <- ev:
			default:
			}
		}
	}
	if shard := s.byScope[subScopeKey{ev.Project, ev.Database, ev.ParentPath, ev.Collection}]; shard != nil {
		send(shard)
	}
	if shard := s.byCollG[subCollGKey{ev.Project, ev.Database, ev.Collection}]; shard != nil {
		send(shard)
	}
}

// GetDoc fetches a single active document. Returns codes.NotFound if absent or soft-deleted.
func (s *Store) GetDoc(project, database, path string) (*firestorepb.Document, error) {
	return getDocExec(s.rdb, project, database, path)
}

// GetDocTx fetches a single active document within a transaction.
func (s *Store) GetDocTx(tx *sql.Tx, project, database, path string) (*firestorepb.Document, error) {
	return getDocExec(tx, project, database, path)
}

func getDocExec(exec dbExec, project, database, path string) (*firestorepb.Document, error) {
	var data []byte
	err := exec.QueryRow(
		`SELECT data FROM documents WHERE project=? AND database=? AND path=? AND deleted=0`,
		project, database, path,
	).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, status.Errorf(codes.NotFound, "document not found: %s", path)
	}
	if err != nil {
		return nil, fmt.Errorf("get doc: %w", err)
	}
	var doc firestorepb.Document
	if err := proto.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("unmarshal doc: %w", err)
	}
	return &doc, nil
}

// InsertDoc creates a new document, returning codes.AlreadyExists if one is already active.
func (s *Store) InsertDoc(project, database, collection, parentPath, path string, doc *firestorepb.Document) (*firestorepb.Document, error) {
	d, seq, err := insertDocExec(s.wdb, project, database, collection, parentPath, path, doc)
	if err != nil {
		return nil, err
	}
	s.notify(ChangeEvent{Name: d.Name, Doc: d, Seq: seq, Project: project, Database: database, Collection: collection, ParentPath: parentPath})
	return d, nil
}

// InsertDocTx creates a new document within a transaction.
func (s *Store) InsertDocTx(tx *sql.Tx, project, database, collection, parentPath, path string, doc *firestorepb.Document) (*firestorepb.Document, error) {
	d, seq, err := insertDocExec(tx, project, database, collection, parentPath, path, doc)
	if err != nil {
		return nil, err
	}
	s.notify(ChangeEvent{Name: d.Name, Doc: d, Seq: seq, Project: project, Database: database, Collection: collection, ParentPath: parentPath})
	return d, nil
}

// InsertDocTxAcc creates a new document within a transaction, collecting the
// ChangeEvent in acc for post-tx fan-out instead of notifying inline.
func (s *Store) InsertDocTxAcc(tx *sql.Tx, project, database, collection, parentPath, path string, doc *firestorepb.Document, acc *FsCommitAccumulator) (*firestorepb.Document, error) {
	d, seq, err := insertDocExec(tx, project, database, collection, parentPath, path, doc)
	if err != nil {
		return nil, err
	}
	acc.events = append(acc.events, ChangeEvent{Name: d.Name, Doc: d, Seq: seq, Project: project, Database: database, Collection: collection, ParentPath: parentPath})
	return d, nil
}

func insertDocExec(exec dbExec, project, database, collection, parentPath, path string, doc *firestorepb.Document) (*firestorepb.Document, int64, error) {
	var dummy int
	err := exec.QueryRow(
		`SELECT 1 FROM documents WHERE project=? AND database=? AND path=? AND deleted=0`,
		project, database, path,
	).Scan(&dummy)
	if err == nil {
		return nil, 0, status.Errorf(codes.AlreadyExists, "document already exists: %s", path)
	}

	now := monotonicNow()
	d := cloneDoc(doc)
	d.CreateTime = now
	d.UpdateTime = now

	if err := saveDocExec(exec, project, database, collection, parentPath, path, d); err != nil {
		return nil, 0, err
	}
	data, _ := proto.Marshal(d)
	seq, err := appendChangeExec(exec, project, database, path, collection, parentPath, false, data, now.AsTime())
	if err != nil {
		return nil, 0, err
	}
	return d, seq, nil
}

// UpsertDoc creates or replaces a document, preserving create_time for existing active docs.
func (s *Store) UpsertDoc(project, database, collection, parentPath, path string, doc *firestorepb.Document) (*firestorepb.Document, error) {
	d, seq, err := upsertDocExec(s.wdb, project, database, collection, parentPath, path, doc)
	if err != nil {
		return nil, err
	}
	s.notify(ChangeEvent{Name: d.Name, Doc: d, Seq: seq, Project: project, Database: database, Collection: collection, ParentPath: parentPath})
	return d, nil
}

// UpsertDocTx creates or replaces a document within a transaction.
func (s *Store) UpsertDocTx(tx *sql.Tx, project, database, collection, parentPath, path string, doc *firestorepb.Document) (*firestorepb.Document, error) {
	d, seq, err := upsertDocExec(tx, project, database, collection, parentPath, path, doc)
	if err != nil {
		return nil, err
	}
	s.notify(ChangeEvent{Name: d.Name, Doc: d, Seq: seq, Project: project, Database: database, Collection: collection, ParentPath: parentPath})
	return d, nil
}

// UpsertDocTxAcc creates or replaces a document within a transaction, collecting the
// ChangeEvent in acc for post-tx fan-out instead of notifying inline.
func (s *Store) UpsertDocTxAcc(tx *sql.Tx, project, database, collection, parentPath, path string, doc *firestorepb.Document, acc *FsCommitAccumulator) (*firestorepb.Document, error) {
	d, seq, err := upsertDocExec(tx, project, database, collection, parentPath, path, doc)
	if err != nil {
		return nil, err
	}
	acc.events = append(acc.events, ChangeEvent{Name: d.Name, Doc: d, Seq: seq, Project: project, Database: database, Collection: collection, ParentPath: parentPath})
	return d, nil
}

func upsertDocExec(exec dbExec, project, database, collection, parentPath, path string, doc *firestorepb.Document) (*firestorepb.Document, int64, error) {
	now := monotonicNow()

	// Preserve create_time for existing active documents.
	createTime := now
	var existingCreateStr string
	err := exec.QueryRow(
		`SELECT create_time FROM documents WHERE project=? AND database=? AND path=? AND deleted=0`,
		project, database, path,
	).Scan(&existingCreateStr)
	if err == nil {
		if t, e := time.Parse(timeLayout, existingCreateStr); e == nil {
			createTime = timestamppb.New(t)
		}
	}

	d := cloneDoc(doc)
	d.CreateTime = createTime
	d.UpdateTime = now

	if err := saveDocExec(exec, project, database, collection, parentPath, path, d); err != nil {
		return nil, 0, err
	}
	data, _ := proto.Marshal(d)
	seq, err := appendChangeExec(exec, project, database, path, collection, parentPath, false, data, now.AsTime())
	if err != nil {
		return nil, 0, err
	}
	return d, seq, nil
}

// DeleteDoc soft-deletes a document. No-ops silently if the document is already absent.
func (s *Store) DeleteDoc(project, database, path string) error {
	return s.deleteDocWith(s.wdb, project, database, path)
}

// DeleteDocTx soft-deletes a document within a transaction.
func (s *Store) DeleteDocTx(tx *sql.Tx, project, database, path string) error {
	return s.deleteDocWith(tx, project, database, path)
}

// DeleteDocTxAcc soft-deletes a document within a transaction, collecting the
// ChangeEvent in acc for post-tx fan-out instead of notifying inline.
func (s *Store) DeleteDocTxAcc(tx *sql.Tx, project, database, path string, acc *FsCommitAccumulator) error {
	doc, _ := getDocExec(tx, project, database, path)
	name := path
	if doc != nil {
		name = doc.Name
	}
	if err := deleteDocExec(tx, project, database, path); err != nil {
		return err
	}
	coll, parent := splitChangePath(path)
	seq, err := appendChangeExec(tx, project, database, path, coll, parent, true, nil, time.Now())
	if err != nil {
		return err
	}
	acc.events = append(acc.events, ChangeEvent{Name: name, Deleted: true, Seq: seq, Project: project, Database: database, Collection: coll, ParentPath: parent})
	return nil
}

func (s *Store) deleteDocWith(exec dbExec, project, database, path string) error {
	doc, _ := getDocExec(exec, project, database, path)
	name := path
	if doc != nil {
		name = doc.Name
	}
	if err := deleteDocExec(exec, project, database, path); err != nil {
		return err
	}
	coll, parent := splitChangePath(path)
	seq, err := appendChangeExec(exec, project, database, path, coll, parent, true, nil, time.Now())
	if err != nil {
		return err
	}
	s.notify(ChangeEvent{Name: name, Deleted: true, Seq: seq, Project: project, Database: database, Collection: coll, ParentPath: parent})
	return nil
}

func deleteDocExec(exec dbExec, project, database, path string) error {
	_, err := exec.Exec(
		`UPDATE documents SET deleted=1, update_time=? WHERE project=? AND database=? AND path=? AND deleted=0`,
		time.Now().UTC().Format(timeLayout), project, database, path,
	)
	if err != nil {
		return err
	}
	_, err = exec.Exec(
		`DELETE FROM field_index WHERE project=? AND database=? AND doc_path=?`,
		project, database, path,
	)
	return err
}

// ListDocs returns documents in a specific collection under parentPath, with cursor pagination.
// pageToken is the path of the last document returned in the previous page (empty for first page).
// When showMissing is true, documents that don't exist but have subcollections are also included
// (with name set but fields/timestamps absent — matching Firestore's showMissing semantics).
// Returns (docs, nextPageToken, error); nextPageToken is empty when there are no more results.
func (s *Store) ListDocs(project, database, parentPath, collection string, pageSize int32, pageToken string, showMissing bool) ([]*firestorepb.Document, string, error) {
	limit := int(pageSize)
	if limit <= 0 {
		limit = 300
	}

	// Relative collection prefix: all documents in this collection have paths starting with it.
	collPrefix := collection
	if parentPath != "" {
		collPrefix = parentPath + "/" + collection
	}

	// Collect all doc paths to include:
	// 1. Real (existing) documents.
	// 2. If showMissing: virtual document IDs that appear as ancestors of descendant documents.
	type entry struct {
		path    string
		docData []byte // nil for missing documents
	}
	byPath := make(map[string]*entry)

	// Query real documents.
	var realRows *sql.Rows
	var err error
	if pageToken == "" {
		realRows, err = s.rdb.Query(
			`SELECT data, path FROM documents
			 WHERE project=? AND database=? AND parent_path=? AND collection=? AND deleted=0
			 ORDER BY path LIMIT ?`,
			project, database, parentPath, collection, limit+1,
		)
	} else {
		realRows, err = s.rdb.Query(
			`SELECT data, path FROM documents
			 WHERE project=? AND database=? AND parent_path=? AND collection=? AND deleted=0
			 AND path > ?
			 ORDER BY path LIMIT ?`,
			project, database, parentPath, collection, pageToken, limit+1,
		)
	}
	if err != nil {
		return nil, "", fmt.Errorf("list docs: %w", err)
	}
	defer realRows.Close()
	for realRows.Next() {
		var data []byte
		var path string
		if err := realRows.Scan(&data, &path); err != nil {
			return nil, "", fmt.Errorf("scan: %w", err)
		}
		byPath[path] = &entry{path: path, docData: data}
	}
	if err := realRows.Err(); err != nil {
		return nil, "", err
	}

	// Query virtual (missing) document IDs — ancestors of descendant docs that don't exist.
	if showMissing {
		// offset is the 1-based starting position of the docId segment in path.
		// collPrefix + "/" + docId + "/" + rest
		offset := len(collPrefix) + 2 // skip leading '/'
		// WHERE: path has at least one more '/' after the docId (i.e., it's in a subcollection).
		missRows, err := s.rdb.Query(
			`SELECT DISTINCT SUBSTR(path, ?, INSTR(SUBSTR(path, ?), '/') - 1) as doc_id
			 FROM documents
			 WHERE project=? AND database=? AND deleted=0
			   AND path LIKE ? || '/%/%'
			   AND INSTR(SUBSTR(path, ?), '/') > 0`,
			offset, offset, project, database, collPrefix, offset,
		)
		if err != nil {
			return nil, "", fmt.Errorf("list missing docs: %w", err)
		}
		defer missRows.Close()
		for missRows.Next() {
			var docID string
			if err := missRows.Scan(&docID); err != nil {
				return nil, "", fmt.Errorf("scan missing: %w", err)
			}
			if docID == "" {
				continue
			}
			docPath := collPrefix + "/" + docID
			if _, exists := byPath[docPath]; !exists {
				// pageToken cursor filter
				if pageToken != "" && docPath <= pageToken {
					continue
				}
				byPath[docPath] = &entry{path: docPath, docData: nil}
			}
		}
		if err := missRows.Err(); err != nil {
			return nil, "", err
		}
	}

	// Collect and sort all entries by path.
	entries := make([]*entry, 0, len(byPath))
	for _, e := range byPath {
		entries = append(entries, e)
	}
	// Insertion sort (list is typically small).
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0 && entries[j].path < entries[j-1].path; j-- {
			entries[j], entries[j-1] = entries[j-1], entries[j]
		}
	}

	// Apply limit+1 to detect next page.
	var nextToken string
	if len(entries) > limit {
		entries = entries[:limit]
		nextToken = entries[limit-1].path
	}

	// Build Document slice.
	docs := make([]*firestorepb.Document, 0, len(entries))
	fullPrefix := "projects/" + project + "/databases/" + database + "/documents/"
	for _, e := range entries {
		if e.docData != nil {
			var doc firestorepb.Document
			if err := proto.Unmarshal(e.docData, &doc); err != nil {
				return nil, "", fmt.Errorf("unmarshal: %w", err)
			}
			docs = append(docs, &doc)
		} else {
			// Missing document: name only, no fields.
			docs = append(docs, &firestorepb.Document{
				Name: fullPrefix + e.path,
			})
		}
	}

	return docs, nextToken, nil
}

// ListCollectionIDs returns the distinct direct-child collection IDs under parentPath.
func (s *Store) ListCollectionIDs(project, database, parentPath string) ([]string, error) {
	rows, err := s.rdb.Query(
		`SELECT DISTINCT collection FROM documents
		 WHERE project=? AND database=? AND parent_path=? AND deleted=0
		 ORDER BY collection`,
		project, database, parentPath,
	)
	if err != nil {
		return nil, fmt.Errorf("list collection IDs: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// QueryCollection returns all active documents in a collection.
// If allDescendants is true, parentPath is ignored and all documents in the collection
// across all ancestor paths are returned (collection group query).
func (s *Store) QueryCollection(project, database, parentPath, collection string, allDescendants bool) ([]*firestorepb.Document, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if allDescendants {
		rows, err = s.rdb.Query(
			`SELECT data FROM documents WHERE project=? AND database=? AND collection=? AND deleted=0`,
			project, database, collection,
		)
	} else {
		rows, err = s.rdb.Query(
			`SELECT data FROM documents WHERE project=? AND database=? AND parent_path=? AND collection=? AND deleted=0`,
			project, database, parentPath, collection,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("query collection: %w", err)
	}
	defer rows.Close()

	var docs []*firestorepb.Document
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		var doc firestorepb.Document
		if err := proto.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
		docs = append(docs, &doc)
	}
	return docs, rows.Err()
}

// ChangedDoc is a document that changed (written or deleted) after a given timestamp.
type ChangedDoc struct {
	Doc     *firestorepb.Document // non-nil when alive
	Name    string                // always set; full resource name
	Deleted bool
}

// GetDocsSince returns all documents (live and deleted) whose update_time is strictly
// after since for the given collection scope.
//   - allDescendants=false: constrain by parent_path AND collection
//   - allDescendants=true:  constrain by collection only (collection-group)
func (s *Store) GetDocsSince(project, database string, since time.Time, parentPath, collection string, allDescendants bool) ([]ChangedDoc, error) {
	sinceStr := since.UTC().Format(timeLayout)
	var rows *sql.Rows
	var err error
	if allDescendants {
		rows, err = s.rdb.Query(
			`SELECT path, data, deleted FROM documents
			 WHERE project=? AND database=? AND collection=? AND update_time > ?`,
			project, database, collection, sinceStr,
		)
	} else {
		rows, err = s.rdb.Query(
			`SELECT path, data, deleted FROM documents
			 WHERE project=? AND database=? AND parent_path=? AND collection=? AND update_time > ?`,
			project, database, parentPath, collection, sinceStr,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("get docs since: %w", err)
	}
	defer rows.Close()

	var results []ChangedDoc
	for rows.Next() {
		var path string
		var data []byte
		var deleted int
		if err := rows.Scan(&path, &data, &deleted); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		name := fmt.Sprintf("projects/%s/databases/%s/documents/%s", project, database, path)
		if deleted != 0 {
			results = append(results, ChangedDoc{Name: name, Deleted: true})
			continue
		}
		var doc firestorepb.Document
		if err := proto.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
		results = append(results, ChangedDoc{Doc: &doc, Name: name})
	}
	return results, rows.Err()
}

// GetDocAsOf returns the state of a single document at or before asOf.
// Returns codes.NotFound if the document did not exist (or was deleted) at that time.
func (s *Store) GetDocAsOf(project, database, path string, asOf time.Time) (*firestorepb.Document, error) {
	asOfStr := asOf.UTC().Format(timeLayout)
	var data []byte
	var deleted int
	err := s.rdb.QueryRow(
		`SELECT data, deleted FROM document_changes
		 WHERE project=? AND database=? AND path=? AND change_time <= ?
		 ORDER BY seq DESC LIMIT 1`,
		project, database, path, asOfStr,
	).Scan(&data, &deleted)
	if err == sql.ErrNoRows || deleted != 0 {
		return nil, status.Errorf(codes.NotFound, "document not found: %s", path)
	}
	if err != nil {
		return nil, fmt.Errorf("get doc as of: %w", err)
	}
	var doc firestorepb.Document
	if err := proto.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("unmarshal doc: %w", err)
	}
	return &doc, nil
}

// ListDocsAsOf returns all non-deleted documents in a collection as they existed at asOf.
// parentPath="" means top-level collection. For collection-group queries pass allDescendants=true.
func (s *Store) ListDocsAsOf(project, database, collection, parentPath string, allDescendants bool, asOf time.Time) ([]*firestorepb.Document, error) {
	asOfStr := asOf.UTC().Format(timeLayout)
	var rows *sql.Rows
	var err error
	if allDescendants {
		rows, err = s.rdb.Query(
			`SELECT path, data FROM (
			   SELECT path, data, deleted,
			          ROW_NUMBER() OVER (PARTITION BY path ORDER BY seq DESC) rn
			   FROM document_changes
			   WHERE project=? AND database=? AND collection=? AND change_time <= ?
			 ) WHERE rn=1 AND deleted=0`,
			project, database, collection, asOfStr,
		)
	} else {
		rows, err = s.rdb.Query(
			`SELECT path, data FROM (
			   SELECT path, data, deleted,
			          ROW_NUMBER() OVER (PARTITION BY path ORDER BY seq DESC) rn
			   FROM document_changes
			   WHERE project=? AND database=? AND collection=? AND parent_path=? AND change_time <= ?
			 ) WHERE rn=1 AND deleted=0`,
			project, database, collection, parentPath, asOfStr,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("list docs as of: %w", err)
	}
	defer rows.Close()

	var docs []*firestorepb.Document
	for rows.Next() {
		var path string
		var data []byte
		if err := rows.Scan(&path, &data); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		var doc firestorepb.Document
		if err := proto.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
		docs = append(docs, &doc)
	}
	return docs, rows.Err()
}

// appendChangeExec inserts a row into document_changes and returns the new sequence number.
// changeTime must be the same timestamp used for the document's update_time so that
// GetDocAsOf queries can find the correct snapshot by time.
// It must be called with the same exec that performed the document write so that both
// operations are in the same transaction (or both auto-committed together).
func appendChangeExec(exec dbExec, project, database, path, collection, parentPath string, deleted bool, data []byte, changeTime time.Time) (int64, error) {
	deletedInt := 0
	if deleted {
		deletedInt = 1
	}
	res, err := exec.Exec(
		`INSERT INTO document_changes (project, database, path, collection, parent_path, change_time, deleted, data)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		project, database, path, collection, parentPath,
		changeTime.UTC().Format(timeLayout), deletedInt, data,
	)
	if err != nil {
		return 0, fmt.Errorf("append change: %w", err)
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("last insert id: %w", err)
	}
	return seq, nil
}

// splitChangePath derives (collection, parentPath) from a raw document path like
// "restaurants/id123" or "restaurants/id123/ratings/r1".
func splitChangePath(path string) (collection, parentPath string) {
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return path, ""
	}
	return parts[len(parts)-2], strings.Join(parts[:len(parts)-2], "/")
}

// CurrentSeq returns the highest sequence number in document_changes for the
// given project/database, or 0 if no changes have been recorded yet.
func (s *Store) CurrentSeq(project, database string) (int64, error) {
	var seq int64
	err := s.rdb.QueryRow(
		`SELECT COALESCE(MAX(seq), 0) FROM document_changes WHERE project=? AND database=?`,
		project, database,
	).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("current seq: %w", err)
	}
	return seq, nil
}

// GetDocChangeSince returns the most-recent change log entry for a single named
// document whose seq is strictly greater than sinceSeq, or nil if the document
// has not changed since that position.
func (s *Store) GetDocChangeSince(project, database, path string, sinceSeq int64) (*ChangedDoc, error) {
	var data []byte
	var deleted int
	err := s.rdb.QueryRow(
		`SELECT data, deleted FROM document_changes
		 WHERE project=? AND database=? AND path=? AND seq > ?
		 ORDER BY seq DESC LIMIT 1`,
		project, database, path, sinceSeq,
	).Scan(&data, &deleted)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get doc change since: %w", err)
	}
	name := fmt.Sprintf("projects/%s/databases/%s/documents/%s", project, database, path)
	if deleted != 0 {
		return &ChangedDoc{Name: name, Deleted: true}, nil
	}
	var doc firestorepb.Document
	if err := proto.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &ChangedDoc{Doc: &doc, Name: name}, nil
}

// GetChangesSince returns the latest state of each document that has a change log
// entry with seq strictly greater than sinceSeq, scoped to the given collection.
// If a document was written multiple times, only the most-recent entry is returned.
//   - allDescendants=false: constrain by parent_path AND collection
//   - allDescendants=true:  constrain by collection only (collection-group)
func (s *Store) GetChangesSince(project, database string, sinceSeq int64, parentPath, collection string, allDescendants bool) ([]ChangedDoc, error) {
	// Fetch all change rows in seq order and keep only the last entry per path.
	var rows *sql.Rows
	var err error
	if allDescendants {
		rows, err = s.rdb.Query(
			`SELECT path, data, deleted FROM document_changes
			 WHERE project=? AND database=? AND collection=? AND seq > ?
			 ORDER BY seq ASC`,
			project, database, collection, sinceSeq,
		)
	} else {
		rows, err = s.rdb.Query(
			`SELECT path, data, deleted FROM document_changes
			 WHERE project=? AND database=? AND parent_path=? AND collection=? AND seq > ?
			 ORDER BY seq ASC`,
			project, database, parentPath, collection, sinceSeq,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("get changes since: %w", err)
	}
	defer rows.Close()

	// Deduplicate: last row wins for each path.
	type entry struct {
		data    []byte
		deleted int
	}
	seen := make(map[string]entry)
	var order []string
	for rows.Next() {
		var path string
		var data []byte
		var deleted int
		if err := rows.Scan(&path, &data, &deleted); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		if _, exists := seen[path]; !exists {
			order = append(order, path)
		}
		seen[path] = entry{data, deleted}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var results []ChangedDoc
	for _, path := range order {
		e := seen[path]
		name := fmt.Sprintf("projects/%s/databases/%s/documents/%s", project, database, path)
		if e.deleted != 0 {
			results = append(results, ChangedDoc{Name: name, Deleted: true})
			continue
		}
		var doc firestorepb.Document
		if err := proto.Unmarshal(e.data, &doc); err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
		results = append(results, ChangedDoc{Doc: &doc, Name: name})
	}
	return results, nil
}

// CheckPrecondition validates a write precondition against the current document state.
func (s *Store) CheckPrecondition(project, database, path string, pre *firestorepb.Precondition) error {
	return checkPreconditionExec(s.rdb, project, database, path, pre)
}

// CheckPreconditionTx validates a write precondition within a transaction.
func (s *Store) CheckPreconditionTx(tx *sql.Tx, project, database, path string, pre *firestorepb.Precondition) error {
	return checkPreconditionExec(tx, project, database, path, pre)
}

func checkPreconditionExec(exec dbExec, project, database, path string, pre *firestorepb.Precondition) error {
	switch c := pre.ConditionType.(type) {
	case *firestorepb.Precondition_Exists:
		_, err := getDocExec(exec, project, database, path)
		if c.Exists {
			return err // NotFound if absent
		}
		if err == nil {
			return status.Errorf(codes.FailedPrecondition, "document already exists: %s", path)
		}
		return nil
	case *firestorepb.Precondition_UpdateTime:
		doc, err := getDocExec(exec, project, database, path)
		if err != nil {
			return err
		}
		if !doc.UpdateTime.AsTime().Equal(c.UpdateTime.AsTime()) {
			return status.Errorf(codes.FailedPrecondition, "update_time mismatch for %s", path)
		}
		return nil
	}
	return nil
}

// saveDocExec marshals and persists doc using the given executor (db or tx).
func saveDocExec(exec dbExec, project, database, collection, parentPath, path string, doc *firestorepb.Document) error {
	data, err := proto.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	createStr := doc.CreateTime.AsTime().UTC().Format(timeLayout)
	updateStr := doc.UpdateTime.AsTime().UTC().Format(timeLayout)

	_, err = exec.Exec(`
		INSERT INTO documents (project, database, path, collection, parent_path, data, create_time, update_time, deleted)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
		ON CONFLICT (project, database, path) DO UPDATE SET
			collection   = excluded.collection,
			parent_path  = excluded.parent_path,
			data         = excluded.data,
			create_time  = excluded.create_time,
			update_time  = excluded.update_time,
			deleted      = 0`,
		project, database, path, collection, parentPath, data, createStr, updateStr,
	)
	if err != nil {
		return err
	}
	return indexDocFields(exec, project, database, buildCollectionPath(parentPath, collection), path, doc)
}

// buildCollectionPath returns the relative collection path stored in field_index.collection_path.
// e.g. ("companies/c1", "users") → "companies/c1/users"; ("", "users") → "users"
func buildCollectionPath(parentPath, collection string) string {
	if parentPath == "" {
		return collection
	}
	return parentPath + "/" + collection
}

// fiVals holds the typed value columns for one field_index row.
// At most one field should be set per row.
type fiVals struct {
	str   *string
	i     *int64
	f     *float64
	b     *int64
	null  *int64
	ref   *string
	bytes []byte
}

// fiPendingRow is one row to be batch-inserted into field_index.
type fiPendingRow struct {
	fieldPath string
	vals      fiVals
	inArray   bool
}

// indexDocFields replaces all field_index rows for one document.
// Rows are collected first, then inserted in a single batch statement to minimise
// the number of SQLite write transactions when called outside a Tx.
func indexDocFields(exec dbExec, project, database, collectionPath, docPath string, doc *firestorepb.Document) error {
	if _, err := exec.Exec(
		`DELETE FROM field_index WHERE project=? AND database=? AND doc_path=?`,
		project, database, docPath,
	); err != nil {
		return fmt.Errorf("field_index delete: %w", err)
	}
	var rows []fiPendingRow
	for name, v := range doc.Fields {
		collectValue(name, v, false, &rows)
	}
	if len(rows) == 0 {
		return nil
	}
	return batchInsertFI(exec, project, database, collectionPath, docPath, rows)
}

// collectValue recursively appends field_index rows for a single Firestore value.
// inArray=true means we are already inside an array — nested arrays are skipped
// (Firestore does not index array-of-arrays).
func collectValue(fieldPath string, v *firestorepb.Value, inArray bool, rows *[]fiPendingRow) {
	if v == nil {
		return
	}
	switch vt := v.ValueType.(type) {
	case *firestorepb.Value_NullValue:
		one := int64(1)
		*rows = append(*rows, fiPendingRow{fieldPath, fiVals{null: &one}, inArray})

	case *firestorepb.Value_BooleanValue:
		b := int64(0)
		if vt.BooleanValue {
			b = 1
		}
		*rows = append(*rows, fiPendingRow{fieldPath, fiVals{b: &b}, inArray})

	case *firestorepb.Value_IntegerValue:
		n := vt.IntegerValue
		*rows = append(*rows, fiPendingRow{fieldPath, fiVals{i: &n}, inArray})

	case *firestorepb.Value_DoubleValue:
		f := vt.DoubleValue
		*rows = append(*rows, fiPendingRow{fieldPath, fiVals{f: &f}, inArray})

	case *firestorepb.Value_StringValue:
		s := vt.StringValue
		*rows = append(*rows, fiPendingRow{fieldPath, fiVals{str: &s}, inArray})

	case *firestorepb.Value_BytesValue:
		// Stored in value_bytes (BLOB), keeping it binary-comparable.
		*rows = append(*rows, fiPendingRow{fieldPath, fiVals{bytes: vt.BytesValue}, inArray})

	case *firestorepb.Value_ReferenceValue:
		ref := vt.ReferenceValue
		*rows = append(*rows, fiPendingRow{fieldPath, fiVals{ref: &ref}, inArray})

	case *firestorepb.Value_TimestampValue:
		// Stored as RFC3339Nano in value_string — ISO 8601 sorts lexicographically.
		s := vt.TimestampValue.AsTime().UTC().Format(time.RFC3339Nano)
		*rows = append(*rows, fiPendingRow{fieldPath, fiVals{str: &s}, inArray})

	case *firestorepb.Value_GeoPointValue:
		// No dedicated column. GeoPoint filters fall back to Go-side evaluation.

	case *firestorepb.Value_MapValue:
		if vt.MapValue == nil {
			return
		}
		if !inArray {
			// Sentinel row for the map field itself so that NOT_EQUAL / NOT_IN
			// field-existence checks (EXISTS subquery) find this field_path even
			// when all sub-fields are themselves maps (where no leaf row would
			// carry this path).
			*rows = append(*rows, fiPendingRow{fieldPath, fiVals{}, false})
		}
		for k, child := range vt.MapValue.Fields {
			collectValue(fieldPath+"."+k, child, inArray, rows)
		}

	case *firestorepb.Value_ArrayValue:
		if inArray {
			return // nested arrays are not indexed
		}
		if vt.ArrayValue == nil || len(vt.ArrayValue.Values) == 0 {
			// Empty array: sentinel row with all value columns NULL, in_array=0.
			// Distinct from a null value (value_null=1) but satisfies EXISTS-based
			// checks, so IS_NOT_NULL correctly includes empty arrays.
			*rows = append(*rows, fiPendingRow{fieldPath, fiVals{}, false})
			return
		}
		for _, elem := range vt.ArrayValue.Values {
			collectValue(fieldPath, elem, true, rows)
		}
	}
}

// batchInsertFI inserts all pending rows into field_index using multi-row INSERT
// statements (batched at 500 rows to stay within SQLite's variable limit).
func batchInsertFI(exec dbExec, project, database, collectionPath, docPath string, rows []fiPendingRow) error {
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
		for i, row := range batch {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(placeholder)
			inArrayInt := 0
			if row.inArray {
				inArrayInt = 1
			}
			args = append(args,
				project, database, collectionPath, docPath, row.fieldPath,
				row.vals.str, row.vals.i, row.vals.f, row.vals.b,
				row.vals.null, row.vals.ref, nilIfEmpty(row.vals.bytes),
				inArrayInt,
			)
		}
		if _, err := exec.Exec(sb.String(), args...); err != nil {
			return fmt.Errorf("field_index batch insert: %w", err)
		}
	}
	return nil
}

// nilIfEmpty converts an empty byte slice to nil so SQLite stores NULL instead of an empty BLOB.
func nilIfEmpty(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

// QueryRow executes a query that returns exactly one row and scans the result
// into dest. Returns sql.ErrNoRows if no row is found.
func (s *Store) QueryRow(sqlStr string, args []any, dest ...any) error {
	return s.rdb.QueryRow(sqlStr, args...).Scan(dest...)
}

// QueryDocs executes a parameterized SELECT that returns d.data as its first
// column, unmarshals each row into a Document, and streams it to fn.
// Stops on the first error returned by fn or a scan error.
func (s *Store) QueryDocs(sqlStr string, args []any, fn func(*firestorepb.Document) error) error {
	rows, err := s.rdb.Query(sqlStr, args...)
	if err != nil {
		return fmt.Errorf("query docs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		var doc firestorepb.Document
		if err := proto.Unmarshal(data, &doc); err != nil {
			return fmt.Errorf("unmarshal: %w", err)
		}
		if err := fn(&doc); err != nil {
			return err
		}
	}
	return rows.Err()
}

func cloneDoc(doc *firestorepb.Document) *firestorepb.Document {
	return proto.Clone(doc).(*firestorepb.Document)
}
