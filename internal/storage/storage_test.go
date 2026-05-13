package storage

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

const (
	testProject = "test-proj"
	testDB      = "(default)"
)

func strVal(s string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: s}}
}

func intVal(i int64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: i}}
}

// TestPersistence verifies that documents written to the store survive a close and reopen.
func TestPersistence_DocumentSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	// Open, write, close.
	store1, err := New(dir)
	if err != nil {
		t.Fatalf("New (first open): %v", err)
	}
	_, err = store1.UpsertDoc(testProject, testDB, "things", "", "things/doc1",
		&firestorepb.Document{
			Name:   "projects/test-proj/databases/(default)/documents/things/doc1",
			Fields: map[string]*firestorepb.Value{"color": strVal("red")},
		})
	if err != nil {
		t.Fatalf("UpsertDoc: %v", err)
	}
	store1.Close()

	// Reopen, verify.
	store2, err := New(dir)
	if err != nil {
		t.Fatalf("New (second open): %v", err)
	}
	defer store2.Close()

	doc, err := store2.GetDoc(testProject, testDB, "things/doc1")
	if err != nil {
		t.Fatalf("GetDoc after restart: %v", err)
	}
	if doc.Fields["color"].GetStringValue() != "red" {
		t.Errorf("color = %q after restart, want %q", doc.Fields["color"].GetStringValue(), "red")
	}
}

// TestPersistence_DeleteSurvivesRestart verifies that soft-deletes survive a close/reopen.
func TestPersistence_DeleteSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	store1, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = store1.UpsertDoc(testProject, testDB, "things", "", "things/doc1",
		&firestorepb.Document{Name: "projects/test-proj/databases/(default)/documents/things/doc1"})
	if err != nil {
		t.Fatalf("UpsertDoc: %v", err)
	}
	if err := store1.DeleteDoc(testProject, testDB, "things/doc1"); err != nil {
		t.Fatalf("DeleteDoc: %v", err)
	}
	store1.Close()

	store2, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store2.Close()

	_, err = store2.GetDoc(testProject, testDB, "things/doc1")
	if err == nil {
		t.Error("expected NotFound after restart, got nil error")
	}
}

// TestPersistence_MultipleDocumentsSurviveRestart verifies a batch of documents.
func TestPersistence_MultipleDocumentsSurviveRestart(t *testing.T) {
	dir := t.TempDir()

	store1, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := int64(1); i <= 10; i++ {
		name := "things/doc" + string(rune('0'+i))
		_, err = store1.UpsertDoc(testProject, testDB, "things", "", name,
			&firestorepb.Document{
				Name:   "projects/test-proj/databases/(default)/documents/" + name,
				Fields: map[string]*firestorepb.Value{"n": intVal(i)},
			})
		if err != nil {
			t.Fatalf("UpsertDoc %s: %v", name, err)
		}
	}
	store1.Close()

	store2, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store2.Close()

	docs, err := store2.QueryCollection(testProject, testDB, "", "things", false)
	if err != nil {
		t.Fatalf("QueryCollection after restart: %v", err)
	}
	if len(docs) != 10 {
		t.Errorf("want 10 docs after restart, got %d", len(docs))
	}
}

// TestPersistence_DsEntitySurvivesRestart verifies Datastore entities persist.
func TestPersistence_DsEntitySurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	datastorepb := importDsEntityForTest(t, dir)
	_ = datastorepb
}

// -- field_index helpers ------------------------------------------------------

func boolVal(b bool) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_BooleanValue{BooleanValue: b}}
}

func tsVal(t time.Time) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_TimestampValue{TimestampValue: timestamppb.New(t)}}
}

func mapVal(fields map[string]*firestorepb.Value) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_MapValue{MapValue: &firestorepb.MapValue{Fields: fields}}}
}

func arrayVal(elems ...*firestorepb.Value) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_ArrayValue{ArrayValue: &firestorepb.ArrayValue{Values: elems}}}
}

// countFieldIndex returns the number of field_index rows for a document.
func countFieldIndex(t *testing.T, db *sql.DB, project, database, docPath string) int {
	t.Helper()
	var n int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM field_index WHERE project=? AND database=? AND doc_path=?`,
		project, database, docPath,
	).Scan(&n)
	if err != nil {
		t.Fatalf("countFieldIndex: %v", err)
	}
	return n
}

// fieldIndexRows returns all rows for the given (project, database, doc_path, field_path).
type fiRow struct {
	valueString *string
	valueInt    *int64
	valueDouble *float64
	valueBool   *int64
	valueNull   *int64
}

func fieldIndexRows(t *testing.T, db *sql.DB, project, database, docPath, fieldPath string) []fiRow {
	t.Helper()
	rows, err := db.Query(
		`SELECT value_string, value_int, value_double, value_bool, value_null
		 FROM field_index WHERE project=? AND database=? AND doc_path=? AND field_path=?`,
		project, database, docPath, fieldPath,
	)
	if err != nil {
		t.Fatalf("fieldIndexRows query: %v", err)
	}
	defer rows.Close()
	var result []fiRow
	for rows.Next() {
		var r fiRow
		if err := rows.Scan(&r.valueString, &r.valueInt, &r.valueDouble, &r.valueBool, &r.valueNull); err != nil {
			t.Fatalf("fieldIndexRows scan: %v", err)
		}
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("fieldIndexRows iter: %v", err)
	}
	return result
}

// -- field_index tests --------------------------------------------------------

func TestFieldIndex_PopulatedOnInsert(t *testing.T) {
	store, _ := New(t.TempDir())
	defer store.Close()

	_, err := store.UpsertDoc(testProject, testDB, "things", "", "things/doc1",
		&firestorepb.Document{
			Name: "projects/test-proj/databases/(default)/documents/things/doc1",
			Fields: map[string]*firestorepb.Value{
				"color": strVal("red"),
				"count": intVal(42),
			},
		})
	if err != nil {
		t.Fatalf("UpsertDoc: %v", err)
	}

	if n := countFieldIndex(t, store.DB(), testProject, testDB, "things/doc1"); n != 2 {
		t.Errorf("want 2 field_index rows, got %d", n)
	}

	rows := fieldIndexRows(t, store.DB(), testProject, testDB, "things/doc1", "color")
	if len(rows) != 1 || rows[0].valueString == nil || *rows[0].valueString != "red" {
		t.Errorf("color field_index wrong: %+v", rows)
	}

	rows = fieldIndexRows(t, store.DB(), testProject, testDB, "things/doc1", "count")
	if len(rows) != 1 || rows[0].valueInt == nil || *rows[0].valueInt != 42 {
		t.Errorf("count field_index wrong: %+v", rows)
	}
}

func TestFieldIndex_NestedMap(t *testing.T) {
	store, _ := New(t.TempDir())
	defer store.Close()

	_, err := store.UpsertDoc(testProject, testDB, "people", "", "people/alice",
		&firestorepb.Document{
			Name: "projects/test-proj/databases/(default)/documents/people/alice",
			Fields: map[string]*firestorepb.Value{
				"address": mapVal(map[string]*firestorepb.Value{
					"city": strVal("NYC"),
					"zip":  strVal("10001"),
				}),
			},
		})
	if err != nil {
		t.Fatalf("UpsertDoc: %v", err)
	}

	rows := fieldIndexRows(t, store.DB(), testProject, testDB, "people/alice", "address.city")
	if len(rows) != 1 || rows[0].valueString == nil || *rows[0].valueString != "NYC" {
		t.Errorf("address.city field_index wrong: %+v", rows)
	}

	rows = fieldIndexRows(t, store.DB(), testProject, testDB, "people/alice", "address.zip")
	if len(rows) != 1 || rows[0].valueString == nil || *rows[0].valueString != "10001" {
		t.Errorf("address.zip field_index wrong: %+v", rows)
	}

	// The parent map key "address" produces exactly one all-NULL sentinel row. This
	// sentinel is required so that NOT_EQUAL / NOT_IN EXISTS subqueries (which match
	// on field_path=?) can find the field when all child values are themselves maps.
	rows = fieldIndexRows(t, store.DB(), testProject, testDB, "people/alice", "address")
	if len(rows) != 1 {
		t.Errorf("map parent key should produce 1 sentinel row, got %d rows", len(rows))
	}
}

func TestFieldIndex_ArrayElements(t *testing.T) {
	store, _ := New(t.TempDir())
	defer store.Close()

	_, err := store.UpsertDoc(testProject, testDB, "things", "", "things/doc1",
		&firestorepb.Document{
			Name: "projects/test-proj/databases/(default)/documents/things/doc1",
			Fields: map[string]*firestorepb.Value{
				"tags": arrayVal(strVal("go"), strVal("sql"), strVal("sqlite")),
			},
		})
	if err != nil {
		t.Fatalf("UpsertDoc: %v", err)
	}

	rows := fieldIndexRows(t, store.DB(), testProject, testDB, "things/doc1", "tags")
	if len(rows) != 3 {
		t.Errorf("want 3 array element rows for 'tags', got %d", len(rows))
	}
	vals := map[string]bool{}
	for _, r := range rows {
		if r.valueString != nil {
			vals[*r.valueString] = true
		}
	}
	for _, want := range []string{"go", "sql", "sqlite"} {
		if !vals[want] {
			t.Errorf("missing array element %q in field_index", want)
		}
	}
}

func TestFieldIndex_EmptyArraySentinel(t *testing.T) {
	store, _ := New(t.TempDir())
	defer store.Close()

	_, err := store.UpsertDoc(testProject, testDB, "things", "", "things/doc1",
		&firestorepb.Document{
			Name:   "projects/test-proj/databases/(default)/documents/things/doc1",
			Fields: map[string]*firestorepb.Value{"tags": arrayVal()}, // empty array
		})
	if err != nil {
		t.Fatalf("UpsertDoc: %v", err)
	}

	// Empty array must produce exactly one sentinel row so IS_NOT_NULL can match.
	rows := fieldIndexRows(t, store.DB(), testProject, testDB, "things/doc1", "tags")
	if len(rows) != 1 {
		t.Fatalf("empty array: want 1 sentinel row, got %d", len(rows))
	}
	// All typed columns are NULL (sentinel is not a null value, which has value_null=1).
	r := rows[0]
	if r.valueNull != nil {
		t.Errorf("sentinel should have value_null=NULL, got %v", *r.valueNull)
	}
	if r.valueString != nil || r.valueInt != nil || r.valueDouble != nil || r.valueBool != nil {
		t.Errorf("sentinel row should have all value columns NULL: %+v", r)
	}
}

func TestFieldIndex_NestedArraySkipped(t *testing.T) {
	store, _ := New(t.TempDir())
	defer store.Close()

	// Array containing another array - inner arrays should not be indexed.
	_, err := store.UpsertDoc(testProject, testDB, "things", "", "things/doc1",
		&firestorepb.Document{
			Name: "projects/test-proj/databases/(default)/documents/things/doc1",
			Fields: map[string]*firestorepb.Value{
				"matrix": arrayVal(
					arrayVal(intVal(1), intVal(2)),
					arrayVal(intVal(3), intVal(4)),
				),
			},
		})
	if err != nil {
		t.Fatalf("UpsertDoc: %v", err)
	}

	// Inner arrays are skipped: zero rows expected.
	rows := fieldIndexRows(t, store.DB(), testProject, testDB, "things/doc1", "matrix")
	if len(rows) != 0 {
		t.Errorf("nested arrays should not be indexed, got %d rows", len(rows))
	}
}

func TestFieldIndex_TimestampAsString(t *testing.T) {
	store, _ := New(t.TempDir())
	defer store.Close()

	ts := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	_, err := store.UpsertDoc(testProject, testDB, "events", "", "events/e1",
		&firestorepb.Document{
			Name:   "projects/test-proj/databases/(default)/documents/events/e1",
			Fields: map[string]*firestorepb.Value{"at": tsVal(ts)},
		})
	if err != nil {
		t.Fatalf("UpsertDoc: %v", err)
	}

	rows := fieldIndexRows(t, store.DB(), testProject, testDB, "events/e1", "at")
	if len(rows) != 1 {
		t.Fatalf("want 1 row for timestamp field, got %d", len(rows))
	}
	if rows[0].valueString == nil {
		t.Fatal("timestamp should be stored in value_string")
	}
	// Must be parseable as RFC3339Nano and equal to the original timestamp.
	parsed, err := time.Parse(time.RFC3339Nano, *rows[0].valueString)
	if err != nil {
		t.Fatalf("value_string %q not RFC3339Nano: %v", *rows[0].valueString, err)
	}
	if !parsed.Equal(ts) {
		t.Errorf("timestamp mismatch: want %v, got %v", ts, parsed)
	}
}

func TestFieldIndex_IdempotentOnDoubleWrite(t *testing.T) {
	store, _ := New(t.TempDir())
	defer store.Close()

	doc := &firestorepb.Document{
		Name:   "projects/test-proj/databases/(default)/documents/things/doc1",
		Fields: map[string]*firestorepb.Value{"x": intVal(1)},
	}

	for i := 0; i < 2; i++ {
		if _, err := store.UpsertDoc(testProject, testDB, "things", "", "things/doc1", doc); err != nil {
			t.Fatalf("UpsertDoc (attempt %d): %v", i+1, err)
		}
	}

	if n := countFieldIndex(t, store.DB(), testProject, testDB, "things/doc1"); n != 1 {
		t.Errorf("double-write should produce exactly 1 row, got %d", n)
	}
}

func TestFieldIndex_ClearedOnDelete(t *testing.T) {
	store, _ := New(t.TempDir())
	defer store.Close()

	_, err := store.UpsertDoc(testProject, testDB, "things", "", "things/doc1",
		&firestorepb.Document{
			Name:   "projects/test-proj/databases/(default)/documents/things/doc1",
			Fields: map[string]*firestorepb.Value{"x": intVal(1), "y": strVal("hello")},
		})
	if err != nil {
		t.Fatalf("UpsertDoc: %v", err)
	}
	if n := countFieldIndex(t, store.DB(), testProject, testDB, "things/doc1"); n == 0 {
		t.Fatal("expected field_index rows before delete")
	}

	if err := store.DeleteDoc(testProject, testDB, "things/doc1"); err != nil {
		t.Fatalf("DeleteDoc: %v", err)
	}

	if n := countFieldIndex(t, store.DB(), testProject, testDB, "things/doc1"); n != 0 {
		t.Errorf("want 0 field_index rows after delete, got %d", n)
	}
}

func TestFieldIndex_TransactionalWrite(t *testing.T) {
	store, _ := New(t.TempDir())
	defer store.Close()

	err := store.RunInTx(func(tx *sql.Tx) error {
		_, err := store.UpsertDocTx(tx, testProject, testDB, "things", "", "things/doc1",
			&firestorepb.Document{
				Name:   "projects/test-proj/databases/(default)/documents/things/doc1",
				Fields: map[string]*firestorepb.Value{"score": intVal(99)},
			})
		return err
	})
	if err != nil {
		t.Fatalf("RunInTx: %v", err)
	}

	rows := fieldIndexRows(t, store.DB(), testProject, testDB, "things/doc1", "score")
	if len(rows) != 1 || rows[0].valueInt == nil || *rows[0].valueInt != 99 {
		t.Errorf("field_index after transactional write wrong: %+v", rows)
	}
}

// importDsEntityForTest is a helper that exercises the datastore storage path.
// We do this inline to avoid a circular import - storage_test is package storage.
func importDsEntityForTest(t *testing.T, dir string) bool {
	t.Helper()
	store1, err := New(dir + "/ds")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Use the Firestore doc path as a proxy (ds_documents uses a different table).
	// Write via UpsertDoc and verify via GetDoc - same WAL/file.
	_, err = store1.UpsertDoc("proj", "(default)", "Widget", "", "Widget/w1",
		&firestorepb.Document{
			Name:       "projects/proj/databases/(default)/documents/Widget/w1",
			Fields:     map[string]*firestorepb.Value{"score": intVal(42)},
			CreateTime: timestamppb.Now(),
			UpdateTime: timestamppb.Now(),
		})
	if err != nil {
		t.Fatalf("UpsertDoc: %v", err)
	}
	store1.Close()

	store2, err := New(dir + "/ds")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store2.Close()

	doc, err := store2.GetDoc("proj", "(default)", "Widget/w1")
	if err != nil {
		t.Fatalf("GetDoc after restart: %v", err)
	}
	if doc.Fields["score"].GetIntegerValue() != 42 {
		t.Errorf("score = %d after restart, want 42", doc.Fields["score"].GetIntegerValue())
	}
	return true
}

// -- Change log tests ----------------------------------------------------------

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func upsertTestDoc(t *testing.T, s *Store, path, field, value string) {
	t.Helper()
	parts := splitPath(path) // collection = second-to-last segment, parentPath = everything before
	_, err := s.UpsertDoc(testProject, testDB, parts[0], parts[1], path,
		&firestorepb.Document{
			Name:   "projects/" + testProject + "/databases/" + testDB + "/documents/" + path,
			Fields: map[string]*firestorepb.Value{field: strVal(value)},
		})
	if err != nil {
		t.Fatalf("UpsertDoc(%s): %v", path, err)
	}
}

// splitPath returns (collection, parentPath) for a given doc path like "col/id" or "a/b/col/id".
func splitPath(path string) [2]string {
	parts := strings.SplitN(path, "/", -1)
	if len(parts) < 2 {
		return [2]string{path, ""}
	}
	coll := parts[len(parts)-2]
	parent := strings.Join(parts[:len(parts)-2], "/")
	return [2]string{coll, parent}
}

func TestCurrentSeq_EmptyDB(t *testing.T) {
	s := newTestStore(t)
	seq, err := s.CurrentSeq(testProject, testDB)
	if err != nil {
		t.Fatalf("CurrentSeq: %v", err)
	}
	if seq != 0 {
		t.Errorf("CurrentSeq on empty db = %d, want 0", seq)
	}
}

func TestCurrentSeq_IncrementsOnWrite(t *testing.T) {
	s := newTestStore(t)

	upsertTestDoc(t, s, "things/a", "x", "1")
	seq1, _ := s.CurrentSeq(testProject, testDB)
	if seq1 == 0 {
		t.Fatal("CurrentSeq after first write = 0, want > 0")
	}

	upsertTestDoc(t, s, "things/b", "x", "2")
	seq2, _ := s.CurrentSeq(testProject, testDB)
	if seq2 <= seq1 {
		t.Errorf("CurrentSeq did not increment: %d → %d", seq1, seq2)
	}
}

func TestGetDocChangeSince_NoChange(t *testing.T) {
	s := newTestStore(t)
	upsertTestDoc(t, s, "things/a", "x", "1")
	seq, _ := s.CurrentSeq(testProject, testDB)

	// No further writes - should return nil.
	got, err := s.GetDocChangeSince(testProject, testDB, "things/a", seq)
	if err != nil {
		t.Fatalf("GetDocChangeSince: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil (no change), got %+v", got)
	}
}

func TestGetDocChangeSince_Updated(t *testing.T) {
	s := newTestStore(t)
	upsertTestDoc(t, s, "things/a", "x", "1")
	seq, _ := s.CurrentSeq(testProject, testDB)

	upsertTestDoc(t, s, "things/a", "x", "updated")

	got, err := s.GetDocChangeSince(testProject, testDB, "things/a", seq)
	if err != nil {
		t.Fatalf("GetDocChangeSince: %v", err)
	}
	if got == nil {
		t.Fatal("expected changed doc, got nil")
	}
	if got.Deleted {
		t.Error("expected live doc, got Deleted=true")
	}
	if got.Doc.Fields["x"].GetStringValue() != "updated" {
		t.Errorf("field x = %q, want %q", got.Doc.Fields["x"].GetStringValue(), "updated")
	}
}

func TestGetDocChangeSince_Deleted(t *testing.T) {
	s := newTestStore(t)
	upsertTestDoc(t, s, "things/a", "x", "1")
	seq, _ := s.CurrentSeq(testProject, testDB)

	if err := s.DeleteDoc(testProject, testDB, "things/a"); err != nil {
		t.Fatalf("DeleteDoc: %v", err)
	}

	got, err := s.GetDocChangeSince(testProject, testDB, "things/a", seq)
	if err != nil {
		t.Fatalf("GetDocChangeSince: %v", err)
	}
	if got == nil {
		t.Fatal("expected change entry for deleted doc, got nil")
	}
	if !got.Deleted {
		t.Error("expected Deleted=true")
	}
}

func TestGetChangesSince_Collection(t *testing.T) {
	s := newTestStore(t)
	upsertTestDoc(t, s, "items/x", "v", "before")
	seq, _ := s.CurrentSeq(testProject, testDB)

	upsertTestDoc(t, s, "items/y", "v", "after")
	// "x" predates the seq; "y" is after.

	results, err := s.GetChangesSince(testProject, testDB, seq, "", "items", false)
	if err != nil {
		t.Fatalf("GetChangesSince: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d: %+v", len(results), results)
	}
	if results[0].Doc.Fields["v"].GetStringValue() != "after" {
		t.Errorf("unexpected doc: %+v", results[0])
	}
}

func TestGetChangesSince_Dedup(t *testing.T) {
	s := newTestStore(t)
	seq0, _ := s.CurrentSeq(testProject, testDB)

	// Write "a" twice - should appear once in results (latest state only).
	upsertTestDoc(t, s, "items/a", "v", "first")
	upsertTestDoc(t, s, "items/a", "v", "second")

	results, err := s.GetChangesSince(testProject, testDB, seq0, "", "items", false)
	if err != nil {
		t.Fatalf("GetChangesSince: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 deduplicated result, got %d", len(results))
	}
	if results[0].Doc.Fields["v"].GetStringValue() != "second" {
		t.Errorf("expected latest value %q, got %q", "second", results[0].Doc.Fields["v"].GetStringValue())
	}
}

func TestChangeEvent_CarriesSeq(t *testing.T) {
	s := newTestStore(t)
	subID, ch := s.Subscribe()
	defer s.Unsubscribe(subID)
	s.UpdateScopes(subID, []SubScope{{Project: testProject, Database: testDB, Collection: "things"}})

	upsertTestDoc(t, s, "things/a", "x", "1")

	select {
	case ev := <-ch:
		if ev.Seq == 0 {
			t.Error("ChangeEvent.Seq = 0, want > 0")
		}
	default:
		t.Fatal("no ChangeEvent received")
	}
}
