package storage

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	latlng "google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/protobuf/proto"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// ── helpers ──────────────────────────────────────────────────────────────────

const (
	dsTestProject   = "ds-proj"
	dsTestDatabase  = "(default)"
	dsTestNamespace = ""
)

func newDsTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func dsMakeKey(kind, name string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: dsTestProject},
		Path: []*datastorepb.Key_PathElement{
			{Kind: kind, IdType: &datastorepb.Key_PathElement_Name{Name: name}},
		},
	}
}

func dsMakeEntity(kind, name string, props map[string]*datastorepb.Value) *datastorepb.Entity {
	return &datastorepb.Entity{Key: dsMakeKey(kind, name), Properties: props}
}

func dsIntVal(n int64) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: n}}
}

func dsStrVal(s string) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: s}}
}

func dsBoolVal(b bool) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_BooleanValue{BooleanValue: b}}
}

func dsNullVal() *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_NullValue{}}
}

func dsGeoVal(lat, lng float64) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_GeoPointValue{
		GeoPointValue: &latlng.LatLng{Latitude: lat, Longitude: lng},
	}}
}

func dsEntityValInline(props map[string]*datastorepb.Value) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{
		EntityValue: &datastorepb.Entity{Properties: props},
	}}
}

// countDsFI returns the number of ds_field_index rows for one entity.
func countDsFI(t *testing.T, db *sql.DB, docPath string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM ds_field_index
		 WHERE project=? AND database=? AND namespace=? AND doc_path=?`,
		dsTestProject, dsTestDatabase, dsTestNamespace, docPath,
	).Scan(&n); err != nil {
		t.Fatalf("countDsFI %s: %v", docPath, err)
	}
	return n
}

// dsFIRowsForField returns all ds_field_index rows for a specific field path.
type dsFIRow struct {
	valueString *string
	valueInt    *int64
	valueDouble *float64
	valueBool   *int64
	valueNull   *int64
	valueBytes  []byte
	valueLat    *float64
	valueLng    *float64
	inArray     int
}

func dsFIRows(t *testing.T, db *sql.DB, docPath, fieldPath string) []dsFIRow {
	t.Helper()
	rows, err := db.Query(
		`SELECT value_string, value_int, value_double, value_bool, value_null, value_bytes, value_lat, value_lng, in_array
		 FROM ds_field_index
		 WHERE project=? AND database=? AND namespace=? AND doc_path=? AND field_path=?`,
		dsTestProject, dsTestDatabase, dsTestNamespace, docPath, fieldPath,
	)
	if err != nil {
		t.Fatalf("dsFIRows query: %v", err)
	}
	defer rows.Close()
	var result []dsFIRow
	for rows.Next() {
		var r dsFIRow
		if err := rows.Scan(&r.valueString, &r.valueInt, &r.valueDouble, &r.valueBool, &r.valueNull, &r.valueBytes, &r.valueLat, &r.valueLng, &r.inArray); err != nil {
			t.Fatalf("dsFIRows scan: %v", err)
		}
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("dsFIRows iter: %v", err)
	}
	return result
}

func dsUpsertEntity(t *testing.T, s *Store, path, kind string, entity *datastorepb.Entity) {
	t.Helper()
	_, _, _, _, _, err := s.DsUpsert(dsTestProject, dsTestDatabase, dsTestNamespace, path, kind, "", entity, 0)
	if err != nil {
		t.Fatalf("DsUpsert %s: %v", path, err)
	}
}

// ── ds_field_index tests ─────────────────────────────────────────────────────

func TestDsFieldIndex_PopulatedOnUpsert(t *testing.T) {
	s := newDsTestStore(t)

	path := "Widget/w1"
	e := dsMakeEntity("Widget", "w1", map[string]*datastorepb.Value{
		"score":   dsIntVal(42),
		"label":   dsStrVal("hello"),
		"active":  dsBoolVal(true),
		"nothing": dsNullVal(),
	})
	dsUpsertEntity(t, s, path, "Widget", e)

	// 4 properties → 4 rows.
	if n := countDsFI(t, s.DB(), path); n != 4 {
		t.Errorf("want 4 ds_field_index rows, got %d", n)
	}

	// Integer field.
	rows := dsFIRows(t, s.DB(), path, "score")
	if len(rows) != 1 || rows[0].valueInt == nil || *rows[0].valueInt != 42 {
		t.Errorf("score row wrong: %+v", rows)
	}

	// String field.
	rows = dsFIRows(t, s.DB(), path, "label")
	if len(rows) != 1 || rows[0].valueString == nil || *rows[0].valueString != "hello" {
		t.Errorf("label row wrong: %+v", rows)
	}

	// Boolean field (stored as 0/1 integer).
	rows = dsFIRows(t, s.DB(), path, "active")
	if len(rows) != 1 || rows[0].valueBool == nil || *rows[0].valueBool != 1 {
		t.Errorf("active row wrong: %+v", rows)
	}

	// Null field.
	rows = dsFIRows(t, s.DB(), path, "nothing")
	if len(rows) != 1 || rows[0].valueNull == nil || *rows[0].valueNull != 1 {
		t.Errorf("nothing (null) row wrong: %+v", rows)
	}
}

func TestDsFieldIndex_ArrayElements(t *testing.T) {
	s := newDsTestStore(t)

	path := "Widget/w1"
	e := dsMakeEntity("Widget", "w1", map[string]*datastorepb.Value{
		"tags": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{
			Values: []*datastorepb.Value{dsStrVal("a"), dsStrVal("b"), dsStrVal("c")},
		}}},
	})
	dsUpsertEntity(t, s, path, "Widget", e)

	// 3 array elements + 1 canonical-bytes sentinel = 4 rows.
	rows := dsFIRows(t, s.DB(), path, "tags")
	if len(rows) != 4 {
		t.Fatalf("want 4 rows (3 elements + sentinel), got %d", len(rows))
	}
	got := map[string]bool{}
	var sentinelCount int
	for _, r := range rows {
		if r.inArray == 0 {
			sentinelCount++
			if r.valueBytes == nil {
				t.Error("array sentinel row missing value_bytes")
			}
		} else {
			if r.valueString != nil {
				got[*r.valueString] = true
			}
		}
	}
	if sentinelCount != 1 {
		t.Errorf("want 1 sentinel row, got %d", sentinelCount)
	}
	for _, want := range []string{"a", "b", "c"} {
		if !got[want] {
			t.Errorf("missing array element %q in ds_field_index", want)
		}
	}
}

func TestDsFieldIndex_ReplacedOnReupsert(t *testing.T) {
	s := newDsTestStore(t)
	path := "Widget/w1"

	dsUpsertEntity(t, s, path, "Widget", dsMakeEntity("Widget", "w1", map[string]*datastorepb.Value{
		"x": dsIntVal(1),
		"y": dsStrVal("old"),
	}))
	if n := countDsFI(t, s.DB(), path); n != 2 {
		t.Fatalf("before re-upsert: want 2 rows, got %d", n)
	}

	// Re-upsert with different fields: y removed, z added.
	dsUpsertEntity(t, s, path, "Widget", dsMakeEntity("Widget", "w1", map[string]*datastorepb.Value{
		"x": dsIntVal(2),
		"z": dsStrVal("new"),
	}))

	if n := countDsFI(t, s.DB(), path); n != 2 {
		t.Errorf("after re-upsert: want 2 rows, got %d", n)
	}
	// Old "y" rows are gone; new "z" row is present.
	if rows := dsFIRows(t, s.DB(), path, "y"); len(rows) != 0 {
		t.Errorf("stale 'y' rows still present after re-upsert: %d", len(rows))
	}
	rows := dsFIRows(t, s.DB(), path, "z")
	if len(rows) != 1 || rows[0].valueString == nil || *rows[0].valueString != "new" {
		t.Errorf("'z' row wrong after re-upsert: %+v", rows)
	}
}

func TestDsFieldIndex_ClearedOnDelete(t *testing.T) {
	s := newDsTestStore(t)
	path := "Widget/w1"

	dsUpsertEntity(t, s, path, "Widget", dsMakeEntity("Widget", "w1", map[string]*datastorepb.Value{
		"x": dsIntVal(1),
		"y": dsStrVal("hello"),
	}))
	if n := countDsFI(t, s.DB(), path); n == 0 {
		t.Fatal("expected ds_field_index rows before delete")
	}

	if err := s.DsDelete(dsTestProject, dsTestDatabase, dsTestNamespace, path); err != nil {
		t.Fatalf("DsDelete: %v", err)
	}

	if n := countDsFI(t, s.DB(), path); n != 0 {
		t.Errorf("want 0 ds_field_index rows after delete, got %d", n)
	}
}

// TestDsFieldIndex_LargeBatch exercises the 500-row chunk boundary in
// dsBatchInsertFI. An entity with 600 properties produces 600 ds_field_index
// rows, which requires two INSERT statements (500 + 100).
func TestDsFieldIndex_LargeBatch(t *testing.T) {
	s := newDsTestStore(t)
	path := "Widget/big"

	props := make(map[string]*datastorepb.Value, 600)
	for i := 0; i < 600; i++ {
		props[fmt.Sprintf("field_%d", i)] = dsIntVal(int64(i))
	}
	dsUpsertEntity(t, s, path, "Widget", dsMakeEntity("Widget", "big", props))

	if n := countDsFI(t, s.DB(), path); n != 600 {
		t.Errorf("want 600 ds_field_index rows (large batch), got %d", n)
	}
}

func TestDsFieldIndex_GeoPoint(t *testing.T) {
	s := newDsTestStore(t)
	path := "Place/p1"
	e := dsMakeEntity("Place", "p1", map[string]*datastorepb.Value{
		"loc": dsGeoVal(37.7749, -122.4194),
	})
	dsUpsertEntity(t, s, path, "Place", e)

	rows := dsFIRows(t, s.DB(), path, "loc")
	if len(rows) != 1 {
		t.Fatalf("geopoint: want 1 row, got %d", len(rows))
	}
	r := rows[0]
	if r.valueLat == nil || *r.valueLat != 37.7749 {
		t.Errorf("geopoint: value_lat = %v, want 37.7749", r.valueLat)
	}
	if r.valueLng == nil || *r.valueLng != -122.4194 {
		t.Errorf("geopoint: value_lng = %v, want -122.4194", r.valueLng)
	}
}

func TestDsFieldIndex_EntityValueCanonicalBytes(t *testing.T) {
	s := newDsTestStore(t)
	path := "Doc/d1"
	embedded := &datastorepb.Entity{
		Properties: map[string]*datastorepb.Value{"name": dsStrVal("foo")},
	}
	e := dsMakeEntity("Doc", "d1", map[string]*datastorepb.Value{
		"emb": dsEntityValInline(map[string]*datastorepb.Value{"name": dsStrVal("foo")}),
	})
	dsUpsertEntity(t, s, path, "Doc", e)

	// The entity value produces a sentinel row (value_bytes = canonical proto bytes)
	// plus a recursive sub-property row (emb.name).
	sentinelRows := dsFIRows(t, s.DB(), path, "emb")
	if len(sentinelRows) != 1 {
		t.Fatalf("entityval sentinel: want 1 sentinel row for 'emb', got %d", len(sentinelRows))
	}
	if sentinelRows[0].valueBytes == nil {
		t.Fatal("entityval sentinel: value_bytes is nil")
	}
	wantBytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(embedded)
	if string(sentinelRows[0].valueBytes) != string(wantBytes) {
		t.Errorf("entityval sentinel: value_bytes mismatch")
	}
	// Sub-property row exists.
	subRows := dsFIRows(t, s.DB(), path, "emb.name")
	if len(subRows) != 1 {
		t.Errorf("entityval sub-prop: want 1 row for 'emb.name', got %d", len(subRows))
	}
}

func TestDsFieldIndex_ArrayCanonicalBytes(t *testing.T) {
	s := newDsTestStore(t)
	path := "Doc/d2"
	arr := &datastorepb.ArrayValue{Values: []*datastorepb.Value{dsStrVal("x"), dsStrVal("y")}}
	e := dsMakeEntity("Doc", "d2", map[string]*datastorepb.Value{
		"tags": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: arr}},
	})
	dsUpsertEntity(t, s, path, "Doc", e)

	rows := dsFIRows(t, s.DB(), path, "tags")
	// Expect: 1 sentinel row (in_array=0, value_bytes set) + 2 element rows (in_array=1).
	if len(rows) != 3 {
		t.Fatalf("array canonical bytes: want 3 rows, got %d", len(rows))
	}
	var sentinel *dsFIRow
	for i := range rows {
		if rows[i].inArray == 0 {
			sentinel = &rows[i]
		}
	}
	if sentinel == nil {
		t.Fatal("array canonical bytes: no sentinel row (in_array=0) found")
	}
	if sentinel.valueBytes == nil {
		t.Fatal("array canonical bytes: sentinel value_bytes is nil")
	}
	wantBytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(arr)
	if string(sentinel.valueBytes) != string(wantBytes) {
		t.Errorf("array canonical bytes: value_bytes mismatch")
	}
}

// ── concurrent r/w tests ─────────────────────────────────────────────────────

// TestConcurrentReadWrite verifies that SQLite WAL mode allows reads on the rdb
// pool to proceed concurrently with an open write transaction on wdb.
// Without WAL mode (or if rdb and wdb share the same connection), the readers
// would block until the write transaction commits — this test catches that.
func TestConcurrentReadWrite(t *testing.T) {
	s := newDsTestStore(t)

	// Seed 10 entities so reads return real data.
	for i := 0; i < 10; i++ {
		path := fmt.Sprintf("Widget/%d", i)
		dsUpsertEntity(t, s, path, "Widget", dsMakeEntity("Widget", fmt.Sprintf("%d", i),
			map[string]*datastorepb.Value{"n": dsIntVal(int64(i))}))
	}

	// Open a write transaction and hold it until explicitly released.
	release := make(chan struct{})
	txOpen := make(chan struct{})
	writeDone := make(chan struct{})
	writeErrCh := make(chan error, 1)

	go func() {
		defer close(writeDone)
		writeErrCh <- s.RunInTx(func(tx *sql.Tx) error {
			close(txOpen)  // signal: write transaction is now open
			<-release      // hold it until test releases
			return nil
		})
	}()
	<-txOpen // write transaction is open; wdb is locked

	// Fire 10 concurrent reads against rdb.  They must not block on the writer.
	const nReaders = 10
	var wg sync.WaitGroup
	readErrs := make([]error, nReaders)
	for i := 0; i < nReaders; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, readErrs[idx] = s.DsQueryKind(dsTestProject, dsTestDatabase, dsTestNamespace, "Widget", "", "", nil)
		}(i)
	}

	// Reads must complete well before the write transaction is released.
	// If they blocked on the writer, they would hang here indefinitely.
	readsDone := make(chan struct{})
	go func() { wg.Wait(); close(readsDone) }()

	select {
	case <-readsDone:
		// Reads completed while the write transaction was still open — WAL is working.
	case <-time.After(5 * time.Second):
		t.Fatal("reads blocked on open write transaction: WAL mode may not be active or r/w pool split is broken")
	}

	// Verify the write transaction is still open (our reads didn't wait for it).
	select {
	case <-writeDone:
		t.Error("write transaction closed before we released it — test is invalid")
	default:
	}

	close(release) // unblock the writer
	<-writeDone
	if err := <-writeErrCh; err != nil {
		t.Errorf("write transaction: %v", err)
	}

	for i, err := range readErrs {
		if err != nil {
			t.Errorf("reader %d: %v", i, err)
		}
	}
}

// TestConcurrentMultipleReaders verifies that the rdb pool allows multiple
// reads to execute truly in parallel (not serialized).
func TestConcurrentMultipleReaders(t *testing.T) {
	s := newDsTestStore(t)

	for i := 0; i < 5; i++ {
		dsUpsertEntity(t, s, fmt.Sprintf("Widget/%d", i), "Widget",
			dsMakeEntity("Widget", fmt.Sprintf("%d", i), map[string]*datastorepb.Value{"n": dsIntVal(int64(i))}))
	}

	const nReaders = 20
	var wg sync.WaitGroup
	errs := make([]error, nReaders)

	for i := 0; i < nReaders; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rows, err := s.DsQueryKind(dsTestProject, dsTestDatabase, dsTestNamespace, "Widget", "", "", nil)
			if err != nil {
				errs[idx] = err
				return
			}
			if len(rows) != 5 {
				errs[idx] = fmt.Errorf("reader %d: want 5 rows, got %d", idx, len(rows))
			}
		}(i)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent readers did not complete within 10s")
	}

	for i, err := range errs {
		if err != nil {
			t.Errorf("reader %d: %v", i, err)
		}
	}
}
