package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
	"github.com/magnus-rattlehead/hearthstore/internal/propertypath"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func FuzzOrderedQueryScalarFraming(f *testing.F) {
	f.Add([]byte("a\x00雪"), int64(-1))
	f.Add([]byte{}, int64(0))
	f.Fuzz(func(t *testing.T, data []byte, n int64) {
		if len(data) > 1500 {
			return
		}
		name := strings.ToValidUTF8(string(data), "?")
		key := &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: testProject}, Path: []*datastorepb.Key_PathElement{{Kind: "Scalar", IdType: &datastorepb.Key_PathElement_Id{Id: n}}, {Kind: "Leaf", IdType: &datastorepb.Key_PathElement_Name{Name: name}}}}
		for _, value := range []*datastorepb.Value{
			{ValueType: &datastorepb.Value_NullValue{}},
			{ValueType: &datastorepb.Value_BooleanValue{BooleanValue: n != 0}},
			{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: n}},
			{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: math.Float64frombits(uint64(n))}},
			{ValueType: &datastorepb.Value_StringValue{StringValue: name}},
			{ValueType: &datastorepb.Value_BlobValue{BlobValue: data}},
			{ValueType: &datastorepb.Value_KeyValue{KeyValue: key}},
			{ValueType: &datastorepb.Value_GeoPointValue{GeoPointValue: &latlng.LatLng{Latitude: 1, Longitude: -1}}},
			{ValueType: &datastorepb.Value_TimestampValue{TimestampValue: &timestamppb.Timestamp{Seconds: n % 1_000_000}}},
		} {
			raw, ok := OrderedQueryValue(value, false)
			if !ok || !ValidOrderedQueryValue(raw) {
				t.Fatalf("rejected emitted scalar %x", raw)
			}
			if ValidOrderedQueryValue(append(bytes.Clone(raw), 0)) || ValidOrderedQueryValue(raw[:len(raw)-1]) {
				t.Fatalf("accepted broken scalar frame %x", raw)
			}
		}
	})
}

func TestReviewCoveringPagesBoundRetainedBytes(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	integer := func(n int64) *datastorepb.Value {
		return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: n}}
	}
	const count = 800
	for i := range count {
		name := fmt.Sprintf("%04d", i) + strings.Repeat("k", 1000)
		entity := &datastorepb.Entity{Key: &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: testProject, DatabaseId: testDB}, Path: []*datastorepb.Key_PathElement{{Kind: "CoverPage", IdType: &datastorepb.Key_PathElement_Name{Name: name}}}}, Properties: map[string]*datastorepb.Value{
			"x": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: []*datastorepb.Value{integer(0), integer(0), integer(1)}}}},
			"y": integer(7),
		}}
		if _, err := store.DsInsert(EntityWrite{Project: testProject, Database: testDB, Path: keycodec.Path(entity.Key.Path), Kind: "CoverPage", Entity: entity}); err != nil {
			t.Fatal(err)
		}
	}
	idx, _, err := store.EnsureDsCompositeIndex(context.Background(), DsCompositeIndex{Project: testProject, Kind: "CoverPage", Properties: []DsIndexProperty{{Name: "x"}, {Name: "y"}}}, true)
	if err != nil {
		t.Fatal(err)
	}
	readAt := store.ReadTime()
	t.Run("bounded probes", func(t *testing.T) {
		filter := func(name string, op datastorepb.PropertyFilter_Operator, n int64) *datastorepb.Filter {
			return &datastorepb.Filter{FilterType: &datastorepb.Filter_PropertyFilter{PropertyFilter: &datastorepb.PropertyFilter{Property: &datastorepb.PropertyReference{Name: name}, Op: op, Value: integer(n)}}}
		}
		// Exclusive lower bounds and secondary rejects must charge planning
		// effort even though neither yields a covering row.
		for _, composite := range []bool{false, true} {
			ctx, work := WithQueryWork(context.Background(), 1)
			var probe ProbeResult
			var err error
			if composite {
				f := &datastorepb.Filter{FilterType: &datastorepb.Filter_CompositeFilter{CompositeFilter: &datastorepb.CompositeFilter{Op: datastorepb.CompositeFilter_AND, Filters: []*datastorepb.Filter{filter("x", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, 0), filter("y", datastorepb.PropertyFilter_GREATER_THAN, 7)}}}}
				probe, err = store.DsProbeCompositeAsOf(ctx, CompositeProbe{
					ReadTime:  readAt,
					Project:   testProject,
					Database:  testDB,
					IndexID:   idx.ID,
					Filter:    f,
					Allowance: 10,
				})
			} else {
				probe, err = store.DsProbeBuiltinAsOf(ctx, BuiltinProbe{
					ReadTime:  readAt,
					Project:   testProject,
					Database:  testDB,
					Kind:      "CoverPage",
					Property:  "x",
					Filter:    filter("x", datastorepb.PropertyFilter_GREATER_THAN, 0),
					Allowance: 10,
				})
			}
			if err != nil || probe.Complete || probe.Rows != nil || probe.Visited != 10 || work.Snapshot()[WorkIndexEntries] != 10 {
				t.Fatalf("probe rows=%d visited=%d complete=%v err=%v work=%v", len(probe.Rows), probe.Visited, probe.Complete, err, work.Snapshot())
			}
		}
		probe, err := store.DsProbeBuiltinAsOf(context.Background(), BuiltinProbe{
			ReadTime:  readAt,
			Project:   testProject,
			Database:  testDB,
			Kind:      "CoverPage",
			Property:  "x",
			Allowance: 10000,
		})
		if err != nil || probe.Complete || probe.Rows != nil {
			t.Fatalf("byte-bounded probe rows=%d complete=%v err=%v", len(probe.Rows), probe.Complete, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		probe, err = store.DsProbeBuiltinAsOf(ctx, BuiltinProbe{
			ReadTime:  readAt,
			Project:   testProject,
			Database:  testDB,
			Kind:      "CoverPage",
			Property:  "x",
			Allowance: 10,
		})
		if !errors.Is(err, context.Canceled) || probe.Rows != nil || probe.Complete {
			t.Fatalf("cancelled probe: rows=%d complete=%v err=%v", len(probe.Rows), probe.Complete, err)
		}
	})
	for _, mode := range []string{"builtin", "composite", "union"} {
		t.Run(mode, func(t *testing.T) {
			var accept func(*datastorepb.Entity) bool
			query := func(ctx context.Context, cursor *CursorPayload) (QueryPage, error) {
				if mode == "union" {
					rows, scanned, more, _, err := store.DsQueryEqualityUnionAsOf(ctx, readAt, testProject, testDB, "", "CoverPage", []*datastorepb.PropertyFilter{
						{Property: &datastorepb.PropertyReference{Name: "x"}, Op: datastorepb.PropertyFilter_EQUAL, Value: integer(0)},
						{Property: &datastorepb.PropertyReference{Name: "y"}, Op: datastorepb.PropertyFilter_EQUAL, Value: integer(7)},
					}, cursor, nil, 0, true, accept)
					return QueryPage{Rows: rows, Scanned: scanned, More: more}, err
				}
				if mode == "composite" {
					return store.DsQueryComposite(ctx, CompositeQuery{ReadTime: &readAt, Project: testProject, Database: testDB, IndexID: idx.ID, Cursor: cursor, Projection: true})
				}
				return store.DsQueryBuiltin(ctx, BuiltinQuery{ReadTime: &readAt, Project: testProject, Database: testDB, Kind: "CoverPage", Property: "x", Cursor: cursor, Projection: true})
			}
			var cursor *CursorPayload
			var previous []byte
			total, pages, sum := 0, 0, int64(0)
			for {
				page, err := query(context.Background(), cursor)
				rows, more := page.Rows, page.More
				if err != nil {
					t.Fatal(err)
				}
				retained := 0
				for _, row := range rows {
					if retained > maxMaterializedQueryBytes {
						t.Fatalf("retained bytes %d exceed page target before final row", retained)
					}
					retained += proto.Size(row.Entity) + len(row.Path) + len(row.IndexKey)
					if previous != nil && bytes.Compare(previous, row.IndexKey) >= 0 {
						t.Fatal("duplicate or out-of-order covering tuple")
					}
					previous = row.IndexKey
					sum += row.Entity.Properties["x"].GetIntegerValue()
					total++
				}
				pages++
				if !more {
					break
				}
				if len(rows) == 0 {
					t.Fatal("non-progressing covering page")
				}
				cursor = &CursorPayload{K: rows[len(rows)-1].IndexKey}
				if mode == "union" {
					cursor.I, cursor.G, cursor.P = "builtin:__key__", 1, rows[len(rows)-1].Path
				}
			}
			wantTotal, wantSum := 2*count, int64(count)
			if mode == "union" {
				wantTotal, wantSum = count, 0
			}
			if total != wantTotal || sum != wantSum || pages < 2 {
				t.Fatalf("total=%d sum=%d pages=%d", total, sum, pages)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := query(ctx, nil); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled read: %v", err)
			}
			if page, err := query(context.Background(), nil); err != nil || len(page.Rows) == 0 {
				t.Fatalf("read after cancellation: rows=%d err=%v", len(page.Rows), err)
			}
			if mode == "union" {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				seen := 0
				accept = func(*datastorepb.Entity) bool {
					seen++
					if seen == 10 {
						cancel()
					}
					return true
				}
				if page, err := query(ctx, nil); !errors.Is(err, context.Canceled) || page.Rows != nil {
					t.Fatalf("mid-merge cancellation returned rows=%d err=%v", len(page.Rows), err)
				}
				accept = nil
				ctx, work := WithQueryWork(context.Background(), 1)
				if page, err := query(ctx, nil); err != nil || len(page.Rows) == 0 {
					t.Fatalf("read after mid-merge cancellation: rows=%d err=%v", len(page.Rows), err)
				}
				if work.Snapshot()[WorkScratchWriteBytes] != 0 || work.Snapshot()[WorkScratchReadBytes] != 0 {
					t.Fatal("streaming union used scratch sorting")
				}
			}
			if mode == "composite" {
				if err := store.DeleteDsCompositeIndex(context.Background(), testProject, idx.ID); err != nil {
					t.Fatal(err)
				}
				if _, _, err := store.EnsureDsCompositeIndex(context.Background(), idx, true); err != nil {
					t.Fatal(err)
				}
				if _, err := query(context.Background(), &CursorPayload{G: idx.ActiveGeneration}); err == nil {
					t.Fatal("replaced index accepted at the pinned snapshot")
				}
			}
		})
	}
}

func TestReviewQueryWorkRetainsProgress(t *testing.T) {
	for _, quantum := range []uint64{1, 7} {
		ctx, work := WithQueryWork(context.Background(), quantum)
		nested, inherited := WithQueryWork(ctx, 99)
		if inherited != work {
			t.Fatal("nested page reset query accounting")
		}
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 21 {
					work.Charge(WorkComparisons, 2)
					if err := work.Checkpoint(nested); err != nil {
						t.Error(err)
					}
				}
			}()
		}
		wg.Wait()
		if got := work.Snapshot(); got[WorkAttempts] != 84 || got[WorkComparisons] != 168 || got[WorkYields] != 84/quantum {
			t.Fatalf("quantum=%d counts=%v", quantum, got)
		}
		work.Charge(WorkDecodedBytes, math.MaxUint64)
		work.Charge(WorkDecodedBytes, 1)
		if got := work.Snapshot()[WorkDecodedBytes]; got != math.MaxUint64 {
			t.Fatalf("counter wrapped: %d", got)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if err := work.Checkpoint(canceled); err != context.Canceled {
			t.Fatalf("cancellation=%v", err)
		}
		if err := work.Checkpoint(ctx); err != nil {
			t.Fatalf("canceled waiter poisoned shared accounting: %v", err)
		}
	}
}

type cancelAfterWorkComparison struct {
	context.Context
	work *QueryWork
}

func (c cancelAfterWorkComparison) Err() error {
	if c.work.Snapshot()[WorkComparisons] > 0 {
		return context.Canceled
	}
	return c.Context.Err()
}

func TestReviewScratchSortHonorsCancellation(t *testing.T) {
	ctx, work := WithQueryWork(context.Background(), 1)
	ctx = cancelAfterWorkComparison{ctx, work}
	acc, err := newPathAccumulator(ctx, t.TempDir(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer acc.Close()
	for i := 127; i >= 0; i-- {
		if err := acc.Add(fmt.Sprintf("%04d", i)); err != nil {
			t.Fatal(err)
		}
	}
	err = acc.ForEach(func(string) error { t.Fatal("emitted canceled sort"); return nil })
	if err != context.Canceled || work.Snapshot()[WorkComparisons] != 1 {
		t.Fatalf("sort err=%v work=%v", err, work.Snapshot())
	}
}

func TestReviewIndexVisitor(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	for i := range 3 {
		_, _, err := store.EnsureDsCompositeIndex(context.Background(), DsCompositeIndex{Project: "visitor-project", Kind: fmt.Sprintf("Visitor%d", i), Properties: []DsIndexProperty{{Name: "x"}, {Name: "y"}}}, true)
		if err != nil {
			t.Fatal(err)
		}
	}
	visited := 0
	exhausted, err := store.VisitDsCompositeIndexes(context.Background(), "visitor-project", func(index DsCompositeIndex) bool {
		visited++
		if index.State != DsIndexReady || index.ReadySince == 0 {
			t.Fatalf("unusable index metadata: %+v", index)
		}
		return visited < 2
	})
	if err != nil || exhausted || visited != 2 {
		t.Fatalf("visit exhausted=%t visited=%d err=%v", exhausted, visited, err)
	}
	visited = 0
	exhausted, err = store.VisitDsCompositeIndexes(context.Background(), "visitor-project", func(DsCompositeIndex) bool { visited++; return true })
	if err != nil || !exhausted || visited != 3 {
		t.Fatalf("complete visit exhausted=%t visited=%d err=%v", exhausted, visited, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = store.VisitDsCompositeIndexes(ctx, "visitor-project", func(DsCompositeIndex) bool { t.Fatal("visited after cancellation"); return false })
	if err != context.Canceled {
		t.Fatalf("canceled visit err=%v", err)
	}
}

func TestReviewIndexSpanSets(t *testing.T) {
	point := compositeScanBounds{lower: []byte{5}, upper: []byte{5}, lowerInclusive: true, upperInclusive: true}
	left, right := make([]compositeScanBounds, 128), make([]compositeScanBounds, 128)
	for i := range left {
		left[i], right[i] = point, point
	}
	if got := intersectSpanSets(left, right); len(got) != 1 || !spanContains(got[0], []byte{5}) {
		t.Fatalf("duplicate intersection emitted %d spans, want one point", len(got))
	}
	for _, tc := range []struct {
		name                    string
		leftClosed, rightClosed bool
		want                    int
	}{
		{"hole", false, false, 2}, {"left_closed", true, false, 1}, {"right_closed", false, true, 1}, {"both_closed", true, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spans := normalizeSpans([]compositeScanBounds{{lower: []byte{5}, lowerInclusive: tc.rightClosed}, {upper: []byte{5}, upperInclusive: tc.leftClosed}})
			if len(spans) != tc.want {
				t.Fatalf("normalized=%v want %d spans", spans, tc.want)
			}
			for _, value := range []byte{0, 5, 9} {
				got := false
				for _, span := range spans {
					got = got || spanContains(span, []byte{value})
				}
				want := value != 5 || tc.leftClosed || tc.rightClosed
				if got != want {
					t.Fatalf("value=%d got=%t want=%t", value, got, want)
				}
			}
		})
	}
}

func FuzzBuiltinSpanSets(f *testing.F) {
	f.Add([]byte{1, 5, 3, 5, 9, 2, 2, 7, 0, 4, 6, 1})
	f.Add([]byte{5, 5, 3, 5, 5, 0, 0, 0, 12, 0, 0, 12})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 96 {
			data = data[:96]
		}
		var left, right []compositeScanBounds
		for i := 0; i+2 < len(data); i += 3 {
			flags := data[i+2]
			span := compositeScanBounds{lowerInclusive: flags&1 != 0, upperInclusive: flags&2 != 0}
			if flags&4 == 0 {
				span.lower = []byte{data[i] % 16}
			}
			if flags&8 == 0 {
				span.upper = []byte{data[i+1] % 16}
			}
			if i/3%2 == 0 {
				left = append(left, span)
			} else {
				right = append(right, span)
			}
		}
		originalLeft, originalRight := append([]compositeScanBounds(nil), left...), append([]compositeScanBounds(nil), right...)
		got := intersectSpanSets(left, right)
		contains := func(spans []compositeScanBounds, point int) bool {
			for _, span := range spans {
				lower := span.lower == nil || point > 2*int(span.lower[0]) || span.lowerInclusive && point == 2*int(span.lower[0])
				upper := span.upper == nil || point < 2*int(span.upper[0]) || span.upperInclusive && point == 2*int(span.upper[0])
				if lower && upper {
					return true
				}
			}
			return false
		}
		for point := 0; point < 34; point++ {
			encoded := []byte{byte(point / 2)}
			if point%2 != 0 {
				encoded = append(encoded, 128)
			}
			matches := 0
			for _, span := range got {
				if spanContains(span, encoded) {
					matches++
				}
			}
			want := contains(originalLeft, point) && contains(originalRight, point)
			if matches > 1 || (matches == 1) != want {
				t.Fatalf("data=%x point=%d matches=%d want=%t", data, point, matches, want)
			}
		}
	})
}

func FuzzDottedQueryIndexAgreement(f *testing.F) {
	for _, names := range [][2]string{{"a", "b"}, {"a..b", "c"}, {"a", "b.c"}, {"日本", "é"}} {
		f.Add(names[0], names[1], false, false)
	}
	f.Fuzz(func(t *testing.T, parent, leaf string, excludeLiteral, excludeParent bool) {
		if parent == "" || leaf == "" || len(parent)+len(leaf) > 128 {
			t.Skip()
		}
		path := parent + "." + leaf
		entity := &datastorepb.Entity{Properties: map[string]*datastorepb.Value{
			path: {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 70}, ExcludeFromIndexes: excludeLiteral},
			parent: {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{
				leaf: {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 80}},
			}}}, ExcludeFromIndexes: excludeParent},
		}}
		indexed, err := indexedProperties(entity)
		if err != nil {
			t.Fatal(err)
		}
		stored := make(map[int64]bool)
		for _, value := range indexed[path] {
			stored[value.GetIntegerValue()] = true
		}
		propertypath.QueryValues(entity, path, func(value *datastorepb.Value) bool {
			if !stored[value.GetIntegerValue()] {
				t.Errorf("query candidate %v absent from index for parent=%q leaf=%q", value, parent, leaf)
			}
			delete(stored, value.GetIntegerValue())
			return true
		})
		if len(stored) != 0 {
			t.Fatalf("unqueryable index values %v for parent=%q leaf=%q", stored, parent, leaf)
		}
	})
}

func TestReviewIndexKeysAvoidHexExpansion(t *testing.T) {
	key, ok := builtinIndexEntry(builtinIndexBase("p", "d", "", "K", "s", ""), &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: strings.Repeat("x", 1500)}}, "path", nil)
	if !ok || len(key) >= 2000 {
		t.Fatalf("1500-byte indexed string produced %d-byte key", len(key))
	}
}

func FuzzIndexComponentOrder(f *testing.F) {
	f.Add([]byte("a/\x00b"), []byte("a/\x00c"))
	f.Add([]byte{}, []byte{0})
	f.Fuzz(func(t *testing.T, a, b []byte) {
		if bytes.Compare(a, b) != bytes.Compare(encodeIndexComponent(a), encodeIndexComponent(b)) {
			t.Fatal("component ordering changed")
		}
		framed := appendIndexComponent(nil, a)
		component, rest, ok := takeIndexComponent(append(framed, 'x'))
		if !ok || !bytes.Equal(component, encodeIndexComponent(a)) || string(rest) != "x" {
			t.Fatal("component framing changed")
		}
	})
}

func TestReviewSpillRunsStayBounded(t *testing.T) {
	ctx, work := WithQueryWork(context.Background(), 1)
	acc, err := newPathAccumulator(ctx, t.TempDir(), 32)
	if err != nil {
		t.Fatal(err)
	}
	defer acc.Close()
	for i := 0; i < 4096; i++ {
		if err := acc.Add(fmt.Sprintf("%08d", (4095-i)%257)); err != nil {
			t.Fatal(err)
		}
		if i%256 == 255 {
			files, err := os.ReadDir(acc.dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(files) > 32 {
				t.Fatalf("spill files grew with input: %d", len(files))
			}
		}
	}
	for pass := 0; pass < 2; pass++ {
		n := 0
		if err := acc.ForEach(func(path string) error {
			want := fmt.Sprintf("%08d", n)
			if path != want {
				return fmt.Errorf("got %q, want %q", path, want)
			}
			n++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if n != 257 {
			t.Fatalf("got %d distinct records", n)
		}
	}
	counts := work.Snapshot()
	if counts[WorkScratchReadBytes] == 0 || counts[WorkScratchWriteBytes] == 0 || counts[WorkYields] != counts[WorkAttempts] || counts[WorkAttempts] < 4096 {
		t.Fatalf("unaccounted spill/merge: %v", counts)
	}
}

func TestReviewSpillFailureReleasesAdmission(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	store.exactAccumulatorBytes = 32
	store.exactSlots = make(chan struct{}, 2)
	for _, failure := range []string{"cancel", "write"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx, _ = WithQueryWork(ctx, 1)
			acc, err := store.NewPathAccumulator(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if failure == "cancel" {
				cancel()
			} else {
				// A directory at the next owned run filename forces a real create
				// failure without permissions assumptions or touching external data.
				if err := os.Mkdir(filepath.Join(acc.dir, "run-0"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			err = acc.Add(strings.Repeat("x", 40))
			if err == nil {
				t.Fatal("expected cancellation/write error")
			}
			if closeErr := acc.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			if len(store.exactSlots) != 0 {
				t.Fatal("scratch admission leaked")
			}
			files, err := os.ReadDir(store.scratchDir)
			if err != nil || len(files) != 0 {
				t.Fatalf("scratch remains: %v, %v", files, err)
			}
		})
	}
	// More callers than slots, with frequent spill/merge, must all progress.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, _ := WithQueryWork(ctx, 1)
			acc, err := store.NewPathAccumulator(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			defer func() {
				if err := acc.Close(); err != nil {
					t.Error(err)
				}
			}()
			for i := 63; i >= 0; i-- {
				if err := acc.Add(fmt.Sprintf("%04d", i)); err != nil {
					t.Error(err)
					return
				}
			}
			count, err := acc.Count()
			if err != nil || count != 64 {
				t.Errorf("count=%d err=%v", count, err)
			}
		}()
	}
	wg.Wait()
	if len(store.exactSlots) != 0 {
		t.Fatal("concurrent callers leaked scratch admission")
	}
}

func TestReviewFailedOpenPreservesScratch(t *testing.T) {
	dir := t.TempDir()
	first, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := first.Close(); err != nil {
			t.Error(err)
		}
	})
	path := filepath.Join(dir, "scratch", "active-query")
	if err := os.WriteFile(path, []byte("active"), 0600); err != nil {
		t.Fatal(err)
	}
	second, err := New(dir)
	if err == nil {
		second.Close()
		t.Fatal("second open succeeded")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "active" {
		t.Fatalf("scratch changed: %q, %v", data, err)
	}
}

func TestReviewSnapshotLeaseBlocksPrematureDiscard(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	snapshot := store.ReadTime()
	release, err := store.PinReadTime(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	store.advanceDiscardTime(snapshot.Add(time.Minute))
	second, err := store.PinReadTime(snapshot)
	if err != nil {
		t.Fatalf("active snapshot was discarded: %v", err)
	}
	second()
	release()
	store.advanceDiscardTime(snapshot.Add(time.Minute))
	if release, err := store.PinReadTime(snapshot); err == nil {
		release()
		t.Fatal("discarded snapshot was accepted")
	}
}
