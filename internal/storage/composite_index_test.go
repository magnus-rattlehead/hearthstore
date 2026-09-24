package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/dgraph-io/badger/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testProject = "test-project"
const testDB = "(default)"

func TestPendingOversizedIndexAllowsRepair(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete=%t", remove), func(t *testing.T) {
			store, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			var values []*datastorepb.Value
			for i := range 150 {
				values = append(values, &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: int64(i)}})
			}
			array := &datastorepb.Value{ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: values}}}
			entity := &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "Repair", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}, Properties: map[string]*datastorepb.Value{"x": array, "y": array}}
			if _, err := store.DsInsert(EntityWrite{Project: testProject, Database: testDB, Path: "Repair/one", Kind: "Repair", Entity: entity}); err != nil {
				t.Fatal(err)
			}
			index := DsCompositeIndex{Project: testProject, Kind: "Repair", State: DsIndexCreating, BuildingGeneration: 1, Properties: []DsIndexProperty{{Name: "x"}, {Name: "y"}}}
			index.ID = DsCompositeIndexID(index.Kind, false, index.Properties)
			raw, err := json.Marshal(index)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.RunInTx(func(tx *Txn) error { return tx.Set(indexKey(testProject, index.ID), raw) }); err != nil {
				t.Fatal(err)
			}
			if remove {
				err = store.DsDelete(testProject, testDB, "", "Repair/one")
			} else {
				entity.Properties = map[string]*datastorepb.Value{"x": values[0], "y": values[1]}
				_, err = store.DsUpsert(EntityWrite{Project: testProject, Database: testDB, Path: "Repair/one", Kind: "Repair", Entity: entity})
			}
			if err != nil {
				t.Fatalf("repair blocked by unpublished oversized index: %v", err)
			}
		})
	}
}

func TestEntityWideCompositeLimits(t *testing.T) {
	for _, bytesLimit := range []bool{false, true} {
		for _, building := range []bool{false, true} {
			t.Run(fmt.Sprintf("bytes=%t/build=%t", bytesLimit, building), func(t *testing.T) {
				store, err := New(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := store.Close(); err != nil {
						t.Error(err)
					}
				})
				entity := &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "Limits", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}, Properties: map[string]*datastorepb.Value{}}
				for _, name := range []string{"x", "y", "z"} {
					var values []*datastorepb.Value
					for i := range 100 {
						v := &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: int64(i)}}
						if bytesLimit && name == "x" {
							v = &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: fmt.Sprintf("%03d", i) + strings.Repeat("s", 1397)}}
						}
						values = append(values, v)
					}
					entity.Properties[name] = &datastorepb.Value{ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: values}}}
				}
				insert := func() error {
					_, err := store.DsInsert(EntityWrite{Project: testProject, Database: testDB, Path: "Limits/one", Kind: "Limits", Entity: entity})
					return err
				}
				if building {
					if err := insert(); err != nil {
						t.Fatal(err)
					}
				}
				var indexes []DsCompositeIndex
				for _, second := range []string{"y", "z"} {
					idx, _, err := store.EnsureDsCompositeIndex(context.Background(), DsCompositeIndex{Project: testProject, Kind: "Limits", Properties: []DsIndexProperty{{Name: "x"}, {Name: second}}}, true)
					if err != nil {
						t.Fatal(err)
					}
					indexes = append(indexes, idx)
					if building && (bytesLimit || second == "z") {
						if idx.State != DsIndexError {
							t.Fatalf("oversized build state=%s, want ERROR", idx.State)
						}
						return
					}
					if idx.State != DsIndexReady {
						t.Fatalf("index=%+v", idx)
					}
				}
				if err := insert(); status.Code(err) != codes.InvalidArgument {
					t.Fatalf("oversized write=%v, want InvalidArgument", err)
				}
				for _, idx := range indexes {
					got, err := store.GetDsCompositeIndex(testProject, idx.ID)
					if err != nil || got.State != DsIndexReady {
						t.Fatalf("write disabled healthy index: %+v, %v", got, err)
					}
				}
				if err := store.view(func(tx *Txn) error { _, _, err := getDSTxn(tx, testProject, testDB, "", "Limits/one"); return err }); status.Code(err) != codes.NotFound {
					t.Fatalf("failed write persisted entity: %v", err)
				}
			})
		}
	}
}

func TestCompositeBuildStreamsLargePhysicalEntity(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if store != nil {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	// Datastore composite-entry sizes omit property names. These long names
	// make Hearthstore's covering payload exceed one Badger transaction, while
	// the 10,000 logical entries total 760,000 bytes (key 28 + values 16 + 32).
	names := []string{strings.Repeat("x", 1000), strings.Repeat("y", 1000)}
	entity := &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "Product", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}, Properties: map[string]*datastorepb.Value{}}
	for _, name := range names {
		var values []*datastorepb.Value
		for i := range 100 {
			values = append(values, &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: int64(i)}})
		}
		entity.Properties[name] = &datastorepb.Value{ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: values}}}
	}
	if _, err := store.DsInsert(EntityWrite{Project: testProject, Database: testDB, Path: "Product/one", Kind: "Product", Entity: entity}); err != nil {
		t.Fatal(err)
	}
	index, _, err := store.EnsureDsCompositeIndex(context.Background(), DsCompositeIndex{Project: testProject, Kind: "Product", Properties: []DsIndexProperty{{Name: names[0]}, {Name: names[1]}}}, true)
	if err != nil || index.State != DsIndexReady {
		t.Fatalf("production-valid build: state=%s detail=%s err=%v", index.State, index.Error, err)
	}
	page, err := store.DsQueryComposite(context.Background(), CompositeQuery{Project: testProject, Database: testDB, IndexID: index.ID, Limit: 10})
	rows := page.Rows
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	entries := 0
	if err := store.view(func(tx *Txn) error {
		prefix := compositeBase(index, testDB, "", index.ActiveGeneration)
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			entries++
		}
		return nil
	}); err != nil || entries != 10000 {
		t.Fatalf("persisted entries=%d want=10000 err=%v", entries, err)
	}
	// Interrupt after a committed entry batch, then reopen the unpublished
	// generation. A resumed build must complete and deduplicate its partial work.
	index.State, index.BuildingGeneration = DsIndexCreating, 2
	raw, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx *Txn) error { return tx.Set(indexKey(testProject, index.ID), raw) }); err != nil {
		t.Fatal(err)
	}
	prior := store.commitTotal.Load()
	canceled, cancel := context.WithCancel(context.Background())
	cancelCtx := compositeBuildEventContext{Context: canceled, onCheck: func() {
		if store.commitTotal.Load() > prior {
			cancel()
		}
	}}
	err = store.streamCompositeBuildTarget(cancelCtx, testProject, index, 2, compositeBuildTarget{database: testDB, path: "Product/one"})
	cancel()
	if err != context.Canceled {
		t.Fatalf("partial generation cancellation=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil
	store, err = New(dir)
	if err != nil {
		t.Fatal(err)
	}
	index, _, err = store.EnsureDsCompositeIndex(context.Background(), index, true)
	if err != nil || index.State != DsIndexReady || index.ActiveGeneration != 2 {
		t.Fatalf("partial generation restart: index=%+v err=%v", index, err)
	}
	// Change the source after the next generation's first streamed batch.
	// Maintenance must win; the remaining old tuples cannot reappear.
	index.State, index.BuildingGeneration = DsIndexCreating, 3
	raw, err = json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx *Txn) error { return tx.Set(indexKey(testProject, index.ID), raw) }); err != nil {
		t.Fatal(err)
	}
	before := store.commitTotal.Load()
	updated := false
	ctx := compositeBuildEventContext{Context: context.Background(), onCheck: func() {
		if !updated && store.commitTotal.Load() > before {
			updated = true
			for _, name := range names {
				entity.Properties[name] = &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 0}}
			}
			if _, err := store.DsUpsert(EntityWrite{Project: testProject, Database: testDB, Path: "Product/one", Kind: "Product", Entity: entity}); err != nil {
				t.Fatal(err)
			}
		}
	}}
	if err := store.streamCompositeBuildTarget(ctx, testProject, index, 3, compositeBuildTarget{database: testDB, path: "Product/one"}); err != nil || !updated {
		t.Fatalf("updated=%v stream err=%v", updated, err)
	}
	entries = 0
	if err := store.view(func(tx *Txn) error {
		prefix := compositeBase(index, testDB, "", 3)
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			entries++
		}
		return nil
	}); err != nil || entries != 1 {
		t.Fatalf("source update left stale entries: entries=%d err=%v", entries, err)
	}
}

type compositeBuildCancelContext struct {
	context.Context
	work   *QueryWork
	cancel context.CancelFunc
}

type compositeBuildEventContext struct {
	context.Context
	onCheck func()
}

func (c compositeBuildEventContext) Err() error {
	c.onCheck()
	return c.Context.Err()
}

func TestCompositeBuildDoesNotRepublishDeletedIndex(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	entity := &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "Product", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}, Properties: map[string]*datastorepb.Value{"x": {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 1}}}}
	if _, err := store.DsInsert(EntityWrite{Project: testProject, Database: testDB, Path: "Product/one", Kind: "Product", Entity: entity}); err != nil {
		t.Fatal(err)
	}
	index := DsCompositeIndex{Project: testProject, Kind: "Product", State: DsIndexCreating, BuildingGeneration: 1, Properties: []DsIndexProperty{{Name: "x"}}}
	index.ID = DsCompositeIndexID(index.Kind, false, index.Properties)
	raw, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx *Txn) error { return tx.Set(indexKey(testProject, index.ID), raw) }); err != nil {
		t.Fatal(err)
	}
	ctx, work := WithQueryWork(context.Background(), 1)
	deleted := false
	ctx = compositeBuildEventContext{Context: ctx, onCheck: func() {
		if !deleted && work.Snapshot()[WorkIndexEntries] >= 2 {
			deleted = true
			if err := store.DeleteDsCompositeIndex(context.Background(), testProject, index.ID); err != nil {
				t.Fatal(err)
			}
		}
	}}
	err = store.BuildDsCompositeIndex(ctx, testProject, index.ID)
	if !deleted || err == nil {
		t.Fatalf("deleted=%v build err=%v", deleted, err)
	}
	if _, err := store.GetDsCompositeIndex(testProject, index.ID); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("deleted index was republished: %v", err)
	}
}

func (c compositeBuildCancelContext) Err() error {
	if c.work.Snapshot()[WorkIndexEntries] > 0 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestCompositeBuildCancellationRemainsRetryable(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if store != nil {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	entity := &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "Product", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}, Properties: map[string]*datastorepb.Value{"x": {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 1}}, "y": {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 2}}}}
	if _, err := store.DsInsert(EntityWrite{Project: testProject, Database: testDB, Path: "Product/one", Kind: "Product", Entity: entity}); err != nil {
		t.Fatal(err)
	}
	index := DsCompositeIndex{Project: testProject, Kind: "Product", State: DsIndexCreating, BuildingGeneration: 1, Properties: []DsIndexProperty{{Name: "x"}, {Name: "y"}}}
	index.ID = DsCompositeIndexID(index.Kind, false, index.Properties)
	raw, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunInTx(func(tx *Txn) error { return tx.Set(indexKey(testProject, index.ID), raw) }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, work := WithQueryWork(ctx, 1)
	err = store.BuildDsCompositeIndex(compositeBuildCancelContext{Context: ctx, work: work, cancel: cancel}, testProject, index.ID)
	if err != context.Canceled {
		t.Fatalf("canceled build err=%v work=%v", err, work.Snapshot())
	}
	interrupted, err := store.GetDsCompositeIndex(testProject, index.ID)
	if err != nil || interrupted.State != DsIndexCreating || interrupted.ActiveGeneration != 0 {
		t.Fatalf("interrupted index=%+v err=%v", interrupted, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil
	store, err = New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ready, created, err := store.EnsureDsCompositeIndex(context.Background(), index, true)
	if err != nil || created || ready.State != DsIndexReady || ready.ActiveGeneration != 1 {
		t.Fatalf("resumed index=%+v created=%v err=%v", ready, created, err)
	}
	page, err := store.DsQueryComposite(context.Background(), CompositeQuery{Project: testProject, Database: testDB, IndexID: index.ID, Limit: 10})
	rows := page.Rows
	if err != nil || len(rows) != 1 {
		t.Fatalf("resumed rows=%d err=%v", len(rows), err)
	}
}

func TestCompositeProductCountsOnlyActualEntries(t *testing.T) {
	integer := func(n int64, excluded bool) *datastorepb.Value {
		return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: n}, ExcludeFromIndexes: excluded}
	}
	for _, scenario := range []string{"duplicates", "excluded", "missing_later"} {
		t.Run(scenario, func(t *testing.T) {
			store, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			var x, y []*datastorepb.Value
			for i := range 142 {
				if scenario == "duplicates" {
					x = append(x, integer(1, false))
					y = append(y, integer(2, false))
				} else {
					x = append(x, integer(int64(i), false))
					y = append(y, integer(int64(i), scenario == "excluded" && i != 2))
				}
			}
			entity := &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "Product", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}, Properties: map[string]*datastorepb.Value{"x": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: x}}}, "y": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: y}}}}}
			if _, err := store.DsInsert(EntityWrite{Project: testProject, Database: testDB, Path: "Product/one", Kind: "Product", Entity: entity}); err != nil {
				t.Fatal(err)
			}
			properties := []DsIndexProperty{{Name: "x"}, {Name: "y"}}
			want := 1
			if scenario == "missing_later" {
				properties = append(properties, DsIndexProperty{Name: "z"})
				want = 0
			}
			idx, _, err := store.EnsureDsCompositeIndex(context.Background(), DsCompositeIndex{Project: testProject, Kind: "Product", Properties: properties}, true)
			if err != nil || idx.State != DsIndexReady {
				t.Fatalf("valid product rejected: state=%s detail=%s err=%v", idx.State, idx.Error, err)
			}
			page, err := store.DsQueryComposite(context.Background(), CompositeQuery{Project: testProject, Database: testDB, IndexID: idx.ID, Limit: 10})
			rows := page.Rows
			if err != nil || len(rows) != want {
				t.Fatalf("rows=%d want=%d err=%v", len(rows), want, err)
			}
			keys, err := dsCompositeEntryKeys(idx, idx.ActiveGeneration, testDB, "", "Product/one", entity)
			wantKeys := want
			if scenario == "excluded" {
				wantKeys = 142
			}
			if err != nil || len(keys) != wantKeys {
				t.Fatalf("keys=%d want=%d err=%v", len(keys), wantKeys, err)
			}
		})
	}
}

func TestCompositeEntryLimitsPreflightExactUnion(t *testing.T) {
	array := func(start, count int) *datastorepb.Value {
		var values []*datastorepb.Value
		for i := range count {
			values = append(values, &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: int64(start + i)}})
		}
		return &datastorepb.Value{ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: values}}}
	}
	for _, tc := range []struct {
		name                                string
		x, y, nestedX, nestedY, nestedStart int
		ancestor                            bool
		want                                int
	}{
		{name: "single_invalid", x: 200, y: 101, want: -1},
		{name: "union_invalid", x: 100, y: 100, nestedX: 100, nestedY: 101, nestedStart: 100, want: -1},
		{name: "overlapping_union", x: 100, y: 100, nestedX: 100, nestedY: 100, nestedStart: 50, want: 15000},
		{name: "ancestor_invalid", x: 101, y: 100, ancestor: true, want: -1},
		{name: "ancestor_valid", x: 98, y: 100, ancestor: true, want: 19600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entity := &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "Parent", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}, {Kind: "Product", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}, Properties: map[string]*datastorepb.Value{"a.b": array(0, tc.x), "c.d": array(0, tc.y)}}
			if tc.nestedX > 0 {
				entity.Properties["a"] = &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"b": array(tc.nestedStart, tc.nestedX)}}}}
				entity.Properties["c"] = &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"d": array(0, tc.nestedY)}}}}
			}
			index := DsCompositeIndex{Project: testProject, Kind: "Product", Ancestor: tc.ancestor, Properties: []DsIndexProperty{{Name: "a.b"}, {Name: "c.d"}}}
			index.ID = DsCompositeIndexID(index.Kind, index.Ancestor, index.Properties)
			count := 0
			err := visitCompositeEntryKeys(context.Background(), index, 1, testDB, "", "Parent/one/Product/one", entity, func([]byte) error { count++; return nil })
			if tc.want < 0 {
				if err == nil || count != 0 {
					t.Fatalf("invalid union emitted %d entries before error=%v", count, err)
				}
			} else if err != nil || count != tc.want {
				t.Fatalf("entries=%d want=%d err=%v", count, tc.want, err)
			}
		})
	}
}

func TestCompositeEntryStreamPreservesCorrelatedUnion(t *testing.T) {
	array := func(numbers ...int64) *datastorepb.Value {
		var values []*datastorepb.Value
		for _, number := range numbers {
			values = append(values, &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: number}})
		}
		return &datastorepb.Value{ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: values}}}
	}
	entity := &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "Product", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}, Properties: map[string]*datastorepb.Value{
		"a.b": array(1, 2), "c.d": array(10, 20),
		"a": {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"b": array(2, 3)}}}},
		"c": {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"d": array(20, 30)}}}},
	}}
	index := DsCompositeIndex{Project: testProject, Kind: "Product", ActiveGeneration: 1, Properties: []DsIndexProperty{{Name: "a.b"}, {Name: "c.d"}}}
	index.ID = DsCompositeIndexID(index.Kind, false, index.Properties)
	for _, stopEarly := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		ctx, work := WithQueryWork(ctx, 1)
		seen := map[[2]int64]bool{}
		err := visitCompositeEntryKeys(ctx, index, 1, testDB, "", "Product/one", entity, func(key []byte) error {
			projected, err := compositeProjection(index, testDB, "", "", key, entity)
			if err != nil {
				return err
			}
			pair := [2]int64{projected.Properties["a.b"].GetIntegerValue(), projected.Properties["c.d"].GetIntegerValue()}
			if seen[pair] {
				t.Fatalf("duplicate tuple=%v", pair)
			}
			seen[pair] = true
			if stopEarly && len(seen) == 3 {
				cancel()
			}
			return nil
		})
		cancel()
		if stopEarly {
			if err != context.Canceled || len(seen) != 3 {
				t.Fatalf("canceled stream tuples=%d err=%v", len(seen), err)
			}
		} else {
			if err != nil || len(seen) != 7 {
				t.Fatalf("stream tuples=%v err=%v", seen, err)
			}
			for _, pair := range [][2]int64{{1, 10}, {1, 20}, {2, 10}, {2, 20}, {2, 30}, {3, 20}, {3, 30}} {
				if !seen[pair] {
					t.Fatalf("missing tuple=%v", pair)
				}
			}
		}
		if work.Snapshot()[WorkYields] == 0 {
			t.Fatal("entry stream did not checkpoint")
		}
	}
}

func TestDottedCompositePersistsAndMaintainsBothInterpretations(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if store != nil {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	integer := func(n int64) *datastorepb.Value {
		return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: n}}
	}
	entity := &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "Dotted", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}, Properties: map[string]*datastorepb.Value{
		"a.b": integer(70), "a": {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"b": integer(80)}}}}, "tag": integer(1),
	}}
	if _, err := store.DsUpsert(EntityWrite{Project: testProject, Database: testDB, Path: "Dotted/one", Kind: "Dotted", Entity: entity}); err != nil {
		t.Fatal(err)
	}
	idx, _, err := store.EnsureDsCompositeIndex(context.Background(), DsCompositeIndex{Project: testProject, Kind: "Dotted", Properties: []DsIndexProperty{{Name: "a.b"}, {Name: "tag"}}}, true)
	if err != nil {
		t.Fatal(err)
	}
	check := func(value int64, want int) {
		t.Helper()
		page, err := store.DsQueryComposite(context.Background(), CompositeQuery{Project: testProject, Database: testDB, IndexID: idx.ID, Prefix: DsCompositePrefix(idx, map[string]*datastorepb.Value{"a.b": integer(value)}), Limit: 10})
		rows := page.Rows
		if err != nil || len(rows) != want {
			t.Fatalf("a.b=%d rows=%d want=%d error=%v", value, len(rows), want, err)
		}
	}
	check(70, 1)
	check(80, 1)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil
	store, err = New(dir)
	if err != nil {
		t.Fatal(err)
	}
	check(70, 1)
	check(80, 1)
	entity.Properties["a.b"] = integer(90)
	if _, err := store.DsUpsert(EntityWrite{Project: testProject, Database: testDB, Path: "Dotted/one", Kind: "Dotted", Entity: entity}); err != nil {
		t.Fatal(err)
	}
	check(70, 0)
	check(80, 1)
	check(90, 1)
	if err := store.DsDelete(testProject, testDB, "", "Dotted/one"); err != nil {
		t.Fatal(err)
	}
	check(80, 0)
	check(90, 0)
}

func TestDatastoreCompositeIndexLifecycle(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	properties := []DsIndexProperty{{Name: "office"}, {Name: "state"}, {Name: "sort_name"}}
	idx, created, err := store.EnsureDsCompositeIndex(context.Background(), DsCompositeIndex{
		Project: testProject, Kind: "UserProfile", Properties: properties, Source: "configured",
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !created || idx.State != DsIndexReady {
		t.Fatalf("created=%v state=%s", created, idx.State)
	}

	office := &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "Office", IdType: &datastorepb.Key_PathElement_Id{Id: 380011}}}}
	makeEntity := func(name, sortName string) *datastorepb.Entity {
		return &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "UserProfile", IdType: &datastorepb.Key_PathElement_Name{Name: name}}}}, Properties: map[string]*datastorepb.Value{
			"office":    {ValueType: &datastorepb.Value_KeyValue{KeyValue: office}},
			"state":     {ValueType: &datastorepb.Value_StringValue{StringValue: "active"}},
			"sort_name": {ValueType: &datastorepb.Value_StringValue{StringValue: sortName}},
		}}
	}
	for _, entry := range []struct{ name, sortName string }{{"second", "Zulu"}, {"first", "Alpha"}} {
		if _, err := store.DsInsert(EntityWrite{Project: testProject, Database: testDB, Path: "UserProfile/" + entry.name, Kind: "UserProfile", Entity: makeEntity(entry.name, entry.sortName)}); err != nil {
			t.Fatal(err)
		}
	}
	prefix := DsCompositePrefix(idx, map[string]*datastorepb.Value{
		"office": {ValueType: &datastorepb.Value_KeyValue{KeyValue: office}},
		"state":  {ValueType: &datastorepb.Value_StringValue{StringValue: "active"}},
	})
	page, err := store.DsQueryComposite(context.Background(), CompositeQuery{Project: testProject, Database: testDB, IndexID: idx.ID, Prefix: prefix, Limit: 10})
	rows, scanned := page.Rows, page.Scanned
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Path != "UserProfile/first" {
		t.Fatalf("paths=%v scanned=%d", []string{rows[0].Path, rows[1].Path}, scanned)
	}

	if err := store.DsDelete(testProject, testDB, "", "UserProfile/first"); err != nil {
		t.Fatal(err)
	}
	page, err = store.DsQueryComposite(context.Background(), CompositeQuery{Project: testProject, Database: testDB, IndexID: idx.ID, Prefix: prefix, Limit: 10})
	rows = page.Rows
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Path != "UserProfile/second" {
		t.Fatalf("rows after delete=%v", rows)
	}
}

func TestBuildDsCompositeIndexBatchesWrites(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const entityCount = 100
	state := &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: "active"}}
	for i := 0; i < entityCount; i++ {
		path := fmt.Sprintf("UserProfile/%03d", i)
		entity := &datastorepb.Entity{
			Key:        &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "UserProfile", IdType: &datastorepb.Key_PathElement_Name{Name: path}}}},
			Properties: map[string]*datastorepb.Value{"state": state},
		}
		if _, err = store.DsInsert(EntityWrite{Project: testProject, Database: testDB, Path: path, Kind: "UserProfile", Entity: entity}); err != nil {
			t.Fatal(err)
		}
	}

	before := store.CounterSnapshot().CommitTotal
	idx, _, err := store.EnsureDsCompositeIndex(context.Background(), DsCompositeIndex{
		Project: testProject, Kind: "UserProfile", Properties: []DsIndexProperty{{Name: "state"}}, Source: "configured",
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	commits := store.CounterSnapshot().CommitTotal - before
	if commits > 6 {
		t.Fatalf("index build commits=%d, want at most 6", commits)
	}

	page, err := store.DsQueryComposite(context.Background(), CompositeQuery{Project: testProject, Database: testDB, IndexID: idx.ID, Prefix: DsCompositePrefix(idx, map[string]*datastorepb.Value{"state": state}), Limit: entityCount})

	rows := page.Rows
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != entityCount {
		t.Fatalf("indexed rows=%d, want %d", len(rows), entityCount)
	}
}

func TestEnsureDsCompositeIndexWaitsForRunningBuild(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.compositeBuildSlots <- struct{}{}
	definition := DsCompositeIndex{
		Project: testProject,
		Kind:    "UserProfile",
		Properties: []DsIndexProperty{
			{Name: "office"},
			{Name: "state"},
		},
	}
	if _, created, err := store.EnsureDsCompositeIndex(context.Background(), definition, false); err != nil || !created {
		t.Fatalf("start build: created=%v err=%v", created, err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	canceledWhileWaiting := make(chan struct{})
	// The build slot is deliberately blocked, so cancellation is the only event
	// that should release this waiter.
	time.AfterFunc(100*time.Millisecond, func() {
		cancel()
		close(canceledWhileWaiting)
	})
	idx, _, err := store.EnsureDsCompositeIndex(canceled, definition, true)
	<-canceledWhileWaiting
	if err != context.Canceled {
		t.Fatalf("wait for running build: state=%s err=%v, want context canceled", idx.State, err)
	}

	<-store.compositeBuildSlots
	idx, created, err := store.EnsureDsCompositeIndex(context.Background(), definition, true)
	if err != nil {
		t.Fatal(err)
	}
	if created || idx.State != DsIndexReady {
		t.Fatalf("joined build: created=%v state=%s, want existing ready index", created, idx.State)
	}
}

func TestDeleteDsCompositeIndexStopsQueuedBuild(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	store.compositeBuildSlots <- struct{}{}
	defer func() { <-store.compositeBuildSlots }()
	index, _, err := store.EnsureDsCompositeIndex(context.Background(), DsCompositeIndex{Project: testProject, Kind: "Product", Properties: []DsIndexProperty{{Name: "x"}}}, false)
	if err != nil {
		t.Fatal(err)
	}
	value, exists := store.compositeBuilds.Load(testProject + "/" + index.ID)
	if !exists {
		t.Fatal("queued build is missing")
	}
	build := value.(*compositeBuild)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := store.DeleteDsCompositeIndex(ctx, testProject, index.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-build.done:
	case <-ctx.Done():
		t.Fatal("deletion left a queued worker alive")
	}
}

func TestWatchDsCompositeIndexReportsPersistedStateAndCompletion(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.compositeBuildSlots <- struct{}{}
	definition := DsCompositeIndex{
		Project: testProject,
		Kind:    "UserProfile",
		Properties: []DsIndexProperty{
			{Name: "office"},
			{Name: "state"},
		},
	}
	creating, created, err := store.EnsureDsCompositeIndex(context.Background(), definition, false)
	if err != nil || !created {
		t.Fatalf("start build: created=%v err=%v", created, err)
	}

	states := make(chan string, 2)
	done := make(chan error, 1)
	go func() {
		done <- store.WatchDsCompositeIndex(context.Background(), creating.Project, creating.ID, func(index DsCompositeIndex) {
			states <- index.State
		})
	}()
	if state := <-states; state != DsIndexCreating {
		t.Fatalf("initial state=%s, want %s", state, DsIndexCreating)
	}

	<-store.compositeBuildSlots
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if state := <-states; state != DsIndexReady {
		t.Fatalf("terminal state=%s, want %s", state, DsIndexReady)
	}
}
