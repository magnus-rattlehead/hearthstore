package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/dgraph-io/badger/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStoredTimestampPrecision(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	stamp := &datastorepb.Value{ExcludeFromIndexes: true, ValueType: &datastorepb.Value_TimestampValue{TimestampValue: &timestamppb.Timestamp{Seconds: -1, Nanos: 123456789}}}
	entity := &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "Precision", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}, Properties: map[string]*datastorepb.Value{"nested": {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"dates": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: []*datastorepb.Value{stamp}}}}}}}}}}
	if _, err := store.DsInsert(EntityWrite{Project: "p", Database: "(default)", Path: "Precision/one", Kind: "Precision", Entity: entity}); err != nil {
		t.Fatal(err)
	}
	stored, _, _, _, err := store.DsGetWithTimes("p", "(default)", "", "Precision/one")
	if err != nil {
		t.Fatal(err)
	}
	got := stored.Properties["nested"].GetEntityValue().Properties["dates"].GetArrayValue().Values[0].GetTimestampValue()
	if got.Seconds != -1 || got.Nanos != 123456000 {
		t.Fatalf("stored timestamp=%v, want (-1,123456000)", got)
	}
	if stamp.GetTimestampValue().Nanos != 123456789 {
		t.Fatal("write mutated caller timestamp")
	}
}

func TestBadgerStorePersists(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	entity := &datastorepb.Entity{Properties: map[string]*datastorepb.Value{
		"name": {ValueType: &datastorepb.Value_StringValue{StringValue: "a"}},
	}}
	if _, err = store.DsUpsert(EntityWrite{Project: "p", Database: "(default)", Path: "Item/a", Kind: "Item", Entity: entity}); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = New(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := store.DsGet("p", "(default)", "", "Item/a")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetProperties()["name"].GetStringValue() != "a" {
		t.Fatalf("name=%q", got.GetProperties()["name"].GetStringValue())
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRejectIncompatibleStorageWithoutModifyingIt(t *testing.T) {
	for _, tc := range []struct {
		name, file, contents string
	}{
		{"legacy_database", "hearthstore.db", "incompatible"},
		{"pre_collision_v3", "storage-format", "datastore-badger-v3\n"},
		{"pre_collision_v4", "storage-format", "datastore-badger-v4\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, tc.file)
			old := []byte(tc.contents)
			if err := os.WriteFile(path, old, 0o600); err != nil {
				t.Fatal(err)
			}
			store, err := New(dir)
			if store != nil {
				if closeErr := store.Close(); closeErr != nil {
					t.Error(closeErr)
				}
			}
			if !errors.Is(err, ErrIncompatibleData) {
				t.Fatalf("open old database: %v, want ErrIncompatibleData", err)
			}
			got, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(got, old) {
				t.Fatalf("existing data modified: %q, error=%v", got, err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 1 {
				t.Fatalf("rejected directory modified: %v, error=%v", entries, err)
			}
		})
	}
}

func TestDsUpsertManySplitsOversizedTransaction(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Index keys remain in the LSM regardless of value-log placement.
	const count = 32
	values := &datastorepb.ArrayValue{}
	for i := 0; i < 128; i++ {
		values.Values = append(values.Values, &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: fmt.Sprintf("%04d%s", i, strings.Repeat("x", 900))}})
	}
	rows := make([]UpsertManyRow, count)
	for i := range rows {
		path := fmt.Sprintf("Import/%d", i)
		rows[i] = UpsertManyRow{
			Path: path,
			Kind: "Import",
			Entity: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{
				"values": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: values}},
			}},
		}
	}
	// Independently prove the input exceeds the engine limit before exercising
	// the adaptive public entry point; abort leaves no partial fixture behind.
	err = store.runUpdateRawUnlocked(context.Background(), false, func(tx *Txn) error {
		_, err := store.DsUpsertManyTx(tx, "project", "(default)", rows, timestamppb.Now(), NewCommitAccumulator())
		return err
	})
	if !errors.Is(err, badger.ErrTxnTooBig) {
		t.Fatalf("unsplit write=%v, want ErrTxnTooBig", err)
	}

	versions, err := store.DsUpsertMany(context.Background(), "project", "(default)", rows, timestamppb.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != count {
		t.Fatalf("committed %d entities, want %d", len(versions), count)
	}
	if store.CounterSnapshot().TxnTooBig == 0 {
		t.Fatal("oversized transaction did not exercise adaptive splitting")
	}
	for _, row := range rows {
		got, _, err := store.DsGet("project", "(default)", "", row.Path)
		if err != nil || len(got.GetProperties()["values"].GetArrayValue().GetValues()) != len(values.Values) {
			t.Fatalf("get %s: %v, error=%v", row.Path, got, err)
		}
	}
}

func TestLargePayloadsStayOutOfLSM(t *testing.T) {
	dir := t.TempDir()
	store, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	const count, payloadSize = 32, 32 << 10
	random := rand.NewChaCha8([32]byte{1})
	payload := make([]byte, payloadSize)
	if _, err := random.Read(payload); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		entity := &datastorepb.Entity{Properties: map[string]*datastorepb.Value{
			"payload": {ExcludeFromIndexes: true, ValueType: &datastorepb.Value_BlobValue{BlobValue: payload}},
		}}
		if _, err := store.DsUpsert(EntityWrite{Project: "p", Path: fmt.Sprintf("Payload/%d", i), Kind: "Payload", Entity: entity}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil
	files, err := filepath.Glob(filepath.Join(dir, "badger", "*.sst"))
	if err != nil || len(files) == 0 {
		t.Fatalf("missing flushed tables: %v", err)
	}
	var tableBytes int64
	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			t.Fatal(err)
		}
		tableBytes += info.Size()
	}
	if tableBytes >= count*payloadSize/2 {
		t.Fatalf("LSM contains large payloads: %d table bytes for %d payload bytes", tableBytes, count*payloadSize)
	}
	store, err = New(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		got, _, err := store.DsGet("p", "", "", fmt.Sprintf("Payload/%d", i))
		if err != nil || !bytes.Equal(got.GetProperties()["payload"].GetBlobValue(), payload) {
			t.Fatalf("payload %d did not round-trip: %v", i, err)
		}
	}
}

func TestConfigureBlockCacheAppliesBudgetImmediately(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	want := int64(384 << 20)
	if err := store.ConfigureBlockCache(want); err != nil {
		t.Fatal(err)
	}
	got, err := store.db.CacheMaxCost(badger.BlockCache, -1)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("block cache budget = %d, want %d", got, want)
	}
}

func TestValueLogGCTriggersAfterOneValueLogFileOfWrites(t *testing.T) {
	const fileSize = int64(1 << 30)
	if valueLogGCReady(fileSize-1, fileSize) {
		t.Fatal("GC triggered before one value-log file of writes")
	}
	if !valueLogGCReady(fileSize, fileSize) {
		t.Fatal("GC did not trigger at one value-log file of writes")
	}
}

func TestValueLogReclamationPreservesPinnedPayloads(t *testing.T) {
	// Smaller files/buffers force rotation and compaction without a multi-GB
	// unit-suite fixture. Value placement and snapshot rules use production options.
	dir := t.TempDir()
	opts := badgerOptions(dir).WithValueLogFileSize(1 << 20).WithMemTableSize(1 << 20).WithNumLevelZeroTables(2)
	db, err := badger.OpenManaged(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	// Drive GC synchronously so the test can observe both sides of the lease.
	store := &Store{db: db, gcWake: make(chan struct{}, 1), syncDB: db.Sync,
		commitTxn: func(tx *Txn, ts uint64) error { return tx.CommitAt(ts, nil) }}
	const count = 64
	payload := bytes.Repeat([]byte("original"), 8<<10)
	replacement := bytes.Repeat([]byte("updated!"), 8<<10)
	write := func(i int, data []byte) {
		t.Helper()
		entity := &datastorepb.Entity{Properties: map[string]*datastorepb.Value{
			"payload": {ExcludeFromIndexes: true, ValueType: &datastorepb.Value_BlobValue{BlobValue: data}},
		}}
		if _, err := store.DsUpsert(EntityWrite{Project: "p", Path: fmt.Sprintf("Payload/%d", i), Kind: "Payload", Entity: entity}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < count; i++ {
		write(i, payload)
	}
	snapshot := store.ReadTime()
	release, err := store.PinReadTime(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for i := 0; i < count; i++ {
		if i%2 == 0 {
			write(i, replacement)
		} else if err := store.DsDelete("p", "", "", fmt.Sprintf("Payload/%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	flush := func(phase int) {
		t.Helper()
		cutoff := store.latestTs.Load()
		// Enough inline padding to flush every preceding mutation; wait for the
		// resulting table, not an assumed compaction duration.
		for i := 0; i < 3072; i++ {
			if err := store.RunInTx(func(tx *Txn) error {
				return tx.Set([]byte(fmt.Sprintf("padding/%d/%04d", phase, i)), bytes.Repeat([]byte("x"), 1024))
			}); err != nil {
				t.Fatal(err)
			}
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			ready := false
			for _, table := range db.Tables() {
				ready = ready || table.MaxVersion >= cutoff
			}
			if ready {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("memtable did not flush preceding mutations")
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err := db.Flatten(2); err != nil {
			t.Fatal(err)
		}
	}
	checkCurrent := func() {
		t.Helper()
		for i := 0; i < count; i++ {
			got, _, err := store.DsGet("p", "", "", fmt.Sprintf("Payload/%d", i))
			if i%2 == 1 {
				if status.Code(err) != codes.NotFound {
					t.Fatalf("deleted payload %d: %v", i, err)
				}
			} else if err != nil || !bytes.Equal(got.GetProperties()["payload"].GetBlobValue(), replacement) {
				t.Fatalf("replacement %d: %v", i, err)
			}
		}
	}
	store.advanceDiscardTime(time.Now())
	flush(0)
	if err := db.RunValueLogGC(valueLogGCDiscardRatio); err != nil && !errors.Is(err, badger.ErrNoRewrite) {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		got, err := store.DsGetAsOf("p", "", "", fmt.Sprintf("Payload/%d", i), snapshot)
		if err != nil || !bytes.Equal(got.GetProperties()["payload"].GetBlobValue(), payload) {
			t.Fatalf("pinned payload %d: %v", i, err)
		}
	}
	checkCurrent()
	release()
	store.advanceDiscardTime(time.Now())
	// Overlap the payload table again: disjoint padding alone need not revisit
	// an already-compacted table when the discard watermark changes.
	write(0, replacement)
	flush(1)
	var reclaimed int
	for {
		err := db.RunValueLogGC(valueLogGCDiscardRatio)
		if errors.Is(err, badger.ErrNoRewrite) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		reclaimed++
	}
	if reclaimed == 0 {
		t.Fatal("no obsolete value-log file reclaimed after releasing snapshot")
	}
	checkCurrent()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = badger.OpenManaged(opts)
	if err != nil {
		t.Fatal(err)
	}
	store.db = db
	store.latestTs.Store(db.MaxVersion())
	checkCurrent()
}

func TestExactQueryLimitsDeriveBoundedAccumulatorCapacity(t *testing.T) {
	limits, err := deriveExactQueryLimits(16<<20, 8)
	if err != nil {
		t.Fatal(err)
	}
	if limits.maxAccumulators != 16 {
		t.Fatalf("max accumulators = %d, want 16", limits.maxAccumulators)
	}
	if got, want := int64(limits.accumulatorBytes+mergeBufferReserveBytes(exactQueryMergeFanIn))*int64(limits.maxAccumulators), int64(16<<20); got > want {
		t.Fatalf("accumulator capacity = %d, exceeds workspace %d", got, want)
	}

	if _, err := deriveExactQueryLimits(1, 8); err == nil {
		t.Fatal("expected undersized workspace error")
	}
}

func TestDsGetManyWithTimesParallelPreservesOrder(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	paths := make([]string, 512)
	for i := range paths {
		paths[i] = fmt.Sprintf("CallLeg/%04d", len(paths)-i)
	}
	foundPaths := map[string]bool{paths[0]: true, paths[100]: true, paths[511]: true}
	for path := range foundPaths {
		if _, err := store.DsUpsert(EntityWrite{Project: "project", Database: "(default)", Path: path, Kind: "CallLeg", Entity: &datastorepb.Entity{}}); err != nil {
			t.Fatal(err)
		}
	}

	found, missing, err := store.DsGetManyWithTimes("project", "(default)", "", paths)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != len(foundPaths) || len(missing) != len(paths)-len(foundPaths) {
		t.Fatalf("found/missing = %d/%d, want %d/%d", len(found), len(missing), len(foundPaths), len(paths)-len(foundPaths))
	}
	var foundIndex, missingIndex int
	for _, path := range paths {
		if foundPaths[path] {
			if found[foundIndex].Path != path {
				t.Fatalf("found[%d] = %q, want %q", foundIndex, found[foundIndex].Path, path)
			}
			foundIndex++
			continue
		}
		if missing[missingIndex] != path {
			t.Fatalf("missing[%d] = %q, want %q", missingIndex, missing[missingIndex], path)
		}
		missingIndex++
	}
}

func TestListDsKindsReturnsDistinctSortedKinds(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i, kind := range []string{"Zulu", "Alpha", "Zulu"} {
		path := fmt.Sprintf("%s/%d", kind, i)
		if _, err := store.DsUpsert(EntityWrite{Project: "project", Database: "(default)", Path: path, Kind: kind, Entity: &datastorepb.Entity{}}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.ListDsKinds("project")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"Alpha", "Zulu"}) {
		t.Fatalf("kinds = %v", got)
	}
}

func BenchmarkPathAccumulatorBuffer(b *testing.B) {
	type workload struct {
		name       string
		paths      []string
		uniquePath int
	}
	buildPaths := func(count, width, duplicates int) []string {
		paths := make([]string, count)
		for i := range paths {
			paths[i] = fmt.Sprintf("Widget/%0*d", width, i/duplicates)
		}
		return paths
	}
	workloads := []workload{
		{name: "short_unique", paths: buildPaths(50_000, 8, 1), uniquePath: 50_000},
		{name: "typical_half_duplicate", paths: buildPaths(50_000, 120, 2), uniquePath: 25_000},
		{name: "max_path_high_duplicate", paths: buildPaths(5_000, maxAccumulatedPathBytes-len("Widget/"), 20), uniquePath: 250},
	}
	for _, workload := range workloads {
		for _, bufferBytes := range []int{256 << 10, 1 << 20, 4 << 20, 8 << 20} {
			b.Run(fmt.Sprintf("%s/buffer_%dKiB", workload.name, bufferBytes>>10), func(b *testing.B) {
				root := b.TempDir()
				b.ReportAllocs()
				b.ReportMetric(float64(len(workload.paths)), "paths/op")
				b.ReportMetric(float64(bufferBytes), "buffer-B")
				for range b.N {
					accumulator, err := newPathAccumulator(context.Background(), root, bufferBytes)
					if err != nil {
						b.Fatal(err)
					}
					for _, path := range workload.paths {
						if err := accumulator.Add(path); err != nil {
							b.Fatal(err)
						}
					}
					count, err := accumulator.Count()
					if err != nil {
						b.Fatal(err)
					}
					if count != int64(workload.uniquePath) {
						b.Fatalf("count = %d, want %d", count, workload.uniquePath)
					}
					if err := accumulator.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkPathAccumulatorMergeFanIn(b *testing.B) {
	paths := make([]string, 100_000)
	for i := range paths {
		paths[i] = fmt.Sprintf("Widget/%0112d", i/2)
	}
	for _, fanIn := range []int{8, 16, 32, 64, 128} {
		b.Run(fmt.Sprintf("fan_in_%d", fanIn), func(b *testing.B) {
			root := b.TempDir()
			b.ReportAllocs()
			b.ReportMetric(float64(fanIn), "merge-fan-in")
			for range b.N {
				accumulator, err := newPathAccumulatorWithFanIn(context.Background(), root, 64<<10, fanIn)
				if err != nil {
					b.Fatal(err)
				}
				for _, path := range paths {
					if err := accumulator.Add(path); err != nil {
						b.Fatal(err)
					}
				}
				count, err := accumulator.Count()
				if err != nil {
					b.Fatal(err)
				}
				if count != 50_000 {
					b.Fatalf("count = %d, want 50000", count)
				}
				if err := accumulator.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
