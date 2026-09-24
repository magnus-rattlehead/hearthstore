package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/dgraph-io/badger/v4"
	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
)

// Every child owns only a parent-created disposable directory. Pipes establish
// the exact crash point; no timer guesses when a batch has reached storage.
func crashAt(t *testing.T, dir, phase string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestRecoveryCrashProcess$", "--", "--recovery-child", dir, phase)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stdin.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Error(err)
		}
	}()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	ready := false
	var output strings.Builder
	for scanner.Scan() {
		if scanner.Text() == "READY" {
			ready = true
			break
		}
		output.WriteString(scanner.Text() + "\n")
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Error(err)
	}
	waitErr := cmd.Wait()
	if !ready || waitErr == nil || ctx.Err() != nil {
		t.Fatalf("child %s missed crash boundary: ready=%v wait=%v context=%v stdout=%s stderr=%s", phase, ready, waitErr, ctx.Err(), output.String(), stderr.String())
	}
}

func crashDirectory(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".recovery-test-owned"), []byte("disposable"), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestImportProcessCrashRecovery(t *testing.T) {
	for _, phase := range []string{"applying", "rollback", "committed", "cleanup", "repeated-recovery"} {
		t.Run(phase, func(t *testing.T) {
			dir := crashDirectory(t)
			if phase == "repeated-recovery" {
				crashAt(t, dir, "applying")
				crashAt(t, dir, "recover-rollback")
				crashAt(t, dir, "recover-rollback")
			} else {
				crashAt(t, dir, phase)
			}
			s, err := NewWithOptions(dir, OpenOptions{BlockCacheBytes: 8 << 20, IndexCacheBytes: 4 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			}()
			committed := phase == "committed" || phase == "cleanup"
			wantCount, wantValue := int64(2), int64(1)
			if committed {
				wantCount, wantValue = 129, 2
			}
			entity, version, err := s.DsGet("project", "", "", reviewNamedPath("Recovery", "0"))
			if err != nil || entity.GetProperties()["value"].GetIntegerValue() != wantValue || version != wantValue {
				t.Fatalf("overwrite recovery: value=%v version=%d err=%v", entity, version, err)
			}
			untouched, _, err := s.DsGet("project", "", "", reviewNamedPath("Recovery", "keep"))
			if err != nil || untouched.GetProperties()["value"].GetIntegerValue() != 42 {
				t.Fatalf("unrelated entity changed: %v", err)
			}
			count, _, err := s.DsCountBuiltin(context.Background(), "project", "", "", "Recovery", "value", "", nil)
			if err != nil || count != wantCount {
				t.Fatalf("builtin count=%d want=%d err=%v", count, wantCount, err)
			}
			index := recoveryIndex(false)
			page, err := s.DsQueryComposite(context.Background(), CompositeQuery{Project: "project", IndexID: index.ID, Limit: 200})
			rows := page.Rows
			if err != nil || int64(len(rows)) != wantCount {
				t.Fatalf("composite rows=%d want=%d err=%v", len(rows), wantCount, err)
			}
			var names []string
			_, err = s.DsVisitMetadataAsOf(context.Background(), s.ReadTime(), "project", "", "", "__property__", func(row *DsEntityRow) error {
				names = append(names, row.Entity.Key.Path[1].GetName())
				return nil
			})
			if err != nil || slices.Contains(names, "imported") != committed || slices.Contains(names, "original") == committed {
				t.Fatalf("property metadata=%v committed=%v err=%v", names, committed, err)
			}
			ids, err := s.DsAllocateIDBlock(context.Background(), "project", "", "", "Recovery", []string{""})
			if err != nil {
				t.Fatal(err)
			}
			counter, valid := scatteredCounter(ids[0])
			if !valid || committed && counter <= 100 || !committed && counter != 1 {
				t.Fatalf("ID reservation counter=%d committed=%v", counter, committed)
			}
			if _, found, err := s.readImportState(); found || err != nil {
				t.Fatalf("journal remains: found=%v err=%v", found, err)
			}
		})
	}
}

func recoveryIndex(stream bool) DsCompositeIndex {
	properties := []DsIndexProperty{{Name: "value"}}
	if stream {
		properties = []DsIndexProperty{{Name: strings.Repeat("x", 1000)}, {Name: strings.Repeat("y", 1000)}}
	}
	return DsCompositeIndex{Project: "project", ID: DsCompositeIndexID("Recovery", false, properties), Kind: "Recovery", Properties: properties, State: DsIndexCreating, BuildingGeneration: 1}
}

func TestIndexProcessCrashRecovery(t *testing.T) {
	for _, phase := range []string{"index-batch", "index-stream", "index-ready", "index-delete"} {
		t.Run(phase, func(t *testing.T) {
			dir := crashDirectory(t)
			crashAt(t, dir, phase)
			s, err := NewWithOptions(dir, OpenOptions{BlockCacheBytes: 8 << 20, IndexCacheBytes: 4 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			}()
			definition := recoveryIndex(phase == "index-stream")
			idx, err := s.GetDsCompositeIndex("project", definition.ID)
			if err != nil {
				t.Fatal(err)
			}
			if phase != "index-ready" {
				if idx.State == DsIndexReady {
					t.Fatal("unfinished generation advertised READY")
				}
				page, err := s.DsQueryComposite(context.Background(), CompositeQuery{Project: "project", IndexID: idx.ID, Limit: 10})
				rows := page.Rows
				if err == nil || len(rows) != 0 {
					t.Fatalf("unfinished index served rows: %d err=%v", len(rows), err)
				}
			}
			if phase == "index-delete" {
				if err := s.DeleteDsCompositeIndex(context.Background(), "project", idx.ID); err != nil {
					t.Fatal(err)
				}
				if _, err := s.GetDsCompositeIndex("project", idx.ID); !errors.Is(err, ErrIndexNotFound) {
					t.Fatalf("deleted index exists: %v", err)
				}
				return
			}
			idx, _, err = s.EnsureDsCompositeIndex(context.Background(), definition, true)
			if err != nil || idx.State != DsIndexReady {
				t.Fatalf("resume state=%s err=%v", idx.State, err)
			}
			page, err := s.DsQueryComposite(context.Background(), CompositeQuery{Project: "project", IndexID: idx.ID, Limit: 10})
			rows := page.Rows
			if err != nil || len(rows) != 1 {
				t.Fatalf("resumed rows=%d err=%v", len(rows), err)
			}
			wantEntries := 1
			if phase == "index-stream" {
				wantEntries = 10000
			}
			entries := 0
			err = s.view(func(tx *Txn) error {
				it := tx.NewIterator(badger.DefaultIteratorOptions)
				defer it.Close()
				prefix := []byte(compositeBase(idx, "", "", idx.ActiveGeneration))
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					entries++
				}
				return nil
			})
			if err != nil || entries != wantEntries {
				t.Fatalf("entries=%d want=%d err=%v", entries, wantEntries, err)
			}
		})
	}
}

func TestRecoveryCrashProcess(t *testing.T) {
	arg := slices.Index(os.Args, "--recovery-child")
	if arg < 0 {
		return
	}
	if len(os.Args) != arg+3 {
		t.Fatal("invalid child arguments")
	}
	dir, phase := os.Args[arg+1], os.Args[arg+2]
	marker, err := os.ReadFile(filepath.Join(dir, ".recovery-test-owned"))
	if err != nil || string(marker) != "disposable" || !filepath.IsAbs(dir) {
		t.Fatal("child requires an owned temporary directory")
	}
	var s *Store
	if phase == "recover-rollback" {
		// Open the engine without automatic recovery so the same recovery routine
		// used at startup can be interrupted through its persistence dependency.
		dbDir := filepath.Join(dir, "badger")
		prior, err := os.ReadDir(dbDir)
		if err != nil {
			t.Fatal(err)
		}
		db, err := badger.OpenManaged(badgerOptions(dbDir).WithBlockCacheSize(8 << 20).WithIndexCacheSize(4 << 20))
		if err != nil {
			t.Fatal(err)
		}
		// Isolate Hearthstore's rollback boundary from Badger's independent
		// startup WAL deletion (truncate-before-unlink); see the reliability report.
		deadline := time.Now().Add(5 * time.Second)
		for _, entry := range prior {
			if !strings.HasSuffix(entry.Name(), ".mem") {
				continue
			}
			for {
				_, err := os.Stat(filepath.Join(dbDir, entry.Name()))
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if time.Now().After(deadline) {
					t.Fatal("replayed WAL was not retired")
				}
				time.Sleep(10 * time.Millisecond) // Poll the actual retirement condition.
			}
		}
		s = &Store{db: db, syncDB: db.Sync, commitTxn: func(tx *Txn, ts uint64) error { return tx.CommitAt(ts, nil) }, gcWake: make(chan struct{}, 1)}
		s.latestTs.Store(db.MaxVersion())
	} else {
		s, err = NewWithOptions(dir, OpenOptions{BlockCacheBytes: 8 << 20, IndexCacheBytes: 4 << 20})
		if err != nil {
			t.Fatal(err)
		}
	}
	pause := func() {
		if err := s.db.Sync(); err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprintln(os.Stdout, "READY"); err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
			t.Fatal(err)
		}
		t.Fatal("parent closed pipe without killing child")
	}
	realCommit := s.commitTxn
	stopAfterCommit := func(tx *Txn, ts uint64) error {
		if err := realCommit(tx, ts); err != nil {
			return err
		}
		pause()
		return nil
	}
	if phase == "recover-rollback" {
		s.commitTxn = stopAfterCommit
		if err := s.recoverImport(); err != nil {
			t.Fatal(err)
		}
		t.Fatal("recovery did not write")
	}
	insert := func(entity *datastorepb.Entity) {
		_, err := s.DsUpsert(EntityWrite{Project: "project", Path: keycodec.Path(entity.Key.Path), Kind: "Recovery", Entity: entity})
		if err != nil {
			t.Fatal(err)
		}
	}
	index := recoveryIndex(phase == "index-stream")
	if strings.HasPrefix(phase, "index-") {
		entity := reliabilityEntity("0", 1)
		if phase == "index-stream" {
			entity.Properties = map[string]*datastorepb.Value{}
			for _, property := range index.Properties {
				values := make([]*datastorepb.Value, 100)
				for i := range values {
					values[i] = &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: int64(i)}}
				}
				entity.Properties[property.Name] = &datastorepb.Value{ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: values}}}
			}
		}
		insert(entity)
		raw, err := json.Marshal(index)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.RunInTx(func(tx *Txn) error { return tx.Set(indexKey("project", index.ID), raw) }); err != nil {
			t.Fatal(err)
		}
		commits := 0
		s.commitTxn = func(tx *Txn, ts uint64) error {
			item, err := tx.Get(indexKey("project", index.ID))
			if err != nil {
				return err
			}
			raw, err := itemValue(item)
			if err != nil {
				return err
			}
			var current DsCompositeIndex
			if err := json.Unmarshal(raw, &current); err != nil {
				return err
			}
			if err := realCommit(tx, ts); err != nil {
				return err
			}
			commits++
			if (phase == "index-batch" || phase == "index-stream") && commits == 2 || phase == "index-ready" && current.State == DsIndexReady {
				pause()
			}
			return nil
		}
		if err := s.BuildDsCompositeIndex(context.Background(), "project", index.ID); err != nil {
			t.Fatal(err)
		}
		if phase == "index-delete" {
			s.commitTxn = stopAfterCommit
			if err := s.DeleteDsCompositeIndex(context.Background(), "project", index.ID); err != nil {
				t.Fatal(err)
			}
		}
		t.Fatal("index boundary was not reached")
	}
	original := reliabilityEntity("0", 1)
	original.Properties["original"] = &datastorepb.Value{ValueType: &datastorepb.Value_BooleanValue{BooleanValue: true}}
	insert(original)
	insert(reliabilityEntity("keep", 42))
	if _, _, err := s.EnsureDsCompositeIndex(context.Background(), index, true); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	realSync, syncs := s.syncDB, 0
	s.syncDB = func() error {
		if err := realSync(); err != nil {
			return err
		}
		syncs++
		if syncs == 3 {
			if phase == "committed" {
				pause()
			}
			if phase == "cleanup" {
				s.commitTxn = stopAfterCommit
			}
		}
		return nil
	}
	err = s.DsImportAtomic(context.Background(), "project", "", func(yield func(*datastorepb.Entity) error) error {
		for i := range 128 {
			entity := reliabilityEntity(fmt.Sprint(i), 2)
			entity.Properties["imported"] = &datastorepb.Value{ValueType: &datastorepb.Value_BooleanValue{BooleanValue: true}}
			if i == 127 {
				id, err := scatteredID(100)
				if err != nil {
					return err
				}
				entity.Key.Path[0].IdType = &datastorepb.Key_PathElement_Id{Id: id}
			}
			if err := yield(entity); err != nil {
				return err
			}
		}
		if phase == "applying" {
			pause()
		}
		if phase == "rollback" {
			s.commitTxn = stopAfterCommit
			return errors.New("source interrupted")
		}
		return nil
	})
	t.Fatalf("import boundary not reached: %v", err)
}
