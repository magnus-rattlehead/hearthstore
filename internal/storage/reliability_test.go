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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestUpdatePreservesCorruptRecord(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	path := reviewNamedPath("Recovery", "0")
	key := dsKey("project", "", "", path)
	const corrupt = "invalid"
	if err := s.RunInTxCtx(context.Background(), func(tx *Txn) error {
		return tx.Set(key, []byte(corrupt))
	}); err != nil {
		t.Fatal(err)
	}
	for _, transactional := range []bool{false, true} {
		t.Run(fmt.Sprintf("transactional=%v", transactional), func(t *testing.T) {
			if transactional {
				err = s.RunInTxCtx(context.Background(), func(tx *Txn) error {
					_, err := s.DsUpdateTx(tx, EntityWrite{Project: "project", Path: path, Entity: reliabilityEntity("0", 2)}, NewCommitAccumulator())
					return err
				})
			} else {
				_, err = s.DsUpdate(EntityWrite{Project: "project", Path: path, Entity: reliabilityEntity("0", 2)})
			}
			if err == nil || err.Error() != "invalid storage record header" {
				t.Errorf("update error = %v, want stored-record corruption", err)
			}
			if err := s.view(func(tx *Txn) error {
				data, exists, err := rawValueTxn(tx, key)
				if err == nil && (!exists || string(data) != corrupt) {
					t.Errorf("failed update changed stored record: %q", data)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTransactionCancellationBeforeCommit(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = s.RunInTxCtx(ctx, func(tx *Txn) error {
		if err := tx.Set([]byte("test/cancel"), []byte("uncommitted")); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("commit error = %v, want cancellation", err)
	}
	if err := s.view(func(tx *Txn) error {
		_, err := tx.Get([]byte("test/cancel"))
		if !errors.Is(err, badger.ErrKeyNotFound) {
			t.Errorf("cancelled write exists: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type commitWaitContext struct {
	context.Context
	checks   int
	prepared chan struct{}
}

func (c *commitWaitContext) Err() error {
	err := c.Context.Err()
	c.checks++
	if c.checks == 2 {
		close(c.prepared)
	}
	return err
}

func TestTransactionCancellationWhileWaitingForCommitLock(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prepared := make(chan struct{})
	waiting := &commitWaitContext{Context: ctx, prepared: prepared}
	s.commitMu.Lock()
	result := make(chan error, 1)
	go func() {
		result <- s.RunInTxCtx(waiting, func(tx *Txn) error { return tx.Set([]byte("test/wait"), []byte("pending")) })
	}()
	select {
	case <-prepared:
	case <-time.After(5 * time.Second):
		s.commitMu.Unlock()
		t.Fatal("transaction did not reach commit preparation")
	}
	cancel()
	s.commitMu.Unlock()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter committed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled transaction remained blocked")
	}
	if err := s.view(func(tx *Txn) error {
		_, err := tx.Get([]byte("test/wait"))
		if !errors.Is(err, badger.ErrKeyNotFound) {
			t.Errorf("cancelled waiter wrote data: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestImportCancellationRestoresCommittedBatches(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := s.DsUpsert(EntityWrite{Project: "project", Path: reviewNamedPath("Recovery", "0"), Kind: "Recovery", Entity: reliabilityEntity("0", 1)}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = s.DsImportAtomic(ctx, "project", "", func(yield func(*datastorepb.Entity) error) error {
		for i := range 128 {
			if err := yield(reliabilityEntity(fmt.Sprint(i), 2)); err != nil {
				return err
			}
		}
		cancel()
		return nil // Cancellation after the final yield must still abort publication.
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	count, _, err := s.DsCountBuiltin(context.Background(), "project", "", "", "Recovery", "value", "", nil)
	if err != nil || count != 1 {
		t.Fatalf("rollback count=%d err=%v", count, err)
	}
	entity, version, err := s.DsGet("project", "", "", reviewNamedPath("Recovery", "0"))
	if err != nil || version != 1 || entity.GetProperties()["value"].GetIntegerValue() != 1 {
		t.Fatalf("original not restored: version=%d err=%v", version, err)
	}
}

func TestImportCommitPublicationErrorDoesNotGuessOutcome(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprint(applied), func(t *testing.T) {
			dir := t.TempDir()
			s, err := New(dir)
			if err != nil {
				t.Fatal(err)
			}
			realCommit := s.commitTxn
			fault := errors.New("commit publication failed")
			s.commitTxn = func(tx *Txn, ts uint64) error {
				item, err := tx.Get([]byte(importJournalStateKey))
				if err == nil {
					raw, err := itemValue(item)
					if err != nil {
						return err
					}
					var state importJournalState
					if err := json.Unmarshal(raw, &state); err != nil {
						return err
					}
					if state.Status == importStateCommitted {
						if applied {
							if err := realCommit(tx, ts); err != nil {
								return err
							}
						}
						return fault
					}
				} else if !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
				return realCommit(tx, ts)
			}
			err = s.DsImportAtomic(context.Background(), "project", "", func(yield func(*datastorepb.Entity) error) error { return yield(reliabilityEntity("one", 2)) })
			if !errors.Is(err, fault) || status.Code(s.CheckAvailable()) != codes.FailedPrecondition {
				t.Fatalf("uncertain publication served data: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = New(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			}()
			entity, _, err := s.DsGet("project", "", "", reviewNamedPath("Recovery", "one"))
			if applied {
				if err != nil || entity.GetProperties()["value"].GetIntegerValue() != 2 {
					t.Fatalf("committed outcome lost: %v", err)
				}
			} else if status.Code(err) != codes.NotFound {
				t.Fatalf("uncommitted outcome not rolled back: %v", err)
			}
		})
	}
}

func TestNontransactionalBatchFailureRetainsCommittedChunks(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	realCommit, calls := s.commitTxn, 0
	fault := errors.New("later chunk write failed")
	s.commitTxn = func(tx *Txn, ts uint64) error {
		calls++
		if calls == 1 {
			return badger.ErrTxnTooBig
		}
		if calls == 3 {
			return fault
		}
		return realCommit(tx, ts)
	}
	rows := make([]UpsertManyRow, 4)
	for i := range rows {
		rows[i] = UpsertManyRow{Path: reviewNamedPath("Recovery", fmt.Sprint(i)), Kind: "Recovery", Entity: reliabilityEntity(fmt.Sprint(i), 1)}
	}
	results, err := s.DsUpsertMany(context.Background(), "project", "", rows, timestamppb.Now())
	if !errors.Is(err, fault) || results != nil {
		t.Fatalf("partial success: results=%v err=%v", results, err)
	}
	for i, row := range rows {
		_, _, err := s.DsGet("project", "", "", row.Path)
		if i < 2 && err != nil || i >= 2 && status.Code(err) != codes.NotFound {
			t.Fatalf("chunk result %d: %v", i, err)
		}
	}
}

func TestIndexBuildErrorsReachWaiters(t *testing.T) {
	for _, failRecording := range []bool{false, true} {
		t.Run(fmt.Sprint(failRecording), func(t *testing.T) {
			s, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			}()
			if _, err := s.DsUpsert(EntityWrite{Project: "project", Path: reviewNamedPath("Recovery", "0"), Kind: "Recovery", Entity: reliabilityEntity("0", 1)}); err != nil {
				t.Fatal(err)
			}
			realCommit, calls := s.commitTxn, 0
			buildErr, recordErr := errors.New("index entry write failed"), errors.New("index error-state write failed")
			s.commitTxn = func(tx *Txn, ts uint64) error {
				calls++
				if calls == 3 {
					return buildErr
				}
				if calls == 4 && failRecording {
					return recordErr
				}
				return realCommit(tx, ts)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			idx, _, err := s.EnsureDsCompositeIndex(ctx, recoveryIndex(false), true)
			if failRecording {
				if !errors.Is(err, buildErr) || !errors.Is(err, recordErr) {
					t.Fatalf("lost build/recording error: %v", err)
				}
			} else if err != nil || idx.State != DsIndexError || !strings.Contains(idx.Error, buildErr.Error()) {
				t.Fatalf("failure not reported: state=%s detail=%s err=%v", idx.State, idx.Error, err)
			}
			definition := recoveryIndex(false)
			definition.Properties[0].Desc = true
			definition.ID = DsCompositeIndexID(definition.Kind, false, definition.Properties)
			idx, _, err = s.EnsureDsCompositeIndex(ctx, definition, true)
			if err != nil || idx.State != DsIndexReady {
				t.Fatalf("failed build leaked admission: state=%s err=%v", idx.State, err)
			}
		})
	}
}

func TestImportPersistenceFailureOutcomes(t *testing.T) {
	for _, phase := range []string{"rollback", "publication", "cleanup"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			s, err := New(dir)
			if err != nil {
				t.Fatal(err)
			}
			fault := errors.New("injected sync failure")
			calls := 0
			realSync := s.syncDB
			s.syncDB = func() error {
				calls++
				failAt := map[string]int{"rollback": 2, "publication": 3, "cleanup": 4}[phase]
				if calls == failAt {
					return fault
				}
				return realSync()
			}
			err = s.DsImportAtomic(context.Background(), "project", "", func(yield func(*datastorepb.Entity) error) error {
				return yield(reliabilityEntity("one", 2))
			})
			if !errors.Is(err, fault) {
				t.Fatalf("lost sync error: %v", err)
			}
			_, _, readErr := s.DsGet("project", "", "", reviewNamedPath("Recovery", "one"))
			switch phase {
			case "rollback":
				if status.Code(readErr) != codes.NotFound {
					t.Errorf("failed pre-publication import visible: %v", readErr)
				}
			case "publication":
				if status.Code(readErr) != codes.FailedPrecondition {
					t.Errorf("uncertain import not quarantined: %v", readErr)
				}
			case "cleanup":
				if readErr != nil || !strings.Contains(err.Error(), "committed") {
					t.Errorf("committed outcome lost: read=%v import=%v", readErr, err)
				}
			}
			s.syncDB = realSync
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = New(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			}()
			entity, _, err := s.DsGet("project", "", "", reviewNamedPath("Recovery", "one"))
			if phase == "rollback" {
				if status.Code(err) != codes.NotFound {
					t.Fatalf("rolled-back entity reappeared: %v", err)
				}
			} else if err != nil || entity.GetProperties()["value"].GetIntegerValue() != 2 {
				t.Fatalf("published entity not retained: entity=%v err=%v", entity, err)
			}
		})
	}
}

func TestImportCleanupFailureDoesNotMisreportNextImport(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	realSync, realCommit := s.syncDB, s.commitTxn
	syncs := 0
	fault := errors.New("journal cleanup write failed")
	s.syncDB = func() error {
		if err := realSync(); err != nil {
			return err
		}
		syncs++
		if syncs == 3 {
			s.commitTxn = func(*Txn, uint64) error { return fault }
		}
		return nil
	}
	err = s.DsImportAtomic(context.Background(), "project", "", func(yield func(*datastorepb.Entity) error) error { return yield(reliabilityEntity("one", 2)) })
	var outcome *ImportError
	if !errors.As(err, &outcome) || outcome.Outcome != "committed" || !errors.Is(err, fault) {
		t.Fatalf("first outcome=%v", err)
	}
	err = s.DsImportAtomic(context.Background(), "project", "", func(func(*datastorepb.Entity) error) error {
		t.Error("new source read before cleanup succeeded")
		return nil
	})
	if !errors.As(err, &outcome) || outcome.Outcome != "not started" || !errors.Is(err, fault) {
		t.Fatalf("new outcome=%v", err)
	}
	entity, _, err := s.DsGet("project", "", "", reviewNamedPath("Recovery", "one"))
	if err != nil || entity.GetProperties()["value"].GetIntegerValue() != 2 {
		t.Fatalf("known committed data blocked or lost: %v", err)
	}
	s.syncDB, s.commitTxn = realSync, realCommit
	if err := s.DsImportAtomic(context.Background(), "project", "", func(yield func(*datastorepb.Entity) error) error { return yield(reliabilityEntity("two", 3)) }); err != nil {
		t.Fatal(err)
	}
	count, _, err := s.DsCountBuiltin(context.Background(), "project", "", "", "Recovery", "value", "", nil)
	if err != nil || count != 2 {
		t.Fatalf("cleanup retry/import lost data: count=%d err=%v", count, err)
	}
}

func reliabilityEntity(name string, value int64) *datastorepb.Entity {
	return &datastorepb.Entity{
		Key:        &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: "project"}, Path: []*datastorepb.Key_PathElement{{Kind: "Recovery", IdType: &datastorepb.Key_PathElement_Name{Name: name}}}},
		Properties: map[string]*datastorepb.Value{"value": {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: value}}},
	}
}

func TestImportRollbackFailureBlocksAccess(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	applyErr := errors.New("source failed")
	err = s.DsImportAtomic(context.Background(), "project", "", func(yield func(*datastorepb.Entity) error) error {
		// A full input batch ensures there is committed work to undo.
		for i := range 128 {
			if err := yield(reliabilityEntity(fmt.Sprint(i), 2)); err != nil {
				return err
			}
		}
		if err := s.runUpdateRawUnlocked(context.Background(), false, func(tx *Txn) error {
			return tx.Set([]byte(importJournalEntryKey+"broken"), []byte("invalid journal"))
		}); err != nil {
			return err
		}
		return applyErr
	})
	if !errors.Is(err, applyErr) {
		t.Errorf("lost original error: %v", err)
	}
	_, _, readErr := s.DsGet("project", "", "", reviewNamedPath("Recovery", "0"))
	if status.Code(readErr) != codes.FailedPrecondition {
		t.Errorf("read after failed rollback = %v", readErr)
	}
	writeErr := s.RunInTx(func(tx *Txn) error { return tx.Set([]byte("test/unsafe"), []byte("unsafe")) })
	if status.Code(writeErr) != codes.FailedPrecondition {
		t.Errorf("write after failed rollback = %v", writeErr)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(dir)
	if err == nil {
		_ = reopened.Close()
		t.Fatal("startup accepted an unrecoverable journal")
	}
}
