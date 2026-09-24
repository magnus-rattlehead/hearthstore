package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/dgraph-io/badger/v4"

	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
)

func TestPropertyMetadataImportRecovery(t *testing.T) {
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
	entity := &datastorepb.Entity{Key: &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: "project"}, Path: []*datastorepb.Key_PathElement{{Kind: "Properties", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}, Properties: map[string]*datastorepb.Value{"original": {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 1}}}}
	if _, err := store.DsUpsert(EntityWrite{Project: "project", Path: keycodec.Path(entity.Key.Path), Kind: "Properties", Entity: entity}); err != nil {
		t.Fatal(err)
	}
	read := func() []string {
		var names []string
		_, err := store.DsVisitMetadataAsOf(context.Background(), store.ReadTime(), "project", "", "", "__property__", func(row *DsEntityRow) error {
			names = append(names, row.Entity.Key.Path[1].GetName())
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return names
	}
	// Journal and apply an interrupted import, then reopen to exercise startup
	// recovery rather than only the in-process rollback callback.
	state := importJournalState{Status: importStateApplying, Project: "project", Database: ""}
	if err := store.writeImportState(state); err != nil {
		t.Fatal(err)
	}
	entity.Properties = map[string]*datastorepb.Value{"temporary": {ValueType: &datastorepb.Value_StringValue{StringValue: "discard"}}}
	if err := store.runUpdateRawUnlocked(context.Background(), false, func(tx *Txn) error {
		_, err := store.applyImportedEntity(tx, "project", "", entity, 0, NewCommitAccumulator())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if got := read(); !reflect.DeepEqual(got, []string{"temporary"}) {
		t.Fatalf("during import: %v", got)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = nil
	store, err = New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := read(); !reflect.DeepEqual(got, []string{"original"}) {
		t.Fatalf("after recovery: %v", got)
	}
	if err := store.DsImportAtomic(context.Background(), "project", "", func(yield func(*datastorepb.Entity) error) error { return yield(entity) }); err != nil {
		t.Fatal(err)
	}
	if got := read(); !reflect.DeepEqual(got, []string{"temporary"}) {
		t.Fatalf("after successful import: %v", got)
	}
}

func TestDsImportSplitsIndexAmplifiedTransactions(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var entities []*datastorepb.Entity
	for i := 0; i < 32; i++ {
		array := &datastorepb.ArrayValue{}
		for value := 0; value < 128; value++ {
			array.Values = append(array.Values, &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: fmt.Sprintf("%04d%s", value, strings.Repeat("x", 900))}})
		}
		entities = append(entities, &datastorepb.Entity{Key: &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: "project"}, Path: []*datastorepb.Key_PathElement{{Kind: "ImportFanout", IdType: &datastorepb.Key_PathElement_Name{Name: fmt.Sprint(i)}}}}, Properties: map[string]*datastorepb.Value{"values": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: array}}}})
	}
	// Prove this fixture really exceeds Badger's transaction limit even though
	// its protobuf payload fits the import input-buffer budget.
	err = store.runUpdateRawUnlocked(context.Background(), false, func(tx *Txn) error {
		var next int64
		acc := NewCommitAccumulator()
		for _, entity := range entities {
			added, err := store.applyImportedEntity(tx, "project", "", entity, next, acc)
			if err != nil {
				return err
			}
			next += added
		}
		return nil
	})
	if !errors.Is(err, badger.ErrTxnTooBig) {
		t.Fatalf("unsplit import error=%v, want ErrTxnTooBig", err)
	}
	if err := store.DsImportAtomic(context.Background(), "project", "", func(yield func(*datastorepb.Entity) error) error {
		for _, entity := range entities {
			if err := yield(entity); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := range entities {
		entity, _, err := store.DsGet("project", "", "", reviewNamedPath("ImportFanout", fmt.Sprint(i)))
		if err != nil || len(entity.GetProperties()["values"].GetArrayValue().GetValues()) != 128 {
			t.Fatalf("entity %d missing after split: %v", i, err)
		}
	}
}

func TestDsImportRetryAfterDuplicateRollback(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// Sources may reuse an entity object after yield returns.
	entity := &datastorepb.Entity{Key: &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: "project"},
		Path:        []*datastorepb.Key_PathElement{{Kind: "Widget"}},
	}}
	source := func(duplicate bool) func(func(*datastorepb.Entity) error) error {
		return func(yield func(*datastorepb.Entity) error) error {
			for i := 0; i < 300; i++ {
				entity.Key.Path[0].IdType = &datastorepb.Key_PathElement_Name{Name: fmt.Sprint(i)}
				if err := yield(entity); err != nil {
					return err
				}
			}
			if duplicate {
				entity.Key.Path[0].IdType = &datastorepb.Key_PathElement_Name{Name: "0"}
				return yield(entity)
			}
			return nil
		}
	}
	if err := store.DsImportAtomic(context.Background(), "project", "", source(true)); err == nil {
		t.Fatal("duplicate import succeeded")
	}
	if _, _, err := store.DsGet("project", "", "", reviewNamedPath("Widget", "0")); err == nil {
		t.Fatal("rolled-back entity remained")
	}
	if kinds, err := store.ListDsKinds("project"); err != nil || len(kinds) != 0 {
		t.Fatalf("rolled-back import left metadata: %v, %v", kinds, err)
	}
	if err := store.DsImportAtomic(context.Background(), "project", "", source(false)); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	for i := 0; i < 300; i++ {
		if _, _, err := store.DsGet("project", "", "", reviewNamedPath("Widget", fmt.Sprint(i))); err != nil {
			t.Fatalf("entity %d missing: %v", i, err)
		}
	}
}

func TestDsVisitAllEntitiesUsesOneProjectDatabaseSnapshot(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	put := func(project, database, namespace, kind, name string) {
		t.Helper()
		key := &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: project, DatabaseId: database, NamespaceId: namespace}, Path: []*datastorepb.Key_PathElement{{Kind: kind, IdType: &datastorepb.Key_PathElement_Name{Name: name}}}}
		_, err := store.DsUpsert(EntityWrite{Project: project, Database: database, Namespace: namespace, Path: reviewNamedPath(kind, name), Kind: kind, Entity: &datastorepb.Entity{Key: key}})
		if err != nil {
			t.Fatal(err)
		}
	}
	put("project", "database", "", "Widget", "one")
	put("project", "database", "tenant", "Widget", "two")
	put("other", "database", "", "Widget", "ignored")

	var namespaces []string
	err = store.DsVisitAllEntities(context.Background(), "project", "database", func(namespace string, row *DsEntityRow) error {
		namespaces = append(namespaces, namespace)
		if len(namespaces) == 1 {
			put("project", "database", "", "Widget", "created-during-export")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(namespaces) != 2 {
		t.Fatalf("visited %d entities, want 2", len(namespaces))
	}
}

func TestDsImportAtomicRollsBackAndReservesImportedIDs(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	entity := func(name, value string, id int64) *datastorepb.Entity {
		key := &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: "project", DatabaseId: "database"}, Path: []*datastorepb.Key_PathElement{{Kind: "Widget"}}}
		if id != 0 {
			key.Path[0].IdType = &datastorepb.Key_PathElement_Id{Id: id}
		} else {
			key.Path[0].IdType = &datastorepb.Key_PathElement_Name{Name: name}
		}
		return &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"value": {ValueType: &datastorepb.Value_StringValue{StringValue: value}}}}
	}
	original := entity("one", "original", 0)
	if _, err := store.DsUpsert(EntityWrite{Project: "project", Database: "database", Path: reviewNamedPath("Widget", "one"), Kind: "Widget", Entity: original}); err != nil {
		t.Fatal(err)
	}
	_, originalVersion, originalCreate, originalUpdate, err := store.DsGetWithTimes("project", "database", "", reviewNamedPath("Widget", "one"))
	if err != nil {
		t.Fatal(err)
	}

	injected := errors.New("injected import failure")
	err = store.DsImportAtomic(context.Background(), "project", "database", func(yield func(*datastorepb.Entity) error) error {
		if err := yield(entity("one", "replacement", 0)); err != nil {
			return err
		}
		if err := yield(entity("two", "new", 0)); err != nil {
			return err
		}
		return injected
	})
	if !errors.Is(err, injected) {
		t.Fatalf("import error = %v, want injected failure", err)
	}
	restored, version, created, updated, err := store.DsGetWithTimes("project", "database", "", reviewNamedPath("Widget", "one"))
	if err != nil {
		t.Fatal(err)
	}
	if restored.GetProperties()["value"].GetStringValue() != "original" || version != originalVersion || !created.AsTime().Equal(originalCreate.AsTime()) || !updated.AsTime().Equal(originalUpdate.AsTime()) {
		t.Fatalf("original entity metadata was not restored: entity=%v version=%d created=%v updated=%v", restored, version, created, updated)
	}
	if _, _, err := store.DsGet("project", "database", "", reviewNamedPath("Widget", "two")); err == nil {
		t.Fatal("new entity remained after rollback")
	}

	importedID, err := scatteredID(100)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DsImportAtomic(context.Background(), "project", "database", func(yield func(*datastorepb.Entity) error) error {
		return yield(entity("", "imported", importedID))
	}); err != nil {
		t.Fatal(err)
	}
	allocated, err := store.DsAllocateIDBlock(context.Background(), "project", "database", "", "Widget", []string{""})
	if err != nil {
		t.Fatal(err)
	}
	if counter, ok := scatteredCounter(allocated[0]); !ok || counter <= 100 {
		t.Fatalf("allocated ID %d was not reserved past imported ID %d", allocated[0], importedID)
	}
}

func TestNewRecoversInterruptedImportJournal(t *testing.T) {
	directory := t.TempDir()
	store, err := New(directory)
	if err != nil {
		t.Fatal(err)
	}
	key := &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: "project", DatabaseId: "database"}, Path: []*datastorepb.Key_PathElement{{Kind: "Widget", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}
	original := &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"value": {ValueType: &datastorepb.Value_StringValue{StringValue: "original"}}}}
	if _, err := store.DsUpsert(EntityWrite{Project: "project", Database: "database", Path: reviewNamedPath("Widget", "one"), Kind: "Widget", Entity: original}); err != nil {
		t.Fatal(err)
	}
	state := importJournalState{Status: importStateApplying, Project: "project", Database: "database"}
	if err := store.writeImportState(state); err != nil {
		t.Fatal(err)
	}
	err = store.runUpdateRawUnlocked(context.Background(), false, func(tx *Txn) error {
		previous, existed, err := rawValueTxn(tx, dsKey("project", "database", "", reviewNamedPath("Widget", "one")))
		if err != nil {
			return err
		}
		if err := setImportEntry(tx, 0, importJournalEntry{Type: "entity", Path: reviewNamedPath("Widget", "one"), Existed: existed, Previous: previous}); err != nil {
			return err
		}
		replacement := &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"value": {ValueType: &datastorepb.Value_StringValue{StringValue: "partial"}}}}
		_, err = store.putDS(tx, EntityWrite{Project: "project", Database: "database", Path: reviewNamedPath("Widget", "one"), Kind: "Widget", Entity: replacement}, putOptions{}, NewCommitAccumulator())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	restored, _, err := reopened.DsGet("project", "database", "", reviewNamedPath("Widget", "one"))
	if err != nil {
		t.Fatal(err)
	}
	if got := restored.GetProperties()["value"].GetStringValue(); got != "original" {
		t.Fatalf("recovered value = %q, want original", got)
	}
}

func reviewNamedPath(kind, name string) string {
	return keycodec.Path([]*datastorepb.Key_PathElement{{Kind: kind, IdType: &datastorepb.Key_PathElement_Name{Name: name}}})
}
