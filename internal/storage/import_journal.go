package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/dgraph-io/badger/v4"
	"google.golang.org/protobuf/proto"

	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
)

const (
	importJournalStateKey = "sys/import/v1/state"
	importJournalEntryKey = "sys/import/v1/data/entry/"
	importSeenPrefix      = "sys/import/v1/data/seen/"
	importStateApplying   = "applying"
	importStateCommitted  = "committed"
)

type importJournalState struct {
	Status   string `json:"status"`
	Project  string `json:"project"`
	Database string `json:"database"`
}

type importJournalEntry struct {
	Type      string `json:"type"`
	Key       []byte `json:"key,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Path      string `json:"path,omitempty"`
	Existed   bool   `json:"existed"`
	Previous  []byte `json:"previous,omitempty"`
}

// ImportError reports the outcome separately from the operation's underlying failure.
// Outcome is "not started", "rolled back", "committed", or "unresolved".
type ImportError struct {
	Outcome string
	Err     error
}

func (e *ImportError) Error() string { return fmt.Sprintf("import %s: %v", e.Outcome, e.Err) }
func (e *ImportError) Unwrap() error { return e.Err }

func (s *Store) requireRecovery(err error) error {
	s.recoveryErr = err // Caller holds maintenanceMu exclusively.
	slog.Error("Storage recovery required; data access blocked until reopen", "error", err)
	return &ImportError{Outcome: "unresolved", Err: err}
}

func (s *Store) abortImport(state importJournalState, cause error) error {
	if err := s.rollbackImport(state); err != nil {
		return s.requireRecovery(errors.Join(cause, fmt.Errorf("rolling back import: %w", err)))
	}
	return &ImportError{Outcome: "rolled back", Err: cause}
}

// DsImportAtomic applies a preflighted entity stream atomically. Ordinary
// reads and writes wait until the import commits or has fully rolled back.
func (s *Store) DsImportAtomic(ctx context.Context, project, database string, source func(func(*datastorepb.Entity) error) error) error {
	if source == nil {
		return fmt.Errorf("import entity source is nil")
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	if err := s.checkAvailableUnlocked(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.recoverImport(); err != nil {
		cause := fmt.Errorf("recovering previous import journal: %w", err)
		var previous *ImportError
		if errors.As(err, &previous) && previous.Outcome == "committed" {
			return &ImportError{Outcome: "not started", Err: cause}
		}
		return s.requireRecovery(cause)
	}

	state := importJournalState{Status: importStateApplying, Project: project, Database: database}
	if err := s.writeImportState(state); err != nil {
		return s.abortImport(state, err)
	}
	if err := s.Sync(); err != nil {
		return s.abortImport(state, fmt.Errorf("syncing import journal: %w", err))
	}

	var entryNumber int64
	const batchBytesLimit = 4 << 20
	const batchCountLimit = 128
	batch := make([]*datastorepb.Entity, 0, batchCountLimit)
	batchBytes := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := s.applyImportBatch(ctx, project, database, batch, &entryNumber); err != nil {
			return err
		}
		clear(batch)
		batch = batch[:0]
		batchBytes = 0
		return nil
	}
	applyErr := source(func(entity *datastorepb.Entity) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, _, _, _, _, err := importedEntityComponents(entity, project, database); err != nil {
			return err
		}
		size := proto.Size(entity)
		if batchBytes+size > batchBytesLimit {
			if err := flush(); err != nil {
				return err
			}
		}
		// The producer may reuse its object as soon as yield returns.
		batch = append(batch, proto.Clone(entity).(*datastorepb.Entity))
		batchBytes += size
		if len(batch) >= batchCountLimit || batchBytes >= batchBytesLimit {
			return flush()
		}
		return nil
	})
	if applyErr == nil {
		applyErr = flush()
	}
	if applyErr == nil {
		applyErr = ctx.Err()
	}
	if applyErr != nil {
		return s.abortImport(state, applyErr)
	}
	if err := s.Sync(); err != nil {
		return s.abortImport(state, fmt.Errorf("syncing imported data: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return s.abortImport(state, err)
	}
	state.Status = importStateCommitted
	if err := s.writeImportState(state); err != nil {
		return s.requireRecovery(fmt.Errorf("publishing import commit: %w", err))
	}
	if err := s.Sync(); err != nil {
		// A sync error cannot prove that the committed marker is absent on disk.
		// Preserve the journal and let reopening determine the durable outcome.
		return s.requireRecovery(fmt.Errorf("syncing committed import journal: %w", err))
	}
	if err := s.clearImportJournal(); err != nil {
		return &ImportError{Outcome: "committed", Err: fmt.Errorf("clearing committed import journal: %w", err)}
	}
	return nil
}

// applyImportBatch splits oversized transactions caused by index fanout or undo
// records without retaining an unbounded portion of the input stream.
func (s *Store) applyImportBatch(ctx context.Context, project, database string, entities []*datastorepb.Entity, entryNumber *int64) error {
	next := *entryNumber
	err := s.runUpdateRawUnlocked(ctx, false, func(tx *Txn) error {
		acc := NewCommitAccumulator()
		for _, entity := range entities {
			if err := ctx.Err(); err != nil {
				return err
			}
			added, err := s.applyImportedEntity(tx, project, database, entity, next, acc)
			if err != nil {
				return err
			}
			next += added
		}
		return nil
	})
	if errors.Is(err, badger.ErrTxnTooBig) && len(entities) > 1 {
		mid := len(entities) / 2
		if err := s.applyImportBatch(ctx, project, database, entities[:mid], entryNumber); err != nil {
			return err
		}
		return s.applyImportBatch(ctx, project, database, entities[mid:], entryNumber)
	}
	if err == nil {
		*entryNumber = next
	}
	return err
}

func (s *Store) applyImportedEntity(tx *Txn, project, database string, entity *datastorepb.Entity, entryNumber int64, acc *CommitAccumulator) (int64, error) {
	namespace, path, kind, parent, numericID, err := importedEntityComponents(entity, project, database)
	if err != nil {
		return 0, err
	}
	sequenceKey := dsIDSequenceKey(project, database, namespace, kind)
	_, reservable := scatteredCounter(numericID)
	// Persist the catalog's prior existence as well as its count. A failed
	// import must not leave an otherwise permanent kind/namespace behind.
	var catalogEntries int64
	catalogKey := kindCatalogKey(project, database, namespace, kind)
	catalogMarker := []byte(importSeenPrefix + "catalog/" + enc(string(catalogKey)))
	if _, seen, err := rawValueTxn(tx, catalogMarker); err != nil {
		return 0, err
	} else if !seen {
		previous, existed, err := rawValueTxn(tx, catalogKey)
		if err != nil {
			return 0, err
		}
		if err := setImportEntry(tx, entryNumber, importJournalEntry{Type: "raw", Key: catalogKey, Existed: existed, Previous: previous}); err != nil {
			return 0, err
		}
		if err := tx.Set(catalogMarker, nil); err != nil {
			return 0, err
		}
		entryNumber++
		catalogEntries = 1
	}

	entityMarker := []byte(importSeenPrefix + "entity/" + enc(namespace) + "/" + enc(path))
	if _, duplicate, err := rawValueTxn(tx, entityMarker); err != nil {
		return 0, err
	} else if duplicate {
		return 0, fmt.Errorf("import contains duplicate entity key %s", path)
	}
	if err := tx.Set(entityMarker, nil); err != nil {
		return 0, err
	}
	sequenceMarker := []byte(importSeenPrefix + "sequence/" + enc(string(sequenceKey)))
	_, sequenceAlreadyJournaled, err := rawValueTxn(tx, sequenceMarker)
	if err != nil {
		return 0, err
	}
	journalSequence := reservable && !sequenceAlreadyJournaled
	entriesAdded := int64(1)
	if journalSequence {
		entriesAdded++
		if err := tx.Set(sequenceMarker, nil); err != nil {
			return 0, err
		}
	}
	previous, existed, err := rawValueTxn(tx, dsKey(project, database, namespace, path))
	if err != nil {
		return 0, err
	}
	entityEntry := importJournalEntry{Type: "entity", Namespace: namespace, Path: path, Existed: existed, Previous: previous}
	if err := setImportEntry(tx, entryNumber, entityEntry); err != nil {
		return 0, err
	}
	nextEntry := entryNumber + 1
	if journalSequence {
		previousSequence, sequenceExisted, err := rawValueTxn(tx, sequenceKey)
		if err != nil {
			return 0, err
		}
		sequenceEntry := importJournalEntry{Type: "raw", Key: sequenceKey, Existed: sequenceExisted, Previous: previousSequence}
		if err := setImportEntry(tx, nextEntry, sequenceEntry); err != nil {
			return 0, err
		}
	}
	if reservable {
		if err := s.DsReserveIDTx(tx, project, database, namespace, kind, numericID); err != nil {
			return 0, err
		}
	}
	if _, err := s.putDS(tx, EntityWrite{Project: project, Database: database, Namespace: namespace, Path: path, Kind: kind, ParentPath: parent, Entity: entity}, putOptions{}, acc); err != nil {
		return 0, err
	}
	return entriesAdded + catalogEntries, nil
}

func importedEntityComponents(entity *datastorepb.Entity, project, database string) (namespace, path, kind, parent string, numericID int64, err error) {
	if entity == nil || entity.Key == nil {
		return "", "", "", "", 0, fmt.Errorf("import entity has no key")
	}
	partition := entity.Key.GetPartitionId()
	if partition.GetProjectId() != project || partition.GetDatabaseId() != database {
		return "", "", "", "", 0, fmt.Errorf("import entity key is outside target project and database")
	}
	parts := entity.Key.GetPath()
	if len(parts) == 0 {
		return "", "", "", "", 0, fmt.Errorf("import entity has an empty key path")
	}
	for _, element := range parts {
		if element.GetKind() == "" {
			return "", "", "", "", 0, fmt.Errorf("import entity key has an empty kind")
		}
		switch id := element.GetIdType().(type) {
		case *datastorepb.Key_PathElement_Id:
			if id.Id == 0 {
				return "", "", "", "", 0, fmt.Errorf("import entity key has an incomplete numeric ID")
			}
		case *datastorepb.Key_PathElement_Name:
			if id.Name == "" {
				return "", "", "", "", 0, fmt.Errorf("import entity key has an incomplete name")
			}
		default:
			return "", "", "", "", 0, fmt.Errorf("import entity key is incomplete")
		}
	}
	last := parts[len(parts)-1]
	if id, ok := last.GetIdType().(*datastorepb.Key_PathElement_Id); ok {
		numericID = id.Id
	}
	path = keycodec.Path(parts)
	parent = keycodec.Path(parts[:len(parts)-1])
	return partition.GetNamespaceId(), path, last.GetKind(), parent, numericID, nil
}

func (s *Store) recoverImport() error {
	state, found, err := s.readImportState()
	if err != nil || !found {
		return err
	}
	switch state.Status {
	case importStateApplying:
		return s.rollbackImport(state)
	case importStateCommitted:
		if err := s.clearImportJournal(); err != nil {
			return &ImportError{Outcome: "committed", Err: err}
		}
		return nil
	default:
		return fmt.Errorf("unknown import journal state %q", state.Status)
	}
}

func (s *Store) rollbackImport(state importJournalState) error {
	for {
		key, entry, found, err := s.lastImportEntry()
		if err != nil {
			return err
		}
		if !found {
			break
		}
		if err := s.runUpdateRawUnlocked(context.Background(), false, func(tx *Txn) error {
			switch entry.Type {
			case "entity":
				if err := s.restoreImportedEntity(tx, state.Project, state.Database, entry); err != nil {
					return err
				}
			case "raw":
				if entry.Existed {
					if err := tx.Set(entry.Key, entry.Previous); err != nil {
						return err
					}
				} else if err := tx.Delete(entry.Key); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unknown import journal entry type %q", entry.Type)
			}
			return tx.Delete(key)
		}); err != nil {
			return err
		}
	}
	return s.clearImportJournal()
}

func (s *Store) restoreImportedEntity(tx *Txn, project, database string, entry importJournalEntry) error {
	currentRaw, currentExists, err := rawValueTxn(tx, dsKey(project, database, entry.Namespace, entry.Path))
	if err != nil {
		return err
	}
	var currentRecord, previousRecord dsRecord
	var currentEntity, previousEntity *datastorepb.Entity
	if currentExists {
		currentRecord, currentEntity, err = decodeDS(currentRaw)
		if err != nil {
			return err
		}
	}
	if entry.Existed {
		previousRecord, previousEntity, err = decodeDS(entry.Previous)
		if err != nil {
			return err
		}
	}
	acc := NewCommitAccumulator()
	if currentEntity != nil {
		if err := maintainPropertyCatalog(tx, project, database, entry.Namespace, currentRecord.Kind, currentEntity, nil); err != nil {
			return err
		}
		if err := adjustKindCount(tx, project, database, entry.Namespace, currentRecord.Kind, -1); err != nil {
			return err
		}
		if err := s.maintainBuiltinIndexes(tx, project, database, entry.Namespace, entry.Path, currentRecord.Kind, currentEntity, nil, currentRecord); err != nil {
			return err
		}
		if err := s.maintainCompositeIndexes(tx, project, database, entry.Namespace, entry.Path, currentRecord.Kind, currentEntity, nil, acc, currentRecord); err != nil {
			return err
		}
		if err := tx.Delete(dsKindKey(project, database, entry.Namespace, currentRecord.Kind, entry.Path)); err != nil {
			return err
		}
	}
	if previousEntity != nil {
		if err := maintainPropertyCatalog(tx, project, database, entry.Namespace, previousRecord.Kind, nil, previousEntity); err != nil {
			return err
		}
		if err := adjustKindCount(tx, project, database, entry.Namespace, previousRecord.Kind, 1); err != nil {
			return err
		}
		if err := s.maintainBuiltinIndexes(tx, project, database, entry.Namespace, entry.Path, previousRecord.Kind, nil, previousEntity, previousRecord); err != nil {
			return err
		}
		if err := s.maintainCompositeIndexes(tx, project, database, entry.Namespace, entry.Path, previousRecord.Kind, nil, previousEntity, acc, previousRecord); err != nil {
			return err
		}
		if err := tx.Set(dsKindKey(project, database, entry.Namespace, previousRecord.Kind, entry.Path), []byte(entry.Path)); err != nil {
			return err
		}
	}
	if entry.Existed {
		return tx.Set(dsKey(project, database, entry.Namespace, entry.Path), entry.Previous)
	}
	return tx.Delete(dsKey(project, database, entry.Namespace, entry.Path))
}

func setImportEntry(tx *Txn, number int64, entry importJournalEntry) error {
	encoded, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("%s%020d", importJournalEntryKey, number))
	return tx.Set(key, encoded)
}

func (s *Store) writeImportState(state importJournalState) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return s.runUpdateRawUnlocked(context.Background(), false, func(tx *Txn) error {
		return tx.Set([]byte(importJournalStateKey), encoded)
	})
}

func (s *Store) readImportState() (importJournalState, bool, error) {
	var state importJournalState
	found := false
	err := s.viewAtUnlocked(s.latestTs.Load(), func(tx *Txn) error {
		value, exists, err := rawValueTxn(tx, []byte(importJournalStateKey))
		if err != nil || !exists {
			return err
		}
		found = true
		return json.Unmarshal(value, &state)
	})
	return state, found, err
}

func (s *Store) lastImportEntry() ([]byte, importJournalEntry, bool, error) {
	var key []byte
	var entry importJournalEntry
	err := s.viewAtUnlocked(s.latestTs.Load(), func(tx *Txn) error {
		options := badger.DefaultIteratorOptions
		options.Reverse = true
		iterator := tx.NewIterator(options)
		defer iterator.Close()
		prefix := []byte(importJournalEntryKey)
		seek := append(append([]byte(nil), prefix...), 0xff)
		for iterator.Seek(seek); iterator.Valid(); iterator.Next() {
			itemKey := iterator.Item().Key()
			if !strings.HasPrefix(string(itemKey), importJournalEntryKey) {
				break
			}
			key = append(key, itemKey...)
			value, err := itemValue(iterator.Item())
			if err != nil {
				return err
			}
			return json.Unmarshal(value, &entry)
		}
		return nil
	})
	return key, entry, key != nil, err
}

func (s *Store) clearImportJournal() error {
	for {
		var keys [][]byte
		err := s.viewAtUnlocked(s.latestTs.Load(), func(tx *Txn) error {
			options := badger.DefaultIteratorOptions
			options.PrefetchValues = false
			iterator := tx.NewIterator(options)
			defer iterator.Close()
			prefix := []byte("sys/import/v1/data/")
			for iterator.Seek(prefix); iterator.ValidForPrefix(prefix) && len(keys) < 500; iterator.Next() {
				keys = append(keys, append([]byte(nil), iterator.Item().Key()...))
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			break
		}
		if err := s.runUpdateRawUnlocked(context.Background(), false, func(tx *Txn) error {
			for _, key := range keys {
				if err := tx.Delete(key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	if err := s.deleteImportState(); err != nil {
		return err
	}
	return s.Sync()
}

func (s *Store) deleteImportState() error {
	return s.runUpdateRawUnlocked(context.Background(), false, func(tx *Txn) error {
		return tx.Delete([]byte(importJournalStateKey))
	})
}

func rawValueTxn(tx *Txn, key []byte) ([]byte, bool, error) {
	item, err := tx.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	value, err := itemValue(item)
	return value, err == nil, err
}
