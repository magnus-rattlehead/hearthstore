package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/dgraph-io/badger/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A strategy size, not an acceptance limit. An individual entry may exceed it;
// only the storage engine's actual inability to write an entry is an error.
const compositeEntryBatchBytes = 256 << 10

var errCompositeSourceChanged = errors.New("composite build source changed")

// Read the generation in every write transaction. Deletion or a writer marking
// an index ERROR must win over an old builder's progress or READY publication.
func checkCompositeBuild(tx *Txn, idx DsCompositeIndex, generation int64) error {
	item, err := tx.Get(indexKey(idx.Project, idx.ID))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return ErrIndexNotFound
	}
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
	if current.State != DsIndexCreating || current.BuildingGeneration != generation {
		return fmt.Errorf("composite index %s build generation %d is no longer active (state %s)", idx.ID, generation, current.State)
	}
	return nil
}

func (s *Store) persistCompositeBuild(ctx context.Context, idx DsCompositeIndex, generation int64) error {
	raw, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	return s.RunInTxCtx(ctx, func(tx *Txn) error {
		if err := checkCompositeBuild(tx, idx, generation); err != nil {
			return err
		}
		return tx.Set(indexKey(idx.Project, idx.ID), raw)
	})
}

// streamCompositeBuildTarget handles an entity whose physical covering entries
// do not fit one transaction. Every batch rechecks the source version: a writer
// maintains the whole building generation atomically, so after a source change
// its entries supersede this stream. No old entries may be written afterwards.
func (s *Store) streamCompositeBuildTarget(ctx context.Context, project string, idx DsCompositeIndex, generation int64, target compositeBuildTarget) error {
	var record dsRecord
	var entity *datastorepb.Entity
	err := s.view(func(tx *Txn) error {
		var err error
		record, entity, err = getDSTxn(tx, project, target.database, target.namespace, target.path)
		if err != nil {
			return err
		}
		indexes, err := compositeIndexesForKind(tx, project, idx.Kind, nil)
		if err != nil {
			return err
		}
		return validateEntityIndexLimits(ctx, entity, target.path, indexes)
	})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	if err != nil {
		return err
	}
	work := QueryWorkFromContext(ctx)
	work.Charge(WorkDecodedBytes, uint64(len(record.Data)))
	type entry struct{ key, value []byte }
	var batch []entry
	batchBytes := 0
	flush := func() error {
		for len(batch) > 0 {
			written := 0
			err := s.runUpdateRaw(ctx, true, func(tx *Txn) error {
				written = 0 // A transaction-conflict retry replays only this batch.
				if err := checkCompositeBuild(tx, idx, generation); err != nil {
					return err
				}
				current, _, err := getDSTxn(tx, project, target.database, target.namespace, target.path)
				if status.Code(err) == codes.NotFound || err == nil && current.Version != record.Version {
					return errCompositeSourceChanged
				}
				if err != nil {
					return err
				}
				work.Charge(WorkDecodedBytes, uint64(len(current.Data)))
				indexes, err := compositeIndexesForKind(tx, project, idx.Kind, nil)
				if err != nil {
					return err
				}
				if err := validateEntityIndexLimits(ctx, entity, target.path, indexes); err != nil {
					return err
				}
				for _, entry := range batch {
					if err := work.Checkpoint(ctx); err != nil {
						return err
					}
					err := tx.Set(entry.key, entry.value)
					if errors.Is(err, badger.ErrTxnTooBig) && written > 0 {
						return nil
					}
					if err != nil {
						return err
					}
					written++
				}
				return nil
			})
			if err != nil {
				return err
			}
			clear(batch[:written])
			batch = batch[written:]
		}
		batchBytes = 0
		return nil
	}
	building := idx
	building.ActiveGeneration = generation
	err = visitCompositeEntryKeys(ctx, idx, generation, target.database, target.namespace, target.path, entity, func(key []byte) error {
		work.Charge(WorkIndexEntries, 1)
		value, err := compositeCoverValue(building, target.database, target.namespace, target.path, key, entity, record)
		if err != nil {
			return err
		}
		batch = append(batch, entry{key: key, value: value})
		batchBytes += len(key) + len(value)
		if batchBytes >= compositeEntryBatchBytes {
			return flush()
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	if errors.Is(err, errCompositeSourceChanged) {
		return nil
	}
	return err
}
