package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/dgraph-io/badger/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	DsIndexCreating = "CREATING"
	DsIndexReady    = "READY"
	DsIndexDeleting = "DELETING"
	DsIndexError    = "ERROR"
)

var ErrIndexNotFound = errors.New("composite index not found")

type compositeBuild struct {
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	err     error
	mu      sync.Mutex
	changed chan struct{}
}

func newCompositeBuild(ctx context.Context) *compositeBuild {
	ctx, cancel := context.WithCancel(ctx)
	return &compositeBuild{ctx: ctx, cancel: cancel, done: make(chan struct{}), changed: make(chan struct{})}
}

func (b *compositeBuild) signal() {
	b.mu.Lock()
	close(b.changed)
	b.changed = make(chan struct{})
	b.mu.Unlock()
}

func (b *compositeBuild) changes() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.changed
}

type DsIndexProperty struct {
	Name string `json:"name" yaml:"name"`
	Desc bool   `json:"desc" yaml:"-"`
}
type DsCompositeIndex struct {
	ReadySince         int64             `json:"-"`
	Project            string            `json:"project"`
	ID                 string            `json:"id"`
	Kind               string            `json:"kind"`
	Ancestor           bool              `json:"ancestor"`
	Properties         []DsIndexProperty `json:"properties"`
	State              string            `json:"state"`
	Source             string            `json:"source"`
	ActiveGeneration   int64             `json:"active_generation"`
	BuildingGeneration int64             `json:"building_generation"`
	ProcessedEntities  int64             `json:"processed_entities"`
	TotalEntities      int64             `json:"total_entities"`
	Error              string            `json:"error,omitempty"`
}

func DsCompositeIndexID(kind string, ancestor bool, props []DsIndexProperty) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%t", kind, ancestor)
	for _, p := range props {
		fmt.Fprintf(h, "\x00%s\x00%t", p.Name, p.Desc)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}
func indexKey(project, id string) []byte { return []byte("meta/index/" + enc(project) + "/" + id) }
func compositeBase(idx DsCompositeIndex, database, namespace string, generation int64) []byte {
	return []byte(fmt.Sprintf("composite/%s/%s/%020d/%s/%s/", enc(idx.Project), idx.ID, generation, enc(database), enc(namespace)))
}

func compositeScanBase(idx DsCompositeIndex, database, namespace string, generation int64, ancestor string) []byte {
	return append(compositeBase(idx, database, namespace, generation), indexScopeSegment(ancestor)...)
}

func (s *Store) EnsureDsCompositeIndex(ctx context.Context, idx DsCompositeIndex, wait bool) (DsCompositeIndex, bool, error) {
	if idx.ID == "" {
		idx.ID = DsCompositeIndexID(idx.Kind, idx.Ancestor, idx.Properties)
	}
	if idx.Source == "" {
		idx.Source = "generated"
	}
	created := false
	err := s.RunInTxCtx(ctx, func(tx *Txn) error {
		item, e := tx.Get(indexKey(idx.Project, idx.ID))
		if e == nil {
			v, err := itemValue(item)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(v, &idx); err != nil {
				return err
			}
			if idx.State == DsIndexReady {
				idx.ReadySince = int64(item.Version())
			}
			return nil
		}
		if !errors.Is(e, badger.ErrKeyNotFound) {
			return e
		}
		idx.State = DsIndexCreating
		idx.BuildingGeneration = 1
		raw, _ := json.Marshal(idx)
		created = true
		return tx.Set(indexKey(idx.Project, idx.ID), raw)
	})
	if err != nil {
		return idx, created, err
	}
	if idx.State == DsIndexReady {
		return idx, created, nil
	}
	if idx.State == DsIndexError || idx.State == DsIndexDeleting {
		return idx, created, nil
	}
	key := idx.Project + "/" + idx.ID
	pending := newCompositeBuild(s.ctx)
	actual, loaded := s.compositeBuilds.LoadOrStore(key, pending)
	build := actual.(*compositeBuild)
	if loaded {
		pending.cancel()
	}
	if !loaded {
		s.workerMu.Lock()
		if s.closing {
			build.cancel()
			build.err = context.Canceled
			close(build.done)
			s.compositeBuilds.CompareAndDelete(key, build)
		} else {
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer build.cancel()
				buildCtx := build.ctx
				select {
				case s.compositeBuildSlots <- struct{}{}:
					defer func() { <-s.compositeBuildSlots }()
				case <-buildCtx.Done():
					build.err = buildCtx.Err()
					close(build.done)
					s.compositeBuilds.CompareAndDelete(key, build)
					return
				}
				build.err = s.BuildDsCompositeIndex(buildCtx, idx.Project, idx.ID)
				build.signal()
				close(build.done)
				s.compositeBuilds.CompareAndDelete(key, build)
			}()
		}
		s.workerMu.Unlock()
	}
	if !wait {
		return idx, created, nil
	}
	select {
	case <-ctx.Done():
		return idx, created, ctx.Err()
	case <-build.done:
	}
	idx, err = s.GetDsCompositeIndex(idx.Project, idx.ID)
	if err == nil && idx.State != DsIndexReady && idx.State != DsIndexError && build.err != nil {
		err = build.err
	}
	return idx, created, err
}

func (s *Store) GetDsCompositeIndex(project, id string) (DsCompositeIndex, error) {
	var idx DsCompositeIndex
	err := s.view(func(tx *Txn) error {
		item, e := tx.Get(indexKey(project, id))
		if errors.Is(e, badger.ErrKeyNotFound) {
			return ErrIndexNotFound
		}
		if e != nil {
			return e
		}
		v, e := itemValue(item)
		if e != nil {
			return e
		}
		if err := json.Unmarshal(v, &idx); err != nil {
			return err
		}
		if idx.State == DsIndexReady {
			idx.ReadySince = int64(item.Version())
		}
		return nil
	})
	return idx, err
}

// WaitDsCompositeIndex waits for the in-process build, if any, and returns its
// persisted terminal state without polling.
func (s *Store) WaitDsCompositeIndex(ctx context.Context, project, id string) (DsCompositeIndex, error) {
	key := project + "/" + id
	if value, ok := s.compositeBuilds.Load(key); ok {
		build := value.(*compositeBuild)
		select {
		case <-ctx.Done():
			return DsCompositeIndex{}, ctx.Err()
		case <-build.done:
		}
		if build.err != nil {
			if idx, err := s.GetDsCompositeIndex(project, id); err == nil && idx.State == DsIndexError {
				return idx, nil
			}
			return DsCompositeIndex{}, build.err
		}
	}
	return s.GetDsCompositeIndex(project, id)
}

// WatchDsCompositeIndex reports each persisted build state and returns after a
// terminal state is stored. Build signals, rather than a polling interval,
// drive subsequent reads.
func (s *Store) WatchDsCompositeIndex(ctx context.Context, project, id string, visit func(DsCompositeIndex)) error {
	key := project + "/" + id
	var previous DsCompositeIndex
	for {
		idx, err := s.GetDsCompositeIndex(project, id)
		if err != nil {
			return err
		}
		if idx.State != previous.State || idx.ProcessedEntities != previous.ProcessedEntities || idx.TotalEntities != previous.TotalEntities {
			visit(idx)
			previous = idx
		}
		if idx.State == DsIndexReady || idx.State == DsIndexError {
			return nil
		}
		value, ok := s.compositeBuilds.Load(key)
		if !ok {
			return fmt.Errorf("composite index %q has no active build", id)
		}
		build := value.(*compositeBuild)
		changed := build.changes()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		case <-build.done:
			if build.err != nil {
				if current, getErr := s.GetDsCompositeIndex(project, id); getErr == nil && current.State == DsIndexError {
					visit(current)
					return nil
				}
				return build.err
			}
		}
	}
}
func (s *Store) ListDsCompositeIndexes(project string) ([]DsCompositeIndex, error) {
	var out []DsCompositeIndex
	_, err := s.VisitDsCompositeIndexes(context.Background(), project, func(index DsCompositeIndex) bool {
		out = append(out, index)
		return true
	})
	SortDsCompositeIndexes(out)
	return out, err
}

// VisitDsCompositeIndexes streams metadata in physical key order. The result is
// false when visit stops early; no unvisited catalog entries are materialized.
func (s *Store) VisitDsCompositeIndexes(ctx context.Context, project string, visit func(DsCompositeIndex) bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	exhausted := true
	err := s.view(func(tx *Txn) error {
		p := []byte("meta/index/")
		if project != "" {
			p = []byte("meta/index/" + enc(project) + "/")
		}
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(p); it.ValidForPrefix(p); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			v, e := itemValue(it.Item())
			if e != nil {
				return e
			}
			var idx DsCompositeIndex
			if e = json.Unmarshal(v, &idx); e != nil {
				return e
			}
			if idx.State == DsIndexReady {
				idx.ReadySince = int64(it.Item().Version())
			}
			if !visit(idx) {
				exhausted = false
				return nil
			}
		}
		return nil
	})
	return exhausted, err
}
func SortDsCompositeIndexes(indexes []DsCompositeIndex) {
	sort.Slice(indexes, func(i, j int) bool {
		if indexes[i].Project != indexes[j].Project {
			return indexes[i].Project < indexes[j].Project
		}
		if indexes[i].Kind != indexes[j].Kind {
			return indexes[i].Kind < indexes[j].Kind
		}
		return indexes[i].ID < indexes[j].ID
	})
}
func (s *Store) ListDsProjects() ([]string, error) {
	set := map[string]bool{}
	err := s.view(func(tx *Txn) error {
		entries, err := s.kindCatalog(tx, "")
		if err != nil {
			return err
		}
		for _, entry := range entries {
			set[entry.Project] = true
		}
		return nil
	})
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	return out, err
}

func (s *Store) BuildDsCompositeIndex(ctx context.Context, project, id string) error {
	ctx, work := WithQueryWork(ctx, 0)
	started := time.Now()
	defer func() {
		counts := work.Snapshot()
		slog.Debug("Datastore composite build work", "project", project, "index", id, "attempts", counts[WorkAttempts], "entries", counts[WorkIndexEntries], "decoded_bytes", counts[WorkDecodedBytes], "yields", counts[WorkYields])
	}()
	idx, err := s.GetDsCompositeIndex(project, id)
	if err != nil {
		return err
	}
	generation := idx.BuildingGeneration
	if generation == 0 {
		generation = idx.ActiveGeneration + 1
	}
	total := int64(0)
	err = s.view(func(tx *Txn) error {
		entries, err := s.kindCatalog(tx, project)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.Kind == idx.Kind {
				total += entry.Count
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	idx.TotalEntities = total
	if err := s.persistCompositeBuild(ctx, idx, generation); err != nil {
		return err
	}
	slog.Info("Building Datastore composite index", "project", project, "index", id, "kind", idx.Kind, "entities", total)
	lastProgress := time.Now()
	targetBytes := s.db.MaxBatchSize()
	var after []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		targets, next, scanDone, scanErr := s.compositeBuildTargets(ctx, project, idx.Kind, after, targetBytes)
		if scanErr != nil {
			return s.failCompositeBuild(idx, scanErr)
		}
		if len(targets) == 0 && scanDone {
			break
		}
		chunkSize := len(targets)
		for {
			err = s.writeCompositeBuildTargets(ctx, project, idx, generation, targets[:chunkSize])
			if !errors.Is(err, badger.ErrTxnTooBig) || chunkSize == 1 {
				break
			}
			chunkSize = (chunkSize + 1) / 2
		}
		if errors.Is(err, badger.ErrTxnTooBig) && chunkSize == 1 {
			err = s.streamCompositeBuildTarget(ctx, project, idx, generation, targets[0])
		}
		if err != nil {
			return s.failCompositeBuild(idx, err)
		}
		if chunkSize < len(targets) {
			next = targets[chunkSize-1].key
			scanDone = false
		}
		after = append(after[:0], next...)
		idx.ProcessedEntities += int64(chunkSize)
		if idx.ProcessedEntities > 0 && time.Since(lastProgress) >= time.Second {
			if err = s.persistCompositeBuild(ctx, idx, generation); err != nil {
				return s.failCompositeBuild(idx, err)
			}
			s.signalCompositeBuild(project, id)
			slog.Info("Datastore composite index progress", "project", project, "index", id, "processed", idx.ProcessedEntities, "total", idx.TotalEntities)
			lastProgress = time.Now()
		}
		if scanDone {
			break
		}
	}
	idx.State = DsIndexReady
	idx.ActiveGeneration = generation
	idx.BuildingGeneration = 0
	idx.Error = ""
	idx.ProcessedEntities = total
	err = s.persistCompositeBuild(ctx, idx, generation)
	if err == nil {
		slog.Info("Datastore composite index ready", "project", project, "index", id, "entities", idx.ProcessedEntities, "duration", time.Since(started))
	}
	return err
}

func (s *Store) signalCompositeBuild(project, id string) {
	if value, ok := s.compositeBuilds.Load(project + "/" + id); ok {
		value.(*compositeBuild).signal()
	}
}

func (s *Store) writeCompositeBuildTargets(ctx context.Context, project string, idx DsCompositeIndex, generation int64, targets []compositeBuildTarget) error {
	work := QueryWorkFromContext(ctx)
	return s.runUpdateRaw(ctx, true, func(tx *Txn) error {
		if err := checkCompositeBuild(tx, idx, generation); err != nil {
			return err
		}
		for _, target := range targets {
			if err := work.Checkpoint(ctx); err != nil {
				return err
			}
			record, entity, err := getDSTxn(tx, project, target.database, target.namespace, target.path)
			if status.Code(err) == codes.NotFound {
				continue
			}
			if err != nil {
				return err
			}
			work.Charge(WorkDecodedBytes, uint64(len(record.Data)))
			indexes, err := compositeIndexesForKind(tx, project, idx.Kind, nil)
			if err != nil {
				return err
			}
			if err := validateEntityIndexLimits(ctx, entity, target.path, indexes); err != nil {
				return err
			}
			building := idx
			building.ActiveGeneration = generation
			err = visitCompositeEntryKeys(ctx, idx, generation, target.database, target.namespace, target.path, entity, func(key []byte) error {
				work.Charge(WorkIndexEntries, 1)
				value, err := compositeCoverValue(building, target.database, target.namespace, target.path, key, entity, record)
				if err != nil {
					return err
				}
				return tx.Set(key, value)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

type compositeBuildTarget struct {
	database, namespace, path string
	key                       []byte
}

func (s *Store) compositeBuildTargets(ctx context.Context, project, kind string, after []byte, byteLimit int64) ([]compositeBuildTarget, []byte, bool, error) {
	work := QueryWorkFromContext(ctx)
	var targets []compositeBuildTarget
	var next []byte
	var targetBytes int64
	done := true
	err := s.view(func(tx *Txn) error {
		entries, err := s.kindCatalog(tx, project)
		if err != nil {
			return err
		}
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := tx.NewIterator(opts)
		defer it.Close()
		for _, entry := range entries {
			if entry.Kind != kind {
				continue
			}
			prefix := dsKindPrefix(project, entry.Database, entry.Namespace, kind)
			if len(after) > 0 && bytes.Compare(prefixEnd(prefix), after) <= 0 {
				continue
			}
			start := prefix
			if bytes.Compare(after, start) > 0 {
				start = after
			}
			for it.Seek(start); it.ValidForPrefix(prefix); it.Next() {
				if err := work.Checkpoint(ctx); err != nil {
					return err
				}
				work.Charge(WorkIndexEntries, 1)
				key := it.Item().KeyCopy(nil)
				if bytes.Equal(key, after) {
					continue
				}
				path := dec(string(key[len(prefix):]))
				targets = append(targets, compositeBuildTarget{database: entry.Database, namespace: entry.Namespace, path: path, key: key})
				targetBytes += int64(len(key) + len(path))
				next = key
				if targetBytes >= byteLimit {
					done = false
					return nil
				}
			}
		}
		return nil
	})
	return targets, next, done, err
}
func (s *Store) failCompositeBuild(idx DsCompositeIndex, buildErr error) error {
	// Interrupted builds remain CREATING. Their generation is invisible, and
	// Ensure can restart idempotently after reopening or a later caller retry.
	if errors.Is(buildErr, context.Canceled) || errors.Is(buildErr, context.DeadlineExceeded) {
		return buildErr
	}
	idx.State = DsIndexError
	idx.Error = buildErr.Error()
	if err := s.persistCompositeBuild(context.Background(), idx, idx.BuildingGeneration); err != nil {
		return errors.Join(buildErr, fmt.Errorf("persisting index failure: %w", err))
	}
	slog.Error("Datastore composite index build failed", "project", idx.Project, "index", idx.ID, "error", buildErr)
	return buildErr
}
func (s *Store) DeleteDsCompositeIndex(ctx context.Context, project, id string) error {
	idx, err := s.GetDsCompositeIndex(project, id)
	if err != nil {
		return err
	}
	// Fence builders and entity maintenance before deleting entries. A build's
	// metadata read conflicts with this transition, even if its batch is in flight.
	if err := s.RunInTxCtx(ctx, func(tx *Txn) error {
		item, err := tx.Get(indexKey(project, id))
		if err != nil {
			return err
		}
		raw, err := itemValue(item)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &idx); err != nil {
			return err
		}
		idx.State = DsIndexDeleting
		raw, err = json.Marshal(idx)
		if err != nil {
			return err
		}
		return tx.Set(indexKey(project, id), raw)
	}); err != nil {
		return err
	}
	if value, ok := s.compositeBuilds.Load(project + "/" + id); ok {
		build := value.(*compositeBuild)
		build.cancel()
		select {
		case <-build.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, g := range []int64{idx.ActiveGeneration, idx.BuildingGeneration} {
		if g == 0 {
			continue
		}
		p := []byte(fmt.Sprintf("composite/%s/%s/%020d/", enc(project), id, g))
		for {
			deleted := 0
			err = s.RunInTxCtx(ctx, func(tx *Txn) error {
				it := tx.NewIterator(badger.DefaultIteratorOptions)
				defer it.Close()
				for it.Seek(p); it.ValidForPrefix(p); it.Next() {
					deleteErr := tx.Delete(it.Item().KeyCopy(nil))
					if errors.Is(deleteErr, badger.ErrTxnTooBig) && deleted > 0 {
						break
					}
					if deleteErr != nil {
						return deleteErr
					}
					deleted++
				}
				return nil
			})
			if err != nil {
				return err
			}
			if deleted == 0 {
				break
			}
		}
	}
	return s.RunInTxCtx(ctx, func(tx *Txn) error { return tx.Delete(indexKey(project, id)) })
}

func (s *Store) maintainCompositeIndexes(tx *Txn, project, database, namespace, path, kind string, oldEntity, newEntity *datastorepb.Entity, acc *CommitAccumulator, record dsRecord) error {
	indexes, err := compositeIndexesForKind(tx, project, kind, acc)
	if err != nil {
		return err
	}
	for _, idx := range indexes {
		gens := []int64{idx.ActiveGeneration, idx.BuildingGeneration}
		for _, g := range gens {
			if g == 0 {
				continue
			}
			if oldEntity != nil {
				keys, e := dsCompositeEntryKeys(idx, g, database, namespace, path, oldEntity)
				if e != nil {
					// A new build preflights each entity before writing any entries.
					// An oversized old entity therefore has nothing to remove in
					// this unpublished generation; deleting/reducing it must work.
					if idx.ActiveGeneration != 0 || g != idx.BuildingGeneration || status.Code(validateEntityIndexLimits(context.Background(), oldEntity, path, []DsCompositeIndex{idx})) != codes.InvalidArgument {
						return e
					}
				}
				for _, key := range keys {
					if e = tx.Delete(key); e != nil {
						return e
					}
				}
			}
			if newEntity != nil {
				keys, e := dsCompositeEntryKeys(idx, g, database, namespace, path, newEntity)
				if e != nil {
					return e
				}
				for _, key := range keys {
					generation := idx
					generation.ActiveGeneration = g
					value, err := compositeCoverValue(generation, database, namespace, path, key, newEntity, record)
					if err != nil {
						return err
					}
					if e = tx.Set(key, value); e != nil {
						return e
					}
				}
			}
		}
	}
	return nil
}
