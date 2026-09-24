package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"strings"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/dgraph-io/badger/v4"
	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CursorPayload struct {
	Before bool   `json:"-"`           // Request-local reversed boundary; never serialized.
	B      []byte `json:"b,omitempty"` // Logical order tuple, independent of the access index.
	D      string `json:"d,omitempty"`
	O      int    `json:"o,omitempty"`
	H      []byte `json:"h,omitempty"`
	V      int    `json:"v"`
	P      string `json:"p"`
	I      string `json:"i,omitempty"`
	G      int64  `json:"g,omitempty"`
	K      []byte `json:"k,omitempty"`
}

type dsRecord struct {
	Data       []byte `json:"data,omitempty"`
	Kind       string `json:"kind"`
	ParentPath string `json:"parent_path"`
	Version    int64  `json:"version"`
	Created    int64  `json:"created"`
	Updated    int64  `json:"updated"`
	Deleted    bool   `json:"deleted,omitempty"`
}
type DsEntityRow struct {
	ProjectionOffset       int
	Entity                 *datastorepb.Entity
	Version                int64
	CreateTime, UpdateTime *timestamppb.Timestamp
	Path                   string
	IndexKey               []byte
}

// EntityWrite identifies an entity and its optional version precondition.
// Updates retain the stored kind and parent; inserts ignore BaseVersion.
type EntityWrite struct {
	Project, Database, Namespace string
	Path, Kind, ParentPath       string
	Entity                       *datastorepb.Entity
	BaseVersion                  int64
}

// WriteResult describes either a persisted write or a version conflict.
// On conflict, Version is the stored version and no write is performed.
type WriteResult struct {
	Entity                 *datastorepb.Entity
	Version                int64
	CreateTime, UpdateTime *timestamppb.Timestamp
	Conflict               bool
}

type putOptions struct {
	insertOnly, updateOnly, knownMissing bool
}

type UpsertManyRow struct {
	Namespace, Path, Kind, ParentPath string
	Entity                            *datastorepb.Entity
	AllocateID                        bool
	AllocatedID                       bool
}
type UpsertManyResult struct {
	Version    int64
	Key        *datastorepb.Key
	UpdateTime *timestamppb.Timestamp
}
type compositeIndexCacheKey struct {
	project string
	kind    string
}

// CommitAccumulator caches index definitions for one transaction; writes are immediate.
type CommitAccumulator struct {
	compositeIndexes map[compositeIndexCacheKey][]DsCompositeIndex
}

func NewCommitAccumulator() *CommitAccumulator {
	return &CommitAccumulator{
		compositeIndexes: make(map[compositeIndexCacheKey][]DsCompositeIndex),
	}
}

func dsPrefix(project, database, namespace string) []byte {
	return []byte("ds/doc/" + enc(project) + "/" + enc(database) + "/" + enc(namespace) + "/")
}
func dsKey(project, database, namespace, path string) []byte {
	return append(dsPrefix(project, database, namespace), enc(path)...)
}
func dsKindPrefix(project, database, namespace, kind string) []byte {
	return []byte("ds/kind/" + enc(project) + "/" + enc(database) + "/" + enc(namespace) + "/" + enc(kind) + "/")
}
func dsKindKey(project, database, namespace, kind, path string) []byte {
	return append(dsKindPrefix(project, database, namespace, kind), enc(path)...)
}

func indexScopes(path string) []string {
	parts := strings.Split(path, "/")
	scopes := []string{""}
	for end := 2; end <= len(parts); end += 2 {
		scopes = append(scopes, strings.Join(parts[:end], "/"))
	}
	return scopes
}

func indexScopeSegment(ancestor string) string {
	if ancestor == "" {
		return "_"
	}
	return "a" + enc(ancestor)
}
func decodeDS(v []byte) (dsRecord, *datastorepb.Entity, error) {
	r, err := decodeRecord(v)
	if err != nil {
		return r, nil, err
	}
	if r.Deleted {
		return r, nil, nil
	}
	var e datastorepb.Entity
	if err := proto.Unmarshal(r.Data, &e); err != nil {
		return r, nil, err
	}
	return r, &e, nil
}
func getDSTxn(tx *Txn, project, database, namespace, path string) (dsRecord, *datastorepb.Entity, error) {
	item, err := tx.Get(dsKey(project, database, namespace, path))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return dsRecord{}, nil, status.Errorf(codes.NotFound, "entity not found: %s", path)
	}
	if err != nil {
		return dsRecord{}, nil, err
	}
	v, err := itemValue(item)
	if err != nil {
		return dsRecord{}, nil, err
	}
	r, e, err := decodeDS(v)
	if err == nil && r.Deleted {
		err = status.Errorf(codes.NotFound, "entity not found: %s", path)
	}
	return r, e, err
}
func rowFrom(r dsRecord, e *datastorepb.Entity, path string) *DsEntityRow {
	return &DsEntityRow{Entity: e, Version: r.Version, CreateTime: timestamppb.New(time.Unix(0, r.Created)), UpdateTime: timestamppb.New(time.Unix(0, r.Updated)), Path: path}
}

func (s *Store) DsGet(project, database, namespace, path string) (e *datastorepb.Entity, v int64, err error) {
	err = s.view(func(tx *Txn) error {
		r, x, z := getDSTxn(tx, project, database, namespace, path)
		e, v, err = x, r.Version, z
		return z
	})
	return
}

// DsGetTx reads an entity inside the caller's transaction, including pending writes.
func (s *Store) DsGetTx(tx *Txn, project, database, namespace, path string) (*datastorepb.Entity, int64, error) {
	r, entity, err := getDSTxn(tx, project, database, namespace, path)
	return entity, r.Version, err
}

// DsVisitAllEntities visits every live entity for a project and database from
// one consistent read snapshot.
func (s *Store) DsVisitAllEntities(ctx context.Context, project, database string, visit func(namespace string, row *DsEntityRow) error) error {
	prefix := []byte("ds/doc/" + enc(project) + "/" + enc(database) + "/")
	return s.view(func(tx *Txn) error {
		iterator := tx.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()
		for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			remainder := string(iterator.Item().Key()[len(prefix):])
			separator := strings.IndexByte(remainder, '/')
			if separator < 0 {
				return fmt.Errorf("invalid Datastore entity key %q", iterator.Item().Key())
			}
			namespace := dec(remainder[:separator])
			path := dec(remainder[separator+1:])
			raw, err := itemValue(iterator.Item())
			if err != nil {
				return err
			}
			record, entity, err := decodeDS(raw)
			if err != nil {
				return fmt.Errorf("decoding entity %s: %w", path, err)
			}
			if record.Deleted {
				continue
			}
			if err := visit(namespace, rowFrom(record, entity, path)); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListDsKinds returns the distinct kinds stored for a project.
func (s *Store) ListDsKinds(project string) ([]string, error) {
	kinds := make(map[string]struct{})
	err := s.view(func(tx *Txn) error {
		entries, err := s.kindCatalog(tx, project)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			kinds[entry.Kind] = struct{}{}
		}
		return nil
	})
	out := make([]string, 0, len(kinds))
	for kind := range kinds {
		out = append(out, kind)
	}
	slices.Sort(out)
	return out, err
}
func (s *Store) DsVersionTx(tx *Txn, project, database, namespace, path string) (int64, error) {
	r, _, err := getDSTxn(tx, project, database, namespace, path)
	return r.Version, err
}
func (s *Store) DsGetWithTimes(project, database, namespace, path string) (e *datastorepb.Entity, v int64, ct, ut *timestamppb.Timestamp, err error) {
	err = s.view(func(tx *Txn) error {
		r, x, z := getDSTxn(tx, project, database, namespace, path)
		if z == nil {
			row := rowFrom(r, x, path)
			e, v, ct, ut = row.Entity, row.Version, row.CreateTime, row.UpdateTime
		}
		return z
	})
	return
}
func (s *Store) DsGetManyWithTimes(project, database, namespace string, paths []string) ([]*DsEntityRow, []string, error) {
	var found []*DsEntityRow
	var missing []string
	err := s.DsVisitManyWithTimes(context.Background(), project, database, namespace, paths, func(row *DsEntityRow, missingPath string) bool {
		if row == nil {
			missing = append(missing, missingPath)
		} else {
			found = append(found, row)
		}
		return true
	})
	return found, missing, err
}

// DsVisitManyWithTimes visits requested paths in order using one read snapshot.
// Returning false from visit stops before decoding another entity.
func (s *Store) DsVisitManyWithTimes(ctx context.Context, project, database, namespace string, paths []string, visit func(*DsEntityRow, string) bool) error {
	return s.dsVisitManyAt(ctx, 0, project, database, namespace, paths, visit)
}

// DsVisitManyWithTimesAsOf visits requested paths at a historical snapshot.
func (s *Store) DsVisitManyWithTimesAsOf(ctx context.Context, asOf time.Time, project, database, namespace string, paths []string, visit func(*DsEntityRow, string) bool) error {
	return s.dsVisitManyAt(ctx, uint64(asOf.UnixNano()), project, database, namespace, paths, visit)
}

func (s *Store) dsVisitManyAt(ctx context.Context, readTs uint64, project, database, namespace string, paths []string, visit func(*DsEntityRow, string) bool) error {
	view := s.view
	if readTs != 0 {
		view = func(fn func(*Txn) error) error { return s.viewAt(readTs, fn) }
	}
	return view(func(tx *Txn) error {
		for _, path := range paths {
			if err := ctx.Err(); err != nil {
				return err
			}
			record, entity, err := getDSQueryTxn(ctx, tx, project, database, namespace, path)
			if status.Code(err) == codes.NotFound {
				if !visit(nil, path) {
					return nil
				}
				continue
			}
			if err != nil {
				return err
			}
			if !visit(rowFrom(record, entity, path), "") {
				return nil
			}
		}
		return nil
	})
}

func (s *Store) putDS(tx *Txn, write EntityWrite, options putOptions, acc *CommitAccumulator) (WriteResult, error) {
	var old dsRecord
	var oldEntity *datastorepb.Entity
	var err error
	exists := false
	if !options.knownMissing {
		old, oldEntity, err = getDSTxn(tx, write.Project, write.Database, write.Namespace, write.Path)
		exists = err == nil
	}
	if err != nil && status.Code(err) != codes.NotFound {
		return WriteResult{}, err
	}
	if options.insertOnly && exists {
		return WriteResult{}, status.Errorf(codes.AlreadyExists, "entity already exists: %s", write.Path)
	}
	if options.updateOnly && !exists {
		return WriteResult{}, status.Errorf(codes.NotFound, "entity not found: %s", write.Path)
	}
	if write.BaseVersion != 0 && (!exists || old.Version != write.BaseVersion) {
		return WriteResult{Entity: write.Entity, Version: old.Version, Conflict: true}, nil
	}
	if options.updateOnly {
		write.Kind, write.ParentPath = old.Kind, old.ParentPath
	}
	if write.Kind == "__namespace__" || write.Kind == "__kind__" || write.Kind == "__property__" {
		return WriteResult{}, status.Error(codes.InvalidArgument, "metadata entities cannot be stored")
	}
	write.Entity, err = normalizeEntityTimestamps(write.Entity)
	if err != nil {
		return WriteResult{}, err
	}
	indexes, err := compositeIndexesForKind(tx, write.Project, write.Kind, acc)
	if err != nil {
		return WriteResult{}, err
	}
	if err := validateEntityIndexLimits(context.Background(), write.Entity, write.Path, indexes); err != nil {
		return WriteResult{}, err
	}
	if !exists {
		if err := adjustKindCount(tx, write.Project, write.Database, write.Namespace, write.Kind, 1); err != nil {
			return WriteResult{}, err
		}
	}
	now := monotonicNow().AsTime().UnixNano()
	created := now
	version := old.Version + 1
	if exists {
		created = old.Created
		version = old.Version + 1
		if old.Kind != write.Kind {
			_ = tx.Delete(dsKindKey(write.Project, write.Database, write.Namespace, old.Kind, write.Path))
		}
	}
	data, err := proto.Marshal(write.Entity)
	if err != nil {
		return WriteResult{}, err
	}
	r := dsRecord{Data: data, Kind: write.Kind, ParentPath: write.ParentPath, Version: version, Created: created, Updated: now}
	raw := encodeRecord(r)
	if err = tx.Set(dsKey(write.Project, write.Database, write.Namespace, write.Path), raw); err != nil {
		return WriteResult{}, err
	}
	if err = tx.Set(dsKindKey(write.Project, write.Database, write.Namespace, write.Kind, write.Path), []byte(write.Path)); err != nil {
		return WriteResult{}, err
	}
	if err = s.maintainBuiltinIndexes(tx, write.Project, write.Database, write.Namespace, write.Path, write.Kind, oldEntity, write.Entity, r); err != nil {
		return WriteResult{}, err
	}
	if err = maintainPropertyCatalog(tx, write.Project, write.Database, write.Namespace, write.Kind, oldEntity, write.Entity); err != nil {
		return WriteResult{}, err
	}
	if err = s.maintainCompositeIndexes(tx, write.Project, write.Database, write.Namespace, write.Path, write.Kind, oldEntity, write.Entity, acc, r); err != nil {
		return WriteResult{}, err
	}
	if err = touchKind(tx, write.Project, write.Database, write.Namespace, write.Kind); err != nil {
		return WriteResult{}, err
	}
	return WriteResult{
		Entity: write.Entity, Version: version,
		CreateTime: timestamppb.New(time.Unix(0, created)),
		UpdateTime: timestamppb.New(time.Unix(0, now)),
	}, nil
}
func (s *Store) DsInsert(write EntityWrite) (result WriteResult, err error) {
	err = s.RunBatchedTx(context.Background(), func(tx *Txn) error {
		result, err = s.DsInsertTx(tx, write, NewCommitAccumulator())
		return err
	})
	return
}

func (s *Store) DsInsertTx(tx *Txn, write EntityWrite, acc *CommitAccumulator) (WriteResult, error) {
	write.BaseVersion = 0
	return s.putDS(tx, write, putOptions{insertOnly: true}, acc)
}

func (s *Store) DsUpdate(write EntityWrite) (result WriteResult, err error) {
	err = s.RunBatchedTx(context.Background(), func(tx *Txn) error {
		result, err = s.DsUpdateTx(tx, write, NewCommitAccumulator())
		return err
	})
	return
}

func (s *Store) DsUpdateTx(tx *Txn, write EntityWrite, acc *CommitAccumulator) (WriteResult, error) {
	return s.putDS(tx, write, putOptions{updateOnly: true}, acc)
}

func (s *Store) DsUpsert(write EntityWrite) (result WriteResult, err error) {
	err = s.RunBatchedTx(context.Background(), func(tx *Txn) error {
		result, err = s.DsUpsertTx(tx, write, NewCommitAccumulator())
		return err
	})
	return
}

func (s *Store) DsUpsertTx(tx *Txn, write EntityWrite, acc *CommitAccumulator) (WriteResult, error) {
	return s.putDS(tx, write, putOptions{}, acc)
}

func (s *Store) DsUpsertManyTx(tx *Txn, project, database string, rows []UpsertManyRow, _ *timestamppb.Timestamp, acc *CommitAccumulator) ([]UpsertManyResult, error) {
	type allocatorKey struct{ namespace, kind string }
	allocators := make(map[allocatorKey]*dsIDBatch)
	out := make([]UpsertManyResult, len(rows))
	for i, r := range rows {
		entity, path, knownMissing := r.Entity, r.Path, false
		if r.AllocateID {
			key := allocatorKey{r.Namespace, r.Kind}
			allocator := allocators[key]
			if allocator == nil {
				var err error
				allocator, err = newDsIDBatch(tx, project, database, r.Namespace, r.Kind)
				if err != nil {
					return nil, err
				}
				allocators[key] = allocator
			}
			for {
				id, err := allocator.nextID()
				if err != nil {
					return nil, err
				}
				path = keycodec.AppendID(r.ParentPath, r.Kind, id)
				_, _, err = getDSTxn(tx, project, database, r.Namespace, path)
				if status.Code(err) == codes.NotFound {
					entity = proto.Clone(r.Entity).(*datastorepb.Entity)
					last := entity.Key.Path[len(entity.Key.Path)-1]
					last.IdType = &datastorepb.Key_PathElement_Id{Id: id}
					knownMissing = true
					break
				}
				if err != nil {
					return nil, err
				}
			}
		}
		result, err := s.putDS(tx, EntityWrite{
			Project: project, Database: database, Namespace: r.Namespace,
			Path: path, Kind: r.Kind, ParentPath: r.ParentPath, Entity: entity,
		}, putOptions{insertOnly: r.AllocatedID, knownMissing: knownMissing}, acc)
		if err != nil {
			return nil, err
		}
		out[i].Version = result.Version
		out[i].UpdateTime = result.UpdateTime
		if r.AllocateID || r.AllocatedID {
			out[i].Key = entity.Key
		}
	}
	for _, allocator := range allocators {
		if err := allocator.flush(tx); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// DsUpsertMany commits independent upserts in the largest transactions Badger accepts.
func (s *Store) DsUpsertMany(ctx context.Context, project, database string, rows []UpsertManyRow, commitTime *timestamppb.Timestamp) ([]UpsertManyResult, error) {
	var err error
	rows, err = s.allocateBulkIDs(ctx, project, database, rows)
	if err != nil {
		return nil, err
	}
	results := make([]UpsertManyResult, len(rows))
	chunkSize := len(rows)
	for start := 0; start < len(rows); {
		if remaining := len(rows) - start; chunkSize > remaining {
			chunkSize = remaining
		}
		end := start + chunkSize
		var chunkResults []UpsertManyResult
		err := s.runUpdateRaw(ctx, true, func(tx *Txn) error {
			acc := NewCommitAccumulator()
			var err error
			chunkResults, err = s.DsUpsertManyTx(tx, project, database, rows[start:end], commitTime, acc)
			return err
		})
		if errors.Is(err, badger.ErrTxnTooBig) {
			s.txnTooBig.Add(1)
			if chunkSize == 1 {
				return nil, status.Error(codes.ResourceExhausted, "entity exceeds Badger transaction limits")
			}
			chunkSize = (chunkSize + 1) / 2
			continue
		}
		if errors.Is(err, badger.ErrConflict) {
			return nil, status.Error(codes.Aborted, "storage transaction conflicted; retry the operation")
		}
		if err != nil {
			return nil, err
		}
		copy(results[start:end], chunkResults)
		start = end
	}
	return results, nil
}

func (s *Store) allocateBulkIDs(ctx context.Context, project, database string, rows []UpsertManyRow) ([]UpsertManyRow, error) {
	type allocatorKey struct{ namespace, kind string }
	groups := make(map[allocatorKey][]int)
	for i, row := range rows {
		if row.AllocateID {
			key := allocatorKey{row.Namespace, row.Kind}
			groups[key] = append(groups[key], i)
		}
	}
	if len(groups) == 0 {
		return rows, nil
	}

	prepared := append([]UpsertManyRow(nil), rows...)
	for key, indexes := range groups {
		parents := make([]string, len(indexes))
		for i, index := range indexes {
			parents[i] = rows[index].ParentPath
		}
		ids, err := s.DsAllocateIDBlock(ctx, project, database, key.namespace, key.kind, parents)
		if err != nil {
			return nil, err
		}
		for i, index := range indexes {
			row := rows[index]
			entity := proto.Clone(row.Entity).(*datastorepb.Entity)
			entity.Key.Path[len(entity.Key.Path)-1].IdType = &datastorepb.Key_PathElement_Id{Id: ids[i]}
			path := keycodec.AppendID(row.ParentPath, row.Kind, ids[i])
			row.Entity = entity
			row.Path = path
			row.AllocateID = false
			row.AllocatedID = true
			prepared[index] = row
		}
	}
	return prepared, nil
}
func (s *Store) DsDelete(project, database, namespace, path string) error {
	return s.RunBatchedTx(context.Background(), func(tx *Txn) error { return s.DsDeleteTx(tx, project, database, namespace, path, nil) })
}
func (s *Store) DsDeleteTx(tx *Txn, project, database, namespace, path string, acc *CommitAccumulator) error {
	old, e, err := getDSTxn(tx, project, database, namespace, path)
	if status.Code(err) == codes.NotFound {
		return nil
	}
	if err != nil {
		return err
	}
	now := monotonicNow().AsTime().UnixNano()
	data, _ := proto.Marshal(e)
	r := dsRecord{Data: data, Kind: old.Kind, ParentPath: old.ParentPath, Version: old.Version + 1, Created: old.Created, Updated: now, Deleted: true}
	raw := encodeRecord(r)
	if err = tx.Set(dsKey(project, database, namespace, path), raw); err != nil {
		return err
	}
	if err = s.maintainCompositeIndexes(tx, project, database, namespace, path, old.Kind, e, nil, acc, r); err != nil {
		return err
	}
	if err = s.maintainBuiltinIndexes(tx, project, database, namespace, path, old.Kind, e, nil, r); err != nil {
		return err
	}
	if err = touchKind(tx, project, database, namespace, old.Kind); err != nil {
		return err
	}
	if err = adjustKindCount(tx, project, database, namespace, old.Kind, -1); err != nil {
		return err
	}
	if err = maintainPropertyCatalog(tx, project, database, namespace, old.Kind, e, nil); err != nil {
		return err
	}
	return tx.Delete(dsKindKey(project, database, namespace, old.Kind, path))
}

const (
	scatteredIDBase       = int64(1 << 52)
	maxScatteredIDCounter = int64((1 << 51) - 1)
	scatterShift          = 13
)

func dsIDSequenceKey(project, database, namespace, kind string) []byte {
	return []byte("ds/sequence/" + enc(project) + "/" + enc(database) + "/" + enc(namespace) + "/" + enc(kind))
}

func readIDCounter(tx *Txn, key []byte) (int64, error) {
	next := int64(1)
	item, err := tx.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return next, nil
	}
	if err != nil {
		return 0, err
	}
	v, err := itemValue(item)
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(v, &next); err != nil {
		return 0, err
	}
	return next, nil
}

func scatteredID(counter int64) (int64, error) {
	if counter < 1 || counter > maxScatteredIDCounter {
		return 0, fmt.Errorf("Datastore automatic ID space exhausted")
	}
	return scatteredIDBase + int64(bits.Reverse64(uint64(counter)<<scatterShift)), nil
}

func scatteredCounter(id int64) (int64, bool) {
	offset := id - scatteredIDBase
	if offset < 0 || offset > maxScatteredIDCounter {
		return 0, false
	}
	return int64(bits.Reverse64(uint64(offset)) >> scatterShift), true
}

type dsIDBatch struct {
	key  []byte
	next int64
}

func newDsIDBatch(tx *Txn, project, database, namespace, kind string) (*dsIDBatch, error) {
	key := dsIDSequenceKey(project, database, namespace, kind)
	next, err := readIDCounter(tx, key)
	if err != nil {
		return nil, err
	}
	return &dsIDBatch{key: key, next: next}, nil
}

func (b *dsIDBatch) nextID() (int64, error) {
	id, err := scatteredID(b.next)
	if err != nil {
		return 0, err
	}
	b.next++
	return id, nil
}

func (b *dsIDBatch) flush(tx *Txn) error {
	v, err := json.Marshal(b.next)
	if err != nil {
		return err
	}
	return tx.Set(b.key, v)
}

// DsAllocateIDBlock reserves IDs in a short transaction before bulk entity writes.
func (s *Store) DsAllocateIDBlock(ctx context.Context, project, database, namespace, kind string, parentPaths []string) ([]int64, error) {
	ids := make([]int64, len(parentPaths))
	err := s.RunBatchedTx(ctx, func(tx *Txn) error {
		batch, err := newDsIDBatch(tx, project, database, namespace, kind)
		if err != nil {
			return err
		}
		for i, parent := range parentPaths {
			for {
				id, err := batch.nextID()
				if err != nil {
					return err
				}
				path := keycodec.AppendID(parent, kind, id)
				_, _, err = getDSTxn(tx, project, database, namespace, path)
				if status.Code(err) == codes.NotFound {
					ids[i] = id
					break
				}
				if err != nil {
					return err
				}
			}
		}
		return batch.flush(tx)
	})
	return ids, err
}

func (s *Store) DsAllocateIDTx(tx *Txn, project, database, namespace, kind string) (int64, error) {
	batch, err := newDsIDBatch(tx, project, database, namespace, kind)
	if err != nil {
		return 0, err
	}
	id, err := batch.nextID()
	if err != nil {
		return 0, err
	}
	return id, batch.flush(tx)
}

func (s *Store) DsReserveIDTx(tx *Txn, project, database, namespace, kind string, id int64) error {
	counter, ok := scatteredCounter(id)
	if !ok {
		return nil
	}
	key := dsIDSequenceKey(project, database, namespace, kind)
	next, err := readIDCounter(tx, key)
	if err != nil || next > counter {
		return err
	}
	v, _ := json.Marshal(counter + 1)
	return tx.Set(key, v)
}
func (s *Store) DsGetAsOf(project, database, namespace, path string, asOf time.Time) (*datastorepb.Entity, error) {
	var entity *datastorepb.Entity
	err := s.viewAt(uint64(asOf.UnixNano()), func(tx *Txn) error {
		_, found, err := getDSTxn(tx, project, database, namespace, path)
		entity = found
		return err
	})
	return entity, err
}
