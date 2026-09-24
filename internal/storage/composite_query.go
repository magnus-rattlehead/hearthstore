package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/dgraph-io/badger/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// checkCompositeRead fences selection against deletion/publication in the same
// snapshot that supplies the physical entries, not a preceding metadata read.
func checkCompositeRead(tx *Txn, selected DsCompositeIndex) error {
	item, err := tx.Get(indexKey(selected.Project, selected.ID))
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
	if current.State != DsIndexReady || current.ActiveGeneration != selected.ActiveGeneration {
		return status.Error(codes.FailedPrecondition, "composite index generation is not ready at the read snapshot")
	}
	return nil
}

// DsCountComposite counts unique entity paths using only a compatible composite index.
func (s *Store) DsCountComposite(ctx context.Context, project, database, namespace, indexID, ancestor string, filter *datastorepb.Filter) (int64, int64, bool, error) {
	return s.dsCountCompositeAt(ctx, 0, project, database, namespace, indexID, ancestor, filter)
}

// DsCountCompositeAsOf counts unique entity paths at a historical timestamp.
func (s *Store) DsCountCompositeAsOf(ctx context.Context, asOf time.Time, project, database, namespace, indexID, ancestor string, filter *datastorepb.Filter) (int64, int64, bool, error) {
	return s.dsCountCompositeAt(ctx, uint64(asOf.UnixNano()), project, database, namespace, indexID, ancestor, filter)
}

func (s *Store) dsCountCompositeAt(ctx context.Context, readTs uint64, project, database, namespace, indexID, ancestor string, filter *datastorepb.Filter) (int64, int64, bool, error) {
	work := QueryWorkFromContext(ctx)
	if err := work.Checkpoint(ctx); err != nil {
		return 0, 0, true, err
	}
	idx, err := s.GetDsCompositeIndex(project, indexID)
	if err != nil {
		return 0, 0, true, err
	}
	if idx.State != DsIndexReady || idx.ActiveGeneration == 0 {
		return 0, 0, true, status.Error(codes.FailedPrecondition, "composite index is not ready")
	}
	bounds, ok := buildCompositeScanBounds(idx, filter)
	if !ok {
		return 0, 0, false, nil
	}
	scan := append(compositeScanBase(idx, database, namespace, idx.ActiveGeneration, ancestor), '/')
	scan = append(scan, bounds.prefix...)
	start := scan
	if bounds.lower != nil {
		start = append(append([]byte{}, scan...), bounds.lower...)
	}
	paths, err := s.NewPathAccumulator(ctx)
	if err != nil {
		return 0, 0, true, err
	}
	defer paths.Close()
	var scanned int64
	view := s.view
	if readTs != 0 {
		view = func(fn func(*Txn) error) error { return s.viewAt(readTs, fn) }
	}
	err = view(func(tx *Txn) error {
		if err := checkCompositeRead(tx, idx); err != nil {
			return err
		}
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(start); it.ValidForPrefix(scan); it.Next() {
			work.Charge(WorkIndexEntries, 1)
			if err := work.Checkpoint(ctx); err != nil {
				return err
			}
			key := it.Item().KeyCopy(nil)
			if bounds.lower != nil || bounds.upper != nil {
				component, _, ok := takeIndexComponent(key[len(scan):])
				if !ok {
					continue
				}
				if bounds.lower != nil {
					cmp := bytes.Compare(component, bounds.lower)
					if cmp < 0 || (cmp == 0 && !bounds.lowerInclusive) {
						continue
					}
				}
				if bounds.upper != nil {
					cmp := bytes.Compare(component, bounds.upper)
					if cmp > 0 || (cmp == 0 && !bounds.upperInclusive) {
						break
					}
				}
			}
			scanned++
			value, err := itemValue(it.Item())
			if err != nil {
				return err
			}
			path, _, err := splitIndexValue(value)
			if err != nil {
				return err
			}
			if ancestor != "" && path != ancestor && !strings.HasPrefix(path, ancestor+"/") {
				continue
			}
			if err := paths.Add(path); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, scanned, true, err
	}
	count, err := paths.Count()
	return count, scanned, true, err
}

// DsQueryComposite scans an index using the requested snapshot, filtering, and row shape.
func (s *Store) DsQueryComposite(ctx context.Context, q CompositeQuery) (QueryPage, error) {
	return s.dsQueryComposite(ctx, q, nil)
}

func (s *Store) dsQueryComposite(ctx context.Context, q CompositeQuery, probe *indexProbe) (QueryPage, error) {
	var readTs uint64
	if q.ReadTime != nil {
		readTs = uint64(q.ReadTime.UnixNano())
	}
	work := QueryWorkFromContext(ctx)
	if err := work.Checkpoint(ctx); err != nil {
		return QueryPage{}, err
	}
	idx, err := s.GetDsCompositeIndex(q.Project, q.IndexID)
	if err != nil {
		return QueryPage{}, err
	}
	if idx.State != DsIndexReady || idx.ActiveGeneration == 0 {
		return QueryPage{}, status.Error(codes.FailedPrecondition, "composite index is not ready")
	}
	if q.Projection && q.Cursor != nil && q.Cursor.G != 0 && (idx.State != DsIndexReady || idx.ActiveGeneration != q.Cursor.G || readTs != 0 && idx.ReadySince > int64(readTs)) {
		return QueryPage{}, status.Error(codes.FailedPrecondition, "aggregation index generation is no longer available at the read snapshot")
	}
	bounds, bounded := buildCompositeBounds(idx, q.Filter, q.Accept == nil)
	if !bounded {
		return QueryPage{}, status.Error(codes.FailedPrecondition, "query cannot be bounded by the selected composite index")
	}
	if q.Filter != nil {
		q.Prefix = bounds.prefix
	}
	scan := append(compositeScanBase(idx, q.Database, q.Namespace, idx.ActiveGeneration, q.Ancestor), '/')
	scan = append(scan, q.Prefix...)
	if probe != nil && q.Cursor != nil {
		if !bytes.HasPrefix(q.Cursor.K, scan) {
			return QueryPage{}, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		q.Cursor = nil // Complete-range deduplication precedes cursor filtering.
	}
	var out []*DsEntityRow
	materializedBytes := 0
	var scanned int64
	var more bool
	var seen map[string]struct{}
	if !q.Projection {
		seen = make(map[string]struct{})
	}
	view := s.view
	if readTs != 0 {
		view = func(fn func(*Txn) error) error { return s.viewAt(readTs, fn) }
	}
	err = view(func(tx *Txn) error {
		opts := badger.DefaultIteratorOptions
		if err := checkCompositeRead(tx, idx); err != nil {
			return err
		}
		if len(idx.Properties) > 0 && idx.Properties[0].Desc {
			_ = opts
		}
		it := tx.NewIterator(opts)
		defer it.Close()
		start := scan
		if bounds.lower != nil {
			start = append(append([]byte{}, scan...), bounds.lower...)
		}
		if q.Cursor != nil && len(q.Cursor.K) > 0 {
			start = q.Cursor.K
		}
		for it.Seek(start); it.ValidForPrefix(scan); it.Next() {
			if err := probe.step(); err != nil {
				return err
			}
			work.Charge(WorkIndexEntries, 1)
			if err := work.Checkpoint(ctx); err != nil {
				return err
			}
			key := it.Item().KeyCopy(nil)
			if bounds.lower != nil || bounds.upper != nil {
				component, _, ok := takeIndexComponent(key[len(scan):])
				if !ok {
					continue
				}
				if bounds.lower != nil {
					cmp := bytes.Compare(component, bounds.lower)
					if cmp < 0 || (cmp == 0 && !bounds.lowerInclusive) {
						continue
					}
				}
				if bounds.upper != nil {
					cmp := bytes.Compare(component, bounds.upper)
					if cmp > 0 || (cmp == 0 && !bounds.upperInclusive) {
						break
					}
				}
			}
			scanned++
			if q.Cursor != nil && q.Cursor.O == 0 && len(q.Cursor.K) > 0 && string(key) == string(q.Cursor.K) {
				continue
			}
			// Check all tuple ranges before loading the source entity. Secondary
			// array entries can be numerous even when the leading range is tight.
			tail := key[len(scan)-len(q.Prefix):]
			matchesBounds := true
			for _, property := range idx.Properties {
				component, rest, ok := takeIndexComponent(tail)
				if !ok {
					return status.Error(codes.Internal, "invalid composite index tuple")
				}
				if !spanContains(bounds.properties[property.Name], component) {
					matchesBounds = false
					break
				}
				tail = rest
			}
			if !matchesBounds {
				continue
			}
			v, e := itemValue(it.Item())
			if e != nil {
				return e
			}
			path, _, e := splitIndexValue(v)
			if e != nil {
				return e
			}
			if _, ok := seen[path]; ok && !q.Projection {
				continue
			}
			var r dsRecord
			var entity *datastorepb.Entity
			if q.Projection {
				_, r, entity, e = decodeIndexValue(v)
				work.Charge(WorkDecodedBytes, uint64(len(r.Data)))
			} else {
				r, entity, e = getDSQueryTxn(ctx, tx, q.Project, q.Database, q.Namespace, path)
			}
			if e != nil {
				return e
			}
			if q.Accept != nil && !q.Accept(entity) {
				continue
			}
			if !q.Projection {
				first, err := firstCompositeArrayEntry(ctx, idx, q.Database, q.Namespace, q.Ancestor, path, entity, q.Prefix, bounds)
				if err != nil {
					return err
				}
				if first != nil && !bytes.Equal(key, first) {
					continue
				}
			}
			row := rowFrom(r, entity, path)
			row.IndexKey = key
			out = append(out, row)
			if q.Projection {
				materializedBytes += len(path) + len(key)
			} else {
				seen[path] = struct{}{}
			}
			if queryMaterializedLimitReached(&materializedBytes, entity) || q.Limit > 0 && len(out) >= q.Limit {
				more = true
				break
			}
		}
		return nil
	})
	return QueryPage{Rows: out, Scanned: scanned, More: more}, err
}
