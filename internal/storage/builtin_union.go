package storage

import (
	"bytes"
	"container/heap"
	"context"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/dgraph-io/badger/v4"
	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type equalityHead struct {
	iterator *badger.Iterator
	prefix   []byte
	key      []byte
	tail     []byte // Ordered entity-key component followed by the typed path.
}

type equalityHeads []*equalityHead

func (h equalityHeads) Len() int           { return len(h) }
func (h equalityHeads) Less(i, j int) bool { return bytes.Compare(h[i].tail, h[j].tail) < 0 }
func (h equalityHeads) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *equalityHeads) Push(x any)        { *h = append(*h, x.(*equalityHead)) }
func (h *equalityHeads) Pop() any {
	last := len(*h) - 1
	x := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return x
}

// DsQueryEqualityUnionAsOf merges point-index streams with O(branches) heads.
// The caller supplies pure scalar equality branches and the exact residual matcher.
func (s *Store) DsQueryEqualityUnionAsOf(ctx context.Context, asOf time.Time, project, database, namespace, kind string, branches []*datastorepb.PropertyFilter, cursor, end *CursorPayload, limit int, keysOnly bool, accept func(*datastorepb.Entity) bool) ([]*DsEntityRow, int64, bool, bool, error) {
	work := QueryWorkFromContext(ctx)
	keyBase := builtinIndexBase(project, database, namespace, kind, "__key__", "")
	boundary := func(c *CursorPayload) ([]byte, error) {
		if c == nil {
			return nil, nil
		}
		path, err := keycodec.ParsePath(c.P)
		if err != nil || len(path) == 0 || path[len(path)-1].Kind != kind {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		key := &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: project, DatabaseId: database, NamespaceId: namespace}, Path: path}
		expected, ok := builtinIndexEntry(keyBase, &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: key}}, c.P, key)
		if !ok || c.I != "builtin:__key__" || c.G != 1 || c.O != 0 || !bytes.Equal(c.K, expected) {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		_, tail, ok := takeIndexComponent(expected[len(keyBase):])
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		return tail, nil
	}
	startTail, err := boundary(cursor)
	if err != nil {
		return nil, 0, false, false, err
	}
	endTail, err := boundary(end)
	if err != nil {
		return nil, 0, false, false, err
	}
	var rows []*DsEntityRow
	var scanned int64
	var more bool
	var merged bool
	err = s.viewAt(uint64(asOf.UnixNano()), func(tx *Txn) error {
		materialized := 0
		// Dense full-entity queries need no branch iterators. Consume a matching
		// key prefix; at the first gap, seek every branch strictly beyond it.
		// Sparse queries pay at most one rejected source read per invocation.
		if !keysOnly {
			finished, err := func() (bool, error) {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchValues = false
				it := tx.NewIterator(opts)
				defer it.Close()
				start := keyBase
				if cursor != nil {
					start = cursor.K
				}
				for it.Seek(start); it.ValidForPrefix(keyBase); it.Next() {
					if err := work.Checkpoint(ctx); err != nil {
						return false, err
					}
					key := it.Item().KeyCopy(nil)
					if cursor != nil && bytes.Equal(key, cursor.K) {
						continue
					}
					_, tail, ok := takeIndexComponent(key[len(keyBase):])
					if !ok {
						return false, status.Error(codes.Internal, "invalid key index entry")
					}
					if endTail != nil && bytes.Compare(tail, endTail) > 0 {
						return true, nil
					}
					scanned++
					work.Charge(WorkIndexEntries, 1)
					value, err := itemValue(it.Item())
					if err != nil {
						return false, err
					}
					path, _, err := splitIndexValue(value)
					if err != nil {
						return false, err
					}
					record, entity, err := getDSQueryTxn(ctx, tx, project, database, namespace, path)
					if err != nil {
						return false, err
					}
					if accept != nil && !accept(entity) {
						startTail = tail
						return false, nil
					}
					row := rowFrom(record, entity, path)
					row.IndexKey = key
					rows = append(rows, row)
					materialized += len(path) + len(key)
					if queryMaterializedLimitReached(&materialized, entity) || limit > 0 && len(rows) >= limit {
						it.Next()
						more = it.ValidForPrefix(keyBase)
						return true, nil
					}
				}
				return true, nil
			}()
			if err != nil || finished {
				return err
			}
		}
		merged = true
		var heads equalityHeads
		readHead := func(h *equalityHead) (bool, error) {
			if err := work.Checkpoint(ctx); err != nil {
				return false, err
			}
			if !h.iterator.ValidForPrefix(h.prefix) {
				return false, nil
			}
			h.key = h.iterator.Item().KeyCopy(nil)
			h.tail = h.key[len(h.prefix):]
			scanned++
			work.Charge(WorkIndexEntries, 1)
			if _, _, ok := takeIndexComponent(h.tail); !ok {
				return false, status.Error(codes.Internal, "invalid equality index entry")
			}
			return endTail == nil || bytes.Compare(h.tail, endTail) <= 0, nil
		}
		for _, branch := range branches {
			if err := work.Checkpoint(ctx); err != nil {
				return err
			}
			raw, ok := orderedValue(branch.Value, false)
			if !ok {
				return status.Error(codes.Internal, "invalid equality union branch")
			}
			prefix := appendIndexComponent(builtinIndexBase(project, database, namespace, kind, branch.Property.GetName(), ""), raw)
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := tx.NewIterator(opts)
			defer it.Close()
			h := &equalityHead{iterator: it, prefix: prefix}
			it.Seek(append(append([]byte{}, prefix...), startTail...))
			ok, err := readHead(h)
			if err != nil {
				return err
			}
			if ok && startTail != nil && bytes.Equal(h.tail, startTail) {
				it.Next()
				ok, err = readHead(h)
				if err != nil {
					return err
				}
			}
			if ok {
				heap.Push(&heads, h)
			}
		}
		for len(heads) > 0 {
			if err := work.Checkpoint(ctx); err != nil {
				return err
			}
			head := heads[0]
			tail := head.tail
			value, err := itemValue(head.iterator.Item())
			if err != nil {
				return err
			}
			path, _, err := splitIndexValue(value)
			if err != nil {
				return err
			}
			var record dsRecord
			var entity *datastorepb.Entity
			if keysOnly {
				_, record, entity, err = decodeIndexValue(value)
				work.Charge(WorkDecodedBytes, uint64(len(record.Data)))
			} else {
				record, entity, err = getDSQueryTxn(ctx, tx, project, database, namespace, path)
			}
			if err != nil {
				return err
			}
			// Equal keys are adjacent across streams. Advance all their heads before
			// page limits, without a result-sized seen map or duplicate entity reads.
			for len(heads) > 0 && bytes.Equal(heads[0].tail, tail) {
				h := heap.Pop(&heads).(*equalityHead)
				h.iterator.Next()
				ok, err := readHead(h)
				if err != nil {
					return err
				}
				if ok {
					heap.Push(&heads, h)
				}
			}
			if accept != nil && !accept(entity) {
				continue
			}
			row := rowFrom(record, entity, path)
			var ok bool
			row.IndexKey, ok = builtinIndexEntry(keyBase, &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: entity.Key}}, path, entity.Key)
			if !ok {
				return status.Error(codes.Internal, "invalid union result key")
			}
			rows = append(rows, row)
			materialized += len(path) + len(row.IndexKey)
			if queryMaterializedLimitReached(&materialized, entity) || limit > 0 && len(rows) >= limit {
				more = len(heads) > 0
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, scanned, false, merged, err
	}
	return rows, scanned, more, merged, nil
}
