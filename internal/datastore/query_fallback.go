package datastore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"iter"
	"strings"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
	"github.com/magnus-rattlehead/hearthstore/internal/propertypath"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var errQueryPageFull = errors.New("query page full")

func isMetadataKind(kind string) bool {
	return kind == "__kind__" || kind == "__namespace__" || kind == "__property__"
}

// fallbackQueryRows sorts compact order tuples and paths on disk, never the
// entire result set in memory. It also serves snapshots predating index creation.
func (g *GRPCServer) fallbackQueryRows(ctx context.Context, snapshot time.Time, project, database, namespace, kind, ancestor string, query *datastorepb.Query, condition *compiledCondition, cursor *storage.CursorPayload, limit int) ([]*storage.DsEntityRow, int64, bool, error) {
	stream, scanned, err := g.prepareFallbackRows(ctx, snapshot, project, database, namespace, kind, ancestor, query, condition, cursor, nil)
	if err != nil {
		return nil, scanned, false, err
	}
	rows, more, err := stream.page(ctx, limit)
	return rows, scanned, more, errors.Join(err, stream.close())
}

// fallbackRows retains one bounded candidate batch until response trimming has
// acknowledged its consumed prefix. It never caches the complete result set.
type fallbackRows struct {
	paths   *storage.PathAccumulator
	next    func() (*storage.DsEntityRow, error, bool)
	stop    func()
	pending []*storage.DsEntityRow
}

func (s *fallbackRows) close() error {
	if s == nil || s.paths == nil {
		return nil
	}
	s.stop() // Unwind merge readers before removing their files.
	err := s.paths.Close()
	s.paths, s.pending = nil, nil
	s.next, s.stop = nil, nil
	return err
}

func (s *fallbackRows) acknowledge(cursor []byte) {
	cut := 0
	for cut < len(s.pending) && bytes.Compare(s.pending[cut].IndexKey, cursor) <= 0 {
		cut++
	}
	copy(s.pending, s.pending[cut:])
	clear(s.pending[len(s.pending)-cut:])
	s.pending = s.pending[:len(s.pending)-cut]
}

func (s *fallbackRows) page(ctx context.Context, limit int) ([]*storage.DsEntityRow, bool, error) {
	var rows []*storage.DsEntityRow
	materialized := 0
	for {
		if err := storage.QueryWorkFromContext(ctx).Checkpoint(ctx); err != nil {
			return nil, false, err
		}
		var row *storage.DsEntityRow
		if len(rows) < len(s.pending) {
			row = s.pending[len(rows)]
		} else {
			var err error
			var ok bool
			row, err, ok = s.next()
			if err != nil || !ok {
				return rows, false, err
			}
			s.pending = append(s.pending, row)
		}
		// Peek before declaring another page, retaining the lookahead even if
		// the caller trims its response further. Include cursor records/paths
		// in the byte budget; pending holds at most one legal row beyond it.
		rowBytes := proto.Size(row.Entity) + len(row.IndexKey) + len(row.Path)
		if limit > 0 && len(rows) >= limit || len(rows) > 0 && materialized+rowBytes > maxQueryResponseBytes {
			return rows, true, nil
		}
		materialized += rowBytes
		rows = append(rows, row)
	}
}

func (g *GRPCServer) prepareFallbackRows(ctx context.Context, snapshot time.Time, project, database, namespace, kind, ancestor string, query *datastorepb.Query, condition *compiledCondition, cursor *storage.CursorPayload, candidates []string) (stream *fallbackRows, scanned int64, err error) {
	paths, err := g.store.NewPathAccumulator(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		if stream == nil {
			err = errors.Join(err, paths.Close())
		}
	}()
	fields := projectionFields(query)
	ordering := newConditionOrdering(query, fields, condition)
	ordering.aggregation = aggregationEntries(ctx)
	names, hasDotted := queryPropertyNames(query)
	cursorTuple := ""
	if cursor != nil {
		cursorTuple = fallbackRecordTuple(string(cursor.K))
	}
	visit := func(fn func(*storage.DsEntityRow) error) (int64, error) {
		if candidates != nil {
			var visited int64
			var visitErr error
			err := g.store.DsVisitManyWithTimesAsOf(ctx, snapshot, project, database, namespace, candidates, func(row *storage.DsEntityRow, missing string) bool {
				visited++
				if row != nil {
					visitErr = fn(row)
				}
				return visitErr == nil
			})
			return visited, errors.Join(err, visitErr)
		}
		if isMetadataKind(kind) {
			return g.store.DsVisitMetadataAsOf(ctx, snapshot, project, database, namespace, kind, fn)
		}
		return g.store.DsVisitKindEntitiesAsOf(ctx, snapshot, project, database, namespace, kind, ancestor, fn)
	}
	scanned, err = visit(func(row *storage.DsEntityRow) error {
		add := func(entity *datastorepb.Entity, selection []int, orderKey []byte) error {
			if orderKey == nil {
				var err error
				orderKey, err = ordering.key(ctx, entity)
				if err != nil || orderKey == nil {
					return err
				}
			}
			orderKey = append(orderKey, '|')
			orderKey = append(orderKey, row.Path...)
			orderKey = append(orderKey, '|')
			var offsets []byte
			for _, offset := range selection {
				offsets = binary.AppendUvarint(offsets, uint64(offset))
			}
			orderKey = append(orderKey, hex.EncodeToString(offsets)...)
			if cursor != nil {
				comparison := strings.Compare(fallbackRecordTuple(string(orderKey)), cursorTuple)
				if comparison < 0 || comparison == 0 && !cursor.Before {
					return nil
				}
			}
			return paths.Add(string(orderKey))
		}
		if len(fields) == 0 && !hasDotted {
			return add(row.Entity, nil, nil)
		}
		var addErr error
		var best *datastorepb.Entity
		var bestKey []byte
		for mode := 0; mode < 2; mode++ {
			if mode == 1 && !hasDotted {
				break
			}
			view := row.Entity
			if hasDotted {
				view = queryInterpretation(row.Entity, names, mode == 1)
			}
			if len(fields) == 0 {
				key, err := ordering.key(ctx, view)
				if err != nil {
					return err
				}
				if key != nil && (best == nil || bytes.Compare(key, bestKey) < 0) {
					best, bestKey = view, key
				}
				continue
			}
			projectionErr := visitProjectionSelection(ctx, view, fields, func(projected *datastorepb.Entity, selection []int, _ bool) bool {
				if ordering.aggregation {
					key, err := ordering.keySelections(ctx, view, "", nil, projected.Properties)
					if err != nil || key == nil {
						addErr = err
						return err == nil
					}
					selection, err = ordering.aggregationSelection(ctx, fields, selection)
					if err != nil {
						addErr = err
						return false
					}
					addErr = add(view, append([]int{mode}, selection...), key)
					return addErr == nil
				}
				merged := projectionWithSource(view, projected)
				selection = append([]int{mode}, selection...)
				addErr = add(merged, selection, nil)
				return addErr == nil
			})
			if projectionErr != nil {
				return projectionErr
			}
			if addErr != nil {
				return addErr
			}
		}
		if best != nil {
			return add(best, nil, bestKey)
		}
		return addErr
	})
	if err != nil {
		return nil, scanned, err
	}
	stream = &fallbackRows{paths: paths}
	stream.next, stream.stop = iter.Pull2(func(yield func(*storage.DsEntityRow, error) bool) {
		emit := func(row *storage.DsEntityRow) error {
			if !yield(row, nil) {
				return errQueryPageFull
			}
			return nil
		}
		accept := queryTuplePredicate(query, cursor, nil) // Stored tuples already passed exact evaluation.
		previousTuple := ""
		err := paths.ForEach(func(record string) error {
			tuple := fallbackRecordTuple(record)
			if tuple == previousTuple {
				return nil // The literal and canonical streams can share a tuple.
			}
			previousTuple = tuple
			parts := strings.Split(record, "|")
			if len(parts) != 3 {
				return status.Error(codes.Internal, "invalid sorted query record")
			}
			path := parts[1]
			if isMetadataKind(kind) {
				elements, err := keycodec.ParsePath(path)
				if err != nil {
					return err
				}
				entity := &datastorepb.Entity{Key: &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: project, DatabaseId: database, NamespaceId: namespace}, Path: elements}}
				if kind == "__property__" {
					entity, err = g.store.DsPropertyMetadataAsOf(ctx, snapshot, entity.Key)
					if err != nil {
						return err
					}
				}
				entity, err = restoreProjectionSelection(entity, names, fields, parts[2])
				if err != nil {
					return err
				}
				return emit(&storage.DsEntityRow{Entity: entity, Path: path, IndexKey: []byte(record)})
			}
			var selectionErr error
			var selected *storage.DsEntityRow
			err := g.store.DsVisitManyWithTimesAsOf(ctx, snapshot, project, database, namespace, []string{path}, func(row *storage.DsEntityRow, _ string) bool {
				if row != nil {
					row.Entity, selectionErr = restoreProjectionSelection(row.Entity, names, fields, parts[2])
					if selectionErr != nil || !accept(row.Entity) {
						return false
					}
					row.IndexKey = []byte(record)
					selected = row
				}
				return true
			})
			if err := errors.Join(err, selectionErr); err != nil {
				return err
			}
			if selected != nil {
				return emit(selected) // No entity-read transaction spans a yield.
			}
			return nil
		})
		if err != nil && !errors.Is(err, errQueryPageFull) {
			yield(nil, err)
		}
	})
	return stream, scanned, nil
}

func projectionWithSource(source, projected *datastorepb.Entity) *datastorepb.Entity {
	properties := make(map[string]*datastorepb.Value, len(source.Properties)+len(projected.Properties))
	for name, value := range source.Properties {
		properties[name] = value
	}
	for name, value := range projected.Properties {
		properties[name] = value
	}
	return &datastorepb.Entity{Key: source.Key, Properties: properties}
}

func restoreProjectionSelection(entity *datastorepb.Entity, names, fields []string, encoded string) (*datastorepb.Entity, error) {
	offsets, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	if len(fields) > 0 {
		mode, n := binary.Uvarint(offsets)
		if n <= 0 || mode > 1 {
			return nil, status.Error(codes.Internal, "invalid projection interpretation")
		}
		offsets = offsets[n:]
		entity = queryInterpretation(entity, names, mode == 1)
	}
	projected := &datastorepb.Entity{Properties: make(map[string]*datastorepb.Value, len(fields))}
	for _, field := range fields {
		offset, n := binary.Uvarint(offsets)
		if n <= 0 {
			return nil, status.Error(codes.Internal, "invalid projection offset")
		}
		offsets = offsets[n:]
		value := getProp(entity, field)
		if array := value.GetArrayValue(); array != nil {
			if offset >= uint64(len(array.Values)) {
				return nil, status.Error(codes.Internal, "projection offset outside snapshot")
			}
			value = array.Values[offset]
		}
		projected.Properties[field] = value
	}
	if len(offsets) != 0 {
		return nil, status.Error(codes.Internal, "unexpected projection offsets")
	}
	return projectionWithSource(entity, projected), nil
}

func fallbackRecordTuple(record string) string {
	if i := strings.LastIndexByte(record, '|'); i >= 0 {
		return record[:i]
	}
	return record
}

// queryInterpretation resolves only properties needed by this query, retaining
// one consistent interpretation across filters, orders and projected values.
func queryInterpretation(entity *datastorepb.Entity, names []string, canonical bool) *datastorepb.Entity {
	properties := make(map[string]*datastorepb.Value, len(names))
	for _, name := range names {
		if name != "__key__" {
			if value := propertypath.QueryValue(entity, name, canonical); value != nil {
				properties[name] = value
			}
		}
	}
	return &datastorepb.Entity{Key: entity.Key, Properties: properties}
}

func queryPropertyNames(query *datastorepb.Query) ([]string, bool) {
	seen := make(map[string]bool)
	var names []string
	dotted := false
	add := func(name string) {
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
			dotted = dotted || strings.Contains(name, ".")
		}
	}
	var visit func(*datastorepb.Filter)
	visit = func(filter *datastorepb.Filter) {
		if property := filter.GetPropertyFilter(); property != nil {
			add(property.Property.GetName())
		}
		for _, child := range filter.GetCompositeFilter().GetFilters() {
			visit(child)
		}
	}
	visit(query.Filter)
	for _, order := range query.Order {
		add(order.Property.GetName())
	}
	for _, projection := range query.Projection {
		add(projection.Property.GetName())
	}
	for _, distinct := range query.DistinctOn {
		add(distinct.GetName())
	}
	return names, dotted
}
