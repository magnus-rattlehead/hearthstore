package storage

import (
	"bytes"
	"context"
	"slices"
	"sort"
	"strings"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/dgraph-io/badger/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
	"github.com/magnus-rattlehead/hearthstore/internal/propertypath"
)

const maxBuiltinValueBytes = 1500
const maxEntityIndexedValues = 20_000
const maxMaterializedQueryBytes = 4 << 20

func queryMaterializedLimitReached(total *int, entity *datastorepb.Entity) bool {
	*total += proto.Size(entity)
	return *total > maxMaterializedQueryBytes
}

func builtinIndexBase(project, database, namespace, kind, property, ancestor string) []byte {
	return []byte("builtin/" + enc(project) + "/" + enc(database) + "/" + enc(namespace) + "/" + enc(kind) + "/" + enc(property) + "/" + indexScopeSegment(ancestor) + "/")
}

func builtinIndexEntry(base []byte, value *datastorepb.Value, path string, entityKey *datastorepb.Key) ([]byte, bool) {
	raw, ok := orderedValue(value, false)
	if !ok || len(value.GetStringValue()) > maxBuiltinValueBytes || len(value.GetBlobValue()) > maxBuiltinValueBytes {
		return nil, false
	}
	key := appendIndexComponent(append([]byte{}, base...), raw)
	key = appendIndexComponent(key, keycodec.Ordered(entityKey))
	key = append(key, enc(path)...)
	return key, true
}

func indexedProperties(entity *datastorepb.Entity) (map[string][]*datastorepb.Value, error) {
	out := make(map[string][]*datastorepb.Value)
	count := 0
	var walk func(string, map[string]*datastorepb.Value) error
	walk = func(prefix string, properties map[string]*datastorepb.Value) error {
		for name, value := range properties {
			if value == nil || value.ExcludeFromIndexes {
				continue
			}
			path := name
			if prefix != "" {
				path = prefix + "." + name
			}
			if nested := value.GetEntityValue(); nested != nil {
				if err := walk(path, nested.Properties); err != nil {
					return err
				}
				continue
			}
			if prefix != "" && propertypath.QueryValue(entity, path, true) != value {
				continue // Intermediate literal dots are not canonical paths.
			}
			values := []*datastorepb.Value{value}
			if array := value.GetArrayValue(); array != nil {
				values = array.Values
			}
			for _, candidate := range values {
				if candidate != nil && !candidate.ExcludeFromIndexes && candidate.GetEntityValue() == nil {
					out[path] = append(out[path], candidate)
					count++
					if count > maxEntityIndexedValues {
						return status.Errorf(codes.InvalidArgument, "entity contains more than %d indexed values", maxEntityIndexedValues)
					}
				}
			}
		}
		return nil
	}
	if err := walk("", entity.GetProperties()); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) maintainBuiltinIndexes(tx *Txn, project, database, namespace, path, kind string, oldEntity, newEntity *datastorepb.Entity, record dsRecord) error {
	apply := func(entity *datastorepb.Entity, remove bool) error {
		if entity == nil {
			return nil
		}
		properties, err := indexedProperties(entity)
		if err != nil {
			return err
		}
		properties["__key__"] = []*datastorepb.Value{{ValueType: &datastorepb.Value_KeyValue{KeyValue: entity.Key}}}
		for property, values := range properties {
			for _, scope := range indexScopes(path) {
				base := builtinIndexBase(project, database, namespace, kind, property, scope)
				seen := make(map[string]struct{}, len(values))
				for _, value := range values {
					key, ok := builtinIndexEntry(base, value, path, entity.Key)
					if !ok {
						continue
					}
					if _, duplicate := seen[string(key)]; duplicate {
						continue
					}
					seen[string(key)] = struct{}{}
					if remove {
						if err := tx.Delete(key); err != nil {
							return err
						}
					} else {
						cover := &datastorepb.Entity{Key: entity.Key}
						if property != "__key__" {
							cover.Properties = map[string]*datastorepb.Value{property: value}
						}
						encoded, err := encodeIndexValue(path, record, cover)
						if err != nil {
							return err
						}
						if err := tx.Set(key, encoded); err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	}
	if err := apply(oldEntity, true); err != nil {
		return err
	}
	return apply(newEntity, false)
}

// DsQueryBuiltin scans an index using the requested snapshot, filtering, and row shape.
func (s *Store) DsQueryBuiltin(ctx context.Context, q BuiltinQuery) (QueryPage, error) {
	return s.dsQueryBuiltin(ctx, q, nil)
}

func (s *Store) dsQueryBuiltin(ctx context.Context, q BuiltinQuery, probe *indexProbe) (QueryPage, error) {
	var readTs uint64
	if q.ReadTime != nil {
		readTs = uint64(q.ReadTime.UnixNano())
	}
	work := QueryWorkFromContext(ctx)
	if err := work.Checkpoint(ctx); err != nil {
		return QueryPage{}, err
	}
	spans, supported := builtinIndexSpans(q.Property, q.Filter)
	if !supported {
		return QueryPage{}, status.Error(codes.FailedPrecondition, "query cannot be bounded by a built-in index")
	}
	if q.Reverse {
		slices.Reverse(spans)
	}
	// A point span contains at most one persisted entry per entity, even when
	// both path interpretations (or repeated array values) encode identically.
	pointSpan := len(spans) == 1 && spans[0].lower != nil && spans[0].upper != nil && spans[0].lowerInclusive && spans[0].upperInclusive && bytes.Equal(spans[0].lower, spans[0].upper)
	base := builtinIndexBase(q.Project, q.Database, q.Namespace, q.Kind, q.Property, q.Ancestor)
	var out []*DsEntityRow
	materializedBytes := 0
	rowLimit := q.Limit
	if rowLimit < 0 {
		rowLimit = -rowLimit
	}
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
	err := view(func(tx *Txn) error {
		var cursorComponent []byte
		resume := q.Cursor != nil && len(q.Cursor.K) > 0
		if resume {
			if !bytes.HasPrefix(q.Cursor.K, base) {
				return status.Error(codes.InvalidArgument, "invalid cursor")
			}
			component, _, ok := takeIndexComponent(q.Cursor.K[len(base):])
			if !ok {
				return status.Error(codes.InvalidArgument, "invalid cursor")
			}
			cursorComponent = component
		}
		if probe != nil {
			// Validate the physical cursor above, but deduplicate the complete
			// range before the caller applies its position.
			q.Cursor, resume = nil, false
		}
		for _, bounds := range spans {
			useCursor := resume && spanContains(bounds, cursorComponent)
			if resume && !useCursor {
				continue
			}
			opts := badger.DefaultIteratorOptions
			opts.Reverse = q.Reverse
			it := tx.NewIterator(opts)
			start := base
			if q.Reverse {
				start = prefixEnd(base)
				if bounds.upper != nil {
					start = append(append(append([]byte{}, base...), bounds.upper...), 0xff)
				}
			} else if bounds.lower != nil {
				start = append(append([]byte{}, base...), bounds.lower...)
			}
			if useCursor {
				start = q.Cursor.K
				resume = false
			}
			for it.Seek(start); it.ValidForPrefix(base); it.Next() {
				if err := probe.step(); err != nil {
					it.Close()
					return err
				}
				work.Charge(WorkIndexEntries, 1)
				if err := work.Checkpoint(ctx); err != nil {
					it.Close()
					return err
				}
				key := it.Item().KeyCopy(nil)
				component, _, ok := takeIndexComponent(key[len(base):])
				if !ok {
					continue
				}
				if bounds.lower != nil {
					cmp := bytes.Compare(component, bounds.lower)
					if cmp < 0 || cmp == 0 && !bounds.lowerInclusive {
						if q.Reverse {
							break
						}
						continue
					}
				}
				if bounds.upper != nil {
					cmp := bytes.Compare(component, bounds.upper)
					if cmp > 0 || cmp == 0 && !bounds.upperInclusive {
						if q.Reverse {
							continue
						}
						break
					}
				}
				scanned++
				if q.Cursor != nil && q.Cursor.O == 0 && bytes.Equal(key, q.Cursor.K) {
					continue
				}
				value, err := itemValue(it.Item())
				if err != nil {
					it.Close()
					return err
				}
				path, _, err := splitIndexValue(value)
				if err != nil {
					it.Close()
					return err
				}
				if _, duplicate := seen[path]; duplicate && !q.Projection {
					continue
				}
				var record dsRecord
				var entity *datastorepb.Entity
				if q.Projection {
					_, record, entity, err = decodeIndexValue(value)
					work.Charge(WorkDecodedBytes, uint64(len(record.Data)))
				} else {
					record, entity, err = getDSQueryTxn(ctx, tx, q.Project, q.Database, q.Namespace, path)
				}
				if err != nil {
					it.Close()
					return err
				}
				// Choose the entity's first qualifying index entry independently of
				// the page cursor, so arrays cannot reappear on subsequent pages.
				if !q.Projection && !pointSpan && (strings.Contains(q.Property, ".") || dsProperty(entity, q.Property).GetArrayValue() != nil) {
					var first []byte
					var candidateErr error
					choose := func(candidate *datastorepb.Value, canonical bool) {
						if candidateErr == nil {
							candidateErr = work.Checkpoint(ctx)
						}
						if candidateErr != nil {
							return
						}
						if q.AcceptCandidate != nil && !q.AcceptCandidate(entity, canonical, candidate) {
							return
						}
						entry, ok := builtinIndexEntry(base, candidate, path, entity.Key)
						if !ok {
							return
						}
						component, _, ok := takeIndexComponent(entry[len(base):])
						if !ok {
							return
						}
						eligible := false
						for _, span := range spans {
							if spanContains(span, component) {
								eligible = true
								break
							}
						}
						if eligible && (first == nil || !q.Reverse && bytes.Compare(entry, first) < 0 || q.Reverse && bytes.Compare(entry, first) > 0) {
							first = entry
						}
					}
					if q.AcceptCandidate != nil {
						propertypath.QueryValuesByInterpretation(entity, q.Property, choose)
					} else {
						propertypath.QueryValues(entity, q.Property, func(candidate *datastorepb.Value) bool {
							choose(candidate, false)
							return candidateErr == nil
						})
					}
					if candidateErr != nil {
						it.Close()
						return candidateErr
					}
					if !bytes.Equal(first, key) {
						continue
					}
				}
				if q.Projection {
					if entity.Properties[q.Property].GetArrayValue() != nil {
						continue
					} // Whole-array entries are not projection tuples.
				}
				if q.Accept != nil && !q.Accept(entity) {
					continue
				}
				row := rowFrom(record, entity, path)
				row.IndexKey = key
				out = append(out, row)
				if q.Projection {
					materializedBytes += len(path) + len(key)
				} else {
					seen[path] = struct{}{}
				}
				memoryLimit := q.Limit >= 0 && queryMaterializedLimitReached(&materializedBytes, entity)
				if memoryLimit || rowLimit > 0 && len(out) >= rowLimit {
					more = true
					break
				}
			}
			it.Close()
			if q.Limit >= 0 && materializedBytes > maxMaterializedQueryBytes || rowLimit > 0 && len(out) >= rowLimit {
				break
			}
		}
		return nil
	})
	return QueryPage{Rows: out, Scanned: scanned, More: more}, err
}

func spanContains(bounds compositeScanBounds, value []byte) bool {
	if bounds.lower != nil {
		cmp := bytes.Compare(value, bounds.lower)
		if cmp < 0 || cmp == 0 && !bounds.lowerInclusive {
			return false
		}
	}
	if bounds.upper != nil {
		cmp := bytes.Compare(value, bounds.upper)
		if cmp > 0 || cmp == 0 && !bounds.upperInclusive {
			return false
		}
	}
	return true
}

// DsCountBuiltin counts matching entity keys without loading entity bodies.
func (s *Store) DsCountBuiltin(ctx context.Context, project, database, namespace, kind, property, ancestor string, filter *datastorepb.Filter) (int64, int64, error) {
	return s.dsCountBuiltinAt(ctx, 0, project, database, namespace, kind, property, ancestor, filter)
}

// DsCountBuiltinAsOf counts matching entity keys at a historical timestamp.
func (s *Store) DsCountBuiltinAsOf(ctx context.Context, asOf time.Time, project, database, namespace, kind, property, ancestor string, filter *datastorepb.Filter) (int64, int64, error) {
	return s.dsCountBuiltinAt(ctx, uint64(asOf.UnixNano()), project, database, namespace, kind, property, ancestor, filter)
}

func (s *Store) dsCountBuiltinAt(ctx context.Context, readTs uint64, project, database, namespace, kind, property, ancestor string, filter *datastorepb.Filter) (int64, int64, error) {
	paths, err := s.NewPathAccumulator(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer paths.Close()
	scanned, err := s.dsVisitBuiltinPathsAt(ctx, readTs, project, database, namespace, kind, property, ancestor, filter, paths.Add)
	if err != nil {
		return 0, scanned, err
	}
	count, err := paths.Count()
	return count, scanned, err
}

// DsVisitBuiltinPaths streams matching index paths, including duplicate array entries.
func (s *Store) DsVisitBuiltinPaths(ctx context.Context, project, database, namespace, kind, property, ancestor string, filter *datastorepb.Filter, visit func(string) error) (int64, error) {
	return s.dsVisitBuiltinPathsAt(ctx, 0, project, database, namespace, kind, property, ancestor, filter, visit)
}

// DsVisitBuiltinPathsAsOf streams matching index paths at a historical timestamp.
func (s *Store) DsVisitBuiltinPathsAsOf(ctx context.Context, asOf time.Time, project, database, namespace, kind, property, ancestor string, filter *datastorepb.Filter, visit func(string) error) (int64, error) {
	return s.dsVisitBuiltinPathsAt(ctx, uint64(asOf.UnixNano()), project, database, namespace, kind, property, ancestor, filter, visit)
}

// DsScanBuiltinPaths returns matching entity paths without loading entity bodies.
func (s *Store) DsScanBuiltinPaths(ctx context.Context, project, database, namespace, kind, property, ancestor string, filter *datastorepb.Filter) ([]string, int64, error) {
	return s.dsScanBuiltinPathsAt(ctx, 0, project, database, namespace, kind, property, ancestor, filter)
}

// DsScanBuiltinPathsAsOf returns matching entity paths at a historical timestamp.
func (s *Store) DsScanBuiltinPathsAsOf(ctx context.Context, asOf time.Time, project, database, namespace, kind, property, ancestor string, filter *datastorepb.Filter) ([]string, int64, error) {
	return s.dsScanBuiltinPathsAt(ctx, uint64(asOf.UnixNano()), project, database, namespace, kind, property, ancestor, filter)
}

func (s *Store) dsScanBuiltinPathsAt(ctx context.Context, readTs uint64, project, database, namespace, kind, property, ancestor string, filter *datastorepb.Filter) ([]string, int64, error) {
	paths, err := s.NewPathAccumulator(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer paths.Close()
	scanned, err := s.dsVisitBuiltinPathsAt(ctx, readTs, project, database, namespace, kind, property, ancestor, filter, paths.Add)
	if err != nil {
		return nil, scanned, err
	}
	var out []string
	err = paths.ForEach(func(path string) error { out = append(out, path); return nil })
	return out, scanned, err
}

func (s *Store) dsVisitBuiltinPathsAt(ctx context.Context, readTs uint64, project, database, namespace, kind, property, ancestor string, filter *datastorepb.Filter, visit func(string) error) (int64, error) {
	work := QueryWorkFromContext(ctx)
	spans, supported := builtinIndexSpans(property, filter)
	if !supported {
		return 0, status.Error(codes.FailedPrecondition, "query cannot be bounded by a built-in index")
	}
	base := builtinIndexBase(project, database, namespace, kind, property, ancestor)
	var scanned int64
	view := s.view
	if readTs != 0 {
		view = func(fn func(*Txn) error) error { return s.viewAt(readTs, fn) }
	}
	err := view(func(tx *Txn) error {
		for _, bounds := range spans {
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			start := base
			if bounds.lower != nil {
				start = append(append([]byte{}, base...), bounds.lower...)
			}
			for it.Seek(start); it.ValidForPrefix(base); it.Next() {
				work.Charge(WorkIndexEntries, 1)
				if err := work.Checkpoint(ctx); err != nil {
					it.Close()
					return err
				}
				key := it.Item().KeyCopy(nil)
				component, _, ok := takeIndexComponent(key[len(base):])
				if !ok {
					continue
				}
				if bounds.lower != nil {
					cmp := bytes.Compare(component, bounds.lower)
					if cmp < 0 || cmp == 0 && !bounds.lowerInclusive {
						continue
					}
				}
				if bounds.upper != nil {
					cmp := bytes.Compare(component, bounds.upper)
					if cmp > 0 || cmp == 0 && !bounds.upperInclusive {
						break
					}
				}
				scanned++
				value, err := itemValue(it.Item())
				if err != nil {
					it.Close()
					return err
				}
				path, _, err := splitIndexValue(value)
				if err != nil {
					it.Close()
					return err
				}
				if err := visit(path); err != nil {
					it.Close()
					return err
				}
			}
			it.Close()
		}
		return nil
	})
	return scanned, err
}

// DsVisitKindEntities streams a kind at the latest snapshot without a page buffer.
func (s *Store) DsVisitKindEntities(ctx context.Context, project, database, namespace, kind, ancestor string, visit func(*DsEntityRow) error) (int64, error) {
	return s.dsVisitKindEntitiesAt(ctx, 0, project, database, namespace, kind, ancestor, visit)
}

// DsVisitKindEntitiesAsOf streams a kind at a historical snapshot.
func (s *Store) DsVisitKindEntitiesAsOf(ctx context.Context, asOf time.Time, project, database, namespace, kind, ancestor string, visit func(*DsEntityRow) error) (int64, error) {
	return s.dsVisitKindEntitiesAt(ctx, uint64(asOf.UnixNano()), project, database, namespace, kind, ancestor, visit)
}

func (s *Store) dsVisitKindEntitiesAt(ctx context.Context, readTs uint64, project, database, namespace, kind, ancestor string, visit func(*DsEntityRow) error) (int64, error) {
	work := QueryWorkFromContext(ctx)
	prefix := dsKindPrefix(project, database, namespace, kind)
	if kind == "" {
		prefix = dsPrefix(project, database, namespace)
	}
	view := s.view
	if readTs != 0 {
		view = func(fn func(*Txn) error) error { return s.viewAt(readTs, fn) }
	}
	var scanned int64
	err := view(func(tx *Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			work.Charge(WorkIndexEntries, 1)
			if err := work.Checkpoint(ctx); err != nil {
				return err
			}
			path := dec(string(it.Item().Key()[len(prefix):]))
			if ancestor != "" && path != ancestor && !strings.HasPrefix(path, ancestor+"/") {
				continue
			}
			scanned++
			record, entity, err := getDSQueryTxn(ctx, tx, project, database, namespace, path)
			if kind == "" && status.Code(err) == codes.NotFound {
				continue
			}
			if err != nil {
				return err
			}
			if err := visit(rowFrom(record, entity, path)); err != nil {
				return err
			}
		}
		return nil
	})
	return scanned, err
}

func builtinIndexSpans(property string, filter *datastorepb.Filter) ([]compositeScanBounds, bool) {
	full := []compositeScanBounds{{}}
	if filter == nil {
		return full, true
	}
	switch typed := filter.FilterType.(type) {
	case *datastorepb.Filter_PropertyFilter:
		pf := typed.PropertyFilter
		if pf.Op == datastorepb.PropertyFilter_HAS_ANCESTOR && pf.Property.GetName() == "__key__" {
			return full, true
		}
		if pf.Property.GetName() != property {
			return nil, false
		}
		return propertyFilterSpans(pf)
	case *datastorepb.Filter_CompositeFilter:
		if typed.CompositeFilter.Op == datastorepb.CompositeFilter_OR {
			var spans []compositeScanBounds
			for _, child := range typed.CompositeFilter.Filters {
				childSpans, ok := builtinIndexSpans(property, child)
				if !ok {
					return nil, false
				}
				spans = append(spans, childSpans...)
			}
			return normalizeSpans(spans), true
		}
		spans := full
		for _, child := range typed.CompositeFilter.Filters {
			childSpans, ok := builtinIndexSpans(property, child)
			if !ok {
				return nil, false
			}
			spans = intersectSpanSets(spans, childSpans)
		}
		return normalizeSpans(spans), true
	default:
		return nil, false
	}
}

func propertyFilterSpans(filter *datastorepb.PropertyFilter) ([]compositeScanBounds, bool) {
	encode := func(value *datastorepb.Value) ([]byte, bool) {
		raw, ok := orderedValue(value, false)
		if !ok {
			return nil, false
		}
		return encodeIndexComponent(raw), true
	}
	value, ok := encode(filter.Value)
	if !ok && filter.Op != datastorepb.PropertyFilter_IN && filter.Op != datastorepb.PropertyFilter_NOT_IN {
		return nil, false
	}
	switch filter.Op {
	case datastorepb.PropertyFilter_EQUAL:
		return []compositeScanBounds{{lower: value, upper: value, lowerInclusive: true, upperInclusive: true}}, true
	case datastorepb.PropertyFilter_LESS_THAN:
		return []compositeScanBounds{{upper: value}}, true
	case datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL:
		return []compositeScanBounds{{upper: value, upperInclusive: true}}, true
	case datastorepb.PropertyFilter_GREATER_THAN:
		return []compositeScanBounds{{lower: value}}, true
	case datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
		return []compositeScanBounds{{lower: value, lowerInclusive: true}}, true
	case datastorepb.PropertyFilter_NOT_EQUAL:
		return []compositeScanBounds{{upper: value}, {lower: value}}, true
	case datastorepb.PropertyFilter_IN:
		var spans []compositeScanBounds
		for _, candidate := range filter.Value.GetArrayValue().GetValues() {
			encoded, valid := encode(candidate)
			if !valid {
				return nil, false
			}
			spans = append(spans, compositeScanBounds{lower: encoded, upper: encoded, lowerInclusive: true, upperInclusive: true})
		}
		return spans, true
	case datastorepb.PropertyFilter_NOT_IN:
		spans := []compositeScanBounds{{}}
		for _, candidate := range filter.Value.GetArrayValue().GetValues() {
			encoded, valid := encode(candidate)
			if !valid {
				return nil, false
			}
			spans = intersectSpanSets(spans, []compositeScanBounds{{upper: encoded}, {lower: encoded}})
		}
		return normalizeSpans(spans), true
	default:
		return nil, false
	}
}

// intersectSpanSets intersects disjoint sorted unions with a two-way sweep.
// Normalize before joining: duplicate/overlapping input must not form a product.
func intersectSpanSets(left, right []compositeScanBounds) []compositeScanBounds {
	left, right = normalizeSpans(left), normalizeSpans(right)
	var out []compositeScanBounds
	for i, j := 0, 0; i < len(left) && j < len(right); {
		a, b := left[i], right[j]
		span := a
		if b.lower != nil {
			span.setLower(b.lower, b.lowerInclusive)
		}
		if b.upper != nil {
			span.setUpper(b.upper, b.upperInclusive)
		}
		if nonemptySpan(span) {
			out = append(out, span)
		}
		switch {
		case a.upper == nil && b.upper == nil:
			i++
			j++
		case a.upper == nil:
			j++
		case b.upper == nil:
			i++
		default:
			cmp := bytes.Compare(a.upper, b.upper)
			if cmp <= 0 {
				i++
			}
			if cmp >= 0 {
				j++
			}
		}
	}
	return out
}

func nonemptySpan(span compositeScanBounds) bool {
	if span.lower == nil || span.upper == nil {
		return true
	}
	cmp := bytes.Compare(span.lower, span.upper)
	return cmp < 0 || cmp == 0 && span.lowerInclusive && span.upperInclusive
}

// normalizeSpans returns the exact disjoint union, retaining open endpoint holes.
func normalizeSpans(spans []compositeScanBounds) []compositeScanBounds {
	sort.Slice(spans, func(i, j int) bool {
		if spans[i].lower == nil {
			return spans[j].lower != nil
		}
		if spans[j].lower == nil {
			return false
		}
		cmp := bytes.Compare(spans[i].lower, spans[j].lower)
		if cmp == 0 {
			return spans[i].lowerInclusive && !spans[j].lowerInclusive
		}
		return cmp < 0
	})
	out := spans[:0]
	for _, span := range spans {
		if !nonemptySpan(span) {
			continue
		}
		if len(out) == 0 {
			out = append(out, span)
			continue
		}
		last := &out[len(out)-1]
		overlap := last.upper == nil || span.lower == nil
		if !overlap {
			cmp := bytes.Compare(span.lower, last.upper)
			overlap = cmp < 0 || cmp == 0 && (span.lowerInclusive || last.upperInclusive)
		}
		if !overlap {
			out = append(out, span)
			continue
		}
		if last.upper == nil {
			continue
		}
		if span.upper == nil {
			last.upper = nil
			last.upperInclusive = false
			continue
		}
		cmp := bytes.Compare(span.upper, last.upper)
		if cmp > 0 {
			last.upper, last.upperInclusive = span.upper, span.upperInclusive
		} else if cmp == 0 {
			last.upperInclusive = last.upperInclusive || span.upperInclusive
		}
	}
	return out
}

func prefixEnd(prefix []byte) []byte {
	end := append([]byte{}, prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
