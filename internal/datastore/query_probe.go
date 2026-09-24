package datastore

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// Short pages should not pay for a full-size planning probe. The 16-entry
// floor retains useful sparse/array candidates; the 256-entry compiler allowance
// remains the ceiling. Incomplete probes always execute the original query.
func queryProbeAllowance(ctx context.Context, q *datastorepb.Query) int {
	allowance := queryOptimizerAllowance(ctx)
	if q.Limit == nil {
		return allowance
	}
	requested := min(int64(q.Limit.Value)+int64(q.Offset)+1, int64(math.MaxInt32))
	return min(allowance, max(16, int(requested)))
}

func scalarRangeFilter(p *datastorepb.PropertyFilter) bool {
	if p == nil || strings.Contains(p.Property.GetName(), ".") {
		return false
	}
	switch p.Value.GetValueType().(type) {
	case nil, *datastorepb.Value_ArrayValue, *datastorepb.Value_EntityValue:
		return false
	}
	switch p.Op {
	case datastorepb.PropertyFilter_EQUAL, datastorepb.PropertyFilter_LESS_THAN, datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL, datastorepb.PropertyFilter_GREATER_THAN, datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
		return true
	}
	return false
}

// candidateORAccess uses only independently complete scalar branches. General
// Boolean/witness shapes keep the existing bounded executor.
func candidateORAccess(q *datastorepb.Query, condition *compiledCondition, allowance int, start, end *storage.CursorPayload) []builtinUnionBranch {
	if len(q.Kind) != 1 || isMetadataKind(q.Kind[0].Name) || len(q.DistinctOn) != 0 || len(q.Projection) > 0 && !isKeysOnly(q) || !condition.accessPlanningFits(allowance) || condition.needsEntityWitnesses() || q.Limit != nil && q.Limit.Value == 0 {
		return nil
	}
	// Existing physical cursors continue on their original access path. New
	// candidate cursors use the existing fallback format, including end bounds.
	for _, c := range []*storage.CursorPayload{start, end} {
		if c != nil && c.I != "fallback" {
			return nil
		}
	}
	_, dotted := queryPropertyNames(q)
	if dotted {
		return nil
	}
	var branches []builtinUnionBranch
	names := map[string]bool{}
	var collect func(*datastorepb.Filter) bool
	collect = func(f *datastorepb.Filter) bool {
		if p := f.GetPropertyFilter(); p != nil {
			if !scalarRangeFilter(p) || p.Property.GetName() == "__key__" {
				return false
			}
			names[p.Property.GetName()] = true
			branches = append(branches, builtinUnionBranch{property: p.Property.GetName(), filter: f})
			return true
		}
		cf := f.GetCompositeFilter()
		if cf == nil || cf.Op != datastorepb.CompositeFilter_OR {
			return false
		}
		for _, child := range cf.Filters {
			if !collect(child) {
				return false
			}
		}
		return true
	}
	if !collect(q.Filter) || len(names) < 2 {
		return nil
	}
	return branches
}

func (g *GRPCServer) probeORPaths(ctx context.Context, snapshot time.Time, project, database, namespace, kind string, branches []builtinUnionBranch, allowance int) ([]string, int64, bool, error) {
	paths := make([]string, 0)
	seen := map[string]bool{}
	retained, scanned := 0, int64(0)
	for _, branch := range branches {
		probe, err := g.store.DsProbeBuiltinAsOf(ctx, storage.BuiltinProbe{
			ReadTime:  snapshot,
			Project:   project,
			Database:  database,
			Namespace: namespace,
			Kind:      kind,
			Property:  branch.property,
			Filter:    branch.filter,
			Allowance: allowance - int(scanned),
		})
		scanned += probe.Visited
		if err != nil || !probe.Complete {
			return nil, scanned, false, err
		}
		for _, row := range probe.Rows {
			if !seen[row.Path] {
				retained += len(row.Path)
				if retained > maxQueryResponseBytes {
					return nil, scanned, false, nil
				}
				seen[row.Path] = true
				paths = append(paths, row.Path)
			}
		}
	}
	return paths, scanned, true, nil
}

func (g *GRPCServer) candidateQueryRows(ctx context.Context, snapshot time.Time, project, database, namespace, kind string, query *datastorepb.Query, condition *compiledCondition, cursor *storage.CursorPayload, limit int, candidates []string) ([]*storage.DsEntityRow, bool, error) {
	stream, _, err := g.prepareFallbackRows(ctx, snapshot, project, database, namespace, kind, "", query, condition, cursor, candidates)
	if err != nil {
		return nil, false, err
	}
	rows, more, err := stream.page(ctx, limit)
	return rows, more, errors.Join(err, stream.close())
}

// rangeKeysAccess proves all predicates and order fields are in each tuple,
// with no independent equality/range witnesses on the same array property.
func rangeKeysAccess(q *datastorepb.Query, idx *storage.DsCompositeIndex, builtin string) bool {
	if !isKeysOnly(q) || len(q.DistinctOn) != 0 || idx == nil && builtin == "__key__" {
		return false
	}
	covered := map[string]bool{"__key__": true}
	if idx != nil {
		if idx.Ancestor || idx.State != storage.DsIndexReady {
			return false
		}
		for _, p := range idx.Properties {
			if strings.Contains(p.Name, ".") {
				return false
			}
			covered[p.Name] = true
		}
	} else {
		covered[builtin] = true
	}
	for _, order := range q.Order {
		if !covered[order.Property.GetName()] {
			return false
		}
	}
	equalities, ranges := map[string]bool{}, map[string]bool{}
	var collect func(*datastorepb.Filter) bool
	collect = func(f *datastorepb.Filter) bool {
		if p := f.GetPropertyFilter(); p != nil {
			name := p.Property.GetName()
			if !covered[name] || !scalarRangeFilter(p) {
				return false
			}
			if p.Op == datastorepb.PropertyFilter_EQUAL {
				if equalities[name] || ranges[name] {
					return false
				}
				equalities[name] = true
			} else {
				if equalities[name] {
					return false
				}
				ranges[name] = true
			}
			return true
		}
		cf := f.GetCompositeFilter()
		if cf == nil || cf.Op != datastorepb.CompositeFilter_AND {
			return false
		}
		for _, child := range cf.Filters {
			if !collect(child) {
				return false
			}
		}
		return true
	}
	return collect(q.Filter) && len(ranges) > 0
}

// Complete probes are bounded. Deduplicate from the beginning of the range,
// then apply the cursor, so later array entries cannot resurrect an entity.
func coveringRangePage(rows []*storage.DsEntityRow, cursor *storage.CursorPayload, reverse bool, limit int, accept func(*datastorepb.Entity) bool) ([]*storage.DsEntityRow, bool) {
	seen := map[string]bool{}
	var out []*storage.DsEntityRow
	for _, row := range rows {
		if !accept(row.Entity) || seen[row.Path] {
			continue
		}
		seen[row.Path] = true
		if cursor != nil {
			cmp := bytes.Compare(row.IndexKey, cursor.K)
			if reverse {
				cmp = -cmp
			}
			if cmp <= 0 {
				continue
			}
		}
		if limit > 0 && len(out) >= limit {
			return out, true
		}
		out = append(out, row)
	}
	return out, false
}
