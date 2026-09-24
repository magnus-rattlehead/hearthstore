package datastore

import (
	"context"
	"encoding/hex"
	"slices"
	"strings"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// aggregationQueryCursors translates the original query's logical boundary to
// the aggregate index schema. Java pads missing postfix dimensions with an
// after-row sentinel; it does not reinterpret the cursor as an entity offset.
func (g *GRPCServer) aggregationQueryCursors(project, database, namespace string, original, planned *datastorepb.Query) error {
	if len(planned.StartCursor) == 0 && len(planned.EndCursor) == 0 {
		return nil
	}
	original = queryWithEffectiveOrder(original)
	originalHash := queryFingerprint(project, database, namespace, original)
	plannedHash := queryFingerprint(project, database, namespace, planned)
	condition, err := compileQueryCondition(original.Filter, 30)
	if err != nil {
		return err
	}
	schema := normalizedQueryOrder(original, condition, projectionFields(original), false)
	for _, target := range []*[]byte{&planned.StartCursor, &planned.EndCursor} {
		if len(*target) == 0 {
			continue
		}
		cursor, err := g.queryCursor(*target, originalHash, project, database, namespace, original, schema, false)
		if err != nil {
			return err
		}
		parts := strings.Split(string(cursor.B), "/")
		key := parts[len(parts)-1]
		var values []string
		for i, order := range schema.Order {
			if order.Property.Name != "__key__" {
				values = append(values, parts[i])
			}
		}
		var bound []string
		missing, suffix := "g", "~" // After every hex scalar / row selection.
		if cursor.Before {
			missing, suffix = "", "!" // Before every hex scalar / row selection.
		}
		if len(values) >= len(planned.Order) {
			return status.Error(codes.InvalidArgument, "cursor does not match aggregate schema")
		}
		// Java cursorBound pads positional postfix slots, then appends the
		// separate cursor key in the final slot, even for key-before-field
		// schemas. Sentinels here are already in sort coordinates.
		bound = append(bound, values...)
		for len(bound) < len(planned.Order)-1 {
			bound = append(bound, missing)
		}
		rawKey, err := hex.DecodeString("07" + key)
		if err != nil {
			return status.Error(codes.InvalidArgument, "invalid cursor key")
		}
		if planned.Order[len(planned.Order)-1].Direction == datastorepb.PropertyOrder_DESCENDING {
			for i := range rawKey {
				rawKey[i] = ^rawKey[i]
			}
		}
		bound = append(bound, hex.EncodeToString(rawKey))
		cursor.I, cursor.G, cursor.O, cursor.H = "fallback", 0, 0, plannedHash
		cursor.K = []byte(strings.Join(bound, "/") + "/" + key + "|" + cursor.P + "|" + suffix)
		*target = encodeCursorFull(*cursor)
	}
	return nil
}

type aggregationEntriesKey struct{}

func aggregationEntries(ctx context.Context) bool {
	active, _ := ctx.Value(aggregationEntriesKey{}).(bool)
	return active
}

// aggregationAccess is RPC-owned. An empty prepared plan means exact fallback,
// not a missing-index error. Internal pages retain the original access decision.
type aggregationAccess struct {
	planned bool
	builtin string
	index   *storage.DsCompositeIndex
}

func (a *aggregationAccess) prepare(ctx context.Context, indexes *IndexManager, project string, q *datastorepb.Query, condition *compiledCondition, readAt time.Time) error {
	if a.planned {
		return nil
	}
	a.planned = true
	if !aggregationIndexEligible(q, condition, queryOptimizerAllowance(ctx)) {
		return nil
	}
	// Built-in storage already orders equal values by ascending entity key.
	// Remove only this implicit dimension from access discovery, not execution.
	accessQuery := &datastorepb.Query{Kind: q.Kind, Filter: q.Filter, Order: q.Order[:len(q.Order)-1], Projection: q.Projection}
	if property, reverse, ok := builtinQueryAccess(accessQuery); ok && !reverse {
		a.builtin = property
		return nil
	}
	index, err := indexes.selectQueryIndex(ctx, project, q, hasAncestorFilter(q.Filter), "")
	if err != nil {
		return err
	}
	if index != nil && index.State == storage.DsIndexReady && index.ReadySince <= readAt.UnixNano() && storage.CompositeBoundsSupported(*index, q.Filter) {
		a.index = index
	}
	return nil
}

// Eligibility depends on query semantics, never on sampled entity shapes.
// Equality prefixes must be disjoint from postfix dimensions: arrays can use
// independent values in those slots, which ordinary projection indexes lack.
func aggregationIndexEligible(q *datastorepb.Query, condition *compiledCondition, allowance int) bool {
	if len(q.Kind) != 1 || isMetadataKind(q.Kind[0].Name) || len(q.Projection) == 0 || len(q.DistinctOn) != 0 || len(q.Order) < 2 || !condition.accessPlanningFits(allowance) || condition.needsEntityWitnesses() {
		return false
	}
	dimensions := make(map[string]bool, len(q.Order))
	for i, order := range q.Order {
		name := order.Property.GetName()
		if strings.Contains(name, ".") || order.Direction != datastorepb.PropertyOrder_ASCENDING || name == "__key__" && i != len(q.Order)-1 {
			return false
		}
		dimensions[name] = true
	}
	if q.Order[len(q.Order)-1].Property.GetName() != "__key__" {
		return false
	}
	for _, node := range condition.nodes {
		if node.op == datastorepb.CompositeFilter_OR {
			return false
		}
		f := node.property
		if f == nil {
			continue
		}
		name := f.Property.GetName()
		if strings.Contains(name, ".") {
			return false
		}
		switch f.Op {
		case datastorepb.PropertyFilter_HAS_ANCESTOR:
			if name != "__key__" {
				return false
			}
		case datastorepb.PropertyFilter_EQUAL:
			if dimensions[name] {
				return false
			}
		case datastorepb.PropertyFilter_LESS_THAN, datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL,
			datastorepb.PropertyFilter_GREATER_THAN, datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
			if name == "__key__" || !dimensions[name] {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// aggregationEntryQuery mirrors Java's aggregation index schema: preserve the
// explicit order, add distinct/range/aggregate fields, then the key. Unlike a
// full-entity query, every distinct ordered array tuple is an input row.
func aggregationEntryQuery(q *datastorepb.Query, condition *compiledCondition, aggs []*datastorepb.AggregationQuery_Aggregation) *datastorepb.Query {
	fields := projectionFields(q)
	for _, agg := range aggs {
		if op := agg.GetSum(); op != nil {
			fields = append(fields, op.Property.Name)
		}
		if op := agg.GetAvg(); op != nil {
			fields = append(fields, op.Property.Name)
		}
	}
	result := normalizedQueryOrder(q, condition, fields, true)
	result.Projection = nil
	for _, order := range result.Order {
		if order.Property.Name != "__key__" {
			result.Projection = append(result.Projection, &datastorepb.Projection{Property: order.Property})
		}
	}
	return result
}

// Prefix filters occupy separate index slots from the ordered postfix. Java's
// FieldAccumulator intentionally reads the first slot for an aggregate field.
// Store its original array offset alongside the postfix key in spill records.
func (p *fallbackOrdering) aggregationSelection(ctx context.Context, fields []string, selection []int) ([]int, error) {
	result := slices.Clone(selection)
	for i, field := range fields {
		if err := p.work.Checkpoint(ctx); err != nil {
			return nil, err
		}
		prefix := p.prefixValues[p.slots[field]]
		if prefix == nil {
			continue
		}
		result[i] = p.valueOffsets[p.slots[field]][prefix]
	}
	return result, nil
}

// Cache independent equality witnesses once per immutable entity. Binding an
// output tuple then checks only its range predicates, not the full arrays again.
func (p *fallbackOrdering) aggregationKey(ctx context.Context, entity *datastorepb.Entity) ([]byte, error) {
	if p.aggregationSource != entity {
		p.aggregationPrefixes = make([][]*datastorepb.Value, len(p.branches))
		bindings := slices.Clone(p.bound)
		clear(p.bound)
		for i, branch := range p.branches {
			matched, err := p.selectBranch(ctx, entity, branch)
			if err != nil {
				copy(p.bound, bindings)
				return nil, err
			}
			if !matched {
				continue
			}
			if err := p.captureBranchPrefixes(ctx, branch); err != nil {
				copy(p.bound, bindings)
				return nil, err
			}
			p.aggregationPrefixes[i] = slices.Clone(p.prefixValues)
		}
		copy(p.bound, bindings)
		p.aggregationSource = entity
	}
	for i, branch := range p.branches {
		if p.aggregationPrefixes[i] == nil {
			continue
		}
		matched := true
		for _, slot := range p.orders {
			candidate := p.bound[slot]
			if candidate == nil {
				candidate = p.domains[slot][0]
			} // The key is not projected.
			p.selected[slot] = candidate
			for _, f := range branch[slot] {
				if err := p.work.Checkpoint(ctx); err != nil {
					return nil, err
				}
				switch f.Op {
				case datastorepb.PropertyFilter_EQUAL, datastorepb.PropertyFilter_IN, datastorepb.PropertyFilter_HAS_ANCESTOR:
				default:
					matched = matched && p.matchesPropertyValue(candidate, f)
				}
			}
		}
		if matched {
			copy(p.prefixValues, p.aggregationPrefixes[i])
			return p.encodeKey(entity)
		}
	}
	return nil, nil
}

func (p *fallbackOrdering) captureBranchPrefixes(ctx context.Context, branch [][]*datastorepb.PropertyFilter) error {
	clear(p.prefixValues)
	for slot, filters := range branch {
		for _, prefix := range filters {
			if prefix.Op != datastorepb.PropertyFilter_EQUAL && prefix.Op != datastorepb.PropertyFilter_IN {
				continue
			}
			err := p.capturePrefix(ctx, slot, prefix, func(value *datastorepb.Value) bool {
				for _, f := range filters {
					if f.Op != datastorepb.PropertyFilter_EQUAL && f.Op != datastorepb.PropertyFilter_IN && f.Op != datastorepb.PropertyFilter_HAS_ANCESTOR && !p.matchesPropertyValue(value, f) {
						return false
					}
				}
				return true
			})
			if err != nil {
				return err
			}
			break // The first equality slot, not the least operand, is observable.
		}
	}
	return nil
}

func (p *fallbackOrdering) capturePrefix(ctx context.Context, slot int, filter *datastorepb.PropertyFilter, allowed func(*datastorepb.Value) bool) error {
	operands := []*datastorepb.Value{filter.Value}
	if filter.Op == datastorepb.PropertyFilter_IN {
		operands = filter.Value.GetArrayValue().Values
	}
	for _, operand := range operands {
		for _, value := range p.domains[slot] {
			if err := p.work.Checkpoint(ctx); err != nil {
				return err
			}
			if p.compareValues(value, operand) == 0 && allowed(value) {
				p.prefixValues[slot] = value
				return nil
			}
		}
	}
	return nil
}

func (p *fallbackOrdering) captureFactoredPrefixes(ctx context.Context) error {
	clear(p.prefixValues)
	stack := []*conditionNode{p.condition.root}
	for len(stack) > 0 {
		if err := p.work.Checkpoint(ctx); err != nil {
			return err
		}
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if node == nil {
			continue
		}
		if f := node.property; f != nil {
			if f.Op != datastorepb.PropertyFilter_EQUAL && f.Op != datastorepb.PropertyFilter_IN {
				continue
			}
			slot := p.slots[f.Property.Name]
			if p.prefixValues[slot] != nil {
				continue
			}
			if err := p.capturePrefix(ctx, slot, f, func(value *datastorepb.Value) bool {
				return p.compareValues(value, p.low[slot]) >= 0 && p.compareValues(value, p.high[slot]) <= 0 && (!p.active[slot] || p.matchesPropertyValue(value, p.exclusion[slot]))
			}); err != nil {
				return err
			}
		} else if node.op == datastorepb.CompositeFilter_AND {
			for i := len(node.children) - 1; i >= 0; i-- {
				stack = append(stack, node.children[i])
			}
		} else {
			for _, child := range node.children {
				if p.truth[child.id] {
					stack = append(stack, child)
					break
				}
			}
		}
	}
	return nil
}
