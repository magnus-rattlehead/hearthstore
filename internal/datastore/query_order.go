package datastore

import (
	"bytes"
	"context"
	"encoding/hex"
	"slices"
	"strings"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// queryWithEffectiveOrder removes only globally required equality sort fields.
// An equality inside one OR branch (and IN itself) does not fix global order.
func queryWithEffectiveOrder(query *datastorepb.Query) *datastorepb.Query {
	if len(query.Order) == 0 || query.Filter == nil {
		return query
	}
	fixed := fixedQueryProperties(query.Filter)
	var result *datastorepb.Query
	for i, order := range query.Order {
		if fixed[order.Property.GetName()] {
			if result == nil {
				result = proto.Clone(query).(*datastorepb.Query)
				result.Order = result.Order[:i]
			}
		} else if result != nil {
			result.Order = append(result.Order, order)
		}
	}
	if result != nil {
		return result
	}
	return query
}

func fixedQueryProperties(filter *datastorepb.Filter) map[string]bool {
	filters := mandatoryFilters(filter)
	fixed := make(map[string]bool)
	for _, filter := range filters {
		name := filter.Property.GetName()
		if filter.Op == datastorepb.PropertyFilter_EQUAL {
			fixed[name] = true
		}
		if filter.Op == datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL {
			for _, upper := range filters {
				if upper.Property.GetName() == name && upper.Op == datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL && compareValues(filter.Value, upper.Value) == 0 {
					fixed[name] = true
				}
			}
		}
	}
	return fixed
}

// normalizedQueryOrder follows Java's Datastore normalizer, not its
// Firestore-only restrictions. Full-query equality sorts remain omitted.
// The existing untagged suffix represents an implicit ascending final key;
// aggregate entry schemas retain that dimension explicitly as well.
func normalizedQueryOrder(q *datastorepb.Query, condition *compiledCondition, fields []string, aggregate bool) *datastorepb.Query {
	result := proto.Clone(q).(*datastorepb.Query)
	result.Filter = q.Filter
	seen := make(map[string]bool)
	direction := datastorepb.PropertyOrder_ASCENDING
	for _, order := range result.Order {
		if order.Direction == datastorepb.PropertyOrder_DIRECTION_UNSPECIFIED {
			order.Direction = datastorepb.PropertyOrder_ASCENDING
		}
		seen[order.Property.Name], direction = true, order.Direction
	}
	add := func(name string) {
		if !seen[name] {
			result.Order = append(result.Order, &datastorepb.PropertyOrder{Property: &datastorepb.PropertyReference{Name: name}, Direction: direction})
			seen[name] = true
		}
	}
	for _, field := range q.DistinctOn {
		add(field.Name)
	}
	fixed := map[string]bool{}
	if !aggregate {
		fixed = fixedQueryProperties(q.Filter)
	}
	var ranges []string
	for _, node := range condition.nodes {
		f := node.property
		if f == nil || fixed[f.Property.Name] {
			continue
		}
		switch f.Op {
		case datastorepb.PropertyFilter_LESS_THAN, datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL,
			datastorepb.PropertyFilter_GREATER_THAN, datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL,
			datastorepb.PropertyFilter_NOT_EQUAL, datastorepb.PropertyFilter_NOT_IN:
			ranges = append(ranges, f.Property.Name)
		case datastorepb.PropertyFilter_EQUAL:
			if f.Property.Name == "__key__" {
				ranges = append(ranges, "__key__")
			}
		}
	}
	for _, name := range sortedQueryPaths(ranges) {
		if name != "__key__" {
			add(name)
		}
	}
	if slices.Contains(ranges, "__key__") {
		add("__key__")
	}
	for _, name := range sortedQueryPaths(fields) {
		if name != "__key__" {
			add(name)
		}
	}
	if aggregate || direction == datastorepb.PropertyOrder_DESCENDING {
		add("__key__")
	}
	return result
}

// Ambiguous query paths split isolated dots, unlike mutation-mask escapes.
// Compare segments, so a.b precedes a! even though '.' follows '!' in UTF-8.
func sortedQueryPaths(fields []string) []string {
	fields = slices.Clone(fields)
	segments := make(map[string][]string, len(fields))
	for _, field := range fields {
		if _, ok := segments[field]; ok {
			continue
		}
		start := 0
		for i := range field {
			if field[i] == '.' && (i == 0 || field[i-1] != '.') && (i+1 == len(field) || field[i+1] != '.') {
				segments[field] = append(segments[field], field[start:i])
				start = i + 1
			}
		}
		segments[field] = append(segments[field], field[start:])
	}
	slices.SortFunc(fields, func(a, b string) int { return slices.CompareFunc(segments[a], segments[b], strings.Compare) })
	return slices.Compact(fields)
}

// Do not relocate a key before other ordered fields. A descending final key
// can use an ordinary typed key component before the storage identity suffix.
func compositeOrderSupported(q *datastorepb.Query) bool {
	for i, order := range q.Order {
		if order.Property.GetName() == "__key__" && i != len(q.Order)-1 {
			return false
		}
	}
	return true
}

func hiddenProjectionOrder(q *datastorepb.Query) bool {
	fields := projectionFields(q)
	if len(fields) == 0 {
		return false
	}
	for i, order := range q.Order {
		if order.Property.GetName() == "__key__" && i == len(q.Order)-1 {
			continue
		}
		if !slices.Contains(fields, order.Property.GetName()) {
			// SHORTCUT: hidden sort fields require bounded fallback until
			// covering rows retain their logical boundary before projection.
			// An earlier key also cannot move behind the projected fields.
			return true
		}
	}
	return false
}

// mandatoryFilters intersects OR children instead of expanding disjunctions.
func mandatoryFilters(filter *datastorepb.Filter) []*datastorepb.PropertyFilter {
	if filter == nil {
		return nil
	}
	if property := filter.GetPropertyFilter(); property != nil {
		return []*datastorepb.PropertyFilter{property}
	}
	cf := filter.GetCompositeFilter()
	var result []*datastorepb.PropertyFilter
	for i, child := range cf.GetFilters() {
		terms := mandatoryFilters(child)
		if i == 0 || cf.Op == datastorepb.CompositeFilter_AND {
			result = append(result, terms...)
			continue
		}
		kept := result[:0]
		for _, term := range result {
			for _, candidate := range terms {
				if term.Op == candidate.Op && term.Property.GetName() == candidate.Property.GetName() && compareValues(term.Value, candidate.Value) == 0 {
					kept = append(kept, term)
					break
				}
			}
		}
		result = kept
	}
	return result
}

// fallbackOrdering keeps immutable compiled conditions and request-local scratch.
// Small alternatives are grouped once; larger conditions use witness envelopes.
type fallbackOrdering struct {
	aggregation         bool
	prefixValues        []*datastorepb.Value
	aggregationSource   *datastorepb.Entity
	domainSource        *datastorepb.Entity
	aggregationPrefixes [][]*datastorepb.Value
	valueOffsets        []map[*datastorepb.Value]int
	work                *storage.QueryWork
	condition           *compiledCondition
	err                 error
	properties          []string
	slots               map[string]int
	orders              []int
	descending          []bool
	ordered             []bool
	inCount             []int
	hasMembership       []bool
	hasRange            []bool
	exclusion           []*datastorepb.PropertyFilter
	branches            [][][]*datastorepb.PropertyFilter
	domains             [][]*datastorepb.Value
	selected            []*datastorepb.Value
	bound               []*datastorepb.Value
	witnesses           []bool
	truth               []bool
	low, high           []*datastorepb.Value
	active              []bool
}

func newFallbackOrdering(query *datastorepb.Query, projected []string) *fallbackOrdering {
	condition, err := compileQueryCondition(query.Filter, 30)
	if err != nil {
		return &fallbackOrdering{err: err}
	}
	return newConditionOrdering(query, projected, condition)
}

func newConditionOrdering(query *datastorepb.Query, projected []string, condition *compiledCondition) *fallbackOrdering {
	p := &fallbackOrdering{condition: condition, slots: make(map[string]int)}
	add := func(name string) int {
		if slot, ok := p.slots[name]; ok {
			return slot
		}
		slot := len(p.properties)
		p.slots[name] = slot
		p.properties = append(p.properties, name)
		p.descending = append(p.descending, false)
		p.ordered = append(p.ordered, false)
		p.inCount = append(p.inCount, 0)
		p.hasMembership = append(p.hasMembership, false)
		p.hasRange = append(p.hasRange, false)
		p.exclusion = append(p.exclusion, nil)
		return slot
	}
	for _, order := range query.Order {
		slot := add(order.Property.GetName())
		p.orders = append(p.orders, slot)
		p.ordered[slot] = true
		p.descending[slot] = order.Direction == datastorepb.PropertyOrder_DESCENDING
	}
	for _, name := range projected {
		slot := add(name)
		if !p.ordered[slot] {
			p.orders = append(p.orders, slot)
			p.ordered[slot] = true
		}
	}
	for _, node := range condition.nodes {
		f := node.property
		if f == nil {
			continue
		}
		slot := add(f.Property.GetName())
		switch f.Op {
		case datastorepb.PropertyFilter_IN:
			p.inCount[slot]++
			p.hasMembership[slot] = true
		case datastorepb.PropertyFilter_EQUAL:
			p.hasMembership[slot] = true
		case datastorepb.PropertyFilter_NOT_EQUAL, datastorepb.PropertyFilter_NOT_IN:
			// Validation permits only one distinct exclusion predicate.
			p.exclusion[slot] = f
		case datastorepb.PropertyFilter_LESS_THAN, datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL,
			datastorepb.PropertyFilter_GREATER_THAN, datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
			p.hasRange[slot] = true
		}
	}
	for _, branch := range condition.branches {
		grouped := make([][]*datastorepb.PropertyFilter, len(p.properties))
		for _, f := range branch {
			slot := p.slots[f.Property.GetName()]
			grouped[slot] = append(grouped[slot], f)
		}
		p.branches = append(p.branches, grouped)
	}
	p.domains = make([][]*datastorepb.Value, len(p.properties))
	p.selected = make([]*datastorepb.Value, len(p.properties))
	p.bound = make([]*datastorepb.Value, len(p.properties))
	p.prefixValues = make([]*datastorepb.Value, len(p.properties))
	p.low = make([]*datastorepb.Value, len(p.properties))
	p.high = make([]*datastorepb.Value, len(p.properties))
	p.active = make([]bool, len(p.properties))
	p.truth = make([]bool, len(condition.nodes))
	return p
}

func (p *fallbackOrdering) key(ctx context.Context, entity *datastorepb.Entity) ([]byte, error) {
	return p.keySelection(ctx, entity, "", nil)
}

func (p *fallbackOrdering) keySelection(ctx context.Context, entity *datastorepb.Entity, property string, selected *datastorepb.Value) ([]byte, error) {
	return p.keySelections(ctx, entity, property, selected, nil)
}

func (p *fallbackOrdering) keySelections(ctx context.Context, entity *datastorepb.Entity, property string, selected *datastorepb.Value, bindings map[string]*datastorepb.Value) ([]byte, error) {
	p.work = storage.QueryWorkFromContext(ctx)
	if p.err != nil {
		return nil, p.err
	}
	if p.aggregation && p.valueOffsets == nil {
		p.valueOffsets = make([]map[*datastorepb.Value]int, len(p.properties))
	}
	for slot, name := range p.properties {
		p.bound[slot] = bindings[name]
		if name == property {
			p.bound[slot] = selected
		}
		if p.aggregation && p.domainSource == entity {
			continue
		}
		if err := p.work.Checkpoint(ctx); err != nil {
			return nil, err
		}
		p.domains[slot] = p.domains[slot][:0]
		value := getProp(entity, name)
		values := []*datastorepb.Value{value}
		if array := value.GetArrayValue(); array != nil {
			values = array.Values
		}
		if p.aggregation {
			p.valueOffsets[slot] = make(map[*datastorepb.Value]int, len(values))
		}
		for offset, candidate := range values {
			if err := p.work.Checkpoint(ctx); err != nil {
				return nil, err
			}
			if candidate != nil && !candidate.ExcludeFromIndexes && candidate.GetEntityValue() == nil {
				p.domains[slot] = append(p.domains[slot], candidate)
				if p.aggregation {
					if _, exists := p.valueOffsets[slot][candidate]; !exists {
						p.valueOffsets[slot][candidate] = offset
					}
				}
			}
		}
		if p.ordered[slot] && len(p.domains[slot]) == 0 {
			return nil, nil
		}
	}
	if p.aggregation {
		p.domainSource = entity
		if p.branches != nil {
			return p.aggregationKey(ctx, entity)
		}
	}
	if p.branches == nil {
		return p.factoredKey(ctx, entity)
	}
	var best []byte
	for _, branch := range p.branches {
		matched, err := p.selectBranch(ctx, entity, branch)
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		key, err := p.encodeKey(entity)
		if err != nil {
			return nil, err
		}
		if best == nil || bytes.Compare(key, best) < 0 {
			best = key
		}
	}
	return best, nil
}

// selectBranch keeps equality/IN witnesses independent, but requires every
// witness to satisfy the branch's same-property ranges (Java index-slot logic).
func (p *fallbackOrdering) selectBranch(ctx context.Context, entity *datastorepb.Entity, branch [][]*datastorepb.PropertyFilter) (bool, error) {
	for slot, filters := range branch {
		p.selected[slot] = nil
		if len(filters) == 0 && !p.ordered[slot] {
			continue
		}
		p.witnesses = p.witnesses[:0]
		remaining := 0
		for _, f := range filters {
			membership := f.Op == datastorepb.PropertyFilter_EQUAL || f.Op == datastorepb.PropertyFilter_IN
			p.witnesses = append(p.witnesses, !membership)
			if membership {
				remaining++
			}
			if p.properties[slot] == "__key__" && !matchesKeyFilter(entity, f) {
				return false, nil
			}
		}
		for _, candidate := range p.domains[slot] {
			if err := p.work.Checkpoint(ctx); err != nil {
				return false, err
			}
			qualified := true
			for _, f := range filters {
				switch f.Op {
				case datastorepb.PropertyFilter_EQUAL, datastorepb.PropertyFilter_IN, datastorepb.PropertyFilter_HAS_ANCESTOR:
				default:
					if !p.matchesPropertyValue(candidate, f) {
						qualified = false
					}
				}
				if !qualified {
					break
				}
			}
			if !qualified {
				continue
			}
			orderMatch := true
			for i, f := range filters {
				if f.Op != datastorepb.PropertyFilter_EQUAL && f.Op != datastorepb.PropertyFilter_IN {
					continue
				}
				matches := p.matchesPropertyValue(candidate, f)
				if matches && !p.witnesses[i] {
					p.witnesses[i] = true
					remaining--
				}
				if !p.aggregation && p.ordered[slot] && f.Op == datastorepb.PropertyFilter_IN && p.inCount[slot] == 1 && !matches {
					orderMatch = false
				}
			}
			if !orderMatch || p.bound[slot] != nil && p.compareValues(candidate, p.bound[slot]) != 0 {
				continue
			}
			previous := p.selected[slot]
			if previous == nil {
				p.selected[slot] = candidate
			} else {
				cmp := p.compareValues(candidate, previous)
				if !p.descending[slot] && cmp < 0 || p.descending[slot] && cmp > 0 {
					p.selected[slot] = candidate
				}
			}
			if !p.ordered[slot] && remaining == 0 {
				break
			}
		}
		if p.selected[slot] == nil || remaining != 0 {
			return false, nil
		}
	}
	return true, nil
}

func (p *fallbackOrdering) compareValues(a, b *datastorepb.Value) int {
	p.work.Charge(storage.WorkComparisons, 1)
	return compareValues(a, b)
}

func (p *fallbackOrdering) matchesPropertyValue(value *datastorepb.Value, filter *datastorepb.PropertyFilter) bool {
	return matchesPropertyValueUsing(value, filter, p.compareValues)
}

func (p *fallbackOrdering) encodeKey(entity *datastorepb.Entity) ([]byte, error) {
	var key []byte
	for _, slot := range p.orders {
		encoded, ok := storage.OrderedQueryValue(p.selected[slot], p.descending[slot])
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "unsupported query ordering value")
		}
		key = append(key, hex.EncodeToString(encoded)...)
		key = append(key, '/')
	}
	return append(key, hex.EncodeToString(keycodec.Ordered(entity.Key))...), nil
}
