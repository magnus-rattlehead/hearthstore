package datastore

import (
	"context"
	"slices"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// factoredKey streams candidate order tuples and witness envelopes. Retained
// state is linear in the condition and entity values, not their Cartesian
// product. Domain search can still be expensive; every loop honors cancellation.
func (p *fallbackOrdering) factoredKey(ctx context.Context, entity *datastorepb.Entity) ([]byte, error) {
	var comparisonErr error
	compare := func(a, b *datastorepb.Value) int {
		if comparisonErr == nil {
			comparisonErr = p.work.Checkpoint(ctx)
		}
		if comparisonErr != nil {
			return 0
		}
		return p.compareValues(a, b)
	}
	for slot := range p.domains {
		slices.SortFunc(p.domains[slot], compare)
		if comparisonErr != nil {
			return nil, comparisonErr
		}
		p.domains[slot] = slices.CompactFunc(p.domains[slot], func(a, b *datastorepb.Value) bool { return compare(a, b) == 0 })
		if comparisonErr != nil {
			return nil, comparisonErr
		}
		p.selected[slot] = nil
	}
	var tuples func(int) (bool, error)
	tuples = func(order int) (bool, error) {
		if err := p.work.Checkpoint(ctx); err != nil {
			return false, err
		}
		if order == len(p.orders) {
			return p.bindEnvelopes(ctx, entity, 0)
		}
		slot := p.orders[order]
		values := p.domains[slot]
		for i := range values {
			position := i
			if p.descending[slot] {
				position = len(values) - 1 - i
			}
			if p.bound[slot] != nil && p.compareValues(values[position], p.bound[slot]) != 0 {
				continue
			}
			p.selected[slot] = values[position]
			matched, err := tuples(order + 1)
			if matched || err != nil {
				return matched, err
			}
		}
		return false, nil
	}
	matched, err := tuples(0)
	if err != nil || !matched {
		return nil, err
	}
	if p.aggregation {
		if err := p.captureFactoredPrefixes(ctx); err != nil {
			return nil, err
		}
	}
	return p.encodeKey(entity)
}

// Binding one interval per property preserves branch correlation without DNF.
// Equality/IN witnesses lie inside it; every active convex range must accept
// both endpoints. The single allowed exclusion is handled by a separate mode
// so an inactive OR branch never removes values from another branch.
func (p *fallbackOrdering) bindEnvelopes(ctx context.Context, entity *datastorepb.Entity, slot int) (bool, error) {
	if err := p.work.Checkpoint(ctx); err != nil {
		return false, err
	}
	if slot == len(p.properties) {
		return p.evaluateEnvelopes(ctx, entity)
	}
	values := p.domains[slot]
	modes := 1
	if p.exclusion[slot] != nil {
		modes = 2
	}
	for mode := 0; mode < modes; mode++ {
		p.active[slot] = mode == 1
		allowed := func(value *datastorepb.Value) bool {
			return !p.active[slot] || p.matchesPropertyValue(value, p.exclusion[slot])
		}
		candidate := p.selected[slot]
		if p.ordered[slot] && !allowed(candidate) {
			continue
		}
		if p.hasRange[slot] && !p.hasMembership[slot] && p.ordered[slot] {
			// The tuple iterator already established that candidate belongs to
			// the domain. Binding its singleton needs no domain rescan.
			p.low[slot], p.high[slot] = candidate, candidate
			matched, err := p.bindEnvelopes(ctx, entity, slot+1)
			if matched || err != nil {
				return matched, err
			}
			continue
		}
		first, last := -1, -1
		for i, value := range values {
			if err := p.work.Checkpoint(ctx); err != nil {
				return false, err
			}
			if allowed(value) {
				if first < 0 {
					first = i
				}
				last = i
			}
		}
		if first < 0 {
			// Missing/empty properties make their leaves false; a different OR
			// branch may still match. Ordered properties were checked earlier.
			if p.ordered[slot] {
				continue
			}
			p.low[slot], p.high[slot] = nil, nil
			matched, err := p.bindEnvelopes(ctx, entity, slot+1)
			if matched || err != nil {
				return matched, err
			}
			continue
		}
		if !p.hasRange[slot] {
			// Without convex ranges the widest interval dominates all narrower
			// ones: independent membership witnesses cannot invalidate it.
			p.low[slot], p.high[slot] = values[first], values[last]
			matched, err := p.bindEnvelopes(ctx, entity, slot+1)
			if matched || err != nil {
				return matched, err
			}
			continue
		}
		for lo := first; lo <= last; lo++ {
			if err := p.work.Checkpoint(ctx); err != nil {
				return false, err
			}
			if !allowed(values[lo]) {
				continue
			}
			if p.ordered[slot] && p.compareValues(values[lo], candidate) > 0 {
				break
			}
			end := last
			// Range-only membership needs one witness, not a pair of endpoints.
			if !p.hasMembership[slot] {
				end = lo
			}
			for hi := lo; hi <= end; hi++ {
				if err := p.work.Checkpoint(ctx); err != nil {
					return false, err
				}
				if !allowed(values[hi]) || p.ordered[slot] && p.compareValues(values[hi], candidate) < 0 {
					continue
				}
				p.low[slot], p.high[slot] = values[lo], values[hi]
				matched, err := p.bindEnvelopes(ctx, entity, slot+1)
				if matched || err != nil {
					return matched, err
				}
			}
		}
	}
	return false, nil
}

func (p *fallbackOrdering) evaluateEnvelopes(ctx context.Context, entity *datastorepb.Entity) (bool, error) {
	for _, node := range p.condition.nodes {
		if err := p.work.Checkpoint(ctx); err != nil {
			return false, err
		}
		if f := node.property; f != nil {
			slot := p.slots[f.Property.GetName()]
			low, high := p.low[slot], p.high[slot]
			matched := false
			if f.Property.GetName() == "__key__" {
				matched = matchesKeyFilter(entity, f)
			} else if low != nil {
				switch f.Op {
				case datastorepb.PropertyFilter_EQUAL, datastorepb.PropertyFilter_IN:
					for _, value := range p.domains[slot] {
						if err := p.work.Checkpoint(ctx); err != nil {
							return false, err
						}
						if p.compareValues(value, low) < 0 || p.compareValues(value, high) > 0 {
							continue
						}
						if p.active[slot] && !p.matchesPropertyValue(value, p.exclusion[slot]) {
							continue
						}
						if p.matchesPropertyValue(value, f) {
							matched = true
							break
						}
					}
					if !p.aggregation && matched && f.Op == datastorepb.PropertyFilter_IN && p.ordered[slot] && p.inCount[slot] == 1 {
						matched = p.matchesPropertyValue(p.selected[slot], f)
					}
				case datastorepb.PropertyFilter_NOT_EQUAL, datastorepb.PropertyFilter_NOT_IN:
					matched = p.active[slot]
				default:
					matched = p.matchesPropertyValue(low, f) && p.matchesPropertyValue(high, f)
				}
			}
			p.truth[node.id] = matched
			continue
		}
		matched := node.op == datastorepb.CompositeFilter_AND
		for _, child := range node.children {
			if node.op == datastorepb.CompositeFilter_AND {
				matched = matched && p.truth[child.id]
			} else {
				matched = matched || p.truth[child.id]
			}
		}
		p.truth[node.id] = matched
	}
	return p.condition.root == nil || p.truth[p.condition.root.id], nil
}
