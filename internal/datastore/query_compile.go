package datastore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"google.golang.org/protobuf/proto"
)

type queryConditionContextKey struct{}

// This is an optimizer effort allowance, not a query acceptance limit.
// Exhaustion selects exact execution without new index discovery.
const defaultQueryOptimizerAllowance = 256

type preparedQueryCondition struct {
	condition          *compiledCondition
	namespace          string
	optimizerAllowance int
}

func queryOptimizerAllowance(ctx context.Context) int {
	if prepared, ok := ctx.Value(queryConditionContextKey{}).(preparedQueryCondition); ok {
		return prepared.optimizerAllowance
	}
	return defaultQueryOptimizerAllowance
}

// accessPlanningFits bounds the expanded syntax and potential interval endpoints
// before physical derivation. Intersections use a sweep, so this bounds retained
// spans linearly and repeated span sorting/combination polynomially, not by a
// Cartesian product. This is a strategy choice, never a validity check.
func (c *compiledCondition) accessPlanningFits(allowance int) bool {
	if allowance <= 0 {
		return false
	}
	return c.root == nil || c.root.accessSize <= min(allowance, defaultQueryOptimizerAllowance)
}

// prepareQueryExecution shares the immutable graph across internal aggregation
// pages. The private context is never reused by another public RPC.
func prepareQueryExecution(ctx context.Context, query *datastorepb.Query, namespace string) (context.Context, *datastorepb.Query, *compiledCondition, error) {
	if prepared, ok := ctx.Value(queryConditionContextKey{}).(preparedQueryCondition); ok && prepared.namespace == namespace && query.Filter == prepared.condition.filter() {
		return ctx, query, prepared.condition, nil
	}
	query, condition, err := prepareQueryConditionContext(ctx, query, namespace)
	if err != nil {
		return ctx, nil, nil, err
	}
	ctx = context.WithValue(ctx, queryConditionContextKey{}, preparedQueryCondition{condition: condition, namespace: namespace, optimizerAllowance: defaultQueryOptimizerAllowance})
	return ctx, query, condition, nil
}

// needsEntityWitnesses prevents scalar index intersections from dropping entities
// whose independent equality/IN witnesses occupy different array entries.
func (c *compiledCondition) needsEntityWitnesses() bool {
	if c.branches == nil {
		return true
	}
	counts := make(map[string]int)
	for _, node := range c.nodes {
		f := node.property
		if f != nil && (f.Op == datastorepb.PropertyFilter_EQUAL || f.Op == datastorepb.PropertyFilter_IN) {
			name := f.Property.GetName()
			counts[name]++
			if counts[name] > 1 {
				return true
			}
		}
	}
	return false
}

// needsCandidateFiltering preserves a conditional equality's independent sort
// slot. Globally mandatory equality orders have already been removed; narrowing
// this scan to equality values would lose Java's qualifying OR-branch order.
func (c *compiledCondition) needsCandidateFiltering(orders []*datastorepb.PropertyOrder) bool {
	for _, node := range c.nodes {
		if f := node.property; f != nil && f.Op == datastorepb.PropertyFilter_EQUAL {
			for _, order := range orders {
				if order.Property.GetName() == f.Property.GetName() {
					return true
				}
			}
		}
	}
	return false
}

// compiledCondition is immutable after construction. A nil branches slice means
// optimization was declined; root still describes the complete exact condition.
type compiledCondition struct {
	root     *conditionNode
	nodes    []*conditionNode
	branches [][]*datastorepb.PropertyFilter
}

type conditionNode struct {
	id         int
	op         datastorepb.CompositeFilter_Operator
	property   *datastorepb.PropertyFilter
	children   []*conditionNode
	syntax     *datastorepb.Filter
	digest     [sha256.Size]byte
	accessSize int
}

func (c *compiledCondition) filter() *datastorepb.Filter {
	if c.root == nil {
		return nil
	}
	return c.root.syntax
}

// compileQueryCondition interns operands without distributing AND over OR.
// Expansion is an optional, checked optimization, never an acceptance rule.
func compileQueryCondition(filter *datastorepb.Filter, allowance int) (*compiledCondition, error) {
	return compileConditionGraph(context.Background(), filter, allowance, false)
}

// Validation preserves Java's pre-DNF composite structure and converts membership
// operators for counting. Execution keeps the original membership roles instead.
func compileConditionGraph(ctx context.Context, filter *datastorepb.Filter, allowance int, validation bool) (*compiledCondition, error) {
	work := storage.QueryWorkFromContext(ctx)
	if err := work.Checkpoint(ctx); err != nil {
		return nil, err
	}
	c := &compiledCondition{}
	// Aggregation accumulators read the first matching index-schema slot;
	// Java's stable union merge makes submitted prefix/operand order observable.
	preserveOrder := aggregationEntries(ctx) && !validation
	if filter == nil {
		if allowance > 0 {
			c.branches = [][]*datastorepb.PropertyFilter{nil}
		}
		return c, nil
	}
	type frame struct {
		filter *datastorepb.Filter
		ready  bool
	}
	stack := []frame{{filter: filter}}
	resolved := make(map[*datastorepb.Filter]*conditionNode)
	interned := make(map[string]*conditionNode)
	converted := make(map[*datastorepb.Filter]*datastorepb.Filter)
	for len(stack) > 0 {
		if err := work.Checkpoint(ctx); err != nil {
			return nil, err
		}
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if resolved[current.filter] != nil {
			continue
		}
		working := current.filter
		if validation {
			if previous := converted[current.filter]; previous != nil {
				working = previous
			} else if pf := working.GetPropertyFilter(); pf != nil && (pf.Op == datastorepb.PropertyFilter_IN || pf.Op == datastorepb.PropertyFilter_NOT_IN) {
				op, comparison := datastorepb.CompositeFilter_OR, datastorepb.PropertyFilter_EQUAL
				if pf.Op == datastorepb.PropertyFilter_NOT_IN {
					op, comparison = datastorepb.CompositeFilter_AND, datastorepb.PropertyFilter_NOT_EQUAL
				}
				var children []*datastorepb.Filter
				for _, value := range pf.Value.GetArrayValue().Values {
					children = append(children, &datastorepb.Filter{FilterType: &datastorepb.Filter_PropertyFilter{PropertyFilter: &datastorepb.PropertyFilter{Property: pf.Property, Op: comparison, Value: value}}})
				}
				working = &datastorepb.Filter{FilterType: &datastorepb.Filter_CompositeFilter{CompositeFilter: &datastorepb.CompositeFilter{Op: op, Filters: children}}}
				converted[current.filter] = working
			}
		}
		cf := working.GetCompositeFilter()
		if cf != nil && !current.ready {
			stack = append(stack, frame{filter: current.filter, ready: true})
			for i := len(cf.Filters) - 1; i >= 0; i-- {
				stack = append(stack, frame{filter: cf.Filters[i]})
			}
			continue
		}
		node := &conditionNode{id: len(c.nodes)}
		var identity []byte
		var stable []byte
		if property := working.GetPropertyFilter(); property != nil {
			var err error
			if preserveOrder {
				node.property = proto.Clone(property).(*datastorepb.PropertyFilter)
			} else {
				node.property, err = canonicalMembership(property)
			}
			if err != nil {
				return nil, err
			}
			encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(node.property)
			if err != nil {
				return nil, fmt.Errorf("encoding query predicate: %w", err)
			}
			identity = append([]byte{0}, encoded...)
			stable = identity
			node.syntax = &datastorepb.Filter{FilterType: &datastorepb.Filter_PropertyFilter{PropertyFilter: node.property}}
		} else if cf != nil {
			node.op = cf.Op
			seen := make(map[int]bool)
			for _, child := range cf.Filters {
				prepared := resolved[child]
				children := []*conditionNode{prepared}
				if !validation && prepared.property == nil && prepared.op == cf.Op {
					children = prepared.children
				}
				for _, candidate := range children {
					if !seen[candidate.id] {
						seen[candidate.id] = true
						node.children = append(node.children, candidate)
					}
				}
			}
			if !validation && len(node.children) == 1 {
				resolved[current.filter] = node.children[0]
				continue
			}
			// IDs establish collision-free identity inside this compilation;
			// digests only establish deterministic presentation across requests.
			ids := make([]int, 0, len(node.children))
			for _, child := range node.children {
				ids = append(ids, child.id)
			}
			if !preserveOrder {
				slices.Sort(ids)
			}
			identity = []byte{1, byte(cf.Op)}
			for _, id := range ids {
				identity = binary.AppendUvarint(identity, uint64(id))
			}
			if !preserveOrder {
				slices.SortFunc(node.children, func(a, b *conditionNode) int { return bytes.Compare(a.digest[:], b.digest[:]) })
			}
			stable = []byte{1, byte(cf.Op)}
			children := make([]*datastorepb.Filter, 0, len(node.children))
			for _, child := range node.children {
				stable = append(stable, child.digest[:]...)
				children = append(children, child.syntax)
			}
			node.syntax = &datastorepb.Filter{FilterType: &datastorepb.Filter_CompositeFilter{CompositeFilter: &datastorepb.CompositeFilter{Op: cf.Op, Filters: children}}}
		} else {
			return nil, fmt.Errorf("query condition has no predicate or composite")
		}
		if previous := interned[string(identity)]; previous != nil {
			resolved[current.filter] = previous
			continue
		}
		node.digest = sha256.Sum256(stable)
		// Saturate at one beyond the internal allowance. Count every reference,
		// not just unique DAG nodes: syntax-based physical helpers revisit them.
		node.accessSize = 1
		if pf := node.property; pf != nil {
			switch pf.Op {
			case datastorepb.PropertyFilter_IN, datastorepb.PropertyFilter_NOT_IN:
				node.accessSize += min(len(pf.Value.GetArrayValue().GetValues()), defaultQueryOptimizerAllowance)
			case datastorepb.PropertyFilter_NOT_EQUAL:
				node.accessSize++
			}
		}
		for _, child := range node.children {
			node.accessSize = min(defaultQueryOptimizerAllowance+1, node.accessSize+child.accessSize)
		}
		interned[string(identity)] = node
		resolved[current.filter] = node
		c.nodes = append(c.nodes, node)
	}
	c.root = resolved[filter]
	c.branches = expandCondition(c.root, allowance)
	return c, nil
}

// canonicalMembership retains typed values and raw operator identity. Validation
// checks the submitted list length before this exact-value set normalization.
func canonicalMembership(property *datastorepb.PropertyFilter) (*datastorepb.PropertyFilter, error) {
	if property.Op != datastorepb.PropertyFilter_IN && property.Op != datastorepb.PropertyFilter_NOT_IN {
		return property, nil
	}
	values := make(map[string]*datastorepb.Value)
	var keys []string
	for _, value := range property.Value.GetArrayValue().GetValues() {
		encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encoding membership value: %w", err)
		}
		key := string(encoded)
		if _, exists := values[key]; !exists {
			keys = append(keys, key)
			values[key] = value
		}
	}
	slices.Sort(keys)
	copyProperty := proto.Clone(property).(*datastorepb.PropertyFilter)
	copyProperty.Value.GetArrayValue().Values = nil
	for _, key := range keys {
		copyProperty.Value.GetArrayValue().Values = append(copyProperty.Value.GetArrayValue().Values, values[key])
	}
	return copyProperty, nil
}

func expandCondition(root *conditionNode, allowance int) [][]*datastorepb.PropertyFilter {
	if allowance <= 0 {
		return nil
	}
	cache := make(map[*conditionNode][][]*datastorepb.PropertyFilter)
	var expand func(*conditionNode) [][]*datastorepb.PropertyFilter
	expand = func(node *conditionNode) [][]*datastorepb.PropertyFilter {
		if previous, ok := cache[node]; ok {
			return previous
		}
		if node.property != nil {
			result := [][]*datastorepb.PropertyFilter{{node.property}}
			cache[node] = result
			return result
		}
		var result [][]*datastorepb.PropertyFilter
		if node.op == datastorepb.CompositeFilter_AND {
			result = [][]*datastorepb.PropertyFilter{nil}
		}
		for _, child := range node.children {
			other := expand(child)
			if other == nil {
				cache[node] = nil
				return nil
			}
			if node.op == datastorepb.CompositeFilter_OR {
				if len(other) > allowance-len(result) {
					cache[node] = nil
					return nil
				}
				result = append(result, other...)
				continue
			}
			if len(result) > allowance/len(other) {
				cache[node] = nil
				return nil
			}
			next := make([][]*datastorepb.PropertyFilter, 0, len(result)*len(other))
			for _, left := range result {
				for _, right := range other {
					terms := append([]*datastorepb.PropertyFilter(nil), left...)
					for _, term := range right {
						if !slices.Contains(terms, term) {
							terms = append(terms, term)
						}
					}
					next = append(next, terms)
				}
			}
			result = next
		}
		cache[node] = result
		return result
	}
	return expand(root)
}
