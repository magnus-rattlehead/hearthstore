package datastore

import (
	"context"
	"strings"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// queryMatcher adapts exact evaluation to storage's boolean predicates. Errors
// are sticky and must be checked by the caller before returning any results.
type queryMatcher struct {
	ctx      context.Context
	ordering *fallbackOrdering
	dotted   bool
	err      error
}

func newQueryMatcher(ctx context.Context, condition *compiledCondition, selectedProperty string) *queryMatcher {
	query := &datastorepb.Query{}
	if selectedProperty != "" {
		query.Order = []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: selectedProperty}}}
	}
	m := &queryMatcher{ctx: ctx, ordering: newConditionOrdering(query, nil, condition)}
	for _, name := range m.ordering.properties {
		m.dotted = m.dotted || strings.Contains(name, ".")
	}
	return m
}

func (m *queryMatcher) accept(entity *datastorepb.Entity) bool {
	if m.err != nil {
		return false
	}
	for mode := 0; mode < 2; mode++ {
		if mode == 1 && !m.dotted {
			break
		}
		view := entity
		if m.dotted {
			view = queryInterpretation(entity, m.ordering.properties, mode == 1)
		}
		key, err := m.ordering.key(m.ctx, view)
		if err != nil {
			m.err = err
			return false
		}
		if key != nil {
			return true
		}
	}
	return false
}

func (m *queryMatcher) acceptSelection(entity *datastorepb.Entity, canonical bool, property string, value *datastorepb.Value) bool {
	if m.err != nil {
		return false
	}
	if m.dotted {
		entity = queryInterpretation(entity, m.ordering.properties, canonical)
	}
	key, err := m.ordering.keySelection(m.ctx, entity, property, value)
	m.err = err
	return key != nil && err == nil
}
