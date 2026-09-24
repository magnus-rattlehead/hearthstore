package storage

import (
	"context"
	"errors"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// BuiltinProbe bounds optimizer work over a built-in covering range at ReadTime.
type BuiltinProbe struct {
	Project, Database, Namespace string
	Kind, Property               string
	ReadTime                     time.Time
	Filter                       *datastorepb.Filter
	Reverse                      bool
	Cursor                       *CursorPayload
	Allowance                    int
}

// CompositeProbe bounds optimizer work over a composite covering range at ReadTime.
type CompositeProbe struct {
	Project, Database, Namespace string
	IndexID                      string
	ReadTime                     time.Time
	Filter                       *datastorepb.Filter
	Cursor                       *CursorPayload
	Allowance                    int
}

// ProbeResult retains rows only when the entire range fits the probe bounds.
// Visited includes rejected entries and work spent on incomplete probes.
type ProbeResult struct {
	Rows     []*DsEntityRow
	Visited  int64
	Complete bool
}

var errIndexProbeFull = errors.New("index planning probe exhausted")

// indexProbe bounds planning work, including entries rejected by scan bounds.
// Exhaustion is private to the probe: callers discard it and execute normally.
type indexProbe struct {
	remaining int
	visited   int64
}

func (p *indexProbe) step() error {
	if p == nil {
		return nil
	}
	if p.remaining <= 0 {
		return errIndexProbeFull
	}
	p.remaining--
	p.visited++
	return nil
}

func (p *indexProbe) result(ctx context.Context, page QueryPage, err error) (ProbeResult, error) {
	result := ProbeResult{Visited: p.visited}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	if errors.Is(err, errIndexProbeFull) || err == nil && page.More {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	result.Rows, result.Complete = page.Rows, true
	return result, nil
}

// DsProbeBuiltinAsOf returns a complete scalar covering range only if its scan
// fits the optimizer allowance and normal materialized-page byte bound.
func (s *Store) DsProbeBuiltinAsOf(ctx context.Context, q BuiltinProbe) (ProbeResult, error) {
	p := &indexProbe{remaining: q.Allowance}
	page, err := s.dsQueryBuiltin(ctx, BuiltinQuery{
		Project: q.Project, Database: q.Database, Namespace: q.Namespace,
		Kind: q.Kind, Property: q.Property, ReadTime: &q.ReadTime,
		Filter: q.Filter, Reverse: q.Reverse, Cursor: q.Cursor, Projection: true,
	}, p)
	return p.result(ctx, page, err)
}

// DsProbeCompositeAsOf is the composite equivalent of DsProbeBuiltinAsOf.
func (s *Store) DsProbeCompositeAsOf(ctx context.Context, q CompositeProbe) (ProbeResult, error) {
	p := &indexProbe{remaining: q.Allowance}
	// The caller evaluates the residual predicate on covering tuples. Enable
	// secondary range bounds just as the existing filtered scanner does.
	page, err := s.dsQueryComposite(ctx, CompositeQuery{
		Project: q.Project, Database: q.Database, Namespace: q.Namespace,
		IndexID: q.IndexID, ReadTime: &q.ReadTime, Filter: q.Filter, Cursor: q.Cursor,
		Accept: func(*datastorepb.Entity) bool { return true }, Projection: true,
	}, p)
	return p.result(ctx, page, err)
}
