package storage

import (
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// QueryPage contains candidate rows and scan accounting before API response shaping.
type QueryPage struct {
	Rows    []*DsEntityRow
	Scanned int64
	// More means a scan limit was reached; a following page may still be empty.
	More bool
}

// BuiltinQuery selects a page from an automatically maintained property index.
type BuiltinQuery struct {
	Project, Database, Namespace string
	Kind, Property, Ancestor     string
	// ReadTime selects a snapshot; nil reads the current store.
	ReadTime *time.Time
	Filter   *datastorepb.Filter
	Cursor   *CursorPayload
	Reverse  bool
	// Limit bounds candidate rows. Zero uses only the byte bound; a negative
	// value bounds rows by its magnitude and bypasses the byte bound.
	Limit int
	// Projection reads scalar covering tuples instead of full entities.
	Projection bool
	// Accept filters rows before the page limit. AcceptCandidate selects an
	// array/dotted property's representative entry when reading full entities.
	Accept          func(*datastorepb.Entity) bool
	AcceptCandidate func(entity *datastorepb.Entity, canonical bool, candidate *datastorepb.Value) bool
}

// CompositeQuery selects a page from a persisted composite index.
type CompositeQuery struct {
	Project, Database, Namespace string
	IndexID, Ancestor            string
	// ReadTime selects a snapshot; nil reads the current store.
	ReadTime *time.Time
	// Prefix applies when Filter is nil; otherwise bounds come from Filter.
	Prefix []byte
	Filter *datastorepb.Filter
	Cursor *CursorPayload
	// Limit bounds candidate rows; nonpositive values use only the byte bound.
	Limit int
	// Projection reads scalar covering tuples instead of full entities.
	Projection bool
	// Accept filters rows before the page limit.
	Accept func(*datastorepb.Entity) bool
}
