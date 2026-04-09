// Package query translates Firestore StructuredQuery protos into SQL.
package query

// TODO: implement Firestore → SQL query translation.
//
// Responsibilities:
//   - Parse StructuredQuery.where (CompositeFilter, FieldFilter, UnaryFilter)
//   - Translate filter operators to SQL comparisons against field_index
//   - Handle order_by (map to SQL ORDER BY with correct Firestore value ordering)
//   - Handle limit / offset / start_at / end_at cursors
//   - Handle collection_group queries (match any collection with the given ID)
