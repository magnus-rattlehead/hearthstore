// Package query translates Firestore StructuredQuery protos into parameterized SQL.
package query

import (
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// Result holds the generated SQL and its bound parameters.
type Result struct {
	SQL string
	// Args are the positional parameters aligned with ? placeholders in SQL.
	Args []any
	// NeedsGoFilter is true when the SQL cannot fully express all predicates
	// (e.g. IS_NAN, GeoPoint). The caller must apply the original filter in Go.
	NeedsGoFilter bool
}

// Build translates a StructuredQuery (single From selector) into a parameterized
// SELECT against the documents + field_index tables.
//
// allDescendants controls whether the query is a collection-group query
// (true -> omit parent_path constraint, match all subcollections with the same ID).
//
// The generated SQL selects d.data as its first (and only) column.
func Build(project, database, parentPath string, allDescendants bool, q *firestorepb.StructuredQuery) (*Result, error) {
	if q == nil {
		return nil, fmt.Errorf("nil StructuredQuery")
	}

	b := &builder{
		project:        project,
		database:       database,
		parentPath:     parentPath,
		allDescendants: allDescendants,
	}

	// Resolve collection ID from the From clause.
	if len(q.From) == 0 {
		return nil, fmt.Errorf("StructuredQuery.from is empty")
	}
	collectionID := q.From[0].CollectionId
	if collectionID == "" && !allDescendants {
		return nil, fmt.Errorf("StructuredQuery.from[0].collection_id is empty")
	}

	// Compute the effective ORDER BY: explicit fields, then implicit inequality
	// fields (sorted lexicographically), then __name__ as tiebreaker.
	effectiveOrders := addImplicitOrderBy(q.OrderBy, q.Where)

	// Build ORDER BY JOINs first (their aliases are needed for cursor clauses).
	b.buildOrderBy(effectiveOrders)

	// orderBy('field') excludes docs without that field. Use EXISTS so non-empty
	// arrays (only in_array=1 rows, NULL LEFT JOIN) are still included.
	for _, o := range q.OrderBy {
		fp := parseProtoFieldPath(o.Field.GetFieldPath())
		if fp == "__name__" {
			continue
		}
		b.whereClauses = append(b.whereClauses,
			"EXISTS(SELECT 1 FROM field_index fi_ex WHERE fi_ex.doc_path=d.path AND fi_ex.project=d.project AND fi_ex.database=d.database AND fi_ex.field_path=?)",
		)
		b.args = append(b.args, fp)
	}

	// Build WHERE filter clause.
	if q.Where != nil {
		clause, args, needsGo := b.buildFilter(q.Where)
		if clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, args...)
		}
		if needsGo {
			b.needsGoFilter = true
		}
	}

	// Build cursor clauses (require ORDER BY aliases to be resolved first).
	if q.StartAt != nil && len(q.StartAt.Values) > 0 {
		clause, args := b.buildCursor(q.StartAt, effectiveOrders, true)
		if clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, args...)
		}
	}
	if q.EndAt != nil && len(q.EndAt.Values) > 0 {
		clause, args := b.buildCursor(q.EndAt, effectiveOrders, false)
		if clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, args...)
		}
	}

	// Assemble final SQL.
	sql, finalArgs := b.assemble(collectionID, q)

	result := &Result{
		SQL:           sql,
		Args:          finalArgs,
		NeedsGoFilter: b.needsGoFilter,
	}
	slog.Debug("query", "sql", result.SQL, "args", result.Args)
	return result, nil
}

// BuildPathOnly is like Build but generates SELECT d.path instead of SELECT d.data.
// Use for lightweight scope repopulation (no proto unmarshal needed).
// Falls back to Build when NeedsGoFilter is set (caller must use full docs).
func BuildPathOnly(project, database, parentPath string, allDescendants bool, q *firestorepb.StructuredQuery) (*Result, error) {
	if q == nil {
		return nil, fmt.Errorf("nil StructuredQuery")
	}

	b := &builder{
		project:        project,
		database:       database,
		parentPath:     parentPath,
		allDescendants: allDescendants,
		pathOnly:       true,
	}

	if len(q.From) == 0 {
		return nil, fmt.Errorf("StructuredQuery.from is empty")
	}
	collectionID := q.From[0].CollectionId
	if collectionID == "" && !allDescendants {
		return nil, fmt.Errorf("StructuredQuery.from[0].collection_id is empty")
	}

	effectiveOrders := addImplicitOrderBy(q.OrderBy, q.Where)
	b.buildOrderBy(effectiveOrders)

	for _, o := range q.OrderBy {
		fp := parseProtoFieldPath(o.Field.GetFieldPath())
		if fp == "__name__" {
			continue
		}
		b.whereClauses = append(b.whereClauses,
			"EXISTS(SELECT 1 FROM field_index fi_ex WHERE fi_ex.doc_path=d.path AND fi_ex.project=d.project AND fi_ex.database=d.database AND fi_ex.field_path=?)",
		)
		b.args = append(b.args, fp)
	}

	if q.Where != nil {
		clause, args, needsGo := b.buildFilter(q.Where)
		if clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, args...)
		}
		if needsGo {
			b.needsGoFilter = true
		}
	}

	if q.StartAt != nil && len(q.StartAt.Values) > 0 {
		clause, args := b.buildCursor(q.StartAt, effectiveOrders, true)
		if clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, args...)
		}
	}
	if q.EndAt != nil && len(q.EndAt.Values) > 0 {
		clause, args := b.buildCursor(q.EndAt, effectiveOrders, false)
		if clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, args...)
		}
	}

	sql, finalArgs := b.assemble(collectionID, q)
	return &Result{SQL: sql, Args: finalArgs, NeedsGoFilter: b.needsGoFilter}, nil
}

// BuildSorted generates a SELECT d.data query driving FROM field_index INNER JOIN documents.
// This allows SQLite to use idx_field_sort_int/str/dbl covering indexes for O(LIMIT) pagination
// instead of scanning the entire collection for ORDER BY.
//
// Preconditions (caller must verify before calling):
//   - Not allDescendants (requires exact collection_path match in field_index)
//   - effectiveOrders has exactly 2 entries: [sortField ASC/DESC, __name__ ASC/DESC]
//   - sortCol is the detected value column ("value_int", "value_string", "value_double")
//   - sortField is the parsed field path (call ParseProtoFieldPath first)
func BuildSorted(project, database, parentPath, collectionID string,
	q *firestorepb.StructuredQuery, sortField, sortCol string) (*Result, error) {
	if q == nil {
		return nil, fmt.Errorf("nil StructuredQuery")
	}

	desc := len(q.OrderBy) > 0 && q.OrderBy[0].Direction == firestorepb.StructuredQuery_DESCENDING
	effectiveOrders := addImplicitOrderBy(q.OrderBy, q.Where)

	b := &builder{
		project:    project,
		database:   database,
		parentPath: parentPath,
	}

	if q.Where != nil {
		clause, args, needsGo := b.buildFilter(q.Where)
		if clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, args...)
		}
		if needsGo {
			b.needsGoFilter = true
		}
	}

	if q.StartAt != nil && len(q.StartAt.Values) > 0 {
		if clause, args := buildSortedCursor(q.StartAt, effectiveOrders, sortField, sortCol, true, desc); clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, args...)
		}
	}
	if q.EndAt != nil && len(q.EndAt.Values) > 0 {
		if clause, args := buildSortedCursor(q.EndAt, effectiveOrders, sortField, sortCol, false, desc); clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, args...)
		}
	}

	collPath := collectionID
	if parentPath != "" {
		collPath = parentPath + "/" + collectionID
	}

	var sb strings.Builder
	var args []any

	sb.WriteString("SELECT d.data FROM field_index fi")
	sb.WriteString("\n  INNER JOIN documents d ON d.project=fi.project AND d.database=fi.database AND d.path=fi.doc_path AND d.deleted=0")
	sb.WriteString("\nWHERE fi.project=? AND fi.database=? AND fi.collection_path=? AND fi.field_path=? AND fi.in_array=0")
	args = append(args, project, database, collPath, sortField)

	for _, wc := range b.whereClauses {
		sb.WriteString("\n  AND ")
		sb.WriteString(wc)
	}
	args = append(args, b.args...)

	dir := "ASC"
	if desc {
		dir = "DESC"
	}
	fmt.Fprintf(&sb, "\nORDER BY fi.%s %s, d.path %s", sortCol, dir, dir)

	hasLimit := q.Limit != nil && q.Limit.Value > 0
	hasOffset := q.Offset > 0
	if hasLimit {
		sb.WriteString("\nLIMIT ?")
		args = append(args, int64(q.Limit.Value))
	} else if hasOffset {
		sb.WriteString("\nLIMIT -1")
	}
	if hasOffset {
		sb.WriteString("\nOFFSET ?")
		args = append(args, int64(q.Offset))
	}

	slog.Debug("query sorted", "sql", sb.String(), "args", args)
	return &Result{SQL: sb.String(), Args: args, NeedsGoFilter: b.needsGoFilter}, nil
}

// buildSortedCursor generates the keyset WHERE clause for a StartAt/EndAt cursor
// in a BuildSorted query. Uses fi.{sortCol} and d.path directly.
func buildSortedCursor(cursor *firestorepb.Cursor, orders []*firestorepb.StructuredQuery_Order,
	sortField, sortCol string, isStart bool, desc bool) (string, []any) {
	if len(cursor.Values) == 0 || len(orders) == 0 {
		return "", nil
	}

	var sortVal any
	var docPath string
	hasSortVal := false
	hasDocPath := false

	for i, o := range orders {
		if i >= len(cursor.Values) {
			break
		}
		fp := parseProtoFieldPath(o.Field.GetFieldPath())
		v := cursor.Values[i]
		if fp == sortField {
			col, val, ok := scalarSQL(v)
			if !ok || col != sortCol {
				return "", nil // type mismatch; caller falls back to regular path
			}
			sortVal = val
			hasSortVal = true
		} else if fp == "__name__" {
			if ref, ok := v.ValueType.(*firestorepb.Value_ReferenceValue); ok {
				docPath = docPathFromRef(ref.ReferenceValue)
				hasDocPath = true
			}
		}
	}

	if !hasSortVal {
		return "", nil
	}

	// Keyset operators for ASC ordering:
	//   StartAt before=true  (inclusive): (col > val) OR (col = val AND path >= docPath)
	//   StartAt before=false (exclusive): (col > val) OR (col = val AND path >  docPath)
	//   EndAt   before=false (inclusive): (col < val) OR (col = val AND path <= docPath)
	//   EndAt   before=true  (exclusive): (col < val) OR (col = val AND path <  docPath)
	var strictOp, pathOp string
	if isStart {
		if cursor.Before {
			strictOp, pathOp = ">", ">="
		} else {
			strictOp, pathOp = ">", ">"
		}
	} else {
		if cursor.Before {
			strictOp, pathOp = "<", "<"
		} else {
			strictOp, pathOp = "<", "<="
		}
	}
	if desc {
		strictOp = flipDesc(strictOp, true)
		pathOp = flipDesc(pathOp, true)
	}

	if !hasDocPath {
		return fmt.Sprintf("fi.%s %s ?", sortCol, pathOp), []any{sortVal}
	}
	return fmt.Sprintf("(fi.%s %s ? OR (fi.%s = ? AND d.path %s ?))", sortCol, strictOp, sortCol, pathOp),
		[]any{sortVal, sortVal, docPath}
}

type builder struct {
	project, database, parentPath string
	allDescendants                bool

	joins      []string // LEFT JOIN clauses (one per ORDER BY field)
	joinArgs   []any    // args for join field_path bindings
	orderExprs []string // ORDER BY expressions

	whereClauses []string
	args         []any

	needsGoFilter bool
	fiCounter     int  // tracks fi_N alias counter for ORDER BY joins
	pathOnly      bool // emit SELECT d.path instead of SELECT d.data
}

// fiAlias returns the LEFT JOIN alias for the Nth ORDER BY field.
func (b *builder) fiAlias(n int) string {
	return fmt.Sprintf("fi_%d", n)
}

// buildOrderBy processes the ORDER BY clause, emitting LEFT JOINs.
func (b *builder) buildOrderBy(orders []*firestorepb.StructuredQuery_Order) {
	for i, o := range orders {
		fp := parseProtoFieldPath(o.Field.GetFieldPath())
		if fp == "__name__" {
			// __name__ maps to d.path directly; no JOIN needed.
			dir := "ASC"
			if o.Direction == firestorepb.StructuredQuery_DESCENDING {
				dir = "DESC"
			}
			b.orderExprs = append(b.orderExprs, fmt.Sprintf("d.path %s", dir))
			continue
		}

		alias := b.fiAlias(i)
		// in_array=0 ensures at most one row per (doc, field_path) for scalar/map/sentinel rows.
		// Array elements (in_array=1) are excluded from the JOIN; existence is checked separately.
		b.joins = append(b.joins,
			fmt.Sprintf(
				"LEFT JOIN field_index %s ON %s.doc_path=d.path AND %s.project=d.project AND %s.database=d.database AND %s.field_path=? AND %s.in_array=0",
				alias, alias, alias, alias, alias, alias,
			),
		)
		b.joinArgs = append(b.joinArgs, fp)

		expr := orderExpr(alias, o.Direction)
		b.orderExprs = append(b.orderExprs, expr)
		b.fiCounter++
	}
}

// orderExpr returns the ORDER BY column expression for a given fi alias.
// We use a CASE expression that covers the common Firestore value types.
// Cross-type ordering (e.g. int vs string) falls back to NULL ordering (acceptable for emulator).
func orderExpr(alias string, dir firestorepb.StructuredQuery_Direction) string {
	d := "ASC"
	if dir == firestorepb.StructuredQuery_DESCENDING {
		d = "DESC"
	}
	// Use COALESCE for numerics; string/timestamp share value_string; bool uses value_bool.
	// The CASE picks the most-likely non-null column.
	return fmt.Sprintf(
		"CASE WHEN %s.value_int IS NOT NULL OR %s.value_double IS NOT NULL THEN CAST(COALESCE(%s.value_int, %s.value_double) AS REAL) END %s, %s.value_string %s, %s.value_bool %s",
		alias, alias, alias, alias, d,
		alias, d,
		alias, d,
	)
}

// ParseProtoFieldPath is the exported form of parseProtoFieldPath.
func ParseProtoFieldPath(s string) string { return parseProtoFieldPath(s) }

// parseProtoFieldPath converts a Firestore proto field_path (which may contain
// backtick-escaped segments for field names with special characters) to the
// storage key format used in field_index (dot-separated raw segment names).
//
// Examples:
//
//	"metadata.createdAt"  ->  "metadata.createdAt"  (unchanged, simple path)
//	"`field.dot`"         ->  "field.dot"            (literal-dot field name)
//	"`field\\slash`"      ->  "field\slash"          (escaped backslash)
func parseProtoFieldPath(s string) string {
	if !strings.ContainsAny(s, "`") {
		return s // fast path: no backticks, return as-is
	}
	var segments []string
	i := 0
	for i < len(s) {
		if s[i] == '`' {
			// Backtick-escaped segment: read until the closing (unescaped) backtick.
			i++ // skip opening backtick
			var seg strings.Builder
			for i < len(s) {
				if s[i] == '\\' && i+1 < len(s) {
					seg.WriteByte(s[i+1]) // unescape: \\ -> \, \` -> `
					i += 2
				} else if s[i] == '`' {
					i++ // skip closing backtick
					break
				} else {
					seg.WriteByte(s[i])
					i++
				}
			}
			segments = append(segments, seg.String())
		} else {
			// Unescaped segment: read until the next '.' separator.
			start := i
			for i < len(s) && s[i] != '.' {
				i++
			}
			if i > start {
				segments = append(segments, s[start:i])
			}
		}
		if i < len(s) && s[i] == '.' {
			i++ // skip segment separator
		}
	}
	return strings.Join(segments, ".")
}

// collectInequalityFields recursively walks a filter and returns the unique
// set of field paths that appear in inequality predicates (NOT_EQUAL, LT,
// LTE, GT, GTE, NOT_IN). The result is sorted lexicographically and used
// to derive Firestore's implicit ORDER BY for multiple-inequality queries.
func collectInequalityFields(f *firestorepb.StructuredQuery_Filter) []string {
	if f == nil {
		return nil
	}
	seen := map[string]struct{}{}
	var walk func(*firestorepb.StructuredQuery_Filter)
	walk = func(f *firestorepb.StructuredQuery_Filter) {
		switch ft := f.FilterType.(type) {
		case *firestorepb.StructuredQuery_Filter_CompositeFilter:
			for _, sub := range ft.CompositeFilter.Filters {
				walk(sub)
			}
		case *firestorepb.StructuredQuery_Filter_FieldFilter:
			ff := ft.FieldFilter
			switch ff.Op {
			case firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL,
				firestorepb.StructuredQuery_FieldFilter_LESS_THAN,
				firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
				firestorepb.StructuredQuery_FieldFilter_GREATER_THAN,
				firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL,
				firestorepb.StructuredQuery_FieldFilter_NOT_IN:
				fp := parseProtoFieldPath(ff.Field.GetFieldPath())
				seen[fp] = struct{}{}
			}
		}
	}
	walk(f)
	fields := make([]string, 0, len(seen))
	for fp := range seen {
		fields = append(fields, fp)
	}
	// Lexicographic sort - matches SDK's getInequalityFilterFields sort order.
	for i := 1; i < len(fields); i++ {
		for j := i; j > 0 && fields[j] < fields[j-1]; j-- {
			fields[j], fields[j-1] = fields[j-1], fields[j]
		}
	}
	return fields
}

// AddImplicitOrderBy is the exported form of addImplicitOrderBy for use by
// packages (e.g. server) that need the effective ORDER BY outside of SQL generation.
func AddImplicitOrderBy(explicit []*firestorepb.StructuredQuery_Order, where *firestorepb.StructuredQuery_Filter) []*firestorepb.StructuredQuery_Order {
	return addImplicitOrderBy(explicit, where)
}

// addImplicitOrderBy returns the effective ORDER BY for a query, adding
// implicit ordering by inequality filter fields (sorted lexicographically)
// when they are not already present. This mirrors the Firestore server
// behaviour for multiple-inequality queries.
//
// The implicit fields use ASC direction (or the direction of the last
// explicit ORDER BY when one exists). __name__ is appended last if absent.
func addImplicitOrderBy(explicit []*firestorepb.StructuredQuery_Order, where *firestorepb.StructuredQuery_Filter) []*firestorepb.StructuredQuery_Order {
	implicit := collectInequalityFields(where)
	if len(implicit) == 0 {
		return explicit // nothing to add
	}

	// Determine the direction for implicit fields.
	dir := firestorepb.StructuredQuery_ASCENDING
	if len(explicit) > 0 {
		dir = explicit[len(explicit)-1].Direction
	}

	// Build a set of field paths already covered by the explicit ORDER BY.
	covered := make(map[string]struct{}, len(explicit))
	for _, o := range explicit {
		covered[parseProtoFieldPath(o.Field.GetFieldPath())] = struct{}{}
	}

	result := make([]*firestorepb.StructuredQuery_Order, len(explicit))
	copy(result, explicit)

	for _, fp := range implicit {
		if _, ok := covered[fp]; ok {
			continue // already in explicit ORDER BY
		}
		if fp == "__name__" {
			continue // will be added as the final tiebreaker below
		}
		result = append(result, &firestorepb.StructuredQuery_Order{
			Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: fp},
			Direction: dir,
		})
		covered[fp] = struct{}{}
	}

	// Always append __name__ as the final tiebreaker if not already present.
	if _, ok := covered["__name__"]; !ok {
		result = append(result, &firestorepb.StructuredQuery_Order{
			Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: "__name__"},
			Direction: dir,
		})
	}

	return result
}

// buildFilter recursively translates a StructuredQuery_Filter to a SQL clause.
// Returns (clause, args, needsGoFilter).
func (b *builder) buildFilter(f *firestorepb.StructuredQuery_Filter) (string, []any, bool) {
	if f == nil {
		return "", nil, false
	}
	switch ft := f.FilterType.(type) {
	case *firestorepb.StructuredQuery_Filter_FieldFilter:
		return buildFieldFilter(ft.FieldFilter)
	case *firestorepb.StructuredQuery_Filter_UnaryFilter:
		return buildUnaryFilter(ft.UnaryFilter)
	case *firestorepb.StructuredQuery_Filter_CompositeFilter:
		return b.buildCompositeFilter(ft.CompositeFilter)
	}
	return "", nil, false
}

func (b *builder) buildCompositeFilter(cf *firestorepb.StructuredQuery_CompositeFilter) (string, []any, bool) {
	var parts []string
	var args []any
	needsGo := false

	for _, sub := range cf.Filters {
		clause, subArgs, subNeedsGo := b.buildFilter(sub)
		if clause != "" {
			parts = append(parts, clause)
			args = append(args, subArgs...)
		}
		if subNeedsGo {
			needsGo = true
		}
	}

	if len(parts) == 0 {
		return "", args, needsGo
	}

	var joined string
	switch cf.Op {
	case firestorepb.StructuredQuery_CompositeFilter_AND:
		joined = strings.Join(parts, " AND ")
	case firestorepb.StructuredQuery_CompositeFilter_OR:
		joined = "(" + strings.Join(parts, " OR ") + ")"
	default:
		joined = strings.Join(parts, " AND ")
	}
	return joined, args, needsGo
}

// existsClause builds an EXISTS subquery for a field_index condition.
func existsClause(fieldPath, cond string) string {
	return fmt.Sprintf(
		"EXISTS(SELECT 1 FROM field_index fi WHERE fi.project=d.project AND fi.database=d.database AND fi.doc_path=d.path AND fi.field_path=? AND (%s))",
		cond,
	)
}

func buildFieldFilter(f *firestorepb.StructuredQuery_FieldFilter) (string, []any, bool) {
	fp := parseProtoFieldPath(f.Field.GetFieldPath())
	v := f.Value

	// __name__ is a virtual field that maps directly to d.path; it is NOT stored
	// in field_index, so we must handle it with direct d.path comparisons.
	if fp == "__name__" {
		return buildDocIDFilter(f.Op, v)
	}

	switch f.Op {
	case firestorepb.StructuredQuery_FieldFilter_EQUAL:
		col, val, ok := scalarSQL(v)
		if !ok {
			return "", nil, true // Go fallback
		}
		clause := existsClause(fp, col+"=? AND fi.in_array=0")
		return clause, []any{fp, val}, false

	case firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
		col, val, ok := scalarSQL(v)
		if !ok {
			return "", nil, true // Go fallback
		}
		clause := existsClause(fp, col+"=? AND fi.in_array=1")
		return clause, []any{fp, val}, false

	case firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL:
		// NaN cannot be expressed in SQL (stored as NULL); fall back to Go.
		if dv, isDouble := v.ValueType.(*firestorepb.Value_DoubleValue); isDouble && math.IsNaN(dv.DoubleValue) {
			return "", nil, true
		}
		col, val, ok := scalarSQL(v)
		if !ok {
			return "", nil, true
		}
		// Field must exist with a non-null scalar value AND must not equal the given value.
		// Excluding null rows (fi.value_null IS NULL) ensures null-valued fields are omitted
		// (Firestore's != semantics exclude nulls).
		fieldExists := existsClause(fp, "fi.value_null IS NULL")
		notEqual := fmt.Sprintf(
			"NOT EXISTS(SELECT 1 FROM field_index fi WHERE fi.project=d.project AND fi.database=d.database AND fi.doc_path=d.path AND fi.field_path=? AND fi.%s=? AND fi.in_array=0)",
			col,
		)
		return "(" + fieldExists + " AND " + notEqual + ")", []any{fp, fp, val}, false

	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN:
		return numericOrScalarCompare(fp, v, "<")
	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
		return numericOrScalarCompare(fp, v, "<=")
	case firestorepb.StructuredQuery_FieldFilter_GREATER_THAN:
		return numericOrScalarCompare(fp, v, ">")
	case firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
		return numericOrScalarCompare(fp, v, ">=")

	case firestorepb.StructuredQuery_FieldFilter_IN:
		return buildInFilter(fp, v, false, false)

	case firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:
		return buildInFilter(fp, v, false, true)

	case firestorepb.StructuredQuery_FieldFilter_NOT_IN:
		return buildInFilter(fp, v, true, false)
	}

	return "", nil, true // unknown op - Go fallback
}

// buildDocIDFilter builds a SQL clause for __name__ (document ID) field filters,
// comparing directly against d.path which stores the relative document path.
func buildDocIDFilter(op firestorepb.StructuredQuery_FieldFilter_Operator, v *firestorepb.Value) (string, []any, bool) {
	refToPath := func(val *firestorepb.Value) (string, bool) {
		ref, ok := val.ValueType.(*firestorepb.Value_ReferenceValue)
		if !ok {
			// String value: treat as relative path segment.
			if s, ok := val.ValueType.(*firestorepb.Value_StringValue); ok {
				return s.StringValue, true
			}
			return "", false
		}
		return docPathFromRef(ref.ReferenceValue), true
	}

	switch op {
	case firestorepb.StructuredQuery_FieldFilter_EQUAL:
		path, ok := refToPath(v)
		if !ok {
			return "", nil, true
		}
		return "d.path=?", []any{path}, false

	case firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL:
		path, ok := refToPath(v)
		if !ok {
			return "", nil, true
		}
		return "d.path!=?", []any{path}, false

	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN,
		firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
		firestorepb.StructuredQuery_FieldFilter_GREATER_THAN,
		firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
		// Range comparisons on __name__ require Firestore's virtual-ID ordering
		// (virtual IDs like __id7__ sort before regular strings, ordered by their
		// int64 value). SQL string comparison cannot express this ordering, so we
		// fall back to Go-side filtering which uses compareDocNames.
		return "", nil, true

	case firestorepb.StructuredQuery_FieldFilter_IN:
		av := v.GetArrayValue()
		if av == nil || len(av.Values) == 0 {
			return "1=0", nil, false
		}
		placeholders := make([]string, len(av.Values))
		args := make([]any, 0, len(av.Values))
		for i, elem := range av.Values {
			path, ok := refToPath(elem)
			if !ok {
				return "", nil, true
			}
			placeholders[i] = "?"
			args = append(args, path)
		}
		return fmt.Sprintf("d.path IN (%s)", strings.Join(placeholders, ",")), args, false

	case firestorepb.StructuredQuery_FieldFilter_NOT_IN:
		av := v.GetArrayValue()
		if av == nil || len(av.Values) == 0 {
			return "1=1", nil, false
		}
		placeholders := make([]string, len(av.Values))
		args := make([]any, 0, len(av.Values))
		for i, elem := range av.Values {
			path, ok := refToPath(elem)
			if !ok {
				return "", nil, true
			}
			placeholders[i] = "?"
			args = append(args, path)
		}
		return fmt.Sprintf("d.path NOT IN (%s)", strings.Join(placeholders, ",")), args, false
	}

	return "", nil, true
}

// numericOrScalarCompare builds a comparison EXISTS clause.
// For numeric values, it emits (value_int OP ? OR value_double OP ?).
// For other types, it uses the single appropriate column.
func numericOrScalarCompare(fieldPath string, v *firestorepb.Value, op string) (string, []any, bool) {
	switch v.ValueType.(type) {
	case *firestorepb.Value_IntegerValue:
		n := v.GetIntegerValue()
		f := float64(n)
		cond := fmt.Sprintf("value_int %s ? OR value_double %s ?", op, op)
		return existsClause(fieldPath, cond), []any{fieldPath, n, f}, false

	case *firestorepb.Value_DoubleValue:
		d := v.GetDoubleValue()
		n := int64(d)
		cond := fmt.Sprintf("value_int %s ? OR value_double %s ?", op, op)
		return existsClause(fieldPath, cond), []any{fieldPath, n, d}, false

	default:
		col, val, ok := scalarSQL(v)
		if !ok {
			return "", nil, true
		}
		cond := fmt.Sprintf("%s %s ?", col, op)
		return existsClause(fieldPath, cond), []any{fieldPath, val}, false
	}
}

// buildInFilter builds an IN / NOT IN / ARRAY_CONTAINS_ANY EXISTS clause.
// matchArray=true targets in_array=1 rows (for ARRAY_CONTAINS_ANY);
// matchArray=false targets in_array=0 rows (for IN / NOT_IN).
func buildInFilter(fieldPath string, v *firestorepb.Value, notIn bool, matchArray bool) (string, []any, bool) {
	av := v.GetArrayValue()
	if av == nil || len(av.Values) == 0 {
		if notIn {
			return "1=1", nil, false // NOT IN empty -> all match
		}
		return "1=0", nil, false // IN empty -> none match
	}

	// NOT IN [null, ...] always returns 0 results per Firestore spec.
	// If any element is a NaN double, SQL cannot represent it; fall back to Go.
	for _, elem := range av.Values {
		if notIn {
			if _, isNull := elem.ValueType.(*firestorepb.Value_NullValue); isNull {
				return "1=0", nil, false
			}
		}
		if dv, isDouble := elem.ValueType.(*firestorepb.Value_DoubleValue); isDouble && math.IsNaN(dv.DoubleValue) {
			return "", nil, true
		}
	}

	// Determine column from first element type; all elements should be same type.
	col, _, ok := scalarSQL(av.Values[0])
	if !ok {
		return "", nil, true
	}

	placeholders := make([]string, len(av.Values))
	args := []any{fieldPath}
	for i, elem := range av.Values {
		_, val, ok := scalarSQL(elem)
		if !ok {
			return "", nil, true
		}
		placeholders[i] = "?"
		args = append(args, val)
	}

	inList := strings.Join(placeholders, ", ")

	inArrayCond := "fi.in_array=0"
	if matchArray {
		inArrayCond = "fi.in_array=1"
	}
	cond := fmt.Sprintf("%s IN (%s) AND %s", col, inList, inArrayCond)

	if notIn {
		// Field must exist with a non-null value AND must not be in the list.
		// Excluding null rows ensures null-valued fields are omitted (Firestore's not-in semantics).
		fieldExists := existsClause(fieldPath, "fi.value_null IS NULL")
		notInClause := fmt.Sprintf(
			"NOT EXISTS(SELECT 1 FROM field_index fi WHERE fi.project=d.project AND fi.database=d.database AND fi.doc_path=d.path AND fi.field_path=? AND fi.%s IN (%s) AND fi.in_array=0)",
			col, inList,
		)
		return "(" + fieldExists + " AND " + notInClause + ")", append([]any{fieldPath}, args...), false
	}
	return existsClause(fieldPath, cond), args, false
}

func buildUnaryFilter(f *firestorepb.StructuredQuery_UnaryFilter) (string, []any, bool) {
	fieldRef, ok := f.OperandType.(*firestorepb.StructuredQuery_UnaryFilter_Field)
	if !ok {
		return "", nil, true
	}
	fp := parseProtoFieldPath(fieldRef.Field.GetFieldPath())

	switch f.Op {
	case firestorepb.StructuredQuery_UnaryFilter_IS_NULL:
		clause := existsClause(fp, "value_null=1")
		return clause, []any{fp}, false

	case firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NULL:
		// Field must exist (has a row) AND that row must not be a null value.
		// value_null IS NULL is true for every non-null type stored in field_index.
		clause := existsClause(fp, "value_null IS NULL")
		return clause, []any{fp}, false

	case firestorepb.StructuredQuery_UnaryFilter_IS_NAN,
		firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NAN:
		// SQLite has no IS NAN predicate; fall back to Go-side filtering.
		return "", nil, true
	}
	return "", nil, true
}

// scalarSQL returns the field_index column name and value for a scalar Value.
// ok=false signals the value cannot be expressed in SQL (GeoPoint, etc.).
func scalarSQL(v *firestorepb.Value) (col string, val any, ok bool) {
	if v == nil {
		return "", nil, false
	}
	switch vt := v.ValueType.(type) {
	case *firestorepb.Value_StringValue:
		return "value_string", vt.StringValue, true
	case *firestorepb.Value_IntegerValue:
		return "value_int", vt.IntegerValue, true
	case *firestorepb.Value_DoubleValue:
		return "value_double", vt.DoubleValue, true
	case *firestorepb.Value_BooleanValue:
		b := int64(0)
		if vt.BooleanValue {
			b = 1
		}
		return "value_bool", b, true
	case *firestorepb.Value_NullValue:
		return "value_null", int64(1), true
	case *firestorepb.Value_ReferenceValue:
		return "value_ref", vt.ReferenceValue, true
	case *firestorepb.Value_BytesValue:
		return "value_bytes", vt.BytesValue, true
	case *firestorepb.Value_TimestampValue:
		s := vt.TimestampValue.AsTime().UTC().Format(time.RFC3339Nano)
		return "value_string", s, true
	}
	return "", nil, false
}



// cursorOp returns the SQL comparison operator for a cursor field.
// strict=true forces a strict inequality regardless of before (used for intermediate fields
// in multi-field cursors where equality on earlier fields is implied).
func cursorOp(isStart, before, strict, desc bool) string {
	var op string
	if strict {
		if isStart {
			op = ">"
		} else {
			op = "<"
		}
	} else if isStart {
		if before {
			op = ">="
		} else {
			op = ">"
		}
	} else {
		if before {
			op = "<"
		} else {
			op = "<="
		}
	}
	return flipDesc(op, desc)
}

// flipDesc flips the direction of a SQL comparison operator when ordering is DESCENDING.
func flipDesc(op string, desc bool) string {
	if !desc {
		return op
	}
	switch op {
	case ">":
		return "<"
	case ">=":
		return "<="
	case "<":
		return ">"
	case "<=":
		return ">="
	}
	return op
}

// buildCursor generates a WHERE clause for a StartAt or EndAt cursor.
// isStart=true -> StartAt, isStart=false -> EndAt.
func (b *builder) buildCursor(cursor *firestorepb.Cursor, orders []*firestorepb.StructuredQuery_Order, isStart bool) (string, []any) {
	if len(cursor.Values) == 0 || len(orders) == 0 {
		return "", nil
	}

	n := len(cursor.Values)
	if n > len(orders) {
		n = len(orders)
	}

	if n == 1 {
		return b.singleFieldCursor(cursor, orders[0], isStart)
	}
	return b.multiFieldCursor(cursor.Values[:n], orders[:n], cursor.Before, isStart)
}

func (b *builder) singleFieldCursor(cursor *firestorepb.Cursor, o *firestorepb.StructuredQuery_Order, isStart bool) (string, []any) {
	v := cursor.Values[0]
	fp := parseProtoFieldPath(o.Field.GetFieldPath())
	desc := o.Direction == firestorepb.StructuredQuery_DESCENDING

	// Map to SQL operator.
	//   StartAt + Before=true (inclusive) + ASC -> >=
	//   StartAt + Before=false (exclusive) + ASC -> >
	//   EndAt   + Before=false (inclusive) + ASC -> <=
	//   EndAt   + Before=true  (exclusive) + ASC -> <
	// DESCENDING flips the direction.
	op := cursorOp(isStart, cursor.Before, false, desc)

	if fp == "__name__" {
		ref, ok := v.ValueType.(*firestorepb.Value_ReferenceValue)
		if !ok {
			return "", nil
		}
		// d.path stores the relative path (after /documents/); strip the resource prefix.
		return fmt.Sprintf("d.path %s ?", op), []any{docPathFromRef(ref.ReferenceValue)}
	}

	// Determine the right fi alias for this order field.
	// The alias index equals the position in the orders slice for non-__name__ fields.
	alias, idx := b.orderAliasFor(fp, o)
	_ = idx
	if alias == "" {
		return "", nil
	}

	col, val, ok := scalarSQL(v)
	if !ok {
		return "", nil
	}

	return fmt.Sprintf("%s.%s %s ?", alias, col, op), []any{val}
}

// multiFieldCursor expands a multi-field cursor into a row-value comparison:
// (f1 > v1) OR (f1 = v1 AND f2 > v2) OR ... OR (f1=v1 AND ... AND fN-1=vN-1 AND fN >= vN)
func (b *builder) multiFieldCursor(vals []*firestorepb.Value, orders []*firestorepb.StructuredQuery_Order, before bool, isStart bool) (string, []any) {
	var parts []string
	var args []any

	n := len(vals)
	for i := 0; i < n; i++ {
		desc := orders[i].Direction == firestorepb.StructuredQuery_DESCENDING
		pivotOp := cursorOp(isStart, before, i < n-1, desc)

		var termParts []string
		var termArgs []any

		// Equality conditions for fields 0..i-1
		for j := 0; j < i; j++ {
			alias, _ := b.orderAliasFor(parseProtoFieldPath(orders[j].Field.GetFieldPath()), orders[j])
			col, val, ok := scalarSQL(vals[j])
			if !ok {
				return "", nil
			}
			if alias == "" {
				// __name__ field: d.path stores relative paths; strip the resource prefix.
				if ref, isRef := vals[j].ValueType.(*firestorepb.Value_ReferenceValue); isRef {
					val = docPathFromRef(ref.ReferenceValue)
				}
				termParts = append(termParts, "d.path=?")
			} else {
				termParts = append(termParts, fmt.Sprintf("%s.%s=?", alias, col))
			}
			termArgs = append(termArgs, val)
		}

		// Comparison at position i
		alias, _ := b.orderAliasFor(parseProtoFieldPath(orders[i].Field.GetFieldPath()), orders[i])
		col, val, ok := scalarSQL(vals[i])
		if !ok {
			return "", nil
		}
		if alias == "" {
			// __name__ field: d.path stores relative paths; strip the resource prefix.
			if ref, isRef := vals[i].ValueType.(*firestorepb.Value_ReferenceValue); isRef {
				val = docPathFromRef(ref.ReferenceValue)
			}
			termParts = append(termParts, fmt.Sprintf("d.path %s ?", pivotOp))
		} else {
			termParts = append(termParts, fmt.Sprintf("%s.%s %s ?", alias, col, pivotOp))
		}
		termArgs = append(termArgs, val)

		parts = append(parts, "("+strings.Join(termParts, " AND ")+")")
		args = append(args, termArgs...)
	}

	if len(parts) == 0 {
		return "", nil
	}
	return "(" + strings.Join(parts, " OR ") + ")", args
}

// docPathFromRef extracts the relative document path from a Firestore reference value.
// Reference format: "projects/{p}/databases/{d}/documents/{path}"
// d.path stores only the segment after /documents/, so we must strip the prefix
// before comparing against it in SQL cursors.
func docPathFromRef(ref string) string {
	const sep = "/documents/"
	if i := strings.LastIndex(ref, sep); i >= 0 {
		return ref[i+len(sep):]
	}
	return ref
}

// orderAliasFor returns the fi_N alias for an ORDER BY field.
// Scans the joins slice to find the matching LEFT JOIN (by field_path arg).
func (b *builder) orderAliasFor(fieldPath string, o *firestorepb.StructuredQuery_Order) (alias string, idx int) {
	if fieldPath == "__name__" {
		return "", -1
	}
	for i, arg := range b.joinArgs {
		if s, ok := arg.(string); ok && s == fieldPath {
			return b.fiAlias(i), i
		}
	}
	return "", -1
}


func (b *builder) assemble(collectionID string, q *firestorepb.StructuredQuery) (string, []any) {
	var sb strings.Builder
	var args []any

	if b.pathOnly {
		sb.WriteString("SELECT d.path FROM documents d")
	} else {
		sb.WriteString("SELECT d.data FROM documents d")
	}

	// ORDER BY JOINs (args come first in the joins, before WHERE args)
	for _, j := range b.joins {
		sb.WriteString("\n  ")
		sb.WriteString(j)
	}
	args = append(args, b.joinArgs...)

	// WHERE
	baseClauses := []string{"d.project=?", "d.database=?"}
	baseArgs := []any{b.project, b.database}

	if b.allDescendants {
		if collectionID != "" {
			baseClauses = append(baseClauses, "d.collection=?")
			baseArgs = append(baseArgs, collectionID)
		}
		// Scope to descendants of parentPath when set (kindless / scoped all-descendants query).
		if b.parentPath != "" {
			baseClauses = append(baseClauses, "d.path LIKE ?")
			baseArgs = append(baseArgs, b.parentPath+"/%")
		}
	} else {
		baseClauses = append(baseClauses, "d.parent_path=?", "d.collection=?")
		baseArgs = append(baseArgs, b.parentPath, collectionID)
	}
	baseClauses = append(baseClauses, "d.deleted=0")

	allWhere := append(baseClauses, b.whereClauses...)
	sb.WriteString("\nWHERE ")
	sb.WriteString(strings.Join(allWhere, "\n  AND "))
	args = append(args, baseArgs...)
	args = append(args, b.args...)

	// ORDER BY
	if len(b.orderExprs) > 0 {
		sb.WriteString("\nORDER BY ")
		sb.WriteString(strings.Join(b.orderExprs, ", "))
	}

	// LIMIT / OFFSET - SQLite requires LIMIT before OFFSET.
	hasLimit := q.Limit != nil && q.Limit.Value > 0
	hasOffset := q.Offset > 0
	if hasLimit {
		sb.WriteString("\nLIMIT ?")
		args = append(args, int64(q.Limit.Value))
	} else if hasOffset {
		// Emit LIMIT -1 (unlimited) so OFFSET is syntactically valid.
		sb.WriteString("\nLIMIT -1")
	}
	if hasOffset {
		sb.WriteString("\nOFFSET ?")
		args = append(args, int64(q.Offset))
	}

	return sb.String(), args
}

// BuildCount generates a parameterized SELECT COUNT(*) for a single-From
// StructuredQuery. If upTo > 0 the count is capped at that value (combined
// with q.Limit when both are set). ORDER BY fields, cursors, and OFFSET from
// the base query are all respected so that `col.limit(5).count()` and
// cursor-based counts work correctly.
//
// requiredFields lists field paths that must exist with a numeric value in each
// matching document. When non-empty (multi-aggregation queries), documents that
// lack any of the required fields are excluded from the count.
//
// NeedsGoFilter is set when the filter contains predicates that cannot be
// expressed in SQL (IS_NAN, GeoPoint). The caller should fall back to streaming
// in that case rather than trust the SQL result.
func BuildCount(project, database, parentPath string, allDescendants bool, q *firestorepb.StructuredQuery, upTo int64, requiredFields []string) (*Result, error) {
	if q == nil {
		return nil, fmt.Errorf("nil StructuredQuery")
	}
	b := &builder{
		project:        project,
		database:       database,
		parentPath:     parentPath,
		allDescendants: allDescendants,
	}
	if len(q.From) == 0 {
		return nil, fmt.Errorf("StructuredQuery.from is empty")
	}
	collectionID := q.From[0].CollectionId
	if collectionID == "" && !allDescendants {
		return nil, fmt.Errorf("StructuredQuery.from[0].collection_id is empty")
	}

	// Build ORDER BY JOINs first (needed for cursor support and implicit existence filter).
	b.buildOrderBy(q.OrderBy)

	// orderBy('field') excludes docs without that field. Use EXISTS so non-empty
	// arrays (only in_array=1 rows, NULL LEFT JOIN) are still included.
	for _, o := range q.OrderBy {
		fp := parseProtoFieldPath(o.Field.GetFieldPath())
		if fp == "__name__" {
			continue
		}
		b.whereClauses = append(b.whereClauses,
			"EXISTS(SELECT 1 FROM field_index fi_ex WHERE fi_ex.doc_path=d.path AND fi_ex.project=d.project AND fi_ex.database=d.database AND fi_ex.field_path=?)",
		)
		b.args = append(b.args, fp)
	}

	// Build WHERE filter.
	if q.Where != nil {
		clause, filterArgs, needsGo := b.buildFilter(q.Where)
		if clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, filterArgs...)
		}
		if needsGo {
			b.needsGoFilter = true
		}
	}

	// Build cursor clauses (require ORDER BY aliases to be resolved first).
	if q.StartAt != nil && len(q.StartAt.Values) > 0 {
		clause, args := b.buildCursor(q.StartAt, q.OrderBy, true)
		if clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, args...)
		}
	}
	if q.EndAt != nil && len(q.EndAt.Values) > 0 {
		clause, args := b.buildCursor(q.EndAt, q.OrderBy, false)
		if clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, args...)
		}
	}

	// Base WHERE clauses.
	baseClauses := []string{"d.project=?", "d.database=?"}
	baseArgs := []any{project, database}
	if allDescendants {
		if collectionID != "" {
			baseClauses = append(baseClauses, "d.collection=?")
			baseArgs = append(baseArgs, collectionID)
		}
		if parentPath != "" {
			baseClauses = append(baseClauses, "d.path LIKE ?")
			baseArgs = append(baseArgs, parentPath+"/%")
		}
	} else {
		baseClauses = append(baseClauses, "d.parent_path=?", "d.collection=?")
		baseArgs = append(baseArgs, parentPath, collectionID)
	}
	baseClauses = append(baseClauses, "d.deleted=0")

	allWhere := append(baseClauses, b.whereClauses...)
	whereStr := strings.Join(allWhere, "\n  AND ")

	// Effective LIMIT = min(upTo, q.Limit.Value) when both are set.
	var effectiveLimit int64
	if upTo > 0 && q.Limit != nil && q.Limit.Value > 0 {
		effectiveLimit = upTo
		if int64(q.Limit.Value) < effectiveLimit {
			effectiveLimit = int64(q.Limit.Value)
		}
	} else if upTo > 0 {
		effectiveLimit = upTo
	} else if q.Limit != nil && q.Limit.Value > 0 {
		effectiveLimit = int64(q.Limit.Value)
	}

	// Build inner SELECT 1 query.
	var sb strings.Builder
	sb.WriteString("SELECT 1 FROM documents d")
	for _, j := range b.joins {
		sb.WriteString("\n  ")
		sb.WriteString(j)
	}
	sb.WriteString("\nWHERE ")
	sb.WriteString(whereStr)

	// When aggregating alongside sum/avg fields, only count documents that have
	// the required fields present (any type). Firestore excludes docs where a
	// sum/avg field is completely absent, but includes docs where the field exists
	// but is non-numeric (the numeric aggregation will simply skip those values).
	var requiredFieldArgs []any
	for _, fp := range requiredFields {
		sb.WriteString(
			"\n  AND EXISTS(SELECT 1 FROM field_index fi_req" +
				" WHERE fi_req.project=d.project AND fi_req.database=d.database" +
				" AND fi_req.doc_path=d.path AND fi_req.field_path=?)",
		)
		requiredFieldArgs = append(requiredFieldArgs, fp)
	}

	hasOffset := q.Offset > 0
	if effectiveLimit > 0 {
		sb.WriteString("\nLIMIT ?")
	} else if hasOffset {
		sb.WriteString("\nLIMIT -1")
	}
	if hasOffset {
		sb.WriteString("\nOFFSET ?")
	}

	sqlStr := "SELECT COUNT(*) FROM (" + sb.String() + ")"

	// Assemble final args: JOIN args, base args, filter/cursor args, required field args, then limit/offset.
	finalArgs := make([]any, 0)
	finalArgs = append(finalArgs, b.joinArgs...)
	finalArgs = append(finalArgs, baseArgs...)
	finalArgs = append(finalArgs, b.args...)
	finalArgs = append(finalArgs, requiredFieldArgs...)
	if effectiveLimit > 0 {
		finalArgs = append(finalArgs, effectiveLimit)
	}
	if hasOffset {
		finalArgs = append(finalArgs, int64(q.Offset))
	}

	return &Result{SQL: sqlStr, Args: finalArgs, NeedsGoFilter: b.needsGoFilter}, nil
}

// NumericAggResult holds the columns returned by BuildNumericAgg.
type NumericAggResult struct {
	// TotalSum is COALESCE(SUM(CAST(COALESCE(value_int, value_double) AS REAL)), 0).
	TotalSum float64
	// NumericCount is the number of documents with a numeric (int or double) value
	// for the field. Zero means no documents matched - AVG should return null.
	NumericCount int64
	// DoubleCount is the number of those documents that had a double (non-integer)
	// value. Zero means all numeric values were integers; the SUM result is exact
	// as int64.
	DoubleCount int64
	// NaNCount is the number of field_index rows where all value columns are NULL.
	// SQLite stores NaN float64 values as NULL, so these rows represent NaN doubles.
	// If NaNCount > 0, the aggregation result must be NaN.
	NaNCount int64
}

// BuildNumericAgg generates a query that returns (totalSum, numericCount, doubleCount, nanCount)
// for fieldPath across documents matching q. Only scalar (non-array) numeric field values are
// included - in_array=0 excludes array-element rows. ORDER BY, cursor (StartAt/EndAt), LIMIT,
// and OFFSET from q are all applied to the document filter so that cursor-based aggregations
// like col.orderBy('n').startAfter(5).aggregate({sum:'n'}) work correctly.
//
// requiredFields lists additional field paths that must each have a numeric value in the document
// for it to be included. Used for multi-aggregation queries where all sum/avg fields must exist.
func BuildNumericAgg(project, database, parentPath string, allDescendants bool, q *firestorepb.StructuredQuery, fieldPath string, requiredFields []string) (*Result, error) {
	if q == nil {
		return nil, fmt.Errorf("nil StructuredQuery")
	}
	b := &builder{
		project:        project,
		database:       database,
		parentPath:     parentPath,
		allDescendants: allDescendants,
	}
	if len(q.From) == 0 {
		return nil, fmt.Errorf("StructuredQuery.from is empty")
	}
	collectionID := q.From[0].CollectionId
	if collectionID == "" && !allDescendants {
		return nil, fmt.Errorf("StructuredQuery.from[0].collection_id is empty")
	}

	// Build ORDER BY JOINs first - needed so cursor clauses can reference fi_N aliases.
	b.buildOrderBy(q.OrderBy)

	// Implicit field-exists filter: orderBy(field) excludes docs without that field.
	for i, o := range q.OrderBy {
		fp := parseProtoFieldPath(o.Field.GetFieldPath())
		if fp == "__name__" {
			continue
		}
		alias := b.fiAlias(i)
		b.whereClauses = append(b.whereClauses, alias+".doc_path IS NOT NULL")
	}

	// Build WHERE filter.
	if q.Where != nil {
		clause, filterArgs, needsGo := b.buildFilter(q.Where)
		if clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, filterArgs...)
		}
		if needsGo {
			b.needsGoFilter = true
		}
	}

	// Build cursor clauses (require ORDER BY aliases to be resolved first).
	if q.StartAt != nil && len(q.StartAt.Values) > 0 {
		clause, args := b.buildCursor(q.StartAt, q.OrderBy, true)
		if clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, args...)
		}
	}
	if q.EndAt != nil && len(q.EndAt.Values) > 0 {
		clause, args := b.buildCursor(q.EndAt, q.OrderBy, false)
		if clause != "" {
			b.whereClauses = append(b.whereClauses, clause)
			b.args = append(b.args, args...)
		}
	}

	// Base WHERE clauses for the document subquery.
	baseClauses := []string{"d.project=?", "d.database=?"}
	baseArgs := []any{project, database}
	if allDescendants {
		if collectionID != "" {
			baseClauses = append(baseClauses, "d.collection=?")
			baseArgs = append(baseArgs, collectionID)
		}
		if parentPath != "" {
			baseClauses = append(baseClauses, "d.path LIKE ?")
			baseArgs = append(baseArgs, parentPath+"/%")
		}
	} else {
		baseClauses = append(baseClauses, "d.parent_path=?", "d.collection=?")
		baseArgs = append(baseArgs, parentPath, collectionID)
	}
	baseClauses = append(baseClauses, "d.deleted=0")

	allWhere := append(baseClauses, b.whereClauses...)
	whereStr := strings.Join(allWhere, "\n  AND ")

	// Effective LIMIT.
	var effectiveLimit int64
	if q.Limit != nil && q.Limit.Value > 0 {
		effectiveLimit = int64(q.Limit.Value)
	}

	// Inner query: the filtered set of document paths.
	var innerSB strings.Builder
	innerSB.WriteString("SELECT d.path FROM documents d")
	for _, j := range b.joins {
		innerSB.WriteString("\n  ")
		innerSB.WriteString(j)
	}
	innerSB.WriteString("\nWHERE ")
	innerSB.WriteString(whereStr)

	// Only include documents that have all required fields present (any type).
	// Firestore excludes docs where a sum/avg field is absent; docs where the
	// field exists but is non-numeric are included (SQL aggregation skips NULLs).
	var requiredFieldArgs []any
	for _, fp := range requiredFields {
		innerSB.WriteString(
			"\n  AND EXISTS(SELECT 1 FROM field_index fi_req" +
				" WHERE fi_req.project=d.project AND fi_req.database=d.database" +
				" AND fi_req.doc_path=d.path AND fi_req.field_path=?)",
		)
		requiredFieldArgs = append(requiredFieldArgs, fp)
	}

	hasOffset := q.Offset > 0
	if effectiveLimit > 0 {
		innerSB.WriteString("\nLIMIT ?")
	} else if hasOffset {
		innerSB.WriteString("\nLIMIT -1")
	}
	if hasOffset {
		innerSB.WriteString("\nOFFSET ?")
	}

	innerArgs := make([]any, 0)
	innerArgs = append(innerArgs, b.joinArgs...)
	innerArgs = append(innerArgs, baseArgs...)
	innerArgs = append(innerArgs, b.args...)
	innerArgs = append(innerArgs, requiredFieldArgs...)
	if effectiveLimit > 0 {
		innerArgs = append(innerArgs, effectiveLimit)
	}
	if hasOffset {
		innerArgs = append(innerArgs, int64(q.Offset))
	}

	// aggSelect includes a 4th column, nan_count: SQLite stores NaN float64 as NULL, so
	// all-NULL rows in field_index represent NaN doubles. If nan_count > 0 the result is NaN.
	const nanRowCond = `fi.value_int IS NULL AND fi.value_double IS NULL` +
		` AND fi.value_string IS NULL AND fi.value_bool IS NULL` +
		` AND fi.value_null IS NULL AND fi.value_ref IS NULL AND fi.value_bytes IS NULL`

	const aggSelect = `SELECT` +
		`  COALESCE(SUM(CAST(COALESCE(fi.value_int, fi.value_double) AS REAL)), 0.0),` +
		`  COUNT(CASE WHEN fi.value_int IS NOT NULL OR fi.value_double IS NOT NULL THEN 1 END),` +
		`  COUNT(fi.value_double),` +
		`  COUNT(CASE WHEN ` + nanRowCond + ` THEN 1 END)`

	const numericOrNaN = `(fi.value_int IS NOT NULL OR fi.value_double IS NOT NULL OR (` + nanRowCond + `))`

	sqlStr := aggSelect +
		` FROM field_index fi` +
		` WHERE fi.project=? AND fi.database=? AND fi.field_path=? AND fi.in_array=0` +
		`   AND ` + numericOrNaN +
		`   AND fi.doc_path IN (` + innerSB.String() + `)`

	finalArgs := make([]any, 0)
	finalArgs = append(finalArgs, project, database, fieldPath)
	finalArgs = append(finalArgs, innerArgs...)

	return &Result{SQL: sqlStr, Args: finalArgs, NeedsGoFilter: b.needsGoFilter}, nil
}
