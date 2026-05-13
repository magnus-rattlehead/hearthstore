package server

import (
	"bytes"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/magnus-rattlehead/hearthstore/internal/query"
)

// virtualIDRe matches emulator virtual doc IDs like __id7__ and __id-9223372036854775808__.
var virtualIDRe = regexp.MustCompile(`^__id(-?[0-9]+)__$`)

// parseVirtualDocID extracts the int64 from a virtual doc ID. Returns (0, false) if not virtual.
func parseVirtualDocID(id string) (int64, bool) {
	m := virtualIDRe.FindStringSubmatch(id)
	if m == nil {
		return 0, false
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// docIDFromPath returns the final path segment (document ID) from a full or relative path.
func docIDFromPath(path string) string {
	if i := strings.LastIndexByte(path, '/'); i >= 0 {
		return path[i+1:]
	}
	return path
}

// compareDocNames compares two Firestore document resource names using full
// segment-by-segment ordering with virtual-ID awareness.
//
// Collection ID segments use regular string comparison. Document ID segments
// (odd positions after stripping the projects/databases/documents/ prefix) use
// virtual-ID ordering: virtual IDs (__id<N>__) sort before all regular string
// IDs, ordered by their int64 value.
//
// This handles cross-collection comparisons correctly (e.g. "col/__id7__" <
// "col\0/__id..." used by BulkWriter as a range boundary).
func compareDocNames(a, b string) int {
	const docSep = "/documents/"
	stripDocPrefix := func(path string) string {
		if i := strings.LastIndex(path, docSep); i >= 0 {
			return path[i+len(docSep):]
		}
		return path
	}
	aRel := stripDocPrefix(a)
	bRel := stripDocPrefix(b)

	aSegs := strings.Split(aRel, "/")
	bSegs := strings.Split(bRel, "/")

	for i := 0; i < len(aSegs) && i < len(bSegs); i++ {
		var cmp int
		if i%2 == 1 {
			// Document ID segment — apply virtual-ID ordering.
			aN, aVirtual := parseVirtualDocID(aSegs[i])
			bN, bVirtual := parseVirtualDocID(bSegs[i])
			switch {
			case aVirtual && bVirtual:
				if aN < bN {
					cmp = -1
				} else if aN > bN {
					cmp = 1
				}
			case aVirtual && !bVirtual:
				cmp = -1
			case !aVirtual && bVirtual:
				cmp = 1
			default:
				if aSegs[i] < bSegs[i] {
					cmp = -1
				} else if aSegs[i] > bSegs[i] {
					cmp = 1
				}
			}
		} else {
			// Collection ID or other non-document segment — regular string comparison.
			if aSegs[i] < bSegs[i] {
				cmp = -1
			} else if aSegs[i] > bSegs[i] {
				cmp = 1
			}
		}
		if cmp != 0 {
			return cmp
		}
	}
	if len(aSegs) < len(bSegs) {
		return -1
	}
	if len(aSegs) > len(bSegs) {
		return 1
	}
	return 0
}

func (s *Server) RunQuery(req *firestorepb.RunQueryRequest, stream firestorepb.Firestore_RunQueryServer) error {
	sq, ok := req.QueryType.(*firestorepb.RunQueryRequest_StructuredQuery)
	if !ok {
		return status.Error(codes.Unimplemented, "only StructuredQuery is supported")
	}
	q := sq.StructuredQuery
	if q == nil {
		return status.Error(codes.InvalidArgument, "structured_query is required")
	}

	project, database, parentPath, err := parseParent(req.Parent)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid parent: %v", err)
	}

	// Handle inline transaction: if new_transaction is requested, register it and
	// include the transaction ID in the first response message.
	var txID []byte
	var readAt *time.Time
	switch cs := req.ConsistencySelector.(type) {
	case *firestorepb.RunQueryRequest_NewTransaction:
		id := newAutoID()
		entry := txEntry{}
		if cs.NewTransaction != nil {
			_, entry.readOnly = cs.NewTransaction.Mode.(*firestorepb.TransactionOptions_ReadOnly_)
		}
		s.txMu.Lock()
		s.txns[id] = entry
		s.txMu.Unlock()
		txID = []byte(id)
	case *firestorepb.RunQueryRequest_Transaction:
		txID = cs.Transaction
		s.txMu.Lock()
		if entry, ok := s.txns[string(cs.Transaction)]; ok && entry.readTime != nil {
			t := entry.readTime.AsTime()
			readAt = &t
		}
		s.txMu.Unlock()
	case *firestorepb.RunQueryRequest_ReadTime:
		if cs.ReadTime != nil {
			t := cs.ReadTime.AsTime()
			readAt = &t
		}
	}

	readTime := timestamppb.Now()
	explainOpts := req.GetExplainOptions()

	// Determine the effective ORDER BY: explicit fields plus implicit inequality
	// fields (sorted lexicographically) plus __name__ tiebreaker.
	effectiveOrders := query.AddImplicitOrderBy(q.OrderBy, q.Where)

	// Plan-only explain: return plan immediately without executing the query.
	// Per spec, plan-only responses must NOT include readTime (the SDK uses the
	// absence of readTime as the signal that no snapshot was produced).
	if explainOpts != nil && !explainOpts.GetAnalyze() {
		plan := buildPlanSummary(effectiveOrders)
		return stream.Send(&firestorepb.RunQueryResponse{
			ExplainMetrics: &firestorepb.ExplainMetrics{PlanSummary: plan},
		})
	}

	start := time.Now()
	sentFirst := false
	resultCount := 0

	// needsGoSort: true when there is any ORDER BY (explicit or implicit).
	needsGoSort := len(effectiveOrders) > 0

	// analyzeExplain: true when profiling is requested. We collect all docs
	// across selectors so we can attach ExplainMetrics to the last response.
	// The SDK's explainStream() counts total data events, and expects metrics+doc
	// to arrive together as one response, not as a separate trailing message.
	analyzeExplain := explainOpts != nil && explainOpts.GetAnalyze()

	// allDocs collects documents across selectors when analyzeExplain is set.
	var allDocs []*firestorepb.Document

	for _, sel := range q.From {
		var buf []*firestorepb.Document
		var goFilterAppliedLimitInGo bool // set when we rebuilt SQL without LIMIT for NeedsGoFilter

		if readAt != nil {
			// Read-time path: fetch historical snapshot from document_changes.
			buf, err = s.store.ListDocsAsOf(project, database, sel.CollectionId, parentPath, sel.AllDescendants, *readAt)
			if err != nil {
				return err
			}
			// Apply Go-side WHERE filter.
			filtered := buf[:0]
			for _, doc := range buf {
				if matchesFilter(doc, q.Where) {
					filtered = append(filtered, doc)
				}
			}
			buf = filtered
			buf = applyOffsetLimit(buf, q.Offset, q.Limit)
		} else {
			// Normal SQL path: build and execute query.
			selQ := &firestorepb.StructuredQuery{
				From:    []*firestorepb.StructuredQuery_CollectionSelector{sel},
				Where:   q.Where,
				OrderBy: q.OrderBy,
				StartAt: q.StartAt,
				EndAt:   q.EndAt,
				Offset:  q.Offset,
				Limit:   q.Limit,
			}
			// Stage 7: INNER JOIN-driven sorted path — O(LIMIT) for single-field ORDER BY.
			// Eligible when: non-allDescendants, exactly one non-__name__ sort field
			// (plus implicit __name__ tiebreaker), no Go filter, no FindNearest, no explain.
			if !analyzeExplain && !sel.AllDescendants && q.FindNearest == nil &&
				len(effectiveOrders) == 2 &&
				effectiveOrders[0].Field.GetFieldPath() != "__name__" &&
				effectiveOrders[1].Field.GetFieldPath() == "__name__" {
				sortField := query.ParseProtoFieldPath(effectiveOrders[0].Field.GetFieldPath())
				collPath := sel.CollectionId
				if parentPath != "" {
					collPath = parentPath + "/" + sel.CollectionId
				}
				if sortCol, detErr := s.store.DetectFsSortColumn(project, database, collPath, sortField); detErr == nil {
					if sortedResult, buildErr := query.BuildSorted(project, database, parentPath, sel.CollectionId, selQ, sortField, sortCol); buildErr == nil && !sortedResult.NeedsGoFilter {
						err = s.store.QueryDocs(sortedResult.SQL, sortedResult.Args, func(doc *firestorepb.Document) error {
							if q.Select != nil && len(q.Select.Fields) > 0 {
								doc = applyQueryProjection(doc, q.Select)
							}
							resp := &firestorepb.RunQueryResponse{Document: doc, ReadTime: readTime}
							if !sentFirst && len(txID) > 0 {
								resp.Transaction = txID
							}
							sentFirst = true
							resultCount++
							return stream.Send(resp)
						})
						if err != nil {
							return err
						}
						continue
					}
				}
			}

			result, err := query.Build(project, database, parentPath, sel.AllDescendants, selQ)
			if err != nil {
				return status.Errorf(codes.Internal, "query build: %v", err)
			}

			if needsGoSort || result.NeedsGoFilter || q.FindNearest != nil || analyzeExplain {
				// When NeedsGoFilter is true, some SQL results will be excluded by Go-side
				// filtering. If we also applied LIMIT in SQL, we might cut off docs before
				// Go-filtering that would have satisfied the real limit. Rebuild without
				// LIMIT/OFFSET so Go-side filtering sees all candidates; we apply the limit
				// in Go after filtering and sorting below.
				if result.NeedsGoFilter {
					selQUnlimited := proto.Clone(selQ).(*firestorepb.StructuredQuery)
					selQUnlimited.Limit = nil
					selQUnlimited.Offset = 0
					result, err = query.Build(project, database, parentPath, sel.AllDescendants, selQUnlimited)
					if err != nil {
						return status.Errorf(codes.Internal, "query build (unlimited): %v", err)
					}
					goFilterAppliedLimitInGo = true
				}
				err = s.store.QueryDocs(result.SQL, result.Args, func(doc *firestorepb.Document) error {
					if result.NeedsGoFilter && !matchesFilter(doc, q.Where) {
						return nil
					}
					buf = append(buf, doc)
					return nil
				})
				if err != nil {
					return err
				}
			} else {
				// Streaming path (no buffering needed).
				err = s.store.QueryDocs(result.SQL, result.Args, func(doc *firestorepb.Document) error {
					if q.Select != nil && len(q.Select.Fields) > 0 {
						doc = applyQueryProjection(doc, q.Select)
					}
					resp := &firestorepb.RunQueryResponse{Document: doc, ReadTime: readTime}
					if !sentFirst && len(txID) > 0 {
						resp.Transaction = txID
					}
					sentFirst = true
					resultCount++
					return stream.Send(resp)
				})
				if err != nil {
					return err
				}
				continue // skip buffered post-processing below
			}
		}

		// Post-process buffered results.

		// Apply FindNearest (vector search) before any other ordering.
		if q.FindNearest != nil {
			buf = applyFindNearest(buf, q.FindNearest)
		} else if len(effectiveOrders) > 0 {
			sortDocsByOrderBy(buf, effectiveOrders)
		}

		// When NeedsGoFilter was true we rebuilt the SQL without LIMIT/OFFSET.
		// Apply them here in Go, after filtering and sorting, so that the limit
		// is computed over the correctly filtered set rather than over the raw
		// SQL result set (which may include docs that the Go filter would reject).
		if goFilterAppliedLimitInGo {
			buf = applyOffsetLimit(buf, q.Offset, q.Limit)
		}

		if analyzeExplain {
			// Accumulate for deferred send with metrics on last doc.
			for _, doc := range buf {
				if q.Select != nil && len(q.Select.Fields) > 0 {
					doc = applyQueryProjection(doc, q.Select)
				}
				allDocs = append(allDocs, doc)
			}
		} else {
			for _, doc := range buf {
				if q.Select != nil && len(q.Select.Fields) > 0 {
					doc = applyQueryProjection(doc, q.Select)
				}
				resp := &firestorepb.RunQueryResponse{Document: doc, ReadTime: readTime}
				if !sentFirst && len(txID) > 0 {
					resp.Transaction = txID
				}
				sentFirst = true
				resultCount++
				if err := stream.Send(resp); err != nil {
					return err
				}
			}
		}
	}

	if analyzeExplain {
		// Send all docs, combining ExplainMetrics with the last document.
		// This ensures the SDK's explainStream counts exactly len(docs) responses,
		// not len(docs)+1 (which would happen with a separate trailing metrics message).
		elapsed := time.Since(start)
		plan := buildPlanSummary(effectiveOrders)
		metrics := buildExplainMetrics(plan, len(allDocs), elapsed)
		for i, doc := range allDocs {
			resp := &firestorepb.RunQueryResponse{Document: doc, ReadTime: readTime}
			if !sentFirst && len(txID) > 0 {
				resp.Transaction = txID
			}
			if i == len(allDocs)-1 {
				resp.ExplainMetrics = metrics
			}
			sentFirst = true
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
		if len(allDocs) == 0 {
			// No matching docs: send standalone metrics+readTime so the SDK
			// can create an empty snapshot with execution stats.
			resp := &firestorepb.RunQueryResponse{
				ExplainMetrics: metrics,
				ReadTime:       readTime,
			}
			if len(txID) > 0 {
				resp.Transaction = txID
			}
			return stream.Send(resp)
		}
		return nil
	}

	// Always send at least one response so the SDK receives a readTime.
	if !sentFirst {
		resp := &firestorepb.RunQueryResponse{ReadTime: readTime}
		if len(txID) > 0 {
			resp.Transaction = txID
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

// applyQueryProjection returns a new Document containing only the fields
// listed in the projection. Handles both top-level and dot-separated paths.
func applyQueryProjection(doc *firestorepb.Document, sel *firestorepb.StructuredQuery_Projection) *firestorepb.Document {
	fields := sel.GetFields()
	if len(fields) == 0 {
		return doc
	}
	// Keys-only: return document with no fields.
	if len(fields) == 1 && fields[0].GetFieldPath() == "__name__" {
		return &firestorepb.Document{Name: doc.Name, CreateTime: doc.CreateTime, UpdateTime: doc.UpdateTime}
	}
	props := make(map[string]*firestorepb.Value, len(fields))
	for _, f := range fields {
		fp := f.GetFieldPath()
		if fp == "__name__" {
			continue
		}
		if v := getField(doc, fp); v != nil {
			setProjectedField(props, fp, v)
		}
	}
	return &firestorepb.Document{Name: doc.Name, Fields: props, CreateTime: doc.CreateTime, UpdateTime: doc.UpdateTime}
}

// setProjectedField sets value v at the dot-separated path within fields,
// creating intermediate MapValues as needed.
func setProjectedField(fields map[string]*firestorepb.Value, path string, v *firestorepb.Value) {
	head, tail, nested := strings.Cut(path, ".")
	if !nested {
		fields[head] = v
		return
	}
	var nestedFields map[string]*firestorepb.Value
	if existing := fields[head]; existing != nil {
		if mv := existing.GetMapValue(); mv != nil {
			nestedFields = mv.Fields
		}
	}
	if nestedFields == nil {
		nestedFields = make(map[string]*firestorepb.Value)
	}
	setProjectedField(nestedFields, tail, v)
	fields[head] = &firestorepb.Value{
		ValueType: &firestorepb.Value_MapValue{MapValue: &firestorepb.MapValue{Fields: nestedFields}},
	}
}

// getField returns the value at the given field path, traversing nested MapValues
// for dot-separated paths (e.g. "address.city").
func getField(doc *firestorepb.Document, path string) *firestorepb.Value {
	if doc == nil || doc.Fields == nil {
		return nil
	}
	head, tail, nested := splitFieldPath(path)
	v := doc.Fields[head]
	if !nested || v == nil {
		return v
	}
	mv := v.GetMapValue()
	if mv == nil {
		return nil
	}
	return getField(&firestorepb.Document{Fields: mv.Fields}, tail)
}

// matchesFilter reports whether doc satisfies filter f.
func matchesFilter(doc *firestorepb.Document, f *firestorepb.StructuredQuery_Filter) bool {
	if f == nil {
		return true
	}
	switch ft := f.FilterType.(type) {
	case *firestorepb.StructuredQuery_Filter_FieldFilter:
		return matchesFieldFilter(doc, ft.FieldFilter)
	case *firestorepb.StructuredQuery_Filter_CompositeFilter:
		cf := ft.CompositeFilter
		switch cf.Op {
		case firestorepb.StructuredQuery_CompositeFilter_AND:
			for _, sub := range cf.Filters {
				if !matchesFilter(doc, sub) {
					return false
				}
			}
			return true
		case firestorepb.StructuredQuery_CompositeFilter_OR:
			for _, sub := range cf.Filters {
				if matchesFilter(doc, sub) {
					return true
				}
			}
			return false
		}
	case *firestorepb.StructuredQuery_Filter_UnaryFilter:
		return matchesUnaryFilter(doc, ft.UnaryFilter)
	}
	return true
}

func matchesFieldFilter(doc *firestorepb.Document, f *firestorepb.StructuredQuery_FieldFilter) bool {
	var docVal *firestorepb.Value
	if f.Field.GetFieldPath() == "__name__" {
		// __name__ is a virtual field corresponding to doc.Name (the full resource path).
		docVal = &firestorepb.Value{ValueType: &firestorepb.Value_ReferenceValue{ReferenceValue: doc.GetName()}}
	} else {
		docVal = getField(doc, f.Field.GetFieldPath())
	}
	filterVal := f.Value

	switch f.Op {
	case firestorepb.StructuredQuery_FieldFilter_EQUAL:
		return compareValues(docVal, filterVal) == 0
	case firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL:
		if docVal == nil {
			return false // field absent — Firestore excludes these from NOT_EQUAL results
		}
		if _, isNull := docVal.ValueType.(*firestorepb.Value_NullValue); isNull {
			return false // null-valued field also excluded from NOT_EQUAL per Firestore semantics
		}
		return compareValues(docVal, filterVal) != 0
	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN:
		return compareValues(docVal, filterVal) < 0
	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
		return compareValues(docVal, filterVal) <= 0
	case firestorepb.StructuredQuery_FieldFilter_GREATER_THAN:
		return compareValues(docVal, filterVal) > 0
	case firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
		return compareValues(docVal, filterVal) >= 0
	case firestorepb.StructuredQuery_FieldFilter_IN:
		av := filterVal.GetArrayValue()
		if av == nil {
			return false
		}
		for _, elem := range av.Values {
			if compareValues(docVal, elem) == 0 {
				return true
			}
		}
		return false
	case firestorepb.StructuredQuery_FieldFilter_NOT_IN:
		// Firestore's not-in semantics exclude docs where the field is absent or null.
		if docVal == nil {
			return false
		}
		if _, isNull := docVal.ValueType.(*firestorepb.Value_NullValue); isNull {
			return false
		}
		av := filterVal.GetArrayValue()
		if av == nil {
			return true
		}
		for _, elem := range av.Values {
			if compareValues(docVal, elem) == 0 {
				return false
			}
		}
		return true
	case firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
		docAv := docVal.GetArrayValue()
		if docAv == nil {
			return false
		}
		for _, elem := range docAv.Values {
			if compareValues(elem, filterVal) == 0 {
				return true
			}
		}
		return false
	case firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:
		docAv := docVal.GetArrayValue()
		filterAv := filterVal.GetArrayValue()
		if docAv == nil || filterAv == nil {
			return false
		}
		for _, fe := range filterAv.Values {
			for _, de := range docAv.Values {
				if compareValues(de, fe) == 0 {
					return true
				}
			}
		}
		return false
	}
	return false
}

func matchesUnaryFilter(doc *firestorepb.Document, f *firestorepb.StructuredQuery_UnaryFilter) bool {
	fieldRef, ok := f.OperandType.(*firestorepb.StructuredQuery_UnaryFilter_Field)
	if !ok {
		return true
	}
	v := getField(doc, fieldRef.Field.GetFieldPath())

	switch f.Op {
	case firestorepb.StructuredQuery_UnaryFilter_IS_NULL:
		if v == nil {
			return false
		}
		_, isNull := v.ValueType.(*firestorepb.Value_NullValue)
		return isNull
	case firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NULL:
		if v == nil {
			return false
		}
		_, isNull := v.ValueType.(*firestorepb.Value_NullValue)
		return !isNull
	case firestorepb.StructuredQuery_UnaryFilter_IS_NAN:
		if v == nil {
			return false
		}
		dv, ok := v.ValueType.(*firestorepb.Value_DoubleValue)
		return ok && math.IsNaN(dv.DoubleValue)
	case firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NAN:
		if v == nil {
			return false // field absent
		}
		if _, isNull := v.ValueType.(*firestorepb.Value_NullValue); isNull {
			return false // null is excluded from IS_NOT_NAN
		}
		if dv, ok := v.ValueType.(*firestorepb.Value_DoubleValue); ok {
			return !math.IsNaN(dv.DoubleValue) // NaN double excluded
		}
		return true // int, string, bool, array, map — all pass IS_NOT_NAN
	}
	return true
}

// compareValues returns -1, 0, or 1 (a < b, a == b, a > b).
// Follows Firestore type ordering: null < bool < number < timestamp < string < bytes < reference < geopoint < array < map.
func compareValues(a, b *firestorepb.Value) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Numbers (int + double) share the same type rank; compare them together.
	// NaN is ordered below all other numbers (including -Infinity) per Firestore spec.
	if isNumeric(a) && isNumeric(b) {
		af, bf := numericFloat(a), numericFloat(b)
		aNaN, bNaN := math.IsNaN(af), math.IsNaN(bf)
		if aNaN && bNaN {
			return 0
		}
		if aNaN {
			return -1
		}
		if bNaN {
			return 1
		}
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}

	ta, tb := valueTypeRank(a), valueTypeRank(b)
	if ta != tb {
		if ta < tb {
			return -1
		}
		return 1
	}

	// Same non-numeric type — full per-type comparison.
	switch av := a.ValueType.(type) {
	case *firestorepb.Value_BooleanValue:
		bv := b.GetBooleanValue()
		if av.BooleanValue == bv {
			return 0
		}
		if !av.BooleanValue {
			return -1
		}
		return 1
	case *firestorepb.Value_StringValue:
		bv := b.GetStringValue()
		if av.StringValue < bv {
			return -1
		}
		if av.StringValue > bv {
			return 1
		}
		return 0
	case *firestorepb.Value_BytesValue:
		return bytes.Compare(av.BytesValue, b.GetBytesValue())
	case *firestorepb.Value_ReferenceValue:
		// Compare references using Firestore's document-name ordering:
		// virtual IDs (__id<N>__) sort before all regular string IDs, ordered by N.
		return compareDocNames(av.ReferenceValue, b.GetReferenceValue())
	case *firestorepb.Value_TimestampValue:
		at := av.TimestampValue.AsTime()
		bt := b.GetTimestampValue().AsTime()
		if at.Before(bt) {
			return -1
		}
		if at.After(bt) {
			return 1
		}
		return 0
	case *firestorepb.Value_GeoPointValue:
		ag, bg := av.GeoPointValue, b.GetGeoPointValue()
		if ag.GetLatitude() < bg.GetLatitude() {
			return -1
		}
		if ag.GetLatitude() > bg.GetLatitude() {
			return 1
		}
		if ag.GetLongitude() < bg.GetLongitude() {
			return -1
		}
		if ag.GetLongitude() > bg.GetLongitude() {
			return 1
		}
		return 0
	case *firestorepb.Value_ArrayValue:
		aElems := av.ArrayValue.GetValues()
		bElems := b.GetArrayValue().GetValues()
		for i := 0; i < len(aElems) && i < len(bElems); i++ {
			if c := compareValues(aElems[i], bElems[i]); c != 0 {
				return c
			}
		}
		if len(aElems) < len(bElems) {
			return -1
		}
		if len(aElems) > len(bElems) {
			return 1
		}
		return 0
	case *firestorepb.Value_MapValue:
		// Vectors compare by dimension (length) first, then element-by-element.
		if isVector(a) {
			aElems := av.MapValue.Fields["value"].GetArrayValue().GetValues()
			bElems := b.GetMapValue().GetFields()["value"].GetArrayValue().GetValues()
			if len(aElems) < len(bElems) {
				return -1
			}
			if len(aElems) > len(bElems) {
				return 1
			}
			for i := range aElems {
				if c := compareValues(aElems[i], bElems[i]); c != 0 {
					return c
				}
			}
			return 0
		}
		// Plain map: compare key-by-key in sorted order.
		aFields := av.MapValue.GetFields()
		bFields := b.GetMapValue().GetFields()
		aKeys := sortedMapKeys(aFields)
		bKeys := sortedMapKeys(bFields)
		for i := 0; i < len(aKeys) && i < len(bKeys); i++ {
			if aKeys[i] < bKeys[i] {
				return -1
			}
			if aKeys[i] > bKeys[i] {
				return 1
			}
			if c := compareValues(aFields[aKeys[i]], bFields[bKeys[i]]); c != 0 {
				return c
			}
		}
		if len(aKeys) < len(bKeys) {
			return -1
		}
		if len(aKeys) > len(bKeys) {
			return 1
		}
		return 0
	}
	return 0
}

func sortedMapKeys(m map[string]*firestorepb.Value) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func isNumeric(v *firestorepb.Value) bool {
	switch v.ValueType.(type) {
	case *firestorepb.Value_IntegerValue, *firestorepb.Value_DoubleValue:
		return true
	}
	return false
}

func numericFloat(v *firestorepb.Value) float64 {
	switch vt := v.ValueType.(type) {
	case *firestorepb.Value_IntegerValue:
		return float64(vt.IntegerValue)
	case *firestorepb.Value_DoubleValue:
		return vt.DoubleValue
	}
	return 0
}

// isVector returns true if v is a Firestore VectorValue — a MapValue whose
// __type__ field equals "__vector__". Vectors sort between arrays (rank 8) and
// plain maps (rank 10), and are compared by dimension then element values.
func isVector(v *firestorepb.Value) bool {
	m, ok := v.ValueType.(*firestorepb.Value_MapValue)
	if !ok || m.MapValue == nil {
		return false
	}
	typeField, exists := m.MapValue.Fields["__type__"]
	if !exists {
		return false
	}
	sv, ok := typeField.ValueType.(*firestorepb.Value_StringValue)
	return ok && sv.StringValue == "__vector__"
}

func valueTypeRank(v *firestorepb.Value) int {
	switch v.ValueType.(type) {
	case *firestorepb.Value_NullValue:
		return 0
	case *firestorepb.Value_BooleanValue:
		return 1
	case *firestorepb.Value_IntegerValue, *firestorepb.Value_DoubleValue:
		return 2
	case *firestorepb.Value_TimestampValue:
		return 3
	case *firestorepb.Value_StringValue:
		return 4
	case *firestorepb.Value_BytesValue:
		return 5
	case *firestorepb.Value_ReferenceValue:
		return 6
	case *firestorepb.Value_GeoPointValue:
		return 7
	case *firestorepb.Value_ArrayValue:
		return 8
	case *firestorepb.Value_MapValue:
		if isVector(v) {
			return 9 // VectorValue rank: after arrays, before plain maps
		}
		return 10
	}
	return -1
}

// filterInequalityField returns the field path of the first inequality predicate
// found in filter f. Firestore implicitly orders results by this field when no
// explicit ORDER BY is present. Returns "" if no inequality filter exists.
func filterInequalityField(f *firestorepb.StructuredQuery_Filter) string {
	if f == nil {
		return ""
	}
	switch ft := f.FilterType.(type) {
	case *firestorepb.StructuredQuery_Filter_FieldFilter:
		ff := ft.FieldFilter
		switch ff.Op {
		case firestorepb.StructuredQuery_FieldFilter_LESS_THAN,
			firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
			firestorepb.StructuredQuery_FieldFilter_GREATER_THAN,
			firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL,
			firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL,
			firestorepb.StructuredQuery_FieldFilter_NOT_IN:
			return ff.Field.GetFieldPath()
		}
	case *firestorepb.StructuredQuery_Filter_CompositeFilter:
		for _, sub := range ft.CompositeFilter.Filters {
			if fp := filterInequalityField(sub); fp != "" {
				return fp
			}
		}
	}
	return ""
}

// sortDocsByOrderBy sorts docs in-place using the given ORDER BY specifications,
// applying Firestore's cross-type value ordering via compareValues.
// For __name__ ordering, virtual IDs (__id<N>__) sort before all regular IDs, by numeric value.
func sortDocsByOrderBy(docs []*firestorepb.Document, orders []*firestorepb.StructuredQuery_Order) {
	sort.SliceStable(docs, func(i, j int) bool {
		for _, order := range orders {
			fp := order.Field.GetFieldPath()
			var cmp int
			if fp == "__name__" {
				cmp = compareDocNames(docs[i].Name, docs[j].Name)
			} else {
				vi := getField(docs[i], fp)
				vj := getField(docs[j], fp)
				cmp = compareValues(vi, vj)
			}
			if order.Direction == firestorepb.StructuredQuery_DESCENDING {
				cmp = -cmp
			}
			if cmp < 0 {
				return true
			}
			if cmp > 0 {
				return false
			}
		}
		return false
	})
}

func applyOffsetLimit(docs []*firestorepb.Document, offset int32, limit *wrapperspb.Int32Value) []*firestorepb.Document {
	if offset > 0 {
		if int(offset) >= len(docs) {
			return nil
		}
		docs = docs[offset:]
	}
	if limit != nil && limit.Value > 0 && int(limit.Value) < len(docs) {
		docs = docs[:limit.Value]
	}
	return docs
}

