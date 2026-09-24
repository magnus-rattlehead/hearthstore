package datastore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"net/http"
	"strings"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"

	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
	"github.com/magnus-rattlehead/hearthstore/internal/propertypath"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func compareQueryRows(q *datastorepb.Query, a, b *storage.DsEntityRow) int {
	for _, order := range q.Order {
		property := order.Property.GetName()
		comparison := compareValues(getProp(a.Entity, property), getProp(b.Entity, property))
		if property == "__key__" {
			comparison = compareKeys(a.Entity.Key, b.Entity.Key)
		}
		if comparison != 0 {
			if order.Direction == datastorepb.PropertyOrder_DESCENDING {
				return -comparison
			}
			return comparison
		}
	}
	return compareKeys(a.Entity.Key, b.Entity.Key)
}

// compositeEqualityKeysCovering requires one fixed tuple per entity. Broader
// spans can contain multiple array entries and still need source-based deduplication.
func compositeEqualityKeysCovering(q *datastorepb.Query, idx *storage.DsCompositeIndex) bool {
	if !isKeysOnly(q) || len(q.DistinctOn) != 0 || idx == nil || idx.State != storage.DsIndexReady || idx.Ancestor || len(idx.Properties) == 0 {
		return false
	}
	equality := make(map[string]bool, len(idx.Properties))
	var collect func(*datastorepb.Filter) bool
	collect = func(filter *datastorepb.Filter) bool {
		if pf := filter.GetPropertyFilter(); pf != nil {
			name := pf.Property.GetName()
			if pf.Op != datastorepb.PropertyFilter_EQUAL || equality[name] || strings.Contains(name, ".") {
				return false
			}
			switch pf.Value.GetValueType().(type) {
			case nil, *datastorepb.Value_ArrayValue, *datastorepb.Value_EntityValue:
				return false
			}
			equality[name] = true
			return true
		}
		cf := filter.GetCompositeFilter()
		if cf == nil || cf.Op != datastorepb.CompositeFilter_AND {
			return false
		}
		for _, child := range cf.Filters {
			if !collect(child) {
				return false
			}
		}
		return true
	}
	if !collect(q.Filter) || len(equality) != len(idx.Properties) {
		return false
	}
	for _, property := range idx.Properties {
		if !equality[property.Name] {
			return false
		}
	}
	for _, order := range q.Order {
		if !equality[order.Property.GetName()] {
			return false
		}
	}
	return true
}

func builtinQueryAccess(q *datastorepb.Query) (property string, reverse, ok bool) {
	if q == nil || len(q.Kind) == 0 {
		return "", false, false
	}
	properties := make(map[string]struct{})
	allEquality := true
	hasPropertyFilter := false
	var collect func(*datastorepb.Filter) bool
	collect = func(filter *datastorepb.Filter) bool {
		if filter == nil {
			return true
		}
		switch typed := filter.FilterType.(type) {
		case *datastorepb.Filter_PropertyFilter:
			pf := typed.PropertyFilter
			if pf.Op == datastorepb.PropertyFilter_HAS_ANCESTOR && pf.Property.GetName() == "__key__" {
				return true
			}
			switch pf.Op {
			case datastorepb.PropertyFilter_EQUAL,
				datastorepb.PropertyFilter_LESS_THAN,
				datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL,
				datastorepb.PropertyFilter_GREATER_THAN,
				datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL,
				datastorepb.PropertyFilter_NOT_EQUAL,
				datastorepb.PropertyFilter_IN,
				datastorepb.PropertyFilter_NOT_IN:
			default:
				return false
			}
			properties[pf.Property.GetName()] = struct{}{}
			hasPropertyFilter = true
			allEquality = allEquality && pf.Op == datastorepb.PropertyFilter_EQUAL
			return true
		case *datastorepb.Filter_CompositeFilter:
			for _, child := range typed.CompositeFilter.Filters {
				if !collect(child) {
					return false
				}
			}
			return true
		default:
			return false
		}
	}
	if !collect(q.Filter) {
		return "", false, false
	}
	orders := builtinAccessOrder(q.Order)
	for _, order := range orders {
		properties[order.Property.GetName()] = struct{}{}
	}
	for _, projection := range q.Projection {
		if name := projection.Property.GetName(); name != "__key__" {
			properties[name] = struct{}{}
		}
	}
	for _, distinct := range q.DistinctOn {
		if name := distinct.GetName(); name != "__key__" {
			properties[name] = struct{}{}
		}
	}
	if len(properties) == 0 {
		return "__key__", false, true
	}
	if len(properties) != 1 {
		return "", false, false
	}
	for property = range properties {
	}
	if len(orders) == 1 && (!hasPropertyFilter || !allEquality) {
		reverse = orders[0].Direction == datastorepb.PropertyOrder_DESCENDING
	}
	return property, reverse, true
}

// A built-in scan reverses the complete [property,key] tuple together.
func builtinAccessOrder(orders []*datastorepb.PropertyOrder) []*datastorepb.PropertyOrder {
	if len(orders) == 2 && orders[1].Property.GetName() == "__key__" &&
		(orders[0].Direction == datastorepb.PropertyOrder_DESCENDING) == (orders[1].Direction == datastorepb.PropertyOrder_DESCENDING) {
		return orders[:1]
	}
	return orders
}

type builtinUnionBranch struct {
	property string
	filter   *datastorepb.Filter
	reverse  bool
}

// equalityUnionAccess selects only key-ordered point streams; ranges and
// independent array witnesses retain their existing execution.
// Full-entity execution can consume a dense key prefix before opening streams.
func equalityUnionAccess(q *datastorepb.Query, condition *compiledCondition, allowance int, cursor *storage.CursorPayload) []*datastorepb.PropertyFilter {
	if len(q.Kind) != 1 || isMetadataKind(q.Kind[0].Name) || len(q.Order) != 0 || len(q.DistinctOn) != 0 || len(q.Projection) > 0 && !isKeysOnly(q) || q.Limit != nil && q.Limit.Value == 0 || !condition.accessPlanningFits(allowance) || condition.needsEntityWitnesses() {
		return nil
	}
	compatible := func(c *storage.CursorPayload) bool {
		return c == nil || c.I == "builtin:__key__" && c.G == 1 && c.O == 0
	}
	if !compatible(cursor) {
		return nil
	}
	if len(q.EndCursor) > 0 {
		end, ok := decodeCursorFull(q.EndCursor)
		if !ok || !compatible(&end) {
			return nil
		}
	}
	if q.Filter.GetCompositeFilter().GetOp() != datastorepb.CompositeFilter_OR {
		return nil
	}
	var branches []*datastorepb.PropertyFilter
	properties := map[string]bool{}
	var collect func(*datastorepb.Filter) bool
	collect = func(filter *datastorepb.Filter) bool {
		if pf := filter.GetPropertyFilter(); pf != nil {
			name := pf.Property.GetName()
			if pf.Op != datastorepb.PropertyFilter_EQUAL || name == "__key__" || strings.Contains(name, ".") {
				return false
			}
			switch pf.Value.GetValueType().(type) {
			case nil, *datastorepb.Value_ArrayValue, *datastorepb.Value_EntityValue:
				return false
			}
			properties[name] = true
			branches = append(branches, pf)
			return true
		}
		cf := filter.GetCompositeFilter()
		if cf == nil || cf.Op != datastorepb.CompositeFilter_OR {
			return false
		}
		for _, child := range cf.Filters {
			if !collect(child) {
				return false
			}
		}
		return true
	}
	if !collect(q.Filter) || len(properties) < 2 {
		return nil
	}
	return branches
}

func builtinUnionAccess(q *datastorepb.Query) ([]builtinUnionBranch, bool) {
	if q == nil || len(q.EndCursor) > 0 {
		return nil, false
	}
	composite, ok := q.Filter.GetFilterType().(*datastorepb.Filter_CompositeFilter)
	if !ok || composite.CompositeFilter.Op != datastorepb.CompositeFilter_OR {
		return nil, false
	}
	branches := make([]builtinUnionBranch, 0, len(composite.CompositeFilter.Filters))
	for _, filter := range composite.CompositeFilter.Filters {
		// Access inspection is read-only; do not clone the entire condition once
		// per branch. Only these query fields participate in built-in selection.
		branchQuery := &datastorepb.Query{Kind: q.Kind, Filter: filter, Order: q.Order, Projection: q.Projection, DistinctOn: q.DistinctOn}
		property, reverse, supported := builtinQueryAccess(branchQuery)
		if !supported {
			return nil, false
		}
		branches = append(branches, builtinUnionBranch{property: property, filter: filter, reverse: reverse})
	}
	return branches, true
}

func (s *Server) handleRunQuery(w http.ResponseWriter, r *http.Request, project string) {
	var req datastorepb.RunQueryRequest
	if !readProtoJSON(w, r.Body, &req) {
		return
	}
	if req.ProjectId == "" {
		req.ProjectId = project
	}
	start := time.Now()
	SetHTTPDetails(r.Context(), DSQueryDetails(&req))
	resp, err := s.grpc.RunQuery(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	MergeHTTPDetails(r.Context(), DSQueryResponseDetails(resp, time.Since(start)))
	writeProtoJSON(w, resp)
}

// evalScalarOp evaluates comparison operators over non-array values.
func evalScalarOp(a, b *datastorepb.Value, op datastorepb.PropertyFilter_Operator) bool {
	return scalarComparisonMatches(compareValues(a, b), op)
}

func scalarComparisonMatches(comparison int, op datastorepb.PropertyFilter_Operator) bool {
	switch op {
	case datastorepb.PropertyFilter_EQUAL:
		return comparison == 0
	case datastorepb.PropertyFilter_NOT_EQUAL:
		return comparison != 0
	case datastorepb.PropertyFilter_LESS_THAN:
		return comparison < 0
	case datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL:
		return comparison <= 0
	case datastorepb.PropertyFilter_GREATER_THAN:
		return comparison > 0
	case datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
		return comparison >= 0
	}
	return false
}

func matchesPropertyValue(val *datastorepb.Value, f *datastorepb.PropertyFilter) bool {
	return matchesPropertyValueUsing(val, f, compareValues)
}

func matchesPropertyValueUsing(val *datastorepb.Value, f *datastorepb.PropertyFilter, compare func(*datastorepb.Value, *datastorepb.Value) int) bool {
	if val == nil {
		return false // Missing/excluded properties never contribute index entries.
	}
	filterVal := f.Value

	// Scalar comparisons against array properties match any element.
	if av, ok := val.GetValueType().(*datastorepb.Value_ArrayValue); ok && av.ArrayValue != nil {
		if f.Op == datastorepb.PropertyFilter_EQUAL {
			if _, wholeArray := filterVal.GetValueType().(*datastorepb.Value_ArrayValue); wholeArray {
				return compare(val, filterVal) == 0
			}
		}
		elems := av.ArrayValue.Values
		switch f.Op {
		case datastorepb.PropertyFilter_IN:
			filterAv := filterVal.GetArrayValue()
			if filterAv == nil {
				return false
			}
			for _, elem := range elems {
				if elem == nil || elem.ExcludeFromIndexes {
					continue
				}
				for _, fe := range filterAv.Values {
					if compare(elem, fe) == 0 {
						return true
					}
				}
			}
			return false
		case datastorepb.PropertyFilter_NOT_IN:
			filterAv := filterVal.GetArrayValue()
			if filterAv == nil {
				return true
			}
			for _, elem := range elems {
				if elem == nil || elem.ExcludeFromIndexes {
					continue
				}
				excluded := false
				for _, fe := range filterAv.Values {
					if compare(elem, fe) == 0 {
						excluded = true
						break
					}
				}
				if !excluded {
					return true
				}
			}
			return false
		case datastorepb.PropertyFilter_HAS_ANCESTOR:
			return true
		default:
			for _, elem := range elems {
				if elem != nil && !elem.ExcludeFromIndexes && scalarComparisonMatches(compare(elem, filterVal), f.Op) {
					return true
				}
			}
			return false
		}
	}

	switch f.Op {
	case datastorepb.PropertyFilter_IN:
		av := filterVal.GetArrayValue()
		if av == nil {
			return false
		}
		for _, elem := range av.Values {
			if compare(val, elem) == 0 {
				return true
			}
		}
		return false
	case datastorepb.PropertyFilter_NOT_IN:
		av := filterVal.GetArrayValue()
		if av == nil {
			return true
		}
		for _, elem := range av.Values {
			if compare(val, elem) == 0 {
				return false
			}
		}
		return true
	case datastorepb.PropertyFilter_HAS_ANCESTOR:
		return true
	default:
		return scalarComparisonMatches(compare(val, filterVal), f.Op)
	}
}

func matchesKeyFilter(entity *datastorepb.Entity, f *datastorepb.PropertyFilter) bool {
	key := entity.GetKey()
	if key == nil {
		return false
	}
	if f.Op == datastorepb.PropertyFilter_HAS_ANCESTOR {
		ancestor := f.Value.GetKeyValue()
		return len(ancestor.GetPath()) > 0 && bytes.HasPrefix(keycodec.Ordered(key), keycodec.Ordered(ancestor))
	}
	if f.Op == datastorepb.PropertyFilter_IN || f.Op == datastorepb.PropertyFilter_NOT_IN {
		found := false
		for _, value := range f.Value.GetArrayValue().GetValues() {
			if other := value.GetKeyValue(); other != nil && compareKeys(key, other) == 0 {
				found = true
				break
			}
		}
		if f.Op == datastorepb.PropertyFilter_NOT_IN {
			return !found
		}
		return found
	}
	if f.Value.GetKeyValue() == nil {
		return false
	}
	return evalScalarOp(&datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: key}}, f.Value, f.Op)
}

func extractAncestorPath(f *datastorepb.Filter, project, database, namespace string) string {
	if f == nil {
		return ""
	}
	switch ft := f.FilterType.(type) {
	case *datastorepb.Filter_PropertyFilter:
		pf := ft.PropertyFilter
		if pf.Op == datastorepb.PropertyFilter_HAS_ANCESTOR && pf.Property.GetName() == "__key__" {
			ancestorKey := pf.Value.GetKeyValue()
			if ancestorKey == nil {
				return ""
			}
			_, _, _, _, _, path := keyComponents(ancestorKey)
			return path
		}
	case *datastorepb.Filter_CompositeFilter:
		if ft.CompositeFilter.Op != datastorepb.CompositeFilter_AND {
			return ""
		}
		for _, sub := range ft.CompositeFilter.Filters {
			if p := extractAncestorPath(sub, project, database, namespace); p != "" {
				return p
			}
		}
	}
	return ""
}

func getProp(entity *datastorepb.Entity, name string) *datastorepb.Value {
	if value := propertypath.QueryValue(entity, name, false); value != nil {
		return value
	}
	return propertypath.QueryValue(entity, name, true)
}

func isKeysOnly(q *datastorepb.Query) bool {
	return len(q.Projection) == 1 && q.Projection[0].Property.GetName() == "__key__"
}

func projectionFields(q *datastorepb.Query) []string {
	if len(q.Projection) == 0 {
		return nil
	}
	names := make([]string, 0, len(q.Projection))
	for _, p := range q.Projection {
		n := p.Property.GetName()
		if n != "__key__" {
			names = append(names, n)
		}
	}
	return names
}

// distinctKey returns a string key representing the distinct_on field values of an entity.
func distinctKey(entity *datastorepb.Entity, distinctOn []*datastorepb.PropertyReference) string {
	var tuple []byte
	for _, ref := range distinctOn {
		v := getProp(entity, ref.GetName())
		encoded, _ := storage.OrderedQueryValue(v, false)
		tuple = binary.AppendUvarint(tuple, uint64(len(encoded)))
		tuple = append(tuple, encoded...)
	}
	hash := sha256.Sum256(tuple)
	return string(hash[:])
}

func queryFingerprint(project, database, namespace string, query *datastorepb.Query) []byte {
	copy := proto.Clone(query).(*datastorepb.Query)
	copy.StartCursor, copy.EndCursor, copy.Limit, copy.Offset = nil, nil, nil, 0
	// An omitted direction has the documented ASCENDING default. Hash its
	// meaning, so equivalent explicit orders and reverse cursors remain usable.
	for _, order := range copy.Order {
		if order.Direction == datastorepb.PropertyOrder_DIRECTION_UNSPECIFIED {
			order.Direction = datastorepb.PropertyOrder_ASCENDING
		}
	}
	encoded, _ := proto.MarshalOptions{Deterministic: true}.Marshal(copy)
	partition, _ := proto.Marshal(&datastorepb.PartitionId{ProjectId: project, DatabaseId: database, NamespaceId: namespace})
	hash := sha256.Sum256(append(append(partition, []byte("\x00normalized-witness-order-v3\x00")...), encoded...))
	return hash[:]
}
