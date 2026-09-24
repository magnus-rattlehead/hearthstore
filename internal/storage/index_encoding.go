package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode/utf8"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"

	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
	"github.com/magnus-rattlehead/hearthstore/internal/propertypath"
)

func dsProperty(e *datastorepb.Entity, path string) *datastorepb.Value {
	return propertypath.QueryValue(e, path, true)
}
func orderedValue(v *datastorepb.Value, desc bool) ([]byte, bool) {
	if v == nil {
		return nil, false
	}
	var raw []byte
	switch x := v.ValueType.(type) {
	case *datastorepb.Value_NullValue:
		raw = []byte{0}
	case *datastorepb.Value_BooleanValue:
		raw = []byte{2, 0}
		if x.BooleanValue {
			raw[1] = 1
		}
	case *datastorepb.Value_IntegerValue:
		raw = orderedInteger(x.IntegerValue)
	case *datastorepb.Value_DoubleValue:
		raw = orderedNumber(x.DoubleValue)
	case *datastorepb.Value_TimestampValue:
		if x.TimestampValue == nil || x.TimestampValue.CheckValid() != nil {
			return nil, false
		}
		raw = orderedInteger(x.TimestampValue.Seconds*1_000_000 + int64(x.TimestampValue.Nanos)/1_000)
	case *datastorepb.Value_StringValue:
		raw = orderedBytes(4, []byte(x.StringValue))
	case *datastorepb.Value_BlobValue:
		raw = orderedBytes(3, x.BlobValue)
	case *datastorepb.Value_KeyValue:
		raw = orderedDatastoreKey(x.KeyValue)
	case *datastorepb.Value_GeoPointValue:
		if x.GeoPointValue == nil {
			return nil, false
		}
		lat := orderedNumber(x.GeoPointValue.Latitude)
		lng := orderedNumber(x.GeoPointValue.Longitude)
		raw = append([]byte{6}, lat[1:]...)
		raw = append(raw, lng[1:]...)
	case *datastorepb.Value_EntityValue:
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(x.EntityValue)
		if err != nil {
			return nil, false
		}
		raw = orderedBytes(9, encoded)
	case *datastorepb.Value_ArrayValue:
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(x.ArrayValue)
		if err != nil {
			return nil, false
		}
		raw = orderedBytes(10, encoded)
	default:
		return nil, false
	}
	if desc {
		for i := range raw {
			raw[i] = ^raw[i]
		}
	}
	return raw, true
}

// OrderedQueryValue shares the index ordering with bounded external query sorts.
func OrderedQueryValue(value *datastorepb.Value, descending bool) ([]byte, bool) {
	return orderedValue(value, descending)
}

// ValidOrderedQueryValue checks a scalar logical cursor component independently
// of its physical index witness. The caller removes descending inversion first.
func ValidOrderedQueryValue(raw []byte) bool {
	if len(raw) == 0 {
		return false
	}
	switch raw[0] {
	case 0:
		return len(raw) == 1
	case 1, 5:
		return len(raw) == 9
	case 2:
		return len(raw) == 2 && raw[1] <= 1
	case 3, 4:
		value, rest, ok := takeOrderedBytes(raw[1:])
		return ok && len(rest) == 0 && (raw[0] == 3 || utf8.Valid(value))
	case 6:
		return len(raw) == 17
	case 7:
		rest := raw[1:]
		for i := 0; i < 3; i++ {
			value, tail, ok := takeOrderedBytes(rest)
			if !ok || !utf8.Valid(value) {
				return false
			}
			rest = tail
		}
		if len(rest) == 0 {
			return false
		}
		for len(rest) > 0 {
			kind, tail, ok := takeOrderedBytes(rest)
			if !ok || !utf8.Valid(kind) || len(tail) == 0 {
				return false
			}
			switch tail[0] {
			case 0:
				rest = tail[1:]
			case 1:
				if len(tail) < 9 {
					return false
				}
				rest = tail[9:]
			case 2:
				name, next, ok := takeOrderedBytes(tail[1:])
				if !ok || !utf8.Valid(name) {
					return false
				}
				rest = next
			default:
				return false
			}
		}
		return true
	default:
		return false
	}
}

func takeOrderedBytes(raw []byte) ([]byte, []byte, bool) {
	component, rest, ok := takeIndexComponent(raw)
	if !ok {
		return nil, nil, false
	}
	return bytes.ReplaceAll(component, []byte{0, 255}, []byte{0}), rest, true
}

// CompositeBoundsSupported reports whether a leading range can bound the scan.
func CompositeBoundsSupported(index DsCompositeIndex, filter *datastorepb.Filter) bool {
	_, ok := buildCompositeBounds(index, filter, false)
	return ok
}

func orderedDatastoreKey(key *datastorepb.Key) []byte {
	return append([]byte{7}, keycodec.Ordered(key)...)
}

func orderedNumber(value float64) []byte {
	// Datastore's floating-point type follows strings. NaN sorts last within
	// this type; both signed zeroes represent the same indexed value.
	if math.IsNaN(value) {
		value = math.NaN()
	} else if value == 0 {
		value = 0
	}
	bits := math.Float64bits(value)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	raw := make([]byte, 9)
	raw[0] = 5
	binary.BigEndian.PutUint64(raw[1:], bits)
	return raw
}

func orderedInteger(value int64) []byte {
	raw := make([]byte, 9)
	raw[0] = 1
	binary.BigEndian.PutUint64(raw[1:], uint64(value)^(1<<63))
	return raw
}

func orderedBytes(tag byte, value []byte) []byte {
	raw := make([]byte, 1, len(value)+3)
	raw[0] = tag
	for _, b := range value {
		if b == 0 {
			raw = append(raw, 0, 255)
		} else {
			raw = append(raw, b)
		}
	}
	return append(raw, 0, 0)
}
func dsCompositeEntryKeys(idx DsCompositeIndex, generation int64, database, namespace, path string, e *datastorepb.Entity) ([][]byte, error) {
	var keys [][]byte
	err := visitCompositeEntryKeys(context.Background(), idx, generation, database, namespace, path, e, func(key []byte) error {
		keys = append(keys, key)
		return nil
	})
	return keys, err
}

func compositeHasDottedProperty(idx DsCompositeIndex) bool {
	for _, property := range idx.Properties {
		if strings.Contains(property.Name, ".") {
			return true
		}
	}
	return false
}

// compositeIndexDomains excludes and deduplicates values before computing a
// product. A missing later property makes the whole interpretation empty.
func compositeIndexDomains(ctx context.Context, idx DsCompositeIndex, e *datastorepb.Entity, canonical bool, path string) ([][][]byte, error) {
	work := QueryWorkFromContext(ctx)
	domains := make([][][]byte, len(idx.Properties))
	for i, property := range idx.Properties {
		value := propertypath.QueryValue(e, property.Name, canonical)
		if value == nil || value.ExcludeFromIndexes {
			return nil, nil
		}
		values := []*datastorepb.Value{value}
		if array := value.GetArrayValue(); array != nil {
			values = array.Values
		}
		for _, candidate := range values {
			if err := work.Checkpoint(ctx); err != nil {
				return nil, err
			}
			if candidate == nil || candidate.ExcludeFromIndexes || candidate.GetEntityValue() != nil {
				continue
			}
			raw, ok := orderedValue(candidate, property.Desc)
			if !ok {
				return nil, fmt.Errorf("composite index %s cannot encode property %s on %s", idx.ID, property.Name, path)
			}
			domains[i] = append(domains[i], appendIndexComponent(nil, raw))
		}
		var compareErr error
		slices.SortFunc(domains[i], func(a, b []byte) int {
			if compareErr == nil {
				compareErr = work.Checkpoint(ctx)
			}
			if compareErr != nil {
				return 0
			}
			work.Charge(WorkComparisons, 1)
			return bytes.Compare(a, b)
		})
		if compareErr != nil {
			return nil, compareErr
		}
		domains[i] = slices.CompactFunc(domains[i], bytes.Equal)
		if len(domains[i]) == 0 {
			return nil, nil
		}
	}
	return domains, nil
}

// visitCompositeEntryKeys retains domains and one tuple, not their Cartesian
// product. The two whole interpretations are unioned without mixing properties.
func visitCompositeEntryKeys(ctx context.Context, idx DsCompositeIndex, generation int64, database, namespace, path string, e *datastorepb.Entity, visit func([]byte) error) error {
	work := QueryWorkFromContext(ctx)
	literal, err := compositeIndexDomains(ctx, idx, e, false, path)
	if err != nil {
		return err
	}
	var canonical [][][]byte
	if compositeHasDottedProperty(idx) {
		canonical, err = compositeIndexDomains(ctx, idx, e, true, path)
		if err != nil {
			return err
		}
	}
	scopes := []string{""}
	if idx.Ancestor {
		scopes = indexScopes(path)[1:]
	}
	// |A union B| = |A| + |B| - |A intersection B|. Products of sorted
	// domain intersections count identical entries without allocating any keys.
	count := func(domains [][][]byte) int {
		if domains == nil {
			return 0
		}
		product := len(scopes)
		for _, domain := range domains {
			if product > maxEntityIndexedValues/len(domain) {
				return maxEntityIndexedValues + 1
			}
			product *= len(domain)
		}
		return product
	}
	a, b := count(literal), count(canonical)
	invalid := func() error {
		return fmt.Errorf("composite index %s produces more than %d entries for %s", idx.ID, maxEntityIndexedValues, path)
	}
	if a > maxEntityIndexedValues || b > maxEntityIndexedValues {
		return invalid()
	}
	intersection := 0
	if a > 0 && b > 0 {
		intersection = len(scopes)
		for i, domain := range literal {
			common := 0
			for j, k := 0, 0; j < len(domain) && k < len(canonical[i]); {
				if err := work.Checkpoint(ctx); err != nil {
					return err
				}
				work.Charge(WorkComparisons, 1)
				comparison := bytes.Compare(domain[j], canonical[i][k])
				if comparison <= 0 {
					j++
				}
				if comparison >= 0 {
					k++
				}
				if comparison == 0 {
					common++
				}
			}
			intersection *= common
		}
	}
	if a+b-intersection > maxEntityIndexedValues {
		return invalid()
	}
	suffix := append(appendIndexComponent(nil, keycodec.Ordered(e.Key)), enc(path)...)
	for interpretation, domains := range [][][][]byte{literal, canonical} {
		if domains == nil {
			continue
		}
		for _, scope := range scopes {
			base := append(compositeScanBase(idx, database, namespace, generation, scope), '/')
			positions := make([]int, len(domains))
			for {
				if err := work.Checkpoint(ctx); err != nil {
					return err
				}
				duplicate := interpretation == 1 && literal != nil
				key := append([]byte(nil), base...)
				for i, domain := range domains {
					component := domain[positions[i]]
					key = append(key, component...)
					if duplicate {
						_, duplicate = slices.BinarySearchFunc(literal[i], component, bytes.Compare)
					}
				}
				if !duplicate {
					if err := visit(append(key, suffix...)); err != nil {
						return err
					}
				}
				i := len(positions) - 1
				for ; i >= 0; i-- {
					positions[i]++
					if positions[i] < len(domains[i]) {
						break
					}
					positions[i] = 0
				}
				if i < 0 {
					break
				}
			}
		}
	}
	return ctx.Err()
}
func DsCompositePrefix(idx DsCompositeIndex, equality map[string]*datastorepb.Value) []byte {
	var out []byte
	for _, p := range idx.Properties {
		v, ok := equality[p.Name]
		if !ok {
			break
		}
		raw, valid := orderedValue(v, p.Desc)
		if !valid {
			break
		}
		out = appendIndexComponent(out, raw)
	}
	return out
}

type compositeScanBounds struct {
	prefix         []byte
	lower          []byte
	upper          []byte
	lowerInclusive bool
	upperInclusive bool
}

// compositeQueryBounds separates the leading physical scan from tuple
// qualification. Secondary ranges must also constrain array representatives.
type compositeQueryBounds struct {
	compositeScanBounds
	properties map[string]compositeScanBounds
}

func buildCompositeScanBounds(idx DsCompositeIndex, filter *datastorepb.Filter) (compositeScanBounds, bool) {
	bounds, ok := buildCompositeBounds(idx, filter, true)
	return bounds.compositeScanBounds, ok
}

func buildCompositeBounds(idx DsCompositeIndex, filter *datastorepb.Filter, exact bool) (compositeQueryBounds, bool) {
	constraints := make(map[string][]*datastorepb.PropertyFilter)
	var collect func(*datastorepb.Filter) bool
	collect = func(f *datastorepb.Filter) bool {
		if f == nil {
			return true
		}
		switch x := f.FilterType.(type) {
		case *datastorepb.Filter_PropertyFilter:
			pf := x.PropertyFilter
			if pf.Property.GetName() == "__key__" && pf.Op == datastorepb.PropertyFilter_HAS_ANCESTOR {
				return true
			}
			switch pf.Op {
			case datastorepb.PropertyFilter_EQUAL,
				datastorepb.PropertyFilter_LESS_THAN,
				datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL,
				datastorepb.PropertyFilter_GREATER_THAN,
				datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
				constraints[pf.Property.GetName()] = append(constraints[pf.Property.GetName()], pf)
				return true
			default:
				return false
			}
		case *datastorepb.Filter_CompositeFilter:
			if x.CompositeFilter.Op != datastorepb.CompositeFilter_AND {
				return false
			}
			for _, child := range x.CompositeFilter.Filters {
				if !collect(child) {
					return false
				}
			}
			return true
		default:
			return false
		}
	}
	if !collect(filter) {
		return compositeQueryBounds{}, false
	}

	equality := make(map[string]*datastorepb.Value)
	consumed := make(map[string]bool)
	rangeProperty := ""
	for _, property := range idx.Properties {
		filters := constraints[property.Name]
		if len(filters) == 0 {
			break
		}
		if len(filters) == 1 && filters[0].Op == datastorepb.PropertyFilter_EQUAL && rangeProperty == "" {
			equality[property.Name] = filters[0].Value
			consumed[property.Name] = true
			continue
		}
		for _, pf := range filters {
			if pf.Op == datastorepb.PropertyFilter_EQUAL {
				return compositeQueryBounds{}, false
			}
		}
		rangeProperty = property.Name
		consumed[property.Name] = true
		break
	}
	for property := range constraints {
		if exact && !consumed[property] {
			return compositeQueryBounds{}, false
		}
	}

	bounds := compositeQueryBounds{compositeScanBounds: compositeScanBounds{prefix: DsCompositePrefix(idx, equality)}, properties: make(map[string]compositeScanBounds)}
	for _, property := range idx.Properties {
		var propertyBounds compositeScanBounds
		for _, pf := range constraints[property.Name] {
			raw, ok := orderedValue(pf.Value, property.Desc)
			if !ok {
				return compositeQueryBounds{}, false
			}
			encoded := encodeIndexComponent(raw)
			op := pf.Op
			if property.Desc {
				switch op {
				case datastorepb.PropertyFilter_LESS_THAN:
					op = datastorepb.PropertyFilter_GREATER_THAN
				case datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL:
					op = datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL
				case datastorepb.PropertyFilter_GREATER_THAN:
					op = datastorepb.PropertyFilter_LESS_THAN
				case datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
					op = datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL
				}
			}
			switch op {
			case datastorepb.PropertyFilter_EQUAL:
				propertyBounds.setLower(encoded, true)
				propertyBounds.setUpper(encoded, true)
			case datastorepb.PropertyFilter_GREATER_THAN:
				propertyBounds.setLower(encoded, false)
			case datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
				propertyBounds.setLower(encoded, true)
			case datastorepb.PropertyFilter_LESS_THAN:
				propertyBounds.setUpper(encoded, false)
			case datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL:
				propertyBounds.setUpper(encoded, true)
			}
		}
		bounds.properties[property.Name] = propertyBounds
		if property.Name == rangeProperty {
			prefix := bounds.prefix
			bounds.compositeScanBounds = propertyBounds
			bounds.prefix = prefix
		}
	}
	return bounds, true
}

func (b *compositeScanBounds) setLower(value []byte, inclusive bool) {
	cmp := bytes.Compare(value, b.lower)
	if b.lower == nil || cmp > 0 || (cmp == 0 && !inclusive) {
		b.lower = value
		b.lowerInclusive = inclusive
	}
}

func (b *compositeScanBounds) setUpper(value []byte, inclusive bool) {
	cmp := bytes.Compare(value, b.upper)
	if b.upper == nil || cmp < 0 || (cmp == 0 && !inclusive) {
		b.upper = value
		b.upperInclusive = inclusive
	}
}
