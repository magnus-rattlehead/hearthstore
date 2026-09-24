package datastore

import (
	"bytes"
	"slices"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"github.com/magnus-rattlehead/hearthstore/internal/valuecodec"
	"google.golang.org/protobuf/proto"
)

// compareValues returns -1, 0, or 1 (a < b, a == b, a > b).
func compareValues(a, b *datastorepb.Value) int {
	// Query predicates, tuple deduplication, indexes and external sorts must use
	// the same Datastore representation (not transform numeric equivalence).
	if a.GetArrayValue() != nil && b.GetArrayValue() != nil {
		return slices.CompareFunc(a.GetArrayValue().Values, b.GetArrayValue().Values, compareValues)
	}
	left, _ := storage.OrderedQueryValue(a, false)
	right, _ := storage.OrderedQueryValue(b, false)
	return bytes.Compare(left, right)
}

// compareTransformValues retains mathematical int/double equivalence for
// mutation transforms, whose contract differs from query index ordering.
func compareTransformValues(a, b *datastorepb.Value) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Numbers (int + double) share the same type rank.
	if isDsNumeric(a) && isDsNumeric(b) {
		return bytes.Compare(valuecodec.Number(a), valuecodec.Number(b))
	}

	ta, tb := dsValueTypeRank(a), dsValueTypeRank(b)
	if ta != tb {
		if ta < tb {
			return -1
		}
		return 1
	}

	// Same non-numeric type.
	switch av := a.ValueType.(type) {
	case *datastorepb.Value_BooleanValue:
		bv := b.GetBooleanValue()
		if av.BooleanValue == bv {
			return 0
		}
		if !av.BooleanValue {
			return -1
		}
		return 1
	case *datastorepb.Value_StringValue:
		bv := b.GetStringValue()
		if av.StringValue < bv {
			return -1
		}
		if av.StringValue > bv {
			return 1
		}
		return 0
	case *datastorepb.Value_TimestampValue:
		at := av.TimestampValue.AsTime()
		bt := b.GetTimestampValue().AsTime()
		if at.Before(bt) {
			return -1
		}
		if at.After(bt) {
			return 1
		}
		return 0
	case *datastorepb.Value_BlobValue:
		as, bs := string(av.BlobValue), string(b.GetBlobValue())
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	case *datastorepb.Value_EntityValue:
		// Use deterministic proto serialization for a stable equality check on nested entities.
		// proto.Marshal with Deterministic:true sorts map keys, ensuring equal entities produce equal bytes.
		opts := proto.MarshalOptions{Deterministic: true}
		ab, _ := opts.Marshal(av.EntityValue)
		bb, _ := opts.Marshal(b.GetEntityValue())
		return bytes.Compare(ab, bb)
	case *datastorepb.Value_GeoPointValue:
		ag, bg := av.GeoPointValue, b.GetGeoPointValue()
		if ag == nil && bg == nil {
			return 0
		}
		if ag == nil {
			return -1
		}
		if bg == nil {
			return 1
		}
		if ag.Latitude < bg.Latitude {
			return -1
		}
		if ag.Latitude > bg.Latitude {
			return 1
		}
		if ag.Longitude < bg.Longitude {
			return -1
		}
		if ag.Longitude > bg.Longitude {
			return 1
		}
		return 0
	case *datastorepb.Value_KeyValue:
		return compareKeys(av.KeyValue, b.GetKeyValue())
	case *datastorepb.Value_ArrayValue:
		ae, be := av.ArrayValue.GetValues(), b.GetArrayValue().GetValues()
		return slices.CompareFunc(ae, be, compareTransformValues)
	}
	return 0
}

func isDsNumeric(v *datastorepb.Value) bool {
	switch v.GetValueType().(type) {
	case *datastorepb.Value_IntegerValue, *datastorepb.Value_DoubleValue:
		return true
	}
	return false
}

func dsNumericFloat(v *datastorepb.Value) float64 {
	switch vt := v.ValueType.(type) {
	case *datastorepb.Value_IntegerValue:
		return float64(vt.IntegerValue)
	case *datastorepb.Value_DoubleValue:
		return vt.DoubleValue
	}
	return 0
}

func dsValueTypeRank(v *datastorepb.Value) int {
	switch v.ValueType.(type) {
	case *datastorepb.Value_NullValue:
		return 0
	case *datastorepb.Value_BooleanValue:
		return 1
	case *datastorepb.Value_IntegerValue, *datastorepb.Value_DoubleValue:
		return 2
	case *datastorepb.Value_TimestampValue:
		return 3
	case *datastorepb.Value_StringValue:
		return 4
	case *datastorepb.Value_BlobValue:
		return 5
	case *datastorepb.Value_KeyValue:
		return 6
	case *datastorepb.Value_GeoPointValue:
		return 7
	case *datastorepb.Value_ArrayValue:
		return 8
	case *datastorepb.Value_EntityValue:
		return 9
	}
	return -1
}

// compareKeys compares two Datastore keys. Within the same kind, integer IDs
// sort before string IDs; integer IDs compare numerically, string IDs lexicographically.
func compareKeys(a, b *datastorepb.Key) int {
	return bytes.Compare(keycodec.Ordered(a), keycodec.Ordered(b))
}
