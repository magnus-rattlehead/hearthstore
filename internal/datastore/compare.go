package datastore

import (
	"bytes"
	"cmp"
	"slices"
	"strings"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
)

// compareValues returns -1, 0, or 1 (a < b, a == b, a > b).
// Follows Datastore type ordering: null < bool < number < timestamp < string < blob < key < geo < array < entity.
func compareValues(a, b *datastorepb.Value) int {
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
		af, bf := dsNumericFloat(a), dsNumericFloat(b)
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
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
		return slices.CompareFunc(ae, be, compareValues)
	}
	return 0
}

func isDsNumeric(v *datastorepb.Value) bool {
	switch v.ValueType.(type) {
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
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	ap, bp := a.GetPartitionId(), b.GetPartitionId()
	if c := strings.Compare(ap.GetProjectId(), bp.GetProjectId()); c != 0 {
		return c
	}
	if c := strings.Compare(ap.GetNamespaceId(), bp.GetNamespaceId()); c != 0 {
		return c
	}
	apes, bpes := a.GetPath(), b.GetPath()
	return slices.CompareFunc(apes, bpes, func(ae, be *datastorepb.Key_PathElement) int {
		return cmp.Or(strings.Compare(ae.GetKind(), be.GetKind()), comparePathElementID(ae, be))
	})
}

func comparePathElementID(a, b *datastorepb.Key_PathElement) int {
	ai, aIsInt := a.GetIdType().(*datastorepb.Key_PathElement_Id)
	bi, bIsInt := b.GetIdType().(*datastorepb.Key_PathElement_Id)
	an, aIsName := a.GetIdType().(*datastorepb.Key_PathElement_Name)
	bn, bIsName := b.GetIdType().(*datastorepb.Key_PathElement_Name)
	switch {
	case aIsInt && bIsInt:
		if ai.Id < bi.Id {
			return -1
		}
		if ai.Id > bi.Id {
			return 1
		}
		return 0
	case aIsInt && bIsName:
		return -1 // integer IDs sort before string IDs
	case aIsName && bIsInt:
		return 1
	case aIsName && bIsName:
		return strings.Compare(an.Name, bn.Name)
	}
	return 0
}
