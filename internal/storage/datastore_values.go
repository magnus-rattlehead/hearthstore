package storage

import (
	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// normalizeEntityTimestamps rounds every stored timestamp down to microseconds,
// including excluded and nested values. Clone only when rounding is necessary.
func normalizeEntityTimestamps(entity *datastorepb.Entity) (*datastorepb.Entity, error) {
	walk := func(entity *datastorepb.Entity, normalize bool) (bool, error) {
		var values []*datastorepb.Value
		for _, value := range entity.GetProperties() {
			values = append(values, value)
		}
		changed := false
		for len(values) > 0 {
			value := values[len(values)-1]
			values = values[:len(values)-1]
			switch v := value.GetValueType().(type) {
			case *datastorepb.Value_TimestampValue:
				if v.TimestampValue == nil || v.TimestampValue.CheckValid() != nil {
					return false, status.Error(codes.InvalidArgument, "invalid property timestamp")
				}
				if v.TimestampValue.Nanos%1000 != 0 {
					changed = true
					if normalize {
						v.TimestampValue.Nanos = v.TimestampValue.Nanos / 1000 * 1000
					}
				}
			case *datastorepb.Value_EntityValue:
				for _, child := range v.EntityValue.GetProperties() {
					values = append(values, child)
				}
			case *datastorepb.Value_ArrayValue:
				values = append(values, v.ArrayValue.GetValues()...)
			}
		}
		return changed, nil
	}
	changed, err := walk(entity, false)
	if err != nil || !changed {
		return entity, err
	}
	result := proto.Clone(entity).(*datastorepb.Entity)
	_, err = walk(result, true)
	return result, err
}
