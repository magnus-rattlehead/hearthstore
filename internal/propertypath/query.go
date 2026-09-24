package propertypath

import datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

// QueryValue resolves one of Datastore's two index interpretations. A composite
// tuple must use the same interpretation for all properties. Mutation paths use
// Get/Set/Delete instead and intentionally do not inherit these rules.
func QueryValue(entity *datastorepb.Entity, path string, canonical bool) *datastorepb.Value {
	if path == "__key__" {
		return &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: entity.GetKey()}}
	}
	for {
		head, tail, nested := path, "", false
		if canonical {
			// Java's ambiguous-path converter splits only isolated dots, not
			// dots in a consecutive run. Avoid regex allocations on hot paths.
			for i := 0; i < len(path); i++ {
				if path[i] == '.' && (i == 0 || path[i-1] != '.') && (i+1 == len(path) || path[i+1] != '.') {
					head, tail, nested = path[:i], path[i+1:], true
					break
				}
			}
		}
		value := entity.GetProperties()[head]
		if value == nil || value.ExcludeFromIndexes {
			return nil
		}
		if !nested {
			if value.GetEntityValue() != nil {
				return nil // Embedded containers are not scalar index values.
			}
			return value
		}
		entity, path = value.GetEntityValue(), tail
	}
}

// QueryValues visits indexed scalar candidates until visit returns false.
// Equal candidates can occur in both interpretations; index keys deduplicate them.
func QueryValues(entity *datastorepb.Entity, path string, visit func(*datastorepb.Value) bool) {
	queryValues(entity, path, false, func(value *datastorepb.Value, _ bool) bool { return visit(value) })
}

// QueryValuesByInterpretation retains provenance for residual query predicates.
// Even a shared value may qualify in only one of the two interpretations.
func QueryValuesByInterpretation(entity *datastorepb.Entity, path string, visit func(*datastorepb.Value, bool)) {
	queryValues(entity, path, true, func(value *datastorepb.Value, canonical bool) bool {
		visit(value, canonical)
		return true
	})
}

func queryValues(entity *datastorepb.Entity, path string, both bool, visit func(*datastorepb.Value, bool) bool) {
	literal := QueryValue(entity, path, false)
	nested := QueryValue(entity, path, true)
	for mode, value := range [2]*datastorepb.Value{literal, nested} {
		if value == nil {
			continue
		}
		if array := value.GetArrayValue(); array != nil {
			for _, item := range array.Values {
				if item != nil && !item.ExcludeFromIndexes && item.GetEntityValue() == nil {
					if !visit(item, mode == 1) {
						return
					}
				}
			}
		} else {
			if !visit(value, mode == 1) {
				return
			}
		}
		if !both && literal == nested {
			break
		}
	}
}
