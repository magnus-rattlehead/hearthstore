// Package propertypath shares Datastore dotted-property traversal between
// mutations, filters, projections, and physical indexes.
package propertypath

import (
	"strings"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// Get reads a property, including excluded values used by mutation masks.
func Get(entity *datastorepb.Entity, path string) *datastorepb.Value {
	return get(entity, path, false)
}

// GetIndexed excludes a value when it or any containing entity is unindexed.
func GetIndexed(entity *datastorepb.Entity, path string) *datastorepb.Value {
	return get(entity, path, true)
}

func get(entity *datastorepb.Entity, path string, indexed bool) *datastorepb.Value {
	for {
		head, tail, nested := strings.Cut(path, ".")
		value := entity.GetProperties()[head]
		if value == nil || indexed && value.ExcludeFromIndexes {
			return nil
		}
		if !nested {
			return value
		}
		entity, path = value.GetEntityValue(), tail
	}
}

// Set writes a property while retaining existing parent metadata.
func Set(entity *datastorepb.Entity, path string, value *datastorepb.Value) {
	for {
		if entity.Properties == nil {
			entity.Properties = make(map[string]*datastorepb.Value)
		}
		head, tail, nested := strings.Cut(path, ".")
		if !nested {
			entity.Properties[head] = value
			return
		}
		parent := entity.Properties[head]
		child := parent.GetEntityValue()
		if child == nil {
			child = &datastorepb.Entity{}
			entity.Properties[head] = &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{EntityValue: child}}
		}
		entity, path = child, tail
	}
}

// Delete removes a leaf property, leaving siblings and parent metadata intact.
func Delete(entity *datastorepb.Entity, path string) {
	for entity != nil {
		head, tail, nested := strings.Cut(path, ".")
		if !nested {
			delete(entity.Properties, head)
			return
		}
		entity, path = entity.Properties[head].GetEntityValue(), tail
	}
}
