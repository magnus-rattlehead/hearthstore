package datastore

import (
	"unicode/utf8"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	maxCommitMutations     = 500
	maxEntityBytes         = 1_048_572
	maxKeyBytes            = 6 << 10
	maxEntityNestingDepth  = 20
	maxIndexedValues       = 20_000
	maxIndexedValueBytes   = 1_500
	maxUnindexedValueBytes = 1_048_487
	maxNameBytes           = 1_500
	maxKeyPathElements     = 100
)

func validateCommitLimits(req *datastorepb.CommitRequest) error {
	if len(req.Mutations) > maxCommitMutations {
		return status.Errorf(codes.InvalidArgument, "commit contains %d mutations; maximum is %d", len(req.Mutations), maxCommitMutations)
	}
	for _, mutation := range req.Mutations {
		var entity *datastorepb.Entity
		var key *datastorepb.Key
		incomplete := false
		switch operation := mutation.GetOperation().(type) {
		case *datastorepb.Mutation_Insert:
			entity = operation.Insert
			incomplete = true
		case *datastorepb.Mutation_Update:
			entity = operation.Update
		case *datastorepb.Mutation_Upsert:
			entity = operation.Upsert
			incomplete = true
		case *datastorepb.Mutation_Delete:
			key = operation.Delete
		default:
			return status.Error(codes.InvalidArgument, "mutation operation is required")
		}
		if entity != nil {
			key = entity.Key
			if err := validateEntityLimits(entity); err != nil {
				return err
			}
		}
		database := req.DatabaseId
		if database == "" {
			database = defaultDatabase
		}
		if err := validateKeyScope(key, req.ProjectId, database, incomplete); err != nil {
			return err
		}
		for _, part := range key.Path {
			if reservedName(part.Kind) || reservedName(part.GetName()) {
				return status.Error(codes.InvalidArgument, "mutation key is reserved/read-only")
			}
		}
		for _, transform := range mutation.PropertyTransforms {
			if transform.GetProperty() == "" || transform.GetTransformType() == nil {
				return status.Error(codes.InvalidArgument, "transform property and operation are required")
			}
		}
	}
	return nil
}

func validateEntityLimits(entity *datastorepb.Entity) error {
	if proto.Size(entity) > maxEntityBytes {
		return status.Error(codes.InvalidArgument, "entity exceeds the Datastore 1 MiB limit")
	}
	if proto.Size(entity.GetKey()) > maxKeyBytes {
		return status.Error(codes.InvalidArgument, "key exceeds the Datastore 6 KiB limit")
	}
	indexedValues := 0
	return validateProperties(entity.GetProperties(), 0, true, &indexedValues)
}

func validateProperties(properties map[string]*datastorepb.Value, depth int, indexed bool, count *int) error {
	if depth > maxEntityNestingDepth {
		return status.Errorf(codes.InvalidArgument, "entity nesting depth exceeds %d", maxEntityNestingDepth)
	}
	for name, value := range properties {
		if name == "" || len(name) > maxNameBytes || !utf8.ValidString(name) || reservedName(name) {
			return status.Error(codes.InvalidArgument, "property name must be valid UTF-8 and contain 1 to 1500 bytes")
		}
		if err := validatePropertyValue(name, value, depth, indexed, count); err != nil {
			return err
		}
	}
	return nil
}

func validatePropertyValue(name string, value *datastorepb.Value, depth int, indexed bool, count *int) error {
	if depth > maxEntityNestingDepth {
		return status.Errorf(codes.InvalidArgument, "entity nesting depth exceeds %d", maxEntityNestingDepth)
	}
	if value == nil || value.ValueType == nil {
		return status.Error(codes.InvalidArgument, "property value type is required")
	}
	if value.Meaning == 18 {
		return status.Error(codes.InvalidArgument, "projection values cannot be written")
	}
	if !utf8.ValidString(value.GetStringValue()) {
		return status.Error(codes.InvalidArgument, "string value must be valid UTF-8")
	}
	valueIndexed := indexed && !value.ExcludeFromIndexes
	if len(value.GetStringValue()) > maxUnindexedValueBytes || len(value.GetBlobValue()) > maxUnindexedValueBytes {
		return status.Errorf(codes.InvalidArgument, "property %q exceeds %d bytes", name, maxUnindexedValueBytes)
	}
	if valueIndexed {
		if value.GetArrayValue() == nil && value.GetEntityValue() == nil {
			*count++
			if *count > maxIndexedValues {
				return status.Errorf(codes.InvalidArgument, "entity contains more than %d indexed values", maxIndexedValues)
			}
		}
		if len(value.GetStringValue()) > maxIndexedValueBytes || len(value.GetBlobValue()) > maxIndexedValueBytes {
			return status.Errorf(codes.InvalidArgument, "indexed property %q exceeds %d bytes", name, maxIndexedValueBytes)
		}
	}
	if nested := value.GetEntityValue(); nested != nil {
		if err := validateProperties(nested.Properties, depth+1, valueIndexed, count); err != nil {
			return err
		}
	}
	if array := value.GetArrayValue(); array != nil {
		// The Java converter dispatches legacy vector meaning 31 before its
		// ordinary-array rule. Preserve that existing wire acceptance here;
		// this change does not add or reinterpret vector query semantics.
		if value.ExcludeFromIndexes && value.Meaning != 31 {
			return status.Error(codes.InvalidArgument, "exclude_from_indexes cannot be set on an array container")
		}
		for _, element := range array.Values {
			if err := validateArrayValue(name, element, depth+1, valueIndexed, count); err != nil {
				return err
			}
		}
	}
	return nil
}

func reservedName(name string) bool {
	return len(name) >= 4 && name[:2] == "__" && name[len(name)-2:] == "__"
}

func validateArrayValue(name string, value *datastorepb.Value, depth int, indexed bool, count *int) error {
	if value.GetArrayValue() != nil {
		return status.Error(codes.InvalidArgument, "nested arrays are not supported")
	}
	return validatePropertyValue(name, value, depth, indexed, count)
}
