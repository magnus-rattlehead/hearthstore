package exportformat

import (
	"fmt"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	latlng "google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const legacyUserMeaning = 20

// ToExportEntity converts a Datastore v1 entity to the EntityProto wire encoding
// used by supported Datastore export files.
func ToExportEntity(entity *datastorepb.Entity) (*EntityProto, error) {
	if entity == nil || entity.Key == nil {
		return nil, fmt.Errorf("export entity has no key")
	}
	return toExportEntity(entity, entity.Key.PartitionId.GetProjectId())
}

func toExportEntity(entity *datastorepb.Entity, defaultProject string) (*EntityProto, error) {
	key := keyToReference(entity.Key, defaultProject)
	if key == nil {
		key = &Reference{App: proto.String(defaultProject), Path: &Path{}}
	}
	legacy := &EntityProto{Key: key, EntityGroup: entityGroup(key)}
	for name, value := range entity.Properties {
		properties, err := valueToProperties(name, value, defaultProject)
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", name, err)
		}
		if value.GetExcludeFromIndexes() {
			legacy.RawProperty = append(legacy.RawProperty, properties...)
		} else {
			legacy.Property = append(legacy.Property, properties...)
		}
	}
	return legacy, nil
}

// FromExportEntity converts an exported EntityProto and remaps every key to
// the import target project and database.
func FromExportEntity(entity *EntityProto, project, database string) (*datastorepb.Entity, error) {
	if entity == nil || entity.Key == nil {
		return nil, fmt.Errorf("import entity has no key")
	}
	result, err := fromExportEntity(entity, project, database, true)
	if err != nil {
		return nil, err
	}
	if len(result.Key.GetPath()) == 0 {
		return nil, fmt.Errorf("import entity has an empty key path")
	}
	return result, nil
}

func fromExportEntity(entity *EntityProto, project, database string, requireKey bool) (*datastorepb.Entity, error) {
	result := &datastorepb.Entity{Properties: make(map[string]*datastorepb.Value)}
	if entity.Key != nil && len(entity.Key.GetPath().GetElement()) > 0 {
		result.Key = referenceToKey(entity.Key, project, database)
	} else if requireKey {
		result.Key = &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: project, DatabaseId: database}}
	}

	if err := addExportProperties(result, entity.Property, false, project, database); err != nil {
		return nil, err
	}
	if err := addExportProperties(result, entity.RawProperty, true, project, database); err != nil {
		return nil, err
	}
	return result, nil
}

func addExportProperties(entity *datastorepb.Entity, properties []*Property, excluded bool, project, database string) error {
	for _, property := range properties {
		value, err := exportPropertyValue(property, project, database)
		if err != nil {
			return fmt.Errorf("property %q: %w", property.GetName(), err)
		}
		value.ExcludeFromIndexes = excluded
		name := property.GetName()
		if property.GetMultiple() {
			existing := entity.Properties[name]
			if existing == nil {
				existing = &datastorepb.Value{ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{}}, ExcludeFromIndexes: excluded}
				if property.GetMeaning() == Property_LEGACY_FORMAT_VECTOR {
					existing.Meaning = int32(Property_LEGACY_FORMAT_VECTOR)
				}
				entity.Properties[name] = existing
			}
			array := existing.GetArrayValue()
			if array == nil {
				return fmt.Errorf("mixes scalar and array values")
			}
			value.ExcludeFromIndexes = false
			if property.GetMeaning() == Property_LEGACY_FORMAT_VECTOR {
				value.Meaning = 0
			}
			array.Values = append(array.Values, value)
			continue
		}
		if _, exists := entity.Properties[name]; exists {
			return fmt.Errorf("contains duplicate scalar values")
		}
		entity.Properties[name] = value
	}
	return nil
}

func valueToProperties(name string, value *datastorepb.Value, defaultProject string) ([]*Property, error) {
	if array := value.GetArrayValue(); array != nil {
		if len(array.Values) == 0 {
			return []*Property{{Name: proto.String(name), Multiple: proto.Bool(false), Value: &PropertyValue{}, Meaning: Property_EMPTY_LIST.Enum()}}, nil
		}
		properties := make([]*Property, 0, len(array.Values))
		for _, item := range array.Values {
			property, err := valueToProperty(name, item, defaultProject)
			if err != nil {
				return nil, err
			}
			property.Multiple = proto.Bool(true)
			if value.GetMeaning() != 0 {
				meaning := Property_Meaning(value.GetMeaning())
				property.Meaning = &meaning
			}
			properties = append(properties, property)
		}
		return properties, nil
	}
	property, err := valueToProperty(name, value, defaultProject)
	if err != nil {
		return nil, err
	}
	return []*Property{property}, nil
}

func valueToProperty(name string, value *datastorepb.Value, defaultProject string) (*Property, error) {
	property := &Property{Name: proto.String(name), Multiple: proto.Bool(false), Value: &PropertyValue{}}
	if value.GetMeaning() != 0 {
		meaning := Property_Meaning(value.GetMeaning())
		property.Meaning = &meaning
	}
	switch typed := value.GetValueType().(type) {
	case *datastorepb.Value_IntegerValue:
		property.Value.Int64Value = proto.Int64(typed.IntegerValue)
	case *datastorepb.Value_BooleanValue:
		property.Value.BooleanValue = proto.Bool(typed.BooleanValue)
	case *datastorepb.Value_StringValue:
		property.Value.StringValue = proto.String(typed.StringValue)
		if value.GetExcludeFromIndexes() && value.GetMeaning() == 0 {
			property.Meaning = Property_TEXT.Enum()
		}
	case *datastorepb.Value_DoubleValue:
		property.Value.DoubleValue = proto.Float64(typed.DoubleValue)
	case *datastorepb.Value_TimestampValue:
		if err := typed.TimestampValue.CheckValid(); err != nil {
			return nil, fmt.Errorf("invalid timestamp: %w", err)
		}
		property.Value.Int64Value = proto.Int64(typed.TimestampValue.AsTime().UnixMicro())
		property.Meaning = Property_GD_WHEN.Enum()
	case *datastorepb.Value_KeyValue:
		property.Value.Referencevalue = keyToReferenceValue(typed.KeyValue, defaultProject)
	case *datastorepb.Value_BlobValue:
		property.Value.StringValue = proto.String(string(typed.BlobValue))
		property.Meaning = Property_BLOB.Enum()
	case *datastorepb.Value_GeoPointValue:
		if typed.GeoPointValue == nil {
			return nil, fmt.Errorf("nil geo point")
		}
		property.Value.Pointvalue = &PropertyValue_PointValue{X: proto.Float64(typed.GeoPointValue.Latitude), Y: proto.Float64(typed.GeoPointValue.Longitude)}
		property.Meaning = Property_GEORSS_POINT.Enum()
	case *datastorepb.Value_EntityValue:
		if typed.EntityValue == nil {
			return nil, fmt.Errorf("nil embedded entity")
		}
		if value.GetMeaning() == legacyUserMeaning {
			user, err := entityToLegacyUser(typed.EntityValue)
			if err != nil {
				return nil, err
			}
			property.Value.Uservalue = user
			property.Meaning = nil
			break
		}
		legacy, err := toExportEntity(typed.EntityValue, defaultProject)
		if err != nil {
			return nil, err
		}
		encoded, err := proto.Marshal(legacy)
		if err != nil {
			return nil, fmt.Errorf("encoding embedded entity: %w", err)
		}
		property.Value.StringValue = proto.String(string(encoded))
		property.Meaning = Property_ENTITY_PROTO.Enum()
	case *datastorepb.Value_NullValue:
		// An empty export PropertyValue represents null.
	case nil:
		return nil, fmt.Errorf("value has no type")
	default:
		return nil, fmt.Errorf("unsupported Datastore value type %T", typed)
	}
	return property, nil
}

func exportPropertyValue(property *Property, project, database string) (*datastorepb.Value, error) {
	legacy := property.GetValue()
	if legacy == nil {
		return nil, fmt.Errorf("has no value")
	}
	value := &datastorepb.Value{}
	switch {
	case property.GetMeaning() == Property_EMPTY_LIST:
		value.ValueType = &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{}}
	case legacy.Int64Value != nil && property.GetMeaning() == Property_GD_WHEN:
		value.ValueType = &datastorepb.Value_TimestampValue{TimestampValue: timestamppb.New(time.UnixMicro(legacy.GetInt64Value()).UTC())}
	case legacy.Int64Value != nil:
		value.ValueType = &datastorepb.Value_IntegerValue{IntegerValue: legacy.GetInt64Value()}
	case legacy.BooleanValue != nil:
		value.ValueType = &datastorepb.Value_BooleanValue{BooleanValue: legacy.GetBooleanValue()}
	case legacy.StringValue != nil && property.GetMeaning() == Property_BLOB:
		value.ValueType = &datastorepb.Value_BlobValue{BlobValue: []byte(legacy.GetStringValue())}
	case legacy.StringValue != nil && property.GetMeaning() == Property_ENTITY_PROTO:
		var nested EntityProto
		if err := proto.Unmarshal([]byte(legacy.GetStringValue()), &nested); err != nil {
			return nil, fmt.Errorf("decoding embedded entity: %w", err)
		}
		entity, err := fromExportEntity(&nested, project, database, false)
		if err != nil {
			return nil, err
		}
		value.ValueType = &datastorepb.Value_EntityValue{EntityValue: entity}
	case legacy.StringValue != nil:
		value.ValueType = &datastorepb.Value_StringValue{StringValue: legacy.GetStringValue()}
	case legacy.DoubleValue != nil:
		value.ValueType = &datastorepb.Value_DoubleValue{DoubleValue: legacy.GetDoubleValue()}
	case legacy.Referencevalue != nil:
		value.ValueType = &datastorepb.Value_KeyValue{KeyValue: referenceValueToKey(legacy.Referencevalue, project, database)}
	case legacy.Pointvalue != nil:
		value.ValueType = &datastorepb.Value_GeoPointValue{GeoPointValue: &latlng.LatLng{Latitude: legacy.Pointvalue.GetX(), Longitude: legacy.Pointvalue.GetY()}}
	case legacy.Uservalue != nil:
		properties := map[string]*datastorepb.Value{
			"email":       predefinedString(legacy.Uservalue.GetEmail()),
			"auth_domain": predefinedString(legacy.Uservalue.GetAuthDomain()),
		}
		if legacy.Uservalue.Nickname != nil {
			properties["user_id"] = predefinedString(legacy.Uservalue.GetNickname())
		}
		if legacy.Uservalue.FederatedIdentity != nil {
			properties["federated_identity"] = predefinedString(legacy.Uservalue.GetFederatedIdentity())
		}
		if legacy.Uservalue.FederatedProvider != nil {
			properties["federated_provider"] = predefinedString(legacy.Uservalue.GetFederatedProvider())
		}
		value.ValueType = &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: properties}}
		value.Meaning = legacyUserMeaning
	default:
		value.ValueType = &datastorepb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}
	}
	switch property.GetMeaning() {
	case Property_NO_MEANING, Property_GD_WHEN, Property_GEORSS_POINT, Property_BLOB, Property_TEXT, Property_ENTITY_PROTO, Property_EMPTY_LIST:
	default:
		value.Meaning = int32(property.GetMeaning())
	}
	return value, nil
}

func entityToLegacyUser(entity *datastorepb.Entity) (*PropertyValue_UserValue, error) {
	read := func(name string, required bool) (*string, error) {
		value := entity.GetProperties()[name]
		if value == nil {
			if required {
				return nil, fmt.Errorf("legacy user entity is missing %q", name)
			}
			return nil, nil
		}
		if _, ok := value.GetValueType().(*datastorepb.Value_StringValue); !ok {
			return nil, fmt.Errorf("legacy user field %q is not a string", name)
		}
		return proto.String(value.GetStringValue()), nil
	}
	email, err := read("email", true)
	if err != nil {
		return nil, err
	}
	authDomain, err := read("auth_domain", true)
	if err != nil {
		return nil, err
	}
	userID, err := read("user_id", false)
	if err != nil {
		return nil, err
	}
	federatedIdentity, err := read("federated_identity", false)
	if err != nil {
		return nil, err
	}
	federatedProvider, err := read("federated_provider", false)
	if err != nil {
		return nil, err
	}
	return &PropertyValue_UserValue{Email: email, AuthDomain: authDomain, Nickname: userID, FederatedIdentity: federatedIdentity, FederatedProvider: federatedProvider}, nil
}

func predefinedString(value string) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: value}, ExcludeFromIndexes: true}
}

func keyToReference(key *datastorepb.Key, defaultProject string) *Reference {
	if key == nil {
		return nil
	}
	project := defaultProject
	namespace := ""
	if key.PartitionId != nil {
		if key.PartitionId.ProjectId != "" {
			project = key.PartitionId.ProjectId
		}
		namespace = key.PartitionId.NamespaceId
	}
	path := &Path{}
	for _, element := range key.Path {
		legacy := &Path_Element{Type: proto.String(element.Kind)}
		switch id := element.IdType.(type) {
		case *datastorepb.Key_PathElement_Id:
			legacy.Id = proto.Int64(id.Id)
		case *datastorepb.Key_PathElement_Name:
			legacy.Name = proto.String(id.Name)
		}
		path.Element = append(path.Element, legacy)
	}
	return &Reference{App: proto.String(project), NameSpace: proto.String(namespace), Path: path}
}

func keyToReferenceValue(key *datastorepb.Key, defaultProject string) *PropertyValue_ReferenceValue {
	reference := keyToReference(key, defaultProject)
	if reference == nil {
		return nil
	}
	value := &PropertyValue_ReferenceValue{App: reference.App, NameSpace: reference.NameSpace}
	for _, element := range reference.Path.Element {
		value.Pathelement = append(value.Pathelement, &PropertyValue_ReferenceValue_PathElement{Type: element.Type, Id: element.Id, Name: element.Name})
	}
	return value
}

func referenceToKey(reference *Reference, project, database string) *datastorepb.Key {
	key := &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: project, DatabaseId: database, NamespaceId: reference.GetNameSpace()}}
	for _, element := range reference.GetPath().GetElement() {
		key.Path = append(key.Path, pathElement(element.GetType(), element.Id, element.Name))
	}
	return key
}

func referenceValueToKey(reference *PropertyValue_ReferenceValue, project, database string) *datastorepb.Key {
	key := &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: project, DatabaseId: database, NamespaceId: reference.GetNameSpace()}}
	for _, element := range reference.GetPathelement() {
		key.Path = append(key.Path, pathElement(element.GetType(), element.Id, element.Name))
	}
	return key
}

func pathElement(kind string, id *int64, name *string) *datastorepb.Key_PathElement {
	element := &datastorepb.Key_PathElement{Kind: kind}
	if id != nil {
		element.IdType = &datastorepb.Key_PathElement_Id{Id: *id}
	} else if name != nil {
		element.IdType = &datastorepb.Key_PathElement_Name{Name: *name}
	}
	return element
}

func entityGroup(key *Reference) *Path {
	if key == nil || len(key.GetPath().GetElement()) == 0 {
		return &Path{}
	}
	root := key.GetPath().GetElement()[0]
	return &Path{Element: []*Path_Element{{Type: root.Type, Id: root.Id, Name: root.Name}}}
}
