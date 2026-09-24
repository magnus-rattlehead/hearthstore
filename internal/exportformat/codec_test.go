package exportformat

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	latlng "google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLevelDBLogRoundTripAcrossBlockBoundary(t *testing.T) {
	records := [][]byte{[]byte("small"), bytes.Repeat([]byte("x"), levelDBBlockSize+100)}
	var encoded bytes.Buffer
	writer := NewLogWriter(&encoded)
	for _, record := range records {
		if err := writer.WriteRecord(record); err != nil {
			t.Fatal(err)
		}
	}

	reader := NewLogReader(bytes.NewReader(encoded.Bytes()))
	for i, want := range records {
		got, err := reader.ReadRecord()
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("record %d differs", i)
		}
	}
}

func TestLevelDBLogRejectsCorruptChecksum(t *testing.T) {
	var encoded bytes.Buffer
	writer := NewLogWriter(&encoded)
	if err := writer.WriteRecord([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	data := encoded.Bytes()
	data[len(data)-1] ^= 0xff

	if _, err := NewLogReader(bytes.NewReader(data)).ReadRecord(); err == nil {
		t.Fatal("corrupt record was accepted")
	}
}

func TestLevelDBLogMatchesOfficialVersionRecord(t *testing.T) {
	var encoded bytes.Buffer
	if err := NewLogWriter(&encoded).WriteRecord([]byte("3")); err != nil {
		t.Fatal(err)
	}
	want := []byte{0xb8, 0x6d, 0x44, 0x4e, 0x01, 0x00, 0x01, 0x33}
	if !bytes.Equal(encoded.Bytes(), want) {
		t.Fatalf("version record = %x, want %x", encoded.Bytes(), want)
	}
}

func TestEntityRoundTripPreservesDatastoreValuesAndRemapsKeys(t *testing.T) {
	sourcePartition := &datastorepb.PartitionId{ProjectId: "source-project", NamespaceId: "tenant"}
	sourceKey := &datastorepb.Key{PartitionId: sourcePartition, Path: []*datastorepb.Key_PathElement{
		{Kind: "Parent", IdType: &datastorepb.Key_PathElement_Id{Id: 42}},
		{Kind: "Child", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}},
	}}
	referenceKey := &datastorepb.Key{PartitionId: sourcePartition, Path: []*datastorepb.Key_PathElement{
		{Kind: "Referenced", IdType: &datastorepb.Key_PathElement_Name{Name: "two"}},
	}}
	entity := &datastorepb.Entity{Key: sourceKey, Properties: map[string]*datastorepb.Value{
		"integer": {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 7}},
		"boolean": {ValueType: &datastorepb.Value_BooleanValue{BooleanValue: true}},
		"string":  {ValueType: &datastorepb.Value_StringValue{StringValue: "hello"}},
		"meaning": {ValueType: &datastorepb.Value_StringValue{StringValue: "legacy"}, Meaning: int32(Property_BYTESTRING)},
		"double":  {ValueType: &datastorepb.Value_DoubleValue{DoubleValue: 1.25}},
		"null":    {ValueType: &datastorepb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}},
		"time":    {ValueType: &datastorepb.Value_TimestampValue{TimestampValue: timestamppb.New(time.Unix(123, 456000000).UTC())}},
		"blob":    {ValueType: &datastorepb.Value_BlobValue{BlobValue: []byte{0, 1, 2}}, ExcludeFromIndexes: true},
		"point":   {ValueType: &datastorepb.Value_GeoPointValue{GeoPointValue: &latlng.LatLng{Latitude: 12.5, Longitude: -33.25}}},
		"key":     {ValueType: &datastorepb.Value_KeyValue{KeyValue: referenceKey}},
		"array": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: []*datastorepb.Value{
			{ValueType: &datastorepb.Value_StringValue{StringValue: "a"}},
			{ValueType: &datastorepb.Value_StringValue{StringValue: "b"}},
		}}}},
		"empty": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{}}},
		"vector": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: []*datastorepb.Value{
			{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: 1}},
			{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: 2}},
		}}}, Meaning: int32(Property_LEGACY_FORMAT_VECTOR)},
		"entity": {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{
			"nested_key": {ValueType: &datastorepb.Value_KeyValue{KeyValue: referenceKey}},
		}}}},
	}}

	legacy, err := ToExportEntity(entity)
	if err != nil {
		t.Fatal(err)
	}
	got, err := FromExportEntity(legacy, "target-project", "target-database")
	if err != nil {
		t.Fatal(err)
	}

	want := proto.Clone(entity).(*datastorepb.Entity)
	remapEntityKeys(want, "target-project", "target-database")
	if !proto.Equal(got, want) {
		t.Fatalf("round trip differs\ngot:  %v\nwant: %v", got, want)
	}
}

func TestToExportEntityUsesRootKeyAsEntityGroup(t *testing.T) {
	entity := &datastorepb.Entity{Key: &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: "project"}, Path: []*datastorepb.Key_PathElement{{Kind: "Widget", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}}
	legacy, err := ToExportEntity(entity)
	if err != nil {
		t.Fatal(err)
	}
	if elements := legacy.GetEntityGroup().GetElement(); len(elements) != 1 || elements[0].GetType() != "Widget" || elements[0].GetName() != "one" {
		t.Fatalf("entity group = %v, want root key", legacy.GetEntityGroup())
	}
}

func TestLegacyUserValueRoundTrip(t *testing.T) {
	legacy := &EntityProto{
		Key:         &Reference{App: proto.String("source"), Path: &Path{Element: []*Path_Element{{Type: proto.String("Widget"), Name: proto.String("one")}}}},
		EntityGroup: &Path{},
		Property: []*Property{{
			Name:     proto.String("owner"),
			Multiple: proto.Bool(false),
			Value: &PropertyValue{Uservalue: &PropertyValue_UserValue{
				Email:             proto.String("person@example.com"),
				AuthDomain:        proto.String("example.com"),
				Nickname:          proto.String("user-id"),
				FederatedIdentity: proto.String("identity"),
				FederatedProvider: proto.String("provider"),
			}},
		}},
	}
	modern, err := FromExportEntity(legacy, "target", "(default)")
	if err != nil {
		t.Fatal(err)
	}
	owner := modern.GetProperties()["owner"]
	if owner.GetMeaning() != legacyUserMeaning || owner.GetEntityValue().GetProperties()["email"].GetStringValue() != "person@example.com" {
		t.Fatalf("legacy user was not converted to a predefined entity: %v", owner)
	}
	roundTripped, err := ToExportEntity(modern)
	if err != nil {
		t.Fatal(err)
	}
	if got := roundTripped.GetProperty()[0].GetValue().GetUservalue(); got == nil || got.GetEmail() != "person@example.com" || got.GetNickname() != "user-id" {
		t.Fatalf("predefined user was not converted back: %v", roundTripped)
	}
}

func remapEntityKeys(entity *datastorepb.Entity, project, database string) {
	if entity.Key != nil {
		entity.Key.PartitionId.ProjectId = project
		entity.Key.PartitionId.DatabaseId = database
	}
	for _, value := range entity.Properties {
		switch typed := value.ValueType.(type) {
		case *datastorepb.Value_KeyValue:
			typed.KeyValue.PartitionId.ProjectId = project
			typed.KeyValue.PartitionId.DatabaseId = database
		case *datastorepb.Value_EntityValue:
			remapEntityKeys(typed.EntityValue, project, database)
		case *datastorepb.Value_ArrayValue:
			for _, nested := range typed.ArrayValue.Values {
				remapEntityKeys(&datastorepb.Entity{Properties: map[string]*datastorepb.Value{"value": nested}}, project, database)
			}
		}
	}
}

func TestExportRoundTripUsesGoogleDirectoryLayout(t *testing.T) {
	entity := &datastorepb.Entity{Key: &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: "source", NamespaceId: "tenant"},
		Path:        []*datastorepb.Key_PathElement{{Kind: "Widget", IdType: &datastorepb.Key_PathElement_Id{Id: 9}}},
	}, Properties: map[string]*datastorepb.Value{
		"name": {ValueType: &datastorepb.Value_StringValue{StringValue: "nine"}},
	}}
	parent := t.TempDir()
	metadataPath, stats, err := WriteExport(context.Background(), parent, time.Unix(1700000000, 0), func(yield func(*datastorepb.Entity) error) error {
		return yield(entity)
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Entities != 1 {
		t.Fatalf("exported %d entities, want 1", stats.Entities)
	}
	if filepath.Base(metadataPath) != "datastore_export_1700000000.overall_export_metadata" {
		t.Fatalf("metadata path %q has unexpected name", metadataPath)
	}
	for _, relative := range []string{
		"all_namespaces/all_kinds/all_namespaces_all_kinds.export_metadata",
		"all_namespaces/all_kinds/output-0",
	} {
		if _, err := os.Stat(filepath.Join(filepath.Dir(metadataPath), relative)); err != nil {
			t.Fatalf("missing %s: %v", relative, err)
		}
	}

	var got []*datastorepb.Entity
	importStats, err := VisitExport(context.Background(), metadataPath, "target", "(default)", func(imported *datastorepb.Entity) error {
		got = append(got, imported)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if importStats.Entities != 1 || len(got) != 1 {
		t.Fatalf("imported stats=%+v entities=%d", importStats, len(got))
	}
	want := proto.Clone(entity).(*datastorepb.Entity)
	remapEntityKeys(want, "target", "(default)")
	if !proto.Equal(got[0], want) {
		t.Fatalf("round trip differs\ngot:  %v\nwant: %v", got[0], want)
	}
}

func TestVisitExportRejectsMetadataPathTraversal(t *testing.T) {
	dir := t.TempDir()
	metadataPath := filepath.Join(dir, "unsafe.overall_export_metadata")
	var encoded bytes.Buffer
	writer := NewLogWriter(&encoded)
	if err := writer.WriteRecord([]byte("3")); err != nil {
		t.Fatal(err)
	}
	unsafe := "../outside.export_metadata"
	payload, err := proto.Marshal(&OverallExportMetadata{EntitySet: []*SingleExportMetadataPointer{{ExportMetadataFile: &unsafe}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteRecord(payload); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metadataPath, encoded.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := VisitExport(context.Background(), metadataPath, "target", "(default)", func(*datastorepb.Entity) error { return nil }); err == nil {
		t.Fatal("unsafe metadata path was accepted")
	}
}

func TestVisitExportRejectsDeclaredEntityCountMismatch(t *testing.T) {
	parent := t.TempDir()
	metadataPath, _, err := WriteExport(context.Background(), parent, time.Unix(1700000001, 0), func(yield func(*datastorepb.Entity) error) error {
		return yield(&datastorepb.Entity{Key: &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: "project"}, Path: []*datastorepb.Key_PathElement{{Kind: "Widget", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}})
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	reader := NewLogReader(bytes.NewReader(raw))
	version, err := reader.ReadRecord()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := reader.ReadRecord()
	if err != nil {
		t.Fatal(err)
	}
	var metadata OverallExportMetadata
	if err := proto.Unmarshal(payload, &metadata); err != nil {
		t.Fatal(err)
	}
	metadata.EntitySet[0].NumEntities = proto.Int64(2)
	payload, err = proto.Marshal(&metadata)
	if err != nil {
		t.Fatal(err)
	}
	var rewritten bytes.Buffer
	writer := NewLogWriter(&rewritten)
	if err := writer.WriteRecord(version); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteRecord(payload); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metadataPath, rewritten.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := VisitExport(context.Background(), metadataPath, "project", "(default)", func(*datastorepb.Entity) error { return nil }); err == nil {
		t.Fatal("declared entity-count mismatch was accepted")
	}
}
