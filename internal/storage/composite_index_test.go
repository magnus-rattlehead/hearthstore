package storage

import (
	"context"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

func TestDatastoreCompositeIndexLifecycle(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	properties := []DsIndexProperty{{Name: "office"}, {Name: "state"}, {Name: "sort_name"}}
	idx, created, err := store.EnsureDsCompositeIndex(context.Background(), DsCompositeIndex{
		Project: testProject, Kind: "UserProfile", Properties: properties, Source: "configured",
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !created || idx.State != DsIndexReady {
		t.Fatalf("created=%v state=%s", created, idx.State)
	}

	office := &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "Office", IdType: &datastorepb.Key_PathElement_Id{Id: 380011}}}}
	makeEntity := func(name, sortName string) *datastorepb.Entity {
		return &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "UserProfile", IdType: &datastorepb.Key_PathElement_Name{Name: name}}}}, Properties: map[string]*datastorepb.Value{
			"office":    {ValueType: &datastorepb.Value_KeyValue{KeyValue: office}},
			"state":     {ValueType: &datastorepb.Value_StringValue{StringValue: "active"}},
			"sort_name": {ValueType: &datastorepb.Value_StringValue{StringValue: sortName}},
		}}
	}
	for _, entry := range []struct{ name, sortName string }{{"second", "Zulu"}, {"first", "Alpha"}} {
		if _, _, _, _, err := store.DsInsert(testProject, testDB, "", "UserProfile/"+entry.name, "UserProfile", "", makeEntity(entry.name, entry.sortName)); err != nil {
			t.Fatal(err)
		}
	}
	prefix := DsCompositePrefix(idx, map[string]*datastorepb.Value{
		"office": {ValueType: &datastorepb.Value_KeyValue{KeyValue: office}},
		"state":  {ValueType: &datastorepb.Value_StringValue{StringValue: "active"}},
	})
	rows, scanned, err := store.DsQueryComposite(testProject, testDB, "", idx.ID, "", prefix, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Path != "UserProfile/first" {
		t.Fatalf("paths=%v scanned=%d", []string{rows[0].Path, rows[1].Path}, scanned)
	}

	if err := store.DsDelete(testProject, testDB, "", "UserProfile/first"); err != nil {
		t.Fatal(err)
	}
	rows, _, err = store.DsQueryComposite(testProject, testDB, "", idx.ID, "", prefix, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Path != "UserProfile/second" {
		t.Fatalf("rows after delete=%v", rows)
	}
}
