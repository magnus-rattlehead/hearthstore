package importexport

import (
	"context"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func TestExportImportRoundTripOverwritesMatchesAndPreservesUnrelated(t *testing.T) {
	ctx := context.Background()
	source, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	target, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Close() })

	put := func(store *storage.Store, project, value, name string) {
		t.Helper()
		key := &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: project, DatabaseId: "(default)"}, Path: []*datastorepb.Key_PathElement{{Kind: "Widget", IdType: &datastorepb.Key_PathElement_Name{Name: name}}}}
		entity := &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"value": {ValueType: &datastorepb.Value_StringValue{StringValue: value}}}}
		if _, err := store.DsUpsert(storage.EntityWrite{Project: project, Database: "(default)", Path: reviewNamedPath("Widget", name), Kind: "Widget", Entity: entity}); err != nil {
			t.Fatal(err)
		}
	}
	put(source, "source", "from-export", "one")
	put(target, "target", "old", "one")
	put(target, "target", "keep", "unrelated")

	metadataPath, exportStats, err := Export(ctx, source, t.TempDir(), "source", "(default)", time.Unix(1700000000, 0))
	if err != nil {
		t.Fatal(err)
	}
	if exportStats.Entities != 1 {
		t.Fatalf("exported %d entities, want 1", exportStats.Entities)
	}
	importStats, err := Import(ctx, target, metadataPath, "target", "(default)")
	if err != nil {
		t.Fatal(err)
	}
	if importStats.Entities != 1 {
		t.Fatalf("imported %d entities, want 1", importStats.Entities)
	}
	for path, want := range map[string]string{reviewNamedPath("Widget", "one"): "from-export", reviewNamedPath("Widget", "unrelated"): "keep"} {
		entity, _, err := target.DsGet("target", "(default)", "", path)
		if err != nil {
			t.Fatal(err)
		}
		if got := entity.GetProperties()["value"].GetStringValue(); got != want {
			t.Fatalf("%s value = %q, want %q", path, got, want)
		}
	}
}

func reviewNamedPath(kind, name string) string {
	return keycodec.Path([]*datastorepb.Key_PathElement{{Kind: kind, IdType: &datastorepb.Key_PathElement_Name{Name: name}}})
}
