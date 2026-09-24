package datastore

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func TestEmulatorImportExportEndpointsRoundTrip(t *testing.T) {
	server := newTestDsServer(t)
	key := &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: testProject, DatabaseId: defaultDatabase}, Path: []*datastorepb.Key_PathElement{{Kind: "Widget", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}}}}
	original := &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"value": dsStr("exported")}}
	_, _, _, _, _, path := keyComponents(key)
	if _, err := server.grpc.store.DsUpsert(storage.EntityWrite{Project: testProject, Database: defaultDatabase, Path: path, Kind: "Widget", Entity: original}); err != nil {
		t.Fatal(err)
	}

	parent := t.TempDir()
	response := postEmulatorOperation(t, server, "/emulator/v1/projects/"+testProject+":export", map[string]string{
		"database":         "projects/" + testProject + "/databases/",
		"export_directory": parent,
	})
	if response.Code != http.StatusOK || response.Body.String() != "{}\n" {
		t.Fatalf("export status=%d body=%s", response.Code, response.Body.String())
	}
	metadata, err := filepath.Glob(filepath.Join(parent, "*", "*.overall_export_metadata"))
	if err != nil || len(metadata) != 1 {
		t.Fatalf("metadata files=%v err=%v", metadata, err)
	}

	replacement := &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"value": dsStr("replacement")}}
	if _, err := server.grpc.store.DsUpsert(storage.EntityWrite{Project: testProject, Database: defaultDatabase, Path: path, Kind: "Widget", Entity: replacement}); err != nil {
		t.Fatal(err)
	}
	response = postEmulatorOperation(t, server, "/emulator/v1/projects/"+testProject+":import", map[string]string{
		"database":         "projects/" + testProject + "/databases/",
		"export_directory": filepath.Dir(metadata[0]),
	})
	if response.Code != http.StatusOK || response.Body.String() != "{}\n" {
		t.Fatalf("import status=%d body=%s", response.Code, response.Body.String())
	}
	got, _, err := server.grpc.store.DsGet(testProject, defaultDatabase, "", path)
	if err != nil {
		t.Fatal(err)
	}
	if value := got.GetProperties()["value"].GetStringValue(); value != "exported" {
		t.Fatalf("imported value=%q, want exported", value)
	}
}

func TestEmulatorExportRejectsMismatchedDatabaseProject(t *testing.T) {
	server := newTestDsServer(t)
	response := postEmulatorOperation(t, server, "/emulator/v1/projects/"+testProject+":export", map[string]string{
		"database":         "projects/other/databases/",
		"export_directory": t.TempDir(),
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func postEmulatorOperation(t *testing.T, server *Server, path string, body map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	return response
}
