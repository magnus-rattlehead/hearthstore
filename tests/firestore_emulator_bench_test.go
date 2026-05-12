//go:build integration

package tests

// Compares hearthstore against the official Google Cloud Firestore emulator
// over real TCP connections. Run while the official emulator is listening:
//
//	gcloud beta emulators firestore start --host-port=localhost:8086 \
//	  --project=fsbench-proj --no-store-on-disk
//
//	go test -tags integration -bench=BenchmarkFsEmulator -benchtime=5s ./tests/ \
//	  -run=^$ -fs-official=http://localhost:8086

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/magnus-rattlehead/hearthstore/internal/server"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

var fsBenchOfficialURL = flag.String(
	"fs-official",
	os.Getenv("FIRESTORE_EMULATOR_HOST_OFFICIAL"),
	"official Firestore emulator base URL (e.g. http://localhost:8086)",
)

const fsBenchProject = "fsbench-proj"
const fsBenchDB = "(default)"

var fsPjsonBench = protojson.MarshalOptions{EmitUnpopulated: false}

// fsEbPost makes a protojson POST to baseURL/v1/projects/fsBenchProject/databases/(default)/documents:{method}.
func fsEbPost(b *testing.B, client *http.Client, baseURL, method string, req, resp proto.Message) {
	b.Helper()
	body, err := fsPjsonBench.Marshal(req)
	if err != nil {
		b.Fatalf("fsEbPost marshal %s: %v", method, err)
	}
	url := fmt.Sprintf("%s/v1/projects/%s/databases/%s/documents:%s", baseURL, fsBenchProject, fsBenchDB, method)
	hr, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		b.Fatalf("fsEbPost request %s: %v", method, err)
	}
	hr.Header.Set("Content-Type", "application/json")
	r, err := client.Do(hr)
	if err != nil {
		b.Fatalf("fsEbPost do %s: %v", method, err)
	}
	defer r.Body.Close()
	raw, _ := io.ReadAll(r.Body)
	if r.StatusCode != 200 {
		b.Fatalf("fsEbPost %s: status %d body: %s", method, r.StatusCode, raw)
	}
	if resp != nil {
		if err := protojson.Unmarshal(raw, resp); err != nil {
			b.Fatalf("fsEbPost unmarshal %s: %v\nbody: %s", method, err, raw)
		}
	}
}

// fsEndpoint bundles a base URL and HTTP client for one Firestore emulator instance.
type fsEndpoint struct {
	name    string
	baseURL string
	client  *http.Client
}

func newHearthstoreFsEndpoint(b *testing.B) *fsEndpoint {
	b.Helper()
	store, err := storage.New(b.TempDir())
	if err != nil {
		b.Fatalf("storage.New: %v", err)
	}
	b.Cleanup(func() { store.Close() })
	ts := httptest.NewServer(server.New(store).RESTHandler())
	b.Cleanup(ts.Close)
	return &fsEndpoint{name: "hearthstore", baseURL: ts.URL, client: ts.Client()}
}

func officialFsEndpoint() *fsEndpoint {
	u := *fsBenchOfficialURL
	if u == "" {
		return nil
	}
	if len(u) >= 4 && u[:4] != "http" {
		u = "http://" + u
	}
	return &fsEndpoint{name: "official", baseURL: u, client: &http.Client{}}
}

func activeFsEndpoints(b *testing.B) []*fsEndpoint {
	b.Helper()
	eps := []*fsEndpoint{newHearthstoreFsEndpoint(b)}
	if off := officialFsEndpoint(); off != nil {
		eps = append(eps, off)
	}
	return eps
}

func fsDocName(collection, id string) string {
	return fmt.Sprintf("projects/%s/databases/%s/documents/%s/%s", fsBenchProject, fsBenchDB, collection, id)
}

func fsWrite(collection, id string, n int64) *firestorepb.Write {
	return &firestorepb.Write{
		Operation: &firestorepb.Write_Update{
			Update: &firestorepb.Document{
				Name: fsDocName(collection, id),
				Fields: map[string]*firestorepb.Value{
					"n": {ValueType: &firestorepb.Value_IntegerValue{IntegerValue: n}},
				},
			},
		},
	}
}

func fsBulkCommit(collection string, batchSize, iter int) *firestorepb.CommitRequest {
	writes := make([]*firestorepb.Write, batchSize)
	for i := 0; i < batchSize; i++ {
		writes[i] = fsWrite(collection, fmt.Sprintf("b%d-d%05d", iter, i), int64(i))
	}
	return &firestorepb.CommitRequest{Writes: writes}
}

func fsBulkBatchWrite(collection string, batchSize, iter int) *firestorepb.BatchWriteRequest {
	writes := make([]*firestorepb.Write, batchSize)
	for i := 0; i < batchSize; i++ {
		writes[i] = fsWrite(collection, fmt.Sprintf("b%d-d%05d", iter, i), int64(i))
	}
	return &firestorepb.BatchWriteRequest{Writes: writes}
}

// BenchmarkFsEmulator_CommitSingle — 1 upsert per Commit RPC.
func BenchmarkFsEmulator_CommitSingle(b *testing.B) {
	for _, ep := range activeFsEndpoints(b) {
		ep := ep
		b.Run(ep.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fsEbPost(b, ep.client, ep.baseURL, "commit",
					fsBulkCommit("single", 1, i), nil)
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "commits/s")
		})
	}
}

// BenchmarkFsEmulator_CommitBulk100 — 100 upserts per Commit RPC.
func BenchmarkFsEmulator_CommitBulk100(b *testing.B) {
	for _, ep := range activeFsEndpoints(b) {
		ep := ep
		b.Run(ep.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fsEbPost(b, ep.client, ep.baseURL, "commit",
					fsBulkCommit("bulk100", 100, i), nil)
			}
			b.ReportMetric(float64(b.N)*100/b.Elapsed().Seconds(), "writes/s")
		})
	}
}

// BenchmarkFsEmulator_CommitBulk500 — 500 upserts per Commit RPC.
func BenchmarkFsEmulator_CommitBulk500(b *testing.B) {
	for _, ep := range activeFsEndpoints(b) {
		ep := ep
		b.Run(ep.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fsEbPost(b, ep.client, ep.baseURL, "commit",
					fsBulkCommit("bulk500", 500, i), nil)
			}
			b.ReportMetric(float64(b.N)*500/b.Elapsed().Seconds(), "writes/s")
		})
	}
}

// BenchmarkFsEmulator_BatchWrite100 — 100 upserts per BatchWrite RPC.
func BenchmarkFsEmulator_BatchWrite100(b *testing.B) {
	for _, ep := range activeFsEndpoints(b) {
		ep := ep
		b.Run(ep.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fsEbPost(b, ep.client, ep.baseURL, "batchWrite",
					fsBulkBatchWrite("bw100", 100, i), nil)
			}
			b.ReportMetric(float64(b.N)*100/b.Elapsed().Seconds(), "writes/s")
		})
	}
}
