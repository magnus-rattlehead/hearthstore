//go:build integration

package tests

// Compares hearthstore against the official Google Cloud Datastore emulator
// over real TCP connections. Run while the official emulator is listening:
//
//	gcloud beta emulators datastore start --host-port=localhost:8081 \
//	  --project=bench-proj --no-store-on-disk
//
//	go test -tags integration -bench=BenchmarkEmulator -benchtime=5s ./tests/ \
//	  -run=^$ -official=http://localhost:8081

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	dspkg "github.com/magnus-rattlehead/hearthstore/internal/datastore"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

var officialEmulatorURL = flag.String(
	"official",
	os.Getenv("DATASTORE_EMULATOR_HOST_OFFICIAL"),
	"official emulator base URL (e.g. http://localhost:8081)",
)

const ebProject = "bench-proj"

var ebPjsonM = protojson.MarshalOptions{EmitUnpopulated: false}

// ebPost makes a protojson POST to baseURL/v1/projects/ebProject:{method}.
func ebPost(b *testing.B, client *http.Client, baseURL, method string, req, resp proto.Message) {
	b.Helper()
	body, err := ebPjsonM.Marshal(req)
	if err != nil {
		b.Fatalf("ebPost marshal %s: %v", method, err)
	}
	url := fmt.Sprintf("%s/v1/projects/%s:%s", baseURL, ebProject, method)
	hr, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		b.Fatalf("ebPost request %s: %v", method, err)
	}
	hr.Header.Set("Content-Type", "application/json")
	r, err := client.Do(hr)
	if err != nil {
		b.Fatalf("ebPost do %s: %v", method, err)
	}
	defer r.Body.Close()
	raw, _ := io.ReadAll(r.Body)
	if r.StatusCode != 200 {
		b.Fatalf("ebPost %s: status %d body: %s", method, r.StatusCode, raw)
	}
	if resp != nil {
		if err := protojson.Unmarshal(raw, resp); err != nil {
			b.Fatalf("ebPost unmarshal %s: %v\nbody: %s", method, err, raw)
		}
	}
}

// emulatorEndpoint bundles a base URL and HTTP client for one emulator instance.
type emulatorEndpoint struct {
	name    string
	baseURL string
	client  *http.Client
}

func newHearthstoreEndpoint(b *testing.B) *emulatorEndpoint {
	b.Helper()
	store, err := storage.New(b.TempDir())
	if err != nil {
		b.Fatalf("storage.New: %v", err)
	}
	b.Cleanup(func() { store.Close() })
	ts := httptest.NewServer(dspkg.New(store).Handler())
	b.Cleanup(ts.Close)
	return &emulatorEndpoint{name: "hearthstore", baseURL: ts.URL, client: ts.Client()}
}

func officialEndpoint() *emulatorEndpoint {
	u := *officialEmulatorURL
	if u == "" {
		return nil
	}
	if len(u) >= 4 && u[:4] != "http" {
		u = "http://" + u
	}
	return &emulatorEndpoint{name: "official", baseURL: u, client: &http.Client{}}
}

func ebIntVal(n int64) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: n}}
}
func ebStrVal(s string) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: s}}
}
func ebKey(kind, name string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: ebProject},
		Path:        []*datastorepb.Key_PathElement{{Kind: kind, IdType: &datastorepb.Key_PathElement_Name{Name: name}}},
	}
}

func singleUpsertCommit(i int) *datastorepb.CommitRequest {
	return &datastorepb.CommitRequest{
		ProjectId: ebProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{
				Upsert: &datastorepb.Entity{
					Key: ebKey("Widget", fmt.Sprintf("w%08d", i)),
					Properties: map[string]*datastorepb.Value{
						"score": ebIntVal(int64(i)),
						"label": ebStrVal("bench"),
					},
				},
			},
		}},
	}
}

func bulkUpsertCommit(batchSize, iter int) *datastorepb.CommitRequest {
	mutations := make([]*datastorepb.Mutation, batchSize)
	for i := 0; i < batchSize; i++ {
		mutations[i] = &datastorepb.Mutation{
			Operation: &datastorepb.Mutation_Upsert{
				Upsert: &datastorepb.Entity{
					Key: ebKey("Widget", fmt.Sprintf("b%d-w%05d", iter, i)),
					Properties: map[string]*datastorepb.Value{
						"n": ebIntVal(int64(i)),
					},
				},
			},
		}
	}
	return &datastorepb.CommitRequest{
		ProjectId: ebProject,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: mutations,
	}
}

// BenchmarkEmulator_CommitSingle — 1 upsert per RPC.
func BenchmarkEmulator_CommitSingle(b *testing.B) {
	for _, ep := range activeEndpoints(b) {
		ep := ep
		b.Run(ep.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ebPost(b, ep.client, ep.baseURL, "commit", singleUpsertCommit(i), nil)
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "commits/s")
		})
	}
}

// BenchmarkEmulator_CommitBulk100 — 100 upserts per RPC.
func BenchmarkEmulator_CommitBulk100(b *testing.B) {
	for _, ep := range activeEndpoints(b) {
		ep := ep
		b.Run(ep.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ebPost(b, ep.client, ep.baseURL, "commit", bulkUpsertCommit(100, i), nil)
			}
			b.ReportMetric(float64(b.N)*100/b.Elapsed().Seconds(), "mutations/s")
		})
	}
}

// BenchmarkEmulator_Lookup100 — seeds 100 entities then fetches all in one Lookup.
func BenchmarkEmulator_Lookup100(b *testing.B) {
	for _, ep := range activeEndpoints(b) {
		ep := ep
		b.Run(ep.name, func(b *testing.B) {
			// Seed once.
			ebPost(b, ep.client, ep.baseURL, "commit", bulkUpsertCommit(100, -1), nil)

			keys := make([]*datastorepb.Key, 100)
			for i := 0; i < 100; i++ {
				keys[i] = ebKey("Widget", fmt.Sprintf("b%d-w%05d", -1, i))
			}
			req := &datastorepb.LookupRequest{ProjectId: ebProject, Keys: keys}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ebPost(b, ep.client, ep.baseURL, "lookup", req, nil)
			}
			b.ReportMetric(float64(b.N)*100/b.Elapsed().Seconds(), "lookups/s")
		})
	}
}

func activeEndpoints(b *testing.B) []*emulatorEndpoint {
	b.Helper()
	eps := []*emulatorEndpoint{newHearthstoreEndpoint(b)}
	if off := officialEndpoint(); off != nil {
		eps = append(eps, off)
	}
	return eps
}
