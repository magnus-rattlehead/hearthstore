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
	"google.golang.org/protobuf/types/known/wrapperspb"

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

// BenchmarkFsEmulator_CommitSingle - 1 upsert per Commit RPC.
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

// BenchmarkFsEmulator_CommitBulk100 - 100 upserts per Commit RPC.
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

// BenchmarkFsEmulator_CommitBulk500 - 500 upserts per Commit RPC.
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

// BenchmarkFsEmulator_BatchWrite100 - 100 upserts per BatchWrite RPC.
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

// fsRunQuery executes a runQuery REST POST, draining the response body.
func fsRunQuery(b *testing.B, ep *fsEndpoint, q *firestorepb.StructuredQuery) {
	b.Helper()
	req := &firestorepb.RunQueryRequest{
		Parent: fmt.Sprintf("projects/%s/databases/%s/documents", fsBenchProject, fsBenchDB),
		QueryType: &firestorepb.RunQueryRequest_StructuredQuery{StructuredQuery: q},
	}
	body, err := fsPjsonBench.Marshal(req)
	if err != nil {
		b.Fatalf("fsRunQuery marshal: %v", err)
	}
	url := fmt.Sprintf("%s/v1/projects/%s/databases/%s/documents:runQuery",
		ep.baseURL, fsBenchProject, fsBenchDB)
	hr, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		b.Fatalf("fsRunQuery request: %v", err)
	}
	hr.Header.Set("Content-Type", "application/json")
	r, err := ep.client.Do(hr)
	if err != nil {
		b.Fatalf("fsRunQuery do: %v", err)
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		raw, _ := io.ReadAll(r.Body)
		b.Fatalf("fsRunQuery: status %d body: %s", r.StatusCode, raw)
	}
	io.Copy(io.Discard, r.Body) //nolint:errcheck
}

// fsRQSeed seeds n docs with field "n"=index into collection on ep.
func fsRQSeed(b *testing.B, ep *fsEndpoint, collection string, n int) {
	b.Helper()
	const batch = 100
	for start := 0; start < n; start += batch {
		end := start + batch
		if end > n {
			end = n
		}
		writes := make([]*firestorepb.Write, end-start)
		for i := start; i < end; i++ {
			writes[i-start] = fsWrite(collection, fmt.Sprintf("d%05d", i), int64(i))
		}
		fsEbPost(b, ep.client, ep.baseURL, "commit",
			&firestorepb.CommitRequest{Writes: writes}, nil)
	}
}

// BenchmarkFsEmulator_RunQuery_RangeOrderLimit: 1 000 docs, n > 500 ORDER BY n ASC LIMIT 10.
// Exercises the Stage-7 INNER JOIN sorted path in hearthstore.
func BenchmarkFsEmulator_RunQuery_RangeOrderLimit(b *testing.B) {
	for _, ep := range activeFsEndpoints(b) {
		ep := ep
		b.Run(ep.name, func(b *testing.B) {
			fsRQSeed(b, ep, "rq_rol", 1000)
			q := &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "rq_rol"}},
				Where: &firestorepb.StructuredQuery_Filter{
					FilterType: &firestorepb.StructuredQuery_Filter_FieldFilter{
						FieldFilter: &firestorepb.StructuredQuery_FieldFilter{
							Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "n"},
							Op:    firestorepb.StructuredQuery_FieldFilter_GREATER_THAN,
							Value: &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 500}},
						},
					},
				},
				OrderBy: []*firestorepb.StructuredQuery_Order{
					{Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "n"},
						Direction: firestorepb.StructuredQuery_ASCENDING},
				},
				Limit: wrapperspb.Int32(10),
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fsRunQuery(b, ep, q)
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/s")
		})
	}
}

// BenchmarkFsEmulator_RunQuery_Scan1k: full scan of 1 000 docs, no ORDER BY.
func BenchmarkFsEmulator_RunQuery_Scan1k(b *testing.B) {
	for _, ep := range activeFsEndpoints(b) {
		ep := ep
		b.Run(ep.name, func(b *testing.B) {
			fsRQSeed(b, ep, "rq_scan", 1000)
			q := &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "rq_scan"}},
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fsRunQuery(b, ep, q)
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/s")
		})
	}
}

// BenchmarkFsEmulator_RunQuery_CursorPage: paginate 1 000 docs at LIMIT 10 per page
// using a StartAt cursor. Each iteration fetches one page (10 docs), cycling through 100 pages.
func BenchmarkFsEmulator_RunQuery_CursorPage(b *testing.B) {
	for _, ep := range activeFsEndpoints(b) {
		ep := ep
		b.Run(ep.name, func(b *testing.B) {
			fsRQSeed(b, ep, "rq_cursor", 1000)
			orderBy := []*firestorepb.StructuredQuery_Order{
				{Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "n"},
					Direction: firestorepb.StructuredQuery_ASCENDING},
			}
			// Pre-build 100 page cursors (StartAfter n=0,10,20,...,990).
			pages := make([]*firestorepb.StructuredQuery, 100)
			for p := 0; p < 100; p++ {
				var startAt *firestorepb.Cursor
				if p > 0 {
					startAt = &firestorepb.Cursor{
						Values: []*firestorepb.Value{
							{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: int64(p*10 - 1)}},
						},
						Before: false, // exclusive (startAfter)
					}
				}
				pages[p] = &firestorepb.StructuredQuery{
					From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "rq_cursor"}},
					OrderBy: orderBy,
					Limit:   wrapperspb.Int32(10),
					StartAt: startAt,
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fsRunQuery(b, ep, pages[i%100])
			}
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/s")
		})
	}
}
