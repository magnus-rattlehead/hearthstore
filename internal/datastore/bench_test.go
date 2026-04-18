package datastore

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// ── benchmark helpers ─────────────────────────────────────────────────────────

func newBenchDsServer(b *testing.B) *Server {
	b.Helper()
	dir := b.TempDir()
	store, err := storage.New(filepath.Join(dir, "bench.db"))
	if err != nil {
		b.Fatalf("storage.New: %v", err)
	}
	b.Cleanup(func() {
		store.Close()
		os.RemoveAll(dir)
	})
	return New(store)
}

// bPost is the *testing.B equivalent of mustPost.
func bPost(b *testing.B, s *Server, method string, req proto.Message, resp proto.Message) {
	b.Helper()
	body, err := pjsonMarshal.Marshal(req)
	if err != nil {
		b.Fatalf("bPost marshal: %v", err)
	}
	hr := httptest.NewRequest(http.MethodPost, projectURL(method), bytes.NewReader(body))
	hr.Header.Set("Content-Type", "application/json")
	rw := httptest.NewRecorder()
	s.Handler().ServeHTTP(rw, hr)
	if rw.Code != 200 {
		b.Fatalf("POST %s: status %d, body: %s", method, rw.Code, rw.Body.Bytes())
	}
	if resp != nil {
		if err := pjsonUnmarshal.Unmarshal(rw.Body.Bytes(), resp); err != nil {
			b.Fatalf("bPost unmarshal: %v\nbody: %s", err, rw.Body.Bytes())
		}
	}
}

// tierFor returns "rare" for 10% of entities (i%10==0), "common" otherwise.
func tierFor(i int) string {
	if i%10 == 0 {
		return "rare"
	}
	return "common"
}

// bulkSeed inserts n entities into the Widget kind via chunked bulk commits.
// Each entity has: score=i, tier=tierFor(i). Used for bench/perf setup.
func bulkSeed(tb testing.TB, s *Server, n int) {
	tb.Helper()
	const batchSize = 500
	for start := 0; start < n; start += batchSize {
		end := start + batchSize
		if end > n {
			end = n
		}
		mutations := make([]*datastorepb.Mutation, end-start)
		for i := start; i < end; i++ {
			mutations[i-start] = &datastorepb.Mutation{
				Operation: &datastorepb.Mutation_Upsert{
					Upsert: dsEntity(dsKey("Widget", fmt.Sprintf("w%06d", i)), map[string]*datastorepb.Value{
						"score": dsInt(int64(i)),
						"tier":  dsStr(tierFor(i)),
					}),
				},
			}
		}
		req := &datastorepb.CommitRequest{ProjectId: testProject, Mutations: mutations}
		switch t := tb.(type) {
		case *testing.T:
			mustPost(t, s, "commit", req, &datastorepb.CommitResponse{})
		case *testing.B:
			bPost(t, s, "commit", req, &datastorepb.CommitResponse{})
		}
	}
}

// ── performance assertions ────────────────────────────────────────────────────
// These run with go test ./... and catch catastrophic regressions.
// Thresholds are extremely conservative (100-1000x below real hardware performance)
// so the tests are never flaky; they only trip on O(n²) bugs or broken WAL mode.

// TestPerf_SequentialCommits writes N entities one commit at a time.
func TestPerf_SequentialCommits(t *testing.T) {
	const n = 500
	s := newTestDsServer(t)

	start := time.Now()
	for i := 0; i < n; i++ {
		upsertEntity(t, s, dsEntity(
			dsKey("Widget", fmt.Sprintf("w%06d", i)),
			map[string]*datastorepb.Value{
				"score": dsInt(int64(i)),
				"label": dsStr(fmt.Sprintf("item-%d", i)),
			},
		))
	}
	elapsed := time.Since(start)
	rate := float64(n) / elapsed.Seconds()
	t.Logf("%d sequential commits: %v (%.0f commits/sec)", n, elapsed.Round(time.Millisecond), rate)

	const floor = 10.0 // commits/sec — real hardware typically does 500-5000+
	if rate < floor {
		t.Errorf("sequential commit rate %.1f/sec is below floor %.0f/sec", rate, floor)
	}
}

// TestPerf_BulkCommit writes 500 entities in a single Commit RPC.
func TestPerf_BulkCommit(t *testing.T) {
	const n = 500
	s := newTestDsServer(t)

	mutations := make([]*datastorepb.Mutation, n)
	for i := 0; i < n; i++ {
		mutations[i] = &datastorepb.Mutation{
			Operation: &datastorepb.Mutation_Upsert{
				Upsert: dsEntity(dsKey("Widget", fmt.Sprintf("w%06d", i)), map[string]*datastorepb.Value{
					"score": dsInt(int64(i)),
					"tier":  dsStr(tierFor(i)),
				}),
			},
		}
	}

	start := time.Now()
	mustPost(t, s, "commit", &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mutations: mutations,
	}, &datastorepb.CommitResponse{})
	elapsed := time.Since(start)
	rate := float64(n) / elapsed.Seconds()
	t.Logf("%d mutations in one commit: %v (%.0f mutations/sec)", n, elapsed.Round(time.Millisecond), rate)

	const ceiling = 30 * time.Second
	if elapsed > ceiling {
		t.Errorf("bulk commit of %d mutations took %v, want < %v", n, elapsed, ceiling)
	}
}

// TestPerf_BulkVsSequential compares per-entity cost of bulk vs sequential commits.
// Bulk must not be more than 2x slower per entity than sequential.
func TestPerf_BulkVsSequential(t *testing.T) {
	const n = 200

	mkMutations := func(prefix string) []*datastorepb.Mutation {
		m := make([]*datastorepb.Mutation, n)
		for i := 0; i < n; i++ {
			m[i] = &datastorepb.Mutation{
				Operation: &datastorepb.Mutation_Upsert{
					Upsert: dsEntity(dsKey("Widget", fmt.Sprintf("%s%06d", prefix, i)),
						map[string]*datastorepb.Value{"n": dsInt(int64(i))}),
				},
			}
		}
		return m
	}

	// Bulk: one commit, 200 mutations.
	sBulk := newTestDsServer(t)
	tBulk := time.Now()
	mustPost(t, sBulk, "commit", &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mutations: mkMutations("bulk"),
	}, &datastorepb.CommitResponse{})
	bulkElapsed := time.Since(tBulk)

	// Sequential: 200 commits, 1 mutation each.
	sSeq := newTestDsServer(t)
	tSeq := time.Now()
	for _, m := range mkMutations("seq") {
		mustPost(t, sSeq, "commit", &datastorepb.CommitRequest{
			ProjectId: testProject,
			Mutations: []*datastorepb.Mutation{m},
		}, &datastorepb.CommitResponse{})
	}
	seqElapsed := time.Since(tSeq)

	bulkRate := float64(n) / bulkElapsed.Seconds()
	seqRate := float64(n) / seqElapsed.Seconds()
	t.Logf("bulk:       %v (%.0f mutations/sec)", bulkElapsed.Round(time.Millisecond), bulkRate)
	t.Logf("sequential: %v (%.0f mutations/sec)", seqElapsed.Round(time.Millisecond), seqRate)

	// Bulk must not be more than 2x slower per entity than sequential.
	if bulkElapsed > seqElapsed*2 {
		t.Errorf("bulk commit (%v) is more than 2× slower than sequential (%v) — unexpected regression",
			bulkElapsed.Round(time.Millisecond), seqElapsed.Round(time.Millisecond))
	}
}

// TestPerf_BatchLookup seeds 500 entities then fetches all of them in one Lookup.
func TestPerf_BatchLookup(t *testing.T) {
	const n = 500
	s := newTestDsServer(t)
	bulkSeed(t, s, n)

	keys := make([]*datastorepb.Key, n)
	for i := 0; i < n; i++ {
		keys[i] = dsKey("Widget", fmt.Sprintf("w%06d", i))
	}

	start := time.Now()
	var resp datastorepb.LookupResponse
	mustPost(t, s, "lookup", &datastorepb.LookupRequest{
		ProjectId: testProject,
		Keys:      keys,
	}, &resp)
	elapsed := time.Since(start)
	rate := float64(n) / elapsed.Seconds()
	t.Logf("Lookup %d entities: %v (%.0f lookups/sec)", n, elapsed.Round(time.Millisecond), rate)

	if len(resp.Found) != n {
		t.Errorf("want %d found, got %d", n, len(resp.Found))
	}
	const ceiling = 30 * time.Second
	if elapsed > ceiling {
		t.Errorf("batch lookup of %d took %v, want < %v", n, elapsed, ceiling)
	}
}

// TestPerf_QueryFilterPushdown seeds 1000 entities (10% rare) and queries for
// the rare ones. With field-index pushdown only 100 entities are deserialised.
func TestPerf_QueryFilterPushdown(t *testing.T) {
	const n = 1000
	const wantMatch = n / 10
	s := newTestDsServer(t)
	bulkSeed(t, s, n)

	start := time.Now()
	var resp datastorepb.RunQueryResponse
	mustPost(t, s, "runQuery", &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
			Kind: []*datastorepb.KindExpression{{Name: "Widget"}},
			Filter: &datastorepb.Filter{
				FilterType: &datastorepb.Filter_PropertyFilter{
					PropertyFilter: &datastorepb.PropertyFilter{
						Property: &datastorepb.PropertyReference{Name: "tier"},
						Op:       datastorepb.PropertyFilter_EQUAL,
						Value:    dsStr("rare"),
					},
				},
			},
		}},
	}, &resp)
	elapsed := time.Since(start)
	got := len(resp.Batch.EntityResults)
	t.Logf("Filter query over %d entities (%d matches): %v", n, got, elapsed.Round(time.Millisecond))

	if got != wantMatch {
		t.Errorf("want %d results, got %d", wantMatch, got)
	}
	const ceiling = 10 * time.Second
	if elapsed > ceiling {
		t.Errorf("filter query over %d entities took %v, want < %v", n, elapsed, ceiling)
	}
}

// TestPerf_CursorPagination reads all 1000 entities via 50 pages of 20.
func TestPerf_CursorPagination(t *testing.T) {
	const n = 1000
	const pageSize = 20
	s := newTestDsServer(t)
	bulkSeed(t, s, n)

	var cursor []byte
	total := 0
	start := time.Now()
	for {
		q := &datastorepb.Query{
			Kind:  []*datastorepb.KindExpression{{Name: "Widget"}},
			Limit: wrapperspb.Int32(pageSize),
		}
		if len(cursor) > 0 {
			q.StartCursor = cursor
		}
		var resp datastorepb.RunQueryResponse
		mustPost(t, s, "runQuery", &datastorepb.RunQueryRequest{
			ProjectId: testProject,
			QueryType: &datastorepb.RunQueryRequest_Query{Query: q},
		}, &resp)
		total += len(resp.Batch.EntityResults)
		cursor = resp.Batch.EndCursor
		if resp.Batch.MoreResults != datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT {
			break
		}
	}
	elapsed := time.Since(start)
	pages := n / pageSize
	t.Logf("%d pages of %d over %d entities: %v (%d total)", pages, pageSize, n, elapsed.Round(time.Millisecond), total)

	if total != n {
		t.Errorf("pagination returned %d entities, want %d", total, n)
	}
	const ceiling = 30 * time.Second
	if elapsed > ceiling {
		t.Errorf("full pagination took %v, want < %v", elapsed, ceiling)
	}
}

// ── benchmarks ────────────────────────────────────────────────────────────────

// BenchmarkDs_CommitSingle measures a single-mutation commit.
func BenchmarkDs_CommitSingle(b *testing.B) {
	s := newBenchDsServer(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bPost(b, s, "commit", &datastorepb.CommitRequest{
			ProjectId: testProject,
			Mutations: []*datastorepb.Mutation{{
				Operation: &datastorepb.Mutation_Upsert{
					Upsert: dsEntity(dsKey("Widget", fmt.Sprintf("w%d", i)),
						map[string]*datastorepb.Value{
							"score": dsInt(int64(i)),
							"label": dsStr("bench"),
						}),
				},
			}},
		}, &datastorepb.CommitResponse{})
	}
}

// BenchmarkDs_CommitBulk200 measures committing 200 mutations in one RPC.
func BenchmarkDs_CommitBulk200(b *testing.B) {
	const batch = 200
	s := newBenchDsServer(b)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		mutations := make([]*datastorepb.Mutation, batch)
		for i := 0; i < batch; i++ {
			mutations[i] = &datastorepb.Mutation{
				Operation: &datastorepb.Mutation_Upsert{
					Upsert: dsEntity(dsKey("Widget", fmt.Sprintf("i%d-w%d", iter, i)),
						map[string]*datastorepb.Value{"n": dsInt(int64(i))}),
				},
			}
		}
		bPost(b, s, "commit", &datastorepb.CommitRequest{
			ProjectId: testProject,
			Mutations: mutations,
		}, &datastorepb.CommitResponse{})
	}
	b.SetBytes(batch)
}

// BenchmarkDs_LookupBatch200 measures a 200-key Lookup over pre-seeded data.
func BenchmarkDs_LookupBatch200(b *testing.B) {
	const n = 200
	s := newBenchDsServer(b)

	keys := make([]*datastorepb.Key, n)
	mutations := make([]*datastorepb.Mutation, n)
	for i := 0; i < n; i++ {
		keys[i] = dsKey("Widget", fmt.Sprintf("w%06d", i))
		mutations[i] = &datastorepb.Mutation{
			Operation: &datastorepb.Mutation_Upsert{
				Upsert: dsEntity(keys[i], map[string]*datastorepb.Value{"n": dsInt(int64(i))}),
			},
		}
	}
	bPost(b, s, "commit", &datastorepb.CommitRequest{ProjectId: testProject, Mutations: mutations},
		&datastorepb.CommitResponse{})

	req := &datastorepb.LookupRequest{ProjectId: testProject, Keys: keys}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bPost(b, s, "lookup", req, &datastorepb.LookupResponse{})
	}
}

// BenchmarkDs_RunQuery_1k_Equality measures an equality filter with field-index
// pushdown over 1 000 entities (~100 match).
func BenchmarkDs_RunQuery_1k_Equality(b *testing.B) {
	const n = 1000
	s := newBenchDsServer(b)
	bulkSeed(b, s, n)

	req := &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
			Kind: []*datastorepb.KindExpression{{Name: "Widget"}},
			Filter: &datastorepb.Filter{
				FilterType: &datastorepb.Filter_PropertyFilter{
					PropertyFilter: &datastorepb.PropertyFilter{
						Property: &datastorepb.PropertyReference{Name: "tier"},
						Op:       datastorepb.PropertyFilter_EQUAL,
						Value:    dsStr("rare"),
					},
				},
			},
		}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bPost(b, s, "runQuery", req, &datastorepb.RunQueryResponse{})
	}
}

// BenchmarkDs_RunQuery_1k_NoFilter measures a full kind scan over 1 000 entities.
func BenchmarkDs_RunQuery_1k_NoFilter(b *testing.B) {
	const n = 1000
	s := newBenchDsServer(b)
	bulkSeed(b, s, n)

	req := &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
			Kind: []*datastorepb.KindExpression{{Name: "Widget"}},
		}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bPost(b, s, "runQuery", req, &datastorepb.RunQueryResponse{})
	}
}
