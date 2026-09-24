package datastore

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func newBenchDsServer(b *testing.B) *Server {
	return newBenchDsServerWithOptions(b, storage.DefaultExactQueryMemoryBytes, storage.DefaultQueryConcurrency)
}

func newBenchDsServerWithOptions(b *testing.B, exactMemoryBytes int64, queryConcurrency int) *Server {
	b.Helper()
	dir := b.TempDir()
	store, err := storage.New(filepath.Join(dir, "bench.db"))
	if err != nil {
		b.Fatalf("storage.New: %v", err)
	}
	if err := store.ConfigureExactQueries(exactMemoryBytes, queryConcurrency); err != nil {
		b.Skipf("infeasible exact-query configuration: %v", err)
	}
	b.Cleanup(func() {
		store.Close()
		os.RemoveAll(dir)
	})
	return NewWithOptions(store, NewIndexManager(store), Options{QueryConcurrency: queryConcurrency})
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

func tierFor(i int) string {
	if i%10 == 0 {
		return "rare"
	}
	return "common"
}

// bulkSeed inserts n entities into the Widget kind via chunked bulk commits.
// Each entity has: score=i, tier=tierFor(i). Used for bench/perf setup.
func bulkSeed(tb testing.TB, s *Server, n int) {
	bulkSeedNamed(tb, s, n, func(i int) string { return fmt.Sprintf("w%06d", i) })
}

func bulkSeedLongPaths(tb testing.TB, s *Server, n int) {
	bulkSeedNamed(tb, s, n, func(i int) string { return fmt.Sprintf("w%01088d", i) })
}

func bulkSeedNamed(tb testing.TB, s *Server, n int, name func(int) string) {
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
					Upsert: dsEntity(dsKey("Widget", name(i)), map[string]*datastorepb.Value{
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

func BenchmarkDs_DottedEquality(b *testing.B) {
	for _, collision := range []bool{false, true} {
		b.Run(fmt.Sprintf("collision=%t", collision), func(b *testing.B) {
			s := newBenchDsServer(b)
			var mutations []*datastorepb.Mutation
			for i := 0; i < 100; i++ {
				properties := map[string]*datastorepb.Value{"a.b": dsInt(70)}
				if collision {
					properties["a"] = &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"b": dsInt(80)}}}}
				}
				mutations = append(mutations, &datastorepb.Mutation{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(dsKey("DottedBench", fmt.Sprint(i)), properties)}})
			}
			if _, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mutations: mutations}); err != nil {
				b.Fatal(err)
			}
			request := &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "DottedBench"}}, Filter: propFilter("a.b", datastorepb.PropertyFilter_EQUAL, dsInt(70))}}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				response, err := s.grpc.RunQuery(context.Background(), request)
				if err != nil || len(response.GetBatch().GetEntityResults()) != 100 {
					b.Fatalf("rows=%d error=%v", len(response.GetBatch().GetEntityResults()), err)
				}
			}
		})
	}
}

func BenchmarkDs_FallbackArrayOrdering(b *testing.B) {
	for _, shape := range []string{"scalar", "array", "filtered_array"} {
		b.Run(shape, func(b *testing.B) {
			s := newBenchDsServer(b)
			var mutations []*datastorepb.Mutation
			for i := range 100 {
				value := dsInt(int64(i))
				if shape != "scalar" {
					value = dsArray(dsInt(int64(i)), dsInt(int64(200+i)))
				}
				mutations = append(mutations, &datastorepb.Mutation{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(dsKey("FallbackBench", fmt.Sprint(i)), map[string]*datastorepb.Value{"items": value, "n": dsInt(int64(i))})}})
			}
			if _, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mutations: mutations}); err != nil {
				b.Fatal(err)
			}
			// Kindless queries exercise the disk-sort path without changing fixtures.
			q := &datastorepb.Query{Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "items"}}, {Property: &datastorepb.PropertyReference{Name: "n"}}}}
			if shape == "filtered_array" {
				q.Filter = propFilter("items", datastorepb.PropertyFilter_GREATER_THAN, dsInt(150))
			}
			req := &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				response, err := s.grpc.RunQuery(context.Background(), req)
				if err != nil || len(response.GetBatch().GetEntityResults()) != 100 {
					b.Fatalf("rows=%d err=%v", len(response.GetBatch().GetEntityResults()), err)
				}
			}
		})
	}
}

// BenchmarkDs_QueryExecutionQuantum measures scheduling overhead without a
// request-work limit. Every sample validates the complete finite result.
func BenchmarkDs_QueryExecutionQuantum(b *testing.B) {
	s := newBenchDsServer(b)
	var mutations []*datastorepb.Mutation
	for i := range 100 {
		entity := dsEntity(dsKey("QuantumBench", fmt.Sprint(i)), map[string]*datastorepb.Value{"x": dsArray(dsInt(1), dsInt(4)), "y": dsArray(dsInt(2), dsInt(5)), "n": dsInt(int64(i))})
		mutations = append(mutations, &datastorepb.Mutation{Operation: &datastorepb.Mutation_Upsert{Upsert: entity}})
	}
	if _, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mutations: mutations}); err != nil {
		b.Fatal(err)
	}
	for _, shape := range []string{"ordinary", "permuted24", "correlated_expanded", "correlated_factored"} {
		for _, quantum := range []uint64{1, 64, 1024, 16384} {
			b.Run(fmt.Sprintf("%s/quantum_%d", shape, quantum), func(b *testing.B) {
				correlated := shape == "correlated_expanded" || shape == "correlated_factored"
				q := &datastorepb.Query{Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}}, {Property: &datastorepb.PropertyReference{Name: "n"}}}}
				want := 100
				base := context.Background()
				if shape == "permuted24" {
					var groups []*datastorepb.Filter
					for i := range 24 {
						var values []*datastorepb.Value
						for j := range 24 {
							values = append(values, dsInt(int64((i+j)%24)))
						}
						groups = append(groups, orFilter(propFilter("x", datastorepb.PropertyFilter_IN, dsArray(values...)), propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1))))
					}
					q.Filter = andFilter(groups...)
				}
				if correlated {
					q.Filter = orFilter(andFilter(propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(3)), propFilter("y", datastorepb.PropertyFilter_GREATER_THAN, dsInt(4))), andFilter(propFilter("x", datastorepb.PropertyFilter_LESS_THAN, dsInt(2)), propFilter("y", datastorepb.PropertyFilter_LESS_THAN, dsInt(3))))
					q.Order[1].Property.Name = "y"
					q.Projection = []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "x"}}, {Property: &datastorepb.PropertyReference{Name: "y"}}}
					prepared, condition, err := prepareQueryCondition(q, "")
					if err != nil {
						b.Fatal(err)
					}
					if shape == "correlated_factored" {
						condition.branches = nil
					}
					base = context.WithValue(base, queryConditionContextKey{}, preparedQueryCondition{condition: condition})
					q, want = prepared, 200
				}
				request := &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}}
				var attempts, comparisons, yields, decoded, scratch uint64
				latencies := make([]int64, 0, b.N)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					ctx, work := storage.WithQueryWork(base, quantum)
					started := time.Now()
					response, err := s.grpc.RunQuery(ctx, request)
					latencies = append(latencies, time.Since(started).Nanoseconds())
					if err != nil || len(response.GetBatch().GetEntityResults()) != want {
						b.Fatalf("rows=%d want=%d err=%v", len(response.GetBatch().GetEntityResults()), want, err)
					}
					if correlated {
						for _, row := range response.Batch.EntityResults {
							x, y := row.Entity.Properties["x"].GetIntegerValue(), row.Entity.Properties["y"].GetIntegerValue()
							if !(x == 1 && y == 2 || x == 4 && y == 5) {
								b.Fatalf("crossed tuple (%d,%d)", x, y)
							}
						}
					}
					counts := work.Snapshot()
					attempts += counts[storage.WorkAttempts]
					comparisons += counts[storage.WorkComparisons]
					yields += counts[storage.WorkYields]
					decoded += counts[storage.WorkDecodedBytes]
					scratch += counts[storage.WorkScratchReadBytes] + counts[storage.WorkScratchWriteBytes]
				}
				b.StopTimer()
				slices.Sort(latencies)
				b.ReportMetric(float64(latencies[(len(latencies)-1)/2]), "p50-ns")
				b.ReportMetric(float64(latencies[(len(latencies)*95+99)/100-1]), "p95-ns")
				for name, value := range map[string]uint64{"attempts/op": attempts, "comparisons/op": comparisons, "yields/op": yields, "decoded-B/op": decoded, "scratch-B/op": scratch} {
					b.ReportMetric(float64(value)/float64(b.N), name)
				}
			})
		}
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

func BenchmarkDs_CommitBulk500Indexed(b *testing.B) {
	const batch = 500
	s := newBenchDsServer(b)
	_, _, err := s.grpc.store.EnsureDsCompositeIndex(context.Background(), storage.DsCompositeIndex{
		Project: testProject,
		Kind:    "Widget",
		Properties: []storage.DsIndexProperty{
			{Name: "tier"},
			{Name: "score", Desc: true},
		},
	}, true)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		mutations := make([]*datastorepb.Mutation, batch)
		for i := range mutations {
			mutations[i] = &datastorepb.Mutation{
				Operation: &datastorepb.Mutation_Upsert{
					Upsert: dsEntity(dsKey("Widget", fmt.Sprintf("i%d-w%d", iter, i)), map[string]*datastorepb.Value{
						"score": dsInt(int64(i)),
						"tier":  dsStr(tierFor(i)),
					}),
				},
			}
		}
		bPost(b, s, "commit", &datastorepb.CommitRequest{ProjectId: testProject, Mutations: mutations}, &datastorepb.CommitResponse{})
	}
	b.ReportMetric(batch, "entities/op")
}

func BenchmarkDs_CommitBulk500AutoID(b *testing.B) {
	const batch = 500
	s := newBenchDsServer(b)
	b.ResetTimer()
	for range b.N {
		mutations := make([]*datastorepb.Mutation, batch)
		for i := range mutations {
			mutations[i] = &datastorepb.Mutation{Operation: &datastorepb.Mutation_Upsert{
				Upsert: dsEntity(incompleteKey("Call"), map[string]*datastorepb.Value{"n": dsInt(int64(i))}),
			}}
		}
		bPost(b, s, "commit", &datastorepb.CommitRequest{
			ProjectId: testProject,
			Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
			Mutations: mutations,
		}, &datastorepb.CommitResponse{})
	}
	b.ReportMetric(batch, "entities/op")
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

func BenchmarkDs_LookupBatch1000(b *testing.B) {
	const n = 1000
	s := newBenchDsServer(b)
	bulkSeed(b, s, n)
	keys := make([]*datastorepb.Key, n)
	for i := range keys {
		keys[i] = dsKey("Widget", fmt.Sprintf("w%06d", i))
	}
	req := &datastorepb.LookupRequest{ProjectId: testProject, Keys: keys}
	b.ResetTimer()
	for range b.N {
		bPost(b, s, "lookup", req, &datastorepb.LookupResponse{})
	}
	b.ReportMetric(n, "entities/op")
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

// BenchmarkDs_FocusedIndexAccess measures exact filtered keys and selective OR reads.
func BenchmarkDs_FocusedIndexAccess(b *testing.B) {
	for _, name := range []string{"equality_keys", "composite_equality_keys", "ordered_or_ranges"} {
		b.Run(name, func(b *testing.B) {
			s := newBenchDsServer(b)
			bulkSeed(b, s, 1000)
			if name == "composite_equality_keys" {
				// Fixed second dimension gives the same 100 results as equality_keys.
				for start := 0; start < 1000; start += 100 {
					var mutations []*datastorepb.Mutation
					for i := start; i < start+100; i++ {
						mutations = append(mutations, &datastorepb.Mutation{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(dsKey("Widget", fmt.Sprintf("w%06d", i)), map[string]*datastorepb.Value{"score": dsInt(1), "tier": dsStr(tierFor(i))})}})
					}
					if _, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL, Mutations: mutations}); err != nil {
						b.Fatal(err)
					}
				}
			}
			query := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Widget"}}}
			var want []string
			if name != "ordered_or_ranges" {
				query.Filter = propFilter("tier", datastorepb.PropertyFilter_EQUAL, dsStr("rare"))
				if name == "composite_equality_keys" {
					query.Filter = andFilter(query.Filter, propFilter("score", datastorepb.PropertyFilter_EQUAL, dsInt(1)))
				}
				query.Projection = []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "__key__"}}}
				for i := 0; i < 1000; i += 10 {
					want = append(want, fmt.Sprintf("w%06d", i))
				}
			} else {
				query.Filter = orFilter(propFilter("score", datastorepb.PropertyFilter_LESS_THAN, dsInt(2)), propFilter("score", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(998)))
				query.Order = []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "score"}, Direction: datastorepb.PropertyOrder_ASCENDING}}
				want = []string{"w000000", "w000001", "w000998", "w000999"}
			}
			req := &datastorepb.RunQueryRequest{ProjectId: testProject, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}, QueryType: &datastorepb.RunQueryRequest_Query{Query: query}}
			// Prepare generated indexes outside the timed steady-state reads.
			if _, err := s.grpc.RunQuery(context.Background(), req); err != nil {
				b.Fatal(err)
			}
			var documents, entries int64
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				response, err := s.grpc.RunQuery(context.Background(), req)
				if err != nil {
					b.Fatal(err)
				}
				var got []string
				for _, row := range response.Batch.EntityResults {
					got = append(got, row.Entity.Key.Path[0].GetName())
				}
				if !slices.Equal(got, want) || response.Batch.MoreResults != datastorepb.QueryResultBatch_NO_MORE_RESULTS {
					b.Fatalf("unexpected results: %v", response.Batch)
				}
				for metric, total := range map[string]*int64{"documents_scanned": &documents, "index_entries_scanned": &entries} {
					value, err := strconv.ParseInt(response.ExplainMetrics.ExecutionStats.DebugStats.Fields[metric].GetStringValue(), 10, 64)
					if err != nil {
						b.Fatal(err)
					}
					*total += value
				}
			}
			b.ReportMetric(float64(documents)/float64(b.N), "entity_reads/op")
			b.ReportMetric(float64(entries)/float64(b.N), "index_entries/op")
		})
	}
}

// BenchmarkDs_EqualityOR measures branch setup, sparse gains and dense overlap cost.
func BenchmarkDs_EqualityOR(b *testing.B) {
	for _, branches := range []int{2, 8, 30} {
		b.Run(fmt.Sprintf("branches_%d", branches), func(b *testing.B) {
			for _, density := range []string{"sparse", "dense", "overlap"} {
				b.Run(density, func(b *testing.B) {
					s := newBenchDsServer(b)
					var filters []*datastorepb.Filter
					for field := range branches {
						filters = append(filters, propFilter(fmt.Sprintf("p%02d", field), datastorepb.PropertyFilter_EQUAL, dsInt(1)))
					}
					var want []string
					for start := 0; start < 1000; start += 100 {
						var mutations []*datastorepb.Mutation
						for i := start; i < start+100; i++ {
							properties := map[string]*datastorepb.Value{}
							matches := density != "sparse" || i >= 996
							for field := range branches {
								value := int64(0)
								if matches && (density == "overlap" || field == i%branches) {
									value = 1
								}
								properties[fmt.Sprintf("p%02d", field)] = dsInt(value)
							}
							name := fmt.Sprintf("w%06d", i)
							mutations = append(mutations, &datastorepb.Mutation{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(dsKey("EqualityORBench", name), properties)}})
							if matches {
								want = append(want, name)
							}
						}
						if _, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL, Mutations: mutations}); err != nil {
							b.Fatal(err)
						}
					}
					for _, pageSize := range []int{1, 1000} {
						b.Run(fmt.Sprintf("page_%d", pageSize), func(b *testing.B) {
							query := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "EqualityORBench"}}, Filter: orFilter(filters...), Limit: wrapperspb.Int32(int32(pageSize))}
							req := &datastorepb.RunQueryRequest{ProjectId: testProject, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}, QueryType: &datastorepb.RunQueryRequest_Query{Query: query}}
							var reads, entries int64
							b.ReportAllocs()
							b.ResetTimer()
							for range b.N {
								response, err := s.grpc.RunQuery(context.Background(), req)
								if err != nil {
									b.Fatal(err)
								}
								var got []string
								for _, row := range response.Batch.EntityResults {
									got = append(got, row.Entity.Key.Path[0].GetName())
								}
								if !slices.Equal(got, want[:min(pageSize, len(want))]) {
									b.Fatalf("keys=%v want=%v", got, want)
								}
								for metric, total := range map[string]*int64{"documents_scanned": &reads, "index_entries_scanned": &entries} {
									value, err := strconv.ParseInt(response.ExplainMetrics.ExecutionStats.DebugStats.Fields[metric].GetStringValue(), 10, 64)
									if err != nil {
										b.Fatal(err)
									}
									*total += value
								}
							}
							b.ReportMetric(float64(reads)/float64(b.N), "entity_reads/op")
							b.ReportMetric(float64(entries)/float64(b.N), "index_entries/op")
						})
					}
				})
			}
		})
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

func BenchmarkDs_SmallExactAggregation(b *testing.B) {
	s := newBenchDsServer(b)
	bulkSeed(b, s, 16)
	if err := s.grpc.store.ConfigureExactQueries(8<<20, 1); err != nil {
		b.Fatal(err)
	}
	req := &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{
		QueryType:    &datastorepb.AggregationQuery_NestedQuery{NestedQuery: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Widget"}}}},
		Aggregations: []*datastorepb.AggregationQuery_Aggregation{sumAgg("total", "score")},
	}}}
	b.ResetTimer()
	for range b.N {
		response, err := s.grpc.RunAggregationQuery(context.Background(), req)
		if err != nil {
			b.Fatal(err)
		}
		if got := response.Batch.AggregationResults[0].AggregateProperties["total"].GetIntegerValue(); got != 120 {
			b.Fatalf("sum=%d, want 120", got)
		}
	}
}

func BenchmarkDs_CoveringAggregation(b *testing.B) {
	for _, n := range []int{1000, 2000, 4000} {
		for _, composite := range []bool{false, true} {
			b.Run(fmt.Sprintf("rows_%d/composite_%t", n, composite), func(b *testing.B) {
				s := newBenchDsServerWithOptions(b, 8<<20, 1)
				bulkSeedLongPaths(b, s, n)
				q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Widget"}}}
				want := int64(n) * int64(n-1) / 2
				if composite {
					q.Filter = propFilter("tier", datastorepb.PropertyFilter_EQUAL, dsStr("common"))
					idx := storage.DsCompositeIndex{Project: testProject, Kind: "Widget", Properties: []storage.DsIndexProperty{{Name: "tier"}, {Name: "score"}}}
					if _, _, err := s.grpc.store.EnsureDsCompositeIndex(context.Background(), idx, true); err != nil {
						b.Fatal(err)
					}
					want -= 10 * int64(n/10) * int64(n/10-1) / 2
				}
				req := &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{
					QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: q}, Aggregations: []*datastorepb.AggregationQuery_Aggregation{sumAgg("s", "score")},
				}}}
				latencies := make([]time.Duration, 0, b.N)
				var reads, visits, scratch uint64
				b.ResetTimer()
				for range b.N {
					ctx, work := storage.WithQueryWork(context.Background(), 0)
					start := time.Now()
					response, err := s.grpc.RunAggregationQuery(ctx, req)
					latencies = append(latencies, time.Since(start))
					if err != nil || response.GetBatch().GetAggregationResults()[0].AggregateProperties["s"].GetIntegerValue() != want {
						b.Fatalf("aggregate=%v want=%d err=%v", response, want, err)
					}
					readCount, err := strconv.ParseUint(response.ExplainMetrics.ExecutionStats.DebugStats.Fields["documents_scanned"].GetStringValue(), 10, 64)
					if err != nil {
						b.Fatal(err)
					}
					reads += readCount
					visits += work.Snapshot()[storage.WorkIndexEntries]
					scratch += work.Snapshot()[storage.WorkScratchWriteBytes]
				}
				b.StopTimer()
				slices.Sort(latencies)
				b.ReportMetric(float64(latencies[(95*len(latencies)+99)/100-1]), "p95-ns")
				b.ReportMetric(float64(reads)/float64(b.N), "entity-reads/op")
				b.ReportMetric(float64(visits)/float64(b.N), "index-visits/op")
				b.ReportMetric(float64(scratch)/float64(b.N), "scratch-write-B/op")
			})
		}
	}
}

func BenchmarkDs_ExactAggregationTuning(b *testing.B) {
	const operationsPerWorker = 1
	base := newBenchDsServer(b)
	bulkSeedLongPaths(b, base, 2_000)
	store := base.grpc.store
	indexes := NewIndexManager(store)

	for _, memoryBytes := range []int64{8 << 20, 16 << 20, 32 << 20, 64 << 20, 128 << 20} {
		for _, concurrency := range []int{1, 2, 4, 8, 12, 16, 24, 32} {
			name := fmt.Sprintf("memory_%dMiB/concurrency_%d", memoryBytes>>20, concurrency)
			b.Run(name, func(b *testing.B) {
				if err := store.ConfigureExactQueries(memoryBytes, concurrency); err != nil {
					b.Skipf("infeasible exact-query configuration: %v", err)
				}
				s := NewWithOptions(store, indexes, Options{QueryConcurrency: concurrency})
				req := &datastorepb.RunAggregationQueryRequest{
					ProjectId: testProject,
					QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{
						AggregationQuery: &datastorepb.AggregationQuery{
							QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: &datastorepb.Query{
								Kind: []*datastorepb.KindExpression{{Name: "Widget"}},
								Filter: orFilter(
									propFilter("tier", datastorepb.PropertyFilter_EQUAL, dsStr("rare")),
									propFilter("score", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(0)),
								),
							}},
							Aggregations: []*datastorepb.AggregationQuery_Aggregation{sumAgg("total", "score")},
						},
					},
				}
				warmup, err := s.grpc.RunAggregationQuery(context.Background(), req)
				if err != nil {
					b.Fatal(err)
				}
				if got := warmup.GetBatch().GetAggregationResults()[0].GetAggregateProperties()["total"].GetIntegerValue(); got != 1_999_000 {
					b.Fatalf("warm-up sum = %d, want 1999000", got)
				}
				type result struct {
					latency time.Duration
					sum     int64
					err     error
				}
				latencies := make([]time.Duration, 0, b.N*concurrency*operationsPerWorker)
				b.ReportMetric(float64(memoryBytes), "scratch-budget-B")
				b.ReportMetric(float64(concurrency), "query-concurrency")
				b.ResetTimer()
				for range b.N {
					start := make(chan struct{})
					results := make(chan result, concurrency*operationsPerWorker)
					var workers sync.WaitGroup
					workers.Add(concurrency)
					for range concurrency {
						go func() {
							defer workers.Done()
							<-start
							for range operationsPerWorker {
								started := time.Now()
								response, err := s.grpc.RunAggregationQuery(context.Background(), req)
								entry := result{latency: time.Since(started), err: err}
								if err == nil {
									entry.sum = response.GetBatch().GetAggregationResults()[0].GetAggregateProperties()["total"].GetIntegerValue()
								}
								results <- entry
							}
						}()
					}
					close(start)
					workers.Wait()
					close(results)
					for entry := range results {
						if entry.err != nil {
							b.Error(entry.err)
							continue
						}
						if entry.sum != 1_999_000 {
							b.Errorf("sum = %d, want 1999000", entry.sum)
						}
						latencies = append(latencies, entry.latency)
					}
				}
				b.StopTimer()
				slices.Sort(latencies)
				if len(latencies) > 0 {
					p95Index := (len(latencies)*95+99)/100 - 1
					b.ReportMetric(float64(latencies[p95Index].Nanoseconds()), "p95-ns")
					b.ReportMetric(float64(len(latencies))/b.Elapsed().Seconds(), "queries/s")
				}
			})
		}
	}
}
