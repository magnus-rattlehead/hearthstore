package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// newBenchServer creates an in-memory test server for benchmarks.
func newBenchServer(b *testing.B) *Server {
	b.Helper()
	store, err := storage.New(b.TempDir())
	if err != nil {
		b.Fatalf("storage.New: %v", err)
	}
	b.Cleanup(func() { store.Close() })
	return New(store)
}

// seedBench inserts n documents into collection with fields produced by mkFields(i).
// Called before b.ResetTimer() so seeding cost is excluded from measurements.
func seedBench(b *testing.B, s *Server, collection string, n int, mkFields func(i int) map[string]*firestorepb.Value) {
	b.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if _, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent:       collectionParent(),
			CollectionId: collection,
			DocumentId:   fmt.Sprintf("doc%06d", i),
			Document:     &firestorepb.Document{Fields: mkFields(i)},
		}); err != nil {
			b.Fatalf("seedBench: %v", err)
		}
	}
}

// runBenchQuery executes a RunQuery and discards results.
func runBenchQuery(b *testing.B, s *Server, q *firestorepb.StructuredQuery) {
	b.Helper()
	stream := &fakeRunQueryStream{fakeServerStream: newFakeStream()}
	if err := s.RunQuery(&firestorepb.RunQueryRequest{
		Parent:    collectionParent(),
		QueryType: &firestorepb.RunQueryRequest_StructuredQuery{StructuredQuery: q},
	}, stream); err != nil {
		b.Fatalf("RunQuery: %v", err)
	}
}

// runBenchAggregation executes a RunAggregationQuery and discards results.
func runBenchAggregation(b *testing.B, s *Server, q *firestorepb.StructuredAggregationQuery) {
	b.Helper()
	stream := &fakeAggregationStream{fakeServerStream: newFakeStream()}
	if err := s.RunAggregationQuery(&firestorepb.RunAggregationQueryRequest{
		Parent: collectionParent(),
		QueryType: &firestorepb.RunAggregationQueryRequest_StructuredAggregationQuery{
			StructuredAggregationQuery: q,
		},
	}, stream); err != nil {
		b.Fatalf("RunAggregationQuery: %v", err)
	}
}

const benchN = 1000

// ── Query benchmarks ──────────────────────────────────────────────────────────

// BenchmarkQuery_SimpleEquality measures an equality filter over 1 000 docs.
// Half the docs have score=1, the other half have score=0; query matches ~500.
func BenchmarkQuery_SimpleEquality(b *testing.B) {
	s := newBenchServer(b)
	seedBench(b, s, "bench", benchN, func(i int) map[string]*firestorepb.Value {
		return map[string]*firestorepb.Value{"score": intVal(int64(i % 2))}
	})

	q := &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "bench"}},
		Where: fieldFilter("score", firestorepb.StructuredQuery_FieldFilter_EQUAL, intVal(1)),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runBenchQuery(b, s, q)
	}
}

// BenchmarkQuery_RangeOrderLimit measures a range filter + ORDER BY + LIMIT 10.
func BenchmarkQuery_RangeOrderLimit(b *testing.B) {
	s := newBenchServer(b)
	seedBench(b, s, "bench", benchN, func(i int) map[string]*firestorepb.Value {
		return map[string]*firestorepb.Value{"n": intVal(int64(i))}
	})

	q := &firestorepb.StructuredQuery{
		From:  []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "bench"}},
		Where: fieldFilter("n", firestorepb.StructuredQuery_FieldFilter_GREATER_THAN, intVal(500)),
		OrderBy: []*firestorepb.StructuredQuery_Order{
			{Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "n"}, Direction: firestorepb.StructuredQuery_ASCENDING},
		},
		Limit: wrapperspb.Int32(10),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runBenchQuery(b, s, q)
	}
}

// BenchmarkQuery_CursorPagination measures three pages of 10 results each.
// Cursors are constructed from known values (docs are sorted by n ascending).
func BenchmarkQuery_CursorPagination(b *testing.B) {
	s := newBenchServer(b)
	seedBench(b, s, "bench", benchN, func(i int) map[string]*firestorepb.Value {
		return map[string]*firestorepb.Value{"n": intVal(int64(i))}
	})

	orderBy := []*firestorepb.StructuredQuery_Order{
		{Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "n"}, Direction: firestorepb.StructuredQuery_ASCENDING},
	}
	limit := wrapperspb.Int32(10)

	// Page 1: n in [0, 9]
	page1 := &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "bench"}},
		OrderBy: orderBy,
		Limit:   limit,
	}
	// Page 2: n in [10, 19] — StartAt exclusive after n=9
	page2 := &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "bench"}},
		OrderBy: orderBy,
		Limit:   limit,
		StartAt: &firestorepb.Cursor{
			Values: []*firestorepb.Value{intVal(9)},
			Before: false, // exclusive: start AFTER n=9
		},
	}
	// Page 3: n in [20, 29] — StartAt exclusive after n=19
	page3 := &firestorepb.StructuredQuery{
		From:    []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "bench"}},
		OrderBy: orderBy,
		Limit:   limit,
		StartAt: &firestorepb.Cursor{
			Values: []*firestorepb.Value{intVal(19)},
			Before: false,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runBenchQuery(b, s, page1)
		runBenchQuery(b, s, page2)
		runBenchQuery(b, s, page3)
	}
}

// BenchmarkQuery_CompoundFilter measures a three-clause AND filter.
// Only 1 document matches all three conditions.
func BenchmarkQuery_CompoundFilter(b *testing.B) {
	s := newBenchServer(b)
	seedBench(b, s, "bench", benchN, func(i int) map[string]*firestorepb.Value {
		return map[string]*firestorepb.Value{
			"a": intVal(int64(i % 10)),
			"b": intVal(int64(i % 7)),
			"c": intVal(int64(i % 3)),
		}
	})

	q := &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "bench"}},
		Where: &firestorepb.StructuredQuery_Filter{
			FilterType: &firestorepb.StructuredQuery_Filter_CompositeFilter{
				CompositeFilter: &firestorepb.StructuredQuery_CompositeFilter{
					Op: firestorepb.StructuredQuery_CompositeFilter_AND,
					Filters: []*firestorepb.StructuredQuery_Filter{
						fieldFilter("a", firestorepb.StructuredQuery_FieldFilter_EQUAL, intVal(3)),
						fieldFilter("b", firestorepb.StructuredQuery_FieldFilter_EQUAL, intVal(3)),
						fieldFilter("c", firestorepb.StructuredQuery_FieldFilter_EQUAL, intVal(0)),
					},
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runBenchQuery(b, s, q)
	}
}

// ── Aggregation benchmarks ────────────────────────────────────────────────────

// BenchmarkQuery_AggregationCount measures COUNT(*) over 1 000 docs.
func BenchmarkQuery_AggregationCount(b *testing.B) {
	s := newBenchServer(b)
	seedBench(b, s, "bench", benchN, func(i int) map[string]*firestorepb.Value {
		return map[string]*firestorepb.Value{"n": intVal(int64(i))}
	})

	q := &firestorepb.StructuredAggregationQuery{
		QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "bench"}},
			},
		},
		Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{
			{
				Alias: "count",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Count_{
					Count: &firestorepb.StructuredAggregationQuery_Aggregation_Count{},
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runBenchAggregation(b, s, q)
	}
}

// BenchmarkQuery_AggregationSum measures SUM(n) over 1 000 docs.
func BenchmarkQuery_AggregationSum(b *testing.B) {
	s := newBenchServer(b)
	seedBench(b, s, "bench", benchN, func(i int) map[string]*firestorepb.Value {
		return map[string]*firestorepb.Value{"n": intVal(int64(i))}
	})

	q := &firestorepb.StructuredAggregationQuery{
		QueryType: &firestorepb.StructuredAggregationQuery_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "bench"}},
			},
		},
		Aggregations: []*firestorepb.StructuredAggregationQuery_Aggregation{
			{
				Alias: "total",
				Operator: &firestorepb.StructuredAggregationQuery_Aggregation_Sum_{
					Sum: &firestorepb.StructuredAggregationQuery_Aggregation_Sum{
						Field: &firestorepb.StructuredQuery_FieldReference{FieldPath: "n"},
					},
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runBenchAggregation(b, s, q)
	}
}

// ── Write benchmarks ──────────────────────────────────────────────────────────

// BenchmarkWrite_UpsertSimple measures writing a 3-field document.
func BenchmarkWrite_UpsertSimple(b *testing.B) {
	s := newBenchServer(b)
	ctx := context.Background()

	// Pre-create the doc so subsequent iterations are updates (not inserts).
	if _, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent: collectionParent(), CollectionId: "bench", DocumentId: "doc",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{
			"a": intVal(0), "b": strVal("x"), "c": boolVal(true),
		}},
	}); err != nil {
		b.Fatalf("create: %v", err)
	}

	name := docName("bench", "doc")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
			Document: &firestorepb.Document{
				Name: name,
				Fields: map[string]*firestorepb.Value{
					"a": intVal(int64(i)), "b": strVal("x"), "c": boolVal(true),
				},
			},
		}); err != nil {
			b.Fatalf("UpdateDocument: %v", err)
		}
	}
}

// BenchmarkWrite_UpsertLargeArray measures writing a document with a 50-element array.
func BenchmarkWrite_UpsertLargeArray(b *testing.B) {
	s := newBenchServer(b)
	ctx := context.Background()

	// Build a 50-element array value.
	arr := make([]*firestorepb.Value, 50)
	for i := range arr {
		arr[i] = intVal(int64(i))
	}
	arrVal := &firestorepb.Value{
		ValueType: &firestorepb.Value_ArrayValue{
			ArrayValue: &firestorepb.ArrayValue{Values: arr},
		},
	}

	if _, err := s.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent: collectionParent(), CollectionId: "bench", DocumentId: "arrdoc",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{"items": arrVal}},
	}); err != nil {
		b.Fatalf("create: %v", err)
	}

	name := docName("bench", "arrdoc")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
			Document: &firestorepb.Document{
				Name:   name,
				Fields: map[string]*firestorepb.Value{"items": arrVal},
			},
		}); err != nil {
			b.Fatalf("UpdateDocument: %v", err)
		}
	}
}

// ── Commit RPC benchmarks ─────────────────────────────────────────────────────

// benchDB returns the Firestore database resource path used by bench helpers.
func benchDB() string {
	return fmt.Sprintf("projects/%s/databases/%s", testProject, testDB)
}

// benchWrite builds a non-transactional Write_Update for a doc in collection c.
func benchWrite(collection, id string, fields map[string]*firestorepb.Value) *firestorepb.Write {
	return &firestorepb.Write{
		Operation: &firestorepb.Write_Update{
			Update: &firestorepb.Document{
				Name:   docName(collection, id),
				Fields: fields,
			},
		},
	}
}

// seedCommitDocs pre-creates n docs in collection so bench iterations are updates.
func seedCommitDocs(b *testing.B, s *Server, collection string, n int) {
	b.Helper()
	ctx := context.Background()
	writes := make([]*firestorepb.Write, n)
	for i := 0; i < n; i++ {
		writes[i] = benchWrite(collection, fmt.Sprintf("doc%06d", i),
			map[string]*firestorepb.Value{"n": intVal(int64(i))})
	}
	if _, err := s.Commit(ctx, &firestorepb.CommitRequest{Writes: writes}); err != nil {
		b.Fatalf("seedCommitDocs: %v", err)
	}
}

// BenchmarkCommit_SingleWrite measures a single-document Commit (update path).
func BenchmarkCommit_SingleWrite(b *testing.B) {
	s := newBenchServer(b)
	ctx := context.Background()
	seedCommitDocs(b, s, "csingle", 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Commit(ctx, &firestorepb.CommitRequest{
			Writes: []*firestorepb.Write{
				benchWrite("csingle", "doc000000", map[string]*firestorepb.Value{"n": intVal(int64(i))}),
			},
		}); err != nil {
			b.Fatalf("Commit: %v", err)
		}
	}
}

// BenchmarkCommit_Bulk100 measures a 100-write Commit (all updates).
func BenchmarkCommit_Bulk100(b *testing.B) {
	s := newBenchServer(b)
	ctx := context.Background()
	seedCommitDocs(b, s, "cbulk100", 100)

	writes := make([]*firestorepb.Write, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			writes[j] = benchWrite("cbulk100", fmt.Sprintf("doc%06d", j),
				map[string]*firestorepb.Value{"n": intVal(int64(i*100 + j))})
		}
		if _, err := s.Commit(ctx, &firestorepb.CommitRequest{Writes: writes}); err != nil {
			b.Fatalf("Commit: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)*100/b.Elapsed().Seconds(), "writes/s")
}

// BenchmarkCommit_Bulk500 measures a 500-write Commit, validating batch chunking.
func BenchmarkCommit_Bulk500(b *testing.B) {
	s := newBenchServer(b)
	ctx := context.Background()
	seedCommitDocs(b, s, "cbulk500", 500)

	writes := make([]*firestorepb.Write, 500)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 500; j++ {
			writes[j] = benchWrite("cbulk500", fmt.Sprintf("doc%06d", j),
				map[string]*firestorepb.Value{"n": intVal(int64(i*500 + j))})
		}
		if _, err := s.Commit(ctx, &firestorepb.CommitRequest{Writes: writes}); err != nil {
			b.Fatalf("Commit: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)*500/b.Elapsed().Seconds(), "writes/s")
}

// BenchmarkBatchWrite_100 measures a 100-write BatchWrite (currently N serial auto-commits).
func BenchmarkBatchWrite_100(b *testing.B) {
	s := newBenchServer(b)
	ctx := context.Background()
	seedCommitDocs(b, s, "bw100", 100)

	writes := make([]*firestorepb.Write, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			writes[j] = benchWrite("bw100", fmt.Sprintf("doc%06d", j),
				map[string]*firestorepb.Value{"n": intVal(int64(i*100 + j))})
		}
		if _, err := s.BatchWrite(ctx, &firestorepb.BatchWriteRequest{Writes: writes}); err != nil {
			b.Fatalf("BatchWrite: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)*100/b.Elapsed().Seconds(), "writes/s")
}

// ── Listen benchmarks ─────────────────────────────────────────────────────────

// startListener creates a controlledListenStream, starts the Listen goroutine,
// sends an AddTarget for the given collection (no token), and waits for the
// initial snapshot to complete (ADD + docs + CURRENT + NO_CHANGE).
// emptyInitial must be true iff the collection has no docs at AddTarget time.
func startListener(b *testing.B, s *Server, db, collection string, tid int32, expectedInitialResponses int) *controlledListenStream {
	b.Helper()
	st := newControlledListenStream()
	go func() { _ = s.Listen(st) }()
	st.sendReq(makeAddTargetReq(db, collection, tid))
	if !st.waitForNResponses(expectedInitialResponses, 30*time.Second) {
		b.Fatalf("startListener: timeout waiting for %d responses on %s", expectedInitialResponses, collection)
	}
	return st
}

// BenchmarkListen_Notify100Subs_1Write measures the fan-out cost of one write
// to 100 subscribers watching disjoint collections (only 1 matches).
func BenchmarkListen_Notify100Subs_1Write(b *testing.B) {
	s := newBenchServer(b)
	ctx := context.Background()
	db := benchDB()

	// 100 listeners on disjoint empty collections (bench-N). Empty → 3 initial responses each.
	streams := make([]*controlledListenStream, 100)
	for i := 0; i < 100; i++ {
		streams[i] = startListener(b, s, db, fmt.Sprintf("notify100-%d", i), 1, 3)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Write to notify100-0 only; only streams[0] should get a DocumentChange.
		target := len(streams[0].sent) + 2 // DocumentChange + NO_CHANGE
		if _, err := s.Commit(ctx, &firestorepb.CommitRequest{
			Writes: []*firestorepb.Write{
				benchWrite("notify100-0", fmt.Sprintf("doc%d", i), map[string]*firestorepb.Value{"n": intVal(int64(i))}),
			},
		}); err != nil {
			b.Fatalf("Commit: %v", err)
		}
		if !streams[0].waitForNResponses(target, 5*time.Second) {
			b.Fatal("timeout waiting for notification on streams[0]")
		}
	}
	b.StopTimer()

	for _, st := range streams {
		st.cancel()
	}
}

// BenchmarkListen_Notify1Sub_100Writes measures per-event dispatch cost:
// one listener receives 100 sequential single-doc commits per iteration.
func BenchmarkListen_Notify1Sub_100Writes(b *testing.B) {
	s := newBenchServer(b)
	ctx := context.Background()
	db := benchDB()

	st := startListener(b, s, db, "notify1sub", 1, 3)

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		base := len(st.sent)
		for i := 0; i < 100; i++ {
			if _, err := s.Commit(ctx, &firestorepb.CommitRequest{
				Writes: []*firestorepb.Write{
					benchWrite("notify1sub", fmt.Sprintf("d%d-%d", iter, i), map[string]*firestorepb.Value{"v": intVal(int64(i))}),
				},
			}); err != nil {
				b.Fatalf("Commit: %v", err)
			}
		}
		// 100 writes × (DocumentChange + NO_CHANGE) = 200 new responses.
		if !st.waitForNResponses(base+200, 10*time.Second) {
			b.Fatal("timeout waiting for 100 notifications")
		}
	}
	b.StopTimer()
	st.cancel()
}

// BenchmarkListen_InitialSnapshot_10kDocs measures sendInitialSnapshot latency
// for a collection with 10 000 documents.
func BenchmarkListen_InitialSnapshot_10kDocs(b *testing.B) {
	s := newBenchServer(b)
	seedBench(b, s, "snap10k", 10000, func(i int) map[string]*firestorepb.Value {
		return map[string]*firestorepb.Value{"n": intVal(int64(i))}
	})
	db := benchDB()
	// ADD + 10000 doc sends + CURRENT + NO_CHANGE = 10002.
	const wantResponses = 10002

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st := newControlledListenStream()
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = s.Listen(st)
		}()
		st.sendReq(makeAddTargetReq(db, "snap10k", 1))
		if !st.waitForNResponses(wantResponses, 60*time.Second) {
			b.Fatal("timeout waiting for initial snapshot")
		}
		st.cancel()
		<-done
	}
}

// BenchmarkListen_DiffSnapshot_100Changes measures sendDiffSnapshotByTime latency
// (which, like sendDiffSnapshot, must re-run the full query to repopulate t.sent)
// when resuming a 10k-doc collection after 100 changes.
func BenchmarkListen_DiffSnapshot_100Changes(b *testing.B) {
	s := newBenchServer(b)
	ctx := context.Background()
	seedBench(b, s, "diff10k", 10000, func(i int) map[string]*firestorepb.Value {
		return map[string]*firestorepb.Value{"n": intVal(int64(i))}
	})
	db := benchDB()

	// Capture a read_time before the 100 changes.
	readTime := timestamppb.Now()
	time.Sleep(2 * time.Millisecond) // ensure monotonicNow advances past readTime

	// Apply 100 changes so the diff has something to deliver.
	writes := make([]*firestorepb.Write, 100)
	for i := 0; i < 100; i++ {
		writes[i] = benchWrite("diff10k", fmt.Sprintf("doc%06d", i),
			map[string]*firestorepb.Value{"n": intVal(int64(i + 99999))})
	}
	if _, err := s.Commit(ctx, &firestorepb.CommitRequest{Writes: writes}); err != nil {
		b.Fatalf("seeding changes: %v", err)
	}

	// ADD + 100 changed docs + CURRENT + NO_CHANGE = 103.
	const wantResponses = 103

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st := newControlledListenStream()
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = s.Listen(st)
		}()
		// read_time token → sendDiffSnapshotByTime, which calls fetchTargetDocs for t.sent.
		st.sendReq(makeAddTargetReqWithReadTime(db, "diff10k", 1, readTime))
		if !st.waitForNResponses(wantResponses, 60*time.Second) {
			b.Fatal("timeout waiting for diff snapshot")
		}
		st.cancel()
		<-done
	}
}
