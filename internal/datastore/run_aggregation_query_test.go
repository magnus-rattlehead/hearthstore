package datastore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestAggregationCoveringExecution(t *testing.T) {
	s := newTestDsServer(t)
	seedKind(t, s, "CoverAggregate", []seedRow{
		{"a", map[string]*datastorepb.Value{"x": dsArray(dsInt(1), dsInt(1), dsInt(2)), "y": dsArray(dsInt(10), dsInt(20)), "tag": dsArray(dsStr("a"), dsStr("b"))}},
		{"b", map[string]*datastorepb.Value{"x": dsInt(3), "y": dsInt(30), "tag": dsStr("b")}},
		{"missing", map[string]*datastorepb.Value{"y": dsInt(90), "tag": dsStr("a")}},
		{"excluded", map[string]*datastorepb.Value{"x": {ExcludeFromIndexes: true, ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 90}}, "y": dsInt(90), "tag": dsStr("a")}},
	})
	idx, _, err := s.grpc.store.EnsureDsCompositeIndex(context.Background(), storage.DsCompositeIndex{Project: testProject, Kind: "CoverAggregate", Properties: []storage.DsIndexProperty{{Name: "tag"}, {Name: "x"}, {Name: "y"}}}, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, composite := range []bool{false, true} {
		t.Run(fmt.Sprintf("composite=%t", composite), func(t *testing.T) {
			q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "CoverAggregate"}}}
			aggs := []*datastorepb.AggregationQuery_Aggregation{countAgg("n"), sumAgg("s", "x"), avgAgg("a", "x")}
			wantCount, wantSum, wantAvg, access := int64(3), int64(6), float64(2), "builtin:x"
			if composite {
				q.Filter = andFilter(propFilter("tag", datastorepb.PropertyFilter_EQUAL, dsStr("a")), propFilter("x", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(1)))
				aggs = append(aggs, sumAgg("y", "y"))
				wantCount, wantSum, wantAvg, access = 4, 6, 1.5, "composite"
			}
			req := &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: q}, Aggregations: aggs}}, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}}
			before := proto.Clone(req)
			ctx, work := storage.WithQueryWork(context.Background(), 1)
			response, err := s.grpc.RunAggregationQuery(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			props := response.Batch.AggregationResults[0].AggregateProperties
			if props["n"].GetIntegerValue() != wantCount || props["s"].GetIntegerValue() != wantSum || props["a"].GetDoubleValue() != wantAvg || composite && props["y"].GetIntegerValue() != 60 {
				t.Fatalf("incorrect aggregate: %v", props)
			}
			debug := response.ExplainMetrics.ExecutionStats.DebugStats.Fields
			reads, err := strconv.ParseInt(debug["documents_scanned"].GetStringValue(), 10, 64)
			if err != nil || reads != 0 || work.Snapshot()[storage.WorkScratchWriteBytes] != 0 {
				t.Fatalf("covering query fetched entities or sorted: reads=%d err=%v work=%v", reads, err, work.Snapshot())
			}
			plan := response.ExplainMetrics.PlanSummary.IndexesUsed[0].Fields
			if plan["access_path"].GetStringValue() != access || composite && plan["index_id"].GetStringValue() != idx.ID {
				t.Fatalf("unexpected plan: %v", plan)
			}
			fallbackReq := proto.Clone(req).(*datastorepb.RunAggregationQueryRequest)
			fallbackReq.ReadOptions = &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: response.Batch.ReadTime}}
			fallback, err := s.grpc.RunAggregationQuery(context.Background(), fallbackReq)
			if err != nil || !proto.Equal(fallback.GetBatch().GetAggregationResults()[0], response.Batch.AggregationResults[0]) {
				t.Fatalf("fallback mismatch: result=%v err=%v", fallback, err)
			}
			planReq := proto.Clone(req).(*datastorepb.RunAggregationQueryRequest)
			planReq.ExplainOptions.Analyze = false
			planCtx, planWork := storage.WithQueryWork(context.Background(), 0)
			planOnly, err := s.grpc.RunAggregationQuery(planCtx, planReq)
			if err != nil || !proto.Equal(planOnly.GetExplainMetrics().GetPlanSummary(), response.ExplainMetrics.PlanSummary) || planWork.Snapshot()[storage.WorkIndexEntries] != 0 {
				t.Fatalf("plan-only differs or scanned: response=%v err=%v work=%v", planOnly, err, planWork.Snapshot())
			}
			if !proto.Equal(before, req) {
				t.Fatal("aggregation mutated input")
			}
			cancelCtx, canceledWork := storage.WithQueryWork(context.Background(), 1)
			cancelCtx, cancel := context.WithCancel(cancelCtx)
			defer cancel()
			cancelCtx = &workCancelContext{Context: cancelCtx, work: canceledWork, kind: storage.WorkIndexEntries, after: 1, cancel: cancel}
			if canceled, err := s.grpc.RunAggregationQuery(cancelCtx, req); err == nil || canceled != nil || canceledWork.Snapshot()[storage.WorkIndexEntries] != 1 {
				t.Fatalf("native cancellation returned partial success or kept scanning: response=%v err=%v work=%v", canceled, err, canceledWork.Snapshot())
			}
			if len(s.grpc.querySlots) != 0 {
				t.Fatal("canceled native RPC leaked admission")
			}
		})
	}
}

func TestAggregationUnavailableIndexFallsBack(t *testing.T) {
	for _, state := range []string{"missing", storage.DsIndexCreating, storage.DsIndexError} {
		t.Run(state, func(t *testing.T) {
			s := newTestDsServer(t)
			seedKind(t, s, "UnavailableAggregate", []seedRow{{"one", map[string]*datastorepb.Value{"x": dsArray(dsInt(1), dsInt(2)), "y": dsArray(dsInt(3), dsInt(4))}}})
			idx := storage.DsCompositeIndex{Project: testProject, Kind: "UnavailableAggregate", State: state, Properties: []storage.DsIndexProperty{{Name: "x"}, {Name: "y"}}}
			idx.ID = storage.DsCompositeIndexID(idx.Kind, false, idx.Properties)
			if state != "missing" {
				// Persist lifecycle fault fixtures without starting a racing build.
				// The metadata key layout is defined by storage.indexKey.
				raw, err := json.Marshal(idx)
				if err != nil {
					t.Fatal(err)
				}
				key := []byte("meta/index/" + base64.RawURLEncoding.EncodeToString([]byte(testProject)) + "/" + idx.ID)
				if err := s.grpc.store.RunInTx(func(tx *storage.Txn) error { return tx.Set(key, raw) }); err != nil {
					t.Fatal(err)
				}
			}
			before, err := s.grpc.store.ListDsCompositeIndexes(testProject)
			if err != nil {
				t.Fatal(err)
			}
			req := &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, ExplainOptions: &datastorepb.ExplainOptions{}, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{
				QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: idx.Kind}}}}, Aggregations: []*datastorepb.AggregationQuery_Aggregation{countAgg("n"), sumAgg("x", "x"), sumAgg("y", "y")},
			}}}
			for _, analyze := range []bool{false, true} {
				req.ExplainOptions.Analyze = analyze
				response, err := s.grpc.RunAggregationQuery(context.Background(), req)
				if err != nil {
					t.Fatal(err)
				}
				if got := response.ExplainMetrics.PlanSummary.IndexesUsed[0].Fields["access_path"].GetStringValue(); got != "disk_sort" {
					t.Fatalf("unavailable index plan=%s", got)
				}
				if analyze {
					props := response.Batch.AggregationResults[0].AggregateProperties
					if props["n"].GetIntegerValue() != 4 || props["x"].GetIntegerValue() != 6 || props["y"].GetIntegerValue() != 14 {
						t.Fatalf("wrong fallback result: %v", props)
					}
				}
			}
			after, err := s.grpc.store.ListDsCompositeIndexes(testProject)
			if err != nil || fmt.Sprint(before) != fmt.Sprint(after) {
				t.Fatalf("aggregation changed index catalog: before=%v after=%v err=%v", before, after, err)
			}
		})
	}
}

func TestAggregationCoveringContinuation(t *testing.T) {
	for _, composite := range []bool{false, true} {
		t.Run(fmt.Sprintf("composite=%t", composite), func(t *testing.T) {
			s := newTestDsServer(t)
			bulkSeed(t, s, 8)
			q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Widget"}}, Order: []*datastorepb.PropertyOrder{
				{Property: &datastorepb.PropertyReference{Name: "score"}, Direction: datastorepb.PropertyOrder_ASCENDING},
				{Property: &datastorepb.PropertyReference{Name: "__key__"}, Direction: datastorepb.PropertyOrder_ASCENDING},
			}, Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "score"}}}, Limit: wrapperspb.Int32(1)}
			if composite {
				q.Filter = propFilter("tier", datastorepb.PropertyFilter_EQUAL, dsStr("common"))
				definition, _ := queryIndexDefinition(q, false)
				definition.Project = testProject
				if _, _, err := s.grpc.store.EnsureDsCompositeIndex(context.Background(), definition, true); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), aggregationEntriesKey{}, true))
			defer cancel()
			owner := querySnapshot{aggregation: &aggregationAccess{}}
			defer func() {
				if err := owner.close(); err != nil {
					t.Error(err)
				}
			}()
			req := &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}}
			first, err := s.grpc.runQueryWithSnapshot(ctx, req, &owner)
			if err != nil {
				t.Fatal(err)
			}
			if owner.fallback != nil || owner.aggregation.builtin == "" && owner.aggregation.index == nil {
				t.Fatal("continuation test did not select a covering index")
			}
			upsertEntity(t, s, dsEntity(dsKey("Widget", "w000002"), map[string]*datastorepb.Value{"score": dsInt(999), "tier": dsStr("common")}))
			q.StartCursor = first.Batch.EndCursor
			req.ReadOptions = &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: first.Batch.ReadTime}}
			for _, want := range []int64{1, 2} {
				if composite && want == 1 {
					continue // The equality prefix excludes score zero.
				}
				next, err := s.grpc.runQueryWithSnapshot(ctx, req, &owner)
				if err != nil || len(next.GetBatch().GetEntityResults()) != 1 || next.Batch.EntityResults[0].Entity.Properties["score"].GetIntegerValue() != want || !proto.Equal(first.Batch.ReadTime, next.Batch.ReadTime) {
					t.Fatalf("snapshot continuation wanted %d: %v err=%v", want, next, err)
				}
				q.StartCursor = next.Batch.EndCursor
			}
			cancel()
			if _, err := s.grpc.runQueryWithSnapshot(ctx, req, &owner); err == nil {
				t.Fatal("canceled native continuation succeeded")
			}
			if err := owner.close(); err != nil {
				t.Fatal(err)
			}
			if got := runAggKind(t, s, "Widget", nil, []*datastorepb.AggregationQuery_Aggregation{sumAgg("s", "score")}).Batch.AggregationResults[0].AggregateProperties["s"].GetIntegerValue(); got != 1025 {
				t.Fatalf("fresh read after cleanup: %d", got)
			}
		})
	}
}

func TestAggregationRetainedPageBounds(t *testing.T) {
	s := newTestDsServer(t)
	if err := s.grpc.store.ConfigureExactQueries(2<<20, 1); err != nil {
		t.Fatal(err)
	}
	bulkSeedLongPaths(t, s, 2000)
	for _, tc := range []struct {
		name          string
		offset, limit int32
		desc          bool
		count, sum    int64
	}{
		{"offset across pages", 1200, 600, false, 600, 899700},
		{"descending", 50, 600, true, 600, 989700},
		{"zero limit", 0, 0, false, 0, 0},
		{"offset past end", 2100, 600, false, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			direction := datastorepb.PropertyOrder_ASCENDING
			if tc.desc {
				direction = datastorepb.PropertyOrder_DESCENDING
			}
			q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Widget"}}, Offset: tc.offset, Limit: wrapperspb.Int32(tc.limit), Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "score"}, Direction: direction}}}
			req := &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: q}, Aggregations: []*datastorepb.AggregationQuery_Aggregation{countAgg("n"), sumAgg("s", "score")}}}}
			before := proto.Clone(req)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			response, err := s.grpc.RunAggregationQuery(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			props := response.Batch.AggregationResults[0].AggregateProperties
			if props["n"].GetIntegerValue() != tc.count || props["s"].GetIntegerValue() != tc.sum {
				t.Fatalf("results=%v, want count=%d sum=%d", props, tc.count, tc.sum)
			}
			if !proto.Equal(before, req) {
				t.Fatal("request mutated")
			}
		})
	}
}

func TestAggregationRetainedStreamLifecycle(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := store.ConfigureExactQueries(2<<20, 1); err != nil {
		t.Fatal(err)
	}
	s := New(store)
	bulkSeedLongPaths(t, s, 2000)
	for _, mode := range []string{"snapshot", "cancel", "read failure", "preparation cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), aggregationEntriesKey{}, true))
			defer cancel()
			ctx, work := storage.WithQueryWork(ctx, 1)
			q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Widget"}}, Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "score"}}}, Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "score"}, Direction: datastorepb.PropertyOrder_ASCENDING}}, Limit: wrapperspb.Int32(1)}
			var owner querySnapshot
			defer func() {
				if err := owner.close(); err != nil {
					t.Error(err)
				}
			}()
			if mode == "read failure" || mode == "preparation cancel" {
				preparedCtx, planned, condition, err := prepareQueryExecution(ctx, q, "")
				if err != nil {
					t.Fatal(err)
				}
				if mode == "preparation cancel" {
					cancel()
				}
				stream, _, err := s.grpc.prepareFallbackRows(preparedCtx, store.ReadTime(), testProject, defaultDatabase, "", "Widget", "", planned, condition, nil, nil)
				if mode == "preparation cancel" {
					if err == nil {
						if stream != nil {
							stream.close()
						}
						t.Fatal("canceled preparation succeeded")
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					owner.fallback = stream
					files, err := filepath.Glob(filepath.Join(dir, "scratch", "query-*", "run-*"))
					if err != nil || len(files) == 0 {
						t.Fatalf("no spill runs: %v", err)
					}
					// Only files under this test's disposable store are faulted.
					if err := os.Remove(files[0]); err != nil {
						t.Fatal(err)
					}
					if _, _, err := stream.page(ctx, 1); err == nil {
						t.Fatal("missing run did not fail")
					}
				}
			} else {
				req := &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}}
				first, err := s.grpc.runQueryWithSnapshot(ctx, req, &owner)
				if err != nil {
					t.Fatal(err)
				}
				if first.Batch.EntityResults[0].Entity.Properties["score"].GetIntegerValue() != 0 {
					t.Fatal("wrong first row")
				}
				q.StartCursor = first.Batch.EndCursor
				if mode == "cancel" {
					cancel()
					if _, err := s.grpc.runQueryWithSnapshot(ctx, req, &owner); err == nil {
						t.Fatal("canceled continuation succeeded")
					}
				} else {
					upsertEntity(t, s, dsEntity(dsKey("Widget", fmt.Sprintf("w%01088d", 1)), map[string]*datastorepb.Value{"score": dsInt(99999)}))
					second, err := s.grpc.runQueryWithSnapshot(ctx, req, &owner)
					if err != nil {
						t.Fatal(err)
					}
					if second.Batch.EntityResults[0].Entity.Properties["score"].GetIntegerValue() != 1 || !proto.Equal(first.Batch.ReadTime, second.Batch.ReadTime) {
						t.Fatal("snapshot changed between pages")
					}
					if got := work.Snapshot()[storage.WorkIndexEntries]; got != 2000 {
						t.Fatalf("source revisited: %d", got)
					}
					upsertEntity(t, s, dsEntity(dsKey("Widget", fmt.Sprintf("w%01088d", 1)), map[string]*datastorepb.Value{"score": dsInt(1)}))
				}
			}
			if err := owner.close(); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(filepath.Join(dir, "scratch"))
			if err != nil || len(entries) != 0 {
				t.Fatalf("scratch leaked: %v %v", entries, err)
			}
			// Acquire all configured scratch slots to prove cleanup returned admission.
			probeCtx, probeCancel := context.WithTimeout(context.Background(), time.Second)
			defer probeCancel()
			for range 2 {
				acc, err := store.NewPathAccumulator(probeCtx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := acc.Close(); err != nil {
						t.Error(err)
					}
				}()
			}
		})
	}
}

func TestCompatibilityAggregationCursorDimensions(t *testing.T) {
	s := newTestDsServer(t)
	for i := 1; i <= 4; i++ {
		upsertEntity(t, s, dsEntity(dsKey("AddedDimensions", fmt.Sprint(i)), map[string]*datastorepb.Value{"n": dsInt(int64(i))}))
	}
	for _, direction := range []datastorepb.PropertyOrder_Direction{datastorepb.PropertyOrder_ASCENDING, datastorepb.PropertyOrder_DESCENDING} {
		q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "AddedDimensions"}}, Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "__key__"}, Direction: direction}}, Limit: wrapperspb.Int32(2)}
		first, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
		if err != nil {
			t.Fatal(err)
		}
		for _, reverse := range []bool{false, true} {
			for _, end := range []bool{false, true} {
				query := proto.Clone(q).(*datastorepb.Query)
				query.Limit = nil
				if reverse {
					query.Order[0].Direction = datastorepb.PropertyOrder_DESCENDING
					if direction == datastorepb.PropertyOrder_DESCENDING {
						query.Order[0].Direction = datastorepb.PropertyOrder_ASCENDING
					}
				}
				if end {
					query.EndCursor = first.Batch.EndCursor
				} else {
					query.StartCursor = first.Batch.EndCursor
				}
				r, err := s.grpc.RunAggregationQuery(context.Background(), &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: query}, Aggregations: []*datastorepb.AggregationQuery_Aggregation{countAgg("c"), sumAgg("s", "n")}}}})
				if err != nil {
					t.Fatal(err)
				}
				// Java cursorBound has no non-key postfix values here. In the
				// [key,n] schema it pads slot zero, then appends the cursor key.
				// In sort coordinates: [after-all,key] normally; [before-all,key]
				// when reversed. The first slot alone decides every comparison.
				count, sum := int64(0), int64(0)
				if end != reverse {
					count, sum = 4, 10
				}
				got := r.Batch.AggregationResults[0].AggregateProperties
				if got["c"].GetIntegerValue() != count || got["s"].GetIntegerValue() != sum {
					t.Errorf("direction=%v reverse=%t end=%t got=%v want=%d/%d", direction, reverse, end, got, count, sum)
				}
			}
		}
	}
}

func TestAggregationQueryCursors(t *testing.T) {
	s := newTestDsServer(t)
	seedKind(t, s, "AggregateCursor", []seedRow{
		{"1", map[string]*datastorepb.Value{"x": dsInt(1), "y": dsInt(9)}},
		{"2", map[string]*datastorepb.Value{"x": dsInt(2), "y": dsInt(8)}},
		{"3", map[string]*datastorepb.Value{"x": dsInt(3), "y": dsInt(7)}},
	})
	for _, mode := range []int{0, 1, 2} {
		ordered := mode != 0
		q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "AggregateCursor"}}, Limit: wrapperspb.Int32(1)}
		if mode == 1 {
			q.Order = []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}, Direction: datastorepb.PropertyOrder_ASCENDING}}
		}
		if mode == 2 {
			q.Filter = propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(0))
		}
		first, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
		if err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"", "x", "y"} {
			for _, end := range []bool{false, true} {
				query := proto.Clone(q).(*datastorepb.Query)
				query.Limit = nil
				if end {
					query.EndCursor = first.Batch.EndCursor
				} else {
					query.StartCursor = first.Batch.EndCursor
				}
				aggs := []*datastorepb.AggregationQuery_Aggregation{countAgg("n")}
				if field != "" {
					aggs = append(aggs, sumAgg("s", field))
				}
				response, err := s.grpc.RunAggregationQuery(context.Background(), &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: query}, Aggregations: aggs}}})
				if err != nil {
					t.Errorf("ordered=%t field=%s end=%t: %v", ordered, field, end, err)
					continue
				}
				count, sum := int64(2), int64(5)
				if field == "y" {
					sum = 15
				}
				if end {
					count, sum = 1, 1
					if field == "y" {
						sum = 9
					}
				}
				// Java pads an omitted ordered dimension with the after-row bound.
				if !ordered && field != "" {
					count, sum = 0, 0
					if end {
						count, sum = 3, 6
						if field == "y" {
							sum = 24
						}
					}
				}
				values := response.Batch.AggregationResults[0].AggregateProperties
				if values["n"].GetIntegerValue() != count || field != "" && values["s"].GetIntegerValue() != sum {
					t.Errorf("ordered=%t field=%s end=%t: %v, want %d/%d", ordered, field, end, values, count, sum)
				}
			}
		}
	}
}

func TestAggregationProjectionValidation(t *testing.T) {
	s := newTestDsServer(t)
	for _, explain := range []*datastorepb.ExplainOptions{nil, {}, {Analyze: true}} {
		for _, field := range []string{"x", "y", "__key__"} {
			for _, agg := range []*datastorepb.AggregationQuery_Aggregation{sumAgg("s", field), avgAgg("a", field)} {
				q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Validation"}}, Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "x"}}}}
				_, err := s.grpc.RunAggregationQuery(context.Background(), &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, ExplainOptions: explain, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: q}, Aggregations: []*datastorepb.AggregationQuery_Aggregation{agg}}}})
				want := codes.InvalidArgument
				if field == "x" {
					want = codes.OK
				}
				if status.Code(err) != want {
					t.Errorf("field=%s explain=%v agg=%v: %v, want %s", field, explain, agg, err, want)
				}
			}
		}
	}
}

func TestAggregationEntryWorkScalesWithValues(t *testing.T) {
	s := newTestDsServer(t)
	var previous uint64
	for _, size := range []int{128, 256} {
		kind := fmt.Sprintf("AggregateWork%d", size)
		values := make([]*datastorepb.Value, size)
		for i := range values {
			values[i] = dsInt(int64(i + 1))
		}
		seedKind(t, s, kind, []seedRow{{"one", map[string]*datastorepb.Value{"x": dsArray(values...)}}})
		ctx, work := storage.WithQueryWork(context.Background(), 0)
		response, err := s.grpc.RunAggregationQuery(ctx, &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}}}, Aggregations: []*datastorepb.AggregationQuery_Aggregation{sumAgg("s", "x")}}}})
		if err != nil {
			t.Fatal(err)
		}
		if sum := response.Batch.AggregationResults[0].AggregateProperties["s"].GetIntegerValue(); sum != int64(size*(size+1)/2) {
			t.Fatalf("sum=%d size=%d", sum, size)
		}
		attempts := work.Snapshot()[storage.WorkAttempts]
		if previous > 0 && attempts > 3*previous {
			t.Fatalf("doubling values grew work from %d to %d: repeated full-domain scans", previous, attempts)
		}
		previous = attempts
	}
}

func TestAggregationIndexEntryContract(t *testing.T) {
	s := newTestDsServer(t)
	seedKind(t, s, "AggregateEntries", []seedRow{
		{"a", map[string]*datastorepb.Value{"x": dsArray(dsInt(1), dsInt(1), dsInt(2), dsInt(9)), "y": dsArray(dsInt(10), dsInt(20))}},
		{"b", map[string]*datastorepb.Value{"x": dsArray(dsInt(3), dsInt(4))}},
		{"c", map[string]*datastorepb.Value{"y": dsArray(dsInt(30))}},
	})
	for _, tc := range []struct {
		name        string
		filter      *datastorepb.Filter
		sums        []string
		count, x, y int64
	}{
		{"count", nil, nil, 3, 0, 0},
		{"sum_x", nil, []string{"x"}, 5, 19, 0},
		{"sum_y", nil, []string{"y"}, 3, 0, 60},
		{"joint", nil, []string{"x", "y"}, 6, 24, 90},
		{"range_count", propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(0)), nil, 5, 0, 0},
		{"range_joint", propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(0)), []string{"x", "y"}, 6, 24, 90},
		{"equal_count", propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)), nil, 1, 0, 0},
		{"equal_sum", propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)), []string{"x"}, 3, 3, 0},
		{"ordered_or_prefix", orFilter(propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(9)), propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1))), []string{"x"}, 3, 27, 0},
		{"ordered_and_prefix", andFilter(propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(9)), propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1))), []string{"x"}, 3, 27, 0},
		{"ordered_in_prefix", propFilter("x", datastorepb.PropertyFilter_IN, dsArray(dsInt(2), dsInt(1))), []string{"x"}, 3, 6, 0},
		{"in_sum", propFilter("x", datastorepb.PropertyFilter_IN, dsArray(dsInt(1), dsInt(2))), []string{"x"}, 3, 3, 0},
		{"independent_equal", andFilter(propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(9))), []string{"x", "y"}, 6, 6, 90},
		{"or_implicit_order", orFilter(propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(0)), propFilter("y", datastorepb.PropertyFilter_EQUAL, dsInt(30))), nil, 5, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			aggs := []*datastorepb.AggregationQuery_Aggregation{countAgg("n")}
			for _, name := range tc.sums {
				aggs = append(aggs, sumAgg(name, name))
			}
			result := runAggKind(t, s, "AggregateEntries", tc.filter, aggs)
			values := result.Batch.AggregationResults[0].AggregateProperties
			if values["n"].GetIntegerValue() != tc.count || values["x"].GetIntegerValue() != tc.x || values["y"].GetIntegerValue() != tc.y {
				t.Fatalf("got %v, want n=%d x=%d y=%d", values, tc.count, tc.x, tc.y)
			}
		})
	}
}

func TestAggregationNumericContract(t *testing.T) {
	s := newTestDsServer(t)
	for _, tc := range []struct {
		name    string
		values  []*datastorepb.Value
		wantSum int64
		wantAvg float64
		wantNaN bool
	}{
		{"exact_integer_sum", []*datastorepb.Value{dsInt(1 << 53), dsInt(1)}, (1 << 53) + 1, 1 << 52, false},
		{"intermediate_integer_overflow", []*datastorepb.Value{dsInt(math.MaxInt64), dsInt(1), dsInt(-math.MaxInt64)}, 1, 1.0 / 3, false},
		{"timestamps", []*datastorepb.Value{
			{ValueType: &datastorepb.Value_TimestampValue{TimestampValue: timestamppb.New(time.Unix(-1, 123456000))}},
			{ValueType: &datastorepb.Value_TimestampValue{TimestampValue: timestamppb.New(time.Unix(2, 0))}},
		}, 1123456, 561728, false},
		{"nan", []*datastorepb.Value{{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: math.NaN()}}, dsInt(2)}, 0, 0, true},
		{"opposing_infinities", []*datastorepb.Value{{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: math.Inf(1)}}, {ValueType: &datastorepb.Value_DoubleValue{DoubleValue: math.Inf(-1)}}}, 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kind := "NumericContract" + tc.name
			var rows []seedRow
			for i, value := range tc.values {
				rows = append(rows, seedRow{fmt.Sprint(i), map[string]*datastorepb.Value{"x": value}})
			}
			seedKind(t, s, kind, rows)
			response := runAggKind(t, s, kind, nil, []*datastorepb.AggregationQuery_Aggregation{sumAgg("s", "x"), avgAgg("a", "x")})
			props := response.Batch.AggregationResults[0].AggregateProperties
			average, ok := props["a"].ValueType.(*datastorepb.Value_DoubleValue)
			if !ok {
				t.Fatalf("AVG = %v, want double", props["a"])
			}
			if tc.wantNaN {
				if !math.IsNaN(average.DoubleValue) || !math.IsNaN(props["s"].GetDoubleValue()) {
					t.Fatalf("expected NaN SUM/AVG, got %v", props)
				}
			} else {
				integer, ok := props["s"].ValueType.(*datastorepb.Value_IntegerValue)
				if !ok || integer.IntegerValue != tc.wantSum || average.DoubleValue != tc.wantAvg {
					t.Fatalf("SUM/AVG = %v, want %d/%v", props, tc.wantSum, tc.wantAvg)
				}
			}
		})
	}
}

func testAggregationUnionReusesTopLevelQueryAdmission(t *testing.T) {
	s := newTestDsServer(t)
	s.grpc.querySlots = make(chan struct{}, 1)
	seedKind(t, s, "Admission", []seedRow{
		{"a", map[string]*datastorepb.Value{"color": dsStr("blue"), "score": dsInt(1)}},
		{"b", map[string]*datastorepb.Value{"color": dsStr("red"), "score": dsInt(2)}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := s.grpc.RunAggregationQuery(ctx, &datastorepb.RunAggregationQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{
			AggregationQuery: &datastorepb.AggregationQuery{
				QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: &datastorepb.Query{
					Kind: []*datastorepb.KindExpression{{Name: "Admission"}},
					Filter: orFilter(
						propFilter("color", datastorepb.PropertyFilter_EQUAL, dsStr("blue")),
						propFilter("score", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(2)),
					),
				}},
				Aggregations: []*datastorepb.AggregationQuery_Aggregation{sumAgg("total", "score")},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := response.GetBatch().GetAggregationResults()[0].GetAggregateProperties()["total"].GetIntegerValue(); got != 3 {
		t.Fatalf("sum = %d, want 3", got)
	}
}

func testQueryAdmissionHonorsCancellation(t *testing.T) {
	g := newGRPCServerWithOptions(nil, nil, Options{QueryConcurrency: 1})
	release, err := g.acquireQuerySlot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.acquireQuerySlot(ctx); err == nil {
		t.Fatal("expected canceled admission error")
	}
}

func countAgg(alias string) *datastorepb.AggregationQuery_Aggregation {
	return &datastorepb.AggregationQuery_Aggregation{
		Alias: alias,
		Operator: &datastorepb.AggregationQuery_Aggregation_Count_{
			Count: &datastorepb.AggregationQuery_Aggregation_Count{},
		},
	}
}

func sumAgg(alias, prop string) *datastorepb.AggregationQuery_Aggregation {
	return &datastorepb.AggregationQuery_Aggregation{
		Alias: alias,
		Operator: &datastorepb.AggregationQuery_Aggregation_Sum_{
			Sum: &datastorepb.AggregationQuery_Aggregation_Sum{
				Property: &datastorepb.PropertyReference{Name: prop},
			},
		},
	}
}

func avgAgg(alias, prop string) *datastorepb.AggregationQuery_Aggregation {
	return &datastorepb.AggregationQuery_Aggregation{
		Alias: alias,
		Operator: &datastorepb.AggregationQuery_Aggregation_Avg_{
			Avg: &datastorepb.AggregationQuery_Aggregation_Avg{
				Property: &datastorepb.PropertyReference{Name: prop},
			},
		},
	}
}

func runAggKind(t *testing.T, s *Server, kind string, filter *datastorepb.Filter, aggs []*datastorepb.AggregationQuery_Aggregation) *datastorepb.RunAggregationQueryResponse {
	t.Helper()
	q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}, Filter: filter}
	var resp datastorepb.RunAggregationQueryResponse
	mustPost(t, s, "runAggregationQuery", &datastorepb.RunAggregationQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{
			AggregationQuery: &datastorepb.AggregationQuery{
				QueryType:    &datastorepb.AggregationQuery_NestedQuery{NestedQuery: q},
				Aggregations: aggs,
			},
		},
	}, &resp)
	return &resp
}

func TestAggregation(t *testing.T) {
	s := newTestDsServer(t)
	t.Run("numeric_operators", func(t *testing.T) {
		const kind = "NumericOperators"
		seedKind(t, s, kind, []seedRow{
			{"a", map[string]*datastorepb.Value{"score": dsInt(10)}},
			{"b", map[string]*datastorepb.Value{"score": dsInt(20)}},
			{"c", map[string]*datastorepb.Value{"score": dsInt(30)}},
		})
		for _, tc := range []struct {
			name string
			aggs []*datastorepb.AggregationQuery_Aggregation
			want map[string]*datastorepb.Value
		}{
			{"count", []*datastorepb.AggregationQuery_Aggregation{countAgg("n")}, map[string]*datastorepb.Value{"n": dsInt(3)}},
			{"sum", []*datastorepb.AggregationQuery_Aggregation{sumAgg("total", "score")}, map[string]*datastorepb.Value{"total": dsInt(60)}},
			{"average", []*datastorepb.AggregationQuery_Aggregation{avgAgg("avg", "score")}, map[string]*datastorepb.Value{"avg": dsDouble(20)}},
			{"multiple", []*datastorepb.AggregationQuery_Aggregation{countAgg("n"), sumAgg("total", "score")}, map[string]*datastorepb.Value{"n": dsInt(3), "total": dsInt(60)}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				response := runAggKind(t, s, kind, nil, tc.aggs)
				results := response.Batch.AggregationResults
				want := &datastorepb.AggregationResult{AggregateProperties: tc.want}
				if len(results) != 1 || !proto.Equal(results[0], want) {
					t.Fatalf("results=%v, want %v", results, want)
				}
			})
		}
	})

	tests := []struct {
		name string
		run  func(t *testing.T, s *Server, kind string)
	}{
		{
			name: "count_empty",
			run: func(t *testing.T, s *Server, kind string) {
				resp := runAggKind(t, s, kind, nil, []*datastorepb.AggregationQuery_Aggregation{countAgg("n")})
				if n := resp.Batch.AggregationResults[0].AggregateProperties["n"].GetIntegerValue(); n != 0 {
					t.Errorf("want 0, got %d", n)
				}
			},
		},
		{
			name: "count_with_eq_filter",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"color": dsStr("red")}},
					{"b", map[string]*datastorepb.Value{"color": dsStr("blue")}},
					{"c", map[string]*datastorepb.Value{"color": dsStr("red")}},
				})
				resp := runAggKind(t, s, kind,
					propFilter("color", datastorepb.PropertyFilter_EQUAL, dsStr("red")),
					[]*datastorepb.AggregationQuery_Aggregation{countAgg("n")},
				)
				if n := resp.Batch.AggregationResults[0].AggregateProperties["n"].GetIntegerValue(); n != 2 {
					t.Errorf("want 2 red, got %d", n)
				}
			},
		},
		{
			name: "count_limit",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"v": dsInt(1)}},
					{"b", map[string]*datastorepb.Value{"v": dsInt(2)}},
					{"c", map[string]*datastorepb.Value{"v": dsInt(3)}},
					{"d", map[string]*datastorepb.Value{"v": dsInt(4)}},
					{"e", map[string]*datastorepb.Value{"v": dsInt(5)}},
				})
				var resp datastorepb.RunAggregationQueryResponse
				mustPost(t, s, "runAggregationQuery", &datastorepb.RunAggregationQueryRequest{
					ProjectId: testProject,
					QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{
						AggregationQuery: &datastorepb.AggregationQuery{
							QueryType: &datastorepb.AggregationQuery_NestedQuery{
								NestedQuery: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}},
							},
							Aggregations: []*datastorepb.AggregationQuery_Aggregation{{
								Alias: "n",
								Operator: &datastorepb.AggregationQuery_Aggregation_Count_{
									Count: &datastorepb.AggregationQuery_Aggregation_Count{UpTo: wrapperspb.Int64(3)},
								},
							}},
						},
					},
				}, &resp)
				if n := resp.Batch.AggregationResults[0].AggregateProperties["n"].GetIntegerValue(); n != 3 {
					t.Errorf("count_limit: want 3, got %d", n)
				}
			},
		},
		{
			name: "avg_missing_field",
			run: func(t *testing.T, s *Server, kind string) {
				seedKind(t, s, kind, []seedRow{
					{"a", map[string]*datastorepb.Value{"score": dsInt(10)}},
					{"b", map[string]*datastorepb.Value{"score": dsInt(20)}},
				})
				resp := runAggKind(t, s, kind, nil, []*datastorepb.AggregationQuery_Aggregation{avgAgg("avg", "nonexistent")})
				v := resp.Batch.AggregationResults[0].AggregateProperties["avg"]
				if _, ok := v.ValueType.(*datastorepb.Value_NullValue); !ok {
					t.Errorf("avg of missing property: want null, got %v", v)
				}
			},
		},
		{
			name: "count_filtered_pushdown",
			run: func(t *testing.T, s *Server, kind string) {
				rows := make([]seedRow, 100)
				for i := range rows {
					status := "inactive"
					if i < 50 {
						status = "active"
					}
					rows[i] = seedRow{
						fmt.Sprintf("e%04d", i),
						map[string]*datastorepb.Value{"status": dsStr(status)},
					}
				}
				seedKind(t, s, kind, rows)

				resp := runAggKind(t, s, kind,
					propFilter("status", datastorepb.PropertyFilter_EQUAL, dsStr("active")),
					[]*datastorepb.AggregationQuery_Aggregation{countAgg("n")},
				)
				if n := resp.Batch.AggregationResults[0].AggregateProperties["n"].GetIntegerValue(); n != 50 {
					t.Errorf("want 50 active, got %d", n)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, s, "K_"+tc.name)
		})
	}
	t.Run("query_admission_reuse", testAggregationUnionReusesTopLevelQueryAdmission)
	t.Run("query_admission_cancellation", testQueryAdmissionHonorsCancellation)
}
