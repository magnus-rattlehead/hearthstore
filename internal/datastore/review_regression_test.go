package datastore

import (
	"context"
	"encoding/hex"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestCompatibilityNormalizedProjectionAndDistinct(t *testing.T) {
	s := newTestDsServer(t)
	for i, x := range []int64{9, 8, 6} {
		upsertEntity(t, s, dsEntity(dsKey("Normalized", strconv.Itoa(i+1)), map[string]*datastorepb.Value{"x": dsInt(x), "z": dsArray(dsInt(10), dsInt(20))}))
	}
	for _, direction := range []datastorepb.PropertyOrder_Direction{datastorepb.PropertyOrder_ASCENDING, datastorepb.PropertyOrder_DESCENDING} {
		q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Normalized"}}, Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "z"}}}, Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "__key__"}, Direction: direction}}, Limit: wrapperspb.Int32(1)}
		var got []int64
		var cursors [][]byte
		for page := 0; page < 7; page++ {
			r, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Batch.EntityResults) == 0 {
				break
			}
			got = append(got, r.Batch.EntityResults[0].Entity.Properties["z"].GetIntegerValue())
			cursors = append(cursors, r.Batch.EndCursor)
			q.StartCursor = r.Batch.EndCursor
		}
		want := []int64{10, 20, 10, 20, 10, 20}
		if direction == datastorepb.PropertyOrder_DESCENDING {
			want = []int64{20, 10, 20, 10, 20, 10}
		}
		if !slices.Equal(got, want) {
			t.Errorf("direction=%v got=%v want=%v", direction, got, want)
		}
		if len(cursors) < 3 {
			t.Fatal("missing projection cursors")
		}
		q.Order[0].Direction = datastorepb.PropertyOrder_DESCENDING
		if direction == datastorepb.PropertyOrder_DESCENDING {
			q.Order[0].Direction = datastorepb.PropertyOrder_ASCENDING
		}
		q.StartCursor, q.Limit = cursors[2], nil
		r, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
		if err != nil {
			t.Fatal(err)
		}
		got = nil
		for _, row := range r.Batch.EntityResults {
			got = append(got, row.Entity.Properties["z"].GetIntegerValue())
		}
		// An after-row cursor becomes before-row when reversed, including
		// that row, just like the existing full/keys reversal contract.
		if !slices.Equal(got, []int64{want[2], want[1], want[0]}) {
			t.Errorf("reverse direction=%v got=%v", direction, got)
		}
	}
	q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Normalized"}}, Filter: propFilter("x", datastorepb.PropertyFilter_NOT_IN, dsArray(dsInt(1), dsInt(2))), Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "z"}}}, DistinctOn: []*datastorepb.PropertyReference{{Name: "z"}}, Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "z"}, Direction: datastorepb.PropertyOrder_ASCENDING}}}
	r, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Batch.EntityResults) != 2 {
		t.Fatalf("distinct rows=%v", r.Batch.EntityResults)
	}
	for i, row := range r.Batch.EntityResults {
		if row.Entity.Key.Path[0].GetName() != "3" || row.Entity.Properties["z"].GetIntegerValue() != int64(10*(i+1)) {
			t.Errorf("distinct row=%v", row)
		}
	}
	t.Run("composite_descending_key_ties", func(t *testing.T) {
		for _, name := range []string{"1", "2", "3"} {
			upsertEntity(t, s, dsEntity(dsKey("NormalizedTies", name), map[string]*datastorepb.Value{"x": dsInt(1), "group": dsInt(1)}))
		}
		q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "NormalizedTies"}}, Filter: propFilter("group", datastorepb.PropertyFilter_EQUAL, dsInt(1)), Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}, Direction: datastorepb.PropertyOrder_DESCENDING}}, Limit: wrapperspb.Int32(1)}
		var names []string
		for page := 0; page < 4; page++ {
			r, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Batch.EntityResults) == 0 {
				break
			}
			names = append(names, r.Batch.EntityResults[0].Entity.Key.Path[0].GetName())
			q.StartCursor = r.Batch.EndCursor
		}
		if !slices.Equal(names, []string{"3", "2", "1"}) {
			t.Fatalf("descending ties=%v", names)
		}
		q.Filter, q.StartCursor = nil, nil
		r, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
		if err != nil {
			t.Fatal(err)
		}
		properties := r.ExplainMetrics.PlanSummary.IndexesUsed[0].Fields["properties"].GetStringValue()
		if r.Batch.EntityResults[0].Entity.Key.Path[0].GetName() != "3" || !strings.Contains(properties, "__key__ DESC") {
			t.Fatalf("builtin descending tie: properties=%s rows=%v", properties, r.Batch.EntityResults)
		}
	})
}

func TestCompatibilityDottedCursorStructure(t *testing.T) {
	s := newTestDsServer(t)
	for i := 1; i <= 2; i++ {
		upsertEntity(t, s, dsEntity(dsKey("DottedStructure", strconv.Itoa(i)), map[string]*datastorepb.Value{"a.b": dsInt(int64(i)), "a": dsEntityVal(map[string]*datastorepb.Value{"b": dsInt(int64(i + 10))})}))
	}
	for _, direction := range []datastorepb.PropertyOrder_Direction{datastorepb.PropertyOrder_ASCENDING, datastorepb.PropertyOrder_DESCENDING} {
		q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "DottedStructure"}}, Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "a.b"}, Direction: direction}}, Limit: wrapperspb.Int32(1)}
		r, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
		if err != nil {
			t.Fatal(err)
		}
		for _, malformed := range []string{"ff", "01", "0202", "0400", "0700", "0800", "0000"} {
			c, ok := decodeCursorFull(r.Batch.EndCursor)
			if !ok {
				t.Fatal("invalid emitted cursor")
			}
			parts := strings.Split(string(c.B), "/")
			raw, err := hex.DecodeString(malformed)
			if err != nil {
				t.Fatal(err)
			}
			if direction == datastorepb.PropertyOrder_DESCENDING {
				for i := range raw {
					raw[i] = ^raw[i]
				}
			}
			parts[0] = hex.EncodeToString(raw)
			c.B = []byte(strings.Join(parts, "/"))
			if c.I == "fallback" {
				c.K = []byte(string(c.B) + "|" + c.P + "|")
			}
			for _, end := range []bool{false, true} {
				q.StartCursor, q.EndCursor = encodeCursorFull(c), nil
				if end {
					q.StartCursor, q.EndCursor = nil, encodeCursorFull(c)
				}
				_, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
				if status.Code(err) != codes.InvalidArgument {
					t.Errorf("direction=%v malformed=%s end=%t: %v", direction, malformed, end, err)
				}
			}
		}
	}
}

func TestReviewCursorStructuralBoundaries(t *testing.T) {
	s := newTestDsServer(t)
	for i := 1; i <= 3; i++ {
		upsertEntity(t, s, dsEntity(dsKey("CursorStructure", strconv.Itoa(i)), map[string]*datastorepb.Value{"x": dsInt(int64(i)), "y": dsInt(int64(i))}))
	}
	for _, fields := range [][]string{{"__key__"}, {"x"}, {"x", "y"}} {
		q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "CursorStructure"}}, Limit: wrapperspb.Int32(1)}
		for _, field := range fields {
			q.Order = append(q.Order, &datastorepb.PropertyOrder{Property: &datastorepb.PropertyReference{Name: field}, Direction: datastorepb.PropertyOrder_ASCENDING})
		}
		request := &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}}
		resp, err := s.grpc.RunQuery(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		for name, corrupt := range map[string]func(*storage.CursorPayload){
			"path":          func(c *storage.CursorPayload) { c.P = "malformed" },
			"generation":    func(c *storage.CursorPayload) { c.G = -1 },
			"offset":        func(c *storage.CursorPayload) { c.O = 1 },
			"physical_tail": func(c *storage.CursorPayload) { c.K = append(slices.Clone(c.K), 'x') },
			"logical_key":   func(c *storage.CursorPayload) { c.B = append(slices.Clone(c.B), '0') },
			"logical_value": func(c *storage.CursorPayload) {
				parts := strings.Split(string(c.B), "/")
				parts[0] = "00"
				c.B = []byte(strings.Join(parts, "/"))
			},
		} {
			for _, end := range []bool{false, true} {
				c, ok := decodeCursorFull(resp.Batch.EndCursor)
				if !ok {
					t.Fatal("generated cursor invalid")
				}
				corrupt(&c)
				q.StartCursor, q.EndCursor = encodeCursorFull(c), nil
				if end {
					q.StartCursor, q.EndCursor = nil, encodeCursorFull(c)
				}
				if _, err := s.grpc.RunQuery(context.Background(), request); status.Code(err) != codes.InvalidArgument {
					t.Errorf("fields=%v %s end=%t error=%v", fields, name, end, err)
				}
			}
		}
	}
}

func TestReviewCursorSurvivesSourceChanges(t *testing.T) {
	s := newTestDsServer(t)
	for i := 1; i <= 3; i++ {
		upsertEntity(t, s, dsEntity(dsKey("CursorMutation", strconv.Itoa(i)), map[string]*datastorepb.Value{"x": dsInt(int64(i))}))
	}
	q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "CursorMutation"}}, Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}, Direction: datastorepb.PropertyOrder_ASCENDING}, {Property: &datastorepb.PropertyReference{Name: "__key__"}, Direction: datastorepb.PropertyOrder_ASCENDING}}, Limit: wrapperspb.Int32(1)}
	request := &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}}
	first, err := s.grpc.RunQuery(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	q.StartCursor, q.Limit = first.Batch.EndCursor, nil
	for _, remove := range []bool{false, true} {
		if remove {
			_, err = s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL, Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{Delete: dsKey("CursorMutation", "1")}}}})
			if err != nil {
				t.Fatal(err)
			}
		} else {
			upsertEntity(t, s, dsEntity(dsKey("CursorMutation", "1"), map[string]*datastorepb.Value{"x": dsInt(0)}))
		}
		response, err := s.grpc.RunQuery(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, row := range response.Batch.EntityResults {
			got = append(got, row.Entity.Key.Path[0].GetName())
		}
		if !slices.Equal(got, []string{"2", "3"}) {
			t.Fatalf("remove=%t rows=%v", remove, got)
		}
	}
}

func FuzzCursorEnvelope(f *testing.F) {
	f.Add([]byte("HS\x04\x00\x00"))
	f.Add(encodeCursorFull(storage.CursorPayload{V: 4, P: "path", I: "fallback", K: []byte("tuple"), H: []byte("hash"), B: []byte("bound")}))
	f.Fuzz(func(t *testing.T, data []byte) {
		cursor, ok := decodeCursorFull(data)
		if !ok {
			return
		}
		roundtrip, ok := decodeCursorFull(encodeCursorFull(cursor))
		if !ok || cursor.P != roundtrip.P || cursor.I != roundtrip.I || cursor.G != roundtrip.G || cursor.O != roundtrip.O || !slices.Equal(cursor.B, roundtrip.B) || !slices.Equal(cursor.K, roundtrip.K) || !slices.Equal(cursor.H, roundtrip.H) || cursor.D != roundtrip.D {
			t.Fatal("cursor round trip changed fields")
		}
		if _, ok := decodeCursorFull(append(slices.Clone(data), 0)); ok {
			t.Fatal("accepted trailing byte")
		}
	})
}

func TestReviewEscapedPropertyMasks(t *testing.T) {
	base := dsEntity(dsKey("Masks", "one"), map[string]*datastorepb.Value{"a.b": dsInt(1), "slash\\name": dsInt(5), "a": dsEntityVal(map[string]*datastorepb.Value{"b": dsInt(2), "c": dsInt(3)})})
	base.Properties["a"].ExcludeFromIndexes = true
	t.Run("new_parent_inherits_exclusion", func(t *testing.T) {
		for _, old := range []*datastorepb.Entity{nil, dsEntity(base.Key, nil), dsEntity(base.Key, map[string]*datastorepb.Value{"a": dsArray(dsInt(1))})} {
			got, err := applyPropertyMask(base, &datastorepb.PropertyMask{Paths: []string{"a.b"}}, func() (*datastorepb.Entity, error) { return old, nil })
			if err != nil || !got.GetProperties()["a"].GetExcludeFromIndexes() || len(got.GetProperties()["a"].GetEntityValue().GetProperties()) != 1 {
				t.Fatalf("parent metadata/selection lost: %v %v", got, err)
			}
		}
	})
	incoming := dsEntity(base.Key, map[string]*datastorepb.Value{"a.b": dsInt(10), "slash\\name": dsInt(50), "a": dsEntityVal(map[string]*datastorepb.Value{"b": dsInt(20)})})
	for _, tc := range []struct {
		path    string
		target  string
		invalid bool
	}{
		{"a.b", "nested", false}, {"`a.b`", "a.b", false}, {"`a\\x2eb`", "a.b", false}, {"`slash\\\\name`", "slash\\name", false}, {"__key__", "", false},
		{"", "", true}, {"a.", "", true}, {"a..b", "", true}, {"a\\.b", "", true}, {"a[0]", "", true}, {"`unterminated", "", true}, {"`bad\\q`", "", true},
	} {
		t.Run(tc.path, func(t *testing.T) {
			got, err := applyPropertyMask(incoming, &datastorepb.PropertyMask{Paths: []string{tc.path}}, func() (*datastorepb.Entity, error) { return base, nil })
			if tc.invalid {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("error=%v want InvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := proto.Clone(base).(*datastorepb.Entity)
			if tc.target == "nested" {
				want.Properties["a"].GetEntityValue().Properties["b"] = dsInt(20)
			} else if tc.target != "" {
				want.Properties[tc.target] = incoming.Properties[tc.target]
			}
			if !proto.Equal(got, want) {
				t.Fatalf("got=%v want=%v", got, want)
			}
			if base.Properties["a.b"].GetIntegerValue() != 1 || base.Properties["a"].GetEntityValue().Properties["b"].GetIntegerValue() != 2 {
				t.Fatal("mask mutated source")
			}
		})
	}
}

func FuzzQuotedMaskPath(f *testing.F) {
	for _, name := range []string{"a", "a.b", "slash\\name", "tick`name", "雪", "line\r\nend"} {
		f.Add(name)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if name == "" || len(name) > maxNameBytes || !utf8.ValidString(name) || reservedName(name) {
			return
		}
		quoted := strconv.Quote(name)
		path := "`" + strings.ReplaceAll(quoted[1:len(quoted)-1], "`", "\\`") + "`"
		parts, err := parseMaskPath(path)
		if err != nil || len(parts) != 1 || parts[0] != name {
			t.Fatalf("quoted %q decoded as %q: %v", name, parts, err)
		}
	})
}

func TestReviewInvalidMaskRejectsBatch(t *testing.T) {
	s := newTestDsServer(t)
	key := dsKey("AtomicMask", "row")
	before := dsEntity(key, map[string]*datastorepb.Value{"x": dsInt(1)})
	upsertEntity(t, s, before)
	marker := dsEntity(dsKey("AtomicMask", "marker"), nil)
	_, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL, Mutations: []*datastorepb.Mutation{
		{Operation: &datastorepb.Mutation_Upsert{Upsert: marker}},
		{Operation: &datastorepb.Mutation_Update{Update: dsEntity(key, map[string]*datastorepb.Value{"x": dsInt(2)})}, PropertyMask: &datastorepb.PropertyMask{Paths: []string{"x", "a..b"}}},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid mask error=%v", err)
	}
	lookup, err := s.grpc.Lookup(context.Background(), &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key, marker.Key}})
	if err != nil || len(lookup.GetFound()) != 1 || len(lookup.GetMissing()) != 1 || !proto.Equal(lookup.Found[0].Entity, before) {
		t.Fatalf("invalid mask partially applied: %v %v", lookup, err)
	}
}

func TestReviewReversedCursorBoundaries(t *testing.T) {
	s := newTestDsServer(t)
	for i := 1; i <= 4; i++ {
		upsertEntity(t, s, dsEntity(dsKey("ReverseBoundary", strconv.Itoa(i)), map[string]*datastorepb.Value{"n": dsInt(int64(i))}))
	}
	run := func(q *datastorepb.Query) *datastorepb.QueryResultBatch {
		t.Helper()
		resp, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
		if err != nil {
			t.Fatal(err)
		}
		return resp.Batch
	}
	for _, fields := range [][]string{{"__key__"}, {"n", "__key__"}} {
		for _, keysOnly := range []bool{false, true} {
			for _, initialDirection := range []datastorepb.PropertyOrder_Direction{datastorepb.PropertyOrder_DIRECTION_UNSPECIFIED, datastorepb.PropertyOrder_ASCENDING, datastorepb.PropertyOrder_DESCENDING} {
				descending := initialDirection == datastorepb.PropertyOrder_DESCENDING
				q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "ReverseBoundary"}}, Limit: wrapperspb.Int32(2)}
				for _, field := range fields {
					q.Order = append(q.Order, &datastorepb.PropertyOrder{Property: &datastorepb.PropertyReference{Name: field}, Direction: initialDirection})
				}
				if keysOnly {
					q.Projection = []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "__key__"}}}
				}
				first := run(q)
				for _, order := range q.Order {
					if descending {
						order.Direction = datastorepb.PropertyOrder_ASCENDING
					} else {
						order.Direction = datastorepb.PropertyOrder_DESCENDING
					}
				}
				q.Limit = nil
				for _, end := range []bool{false, true} {
					q.StartCursor, q.EndCursor = first.EndCursor, nil
					want := []string{"2", "1"}
					if descending {
						want = []string{"3", "4"}
					}
					if end {
						q.StartCursor, q.EndCursor = nil, first.EndCursor
						want = []string{"4", "3"}
						if descending {
							want = []string{"1", "2"}
						}
					}
					var got []string
					for _, row := range run(q).EntityResults {
						got = append(got, row.Entity.Key.Path[0].GetName())
					}
					if !slices.Equal(got, want) {
						t.Fatalf("fields=%v keys=%t desc=%t end=%t got=%v want=%v", fields, keysOnly, descending, end, got, want)
					}
					if !keysOnly {
						aggs := []*datastorepb.AggregationQuery_Aggregation{countAgg("count")}
						if len(fields) > 1 {
							aggs = append(aggs, sumAgg("sum", "n"))
						}
						response, err := s.grpc.RunAggregationQuery(context.Background(), &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: q}, Aggregations: aggs}}})
						if err != nil {
							t.Fatal(err)
						}
						values := response.Batch.AggregationResults[0].AggregateProperties
						if values["count"].GetIntegerValue() != 2 {
							t.Fatalf("reverse aggregate=%v want count 2", values)
						}
						if len(fields) > 1 {
							wantSum := int64(3)
							if want[0] == "3" || want[0] == "4" {
								wantSum = 7
							}
							if values["sum"].GetIntegerValue() != wantSum {
								t.Fatalf("reverse aggregate=%v want sum %d", values, wantSum)
							}
						}
					}
				}
			}
		}
	}
}

func TestReviewReversedCursorPairs(t *testing.T) {
	s := newTestDsServer(t)
	for i := 1; i <= 4; i++ {
		upsertEntity(t, s, dsEntity(dsKey("CursorPairs", strconv.Itoa(i)), nil))
	}
	q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "CursorPairs"}}, Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "__key__"}, Direction: datastorepb.PropertyOrder_ASCENDING}}, Limit: wrapperspb.Int32(3)}
	run := func() *datastorepb.QueryResultBatch {
		t.Helper()
		r, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
		if err != nil {
			t.Fatal(err)
		}
		return r.Batch
	}
	ascending := run().EndCursor
	q.Order[0].Direction = datastorepb.PropertyOrder_DESCENDING
	descending := run().EndCursor
	q.StartCursor, q.EndCursor, q.Limit = ascending, descending, nil
	var got []string
	for _, row := range run().EntityResults {
		got = append(got, row.Entity.Key.Path[0].GetName())
	}
	if !slices.Equal(got, []string{"3", "2"}) {
		t.Fatalf("mixed direction bounds=%v want [3 2]", got)
	}
}

func TestReviewProjectionHiddenOrderPagination(t *testing.T) {
	s := newTestDsServer(t)
	for i := 1; i <= 3; i++ {
		upsertEntity(t, s, dsEntity(dsKey("HiddenProjectionOrder", strconv.Itoa(i)), map[string]*datastorepb.Value{"n": dsInt(int64(i)), "a": dsArray(dsInt(1), dsInt(2))}))
	}
	for _, field := range []string{"n", "__key__"} {
		for _, descending := range []bool{false, true} {
			q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "HiddenProjectionOrder"}}, Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "a"}}}, Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: field}, Direction: datastorepb.PropertyOrder_ASCENDING}}, Limit: wrapperspb.Int32(1)}
			if descending {
				q.Order[0].Direction = datastorepb.PropertyOrder_DESCENDING
			}
			var keys []string
			seen := map[string]bool{}
			for page := 0; page < 7; page++ {
				response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
				if err != nil {
					t.Fatalf("field=%s desc=%t page=%d: %v", field, descending, page, err)
				}
				for _, row := range response.Batch.EntityResults {
					name := row.Entity.Key.Path[0].GetName()
					tuple := name + ":" + strconv.FormatInt(row.Entity.Properties["a"].GetIntegerValue(), 10)
					if seen[tuple] {
						t.Fatalf("repeated projection %s", tuple)
					}
					seen[tuple] = true
					keys = append(keys, name)
				}
				if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
					break
				}
				q.StartCursor = response.Batch.EndCursor
			}
			want := []string{"1", "1", "2", "2", "3", "3"}
			if descending {
				slices.Reverse(want)
			}
			if !slices.Equal(keys, want) || len(seen) != 6 {
				t.Fatalf("field=%s desc=%t keys=%v want=%v", field, descending, keys, want)
			}
		}
	}
}

func TestReviewRESTTransactionUsesRouteProject(t *testing.T) {
	s := newTestDsServer(t)
	var begin datastorepb.BeginTransactionResponse
	mustPost(t, s, "beginTransaction", &datastorepb.BeginTransactionRequest{}, &begin)
	_, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: begin.Transaction}})
	if err != nil {
		t.Fatalf("route-scoped transaction commit: %v", err)
	}
}

func TestReviewRequestValidation(t *testing.T) {
	s := newTestDsServer(t)
	ctx := context.Background()
	foreign := dsKey("ValidationReview", "foreign")
	foreign.PartitionId.ProjectId = "other-project"
	for _, mutation := range []*datastorepb.Mutation{nil, {}, {Operation: &datastorepb.Mutation_Upsert{}}, {Operation: &datastorepb.Mutation_Delete{}}, {Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(foreign, nil)}}} {
		t.Run("invalid mutation", func(t *testing.T) {
			_, err := s.grpc.Commit(ctx, &datastorepb.CommitRequest{ProjectId: testProject, Mutations: []*datastorepb.Mutation{mutation}})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("mutation=%v error=%v", mutation, err)
			}
		})
	}
	for _, query := range []*datastorepb.Query{{Offset: -1}, {Limit: wrapperspb.Int32(-1)}, {Order: []*datastorepb.PropertyOrder{nil}}, {Projection: []*datastorepb.Projection{nil}}, {Filter: &datastorepb.Filter{}}, {Kind: []*datastorepb.KindExpression{{Name: "A"}, {Name: "B"}}}} {
		_, err := s.grpc.RunQuery(ctx, &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: query}})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("query=%v error=%v", query, err)
		}
	}
	_, err := s.grpc.Lookup(ctx, &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{foreign}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("foreign lookup error=%v", err)
	}
	if _, err := s.grpc.AllocateIds(ctx, &datastorepb.AllocateIdsRequest{ProjectId: testProject, Keys: []*datastorepb.Key{foreign}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("foreign allocation error=%v", err)
	}
	if _, err := s.grpc.ReserveIds(ctx, &datastorepb.ReserveIdsRequest{ProjectId: testProject, Keys: []*datastorepb.Key{nil}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid reservation error=%v", err)
	}
	for _, aggregations := range [][]*datastorepb.AggregationQuery_Aggregation{nil, {nil}, {{}}, {{Operator: &datastorepb.AggregationQuery_Aggregation_Sum_{Sum: &datastorepb.AggregationQuery_Aggregation_Sum{}}}}} {
		_, err := s.grpc.RunAggregationQuery(ctx, &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: &datastorepb.Query{}}, Aggregations: aggregations}}})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid aggregations=%v error=%v", aggregations, err)
		}
	}
}

func TestReviewQueryComplexityLimits(t *testing.T) {
	filters := func(n int, op datastorepb.PropertyFilter_Operator, sameField bool) []*datastorepb.Filter {
		var result []*datastorepb.Filter
		for i := range n {
			name := "x" + strconv.Itoa(i)
			if sameField {
				name = "x"
			}
			result = append(result, propFilter(name, op, dsInt(int64(i))))
		}
		return result
	}
	list := func(n int) *datastorepb.Value {
		var values []*datastorepb.Value
		for i := range n {
			values = append(values, dsInt(int64(i)))
		}
		return dsArray(values...)
	}
	product := func(n int) *datastorepb.Filter {
		var groups []*datastorepb.Filter
		for i := range n {
			name := "x" + strconv.Itoa(i)
			groups = append(groups, orFilter(propFilter(name, datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter(name, datastorepb.PropertyFilter_EQUAL, dsInt(9))))
		}
		return andFilter(groups...)
	}
	var repeated []*datastorepb.Filter
	for range 24 {
		repeated = append(repeated, product(1))
	}
	var duplicateValues []*datastorepb.Value
	for range 31 {
		duplicateValues = append(duplicateValues, dsInt(1))
	}
	order := []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "z"}}}
	cases := []struct {
		name   string
		filter *datastorepb.Filter
		order  []*datastorepb.PropertyOrder
		want   codes.Code
	}{
		{"or_30", orFilter(filters(30, datastorepb.PropertyFilter_EQUAL, true)...), nil, codes.OK},
		{"or_31", orFilter(filters(31, datastorepb.PropertyFilter_EQUAL, true)...), nil, codes.InvalidArgument},
		{"in_30", propFilter("x", datastorepb.PropertyFilter_IN, list(30)), nil, codes.OK},
		{"in_31", propFilter("x", datastorepb.PropertyFilter_IN, list(31)), nil, codes.InvalidArgument},
		{"in_duplicate_31", propFilter("x", datastorepb.PropertyFilter_IN, dsArray(duplicateValues...)), nil, codes.InvalidArgument},
		{"not_in_10", propFilter("x", datastorepb.PropertyFilter_NOT_IN, list(10)), nil, codes.OK},
		{"not_in_11", propFilter("x", datastorepb.PropertyFilter_NOT_IN, list(11)), nil, codes.InvalidArgument},
		{"not_in_with_in", andFilter(propFilter("x", datastorepb.PropertyFilter_NOT_IN, list(2)), propFilter("y", datastorepb.PropertyFilter_IN, list(2))), nil, codes.InvalidArgument},
		{"not_in_with_or", orFilter(propFilter("x", datastorepb.PropertyFilter_NOT_IN, list(2)), propFilter("y", datastorepb.PropertyFilter_EQUAL, dsInt(1))), nil, codes.InvalidArgument},
		{"not_in_with_not_equal", andFilter(propFilter("x", datastorepb.PropertyFilter_NOT_IN, list(2)), propFilter("y", datastorepb.PropertyFilter_NOT_EQUAL, dsInt(1))), nil, codes.InvalidArgument},
		{"two_not_equal", andFilter(propFilter("x", datastorepb.PropertyFilter_NOT_EQUAL, dsInt(1)), propFilter("y", datastorepb.PropertyFilter_NOT_EQUAL, dsInt(1))), nil, codes.InvalidArgument},
		{"and_or_16", product(4), nil, codes.OK},
		{"and_or_32", product(5), nil, codes.InvalidArgument},
		{"and_or_256", product(8), nil, codes.InvalidArgument},
		{"and_or_overflow", product(70), nil, codes.InvalidArgument},
		{"duplicate_groups", andFilter(repeated...), nil, codes.OK},
		{"in_product_30", andFilter(propFilter("x", datastorepb.PropertyFilter_IN, list(5)), propFilter("y", datastorepb.PropertyFilter_IN, list(6))), nil, codes.OK},
		{"in_product_36", andFilter(propFilter("x", datastorepb.PropertyFilter_IN, list(6)), propFilter("y", datastorepb.PropertyFilter_IN, list(6))), nil, codes.InvalidArgument},
		{"inequality_fields_10", andFilter(filters(10, datastorepb.PropertyFilter_GREATER_THAN, false)...), nil, codes.OK},
		{"inequality_fields_11", andFilter(filters(11, datastorepb.PropertyFilter_GREATER_THAN, false)...), nil, codes.InvalidArgument},
		{"inequality_same_field_11", andFilter(filters(11, datastorepb.PropertyFilter_GREATER_THAN, true)...), nil, codes.OK},
		{"components_100", andFilter(filters(100, datastorepb.PropertyFilter_EQUAL, false)...), nil, codes.OK},
		{"components_101", andFilter(filters(101, datastorepb.PropertyFilter_EQUAL, false)...), nil, codes.InvalidArgument},
		{"components_99_order", andFilter(filters(99, datastorepb.PropertyFilter_EQUAL, false)...), order, codes.OK},
		{"components_100_order", andFilter(filters(100, datastorepb.PropertyFilter_EQUAL, false)...), order, codes.InvalidArgument},
		{"components_before_dnf", andFilter(propFilter("x", datastorepb.PropertyFilter_IN, list(30)), propFilter("y", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("z", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("w", datastorepb.PropertyFilter_EQUAL, dsInt(1))), nil, codes.OK},
		{"not_in_components_100", andFilter(append(filters(90, datastorepb.PropertyFilter_EQUAL, false), propFilter("y", datastorepb.PropertyFilter_NOT_IN, list(10)))...), nil, codes.OK},
		{"not_in_components_101", andFilter(append(filters(91, datastorepb.PropertyFilter_EQUAL, false), propFilter("y", datastorepb.PropertyFilter_NOT_IN, list(10)))...), nil, codes.InvalidArgument},
		{"ancestor_components_100", andFilter(append(filters(99, datastorepb.PropertyFilter_EQUAL, false), propFilter("__key__", datastorepb.PropertyFilter_HAS_ANCESTOR, &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: dsKey("QueryLimitProbe", "parent")}}))...), nil, codes.OK},
		{"ancestor_components_101", andFilter(append(filters(100, datastorepb.PropertyFilter_EQUAL, false), propFilter("__key__", datastorepb.PropertyFilter_HAS_ANCESTOR, &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: dsKey("QueryLimitProbe", "parent")}}))...), nil, codes.InvalidArgument},
	}
	s := newTestDsServer(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "QueryLimitProbe"}}, Filter: tc.filter, Order: tc.order}
			original := proto.Clone(q)
			_, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
			if status.Code(err) != tc.want {
				t.Errorf("RunQuery: got %v, want %v", err, tc.want)
			}
			_, err = s.grpc.RunAggregationQuery(context.Background(), &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: q}, Aggregations: []*datastorepb.AggregationQuery_Aggregation{{Alias: "count", Operator: &datastorepb.AggregationQuery_Aggregation_Count_{Count: &datastorepb.AggregationQuery_Aggregation_Count{}}}}}}})
			if status.Code(err) != tc.want {
				t.Errorf("RunAggregationQuery: got %v, want %v", err, tc.want)
			}
			if !proto.Equal(q, original) {
				t.Fatal("query was mutated")
			}
		})
	}
}

func TestReviewDuplicateORGroupsComplete(t *testing.T) {
	s := newTestDsServer(t)
	entity := dsEntity(dsKey("QueryLimitProbe", "one"), map[string]*datastorepb.Value{"x": dsArray(dsInt(1), dsInt(9))})
	if _, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: entity}}}}); err != nil {
		t.Fatal(err)
	}
	for _, shape := range []string{"kindless", "kind", "nested", "permuted_membership", "permuted_kind", "permuted_historical", "permuted_aggregate"} {
		t.Run(shape, func(t *testing.T) {
			var groups []*datastorepb.Filter
			for i := range 24 {
				group := orFilter(propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(0)), propFilter("x", datastorepb.PropertyFilter_LESS_THAN, dsInt(10)))
				if strings.HasPrefix(shape, "permuted_") {
					var values []*datastorepb.Value
					for j := range 24 {
						values = append(values, dsInt(int64((i+j)%24)))
					}
					group = orFilter(propFilter("x", datastorepb.PropertyFilter_IN, dsArray(values...)), propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)))
				}
				if shape == "nested" {
					for range i {
						group = orFilter(group)
					}
				}
				groups = append(groups, group)
			}
			// A deadline bounds the old fallback loop; it is not a latency SLA.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			q := &datastorepb.Query{Filter: andFilter(groups...), Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}}}}
			if shape != "kindless" && shape != "permuted_membership" {
				q.Kind = []*datastorepb.KindExpression{{Name: "QueryLimitProbe"}}
			}
			if _, err := prepareQuery(q, ""); err != nil {
				t.Fatalf("accepted-query regression was rejected before execution: %v", err)
			}
			var readOptions *datastorepb.ReadOptions
			if shape == "permuted_historical" {
				readOptions = &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: timestamppb.New(s.grpc.store.ReadTime())}}
			}
			response, err := s.grpc.RunQuery(ctx, &datastorepb.RunQueryRequest{ProjectId: testProject, ReadOptions: readOptions, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
			if err != nil {
				t.Fatal(err)
			}
			rows := response.GetBatch().GetEntityResults()
			if len(rows) != 1 || !proto.Equal(rows[0].Entity.Key, entity.Key) {
				t.Fatalf("expected the matching entity, got %v", rows)
			}
			if shape == "permuted_aggregate" {
				response, err := s.grpc.RunAggregationQuery(ctx, &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: q}, Aggregations: []*datastorepb.AggregationQuery_Aggregation{countAgg("count")}}}})
				if err != nil {
					t.Fatal(err)
				}
				// COUNT consumes the two ordered array entries, not one entity.
				if got := response.Batch.AggregationResults[0].AggregateProperties["count"].GetIntegerValue(); got != 2 {
					t.Fatalf("count=%d want=2", got)
				}
			}
		})
	}
}

func TestReviewOptimizerDeclineKeepsExactResults(t *testing.T) {
	s := newTestDsServer(t)
	const kind = "OptimizerDecline"
	seedKind(t, s, kind, []seedRow{
		{"a", map[string]*datastorepb.Value{"x": dsInt(1), "y": dsInt(2)}},
		{"b", map[string]*datastorepb.Value{"x": dsInt(2), "y": dsInt(1)}},
	})
	q, condition, err := prepareQueryCondition(&datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}, Filter: andFilter(propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(0)), propFilter("y", datastorepb.PropertyFilter_GREATER_THAN, dsInt(0))), Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}}, {Property: &datastorepb.PropertyReference{Name: "y"}}}, Limit: wrapperspb.Int32(1)}, "")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.grpc.store.EnsureDsCompositeIndex(context.Background(), storage.DsCompositeIndex{Project: testProject, Kind: "UnrelatedOptimizerKind", Properties: []storage.DsIndexProperty{{Name: "x"}, {Name: "y"}}}, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, allowance := range []int{0, 1} {
		q.StartCursor = nil
		ctx := context.WithValue(context.Background(), queryConditionContextKey{}, preparedQueryCondition{condition: condition, optimizerAllowance: allowance})
		var got []string
		for range 3 {
			response, err := s.grpc.RunQuery(ctx, &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range response.Batch.EntityResults {
				got = append(got, row.Entity.Key.Path[0].GetName())
			}
			if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
				break
			}
			q.StartCursor = response.Batch.EndCursor
		}
		if !slices.Equal(got, []string{"a", "b"}) {
			t.Fatalf("declined optimization results=%v", got)
		}
		indexes, err := s.grpc.store.ListDsCompositeIndexes(testProject)
		if err != nil {
			t.Fatal(err)
		}
		if len(indexes) != 1 {
			t.Fatalf("allowance=%d: declined optimization left %d indexes, want only the unrelated one", allowance, len(indexes))
		}
	}
	t.Run("builtin_spans", func(t *testing.T) {
		q, condition, err := prepareQueryCondition(&datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}, Filter: propFilter("x", datastorepb.PropertyFilter_IN, dsArray(dsInt(1), dsInt(2))), Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}}}}, "")
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.WithValue(context.Background(), queryConditionContextKey{}, preparedQueryCondition{condition: condition, optimizerAllowance: 1})
		for _, analyze := range []bool{false, true} {
			response, err := s.grpc.RunQuery(ctx, &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}, ExplainOptions: &datastorepb.ExplainOptions{Analyze: analyze}})
			if err != nil {
				t.Fatal(err)
			}
			if access := response.ExplainMetrics.PlanSummary.IndexesUsed[0].Fields["access_path"].GetStringValue(); access != "disk_sort" {
				t.Fatalf("analyze=%t: declined spans access=%s", analyze, access)
			}
			if analyze {
				var got []string
				for _, row := range response.Batch.EntityResults {
					got = append(got, row.Entity.Key.Path[0].GetName())
				}
				if !slices.Equal(got, []string{"a", "b"}) {
					t.Fatalf("declined spans results=%v", got)
				}
			}
		}
	})
}

func TestReviewCompiledConditionsKeepExactFallback(t *testing.T) {
	filter := andFilter(
		orFilter(propFilter("x", datastorepb.PropertyFilter_IN, dsArray(dsInt(1), dsInt(9))), propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1))),
		orFilter(propFilter("x", datastorepb.PropertyFilter_IN, dsArray(dsInt(9), dsInt(1))), propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1))),
	)
	original := proto.Clone(filter)
	expanded, err := compileQueryCondition(filter, 30)
	if err != nil {
		t.Fatal(err)
	}
	factored, err := compileQueryCondition(filter, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(expanded.branches) != 2 || len(factored.branches) != 0 {
		t.Fatalf("expanded alternatives=%d, factored alternatives=%d", len(expanded.branches), len(factored.branches))
	}
	if len(expanded.nodes) != 3 || len(factored.nodes) != 3 {
		t.Fatalf("duplicate membership permutations were not interned: %d / %d nodes", len(expanded.nodes), len(factored.nodes))
	}
	if !proto.Equal(expanded.filter(), factored.filter()) || !proto.Equal(filter, original) {
		t.Fatal("optimizer allowance changed the condition or mutated input")
	}
}

func TestReviewDeepQueryFilterMatchesJava(t *testing.T) {
	s := newTestDsServer(t)
	entity := dsEntity(dsKey("DeepFilter", "one"), map[string]*datastorepb.Value{"x": dsInt(1)})
	upsertEntity(t, s, entity)
	filter := propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1))
	for range 400 {
		filter = andFilter(filter)
	}
	q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "DeepFilter"}}, Filter: filter}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := s.grpc.RunQuery(ctx, &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
	if err != nil {
		t.Fatal(err)
	}
	rows := response.GetBatch().GetEntityResults()
	if len(rows) != 1 || !proto.Equal(rows[0].Entity.Key, entity.Key) {
		t.Fatalf("expected the matching entity through 400 wrappers, got %v", rows)
	}
}

func TestReviewQueryKeyNamespace(t *testing.T) {
	s := newTestDsServer(t)
	ctx := context.Background()
	key := dsKey("NamespaceValidationReview", "one")
	key.PartitionId.NamespaceId = "foreign"
	foreign := &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: key}}
	local := &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: dsKey("NamespaceValidationReview", "one")}}
	list := &datastorepb.Value{ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: []*datastorepb.Value{local, foreign}}}}
	upsertEntity(t, s, dsEntity(dsKey("NamespaceValidationReview", "one"), map[string]*datastorepb.Value{"ref": foreign}))
	for _, tc := range []struct {
		name, namespace string
		filter          *datastorepb.Filter
		want            codes.Code
	}{
		{"default to named", "", propFilter("__key__", datastorepb.PropertyFilter_EQUAL, foreign), codes.InvalidArgument},
		{"named to default", "foreign", propFilter("__key__", datastorepb.PropertyFilter_EQUAL, local), codes.InvalidArgument},
		{"named to other", "other", propFilter("__key__", datastorepb.PropertyFilter_GREATER_THAN, foreign), codes.InvalidArgument},
		{"mixed IN", "", propFilter("__key__", datastorepb.PropertyFilter_IN, list), codes.InvalidArgument},
		{"mixed NOT_IN", "", propFilter("__key__", datastorepb.PropertyFilter_NOT_IN, list), codes.InvalidArgument},
		{"ancestor", "", propFilter("__key__", datastorepb.PropertyFilter_HAS_ANCESTOR, foreign), codes.InvalidArgument},
		{"nested OR", "", &datastorepb.Filter{FilterType: &datastorepb.Filter_CompositeFilter{CompositeFilter: &datastorepb.CompositeFilter{Op: datastorepb.CompositeFilter_OR, Filters: []*datastorepb.Filter{propFilter("__key__", datastorepb.PropertyFilter_EQUAL, local), propFilter("__key__", datastorepb.PropertyFilter_EQUAL, foreign)}}}}, codes.InvalidArgument},
		{"same named namespace", "foreign", propFilter("__key__", datastorepb.PropertyFilter_EQUAL, foreign), codes.OK},
		{"same default namespace", "", propFilter("__key__", datastorepb.PropertyFilter_EQUAL, local), codes.OK},
		{"ordinary cross namespace reference", "", propFilter("ref", datastorepb.PropertyFilter_EQUAL, foreign), codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "NamespaceValidationReview"}}, Filter: tc.filter}
			original := proto.Clone(q)
			partition := &datastorepb.PartitionId{NamespaceId: tc.namespace}
			_, err := s.grpc.RunQuery(ctx, &datastorepb.RunQueryRequest{ProjectId: testProject, PartitionId: partition, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
			if status.Code(err) != tc.want {
				t.Errorf("RunQuery code=%s, want %s: %v", status.Code(err), tc.want, err)
			}
			_, err = s.grpc.RunAggregationQuery(ctx, &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, PartitionId: partition, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: q}, Aggregations: []*datastorepb.AggregationQuery_Aggregation{{Alias: "n", Operator: &datastorepb.AggregationQuery_Aggregation_Count_{Count: &datastorepb.AggregationQuery_Aggregation_Count{}}}}}}})
			if status.Code(err) != tc.want {
				t.Errorf("RunAggregationQuery code=%s, want %s: %v", status.Code(err), tc.want, err)
			}
			if !proto.Equal(original, q) {
				t.Fatal("query validation mutated the request")
			}
		})
	}
}

func TestReviewDottedPropertyCollisionQueries(t *testing.T) {
	s := newTestDsServer(t)
	key := dsKey("DottedCollisionReview", "one")
	upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{
		"a.b": dsInt(70),
		"a":   {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"b": dsInt(80)}}}},
	}))
	for _, value := range []int64{70, 80} {
		t.Run(strconv.FormatInt(value, 10), func(t *testing.T) {
			response := qKind(t, s, "DottedCollisionReview", &datastorepb.Query{Filter: propFilter("a.b", datastorepb.PropertyFilter_EQUAL, dsInt(value))})
			rows := response.GetBatch().GetEntityResults()
			if len(rows) != 1 || !proto.Equal(rows[0].Entity.Key, key) {
				t.Fatalf("a.b=%d: got %v, want entity %v", value, rows, key)
			}
		})
	}
	t.Run("intermediate literal dot does not create a canonical path", func(t *testing.T) {
		upsertEntity(t, s, dsEntity(dsKey("DottedPartialReview", "one"), map[string]*datastorepb.Value{
			"a": {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"b.c": dsInt(90)}}}},
		}))
		response := qKind(t, s, "DottedPartialReview", &datastorepb.Query{Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "a.b.c"}}}})
		if rows := response.GetBatch().GetEntityResults(); len(rows) != 0 {
			t.Fatalf("partial literal path produced %d covering projection rows, want none", len(rows))
		}
	})
	t.Run("correlated tuples and fallback pages", func(t *testing.T) {
		kind := "DottedTupleReview"
		upsertEntity(t, s, dsEntity(dsKey(kind, "one"), map[string]*datastorepb.Value{
			"a.b": dsInt(70), "a": {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"b": dsInt(80)}}}},
			"c.d": dsInt(700), "c": {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"d": dsInt(800)}}}},
		}))
		for _, pair := range [][2]int64{{70, 700}, {80, 800}, {70, 800}, {80, 700}} {
			response := qKind(t, s, kind, &datastorepb.Query{Filter: andFilter(propFilter("a.b", datastorepb.PropertyFilter_EQUAL, dsInt(pair[0])), propFilter("c.d", datastorepb.PropertyFilter_EQUAL, dsInt(pair[1])))})
			want := 0
			if pair[1] == 10*pair[0] {
				want = 1
			}
			if got := len(response.GetBatch().GetEntityResults()); got != want {
				t.Errorf("pair=%v rows=%d, want %d", pair, got, want)
			}
		}
		for _, fallback := range []bool{false, true} {
			q := &datastorepb.Query{Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "a.b"}}, {Property: &datastorepb.PropertyReference{Name: "c.d"}}}, Limit: wrapperspb.Int32(1)}
			if fallback {
				q.Filter = orFilter(propFilter("a.b", datastorepb.PropertyFilter_GREATER_THAN, dsInt(0)), propFilter("a.b", datastorepb.PropertyFilter_LESS_THAN, dsInt(0)))
				q.Order = []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "a.b"}, Direction: datastorepb.PropertyOrder_ASCENDING}, {Property: &datastorepb.PropertyReference{Name: "c.d"}, Direction: datastorepb.PropertyOrder_ASCENDING}}
			}
			var got [][2]int64
			for page := 0; page < 4; page++ {
				response := qKind(t, s, kind, q)
				for _, row := range response.GetBatch().GetEntityResults() {
					got = append(got, [2]int64{row.Entity.Properties["a.b"].GetIntegerValue(), row.Entity.Properties["c.d"].GetIntegerValue()})
				}
				if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
					break
				}
				q.StartCursor = response.Batch.EndCursor
			}
			if want := [][2]int64{{70, 700}, {80, 800}}; !slices.Equal(got, want) {
				t.Errorf("fallback=%t tuples=%v, want %v", fallback, got, want)
			}
		}
	})
	t.Run("arrays exclusions and entity pagination", func(t *testing.T) {
		kind := "DottedArrayReview"
		excluded := dsInt(999)
		excluded.ExcludeFromIndexes = true
		for _, name := range []string{"one", "two"} {
			upsertEntity(t, s, dsEntity(dsKey(kind, name), map[string]*datastorepb.Value{
				"a.b": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: []*datastorepb.Value{dsInt(70), dsInt(70), excluded}}}},
				"a":   {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"b": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: []*datastorepb.Value{dsInt(70), dsInt(80)}}}}}}}},
			}))
		}
		for _, direction := range []datastorepb.PropertyOrder_Direction{datastorepb.PropertyOrder_ASCENDING, datastorepb.PropertyOrder_DESCENDING} {
			q := &datastorepb.Query{Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "a.b"}, Direction: direction}}, Limit: wrapperspb.Int32(1)}
			var names []string
			for page := 0; page < 6; page++ {
				response := qKind(t, s, kind, q)
				for _, row := range response.Batch.EntityResults {
					names = append(names, row.Entity.Key.Path[0].GetName())
				}
				if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
					break
				}
				q.StartCursor = response.Batch.EndCursor
			}
			slices.Sort(names)
			if !slices.Equal(names, []string{"one", "two"}) {
				t.Errorf("direction=%s paged entities=%v", direction, names)
			}
		}
		for _, nestedExcluded := range []bool{false, true} {
			literal := dsInt(70)
			nested := &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"b": dsInt(80)}}}, ExcludeFromIndexes: nestedExcluded}
			literal.ExcludeFromIndexes = !nestedExcluded
			upsertEntity(t, s, dsEntity(dsKey("DottedExcludedReview", "one"), map[string]*datastorepb.Value{"a.b": literal, "a": nested}))
			for _, n := range []int64{70, 80} {
				response := qKind(t, s, "DottedExcludedReview", &datastorepb.Query{Filter: propFilter("a.b", datastorepb.PropertyFilter_EQUAL, dsInt(n))})
				want := 0
				if n == 70 && nestedExcluded || n == 80 && !nestedExcluded {
					want = 1
				}
				if got := len(response.Batch.EntityResults); got != want {
					t.Errorf("nestedExcluded=%t value=%d rows=%d want=%d", nestedExcluded, n, got, want)
				}
			}
		}
		q := &datastorepb.Query{
			Filter:     orFilter(propFilter("a.b", datastorepb.PropertyFilter_GREATER_THAN, dsInt(0)), propFilter("a.b", datastorepb.PropertyFilter_LESS_THAN, dsInt(0))),
			Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "a.b"}}}, Limit: wrapperspb.Int32(1),
		}
		var values []int64
		for page := 0; page < 8; page++ {
			response := qKind(t, s, kind, q)
			for _, row := range response.Batch.EntityResults {
				values = append(values, row.Entity.Properties["a.b"].GetIntegerValue())
			}
			if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
				break
			}
			q.StartCursor = response.Batch.EndCursor
		}
		if !slices.Equal(values, []int64{70, 70, 80, 80}) {
			t.Errorf("fallback repeated/excluded projection tuples: %v", values)
		}
	})
	t.Run("OR ordering uses the qualifying interpretation", func(t *testing.T) {
		kind := "DottedOROrderReview"
		upsertEntity(t, s, dsEntity(dsKey(kind, "collision"), map[string]*datastorepb.Value{
			"a.b": dsInt(70), "a": {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"b": dsInt(80)}}}},
		}))
		upsertEntity(t, s, dsEntity(dsKey(kind, "middle"), map[string]*datastorepb.Value{"a.b": dsInt(77)}))
		q := &datastorepb.Query{Filter: orFilter(propFilter("a.b", datastorepb.PropertyFilter_GREATER_THAN, dsInt(75)), propFilter("a.b", datastorepb.PropertyFilter_LESS_THAN, dsInt(0))), Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "a.b"}, Direction: datastorepb.PropertyOrder_ASCENDING}}}
		response := qKind(t, s, kind, q)
		var names []string
		for _, row := range response.Batch.EntityResults {
			names = append(names, row.Entity.Key.Path[0].GetName())
		}
		if !slices.Equal(names, []string{"middle", "collision"}) {
			t.Fatalf("ordered by an unqualified collision value: %v", names)
		}
	})
}

func TestReviewEmbeddedContainerDoesNotBecomeAnIndexKey(t *testing.T) {
	s := newTestDsServer(t)
	large := dsStr(strings.Repeat("x", 100_000))
	large.ExcludeFromIndexes = true
	upsertEntity(t, s, dsEntity(dsKey("EmbeddedReview", "one"), map[string]*datastorepb.Value{"obj": {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"x": dsInt(1), "large": large}}}}}))
	response := qKind(t, s, "EmbeddedReview", &datastorepb.Query{Filter: propFilter("obj.x", datastorepb.PropertyFilter_EQUAL, dsInt(1))})
	if len(response.Batch.EntityResults) != 1 {
		t.Fatalf("nested scalar query=%v", response)
	}
	response = qKind(t, s, "EmbeddedReview", &datastorepb.Query{Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "obj"}, Direction: datastorepb.PropertyOrder_ASCENDING}}})
	if len(response.Batch.EntityResults) != 0 {
		t.Fatalf("embedded container was indexed: %v", response)
	}
}

func TestReviewMissingKeyPartitionNormalizesIdentity(t *testing.T) {
	s := newTestDsServer(t)
	key := dsKey("ValidationReview", "local")
	key.PartitionId = nil
	upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"x": dsInt(1)}))
	qualified := dsKey("ValidationReview", "local")
	response := qKind(t, s, "ValidationReview", &datastorepb.Query{Filter: propFilter("__key__", datastorepb.PropertyFilter_EQUAL, &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: qualified}})})
	if len(response.Batch.EntityResults) != 1 || response.Batch.EntityResults[0].Entity.Key.PartitionId.GetProjectId() != testProject {
		t.Fatalf("normalized key lookup=%v", response)
	}
	if key.PartitionId != nil {
		t.Fatal("request key was mutated")
	}
}

func TestReviewTransformOnlyPreservesProperties(t *testing.T) {
	s := newTestDsServer(t)
	key := dsKey("TransformReview", "one")
	upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"a": dsInt(1), "b": dsInt(2)}))
	resp, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{
		ProjectId: testProject,
		Mutations: []*datastorepb.Mutation{{
			Operation:          &datastorepb.Mutation_Update{Update: dsEntity(key, nil)},
			PropertyMask:       &datastorepb.PropertyMask{},
			PropertyTransforms: []*datastorepb.PropertyTransform{{Property: "a", TransformType: &datastorepb.PropertyTransform_Increment{Increment: dsInt(5)}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := s.grpc.Lookup(context.Background(), &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}})
	if err != nil {
		t.Fatal(err)
	}
	if len(lookup.Found) != 1 {
		t.Fatalf("found = %v", lookup.Found)
	}
	got := lookup.Found[0]
	if got.Entity.Properties["a"].GetIntegerValue() != 6 || got.Entity.Properties["b"].GetIntegerValue() != 2 {
		t.Errorf("properties = %v, want a=6 b=2", got.Entity.Properties)
	}
	mutation := resp.MutationResults[0]
	if mutation.Version != got.Version || !proto.Equal(mutation.UpdateTime, got.UpdateTime) {
		t.Errorf("commit metadata %v differs from lookup %v", mutation, got)
	}
}

func TestReviewNestedTransformPreservesIndexExclusion(t *testing.T) {
	s := newTestDsServer(t)
	key := dsKey("NestedTransformReview", "one")
	upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{
		"x":      dsInt(1),
		"nested": {ExcludeFromIndexes: true, ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"n": dsInt(1)}}}},
	}))
	_, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mutations: []*datastorepb.Mutation{{
		Operation: &datastorepb.Mutation_Update{Update: dsEntity(key, nil)}, PropertyMask: &datastorepb.PropertyMask{},
		PropertyTransforms: []*datastorepb.PropertyTransform{{Property: "nested.n", TransformType: &datastorepb.PropertyTransform_Increment{Increment: dsInt(1)}}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := s.grpc.Lookup(context.Background(), &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}})
	if err != nil {
		t.Fatal(err)
	}
	nested := response.Found[0].Entity.Properties["nested"]
	if !nested.ExcludeFromIndexes || nested.GetEntityValue().Properties["n"].GetIntegerValue() != 2 {
		t.Fatalf("nested metadata/value changed: %v", nested)
	}
	query, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "NestedTransformReview"}}, Filter: andFilter(propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("nested.n", datastorepb.PropertyFilter_EQUAL, dsInt(2))),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(query.Batch.EntityResults) != 0 {
		t.Fatal("composite query indexed an excluded parent's child")
	}
}

func TestReviewMaskedUpdateChecksFinalEntitySize(t *testing.T) {
	s := newTestDsServer(t)
	key := dsKey("SizeReview", "one")
	large := &datastorepb.Value{ExcludeFromIndexes: true, ValueType: &datastorepb.Value_StringValue{StringValue: strings.Repeat("x", 800_000)}}
	upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"a": large}))
	_, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Update{Update: dsEntity(key, map[string]*datastorepb.Value{"b": large})}, PropertyMask: &datastorepb.PropertyMask{Paths: []string{"b"}}}}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized merged entity: %v", err)
	}
	lookup, err := s.grpc.Lookup(context.Background(), &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}})
	if err != nil {
		t.Fatal(err)
	}
	if len(lookup.Found) != 1 || lookup.Found[0].Entity.Properties["b"] != nil {
		t.Fatal("failed masked update changed stored entity")
	}
}

func TestReviewKeyFilterUsesPartitionAndAncestor(t *testing.T) {
	key := dsKey("Parent", "one")
	child := proto.Clone(key).(*datastorepb.Key)
	child.Path = append(child.Path, &datastorepb.Key_PathElement{Kind: "Child", IdType: &datastorepb.Key_PathElement_Name{Name: "one"}})
	foreign := proto.Clone(child).(*datastorepb.Key)
	foreign.PartitionId.NamespaceId = "foreign"
	for _, tc := range []struct {
		name    string
		operand *datastorepb.Key
		op      datastorepb.PropertyFilter_Operator
		want    bool
	}{
		{"self ancestor", child, datastorepb.PropertyFilter_HAS_ANCESTOR, true},
		{"parent ancestor", key, datastorepb.PropertyFilter_HAS_ANCESTOR, true},
		{"unrelated ancestor", dsKey("Parent", "two"), datastorepb.PropertyFilter_HAS_ANCESTOR, false},
		{"foreign equal", foreign, datastorepb.PropertyFilter_EQUAL, false},
		{"foreign ancestor", foreign, datastorepb.PropertyFilter_HAS_ANCESTOR, false},
		{"less than parent", key, datastorepb.PropertyFilter_LESS_THAN, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter := propFilter("__key__", tc.op, &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: tc.operand}})
			condition, err := compileQueryCondition(filter, 30)
			if err != nil {
				t.Fatal(err)
			}
			matcher := newQueryMatcher(context.Background(), condition, "")
			if got := matcher.accept(dsEntity(child, nil)); got != tc.want {
				t.Fatalf("match=%v want=%v", got, tc.want)
			}
			if matcher.err != nil {
				t.Fatal(matcher.err)
			}
		})
	}
	or := &datastorepb.Filter{FilterType: &datastorepb.Filter_CompositeFilter{CompositeFilter: &datastorepb.CompositeFilter{Op: datastorepb.CompositeFilter_OR, Filters: []*datastorepb.Filter{
		propFilter("__key__", datastorepb.PropertyFilter_HAS_ANCESTOR, &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: key}}),
		propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)),
	}}}}
	if got := extractAncestorPath(or, testProject, defaultDatabase, ""); got != "" {
		t.Fatalf("OR incorrectly narrowed to ancestor %q", got)
	}
}

func TestReviewFallbackArrayOrdering(t *testing.T) {
	for _, tc := range []struct {
		name      string
		filter    *datastorepb.Filter
		direction datastorepb.PropertyOrder_Direction
		want      []string
	}{
		{"range_ascending", propFilter("items", datastorepb.PropertyFilter_GREATER_THAN, dsInt(2)), datastorepb.PropertyOrder_ASCENDING, []string{"B", "A"}},
		{"range_descending", propFilter("items", datastorepb.PropertyFilter_LESS_THAN, dsInt(8)), datastorepb.PropertyOrder_DESCENDING, []string{"B", "C", "A"}},
		{"range_both", andFilter(propFilter("items", datastorepb.PropertyFilter_GREATER_THAN, dsInt(2)), propFilter("items", datastorepb.PropertyFilter_LESS_THAN, dsInt(8))), datastorepb.PropertyOrder_ASCENDING, []string{"B"}},
		{"IN", propFilter("items", datastorepb.PropertyFilter_IN, dsArray(dsInt(2), dsInt(9))), datastorepb.PropertyOrder_ASCENDING, []string{"C", "A"}},
		{"NOT_IN", propFilter("items", datastorepb.PropertyFilter_NOT_IN, dsArray(dsInt(1))), datastorepb.PropertyOrder_ASCENDING, []string{"C", "B", "A"}},
		{"not_equal", propFilter("items", datastorepb.PropertyFilter_NOT_EQUAL, dsInt(1)), datastorepb.PropertyOrder_ASCENDING, []string{"C", "B", "A"}},
		{"equality_ignored", propFilter("items", datastorepb.PropertyFilter_EQUAL, dsInt(1)), datastorepb.PropertyOrder_DESCENDING, []string{"C", "A"}},
		{"collapsed_range", andFilter(propFilter("items", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(1)), propFilter("items", datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL, dsInt(1))), datastorepb.PropertyOrder_DESCENDING, []string{"C", "A"}},
		{"multiple_equalities", andFilter(propFilter("items", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("items", datastorepb.PropertyFilter_EQUAL, dsInt(9))), datastorepb.PropertyOrder_ASCENDING, []string{"A"}},
		{"multiple_IN_witnesses", andFilter(propFilter("items", datastorepb.PropertyFilter_IN, dsArray(dsInt(1), dsInt(2))), propFilter("items", datastorepb.PropertyFilter_IN, dsArray(dsInt(9)))), datastorepb.PropertyOrder_ASCENDING, []string{"D", "A"}},
		// Java's equality index slots must also satisfy same-property ranges.
		// This mixed-array behavior is emulator-backed, not established by docs.
		{"equality_outside_range", andFilter(propFilter("items", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("items", datastorepb.PropertyFilter_GREATER_THAN, dsInt(2))), datastorepb.PropertyOrder_ASCENDING, nil},
		{"OR", orFilter(propFilter("items", datastorepb.PropertyFilter_GREATER_THAN, dsInt(2)), propFilter("n", datastorepb.PropertyFilter_EQUAL, dsInt(100))), datastorepb.PropertyOrder_ASCENDING, []string{"B", "A"}},
		{"OR_other_property", orFilter(propFilter("items", datastorepb.PropertyFilter_GREATER_THAN, dsInt(2)), propFilter("n", datastorepb.PropertyFilter_EQUAL, dsInt(3))), datastorepb.PropertyOrder_ASCENDING, []string{"A", "B"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestDsServer(t)
			const kind = "FallbackArrayOrder"
			for _, row := range []struct {
				name  string
				items []int64
				n     int64
			}{
				{"A", []int64{1, 9}, 3}, {"B", []int64{4, 5, 6, 7}, 2}, {"C", []int64{1, 2}, 1},
			} {
				var values []*datastorepb.Value
				for _, n := range row.items {
					values = append(values, dsInt(n))
				}
				upsertEntity(t, s, dsEntity(dsKey(kind, row.name), map[string]*datastorepb.Value{"items": dsArray(values...), "n": dsInt(row.n)}))
			}
			if tc.name == "multiple_IN_witnesses" {
				upsertEntity(t, s, dsEntity(dsKey(kind, "D"), map[string]*datastorepb.Value{"items": dsArray(dsInt(0), dsInt(1), dsInt(9), dsInt(100)), "n": dsInt(3)}))
			}
			snapshot := timestamppb.New(s.grpc.store.ReadTime())
			q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}, Filter: tc.filter, Order: []*datastorepb.PropertyOrder{
				{Property: &datastorepb.PropertyReference{Name: "items"}, Direction: tc.direction},
				{Property: &datastorepb.PropertyReference{Name: "n"}, Direction: datastorepb.PropertyOrder_ASCENDING},
			}}
			original := proto.Clone(q)
			response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ReadOptions: &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: snapshot}}, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
			if err != nil {
				t.Fatal(err)
			}
			if response.ExplainMetrics.PlanSummary.IndexesUsed[0].Fields["access_path"].GetStringValue() != "disk_sort" {
				t.Fatalf("not a fallback: %v", response.ExplainMetrics)
			}
			var got []string
			for _, row := range response.Batch.EntityResults {
				got = append(got, row.Entity.Key.Path[0].GetName())
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("order=%v, want %v", got, tc.want)
			}
			if !proto.Equal(q, original) {
				t.Fatal("query was mutated")
			}
			if tc.name == "multiple_IN_witnesses" {
				projected := proto.Clone(q).(*datastorepb.Query)
				projected.Projection = []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "n"}}}
				result := qKind(t, s, kind, projected)
				var values []int64
				for _, row := range result.Batch.EntityResults {
					values = append(values, row.Entity.Properties["n"].GetIntegerValue())
				}
				if !slices.Equal(values, []int64{3, 3}) {
					t.Fatalf("projected n=%v want=[3 3]", values)
				}
			}
			if tc.name == "multiple_IN_witnesses" || tc.name == "multiple_equalities" || tc.name == "equality_outside_range" {
				for _, aggregations := range [][]*datastorepb.AggregationQuery_Aggregation{{countAgg("count")}, {countAgg("count"), sumAgg("sum", "n"), avgAgg("avg", "n")}} {
					for _, nested := range []*datastorepb.Query{{Filter: tc.filter}, q} {
						var response datastorepb.RunAggregationQueryResponse
						mustPost(t, s, "runAggregationQuery", &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, ReadOptions: &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: snapshot}}, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: nested}, Aggregations: aggregations}}}, &response)
						values := response.Batch.AggregationResults[0].AggregateProperties
						wantCount := int64(len(tc.want))
						if len(nested.Order) > 0 {
							// Explicit array order retains all postfix index entries.
							if tc.name == "multiple_equalities" {
								wantCount = 2
							}
							if tc.name == "multiple_IN_witnesses" {
								wantCount = 6
							}
						}
						if got := values["count"].GetIntegerValue(); got != wantCount {
							t.Fatalf("count=%d want=%d", got, wantCount)
						}
						if len(aggregations) > 1 {
							if got := values["sum"].GetIntegerValue(); got != 3*wantCount {
								t.Fatalf("sum=%d", got)
							}
							if len(tc.want) > 0 && values["avg"].GetDoubleValue() != 3 {
								t.Fatalf("avg=%v", values["avg"])
							}
						}
					}
				}
			}
			// Re-run at the current snapshot (the index now exists), then page
			// the original historical fallback with both full and keys-only rows.
			for _, keysOnly := range []bool{false, true} {
				paged := proto.Clone(q).(*datastorepb.Query)
				paged.Limit = wrapperspb.Int32(1)
				if keysOnly {
					paged.Projection = []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "__key__"}}}
				}
				var pageKeys []string
				for page := 0; page <= len(tc.want); page++ {
					response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ReadOptions: &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: snapshot}}, QueryType: &datastorepb.RunQueryRequest_Query{Query: paged}})
					if err != nil {
						t.Fatal(err)
					}
					for _, row := range response.Batch.EntityResults {
						pageKeys = append(pageKeys, row.Entity.Key.Path[0].GetName())
					}
					if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
						break
					}
					paged.StartCursor = response.Batch.EndCursor
				}
				if !slices.Equal(pageKeys, tc.want) {
					t.Fatalf("keysOnly=%t paged=%v want=%v", keysOnly, pageKeys, tc.want)
				}
			}
			indexed, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
			if err != nil {
				t.Fatal(err)
			}
			var indexedKeys []string
			for _, row := range indexed.Batch.EntityResults {
				indexedKeys = append(indexedKeys, row.Entity.Key.Path[0].GetName())
			}
			if !slices.Equal(indexedKeys, tc.want) {
				t.Fatalf("current snapshot order=%v want=%v", indexedKeys, tc.want)
			}
			if tc.name == "IN" || tc.name == "NOT_IN" || tc.name == "not_equal" || tc.name == "multiple_equalities" || tc.name == "multiple_IN_witnesses" {
				builtin := proto.Clone(q).(*datastorepb.Query)
				builtin.Order = builtin.Order[:1]
				response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}, QueryType: &datastorepb.RunQueryRequest_Query{Query: builtin}})
				if err != nil {
					t.Fatal(err)
				}
				if tc.name != "multiple_equalities" && tc.name != "multiple_IN_witnesses" && response.ExplainMetrics.PlanSummary.IndexesUsed[0].Fields["access_path"].GetStringValue() != "builtin:items" {
					t.Fatalf("not builtin: %v", response.ExplainMetrics)
				}
				var got []string
				for _, row := range response.Batch.EntityResults {
					got = append(got, row.Entity.Key.Path[0].GetName())
				}
				if !slices.Equal(got, tc.want) {
					t.Fatalf("builtin=%v want=%v", got, tc.want)
				}
			}
			if tc.name == "equality_ignored" || tc.name == "collapsed_range" {
				q.Limit = wrapperspb.Int32(1)
				first, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ReadOptions: &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: snapshot}}, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
				if err != nil {
					t.Fatal(err)
				}
				q.StartCursor = first.Batch.EndCursor
				q.Order[0].Direction = datastorepb.PropertyOrder_ASCENDING
				second, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
				if err != nil {
					t.Fatalf("ignored order changed cursor identity: %v", err)
				}
				if rows := second.Batch.EntityResults; len(rows) != 1 || rows[0].Entity.Key.Path[0].GetName() != "A" {
					t.Fatalf("second page=%v", rows)
				}
			}
		})
	}
}

func TestReviewFallbackCorrelatedArrayPages(t *testing.T) {
	s := newTestDsServer(t)
	const kind = "CorrelatedArrayOrder"
	for _, row := range []struct {
		name string
		x, y []*datastorepb.Value
	}{
		{"A", []*datastorepb.Value{dsInt(1), dsInt(9), dsInt(1)}, []*datastorepb.Value{dsInt(1), dsInt(9)}},
		{"B", []*datastorepb.Value{dsInt(1)}, []*datastorepb.Value{dsInt(8)}},
		{"C", []*datastorepb.Value{dsInt(9)}, []*datastorepb.Value{dsInt(1)}},
	} {
		upsertEntity(t, s, dsEntity(dsKey(kind, row.name), map[string]*datastorepb.Value{"x": dsArray(row.x...), "y": dsArray(row.y...)}))
	}
	q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}, Filter: orFilter(
		andFilter(propFilter("x", datastorepb.PropertyFilter_LESS_THAN, dsInt(3)), propFilter("y", datastorepb.PropertyFilter_GREATER_THAN, dsInt(5))),
		andFilter(propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(8)), propFilter("y", datastorepb.PropertyFilter_LESS_THAN, dsInt(3))),
	), Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}}, {Property: &datastorepb.PropertyReference{Name: "y"}}}, Limit: wrapperspb.Int32(1)}
	for _, projection := range []bool{false, true} {
		query := proto.Clone(q).(*datastorepb.Query)
		if projection {
			query.Projection = []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "x"}}, {Property: &datastorepb.PropertyReference{Name: "y"}}}
		}
		var got []string
		var cursors [][]byte
		for page := 0; page < 6; page++ {
			response := qKind(t, s, kind, query)
			for _, row := range response.Batch.EntityResults {
				got = append(got, row.Entity.Key.Path[0].GetName())
				if projection {
					x, y := row.Entity.Properties["x"].GetIntegerValue(), row.Entity.Properties["y"].GetIntegerValue()
					if !(x < 3 && y > 5 || x > 8 && y < 3) {
						t.Fatalf("invalid projected tuple (%d,%d)", x, y)
					}
				}
			}
			cursors = append(cursors, response.Batch.EndCursor)
			if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
				break
			}
			query.StartCursor = response.Batch.EndCursor
		}
		want := []string{"B", "A", "A", "C"}
		if !projection {
			want = []string{"B", "A", "C"}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("projection=%t got=%v want=%v", projection, got, want)
		}
		query.StartCursor = nil
		query.EndCursor = cursors[1]
		query.Limit = wrapperspb.Int32(10)
		query.Offset = 1
		bounded := qKind(t, s, kind, query)
		if rows := bounded.Batch.EntityResults; len(rows) != 1 || rows[0].Entity.Key.Path[0].GetName() != "A" {
			t.Fatalf("bounded offset rows=%v", rows)
		}
	}
}

func TestReviewFallbackArrayExclusions(t *testing.T) {
	s := newTestDsServer(t)
	const kind = "ArrayExclusions"
	excluded := dsInt(-100)
	excluded.ExcludeFromIndexes = true
	container := dsArray(dsInt(-200))
	container.GetArrayValue().Values[0].ExcludeFromIndexes = true // SDK wire representation of an excluded array.
	for name, value := range map[string]*datastorepb.Value{
		"A": dsArray(excluded, dsInt(9)), "B": dsArray(dsInt(5)),
		"empty": dsArray(), "excluded": dsArray(excluded), "container": container,
	} {
		upsertEntity(t, s, dsEntity(dsKey(kind, name), map[string]*datastorepb.Value{"items": value, "n": dsInt(1)}))
	}
	upsertEntity(t, s, dsEntity(dsKey(kind, "missing"), map[string]*datastorepb.Value{"n": dsInt(1)}))
	snapshot := timestamppb.New(s.grpc.store.ReadTime())
	q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}, Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "items"}}, {Property: &datastorepb.PropertyReference{Name: "n"}}}}
	response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ReadOptions: &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: snapshot}}, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, row := range response.Batch.EntityResults {
		got = append(got, row.Entity.Key.Path[0].GetName())
	}
	if !slices.Equal(got, []string{"B", "A"}) {
		t.Fatalf("indexed values order=%v", got)
	}
}

func FuzzFallbackArrayOrderTuple(f *testing.F) {
	f.Add([]byte{1, 9, 1, 9, 3, 8, 0})
	f.Add([]byte{5, 7, 9, 2, 4, 8, 1})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 7 {
			return
		}
		x := []int64{int64(data[0] % 16), int64(data[1] % 16), int64(data[2] % 16)}
		y := []int64{int64(data[3] % 16), int64(data[4] % 16), int64(data[5] % 16)}
		q := &datastorepb.Query{Filter: orFilter(
			andFilter(propFilter("x", datastorepb.PropertyFilter_LESS_THAN, dsInt(4)), propFilter("y", datastorepb.PropertyFilter_GREATER_THAN, dsInt(6))),
			andFilter(propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(7)), propFilter("y", datastorepb.PropertyFilter_LESS_THAN, dsInt(3))),
		), Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}}, {Property: &datastorepb.PropertyReference{Name: "y"}}}}
		if data[6]&1 != 0 {
			q.Order[0].Direction = datastorepb.PropertyOrder_DESCENDING
		}
		if data[6]&2 != 0 {
			q.Order[1].Direction = datastorepb.PropertyOrder_DESCENDING
		}
		entity := dsEntity(dsKey("FuzzArrayOrder", "one"), map[string]*datastorepb.Value{"x": dsArray(dsInt(x[0]), dsInt(x[1]), dsInt(x[2])), "y": dsArray(dsInt(y[0]), dsInt(y[1]), dsInt(y[2]))})
		got, err := newFallbackOrdering(q, nil).key(context.Background(), entity)
		if err != nil {
			t.Fatal(err)
		}
		// Independent exhaustive enumeration, using scalar encoding only after
		// evaluating the integer contract directly (not the production matcher).
		var want []byte
		encoder := newFallbackOrdering(&datastorepb.Query{Order: q.Order}, nil)
		for _, a := range x {
			for _, b := range y {
				if !(a < 4 && b > 6 || a > 7 && b < 3) {
					continue
				}
				candidate, err := encoder.key(context.Background(), dsEntity(entity.Key, map[string]*datastorepb.Value{"x": dsInt(a), "y": dsInt(b)}))
				if err != nil {
					t.Fatal(err)
				}
				if want == nil || string(candidate) < string(want) {
					want = candidate
				}
			}
		}
		if string(got) != string(want) {
			t.Fatalf("x=%v y=%v directions=%d got=%x want=%x", x, y, data[6]&3, got, want)
		}
		condition, err := compileQueryCondition(q.Filter, 0)
		if err != nil {
			t.Fatal(err)
		}
		factored, err := newConditionOrdering(q, nil, condition).key(context.Background(), entity)
		if err != nil {
			t.Fatal(err)
		}
		if string(factored) != string(want) {
			t.Fatalf("factored x=%v y=%v got=%x want=%x", x, y, factored, want)
		}
	})
}

func TestReviewFactoredWitnessOrdering(t *testing.T) {
	entity := dsEntity(dsKey("FactoredWitness", "one"), map[string]*datastorepb.Value{
		"x":        dsArray(dsInt(0), dsInt(1), dsInt(9), dsInt(100)),
		"y":        dsArray(dsInt(2), dsInt(8)),
		"s":        dsArray(dsStr("z"), dsStr("a")),
		"null":     dsNull(),
		"excluded": {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 1}, ExcludeFromIndexes: true},
	})
	for _, tc := range []struct {
		name         string
		filter       *datastorepb.Filter
		wantX, wantY int64
		match        bool
	}{
		{"independent_IN", andFilter(propFilter("x", datastorepb.PropertyFilter_IN, dsArray(dsInt(1), dsInt(2))), propFilter("x", datastorepb.PropertyFilter_IN, dsArray(dsInt(9)))), 0, 2, true},
		{"single_IN", propFilter("x", datastorepb.PropertyFilter_IN, dsArray(dsInt(9))), 9, 2, true},
		// Datastore indexes distinguish integer and double values, including IN.
		{"numeric_IN", propFilter("x", datastorepb.PropertyFilter_IN, dsArray(dsDouble(9))), 0, 0, false},
		{"numeric_range", andFilter(propFilter("x", datastorepb.PropertyFilter_IN, dsArray(dsDouble(9))), propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsDouble(8.5))), 0, 0, false},
		{"equality_range", andFilter(propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(2))), 0, 0, false},
		{"correlated", orFilter(andFilter(propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(8)), propFilter("y", datastorepb.PropertyFilter_LESS_THAN, dsInt(3))), andFilter(propFilter("x", datastorepb.PropertyFilter_LESS_THAN, dsInt(2)), propFilter("y", datastorepb.PropertyFilter_GREATER_THAN, dsInt(7)))), 0, 8, true},
		{"inactive_exclusion", orFilter(propFilter("x", datastorepb.PropertyFilter_NOT_EQUAL, dsInt(0)), propFilter("y", datastorepb.PropertyFilter_EQUAL, dsInt(2))), 0, 2, true},
		{"active_exclusion", andFilter(propFilter("x", datastorepb.PropertyFilter_NOT_EQUAL, dsInt(0)), propFilter("y", datastorepb.PropertyFilter_GREATER_THAN, dsInt(3))), 1, 8, true},
		{"NOT_IN", propFilter("x", datastorepb.PropertyFilter_NOT_IN, dsArray(dsInt(0), dsInt(1), dsInt(9))), 100, 2, true},
		{"NOT_IN_range", andFilter(propFilter("x", datastorepb.PropertyFilter_NOT_IN, dsArray(dsInt(0), dsInt(1), dsInt(9))), propFilter("x", datastorepb.PropertyFilter_LESS_THAN, dsInt(100))), 0, 0, false},
		{"excluded_equality", andFilter(propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("x", datastorepb.PropertyFilter_NOT_EQUAL, dsInt(1))), 0, 0, false},
		{"string_range", propFilter("s", datastorepb.PropertyFilter_LESS_THAN, dsStr("b")), 0, 2, true},
		{"string_witness_range", andFilter(propFilter("s", datastorepb.PropertyFilter_EQUAL, dsStr("z")), propFilter("s", datastorepb.PropertyFilter_LESS_THAN, dsStr("b"))), 0, 0, false},
		{"null", propFilter("null", datastorepb.PropertyFilter_EQUAL, dsNull()), 0, 2, true},
		{"unindexed", propFilter("excluded", datastorepb.PropertyFilter_EQUAL, dsInt(1)), 0, 0, false},
		{"missing_OR", orFilter(propFilter("missing", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(9))), 0, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := &datastorepb.Query{Filter: tc.filter, Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}}, {Property: &datastorepb.PropertyReference{Name: "y"}}}}
			var want []byte
			if tc.match {
				var err error
				want, err = newFallbackOrdering(&datastorepb.Query{Order: q.Order}, nil).key(context.Background(), dsEntity(entity.Key, map[string]*datastorepb.Value{"x": dsInt(tc.wantX), "y": dsInt(tc.wantY)}))
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, allowance := range []int{0, 30} {
				condition, err := compileQueryCondition(tc.filter, allowance)
				if err != nil {
					t.Fatal(err)
				}
				got, err := newConditionOrdering(q, nil, condition).key(context.Background(), entity)
				if err != nil {
					t.Fatal(err)
				}
				if string(got) != string(want) {
					t.Fatalf("allowance=%d got=%x want=%x", allowance, got, want)
				}
			}
		})
	}
}

func TestReviewFallbackOrderCancellation(t *testing.T) {
	entity := dsEntity(dsKey("CancelArrayOrder", "one"), map[string]*datastorepb.Value{"x": dsArray(dsInt(1), dsInt(9))})
	q := &datastorepb.Query{Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}}}}
	if _, err := newFallbackOrdering(q, nil).key(&cancelAfterFirstCheck{}, entity); err != context.Canceled {
		t.Fatalf("array loop cancellation=%v", err)
	}
	var clauses []*datastorepb.Filter
	for range 24 {
		clauses = append(clauses, orFilter(propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(0)), propFilter("x", datastorepb.PropertyFilter_LESS_THAN, dsInt(10))))
	}
	q.Filter = andFilter(clauses...)
	if _, err := newFallbackOrdering(q, nil).key(&cancelAfterFirstCheck{}, entity); err != context.Canceled {
		t.Fatalf("lazy branch cancellation=%v", err)
	}
}

type queryCheckpointCounter struct {
	context.Context
	checks int
}

func (c *queryCheckpointCounter) Err() error { c.checks++; return c.Context.Err() }

func TestReviewSeparableWitnessWork(t *testing.T) {
	q := &datastorepb.Query{Filter: andFilter(propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(0)), propFilter("y", datastorepb.PropertyFilter_GREATER_THAN, dsInt(0))), Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}}, {Property: &datastorepb.PropertyReference{Name: "y"}}}}
	ordering := newFallbackOrdering(q, nil)
	for _, size := range []int{8, 64, 512} {
		values := make([]*datastorepb.Value, size)
		for i := range values {
			values[i] = dsInt(int64(size - i))
		}
		entity := dsEntity(dsKey("WitnessWork", "one"), map[string]*datastorepb.Value{"x": dsArray(values...), "y": dsArray(values...)})
		ctx := &queryCheckpointCounter{Context: context.Background()}
		got, err := ordering.key(ctx, entity)
		if err != nil {
			t.Fatal(err)
		}
		want, err := newFallbackOrdering(&datastorepb.Query{Order: q.Order}, nil).key(context.Background(), dsEntity(entity.Key, map[string]*datastorepb.Value{"x": dsInt(1), "y": dsInt(1)}))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("size=%d key=%x want=%x", size, got, want)
		}
		// Each check is a bounded loop step here (two fixed scalar constraints).
		// This bound is intentionally not claimed for correlated domain search.
		if ctx.checks < 2*size || ctx.checks > 8*size+10 {
			t.Fatalf("size=%d checkpoints=%d, want linear work", size, ctx.checks)
		}
		t.Logf("values=%d checkpoints=%d", 2*size, ctx.checks)
	}
	for _, size := range []int{8, 32, 64} {
		values := make([]*datastorepb.Value, size)
		for i := range values {
			values[i] = dsInt(int64(i + 1))
		}
		query := &datastorepb.Query{Order: q.Order, Filter: orFilter(andFilter(propFilter("x", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(int64(size))), propFilter("y", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(int64(size)))), andFilter(propFilter("x", datastorepb.PropertyFilter_LESS_THAN, dsInt(0)), propFilter("y", datastorepb.PropertyFilter_LESS_THAN, dsInt(0))))}
		condition, err := compileQueryCondition(query.Filter, 0)
		if err != nil {
			t.Fatal(err)
		}
		entity := dsEntity(dsKey("WitnessWork", "one"), map[string]*datastorepb.Value{"x": dsArray(values...), "y": dsArray(values...)})
		ctx := &queryCheckpointCounter{Context: context.Background()}
		got, err := newConditionOrdering(query, nil, condition).key(ctx, entity)
		if err != nil {
			t.Fatal(err)
		}
		want, err := newFallbackOrdering(&datastorepb.Query{Order: q.Order}, nil).key(context.Background(), dsEntity(entity.Key, map[string]*datastorepb.Value{"x": dsInt(int64(size)), "y": dsInt(int64(size))}))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("factored size=%d key=%x want=%x", size, got, want)
		}
		// Two range-only ordered domains can search N² tuples, but must not
		// rescan all N values to bind each singleton candidate envelope.
		if ctx.checks > 24*size*size+4*size+20 {
			t.Fatalf("factored size=%d checkpoints=%d exceeds quadratic bound", size, ctx.checks)
		}
		t.Logf("factored values=%d checkpoints=%d", 2*size, ctx.checks)
	}
}

func TestReviewHistoricalCompositeAndMultipleRanges(t *testing.T) {
	s := newTestDsServer(t)
	const kind = "HistoricalReview"
	for _, name := range []string{"one", "two"} {
		upsertEntity(t, s, dsEntity(dsKey(kind, name), map[string]*datastorepb.Value{"x": dsInt(1), "y": dsInt(2)}))
	}
	snapshot := timestamppb.New(s.grpc.store.ReadTime())
	for _, tc := range []struct {
		name    string
		query   *datastorepb.Query
		options *datastorepb.ReadOptions
	}{
		{"new_index_at_old_snapshot", &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}, Filter: andFilter(propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("y", datastorepb.PropertyFilter_EQUAL, dsInt(2)))}, &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: snapshot}}},
		{"multiple_ranges", &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}, Filter: andFilter(propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(0)), propFilter("y", datastorepb.PropertyFilter_LESS_THAN, dsInt(3)))}, nil},
		{"kindless", &datastorepb.Query{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ReadOptions: tc.options, QueryType: &datastorepb.RunQueryRequest_Query{Query: tc.query}})
			if err != nil {
				t.Fatal(err)
			}
			if len(response.Batch.EntityResults) != 2 {
				t.Fatalf("got %d results, want 2", len(response.Batch.EntityResults))
			}
		})
	}
	t.Run("conditional_equality_order", func(t *testing.T) {
		const arrayKind = "ConditionalEqualityOrder"
		seedKind(t, s, arrayKind, []seedRow{
			{"A", map[string]*datastorepb.Value{"x": dsArray(dsInt(1), dsInt(9))}},
			{"D", map[string]*datastorepb.Value{"x": dsArray(dsInt(0), dsInt(1), dsInt(9), dsInt(100))}},
		})
		query := &datastorepb.Query{Filter: andFilter(orFilter(propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(100))), propFilter("x", datastorepb.PropertyFilter_LESS_THAN, dsInt(150))), Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}}}, Limit: wrapperspb.Int32(1)}
		var got []string
		for range 3 {
			response := qKind(t, s, arrayKind, query)
			for _, row := range response.Batch.EntityResults {
				got = append(got, row.Entity.Key.Path[0].GetName())
			}
			if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
				break
			}
			query.StartCursor = response.Batch.EndCursor
		}
		if !slices.Equal(got, []string{"D", "A"}) {
			t.Fatalf("conditional equality order=%v want=[D A]", got)
		}
	})
	t.Run("secondary_array_range_order", func(t *testing.T) {
		const arrayKind = "SecondaryArrayRange"
		seedKind(t, s, arrayKind, []seedRow{
			{"A", map[string]*datastorepb.Value{"x": dsInt(1), "y": dsArray(dsInt(0), dsInt(9))}},
			{"B", map[string]*datastorepb.Value{"x": dsInt(1), "y": dsArray(dsInt(8))}},
		})
		q := &datastorepb.Query{Filter: andFilter(propFilter("x", datastorepb.PropertyFilter_GREATER_THAN, dsInt(0)), propFilter("y", datastorepb.PropertyFilter_GREATER_THAN, dsInt(5))), Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}}, {Property: &datastorepb.PropertyReference{Name: "y"}}}}
		q.Kind = []*datastorepb.KindExpression{{Name: arrayKind}}
		for attempt := range 2 {
			response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, row := range response.Batch.EntityResults {
				got = append(got, row.Entity.Key.Path[0].GetName())
			}
			if !slices.Equal(got, []string{"B", "A"}) {
				t.Fatalf("secondary range order=%v want=[B A]", got)
			}
			if attempt == 1 && response.ExplainMetrics.ExecutionStats.DebugStats.Fields["documents_scanned"].GetStringValue() != "2" {
				t.Fatalf("rejected tuple decoded full entity: %v", response.ExplainMetrics.ExecutionStats.DebugStats)
			}
		}
	})
}

func TestReviewImplicitDistinctOrderKeepsGroupsTogether(t *testing.T) {
	s := newTestDsServer(t)
	for i, group := range []int64{1, 2, 1} {
		upsertEntity(t, s, dsEntity(dsKeyID("DistinctOrderReview", int64(i+1)), map[string]*datastorepb.Value{"x": dsInt(group), "y": dsInt(int64(i + 1))}))
	}
	var cursor []byte
	var groups []int64
	for page := 0; page < 5; page++ {
		response := qKind(t, s, "DistinctOrderReview", &datastorepb.Query{Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "y"}}, {Property: &datastorepb.PropertyReference{Name: "x"}}}, DistinctOn: []*datastorepb.PropertyReference{{Name: "x"}}, Limit: wrapperspb.Int32(1), StartCursor: cursor})
		for _, row := range response.Batch.EntityResults {
			groups = append(groups, row.Entity.Properties["x"].GetIntegerValue())
		}
		if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
			break
		}
		cursor = response.Batch.EndCursor
	}
	if !slices.Equal(groups, []int64{1, 2}) {
		t.Fatalf("distinct groups across pages=%v", groups)
	}
}

func TestReviewFallbackProjectionPagination(t *testing.T) {
	s := newTestDsServer(t)
	const kind = "FallbackProjectionReview"
	for i, values := range [][]int64{{1, 9, 1}, {2, 8}} {
		array := &datastorepb.ArrayValue{}
		for _, value := range values {
			array.Values = append(array.Values, dsInt(value))
		}
		upsertEntity(t, s, dsEntity(dsKeyID(kind, int64(i+1)), map[string]*datastorepb.Value{"items": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: array}}, "group": dsInt(1)}))
	}
	snapshot := timestamppb.New(s.grpc.store.ReadTime())
	for _, historical := range []bool{true, false} {
		var cursor []byte
		var got []int64
		for page := 0; page < 8; page++ {
			q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}, Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "items"}}}, Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "items"}, Direction: datastorepb.PropertyOrder_ASCENDING}}, Filter: propFilter("group", datastorepb.PropertyFilter_EQUAL, dsInt(1)), Limit: wrapperspb.Int32(1), StartCursor: cursor}
			request := &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}}
			if historical {
				request.ReadOptions = &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: snapshot}}
			} else {
				q.Filter = &datastorepb.Filter{FilterType: &datastorepb.Filter_CompositeFilter{CompositeFilter: &datastorepb.CompositeFilter{Op: datastorepb.CompositeFilter_OR, Filters: []*datastorepb.Filter{q.Filter, propFilter("group", datastorepb.PropertyFilter_EQUAL, dsInt(2))}}}}
			}
			response, err := s.grpc.RunQuery(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range response.Batch.EntityResults {
				got = append(got, row.Entity.Properties["items"].GetIntegerValue())
			}
			if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
				break
			}
			cursor = response.Batch.EndCursor
		}
		if !slices.Equal(got, []int64{1, 2, 8, 9}) {
			t.Fatalf("historical=%v projection pages=%v", historical, got)
		}
	}
}

func TestReviewCountUpToZero(t *testing.T) {
	s := newTestDsServer(t)
	seedKind(t, s, "ZeroCountReview", []seedRow{{"a", map[string]*datastorepb.Value{"n": dsInt(7)}}})
	for _, mixed := range []bool{false, true} {
		t.Run("mixed="+strconv.FormatBool(mixed), func(t *testing.T) {
			count := countAgg("n")
			count.GetCount().UpTo = wrapperspb.Int64(0)
			aggregations := []*datastorepb.AggregationQuery_Aggregation{count}
			if mixed {
				aggregations = append(aggregations, sumAgg("total", "n"))
			}
			response := runAggKind(t, s, "ZeroCountReview", nil, aggregations)
			properties := response.Batch.AggregationResults[0].AggregateProperties
			if value, ok := properties["n"].GetValueType().(*datastorepb.Value_IntegerValue); !ok || value.IntegerValue != 0 {
				t.Fatalf("count = %v, want integer zero", properties["n"])
			}
			if mixed && properties["total"].GetIntegerValue() != 7 {
				t.Fatalf("sum = %v, want 7", properties["total"])
			}
		})
	}
}

func TestReviewCompositeCountIncludesAncestor(t *testing.T) {
	s := newTestDsServer(t)
	key := dsKey("AncestorCountReview", "root")
	upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"x": dsInt(1), "y": dsInt(2)}))
	response := qKind(t, s, "AncestorCountReview", &datastorepb.Query{Filter: andFilter(propFilter("__key__", datastorepb.PropertyFilter_HAS_ANCESTOR, &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: key}}), propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("y", datastorepb.PropertyFilter_EQUAL, dsInt(2))), Limit: wrapperspb.Int32(0), Offset: math.MaxInt32})
	if response.Batch.SkippedResults != 1 {
		t.Fatalf("count=%d, want ancestor itself", response.Batch.SkippedResults)
	}
}

func TestReviewTimestampOrderAcrossFullRange(t *testing.T) {
	s := newTestDsServer(t)
	years := []int{1, 1600, 1970, 2500, 9999}
	for _, year := range years {
		value := &datastorepb.Value{ValueType: &datastorepb.Value_TimestampValue{TimestampValue: timestamppb.New(time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC))}}
		upsertEntity(t, s, dsEntity(dsKeyID("DateReview", int64(year)), map[string]*datastorepb.Value{"date": value}))
	}
	response := qKind(t, s, "DateReview", &datastorepb.Query{Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "date"}, Direction: datastorepb.PropertyOrder_ASCENDING}}})
	if len(response.Batch.EntityResults) != len(years) {
		t.Fatalf("got %d dates", len(response.Batch.EntityResults))
	}
	for i, row := range response.Batch.EntityResults {
		if got := row.Entity.Properties["date"].GetTimestampValue().AsTime().Year(); got != years[i] {
			t.Fatalf("date %d = %d, want %d", i, got, years[i])
		}
	}
}

func TestReviewExplainCountsFilteredReads(t *testing.T) {
	s := newTestDsServer(t)
	for id := int64(1); id <= 6; id++ {
		upsertEntity(t, s, dsEntity(dsKeyID("ExplainReview", id), map[string]*datastorepb.Value{"left": dsInt(id), "right": dsInt(id)}))
	}
	filter := &datastorepb.Filter{FilterType: &datastorepb.Filter_CompositeFilter{CompositeFilter: &datastorepb.CompositeFilter{Op: datastorepb.CompositeFilter_OR, Filters: []*datastorepb.Filter{
		// Membership alternatives retain residual scanning rather than scalar probes.
		propFilter("left", datastorepb.PropertyFilter_IN, dsArray(dsInt(6))), propFilter("right", datastorepb.PropertyFilter_GREATER_THAN, dsInt(5)),
	}}}}
	response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}, QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "ExplainReview"}}, Filter: filter, Limit: wrapperspb.Int32(1)}}})
	if err != nil {
		t.Fatal(err)
	}
	stats := response.ExplainMetrics.ExecutionStats
	// Normalized right ordering scans six candidates, then reloads the one
	// accepted fallback row. Both reads belong in the diagnostic count.
	if stats.ResultsReturned != 1 || stats.DebugStats.Fields["documents_scanned"].GetStringValue() != "7" {
		t.Fatalf("filtered read stats = %v", stats)
	}
}

func TestReviewExplainPlansWithoutBuildingIndexes(t *testing.T) {
	s := newTestDsServer(t)
	for _, tc := range []struct {
		name          string
		filter        *datastorepb.Filter
		access, state string
	}{
		{"builtin", propFilter("score", datastorepb.PropertyFilter_EQUAL, dsInt(1)), "builtin:score", "READY"},
		{"missing composite", andFilter(propFilter("score", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("group", datastorepb.PropertyFilter_EQUAL, dsInt(1))), "composite", "REQUIRED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ExplainOptions: &datastorepb.ExplainOptions{}, QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "ExplainPlanReview"}}, Filter: tc.filter}}})
			if err != nil {
				t.Fatal(err)
			}
			entry := response.ExplainMetrics.PlanSummary.IndexesUsed[0].Fields
			if entry["access_path"].GetStringValue() != tc.access || entry["state"].GetStringValue() != tc.state {
				t.Fatalf("plan=%v", response.ExplainMetrics.PlanSummary)
			}
			indexes, err := s.grpc.store.ListDsCompositeIndexes(testProject)
			if err != nil {
				t.Fatal(err)
			}
			if len(indexes) != 0 {
				t.Fatal("plan-only explain built an index")
			}
		})
	}
}

func TestReviewInternalQueryPagesKeepSnapshot(t *testing.T) {
	s := newTestDsServer(t)
	const kind = "PinnedPageReview"
	for _, name := range []string{"a", "b"} {
		upsertEntity(t, s, dsEntity(dsKey(kind, name), map[string]*datastorepb.Value{"n": dsInt(1)}))
	}
	query := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}, Limit: wrapperspb.Int32(1)}
	request := &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: query}}
	var snapshot querySnapshot
	defer func() {
		if snapshot.release != nil {
			snapshot.release()
		}
	}()
	first, err := s.grpc.runQueryWithSnapshot(context.Background(), request, &snapshot)
	if err != nil {
		t.Fatal(err)
	}
	upsertEntity(t, s, dsEntity(dsKey(kind, "b"), map[string]*datastorepb.Value{"n": dsInt(2)}))
	query.StartCursor = first.Batch.EndCursor
	second, err := s.grpc.runQueryWithSnapshot(context.Background(), request, &snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Batch.EntityResults) != 1 || second.Batch.EntityResults[0].Entity.Properties["n"].GetIntegerValue() != 1 || !proto.Equal(first.Batch.ReadTime, second.Batch.ReadTime) {
		t.Fatalf("continuation changed snapshot: first=%v second=%v", first, second)
	}
}

func TestReviewFallbackExactPageEnd(t *testing.T) {
	s := newTestDsServer(t)
	upsertEntity(t, s, dsEntity(dsKey("ExactPageEnd", "one"), map[string]*datastorepb.Value{"x": dsInt(1), "y": dsInt(2), "z": dsInt(3)}))
	for _, limit := range []int32{0, 1} {
		response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
			Kind: []*datastorepb.KindExpression{{Name: "ExactPageEnd"}}, Limit: wrapperspb.Int32(limit), Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "z"}}},
			Filter: orFilter(propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("y", datastorepb.PropertyFilter_EQUAL, dsInt(2))),
		}}})
		if err != nil {
			t.Fatal(err)
		}
		if len(response.Batch.EntityResults) != int(limit) || response.Batch.MoreResults != datastorepb.QueryResultBatch_NO_MORE_RESULTS {
			t.Fatalf("limit=%d: %v", limit, response.Batch)
		}
	}
}

func TestReviewAggregationExplainIncludesEveryPage(t *testing.T) {
	for _, longKeys := range []bool{false, true} {
		t.Run(strconv.FormatBool(longKeys), func(t *testing.T) {
			s := newTestDsServer(t)
			budget := int64(64 << 20)
			if longKeys {
				budget = 2 << 20
			}
			if err := s.grpc.store.ConfigureExactQueries(budget, 1); err != nil {
				t.Fatal(err)
			}
			if longKeys {
				bulkSeedLongPaths(t, s, 2000)
			} else {
				bulkSeed(t, s, 2000)
			}
			ctx, work := storage.WithQueryWork(context.Background(), 0)
			// An explicit snapshot keeps this regression on the retained fallback.
			response, err := s.grpc.RunAggregationQuery(ctx, &datastorepb.RunAggregationQueryRequest{ProjectId: testProject, ReadOptions: &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: timestamppb.New(s.grpc.store.ReadTime())}}, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}, QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: &datastorepb.AggregationQuery{
				QueryType:    &datastorepb.AggregationQuery_NestedQuery{NestedQuery: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Widget"}}}},
				Aggregations: []*datastorepb.AggregationQuery_Aggregation{sumAgg("total", "score")},
			}}})
			if err != nil {
				t.Fatal(err)
			}
			if got := response.Batch.AggregationResults[0].AggregateProperties["total"].GetIntegerValue(); got != 1_999_000 {
				t.Fatalf("sum = %d", got)
			}
			reads, err := strconv.ParseInt(response.ExplainMetrics.ExecutionStats.DebugStats.Fields["documents_scanned"].GetStringValue(), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			if reads != 4000 {
				t.Errorf("entity reads = %d, want 4000 (one source scan and one materialization)", reads)
			}
			if got := response.ExplainMetrics.ExecutionStats.DebugStats.Fields["index_entries_scanned"].GetStringValue(); got != "2000" {
				t.Errorf("source entries = %s, want 2000", got)
			}
			if got := work.Snapshot()[storage.WorkIndexEntries]; got != 2000 {
				t.Errorf("instrumented source visits = %d, want 2000", got)
			}
			if spilled := work.Snapshot()[storage.WorkScratchWriteBytes] > 0; spilled != longKeys {
				t.Errorf("spilled = %t, want %t", spilled, longKeys)
			}
		})
	}
}

func TestReviewCoveringQueriesAvoidEntityReads(t *testing.T) {
	s := newTestDsServer(t)
	upsertEntity(t, s, dsEntity(dsKey("CoverReview", "one"), map[string]*datastorepb.Value{
		"n": dsInt(1), "m": dsInt(2), "payload": {ExcludeFromIndexes: true, ValueType: &datastorepb.Value_StringValue{StringValue: strings.Repeat("x", 100_000)}},
	}))
	for _, tc := range []struct {
		name, projection string
		filter           *datastorepb.Filter
	}{
		{"keys", "__key__", nil},
		{"equality keys", "__key__", propFilter("n", datastorepb.PropertyFilter_EQUAL, dsInt(1))},
		{"composite equality keys", "__key__", andFilter(propFilter("n", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("m", datastorepb.PropertyFilter_EQUAL, dsInt(2)))},
		{"builtin projection", "n", nil},
		{"composite projection", "m", propFilter("n", datastorepb.PropertyFilter_EQUAL, dsInt(1))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}, QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
				Kind: []*datastorepb.KindExpression{{Name: "CoverReview"}}, Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: tc.projection}}}, Filter: tc.filter,
			}}})
			if err != nil {
				t.Fatal(err)
			}
			if len(response.Batch.EntityResults) != 1 {
				t.Fatalf("results=%v", response.Batch)
			}
			if got := response.ExplainMetrics.ExecutionStats.DebugStats.Fields["documents_scanned"].GetStringValue(); got != "0" {
				t.Fatalf("covering query read %s entity records", got)
			}
			if response.Batch.EntityResults[0].Entity.Properties["payload"] != nil {
				t.Fatal("unprojected payload returned")
			}
		})
	}
}

func TestReviewEqualityKeysCoveringPagination(t *testing.T) {
	for _, property := range []string{"x", "a.b"} {
		t.Run(property, func(t *testing.T) {
			s := newTestDsServer(t)
			for _, name := range []string{"a", "b", "c", "d", "excluded", "missing"} {
				props := map[string]*datastorepb.Value{property: dsInt(1)}
				switch name {
				case "b":
					props[property] = dsArray(dsInt(1), dsInt(2), dsInt(1))
				case "c", "d":
					if property == "a.b" {
						props["a"] = &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"b": dsInt(1)}}}}
						if name == "c" {
							delete(props, property)
						}
					}
				case "excluded":
					props[property].ExcludeFromIndexes = true
				case "missing":
					delete(props, property)
				}
				upsertEntity(t, s, dsEntity(dsKey("EqualityKeys", name), props))
			}
			snapshot := timestamppb.New(s.grpc.store.ReadTime())
			upsertEntity(t, s, dsEntity(dsKey("EqualityKeys", "b"), map[string]*datastorepb.Value{property: dsInt(2)}))
			for _, historical := range []bool{false, true} {
				var readOptions *datastorepb.ReadOptions
				want := []string{"c", "d"} // Offset skips a; b no longer matches.
				if historical {
					readOptions = &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: snapshot}}
					want = []string{"b", "c", "d"}
				}
				query := &datastorepb.Query{
					Kind: []*datastorepb.KindExpression{{Name: "EqualityKeys"}}, Filter: propFilter(property, datastorepb.PropertyFilter_EQUAL, dsInt(1)), Offset: 1,
					Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: property}, Direction: datastorepb.PropertyOrder_DESCENDING}},
				}
				full, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ReadOptions: readOptions, QueryType: &datastorepb.RunQueryRequest_Query{Query: query}})
				if err != nil {
					t.Fatal(err)
				}
				query.Projection = []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "__key__"}}}
				query.Limit = wrapperspb.Int32(1)
				var got []string
				for page := 0; page < 6; page++ {
					response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ReadOptions: readOptions, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}, QueryType: &datastorepb.RunQueryRequest_Query{Query: query}})
					if err != nil {
						t.Fatal(err)
					}
					if reads := response.ExplainMetrics.ExecutionStats.DebugStats.Fields["documents_scanned"].GetStringValue(); reads != "0" {
						t.Fatalf("historical=%t keys-only query read %s source entities", historical, reads)
					}
					for _, row := range response.Batch.EntityResults {
						if len(got) >= len(full.Batch.EntityResults) {
							t.Fatal("extra keys-only result")
						}
						original := full.Batch.EntityResults[len(got)]
						if !proto.Equal(row.Entity.Key, original.Entity.Key) || row.Version != original.Version || len(row.Entity.Properties) != 0 {
							t.Fatalf("keys-only row differs from full result: %v vs %v", row, original)
						}
						got = append(got, row.Entity.Key.Path[0].GetName())
					}
					if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
						break
					}
					query.StartCursor, query.Offset = response.Batch.EndCursor, 0
				}
				if !slices.Equal(got, want) {
					t.Fatalf("historical=%t got=%v want=%v", historical, got, want)
				}
			}
		})
	}
}

func TestReviewCompositeEqualityKeysPagination(t *testing.T) {
	for _, dimensions := range []int{2, 3} {
		for _, reordered := range []bool{false, true} {
			t.Run(strconv.Itoa(dimensions)+"/reordered="+strconv.FormatBool(reordered), func(t *testing.T) {
				s := newTestDsServer(t)
				values := map[string]*datastorepb.Value{"n": dsInt(1), "m": dsStr("match")}
				if dimensions == 3 {
					values["z"] = &datastorepb.Value{ValueType: &datastorepb.Value_NullValue{}}
				}
				var filters []*datastorepb.Filter
				var properties []storage.DsIndexProperty
				for _, name := range []string{"m", "n", "z"} {
					if value := values[name]; value != nil {
						filters = append(filters, propFilter(name, datastorepb.PropertyFilter_EQUAL, value))
						properties = append(properties, storage.DsIndexProperty{Name: name, Desc: reordered})
					}
				}
				if reordered {
					slices.Reverse(properties)
				}
				for _, name := range []string{"a", "b", "c", "d", "missing", "excluded"} {
					entity := dsEntity(dsKey("CompositeKeys", name), values)
					entity = proto.Clone(entity).(*datastorepb.Entity)
					if name == "b" {
						entity.Properties["n"] = dsArray(dsInt(1), dsInt(2), dsInt(1))
						entity.Properties["m"] = dsArray(dsStr("other"), dsStr("match"), dsStr("match"))
					}
					if name == "missing" {
						delete(entity.Properties, "m")
					}
					if name == "excluded" {
						entity.Properties["m"].ExcludeFromIndexes = true
					}
					upsertEntity(t, s, entity)
				}
				if _, _, err := s.grpc.store.EnsureDsCompositeIndex(context.Background(), storage.DsCompositeIndex{Project: testProject, Kind: "CompositeKeys", Properties: properties, Source: "configured"}, true); err != nil {
					t.Fatal(err)
				}
				snapshot := timestamppb.New(s.grpc.store.ReadTime())
				upsertEntity(t, s, dsEntity(dsKey("CompositeKeys", "b"), map[string]*datastorepb.Value{"n": dsInt(2), "m": dsStr("match")}))
				if _, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL, Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{Delete: dsKey("CompositeKeys", "d")}}}}); err != nil {
					t.Fatal(err)
				}
				for _, historical := range []bool{false, true} {
					var readOptions *datastorepb.ReadOptions
					want := []string{"c"}
					if historical {
						readOptions = &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: snapshot}}
						want = []string{"b", "c", "d"}
					}
					query := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "CompositeKeys"}}, Filter: andFilter(filters...), Offset: 1,
						Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "n"}, Direction: datastorepb.PropertyOrder_DESCENDING}}}
					run := func(q *datastorepb.Query) *datastorepb.RunQueryResponse {
						t.Helper()
						response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ReadOptions: readOptions, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
						if err != nil {
							t.Fatal(err)
						}
						return response
					}
					full := run(query)
					query.Projection = []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "__key__"}}}
					query.Limit = wrapperspb.Int32(1)
					var got []string
					var firstCursor []byte
					for page := 0; page < 6; page++ {
						response := run(query)
						if reads := response.ExplainMetrics.ExecutionStats.DebugStats.Fields["documents_scanned"].GetStringValue(); reads != "0" {
							t.Fatalf("historical=%t read %s source entities", historical, reads)
						}
						for _, row := range response.Batch.EntityResults {
							if len(got) >= len(full.Batch.EntityResults) {
								t.Fatal("extra keys-only result")
							}
							original := proto.Clone(full.Batch.EntityResults[len(got)]).(*datastorepb.EntityResult)
							original.Entity.Properties, original.Cursor = nil, nil
							actual := proto.Clone(row).(*datastorepb.EntityResult)
							actual.Cursor = nil
							if !proto.Equal(actual, original) {
								t.Fatalf("keys/metadata differ: got %v want %v", actual, original)
							}
							got = append(got, row.Entity.Key.Path[0].GetName())
						}
						if page == 0 {
							firstCursor = response.Batch.EndCursor
						}
						if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
							break
						}
						query.StartCursor, query.Offset = response.Batch.EndCursor, 0
					}
					if !slices.Equal(got, want) {
						t.Fatalf("historical=%t keys=%v want=%v", historical, got, want)
					}
					query.StartCursor, query.EndCursor, query.Offset, query.Limit = nil, firstCursor, 0, nil
					bounded := run(query)
					// A returned cursor is after its row, so the end bound includes it.
					if rows := bounded.Batch.EntityResults; len(rows) != 2 || rows[0].Entity.Key.Path[0].GetName() != "a" || rows[1].Entity.Key.Path[0].GetName() != want[0] {
						t.Fatalf("end cursor results=%v", bounded.Batch)
					}
				}
			})
		}
	}
}

func TestReviewCompositeEqualityKeysEligibility(t *testing.T) {
	for _, tc := range []struct {
		name   string
		filter *datastorepb.Filter
		want   bool
	}{
		{"equalities", andFilter(propFilter("m", datastorepb.PropertyFilter_EQUAL, dsStr("match")), propFilter("n", datastorepb.PropertyFilter_EQUAL, dsInt(1))), true},
		{"bool and double", andFilter(propFilter("m", datastorepb.PropertyFilter_EQUAL, &datastorepb.Value{ValueType: &datastorepb.Value_BooleanValue{BooleanValue: true}}), propFilter("n", datastorepb.PropertyFilter_EQUAL, &datastorepb.Value{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: 1.5}})), true},
		{"partial prefix", propFilter("m", datastorepb.PropertyFilter_EQUAL, dsStr("match")), false},
		{"repeated property", andFilter(propFilter("m", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("m", datastorepb.PropertyFilter_EQUAL, dsInt(2)), propFilter("n", datastorepb.PropertyFilter_EQUAL, dsInt(1))), false},
		{"range", andFilter(propFilter("m", datastorepb.PropertyFilter_EQUAL, dsStr("match")), propFilter("n", datastorepb.PropertyFilter_GREATER_THAN, dsInt(1))), false},
		{"or", orFilter(propFilter("m", datastorepb.PropertyFilter_EQUAL, dsStr("match")), propFilter("n", datastorepb.PropertyFilter_EQUAL, dsInt(1))), false},
		{"in", andFilter(propFilter("m", datastorepb.PropertyFilter_EQUAL, dsStr("match")), propFilter("n", datastorepb.PropertyFilter_IN, dsArray(dsInt(1), dsInt(2)))), false},
		{"array operand", andFilter(propFilter("m", datastorepb.PropertyFilter_EQUAL, dsStr("match")), propFilter("n", datastorepb.PropertyFilter_EQUAL, dsArray(dsInt(1)))), false},
		{"entity operand", andFilter(propFilter("m", datastorepb.PropertyFilter_EQUAL, dsStr("match")), propFilter("n", datastorepb.PropertyFilter_EQUAL, &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{}}})), false},
		{"ancestor", andFilter(propFilter("m", datastorepb.PropertyFilter_EQUAL, dsStr("match")), propFilter("n", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("__key__", datastorepb.PropertyFilter_HAS_ANCESTOR, &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: dsKey("Parent", "p")}})), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := &datastorepb.Query{Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "__key__"}}}, Filter: tc.filter}
			idx := &storage.DsCompositeIndex{State: storage.DsIndexReady, Properties: []storage.DsIndexProperty{{Name: "m"}, {Name: "n"}}}
			if got := compositeEqualityKeysCovering(q, idx); got != tc.want {
				t.Fatalf("covering=%t want=%t", got, tc.want)
			}
			if !tc.want {
				return
			}
			for _, excluded := range []string{"unready", "ancestor index", "distinct", "extra order", "dotted", "full entity"} {
				t.Run(excluded, func(t *testing.T) {
					query := proto.Clone(q).(*datastorepb.Query)
					index := *idx
					index.Properties = slices.Clone(idx.Properties)
					switch excluded {
					case "unready":
						index.State = storage.DsIndexCreating
					case "ancestor index":
						index.Ancestor = true
					case "distinct":
						query.DistinctOn = []*datastorepb.PropertyReference{{Name: "m"}}
					case "extra order":
						query.Order = []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "__key__"}}}
					case "dotted":
						query.Filter.GetCompositeFilter().Filters[0].GetPropertyFilter().Property.Name = "a.b"
						index.Properties[0].Name = "a.b"
					case "full entity":
						query.Projection = nil
					}
					if compositeEqualityKeysCovering(query, &index) {
						t.Fatal("ineligible query used covering access")
					}
				})
			}
		})
	}
}

func TestReviewBoundedRangeAccess(t *testing.T) {
	s := newTestDsServer(t)
	for i := 0; i < 300; i++ {
		props := map[string]*datastorepb.Value{"x": dsInt(0), "y": dsInt(0), "rank": dsInt(int64(300 - i))}
		if i >= 296 {
			props["x"] = dsArray(dsInt(1), dsInt(2), dsInt(2))
			props["y"] = dsInt(2)
		}
		upsertEntity(t, s, dsEntity(dsKey("RangeAccess", strconv.Itoa(1000+i)), props))
	}
	for _, mode := range []string{"keys", "composite_keys", "or", "ordered_or"} {
		t.Run(mode, func(t *testing.T) {
			before, err := s.grpc.store.ListDsCompositeIndexes(testProject)
			if err != nil {
				t.Fatal(err)
			}
			q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "RangeAccess"}}, Filter: propFilter("x", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(1)), Limit: wrapperspb.Int32(1)}
			want := []string{"1296", "1297", "1298", "1299"}
			keysOnly := strings.Contains(mode, "keys")
			if keysOnly {
				q.Projection = []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "__key__"}}}
				if mode == "composite_keys" {
					q.Filter = andFilter(q.Filter, propFilter("y", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(1)))
				}
			} else {
				q.Filter = orFilter(q.Filter, propFilter("y", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(1)))
			}
			if mode == "ordered_or" {
				q.Order = []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "rank"}}}
				slices.Reverse(want)
			}
			var got []string
			for page := 0; page < 6; page++ {
				response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
				if err != nil {
					t.Fatal(err)
				}
				reads, err := strconv.Atoi(response.ExplainMetrics.ExecutionStats.DebugStats.Fields["documents_scanned"].GetStringValue())
				if err != nil || keysOnly && reads != 0 || !keysOnly && reads > 8 {
					t.Fatalf("source reads=%d err=%v", reads, err)
				}
				for _, row := range response.Batch.EntityResults {
					got = append(got, row.Entity.Key.Path[0].GetName())
				}
				if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
					break
				}
				q.StartCursor = response.Batch.EndCursor
			}
			if !slices.Equal(got, want) {
				t.Fatalf("keys=%v want=%v", got, want)
			}
			if mode == "ordered_or" {
				after, err := s.grpc.store.ListDsCompositeIndexes(testProject)
				if err != nil || len(after) != len(before) {
					t.Fatalf("candidate probe built an unused index: before=%d after=%d err=%v", len(before), len(after), err)
				}
			}
			// Exceed the probe allowance without imposing a result/work cutoff.
			dense := proto.Clone(q).(*datastorepb.Query)
			dense.StartCursor, dense.Limit = nil, wrapperspb.Int32(1000)
			dense.Filter = propFilter("x", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(0))
			if mode == "composite_keys" {
				dense.Filter = andFilter(dense.Filter, propFilter("y", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(0)))
			}
			if !keysOnly {
				dense.Filter = orFilter(dense.Filter, propFilter("y", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(0)))
			}
			response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: dense}})
			if err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			for _, row := range response.Batch.EntityResults {
				seen[row.Entity.Key.Path[0].GetName()] = true
			}
			if len(response.Batch.EntityResults) != 300 || len(seen) != 300 || response.Batch.MoreResults != datastorepb.QueryResultBatch_NO_MORE_RESULTS {
				t.Fatalf("incomplete probe truncated/duplicated dense query: %v", response.Batch)
			}
		})
	}
}

func TestReviewRangeKeysMatchFullEntities(t *testing.T) {
	s := newTestDsServer(t)
	for i := 0; i < 20; i++ {
		properties := map[string]*datastorepb.Value{"x": dsArray(dsInt(int64(i%5-2)), dsInt(int64(i%3))), "y": dsArray(dsInt(int64(i%4)), dsInt(2))}
		if i%7 == 0 {
			delete(properties, "x")
		}
		if i%9 == 0 {
			for _, value := range properties["y"].GetArrayValue().Values {
				value.ExcludeFromIndexes = true
			}
		}
		upsertEntity(t, s, dsEntity(dsKey("RangeKeysParity", strconv.Itoa(100+i)), properties))
	}
	for _, op := range []datastorepb.PropertyFilter_Operator{datastorepb.PropertyFilter_LESS_THAN, datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL, datastorepb.PropertyFilter_GREATER_THAN, datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL} {
		for _, composite := range []bool{false, true} {
			for _, direction := range []datastorepb.PropertyOrder_Direction{datastorepb.PropertyOrder_ASCENDING, datastorepb.PropertyOrder_DESCENDING} {
				q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "RangeKeysParity"}}, Filter: propFilter("x", op, dsInt(1)), Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}, Direction: direction}}}
				if composite {
					q.Filter = andFilter(q.Filter, propFilter("y", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(1)))
					q.Order = append(q.Order, &datastorepb.PropertyOrder{Property: &datastorepb.PropertyReference{Name: "y"}, Direction: direction})
				}
				var full []*datastorepb.EntityResult
				for _, keysOnly := range []bool{false, true} {
					if keysOnly {
						q.Projection = []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "__key__"}}}
					}
					response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
					if err != nil {
						t.Fatal(err)
					}
					if !keysOnly {
						full = response.Batch.EntityResults
						continue
					}
					if len(full) != len(response.Batch.EntityResults) {
						t.Fatalf("query=%v full=%d keys=%d", q, len(full), len(response.Batch.EntityResults))
					}
					for i, row := range response.Batch.EntityResults {
						full[i].Entity.Properties, full[i].Cursor, row.Cursor = nil, nil, nil
						if !proto.Equal(full[i], row) {
							t.Fatalf("query=%v full=%v keys=%v", q, full[i], row)
						}
					}
				}
			}
		}
	}
}

func TestReviewAdaptiveDenseOR(t *testing.T) {
	s := newTestDsServer(t)
	for i := 0; i < 12; i++ {
		upsertEntity(t, s, dsEntity(dsKey("DenseOR", strconv.Itoa(100+i)), map[string]*datastorepb.Value{"x": dsInt(1), "y": dsInt(1)}))
	}
	q := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "DenseOR"}}, Filter: orFilter(propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("y", datastorepb.PropertyFilter_EQUAL, dsInt(1)))}
	response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Batch.EntityResults) != 12 {
		t.Fatalf("results=%v", response.Batch)
	}
	stats := response.ExplainMetrics.ExecutionStats.DebugStats.Fields
	if stats["index_entries_scanned"].GetStringValue() != "12" || stats["documents_scanned"].GetStringValue() != "12" {
		t.Fatalf("dense query should scan each key once: %v", stats)
	}
}

func TestReviewStreamingEqualityOR(t *testing.T) {
	s := newTestDsServer(t)
	for i := range 200 {
		props := map[string]*datastorepb.Value{"x": dsInt(0), "y": dsInt(0)}
		if i >= 196 {
			props["x"] = dsArray(dsInt(1), dsInt(1), dsInt(2))
		}
		if i == 195 || i == 197 {
			props["y"] = dsInt(1)
		}
		if i == 193 {
			props["x"] = &datastorepb.Value{ExcludeFromIndexes: true, ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 1}}
		}
		if i == 194 {
			delete(props, "x")
		}
		upsertEntity(t, s, dsEntity(dsKey("StreamingOR", strconv.Itoa(1000+i)), props))
	}
	snapshot := timestamppb.New(s.grpc.store.ReadTime())
	upsertEntity(t, s, dsEntity(dsKey("StreamingOR", "1196"), map[string]*datastorepb.Value{"x": dsInt(0)}))
	if _, err := s.grpc.Commit(context.Background(), &datastorepb.CommitRequest{ProjectId: testProject, Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL, Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{Delete: dsKey("StreamingOR", "1198")}}}}); err != nil {
		t.Fatal(err)
	}
	for _, historical := range []bool{false, true} {
		var readOptions *datastorepb.ReadOptions
		want := []string{"1195", "1197", "1199"}
		if historical {
			readOptions = &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: snapshot}}
			want = []string{"1195", "1196", "1197", "1198", "1199"}
		}
		query := &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "StreamingOR"}}, Filter: orFilter(propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(1)), propFilter("y", datastorepb.PropertyFilter_EQUAL, dsInt(1)))}
		run := func(q *datastorepb.Query) *datastorepb.RunQueryResponse {
			t.Helper()
			response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ReadOptions: readOptions, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true}, QueryType: &datastorepb.RunQueryRequest_Query{Query: q}})
			if err != nil {
				t.Fatal(err)
			}
			return response
		}
		full := run(query)
		planned, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, ExplainOptions: &datastorepb.ExplainOptions{}, QueryType: &datastorepb.RunQueryRequest_Query{Query: query}})
		if err != nil || !proto.Equal(planned.GetExplainMetrics().GetPlanSummary(), full.ExplainMetrics.PlanSummary) || len(full.ExplainMetrics.PlanSummary.IndexesUsed) != 3 || full.ExplainMetrics.PlanSummary.IndexesUsed[0].Fields["access_path"].GetStringValue() != "builtin:union" {
			t.Fatalf("union Explain mismatch: plan=%v analyzed=%v err=%v", planned, full.ExplainMetrics, err)
		}
		if got := len(full.Batch.EntityResults); got != len(want) {
			t.Fatalf("full results=%d want=%d", got, len(want))
		}
		if reads := full.ExplainMetrics.ExecutionStats.DebugStats.Fields["documents_scanned"].GetStringValue(); reads != strconv.Itoa(len(want)+1) {
			t.Fatalf("full OR query read %s source entities for %d matches", reads, len(want))
		}
		duplicate := proto.Clone(query).(*datastorepb.Query)
		duplicate.Filter = orFilter(query.Filter, query.Filter)
		duplicateRows := run(duplicate).Batch.EntityResults
		if len(duplicateRows) != len(want) {
			t.Fatalf("duplicate branches returned %d rows", len(duplicateRows))
		}
		for i, row := range duplicateRows {
			if !proto.Equal(row.Entity, full.Batch.EntityResults[i].Entity) {
				t.Fatalf("duplicate branch row=%v want=%v", row.Entity, full.Batch.EntityResults[i].Entity)
			}
		}
		empty := proto.Clone(query).(*datastorepb.Query)
		empty.Filter = orFilter(propFilter("x", datastorepb.PropertyFilter_EQUAL, dsInt(99)), propFilter("y", datastorepb.PropertyFilter_EQUAL, dsInt(99)))
		if response := run(empty); len(response.Batch.EntityResults) != 0 || response.Batch.MoreResults != datastorepb.QueryResultBatch_NO_MORE_RESULTS || response.ExplainMetrics.ExecutionStats.DebugStats.Fields["documents_scanned"].GetStringValue() != "1" {
			t.Fatalf("empty OR response=%v", response)
		}
		for _, keysOnly := range []bool{false, true} {
			for _, pageSize := range []int32{1, 20} {
				q := proto.Clone(query).(*datastorepb.Query)
				q.Limit, q.Offset = wrapperspb.Int32(pageSize), 1
				if keysOnly {
					q.Projection = []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "__key__"}}}
				}
				var got []string
				var firstCursor []byte
				for page := 0; page < 10; page++ {
					response := run(q)
					stats := response.ExplainMetrics.ExecutionStats.DebugStats.Fields
					if keysOnly && stats["documents_scanned"].GetStringValue() != "0" {
						t.Fatalf("keys-only source reads=%v", stats["documents_scanned"])
					}
					entries, err := strconv.Atoi(stats["index_entries_scanned"].GetStringValue())
					if err != nil || entries > 12 {
						t.Fatalf("entries=%d err=%v", entries, err)
					}
					for _, row := range response.Batch.EntityResults {
						if len(got)+1 >= len(full.Batch.EntityResults) {
							t.Fatal("extra OR row")
						}
						original := proto.Clone(full.Batch.EntityResults[len(got)+1]).(*datastorepb.EntityResult)
						actual := proto.Clone(row).(*datastorepb.EntityResult)
						original.Cursor, actual.Cursor = nil, nil
						if keysOnly {
							original.Entity.Properties = nil
						}
						if !proto.Equal(actual, original) {
							t.Fatalf("row/metadata=%v want=%v", actual, original)
						}
						got = append(got, row.Entity.Key.Path[0].GetName())
					}
					if page == 0 {
						firstCursor = response.Batch.EndCursor
					}
					if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
						break
					}
					q.StartCursor, q.Offset = response.Batch.EndCursor, 0
				}
				if !slices.Equal(got, want[1:]) {
					t.Fatalf("keys=%v want=%v", got, want[1:])
				}
				invalid := proto.Clone(q).(*datastorepb.Query)
				decoded, ok := decodeCursorFull(firstCursor)
				if !ok {
					t.Fatal("invalid generated cursor")
				}
				decoded.K = append(slices.Clone(decoded.K), 'x')
				invalid.StartCursor = encodeCursorFull(decoded)
				if _, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: invalid}}); status.Code(err) != codes.InvalidArgument {
					t.Fatalf("malformed key cursor err=%v", err)
				}
				invalid.StartCursor = firstCursor
				if _, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, PartitionId: &datastorepb.PartitionId{NamespaceId: "other"}, QueryType: &datastorepb.RunQueryRequest_Query{Query: invalid}}); status.Code(err) != codes.InvalidArgument {
					t.Fatalf("cross-partition cursor err=%v", err)
				}
				q.StartCursor, q.EndCursor, q.Offset, q.Limit = nil, firstCursor, 0, nil
				bounded := run(q)
				if len(bounded.Batch.EntityResults) != min(int(pageSize)+1, len(want)) {
					t.Fatalf("end bound results=%v", bounded.Batch)
				}
			}
		}
	}
}

func TestReviewSelectiveOrderedORRanges(t *testing.T) {
	s := newTestDsServer(t)
	for i := range 1000 {
		upsertEntity(t, s, dsEntity(dsKey("SelectiveOR", "n"+strconv.Itoa(1000+i)), map[string]*datastorepb.Value{"x": dsInt(int64(i))}))
	}
	for i, name := range []string{"a", "b"} {
		upsertEntity(t, s, dsEntity(dsKey("SelectiveOR", name), map[string]*datastorepb.Value{"x": dsArray(dsInt(int64(-1-i)), dsInt(int64(1001+i)))}))
	}
	filter := orFilter(propFilter("x", datastorepb.PropertyFilter_LESS_THAN, dsInt(2)), propFilter("x", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(998)))
	for _, direction := range []datastorepb.PropertyOrder_Direction{datastorepb.PropertyOrder_ASCENDING, datastorepb.PropertyOrder_DESCENDING} {
		for _, pageSize := range []int32{1, 20} {
			want := []string{"b", "a", "n1000", "n1001", "n1998", "n1999"}
			if direction == datastorepb.PropertyOrder_DESCENDING {
				want = []string{"b", "a", "n1999", "n1998", "n1001", "n1000"}
			}
			var cursor []byte
			var got []string
			var scanned int64
			for page := 0; page < 10; page++ {
				response, err := s.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{
					ProjectId: testProject, ExplainOptions: &datastorepb.ExplainOptions{Analyze: true},
					QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
						Kind: []*datastorepb.KindExpression{{Name: "SelectiveOR"}}, Filter: filter,
						Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "x"}, Direction: direction}},
						Limit: wrapperspb.Int32(pageSize), StartCursor: cursor,
					}},
				})
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range response.Batch.EntityResults {
					got = append(got, row.Entity.Key.Path[0].GetName())
				}
				entries, err := strconv.ParseInt(response.ExplainMetrics.ExecutionStats.DebugStats.Fields["index_entries_scanned"].GetStringValue(), 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				scanned += entries
				if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
					break
				}
				cursor = response.Batch.EndCursor
			}
			if !slices.Equal(got, want) {
				t.Fatalf("direction=%v pageSize=%d keys=%v want=%v", direction, pageSize, got, want)
			}
			if scanned > 40 {
				t.Errorf("direction=%v pageSize=%d scanned %d entries for six matching entities", direction, pageSize, scanned)
			}
		}
	}
}

func TestReviewCompositeArrayPaginationDoesNotRepeatEntities(t *testing.T) {
	s := newTestDsServer(t)
	for _, name := range []string{"a", "b"} {
		upsertEntity(t, s, dsEntity(dsKey("ArrayCompositeReview", name), map[string]*datastorepb.Value{"group": dsInt(1), "items": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: []*datastorepb.Value{dsInt(1), dsInt(2), dsInt(3)}}}}}))
	}
	var cursor []byte
	var names []string
	for page := 0; page < 10; page++ {
		response := qKind(t, s, "ArrayCompositeReview", &datastorepb.Query{Filter: propFilter("group", datastorepb.PropertyFilter_EQUAL, dsInt(1)), Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "items"}, Direction: datastorepb.PropertyOrder_ASCENDING}}, Limit: wrapperspb.Int32(1), StartCursor: cursor})
		for _, row := range response.Batch.EntityResults {
			names = append(names, row.Entity.Key.Path[0].GetName())
		}
		if response.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
			break
		}
		cursor = response.Batch.EndCursor
	}
	if strings.Join(names, ",") != "a,b" {
		t.Fatalf("paginated entities = %v", names)
	}
}

func TestReviewExactNumericTransforms(t *testing.T) {
	for _, tc := range []struct{ current, increment, want int64 }{
		{1 << 53, 1, (1 << 53) + 1}, {math.MaxInt64, 1, math.MaxInt64}, {math.MinInt64, -1, math.MinInt64},
	} {
		entity := dsEntity(nil, map[string]*datastorepb.Value{"n": dsInt(tc.current)})
		got, _, err := applyPropertyTransforms(entity, []*datastorepb.PropertyTransform{{Property: "n", TransformType: &datastorepb.PropertyTransform_Increment{Increment: dsInt(tc.increment)}}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if value := got.Properties["n"].GetIntegerValue(); value != tc.want {
			t.Errorf("%d + %d = %d, want %d", tc.current, tc.increment, value, tc.want)
		}
	}
}

func TestReviewTransactionConflicts(t *testing.T) {
	for _, scenario := range []string{"inline", "repeat", "missing", "recreate", "phantom", "kind_metadata", "namespace_metadata"} {
		t.Run(scenario, func(t *testing.T) {
			s := newTestDsServer(t)
			ctx := context.Background()
			key := dsKey("TransactionReview", "one")
			if scenario != "missing" {
				upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"n": dsInt(1)}))
			}
			var id []byte
			var options *datastorepb.ReadOptions
			if scenario == "inline" {
				options = &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_NewTransaction{NewTransaction: &datastorepb.TransactionOptions{Mode: &datastorepb.TransactionOptions_ReadWrite_{ReadWrite: &datastorepb.TransactionOptions_ReadWrite{}}}}}
			} else {
				tx, err := s.grpc.BeginTransaction(ctx, &datastorepb.BeginTransactionRequest{ProjectId: testProject})
				if err != nil {
					t.Fatal(err)
				}
				id = tx.Transaction
				options = &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_Transaction{Transaction: id}}
			}
			first, err := s.grpc.Lookup(ctx, &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}, ReadOptions: options})
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "inline" {
				id = first.Transaction
			}
			if scenario == "phantom" {
				_, err = s.grpc.RunQuery(ctx, &datastorepb.RunQueryRequest{ProjectId: testProject, ReadOptions: options, QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "TransactionReview"}}}}})
				if err != nil {
					t.Fatal(err)
				}
				key = dsKey("TransactionReview", "new")
			}
			if scenario == "kind_metadata" || scenario == "namespace_metadata" {
				kind := "__kind__"
				if scenario == "namespace_metadata" {
					kind = "__namespace__"
				}
				_, err = s.grpc.RunQuery(ctx, &datastorepb.RunQueryRequest{ProjectId: testProject, ReadOptions: options, QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind}}}}})
				if err != nil {
					t.Fatal(err)
				}
				key = dsKey("NewKind", "new")
				if scenario == "namespace_metadata" {
					key.PartitionId.NamespaceId = "new-namespace"
				}
			}
			if scenario == "recreate" {
				_, err := s.grpc.Commit(ctx, &datastorepb.CommitRequest{ProjectId: testProject, Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{Delete: key}}}})
				if err != nil {
					t.Fatal(err)
				}
			}
			upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"n": dsInt(2)}))
			if scenario == "repeat" {
				second, err := s.grpc.Lookup(ctx, &datastorepb.LookupRequest{ProjectId: testProject, Keys: []*datastorepb.Key{key}, ReadOptions: options})
				if err != nil {
					t.Fatal(err)
				}
				if len(second.Found) != 1 || second.Found[0].Entity.Properties["n"].GetIntegerValue() != 1 {
					t.Errorf("non-repeatable read: %v", second)
				}
			}
			_, err = s.grpc.Commit(ctx, &datastorepb.CommitRequest{ProjectId: testProject, TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: id}, Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntity(dsKey("Result", "one"), nil)}}}})
			if status.Code(err) != codes.Aborted {
				t.Fatalf("commit = %v, want ABORTED", err)
			}
		})
	}
}

func TestReviewQueryBoundaries(t *testing.T) {
	s := newTestDsServer(t)
	const kind = "QueryReview"
	for _, id := range []int64{1, 2, 10, 11, 20} {
		upsertEntity(t, s, dsEntity(dsKeyID(kind, id), map[string]*datastorepb.Value{"n": dsInt(id), "large": dsInt((1 << 53) + id), "text": dsStr(strings.Repeat("x", 1500)), "obj": {ValueType: &datastorepb.Value_EntityValue{EntityValue: dsEntity(nil, map[string]*datastorepb.Value{"x": dsInt(id)})}}}))
	}
	t.Run("pagination", func(t *testing.T) {
		var ids []int64
		var cursor []byte
		for page := 0; page < 8; page++ {
			res := qKind(t, s, kind, &datastorepb.Query{Limit: wrapperspb.Int32(2), StartCursor: cursor})
			for _, row := range res.Batch.EntityResults {
				ids = append(ids, row.Entity.Key.Path[0].GetId())
			}
			if res.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
				break
			}
			cursor = res.Batch.EndCursor
		}
		if !slices.Equal(ids, []int64{1, 2, 10, 11, 20}) {
			t.Fatalf("ids = %v", ids)
		}
	})
	t.Run("offset", func(t *testing.T) {
		res := qKind(t, s, kind, &datastorepb.Query{Offset: 2, Limit: wrapperspb.Int32(2)})
		if len(res.Batch.EntityResults) != 2 || res.Batch.EntityResults[0].Entity.Key.Path[0].GetId() != 10 {
			t.Fatalf("batch = %v", res.Batch)
		}
	})
	for _, tc := range []struct {
		name, property string
		value          *datastorepb.Value
		count          int
	}{
		{"integer_precision", "large", dsInt((1 << 53) + 1), 1},
		{"nested", "obj.x", dsInt(1), 1},
		{"string_boundary", "text", dsStr(strings.Repeat("x", 1500)), 5},
		{"ancestor_self", "__key__", &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: dsKeyID(kind, 1)}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := datastorepb.PropertyFilter_EQUAL
			if tc.name == "ancestor_self" {
				op = datastorepb.PropertyFilter_HAS_ANCESTOR
			}
			res := qKind(t, s, kind, &datastorepb.Query{Filter: propFilter(tc.property, op, tc.value)})
			if len(res.Batch.EntityResults) != tc.count {
				t.Fatalf("got %d results, want %d", len(res.Batch.EntityResults), tc.count)
			}
		})
	}
}

func TestReviewKeyIdentity(t *testing.T) {
	s := newTestDsServer(t)
	keys := []*datastorepb.Key{dsKeyID("IdentityReview", 123), dsKey("IdentityReview", "123"), dsKey("IdentityReview", "a/Child/b"), dsAncestorKey("IdentityReview", "a", "Child", "b")}
	for i, key := range keys {
		upsertEntity(t, s, dsEntity(key, map[string]*datastorepb.Value{"n": dsInt(int64(i))}))
	}
	got, err := s.grpc.Lookup(context.Background(), &datastorepb.LookupRequest{ProjectId: testProject, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Found) != len(keys) {
		t.Fatalf("found %d keys, want %d", len(got.Found), len(keys))
	}
	for _, result := range got.Found {
		i := result.Entity.Properties["n"].GetIntegerValue()
		if i < 0 || i >= int64(len(keys)) || !proto.Equal(result.Entity.Key, keys[i]) {
			t.Fatalf("wrong entity: %v", result)
		}
	}
}

func TestReviewProjectionAndEndCursor(t *testing.T) {
	s := newTestDsServer(t)
	const kind = "ProjectionReview"
	for _, name := range []string{"a", "b", "c", "d"} {
		upsertEntity(t, s, dsEntity(dsKey(kind, name), map[string]*datastorepb.Value{"group": dsStr(name[:1]), "items": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: []*datastorepb.Value{dsInt(1), dsInt(2), dsInt(3)}}}}}))
	}
	t.Run("end_cursor", func(t *testing.T) {
		first := qKind(t, s, kind, &datastorepb.Query{Limit: wrapperspb.Int32(2)})
		bounded := qKind(t, s, kind, &datastorepb.Query{EndCursor: first.Batch.EndCursor})
		if len(bounded.Batch.EntityResults) != 2 {
			t.Fatalf("got %d results, want 2", len(bounded.Batch.EntityResults))
		}
	})
	t.Run("projection_pages", func(t *testing.T) {
		var cursor []byte
		var values []int64
		for page := 0; page < 16; page++ {
			result := qKind(t, s, kind, &datastorepb.Query{Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "items"}}}, Limit: wrapperspb.Int32(1), StartCursor: cursor})
			if len(result.Batch.EntityResults) > 1 {
				t.Fatalf("limit=1 returned %d results", len(result.Batch.EntityResults))
			}
			for _, row := range result.Batch.EntityResults {
				values = append(values, row.Entity.Properties["items"].GetIntegerValue())
			}
			if result.Batch.MoreResults == datastorepb.QueryResultBatch_NO_MORE_RESULTS {
				break
			}
			cursor = result.Batch.EndCursor
		}
		if len(values) != 12 {
			t.Fatalf("got %d projected values, want 12: %v", len(values), values)
		}
	})
}
