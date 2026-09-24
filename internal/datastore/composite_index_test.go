package datastore

import (
	"context"
	"errors"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func TestLiveQueryWaiterCancellationPreservesSharedBuild(t *testing.T) {
	s := newTestDsServer(t)
	names := []string{strings.Repeat("x", 1000), strings.Repeat("y", 1000)}
	properties := make(map[string]*datastorepb.Value)
	for _, name := range names {
		var values []*datastorepb.Value
		for i := range 100 {
			values = append(values, dsInt(int64(i)))
		}
		properties[name] = dsArray(values...)
	}
	// The same valid 10,000-entry, long-name shape as the storage streaming
	// regression makes a real build observable without sleeps or test-only hooks.
	seedKind(t, s, "LiveBuild", []seedRow{{"one", properties}})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	datastorepb.RegisterDatastoreServer(server, s.grpc)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	}()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Error(err)
		}
	})
	client := datastorepb.NewDatastoreClient(connection)
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	request := &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "LiveBuild"}}, Order: []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: names[0]}}, {Property: &datastorepb.PropertyReference{Name: names[1]}}}}}}
	request.ExplainOptions = &datastorepb.ExplainOptions{}
	if _, err := client.RunQuery(ctx, request); err != nil {
		t.Fatal(err)
	}
	if indexes, err := s.grpc.store.ListDsCompositeIndexes(testProject); err != nil || len(indexes) != 0 {
		t.Fatalf("explain started a build: indexes=%v err=%v", indexes, err)
	}
	request.ExplainOptions = nil
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := client.RunQuery(canceled, request); first <- err }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		indexes, err := s.grpc.store.ListDsCompositeIndexes(testProject)
		if err != nil {
			t.Fatal(err)
		}
		if len(indexes) == 1 && indexes[0].State == storage.DsIndexCreating {
			break
		}
		select {
		case err := <-first:
			t.Fatalf("query finished before a shared build was observed: %v", err)
		case <-ctx.Done():
			t.Fatal("shared build did not start")
		case <-ticker.C:
		}
	}
	type result struct {
		response *datastorepb.RunQueryResponse
		err      error
	}
	second := make(chan result, 1)
	go func() { response, err := client.RunQuery(ctx, request); second <- result{response, err} }()
	cancel()
	select {
	case err := <-first:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("first waiter error=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("first waiter did not cancel")
	}
	select {
	case got := <-second:
		if got.err != nil || len(got.response.GetBatch().GetEntityResults()) != 1 || got.response.Batch.EntityResults[0].Entity.Key.Path[0].GetName() != "one" {
			t.Fatalf("surviving waiter response=%v err=%v", got.response, got.err)
		}
	case <-ctx.Done():
		t.Fatal("surviving waiter did not finish")
	}
	if indexes, err := s.grpc.store.ListDsCompositeIndexes(testProject); err != nil || len(indexes) != 1 || indexes[0].State != storage.DsIndexReady {
		t.Fatalf("shared build was not published: indexes=%v err=%v", indexes, err)
	}
}

func TestIndexTemplateReloadReplacesFutureProjectTemplates(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manager := NewIndexManager(store)
	path := filepath.Join(t.TempDir(), "index.yaml")
	for _, kind := range []string{"Before", "After"} {
		data := "indexes:\n- kind: " + kind + "\n  properties:\n  - name: x\n  - name: y\n"
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if err := manager.LoadIndexFiles(context.Background(), path); err != nil {
			t.Fatal(err)
		}
	}
	if err := manager.ensureTemplates(context.Background(), testProject, true); err != nil {
		t.Fatal(err)
	}
	indexes, err := store.ListDsCompositeIndexes(testProject)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 || indexes[0].Kind != "After" {
		t.Fatalf("stale templates applied: %+v", indexes)
	}
}

type cancelAfterFirstCheck struct {
	checks atomic.Int32
}

func (c *cancelAfterFirstCheck) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *cancelAfterFirstCheck) Done() <-chan struct{}       { return nil }
func (c *cancelAfterFirstCheck) Value(any) any               { return nil }
func (c *cancelAfterFirstCheck) Err() error {
	if c.checks.Add(1) > 1 {
		return context.Canceled
	}
	return nil
}

func TestRunQueryBuildsAndUsesCompositeIndex(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := NewWithIndexManager(store, NewIndexManager(store))
	seedKind(t, server, "Invoice", []seedRow{
		{name: "old", props: map[string]*datastorepb.Value{"plan": dsStr("p1"), "state": dsStr("open"), "created_date": dsInt(1)}},
		{name: "new", props: map[string]*datastorepb.Value{"plan": dsStr("p1"), "state": dsStr("open"), "created_date": dsInt(2)}},
		{name: "other", props: map[string]*datastorepb.Value{"plan": dsStr("p2"), "state": dsStr("open"), "created_date": dsInt(3)}},
	})
	query := &datastorepb.Query{
		Kind:   []*datastorepb.KindExpression{{Name: "Invoice"}},
		Filter: andFilter(propFilter("plan", datastorepb.PropertyFilter_EQUAL, dsStr("p1")), propFilter("state", datastorepb.PropertyFilter_EQUAL, dsStr("open"))),
		Order:  []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "created_date"}, Direction: datastorepb.PropertyOrder_DESCENDING}},
	}
	first, err := server.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunQueryRequest_Query{Query: query},
	})
	if err != nil {
		t.Fatalf("first query: %v", err)
	}
	if got := first.Batch.EntityResults[0].Entity.Key.Path[0].GetName(); got != "new" {
		t.Fatalf("first result=%q, want new", got)
	}
	indexes, err := store.ListDsCompositeIndexes(testProject)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 || indexes[0].State != storage.DsIndexReady {
		t.Fatalf("indexes after first query=%+v, want one ready index", indexes)
	}
}

func TestRunQueryReusesCompatibleCompositeIndex(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := NewWithIndexManager(store, NewIndexManager(store))
	seedKind(t, server, "Invoice", []seedRow{
		{name: "old", props: map[string]*datastorepb.Value{"plan": dsStr("p1"), "state": dsStr("open"), "created_date": dsInt(1)}},
		{name: "new", props: map[string]*datastorepb.Value{"plan": dsStr("p1"), "state": dsStr("open"), "created_date": dsInt(2)}},
	})
	configured, _, err := store.EnsureDsCompositeIndex(context.Background(), storage.DsCompositeIndex{
		Project: testProject,
		Kind:    "Invoice",
		Properties: []storage.DsIndexProperty{
			{Name: "state"},
			{Name: "plan"},
			{Name: "created_date", Desc: true},
			{Name: "__key__", Desc: true}, // Normalized descending tie order.
		},
		Source: "configured",
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	query := &datastorepb.Query{
		Kind:   []*datastorepb.KindExpression{{Name: "Invoice"}},
		Filter: andFilter(propFilter("plan", datastorepb.PropertyFilter_EQUAL, dsStr("p1")), propFilter("state", datastorepb.PropertyFilter_EQUAL, dsStr("open"))),
		Order:  []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "created_date"}, Direction: datastorepb.PropertyOrder_DESCENDING}},
	}
	ctx, details := WithHTTPDetails(context.Background())
	response, err := server.grpc.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunQueryRequest_Query{Query: query},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := response.Batch.EntityResults[0].Entity.Key.Path[0].GetName(); got != "new" {
		t.Fatalf("first result=%q, want new", got)
	}
	if got := details.V["index_id"]; got != configured.ID {
		t.Fatalf("index_id=%v, want configured index %s", got, configured.ID)
	}
	indexes, err := store.ListDsCompositeIndexes(testProject)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 {
		t.Fatalf("indexes after query=%+v, want configured index only", indexes)
	}
}

func TestCompositeIndexSatisfiesQuery(t *testing.T) {
	required := storage.DsCompositeIndex{
		Kind: "Invoice",
		Properties: []storage.DsIndexProperty{
			{Name: "plan"},
			{Name: "state"},
			{Name: "created_date", Desc: true},
		},
	}
	filter := andFilter(
		propFilter("plan", datastorepb.PropertyFilter_EQUAL, dsStr("p1")),
		propFilter("state", datastorepb.PropertyFilter_EQUAL, dsStr("open")),
	)
	tests := []struct {
		name       string
		candidate  storage.DsCompositeIndex
		compatible bool
	}{
		{
			name: "permuted equality prefix",
			candidate: storage.DsCompositeIndex{Kind: "Invoice", Properties: []storage.DsIndexProperty{
				{Name: "state", Desc: true}, {Name: "plan"}, {Name: "created_date", Desc: true},
			}},
			compatible: true,
		},
		{
			name: "different ancestor scope",
			candidate: storage.DsCompositeIndex{Kind: "Invoice", Ancestor: true, Properties: []storage.DsIndexProperty{
				{Name: "plan"}, {Name: "state"}, {Name: "created_date", Desc: true},
			}},
		},
		{
			name: "different ordered direction",
			candidate: storage.DsCompositeIndex{Kind: "Invoice", Properties: []storage.DsIndexProperty{
				{Name: "plan"}, {Name: "state"}, {Name: "created_date"},
			}},
		},
		{
			name: "extra trailing property",
			candidate: storage.DsCompositeIndex{Kind: "Invoice", Properties: []storage.DsIndexProperty{
				{Name: "plan"}, {Name: "state"}, {Name: "created_date", Desc: true}, {Name: "total"},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := compositeIndexSatisfiesQuery(test.candidate, required, filter); got != test.compatible {
				t.Fatalf("compatible=%v, want %v", got, test.compatible)
			}
		})
	}
}

func TestCompositeIndexCursorPagination(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manager := NewIndexManager(store)
	server := NewWithIndexManager(store, manager)
	seedKind(t, server, "Invoice", []seedRow{
		{name: "old", props: map[string]*datastorepb.Value{"plan": dsStr("p1"), "state": dsStr("open"), "created_date": dsInt(1)}},
		{name: "new", props: map[string]*datastorepb.Value{"plan": dsStr("p1"), "state": dsStr("open"), "created_date": dsInt(2)}},
	})
	query := &datastorepb.Query{
		Kind:   []*datastorepb.KindExpression{{Name: "Invoice"}},
		Filter: andFilter(propFilter("plan", datastorepb.PropertyFilter_EQUAL, dsStr("p1")), propFilter("state", datastorepb.PropertyFilter_EQUAL, dsStr("open"))),
		Order:  []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "created_date"}, Direction: datastorepb.PropertyOrder_DESCENDING}},
		Limit:  wrapperspb.Int32(1),
	}
	definition, _ := queryIndexDefinition(query, false)
	definition.Project = testProject
	if _, _, err := store.EnsureDsCompositeIndex(context.Background(), definition, true); err != nil {
		t.Fatal(err)
	}

	first := runQuery(t, server, query)
	cursor, ok := decodeCursorFull(first.Batch.EndCursor)
	if !ok || cursor.V != 4 || cursor.I == "" || len(cursor.K) == 0 {
		t.Fatalf("cursor=%+v ok=%v", cursor, ok)
	}
	query.StartCursor = first.Batch.EndCursor
	second := runQuery(t, server, query)
	if got := second.Batch.EntityResults[0].Entity.Key.Path[0].GetName(); got != "old" {
		t.Fatalf("second page=%q", got)
	}

	query.StartCursor = []byte("VXNlclByb2ZpbGUvb2xk")
	_, err = server.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: query}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid cursor error=%v", err)
	}
}

func TestCompositeIndexCursorPinsCompatibleIndex(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := NewWithIndexManager(store, NewIndexManager(store))
	seedKind(t, server, "Invoice", []seedRow{
		{name: "old", props: map[string]*datastorepb.Value{"plan": dsStr("p1"), "state": dsStr("open"), "created_date": dsInt(1)}},
		{name: "new", props: map[string]*datastorepb.Value{"plan": dsStr("p1"), "state": dsStr("open"), "created_date": dsInt(2)}},
	})
	configured, _, err := store.EnsureDsCompositeIndex(context.Background(), storage.DsCompositeIndex{
		Project: testProject,
		Kind:    "Invoice",
		Properties: []storage.DsIndexProperty{
			{Name: "state"},
			{Name: "plan"},
			{Name: "created_date", Desc: true},
			{Name: "__key__", Desc: true}, // Normalized descending tie order.
		},
		Source: "configured",
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	query := &datastorepb.Query{
		Kind:   []*datastorepb.KindExpression{{Name: "Invoice"}},
		Filter: andFilter(propFilter("plan", datastorepb.PropertyFilter_EQUAL, dsStr("p1")), propFilter("state", datastorepb.PropertyFilter_EQUAL, dsStr("open"))),
		Order:  []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "created_date"}, Direction: datastorepb.PropertyOrder_DESCENDING}},
		Limit:  wrapperspb.Int32(1),
	}
	run := func() *datastorepb.RunQueryResponse {
		response, err := server.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{
			ProjectId: testProject,
			QueryType: &datastorepb.RunQueryRequest_Query{Query: query},
		})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	first := run()
	definition, _ := queryIndexDefinition(query, false)
	definition.Project = testProject
	if _, _, err := store.EnsureDsCompositeIndex(context.Background(), definition, true); err != nil {
		t.Fatal(err)
	}
	query.StartCursor = first.Batch.EndCursor
	second := run()
	if got := second.Batch.EntityResults[0].Entity.Key.Path[0].GetName(); got != "old" {
		t.Fatalf("second page=%q, want old", got)
	}
	cursor, ok := decodeCursorFull(second.Batch.EndCursor)
	if !ok || cursor.I != configured.ID {
		t.Fatalf("second cursor=%+v ok=%v, want index %s", cursor, ok, configured.ID)
	}
}

func TestCompositeIndexRangePageBoundsScan(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := NewWithIndexManager(store, NewIndexManager(store))
	rows := make([]seedRow, 100)
	for i := range rows {
		rows[i] = seedRow{
			name: strconv.Itoa(i),
			props: map[string]*datastorepb.Value{
				"plan_id":      dsStr("380011"),
				"applied":      dsBool(true),
				"created_date": dsInt(int64(i)),
			},
		}
	}
	seedKind(t, server, "PlanBalanceChange", rows)
	query := &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "PlanBalanceChange"}},
		Filter: andFilter(
			propFilter("plan_id", datastorepb.PropertyFilter_EQUAL, dsStr("380011")),
			propFilter("applied", datastorepb.PropertyFilter_EQUAL, dsBool(true)),
			propFilter("created_date", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(40)),
			propFilter("created_date", datastorepb.PropertyFilter_LESS_THAN, dsInt(60)),
		),
		Order: []*datastorepb.PropertyOrder{{
			Property:  &datastorepb.PropertyReference{Name: "created_date"},
			Direction: datastorepb.PropertyOrder_DESCENDING,
		}},
		Limit: wrapperspb.Int32(2),
	}
	definition, _ := queryIndexDefinition(query, false)
	definition.Project = testProject
	if _, _, err := store.EnsureDsCompositeIndex(context.Background(), definition, true); err != nil {
		t.Fatal(err)
	}

	resp, err := server.grpc.RunQuery(context.Background(), &datastorepb.RunQueryRequest{
		ProjectId:      testProject,
		QueryType:      &datastorepb.RunQueryRequest_Query{Query: query},
		ExplainOptions: &datastorepb.ExplainOptions{Analyze: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Batch.EntityResults[0].Entity.Properties["created_date"].GetIntegerValue(); got != 59 {
		t.Fatalf("first created_date=%d, want 59", got)
	}
	if got := resp.Batch.EntityResults[1].Entity.Properties["created_date"].GetIntegerValue(); got != 58 {
		t.Fatalf("second created_date=%d, want 58", got)
	}
	if got := resp.Batch.MoreResults; got != datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT {
		t.Fatalf("more_results=%s, want MORE_RESULTS_AFTER_LIMIT", got)
	}
	scanned, err := strconv.ParseInt(resp.ExplainMetrics.ExecutionStats.DebugStats.Fields["index_entries_scanned"].GetStringValue(), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	if scanned != 3 {
		t.Fatalf("index_entries_scanned=%d, want 3", scanned)
	}
}

func TestRunQueryUsesBoundedBuiltInIndex(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := NewWithIndexManager(store, NewIndexManager(store))
	rows := make([]seedRow, 100)
	for i := range rows {
		color := "blue"
		if i == 99 {
			color = "red"
		}
		rows[i] = seedRow{name: strconv.Itoa(i), props: map[string]*datastorepb.Value{"color": dsStr(color)}}
	}
	seedKind(t, server, "Widget", rows)
	query := &datastorepb.Query{
		Kind:   []*datastorepb.KindExpression{{Name: "Widget"}},
		Filter: propFilter("color", datastorepb.PropertyFilter_EQUAL, dsStr("red")),
		Limit:  wrapperspb.Int32(1),
	}
	ctx, details := WithHTTPDetails(context.Background())
	resp, err := server.grpc.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId: testProject,
		QueryType: &datastorepb.RunQueryRequest_Query{Query: query},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Batch.EntityResults[0].Entity.Key.Path[0].GetName(); got != "99" {
		t.Fatalf("result=%q, want 99", got)
	}
	if got := details.V["access_path"]; got != "builtin:color" {
		t.Fatalf("access_path=%v, want builtin:color", got)
	}
	if got := details.V["index_entries_scanned"]; got != int64(1) {
		t.Fatalf("index_entries_scanned=%v, want 1", got)
	}
}

func TestRunQueryUsesBuiltInIndexesWithoutBuildingComposite(t *testing.T) {
	tests := []struct {
		name       string
		query      *datastorepb.Query
		accessPath string
	}{
		{
			name: "keys only",
			query: &datastorepb.Query{Projection: []*datastorepb.Projection{{
				Property: &datastorepb.PropertyReference{Name: "__key__"},
			}}},
			accessPath: "builtin:__key__",
		},
		{
			name: "single property projection",
			query: &datastorepb.Query{Projection: []*datastorepb.Projection{{
				Property: &datastorepb.PropertyReference{Name: "color"},
			}}},
			accessPath: "builtin:color",
		},
		{
			name: "single property distinct",
			query: &datastorepb.Query{
				Projection: []*datastorepb.Projection{{Property: &datastorepb.PropertyReference{Name: "color"}}},
				DistinctOn: []*datastorepb.PropertyReference{{Name: "color"}},
			},
			accessPath: "builtin:color",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := storage.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			server := NewWithIndexManager(store, NewIndexManager(store))
			seedKind(t, server, "Widget", []seedRow{
				{name: "one", props: map[string]*datastorepb.Value{"color": dsStr("red")}},
				{name: "two", props: map[string]*datastorepb.Value{"color": dsStr("blue")}},
			})
			test.query.Kind = []*datastorepb.KindExpression{{Name: "Widget"}}
			ctx, details := WithHTTPDetails(context.Background())
			if _, err := server.grpc.RunQuery(ctx, &datastorepb.RunQueryRequest{
				ProjectId: testProject,
				QueryType: &datastorepb.RunQueryRequest_Query{Query: test.query},
			}); err != nil {
				t.Fatal(err)
			}
			if got := details.V["access_path"]; got != test.accessPath {
				t.Fatalf("access_path=%v, want %s", got, test.accessPath)
			}
			indexes, err := store.ListDsCompositeIndexes(testProject)
			if err != nil {
				t.Fatal(err)
			}
			if len(indexes) != 0 {
				t.Fatalf("composite indexes=%+v, want none", indexes)
			}
		})
	}
}

func TestCompositeIndexCountAvoidsEntityScanAndHonorsCancellation(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := NewWithIndexManager(store, NewIndexManager(store))
	seedKind(t, server, "PlanBalanceChange", []seedRow{
		{name: "two-dates", props: map[string]*datastorepb.Value{"plan_id": dsStr("380011"), "applied": dsBool(true), "created_date": dsArray(dsInt(2), dsInt(3))}},
		{name: "one-date", props: map[string]*datastorepb.Value{"plan_id": dsStr("380011"), "applied": dsBool(true), "created_date": dsInt(2)}},
		{name: "outside-range", props: map[string]*datastorepb.Value{"plan_id": dsStr("380011"), "applied": dsBool(true), "created_date": dsInt(4)}},
		{name: "other-plan", props: map[string]*datastorepb.Value{"plan_id": dsStr("other"), "applied": dsBool(true), "created_date": dsInt(2)}},
	})
	query := &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "PlanBalanceChange"}},
		Filter: andFilter(
			propFilter("plan_id", datastorepb.PropertyFilter_EQUAL, dsStr("380011")),
			propFilter("applied", datastorepb.PropertyFilter_EQUAL, dsBool(true)),
			propFilter("created_date", datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, dsInt(2)),
			propFilter("created_date", datastorepb.PropertyFilter_LESS_THAN, dsInt(4)),
		),
		Order:  []*datastorepb.PropertyOrder{{Property: &datastorepb.PropertyReference{Name: "created_date"}, Direction: datastorepb.PropertyOrder_DESCENDING}},
		Limit:  wrapperspb.Int32(0),
		Offset: math.MaxInt32,
	}
	definition, _ := queryIndexDefinition(query, false)
	definition.Project = testProject
	if _, _, err := store.EnsureDsCompositeIndex(context.Background(), definition, true); err != nil {
		t.Fatal(err)
	}
	req := &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: query}}

	resp, err := server.grpc.RunQuery(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Batch.SkippedResults; got != 2 {
		t.Fatalf("skipped_results=%d, want 2", got)
	}

	_, err = server.grpc.RunQuery(&cancelAfterFirstCheck{}, req)
	if !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
		t.Fatalf("canceled query error=%v, want cancellation", err)
	}
}

func TestGenericCountFallbackStreamsAndHonorsCancellation(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := NewWithIndexManager(store, NewIndexManager(store))
	seedKind(t, server, "PlanBalanceChange", []seedRow{
		{name: "first", props: map[string]*datastorepb.Value{"plan_id": dsStr("380011"), "created_date": dsInt(1)}},
		{name: "second", props: map[string]*datastorepb.Value{"plan_id": dsStr("380011"), "created_date": dsInt(2)}},
		{name: "other", props: map[string]*datastorepb.Value{"plan_id": dsStr("other"), "created_date": dsInt(3)}},
	})
	query := &datastorepb.Query{
		Kind:   []*datastorepb.KindExpression{{Name: "PlanBalanceChange"}},
		Filter: propFilter("plan_id", datastorepb.PropertyFilter_EQUAL, dsStr("380011")),
		Limit:  wrapperspb.Int32(0),
		Offset: math.MaxInt32,
	}
	req := &datastorepb.RunQueryRequest{ProjectId: testProject, QueryType: &datastorepb.RunQueryRequest_Query{Query: query}}

	resp, err := server.grpc.RunQuery(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Batch.SkippedResults; got != 2 {
		t.Fatalf("skipped_results=%d, want 2", got)
	}

	_, err = server.grpc.RunQuery(&cancelAfterFirstCheck{}, req)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled scan error=%v, want context.Canceled", err)
	}
}
