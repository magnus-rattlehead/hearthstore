//go:build integration

package tests

// Advanced integration tests derived from real production query patterns.
// These cover: cursor pagination, batch get/write, compound filters, cross-collection
// transactions, reference fields, timestamp ordering, and NOT_EQUAL/NOT_IN edge cases.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// -- cursor-based pagination ------------------------------------------------
//
// Firespotter pattern: run a query, capture the last snapshot, use it as the
// start cursor for the next page. This exercises the cursor SQL translation
// end-to-end through the real gRPC transport.

func TestAdvanced_CursorPagination_BySnapshot(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("articles")
	for i := 1; i <= 9; i++ {
		col.Doc(fmt.Sprintf("a%02d", i)).Set(ctx, map[string]any{"rank": i})
	}

	q := col.OrderBy("rank", firestore.Asc)

	// Page 1: first 3.
	page1, err := q.Limit(3).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1) != 3 {
		t.Fatalf("page1 len = %d, want 3", len(page1))
	}

	// Page 2: start after the last snapshot from page 1 (rank=3).
	page2, err := q.StartAfter(page1[len(page1)-1]).Limit(3).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2) != 3 {
		t.Fatalf("page2 len = %d, want 3", len(page2))
	}
	if r, _ := page2[0].DataAt("rank"); r != int64(4) {
		t.Errorf("page2 first rank = %v, want 4", r)
	}

	// Page 3: start after last snapshot from page 2 (rank=6).
	page3, err := q.StartAfter(page2[len(page2)-1]).Limit(3).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("page3: %v", err)
	}
	if len(page3) != 3 {
		t.Fatalf("page3 len = %d, want 3", len(page3))
	}
	if r, _ := page3[0].DataAt("rank"); r != int64(7) {
		t.Errorf("page3 first rank = %v, want 7", r)
	}

	// Page 4: nothing left.
	page4, err := q.StartAfter(page3[len(page3)-1]).Limit(3).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("page4: %v", err)
	}
	if len(page4) != 0 {
		t.Errorf("page4 should be empty, got %d", len(page4))
	}
}

// TestAdvanced_CursorPagination_CoversFull verifies that iterating through all
// pages with cursor pagination returns every document exactly once (no gaps,
// no duplicates).
func TestAdvanced_CursorPagination_CoversFull(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	const total = 20
	const pageSize = 6

	col := client.Collection("items")
	for i := 1; i <= total; i++ {
		col.Doc(fmt.Sprintf("d%03d", i)).Set(ctx, map[string]any{"n": i})
	}

	q := col.OrderBy("n", firestore.Asc)
	seen := make(map[int64]bool)
	var lastSnap *firestore.DocumentSnapshot

	for {
		var page []*firestore.DocumentSnapshot
		var err error
		if lastSnap == nil {
			page, err = q.Limit(pageSize).Documents(ctx).GetAll()
		} else {
			page, err = q.StartAfter(lastSnap).Limit(pageSize).Documents(ctx).GetAll()
		}
		if err != nil {
			t.Fatalf("page fetch: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, snap := range page {
			n, _ := snap.DataAt("n")
			nv := n.(int64)
			if seen[nv] {
				t.Errorf("duplicate document n=%d in paginated results", nv)
			}
			seen[nv] = true
		}
		lastSnap = page[len(page)-1]
	}

	if len(seen) != total {
		t.Errorf("paginated results covered %d docs, want %d", len(seen), total)
	}
}

// TestAdvanced_EndBefore_Range exercises a bounded range query using both
// StartAt and EndBefore - the cursor range pattern from firespotter.
func TestAdvanced_EndBefore_Range(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("nodes")
	for i := 1; i <= 10; i++ {
		col.Doc(fmt.Sprintf("n%02d", i)).Set(ctx, map[string]any{"v": i})
	}

	q := col.OrderBy("v", firestore.Asc)
	all, _ := q.Documents(ctx).GetAll()

	// Range: v in [3, 7) - StartAt snap for v=3, EndBefore snap for v=7.
	var start, end *firestore.DocumentSnapshot
	for _, s := range all {
		v, _ := s.DataAt("v")
		if v == int64(3) {
			start = s
		}
		if v == int64(7) {
			end = s
		}
	}

	ranged, err := q.StartAt(start).EndBefore(end).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("range query: %v", err)
	}
	// Expect v=3,4,5,6
	if len(ranged) != 4 {
		t.Fatalf("range [3,7): want 4 docs, got %d", len(ranged))
	}
	if v, _ := ranged[0].DataAt("v"); v != int64(3) {
		t.Errorf("first = %v, want 3", v)
	}
	if v, _ := ranged[len(ranged)-1].DataAt("v"); v != int64(6) {
		t.Errorf("last = %v, want 6", v)
	}
}

// -- batch get -------------------------------------------------------------
//
// Firespotter uses batch gets ("get [key1, key2, key3]") returning results
// in the same order with nil for missing keys.

func TestAdvanced_BatchGet(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("products")
	col.Doc("a").Set(ctx, map[string]any{"price": 10})
	col.Doc("b").Set(ctx, map[string]any{"price": 20})
	col.Doc("c").Set(ctx, map[string]any{"price": 30})

	refs := []*firestore.DocumentRef{
		col.Doc("a"),
		col.Doc("missing"), // does not exist
		col.Doc("c"),
	}
	snaps, err := client.GetAll(ctx, refs)
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(snaps) != 3 {
		t.Fatalf("GetAll: want 3 results, got %d", len(snaps))
	}

	// First result: a (exists).
	if p, _ := snaps[0].DataAt("price"); p != int64(10) {
		t.Errorf("a.price = %v, want 10", p)
	}
	// Second result: missing - snapshot exists but DataAt should fail or data be empty.
	if snaps[1].Exists() {
		t.Error("missing doc should not exist")
	}
	// Third result: c (exists).
	if p, _ := snaps[2].DataAt("price"); p != int64(30) {
		t.Errorf("c.price = %v, want 30", p)
	}
}

// -- batch write ------------------------------------------------------------
//
// Firespotter's mutation batches: multiple creates, updates, and deletes in
// one atomic commit.

func TestAdvanced_WriteBatch_MultipleOps(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("widgets")

	// Seed one doc that will be updated and one that will be deleted.
	col.Doc("update-me").Set(ctx, map[string]any{"v": 1})
	col.Doc("delete-me").Set(ctx, map[string]any{"v": 99})

	batch := client.Batch()
	batch.Set(col.Doc("new-doc"), map[string]any{"v": 42})
	batch.Update(col.Doc("update-me"), []firestore.Update{{Path: "v", Value: 2}})
	batch.Delete(col.Doc("delete-me"))

	if _, err := batch.Commit(ctx); err != nil {
		t.Fatalf("batch commit: %v", err)
	}

	// new-doc exists with v=42.
	snap, err := col.Doc("new-doc").Get(ctx)
	if err != nil {
		t.Fatalf("new-doc get: %v", err)
	}
	if v, _ := snap.DataAt("v"); v != int64(42) {
		t.Errorf("new-doc.v = %v, want 42", v)
	}

	// update-me has v=2.
	snap, err = col.Doc("update-me").Get(ctx)
	if err != nil {
		t.Fatalf("update-me get: %v", err)
	}
	if v, _ := snap.DataAt("v"); v != int64(2) {
		t.Errorf("update-me.v = %v, want 2", v)
	}

	// delete-me is gone.
	if _, err := col.Doc("delete-me").Get(ctx); status.Code(err) != codes.NotFound {
		t.Errorf("delete-me: want NotFound, got %v", err)
	}
}

// -- compound queries -------------------------------------------------------
//
// Firespotter uses multiple .where() chains (AND), which is common in
// production service queries (e.g. filter by service_ref AND active AND date range).

func TestAdvanced_CompoundQuery_ThreeClauses(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("links")
	for _, doc := range []map[string]any{
		{"service": "svc-a", "active": true, "clicks": int64(100)},
		{"service": "svc-a", "active": true, "clicks": int64(5)},
		{"service": "svc-a", "active": false, "clicks": int64(200)},
		{"service": "svc-b", "active": true, "clicks": int64(150)},
	} {
		col.NewDoc().Set(ctx, doc)
	}

	docs, err := col.
		Where("service", "==", "svc-a").
		Where("active", "==", true).
		Where("clicks", ">=", int64(50)).
		Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("compound query: %v", err)
	}
	// Only first doc matches all three: service=svc-a, active=true, clicks>=50.
	if len(docs) != 1 {
		t.Errorf("compound AND: want 1, got %d", len(docs))
	}
	if len(docs) == 1 {
		if c, _ := docs[0].DataAt("clicks"); c != int64(100) {
			t.Errorf("clicks = %v, want 100", c)
		}
	}
}

func TestAdvanced_CompoundQuery_NotIn_ExcludesMissingField(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("entries")
	col.Doc("red").Set(ctx, map[string]any{"color": "red"})
	col.Doc("blue").Set(ctx, map[string]any{"color": "blue"})
	col.Doc("green").Set(ctx, map[string]any{"color": "green"})
	col.Doc("no-color").Set(ctx, map[string]any{"other": "x"}) // field absent

	docs, err := col.Where("color", "not-in", []string{"red", "blue"}).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("not-in query: %v", err)
	}
	// Firestore: NOT_IN excludes documents where the field is absent.
	// Only "green" should match.
	if len(docs) != 1 {
		t.Errorf("not-in [red,blue]: want 1 (green only, no-color excluded), got %d", len(docs))
	}
}

func TestAdvanced_CompoundQuery_NotEqual_ExcludesMissingField(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("entries")
	col.Doc("x").Set(ctx, map[string]any{"status": "active"})
	col.Doc("y").Set(ctx, map[string]any{"status": "inactive"})
	col.Doc("z").Set(ctx, map[string]any{"other": "v"}) // no status field

	docs, err := col.Where("status", "!=", "active").Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("not-equal query: %v", err)
	}
	// Firestore: != excludes documents where the field is absent.
	if len(docs) != 1 {
		t.Errorf("status != active: want 1 (inactive only), got %d", len(docs))
	}
	if len(docs) == 1 {
		if s, _ := docs[0].DataAt("status"); s != "inactive" {
			t.Errorf("status = %v, want inactive", s)
		}
	}
}

// -- cross-collection transaction (XG-style) --------------------------------
//
// Firespotter's XG transactions span multiple entity groups / collections.
// This is the critical scenario for payment/balance operations.

func TestAdvanced_Transaction_CrossCollection(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	accounts := client.Collection("accounts")
	ledger := client.Collection("ledger")

	accounts.Doc("alice").Set(ctx, map[string]any{"balance": int64(500)})
	accounts.Doc("bob").Set(ctx, map[string]any{"balance": int64(200)})

	const transfer = int64(100)

	err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		alice, err := tx.Get(accounts.Doc("alice"))
		if err != nil {
			return err
		}
		bob, err := tx.Get(accounts.Doc("bob"))
		if err != nil {
			return err
		}

		ab, _ := alice.DataAt("balance")
		bb, _ := bob.DataAt("balance")

		if err := tx.Update(accounts.Doc("alice"), []firestore.Update{
			{Path: "balance", Value: ab.(int64) - transfer},
		}); err != nil {
			return err
		}
		if err := tx.Update(accounts.Doc("bob"), []firestore.Update{
			{Path: "balance", Value: bb.(int64) + transfer},
		}); err != nil {
			return err
		}
		// Write to a third collection in the same transaction.
		return tx.Set(ledger.NewDoc(), map[string]any{
			"from": "alice", "to": "bob", "amount": transfer,
		})
	})
	if err != nil {
		t.Fatalf("cross-collection transaction: %v", err)
	}

	a, _ := accounts.Doc("alice").Get(ctx)
	b, _ := accounts.Doc("bob").Get(ctx)
	abal, _ := a.DataAt("balance")
	bbal, _ := b.DataAt("balance")
	if abal != int64(400) {
		t.Errorf("alice balance = %v, want 400", abal)
	}
	if bbal != int64(300) {
		t.Errorf("bob balance = %v, want 300", bbal)
	}

	// Ledger should have exactly one entry.
	entries, err := ledger.Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("ledger query: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("ledger entries = %d, want 1", len(entries))
	}
}

// TestAdvanced_Transaction_IncrementCounter verifies read-then-write within a
// transaction commits the correct incremented value.
func TestAdvanced_Transaction_IncrementCounter(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	ref := client.Collection("counters").Doc("c1")
	ref.Set(ctx, map[string]any{"n": int64(10)})

	err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		snap, err := tx.Get(ref)
		if err != nil {
			return err
		}
		n, _ := snap.DataAt("n")
		return tx.Update(ref, []firestore.Update{{Path: "n", Value: n.(int64) + 1}})
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}
	snap, _ := ref.Get(ctx)
	if n, _ := snap.DataAt("n"); n != int64(11) {
		t.Errorf("n = %v, want 11", n)
	}
}

// -- timestamp ordering -----------------------------------------------------
//
// Firespotter stores created_at / modified_at as UTC timestamps and queries
// by them. This exercises timestamp encoding and ordering through field_index.

func TestAdvanced_TimestampOrdering(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("events")

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ids := []string{"e1", "e2", "e3", "e4"}
	for i, id := range ids {
		col.Doc(id).Set(ctx, map[string]any{
			"name":       id,
			"created_at": base.Add(time.Duration(i) * time.Hour),
		})
	}

	docs, err := col.OrderBy("created_at", firestore.Desc).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("timestamp query: %v", err)
	}
	if len(docs) != 4 {
		t.Fatalf("want 4 docs, got %d", len(docs))
	}
	// Descending: e4, e3, e2, e1.
	if n, _ := docs[0].DataAt("name"); n != "e4" {
		t.Errorf("first doc = %v, want e4", n)
	}
	if n, _ := docs[len(docs)-1].DataAt("name"); n != "e1" {
		t.Errorf("last doc = %v, want e1", n)
	}
}

func TestAdvanced_TimestampFilter_Range(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("logs")
	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		col.Doc(fmt.Sprintf("log%d", i)).Set(ctx, map[string]any{
			"ts": base.Add(time.Duration(i) * 24 * time.Hour),
			"i":  i,
		})
	}

	// Filter: ts in [day1, day3).
	day1 := base.Add(24 * time.Hour)
	day3 := base.Add(3 * 24 * time.Hour)
	docs, err := col.
		Where("ts", ">=", day1).
		Where("ts", "<", day3).
		OrderBy("ts", firestore.Asc).
		Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("timestamp range: %v", err)
	}
	// Should return log1 (day1) and log2 (day2).
	if len(docs) != 2 {
		t.Fatalf("timestamp range: want 2, got %d", len(docs))
	}
	if n, _ := docs[0].DataAt("i"); n != int64(1) {
		t.Errorf("first i = %v, want 1", n)
	}
}

// -- reference fields -------------------------------------------------------
//
// Firespotter stores DocumentReferences (service_ref, service_acc_ref) and
// filters by them. This exercises the value_ref column in field_index.

func TestAdvanced_ReferenceField_Filter(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	services := client.Collection("services")
	links := client.Collection("links")

	// Create two services.
	svcA := services.Doc("svc-a")
	svcB := services.Doc("svc-b")
	svcA.Set(ctx, map[string]any{"name": "Service A"})
	svcB.Set(ctx, map[string]any{"name": "Service B"})

	// Create links, each referencing a service.
	links.Doc("l1").Set(ctx, map[string]any{"url": "https://a.com/1", "service": svcA})
	links.Doc("l2").Set(ctx, map[string]any{"url": "https://a.com/2", "service": svcA})
	links.Doc("l3").Set(ctx, map[string]any{"url": "https://b.com/1", "service": svcB})

	// Query links for service A.
	docs, err := links.Where("service", "==", svcA).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("reference filter: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("links for svc-a: want 2, got %d", len(docs))
	}
}

// -- collection group (allDescendants) with filter -------------------------
//
// Firespotter queries across entity groups. In Firestore this maps to
// collection group queries.

func TestAdvanced_CollectionGroup_WithFilter(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	// Create "comments" as a subcollection under different posts.
	for _, postID := range []string{"post1", "post2", "post3"} {
		sub := client.Collection("posts").Doc(postID).Collection("comments")
		sub.Doc("c1").Set(ctx, map[string]any{"text": "great", "approved": true})
		sub.Doc("c2").Set(ctx, map[string]any{"text": "spam", "approved": false})
	}

	// Collection group query: all approved comments across all posts.
	docs, err := client.CollectionGroup("comments").
		Where("approved", "==", true).
		Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("collection group: %v", err)
	}
	// 3 posts * 1 approved comment each = 3.
	if len(docs) != 3 {
		t.Errorf("approved comments: want 3, got %d", len(docs))
	}
}

// -- array-contains-any -----------------------------------------------------

func TestAdvanced_ArrayContainsAny(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("services")
	col.Doc("s1").Set(ctx, map[string]any{"domains": []string{"example.com", "www.example.com"}})
	col.Doc("s2").Set(ctx, map[string]any{"domains": []string{"other.io"}})
	col.Doc("s3").Set(ctx, map[string]any{"domains": []string{"foo.net", "bar.net"}})

	// Find services that host either "example.com" or "bar.net".
	docs, err := col.
		Where("domains", "array-contains-any", []string{"example.com", "bar.net"}).
		Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("array-contains-any: %v", err)
	}
	if len(docs) != 2 {
		t.Errorf("array-contains-any: want 2, got %d", len(docs))
	}
}

// -- offset + limit pagination ----------------------------------------------
//
// Firespotter uses .offset(n).limit(m) for page-based results.

func TestAdvanced_OffsetPagination(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("rows")
	for i := 1; i <= 10; i++ {
		col.Doc(fmt.Sprintf("r%02d", i)).Set(ctx, map[string]any{"n": i})
	}

	q := col.OrderBy("n", firestore.Asc)

	// Page 2 of size 3: offset=3, limit=3 -> n=4,5,6.
	page, err := q.Offset(3).Limit(3).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("offset pagination: %v", err)
	}
	if len(page) != 3 {
		t.Fatalf("offset page len = %d, want 3", len(page))
	}
	if n, _ := page[0].DataAt("n"); n != int64(4) {
		t.Errorf("first = %v, want 4", n)
	}
	if n, _ := page[len(page)-1].DataAt("n"); n != int64(6) {
		t.Errorf("last = %v, want 6", n)
	}
}

// -- is_null / is_not_null on missing fields --------------------------------
//
// Firespotter tests null fields. IS_NOT_NULL must exclude docs where the
// field is entirely absent (not stored), not just docs where it is null.

func TestAdvanced_IsNull_OnlyMatchesExplicitNull(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("entries")
	col.Doc("has-null").Set(ctx, map[string]any{"x": nil})
	col.Doc("has-value").Set(ctx, map[string]any{"x": int64(1)})
	col.Doc("missing-x").Set(ctx, map[string]any{"y": int64(1)}) // no x

	docs, err := col.Where("x", "==", nil).Documents(ctx).GetAll()
	if err != nil {
		t.Fatalf("is_null: %v", err)
	}
	// Only has-null; has-value and missing-x must not match.
	if len(docs) != 1 {
		t.Errorf("IS_NULL: want 1 (has-null), got %d", len(docs))
	}
}

// -- DocumentIterator / streaming ------------------------------------------
//
// Firespotter uses .run() for streaming iteration. Test the equivalent Go
// iterator pattern which streams results one at a time without buffering all.

func TestAdvanced_StreamingIterator(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("stream")
	for i := 1; i <= 5; i++ {
		col.Doc(fmt.Sprintf("s%d", i)).Set(ctx, map[string]any{"v": i})
	}

	iter := col.OrderBy("v", firestore.Asc).Documents(ctx)
	defer iter.Stop()

	var got []int64
	for {
		snap, err := iter.Next()
		if err != nil {
			break // io.EOF or context done
		}
		v, _ := snap.DataAt("v")
		got = append(got, v.(int64))
	}

	if len(got) != 5 {
		t.Fatalf("streaming: want 5 docs, got %d", len(got))
	}
	for i, v := range got {
		if v != int64(i+1) {
			t.Errorf("got[%d] = %d, want %d", i, v, i+1)
		}
	}
}

// -- aggregation sum + avg with filter -------------------------------------
//
// Firespotter uses count(limit=n). Test sum/avg with a WHERE clause, which
// requires the SQL aggregation streaming path to apply the filter correctly.

func TestAdvanced_AggregationSumWithFilter(t *testing.T) {
	client := newClient(t)
	ctx := context.Background()

	col := client.Collection("orders")
	for _, doc := range []map[string]any{
		{"region": "us", "amount": int64(100)},
		{"region": "us", "amount": int64(200)},
		{"region": "eu", "amount": int64(500)},
	} {
		col.NewDoc().Set(ctx, doc)
	}

	filtered := col.Where("region", "==", "us")
	result, err := filtered.NewAggregationQuery().
		WithSum("amount", "total").
		Get(ctx)
	if err != nil {
		t.Fatalf("aggregation sum: %v", err)
	}
	if total := aggInt(result["total"]); total != 300 {
		t.Errorf("sum(amount) for us = %d, want 300", total)
	}
}
