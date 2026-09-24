package server

import "testing"

func testOperationLogBoundsHistory(t *testing.T) {
	log := NewOperationLog("test")
	for i := 0; i < operationHistoryLimit+1; i++ {
		log.Add(OperationEntry{Method: "lookup"})
	}

	got := log.Query(OperationQuery{Limit: operationHistoryLimit})
	if got.Total != operationHistoryLimit {
		t.Fatalf("total = %d, want %d", got.Total, operationHistoryLimit)
	}
	if got.Entries[0].ID != 2 {
		t.Fatalf("oldest retained ID = %d, want 2", got.Entries[0].ID)
	}
}

func testOperationLogQueryReadsRingWithoutMutatingStoredDetails(t *testing.T) {
	log := NewOperationLog("test")
	details := map[string]any{"kind": "original"}
	added := log.Add(OperationEntry{Method: "lookup", Details: details})
	log.Add(OperationEntry{Method: "commit"})
	log.Add(OperationEntry{Method: "lookup"})
	details["kind"] = "changed"
	added.Details["kind"] = "changed through returned entry"

	got := log.Query(OperationQuery{Method: "lookup", OrderDesc: true, Offset: 1, Limit: 1})
	if got.Total != 3 || got.Matched != 2 || len(got.Entries) != 1 || got.Entries[0].ID != 1 {
		t.Fatalf("query result = total %d, matched %d, entries %+v", got.Total, got.Matched, got.Entries)
	}
	if got.Entries[0].Details["kind"] != "original" {
		t.Fatalf("stored details = %v, want original", got.Entries[0].Details["kind"])
	}

	recent := log.Recent(2)
	if len(recent) != 2 || recent[0].ID != 2 || recent[1].ID != 3 {
		t.Fatalf("recent entries = %+v, want IDs 2 and 3", recent)
	}
}

func TestOperationLog(t *testing.T) {
	testOperationLogBoundsHistory(t)
	testOperationLogQueryReadsRingWithoutMutatingStoredDetails(t)
}
