package webchannel_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/magnus-rattlehead/hearthstore/internal/config"
	"github.com/magnus-rattlehead/hearthstore/internal/server"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"github.com/magnus-rattlehead/hearthstore/internal/webchannel"
)

const (
	wcTestProject = "test-proj"
	wcTestDB      = "(default)"
	wcTestDBPath  = "projects/test-proj/databases/(default)"
)

func newTestHandler(t *testing.T) (*webchannel.Handler, *storage.Store) {
	t.Helper()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	h := webchannel.New(server.New(store))
	t.Cleanup(func() { h.Close() })
	return h, store
}

// readChunk reads one WebChannel chunk: <decimal_size>\n<payload>
func readChunk(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	n, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("invalid chunk size %q: %v", line, err)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(br, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// parseChunkResponses decodes a data chunk ([[seqNo, [protoJSON…]], …]) into
// ListenResponses. The handshake chunk ([[0, ["c", SID, …]]]) is silently skipped.
func parseChunkResponses(chunk []byte) ([]*firestorepb.ListenResponse, error) {
	var outer []json.RawMessage
	if err := json.Unmarshal(chunk, &outer); err != nil {
		return nil, err
	}
	var out []*firestorepb.ListenResponse
	for _, item := range outer {
		var pair [2]json.RawMessage
		if err := json.Unmarshal(item, &pair); err != nil {
			continue
		}
		var protoArray []json.RawMessage
		if err := json.Unmarshal(pair[1], &protoArray); err != nil {
			continue
		}
		for _, raw := range protoArray {
			var resp firestorepb.ListenResponse
			if err := protojson.Unmarshal(raw, &resp); err != nil {
				return nil, fmt.Errorf("unmarshal response: %v (raw: %s)", err, raw)
			}
			out = append(out, &resp)
		}
	}
	return out, nil
}

// buildAddTargetBody builds a URL-encoded POST body containing one AddTarget
// ListenRequest for the given collection and target ID.
func buildAddTargetBody(db, collection string, targetID int32) string {
	parent := db + "/documents"
	req := &firestorepb.ListenRequest{
		Database: db,
		TargetChange: &firestorepb.ListenRequest_AddTarget{
			AddTarget: &firestorepb.Target{
				TargetId: targetID,
				TargetType: &firestorepb.Target_Query{
					Query: &firestorepb.Target_QueryTarget{
						Parent: parent,
						QueryType: &firestorepb.Target_QueryTarget_StructuredQuery{
							StructuredQuery: &firestorepb.StructuredQuery{
								From: []*firestorepb.StructuredQuery_CollectionSelector{
									{CollectionId: collection},
								},
							},
						},
					},
				},
			},
		},
	}
	reqJSON, _ := protojson.Marshal(req)
	return url.Values{
		"count":         {"1"},
		"ofs":           {"0"},
		"req0___data__": {string(reqJSON)},
	}.Encode()
}

// extractSID parses the SID from a WebChannel handshake chunk.
// Handshake format: [[0, ["c", "<SID>", null, 8]]]
func extractSID(t *testing.T, chunk []byte) string {
	t.Helper()
	var outer []json.RawMessage
	if err := json.Unmarshal(chunk, &outer); err != nil {
		t.Fatalf("parse handshake outer: %v", err)
	}
	if len(outer) == 0 {
		t.Fatal("empty handshake array")
	}
	var pair [2]json.RawMessage
	if err := json.Unmarshal(outer[0], &pair); err != nil {
		t.Fatalf("parse handshake pair: %v", err)
	}
	var data []json.RawMessage
	if err := json.Unmarshal(pair[1], &data); err != nil {
		t.Fatalf("parse handshake data: %v", err)
	}
	if len(data) < 2 {
		t.Fatalf("handshake data too short: %s", pair[1])
	}
	var sid string
	if err := json.Unmarshal(data[1], &sid); err != nil {
		t.Fatalf("parse SID: %v", err)
	}
	return sid
}

// doInitialPOST sends the opening WebChannel POST. The caller must close resp.Body.
func doInitialPOST(t *testing.T, ts *httptest.Server, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost,
		ts.URL+"/google.firestore.v1.Firestore/Listen/channel?VER=8&t=1",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("initial POST: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("initial POST status = %d", resp.StatusCode)
	}
	return resp
}

// openSession sends the initial POST, reads (and returns) the handshake chunk,
// closes the POST response, and returns the SID.
func openSession(t *testing.T, ts *httptest.Server, body string) string {
	t.Helper()
	resp := doInitialPOST(t, ts, body)
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	handshake, err := readChunk(br)
	if err != nil {
		t.Fatalf("readChunk handshake: %v", err)
	}
	return extractSID(t, handshake)
}

// collectResponses gathers at least n ListenResponses via successive GET long-polls.
// aid tracks the last acknowledged seqNo across calls and is updated in place.
// The function returns as soon as n responses are collected or the context expires.
func collectResponses(t *testing.T, ctx context.Context, ts *httptest.Server, sid string, aid *int, n int) []*firestorepb.ListenResponse {
	t.Helper()
	var responses []*firestorepb.ListenResponse
	for len(responses) < n {
		pollURL := fmt.Sprintf(
			"%s/google.firestore.v1.Firestore/Listen/channel?VER=8&SID=%s&RID=rpc&AID=%d&CI=0&TYPE=xmlhttp",
			ts.URL, sid, *aid)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, pollURL, nil)
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Logf("GET poll stopped after %d/%d responses: %v", len(responses), n, err)
			return responses
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			t.Fatalf("GET poll status = %d", resp.StatusCode)
		}
		br := bufio.NewReader(resp.Body)
		before := len(responses)
		for {
			chunk, err := readChunk(br)
			if err != nil {
				break
			}
			parsed, err := parseChunkResponses(chunk)
			if err != nil {
				resp.Body.Close()
				t.Fatalf("parseChunkResponses: %v", err)
			}
			responses = append(responses, parsed...)
			// AID is the WebChannel seqNo of the last acknowledged chunk.
			// Increment by 1 per chunk (each stream.Send is one chunk) so the
			// value accumulates across successive collectResponses calls rather
			// than being reset to the per-call response count.
			*aid++
			if len(responses) >= n {
				// Collected enough — close the body to unblock the streaming
				// server-side handler (r.Context().Done() fires on close).
				break
			}
		}
		resp.Body.Close()
		if len(responses) == before {
			// No progress — session ended with no data.
			break
		}
	}
	return responses
}

func hasTargetChange(responses []*firestorepb.ListenResponse, want firestorepb.TargetChange_TargetChangeType) bool {
	for _, r := range responses {
		if tc, ok := r.ResponseType.(*firestorepb.ListenResponse_TargetChange); ok {
			if tc.TargetChange.TargetChangeType == want {
				return true
			}
		}
	}
	return false
}

// buildSubsequentPOSTBody builds a URL-encoded POST body for a subsequent POST
// (with SID already established), sending one ListenRequest.
func buildSubsequentPOSTBody(req *firestorepb.ListenRequest, ofs int) string {
	reqJSON, _ := protojson.Marshal(req)
	return url.Values{
		"count":         {"1"},
		"ofs":           {strconv.Itoa(ofs)},
		"req0___data__": {string(reqJSON)},
	}.Encode()
}

// doSubsequentPOST sends a subsequent WebChannel POST (with SID).
func doSubsequentPOST(t *testing.T, ts *httptest.Server, sid string, body string) {
	t.Helper()
	postURL := fmt.Sprintf("%s/google.firestore.v1.Firestore/Listen/channel?VER=8&SID=%s&RID=rpc", ts.URL, sid)
	req, _ := http.NewRequest(http.MethodPost, postURL, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("subsequent POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("subsequent POST status = %d", resp.StatusCode)
	}
}

// buildDocTargetBody builds a URL-encoded POST body for a Target.Documents AddTarget.
func buildDocTargetBody(db string, docPaths []string, targetID int32) string {
	fullNames := make([]string, len(docPaths))
	for i, p := range docPaths {
		fullNames[i] = db + "/documents/" + p
	}
	req := &firestorepb.ListenRequest{
		Database: db,
		TargetChange: &firestorepb.ListenRequest_AddTarget{
			AddTarget: &firestorepb.Target{
				TargetId: targetID,
				TargetType: &firestorepb.Target_Documents{
					Documents: &firestorepb.Target_DocumentsTarget{
						Documents: fullNames,
					},
				},
			},
		},
	}
	reqJSON, _ := protojson.Marshal(req)
	return url.Values{
		"count":         {"1"},
		"ofs":           {"0"},
		"req0___data__": {string(reqJSON)},
	}.Encode()
}

// buildSubcollectionTargetReq builds a ListenRequest for a subcollection query.
func buildSubcollectionTargetReq(db, parentDocPath, subcollection string, targetID int32) *firestorepb.ListenRequest {
	parent := db + "/documents/" + parentDocPath
	return &firestorepb.ListenRequest{
		Database: db,
		TargetChange: &firestorepb.ListenRequest_AddTarget{
			AddTarget: &firestorepb.Target{
				TargetId: targetID,
				TargetType: &firestorepb.Target_Query{
					Query: &firestorepb.Target_QueryTarget{
						Parent: parent,
						QueryType: &firestorepb.Target_QueryTarget_StructuredQuery{
							StructuredQuery: &firestorepb.StructuredQuery{
								From: []*firestorepb.StructuredQuery_CollectionSelector{
									{CollectionId: subcollection},
								},
							},
						},
					},
				},
			},
		},
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestWebChannel_Handshake(t *testing.T) {
	h, _ := newTestHandler(t)
	ts := httptest.NewServer(h)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := buildAddTargetBody(wcTestDBPath, "things", 1)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		ts.URL+"/google.firestore.v1.Firestore/Listen/channel?VER=8&t=1",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	br := bufio.NewReader(resp.Body)
	chunk, err := readChunk(br)
	if err != nil {
		t.Fatalf("readChunk: %v", err)
	}

	sid := extractSID(t, chunk)
	if sid == "" {
		t.Error("handshake SID is empty")
	}
}

func TestWebChannel_AddTarget_TargetChangeADD(t *testing.T) {
	h, _ := newTestHandler(t)
	ts := httptest.NewServer(h)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := buildAddTargetBody(wcTestDBPath, "things", 1)
	sid := openSession(t, ts, body)

	// GET poll — empty collection: ADD + CURRENT + NO_CHANGE = 3 responses.
	aid := 0
	responses := collectResponses(t, ctx, ts, sid, &aid, 3)

	if !hasTargetChange(responses, firestorepb.TargetChange_ADD) {
		t.Error("want TargetChange_ADD in responses")
	}
	if !hasTargetChange(responses, firestorepb.TargetChange_CURRENT) {
		t.Error("want TargetChange_CURRENT in responses")
	}
	if !hasTargetChange(responses, firestorepb.TargetChange_NO_CHANGE) {
		t.Error("want TargetChange_NO_CHANGE in responses")
	}
}

func TestWebChannel_DocumentStreamed(t *testing.T) {
	h, store := newTestHandler(t)
	ts := httptest.NewServer(h)
	defer ts.Close()

	// Pre-seed a document.
	_, err := store.InsertDoc(wcTestProject, wcTestDB, "restaurants", "", "restaurants/r1",
		&firestorepb.Document{
			Name: wcTestDBPath + "/documents/restaurants/r1",
			Fields: map[string]*firestorepb.Value{
				"name": {ValueType: &firestorepb.Value_StringValue{StringValue: "Test Restaurant"}},
			},
		})
	if err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := buildAddTargetBody(wcTestDBPath, "restaurants", 1)
	sid := openSession(t, ts, body)

	// With one document: ADD + DocumentChange + CURRENT + NO_CHANGE = 4 responses.
	aid := 0
	responses := collectResponses(t, ctx, ts, sid, &aid, 4)

	foundDoc := false
	for _, r := range responses {
		if dc, ok := r.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			if strings.HasSuffix(dc.DocumentChange.Document.Name, "/restaurants/r1") {
				foundDoc = true
			}
		}
	}
	if !foundDoc {
		t.Error("want DocumentChange for restaurants/r1 in initial snapshot")
	}
	if !hasTargetChange(responses, firestorepb.TargetChange_ADD) {
		t.Error("want TargetChange_ADD")
	}
	if !hasTargetChange(responses, firestorepb.TargetChange_CURRENT) {
		t.Error("want TargetChange_CURRENT")
	}
}

func TestWebChannel_SubsequentPOST_RemoveTarget(t *testing.T) {
	h, _ := newTestHandler(t)
	ts := httptest.NewServer(h)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := buildAddTargetBody(wcTestDBPath, "things", 1)
	sid := openSession(t, ts, body)

	// Consume initial snapshot (ADD + CURRENT + NO_CHANGE).
	aid := 0
	collectResponses(t, ctx, ts, sid, &aid, 3)

	// Send a subsequent POST with RemoveTarget.
	removeReq := &firestorepb.ListenRequest{
		Database: wcTestDBPath,
		TargetChange: &firestorepb.ListenRequest_RemoveTarget{
			RemoveTarget: 1,
		},
	}
	removeJSON, _ := protojson.Marshal(removeReq)
	removeBody := url.Values{
		"count":         {"1"},
		"ofs":           {"1"},
		"req0___data__": {string(removeJSON)},
	}.Encode()

	postURL := fmt.Sprintf("%s/google.firestore.v1.Firestore/Listen/channel?VER=8&SID=%s&RID=2", ts.URL, sid)
	postReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, postURL, strings.NewReader(removeBody))
	postReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	postResp, err := ts.Client().Do(postReq)
	if err != nil {
		t.Fatalf("subsequent POST: %v", err)
	}
	postResp.Body.Close()

	if postResp.StatusCode != http.StatusOK {
		t.Fatalf("subsequent POST status = %d", postResp.StatusCode)
	}

	// GET poll — the REMOVE TargetChange should arrive.
	moreResponses := collectResponses(t, ctx, ts, sid, &aid, 1)
	if !hasTargetChange(moreResponses, firestorepb.TargetChange_REMOVE) {
		t.Error("want TargetChange_REMOVE after RemoveTarget")
	}
}

// TestWebChannel_InAppNavigation simulates the Firebase JS SDK flow when a user
// navigates from a list page to a detail page within the app:
//
//  1. Initial POST: AddTarget(query, restaurants collection) → session created
//  2. GET poll: consume initial snapshot (restaurant + ADD/CURRENT/NO_CHANGE)
//  3. Subsequent POST: RemoveTarget(restaurants) — list page teardown
//  4. Subsequent POST: AddTarget(docs, restaurant doc) — detail page doc subscription
//  5. GET poll: consume restaurant document snapshot
//  6. Subsequent POST: AddTarget(query, ratings subcollection) — review list subscription
//  7. GET poll: verify rating DocumentChange arrives
func TestWebChannel_InAppNavigation(t *testing.T) {
	h, store := newTestHandler(t)
	ts := httptest.NewServer(h)
	defer ts.Close()

	// Seed a restaurant and a rating subcollection document.
	_, err := store.InsertDoc(wcTestProject, wcTestDB, "restaurants", "", "restaurants/r1",
		&firestorepb.Document{
			Name: wcTestDBPath + "/documents/restaurants/r1",
			Fields: map[string]*firestorepb.Value{
				"name": {ValueType: &firestorepb.Value_StringValue{StringValue: "Test"}},
			},
		})
	if err != nil {
		t.Fatalf("InsertDoc restaurant: %v", err)
	}
	_, err = store.InsertDoc(wcTestProject, wcTestDB, "ratings", "restaurants/r1", "restaurants/r1/ratings/rev1",
		&firestorepb.Document{
			Name: wcTestDBPath + "/documents/restaurants/r1/ratings/rev1",
			Fields: map[string]*firestorepb.Value{
				"text": {ValueType: &firestorepb.Value_StringValue{StringValue: "Great place"}},
			},
		})
	if err != nil {
		t.Fatalf("InsertDoc rating: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Step 1–2: Establish session with restaurants collection query, drain initial snapshot.
	body := buildAddTargetBody(wcTestDBPath, "restaurants", 1)
	sid := openSession(t, ts, body)
	aid := 0
	collectResponses(t, ctx, ts, sid, &aid, 4) // ADD + DocumentChange + CURRENT + NO_CHANGE

	// Step 3: Simulate list page teardown — RemoveTarget for the restaurants collection.
	removeReq := &firestorepb.ListenRequest{
		Database: wcTestDBPath,
		TargetChange: &firestorepb.ListenRequest_RemoveTarget{RemoveTarget: 1},
	}
	doSubsequentPOST(t, ts, sid, buildSubsequentPOSTBody(removeReq, 1))
	collectResponses(t, ctx, ts, sid, &aid, 1) // REMOVE TargetChange

	// Step 4–5: Detail page adds a Target.Documents for the specific restaurant.
	addDocReq := &firestorepb.ListenRequest{
		Database: wcTestDBPath,
		TargetChange: &firestorepb.ListenRequest_AddTarget{
			AddTarget: &firestorepb.Target{
				TargetId: 4,
				TargetType: &firestorepb.Target_Documents{
					Documents: &firestorepb.Target_DocumentsTarget{
						Documents: []string{wcTestDBPath + "/documents/restaurants/r1"},
					},
				},
			},
		},
	}
	doSubsequentPOST(t, ts, sid, buildSubsequentPOSTBody(addDocReq, 2))
	docResponses := collectResponses(t, ctx, ts, sid, &aid, 4) // ADD + DocumentChange + CURRENT + NO_CHANGE

	foundRestaurant := false
	for _, r := range docResponses {
		if dc, ok := r.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			if strings.HasSuffix(dc.DocumentChange.Document.Name, "/restaurants/r1") {
				foundRestaurant = true
			}
		}
	}
	if !foundRestaurant {
		t.Error("want DocumentChange for restaurants/r1 after in-app navigation")
	}

	// Step 6–7: Review list component adds a subcollection query for ratings.
	ratingsReq := buildSubcollectionTargetReq(wcTestDBPath, "restaurants/r1", "ratings", 5)
	doSubsequentPOST(t, ts, sid, buildSubsequentPOSTBody(ratingsReq, 3))
	ratingResponses := collectResponses(t, ctx, ts, sid, &aid, 4) // ADD + DocumentChange + CURRENT + NO_CHANGE

	foundRating := false
	for _, r := range ratingResponses {
		if dc, ok := r.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			if strings.HasSuffix(dc.DocumentChange.Document.Name, "/ratings/rev1") {
				foundRating = true
			}
		}
	}
	if !foundRating {
		t.Error("want DocumentChange for ratings/rev1 after subcollection AddTarget via subsequent POST")
	}
}

// ── Write stream WebChannel helpers ──────────────────────────────────────────

// parseWriteResponses decodes a Write-channel data chunk into WriteResponses.
// The chunk format is identical to the Listen channel: [[seqNo, [protoJSON…]], …]
// The WebChannel handshake chunk ([[0, ["c", SID, …]]]) is silently skipped.
func parseWriteResponses(chunk []byte) ([]*firestorepb.WriteResponse, error) {
	var outer []json.RawMessage
	if err := json.Unmarshal(chunk, &outer); err != nil {
		return nil, err
	}
	var out []*firestorepb.WriteResponse
	for _, item := range outer {
		var pair [2]json.RawMessage
		if err := json.Unmarshal(item, &pair); err != nil {
			continue
		}
		var protoArray []json.RawMessage
		if err := json.Unmarshal(pair[1], &protoArray); err != nil {
			continue
		}
		for _, raw := range protoArray {
			var resp firestorepb.WriteResponse
			if err := protojson.Unmarshal(raw, &resp); err != nil {
				return nil, fmt.Errorf("unmarshal WriteResponse: %v (raw: %s)", err, raw)
			}
			out = append(out, &resp)
		}
	}
	return out, nil
}

// openWriteSession opens a Write stream WebChannel session:
//  1. POSTs the Write initial request (database handshake)
//  2. Reads the WebChannel handshake to extract SID
//  3. Polls once (AID=0) to drain the Write RPC handshake WriteResponse
//
// Returns (sid, lastSeqNo, handshakeToken) so subsequent GET polls start at AID=lastSeqNo.
func openWriteSession(t *testing.T, ts *httptest.Server) (sid string, lastSeqNo int64, handshakeToken []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	writeReq := &firestorepb.WriteRequest{Database: wcTestDBPath}
	writeReqJSON, _ := protojson.Marshal(writeReq)
	postBody := url.Values{
		"count":         {"1"},
		"ofs":           {"0"},
		"req0___data__": {string(writeReqJSON)},
	}.Encode()

	postReq, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		ts.URL+"/google.firestore.v1.Firestore/Write/channel?VER=8&t=1",
		strings.NewReader(postBody))
	postReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	postResp, err := ts.Client().Do(postReq)
	if err != nil {
		t.Fatalf("Write initial POST: %v", err)
	}
	if postResp.StatusCode != http.StatusOK {
		postResp.Body.Close()
		t.Fatalf("Write initial POST status = %d", postResp.StatusCode)
	}
	br := bufio.NewReader(postResp.Body)
	handshake, err := readChunk(br)
	postResp.Body.Close()
	if err != nil {
		t.Fatalf("read Write handshake: %v", err)
	}
	sid = extractSID(t, handshake)

	// Drain the Write RPC open response (seqNo=1) via a GET poll.
	pollURL := fmt.Sprintf(
		"%s/google.firestore.v1.Firestore/Write/channel?VER=8&SID=%s&RID=rpc&AID=0&CI=0&TYPE=xmlhttp",
		ts.URL, sid)
	getReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, pollURL, nil)
	getResp, err := ts.Client().Do(getReq)
	if err != nil {
		t.Fatalf("Write handshake GET poll: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		getResp.Body.Close()
		t.Fatalf("Write handshake GET poll status = %d", getResp.StatusCode)
	}
	// The Write GET poll is a streaming response that never closes until the
	// session ends. Read only the handshake WriteResponse (seqNo=1), then close
	// the body so the server's GET handler exits promptly via r.Context().Done().
	br = bufio.NewReader(getResp.Body)
	for {
		chunk, err := readChunk(br)
		if err != nil {
			break
		}
		resps, err := parseWriteResponses(chunk)
		if err != nil {
			getResp.Body.Close()
			t.Fatalf("parseWriteResponses: %v", err)
		}
		for _, r := range resps {
			if len(r.StreamToken) > 0 {
				handshakeToken = r.StreamToken
			}
		}
		// Track the highest seqNo in this chunk.
		var outer []json.RawMessage
		if json.Unmarshal(chunk, &outer) == nil {
			for _, item := range outer {
				var pair [2]json.RawMessage
				if json.Unmarshal(item, &pair) == nil {
					var n int64
					if json.Unmarshal(pair[0], &n) == nil && n > lastSeqNo {
						lastSeqNo = n
					}
				}
			}
		}
		if lastSeqNo > 0 {
			// Got the handshake WriteResponse — close the body to signal the
			// server that this GET poll is done. The Write session stays alive.
			break
		}
	}
	getResp.Body.Close()
	if lastSeqNo == 0 {
		t.Fatal("openWriteSession: no chunks received from Write RPC open response")
	}
	return sid, lastSeqNo, handshakeToken
}

// doWritePOST sends a subsequent POST to the Write channel.
func doWritePOST(t *testing.T, ts *httptest.Server, sid string, writeReq *firestorepb.WriteRequest, ofs int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reqJSON, _ := protojson.Marshal(writeReq)
	postBody := url.Values{
		"count":         {"1"},
		"ofs":           {strconv.Itoa(ofs)},
		"req0___data__": {string(reqJSON)},
	}.Encode()

	postURL := fmt.Sprintf("%s/google.firestore.v1.Firestore/Write/channel?VER=8&SID=%s&RID=rpc",
		ts.URL, sid)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, postURL, strings.NewReader(postBody))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("Write subsequent POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Write subsequent POST status = %d", resp.StatusCode)
	}
}

// drainWriteChunks performs one GET poll on the Write channel and returns all
// WriteResponses received plus the highest seqNo seen. Because Write GET polls
// are now streaming (the server keeps the connection open), the function closes
// the body as soon as it receives a WriteResponse with non-empty WriteResults
// (or an error chunk), rather than waiting for EOF.
func drainWriteChunks(t *testing.T, ctx context.Context, ts *httptest.Server, sid string, aid int64) ([]*firestorepb.WriteResponse, int64) {
	t.Helper()
	pollURL := fmt.Sprintf(
		"%s/google.firestore.v1.Firestore/Write/channel?VER=8&SID=%s&RID=rpc&AID=%d&CI=0&TYPE=xmlhttp",
		ts.URL, sid, aid)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, pollURL, nil)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("Write GET poll: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("Write GET poll status = %d", resp.StatusCode)
	}
	var responses []*firestorepb.WriteResponse
	var highSeq int64
	br := bufio.NewReader(resp.Body)
	for {
		chunk, err := readChunk(br)
		if err != nil {
			break
		}
		resps, err := parseWriteResponses(chunk)
		if err != nil {
			resp.Body.Close()
			t.Fatalf("drainWriteChunks parseWriteResponses: %v", err)
		}
		responses = append(responses, resps...)
		var outer []json.RawMessage
		if json.Unmarshal(chunk, &outer) == nil {
			for _, item := range outer {
				var pair [2]json.RawMessage
				if json.Unmarshal(item, &pair) == nil {
					var n int64
					if json.Unmarshal(pair[0], &n) == nil && n > highSeq {
						highSeq = n
					}
				}
			}
		}
		// Got at least one WriteResponse — close the body so the server's
		// streaming GET handler exits via r.Context().Done(). The Write session
		// itself stays alive; the caller can open a new GET poll if needed.
		if len(resps) > 0 {
			break
		}
	}
	resp.Body.Close()
	return responses, highSeq
}

// ── Write stream WebChannel tests ────────────────────────────────────────────

// TestWebChannel_Write_EmptyBatch verifies that the server responds to an empty
// WriteRequest (writes = nil) with an updated stream token. The Firebase JS SDK
// sends such a batch to resume/refresh the stream after the handshake.
func TestWebChannel_Write_EmptyBatch(t *testing.T) {
	h, _ := newTestHandler(t)
	ts := httptest.NewServer(h)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sid, lastSeq, token := openWriteSession(t, ts)

	// Send an empty WriteRequest with the handshake token — no Writes field.
	doWritePOST(t, ts, sid, &firestorepb.WriteRequest{
		StreamToken: token,
	}, 1)

	resps, _ := drainWriteChunks(t, ctx, ts, sid, lastSeq)
	if len(resps) == 0 {
		t.Fatal("want WriteResponse to empty batch, got none")
	}
	if len(resps[0].StreamToken) == 0 {
		t.Error("want non-empty StreamToken in response to empty batch")
	}
	if resps[0].CommitTime == nil {
		t.Error("want CommitTime set in response to empty batch")
	}
}

// TestWebChannel_Write_WithDocument simulates FriendlyChat's addDoc with
// serverTimestamp(): sends a write with UpdateTransforms and verifies the server
// applies the transform and returns a WriteResult with UpdateTime set.
func TestWebChannel_Write_WithDocument(t *testing.T) {
	h, store := newTestHandler(t)
	ts := httptest.NewServer(h)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sid, lastSeq, token := openWriteSession(t, ts)

	docName := wcTestDBPath + "/documents/messages/m1"
	doWritePOST(t, ts, sid, &firestorepb.WriteRequest{
		StreamToken: token,
		Writes: []*firestorepb.Write{
			{
				Operation: &firestorepb.Write_Update{
					Update: &firestorepb.Document{
						Name: docName,
						Fields: map[string]*firestorepb.Value{
							"text": {ValueType: &firestorepb.Value_StringValue{StringValue: "hello"}},
							"uid":  {ValueType: &firestorepb.Value_StringValue{StringValue: "user1"}},
						},
					},
				},
				UpdateTransforms: []*firestorepb.DocumentTransform_FieldTransform{
					{
						FieldPath: "timestamp",
						TransformType: &firestorepb.DocumentTransform_FieldTransform_SetToServerValue{
							SetToServerValue: firestorepb.DocumentTransform_FieldTransform_REQUEST_TIME,
						},
					},
				},
			},
		},
	}, 1)

	resps, _ := drainWriteChunks(t, ctx, ts, sid, lastSeq)
	if len(resps) == 0 {
		t.Fatal("want WriteResponse for document write, got none")
	}
	resp := resps[0]
	if len(resp.WriteResults) != 1 {
		t.Fatalf("want 1 WriteResult, got %d", len(resp.WriteResults))
	}
	if resp.WriteResults[0].UpdateTime == nil {
		t.Error("want UpdateTime set in WriteResult")
	}
	if resp.CommitTime == nil {
		t.Error("want CommitTime set")
	}

	// Verify the document was actually stored with the timestamp field.
	doc, err := store.GetDoc(wcTestProject, wcTestDB, "messages/m1")
	if err != nil {
		t.Fatalf("GetDoc after Write: %v", err)
	}
	if doc.Fields["text"].GetStringValue() != "hello" {
		t.Errorf("text = %q, want hello", doc.Fields["text"].GetStringValue())
	}
	if doc.Fields["timestamp"] == nil {
		t.Error("timestamp field should have been set by server transform")
	}
	if _, ok := doc.Fields["timestamp"].ValueType.(*firestorepb.Value_TimestampValue); !ok {
		t.Errorf("timestamp should be a timestamp value, got %T", doc.Fields["timestamp"].ValueType)
	}
}

// TestWebChannel_ListenAndWrite simulates the complete FriendlyChat session:
// a listener is open on the "messages" collection and then a message is written
// via the Write stream. The test verifies that:
//  1. The Write stream returns a WriteResult for the new document.
//  2. The Listen stream delivers a DocumentChange for the same document.
func TestWebChannel_ListenAndWrite(t *testing.T) {
	h, _ := newTestHandler(t)
	ts := httptest.NewServer(h)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Open a Listen session on "messages" and drain the empty initial snapshot.
	listenBody := buildAddTargetBody(wcTestDBPath, "messages", 1)
	listenSID := openSession(t, ts, listenBody)
	listenAID := 0
	collectResponses(t, ctx, ts, listenSID, &listenAID, 3) // ADD + CURRENT + NO_CHANGE

	// Open a Write session and drain the handshake.
	writeSID, writeSeq, writeToken := openWriteSession(t, ts)

	// Write a message document via the Write stream.
	docName := wcTestDBPath + "/documents/messages/chat1"
	doWritePOST(t, ts, writeSID, &firestorepb.WriteRequest{
		StreamToken: writeToken,
		Writes: []*firestorepb.Write{
			{
				Operation: &firestorepb.Write_Update{
					Update: &firestorepb.Document{
						Name: docName,
						Fields: map[string]*firestorepb.Value{
							"text": {ValueType: &firestorepb.Value_StringValue{StringValue: "FriendlyChat message"}},
						},
					},
				},
			},
		},
	}, 1)

	// Verify the Write stream acknowledges the write.
	writeResps, _ := drainWriteChunks(t, ctx, ts, writeSID, writeSeq)
	if len(writeResps) == 0 {
		t.Fatal("want WriteResponse for the message write, got none")
	}
	if len(writeResps[0].WriteResults) != 1 {
		t.Fatalf("want 1 WriteResult, got %d", len(writeResps[0].WriteResults))
	}

	// Verify the Listen stream delivers a DocumentChange for the new message.
	// The change notification may need one additional GET poll beyond the initial snapshot.
	listenResps := collectResponses(t, ctx, ts, listenSID, &listenAID, 1)

	foundMsg := false
	for _, r := range listenResps {
		if dc, ok := r.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			if strings.HasSuffix(dc.DocumentChange.Document.Name, "/messages/chat1") {
				foundMsg = true
			}
		}
	}
	if !foundMsg {
		t.Error("want DocumentChange for messages/chat1 on the Listen stream after Write stream write")
	}
}


// ── WebChannel regression tests ───────────────────────────────────────────────


// TestWebChannel_Write_FailedPrecondition_CleanClose verifies that when a write
// fails with FAILED_PRECONDITION the server closes the session cleanly (HTTP 200,
// no error chunk in the response body).
//
// We do NOT deliver an error chunk because the Firebase SDK's onMessage_ handler
// calls onError_() synchronously when it sees {"error": {...}}, transitioning the
// stream state from Open to Error/Closing while a microtask-queued sendRequest_()
// still expects Open state — triggering "INTERNAL ASSERTION FAILED: Unexpected state".
//
// The SDK handles stream close via handleWriteStreamClose_, which calls
// isPermanentWriteError(FAILED_PRECONDITION) → true → rejectFailedWrite, so the
// failed mutation is removed from the pipeline without infinite retry.
func TestWebChannel_Write_FailedPrecondition_CleanClose(t *testing.T) {
	h, store := newTestHandler(t)
	ts := httptest.NewServer(h)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Pre-create the document so the exists:false precondition will fail.
	docPath := "precond-msgs/p1"
	docName := wcTestDBPath + "/documents/" + docPath
	_, err := store.UpsertDoc(wcTestProject, wcTestDB, "precond-msgs", "", docPath,
		&firestorepb.Document{
			Name:   docName,
			Fields: map[string]*firestorepb.Value{"x": {ValueType: &firestorepb.Value_StringValue{StringValue: "original"}}},
		})
	if err != nil {
		t.Fatalf("UpsertDoc seed: %v", err)
	}

	sid, lastSeq, token := openWriteSession(t, ts)

	// Start the GET long-poll BEFORE submitting the write. This matches the
	// browser's behaviour: a long-poll is always established before a write is
	// submitted.
	doneCh := make(chan int, 1) // HTTP status code from GET poll
	go func() {
		pollURL := fmt.Sprintf(
			"%s/google.firestore.v1.Firestore/Write/channel?VER=8&SID=%s&RID=rpc&AID=%d&CI=0&TYPE=xmlhttp",
			ts.URL, sid, lastSeq)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, pollURL, nil)
		resp, err := ts.Client().Do(req)
		if err != nil {
			doneCh <- -1
			return
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		doneCh <- resp.StatusCode
	}()

	// Give the GET poll goroutine time to establish.
	time.Sleep(10 * time.Millisecond)

	// Submit a write with exists:false precondition — must fail.
	doWritePOST(t, ts, sid, &firestorepb.WriteRequest{
		StreamToken: token,
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Update{
				Update: &firestorepb.Document{
					Name:   docName,
					Fields: map[string]*firestorepb.Value{"x": {ValueType: &firestorepb.Value_StringValue{StringValue: "new"}}},
				},
			},
			CurrentDocument: &firestorepb.Precondition{
				ConditionType: &firestorepb.Precondition_Exists{Exists: false},
			},
		}},
	}, 1)

	// GET poll must return HTTP 200 (clean close, error chunk is delivered in body).
	code := <-doneCh
	if code != http.StatusOK {
		t.Errorf("want GET poll HTTP 200 on clean close, got %d", code)
	}

	// Session must be gone after the Write goroutine cleaned up.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.SessionCount() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if h.SessionCount() != 0 {
		t.Error("want session removed after FailedPrecondition write error")
	}
}

// TestWebChannel_Write_ErrorChunkDelivery verifies that when a write fails
// (e.g. NOT_FOUND for exists:true on a missing document), the error is
// delivered as a WebChannel message chunk in the GET response before the
// session closes. This is the "to top" first-row creation pattern: Rowy
// attempts a sentinel write with exists:true, receives NOT_FOUND, and
// retries with a plain create.
func TestWebChannel_Write_ErrorChunkDelivery(t *testing.T) {
	h, _ := newTestHandler(t)
	ts := httptest.NewServer(h)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// No document is pre-created: exists:true will return NOT_FOUND.
	docPath := "items/missing"
	docName := wcTestDBPath + "/documents/" + docPath

	sid, lastSeq, token := openWriteSession(t, ts)

	// Collect raw GET body to inspect the error chunk.
	type result struct {
		body []byte
		code int
	}
	resCh := make(chan result, 1)
	go func() {
		pollURL := fmt.Sprintf(
			"%s/google.firestore.v1.Firestore/Write/channel?VER=8&SID=%s&RID=rpc&AID=%d&CI=0&TYPE=xmlhttp",
			ts.URL, sid, lastSeq)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, pollURL, nil)
		resp, err := ts.Client().Do(req)
		if err != nil {
			resCh <- result{code: -1}
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		resCh <- result{body: body, code: resp.StatusCode}
	}()

	time.Sleep(10 * time.Millisecond)

	// Write with exists:true on a missing document → NOT_FOUND.
	doWritePOST(t, ts, sid, &firestorepb.WriteRequest{
		StreamToken: token,
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Update{
				Update: &firestorepb.Document{
					Name:   docName,
					Fields: map[string]*firestorepb.Value{"x": {ValueType: &firestorepb.Value_StringValue{StringValue: "val"}}},
				},
			},
			CurrentDocument: &firestorepb.Precondition{
				ConditionType: &firestorepb.Precondition_Exists{Exists: true},
			},
		}},
	}, 1)

	r := <-resCh
	if r.code != http.StatusOK {
		t.Fatalf("want HTTP 200, got %d", r.code)
	}

	// Parse the body to find the error chunk.
	// Each chunk: "<len>\n<json>". Scan all chunks and look for {"error": ...}.
	type wcError struct {
		Error struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"error"`
	}
	found := false
	br := bufio.NewReader(strings.NewReader(string(r.body)))
	for {
		rawChunk, err := readChunk(br)
		if err != nil {
			break
		}
		// Each chunk is [[seqNo, [msg1, msg2, ...]], ...].
		var outer []json.RawMessage
		if json.Unmarshal(rawChunk, &outer) != nil {
			continue
		}
		for _, item := range outer {
			var pair [2]json.RawMessage
			if json.Unmarshal(item, &pair) != nil {
				continue
			}
			var msgs []json.RawMessage
			if json.Unmarshal(pair[1], &msgs) != nil {
				continue
			}
			for _, msg := range msgs {
				var e wcError
				if json.Unmarshal(msg, &e) == nil && e.Error.Status != "" {
					if e.Error.Status != "NOT_FOUND" {
						t.Errorf("error status = %q, want NOT_FOUND", e.Error.Status)
					}
					found = true
				}
			}
		}
	}
	if !found {
		t.Error("want error chunk with status NOT_FOUND in GET response, got none")
	}
}

// TestWebChannel_Write_SingleChangeEvent_WithTransform is a regression test for
// the double-UpsertDoc → intermediate ChangeEvent bug.
//
// Before the fix, applyWrite called UpsertDoc twice for a Write_Update with
// transforms: once for field updates (no timestamp) and once for transforms
// (timestamp added). The Listen stream received two DocumentChange events for
// the same doc. The SDK consumed the mutation on the intermediate event; when
// the WriteResult arrived, the pipeline was empty → "mutations" crash.
//
// After the fix, a single UpsertDoc fires one ChangeEvent → one DocumentChange.
func TestWebChannel_Write_SingleChangeEvent_WithTransform(t *testing.T) {
	old := config.HeartbeatInterval
	config.HeartbeatInterval = 100 * time.Millisecond
	t.Cleanup(func() { config.HeartbeatInterval = old })

	h, _ := newTestHandler(t)
	ts := httptest.NewServer(h)
	defer ts.Close()
	_ = h // keep reference so GC doesn't close it

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Open a Listen session on "sc-msgs" and drain the initial snapshot.
	listenBody := buildAddTargetBody(wcTestDBPath, "sc-msgs", 1)
	listenSID := openSession(t, ts, listenBody)
	listenAID := 0
	collectResponses(t, ctx, ts, listenSID, &listenAID, 3) // ADD + CURRENT + NO_CHANGE

	// Open a Write session.
	writeSID, writeSeq, writeToken := openWriteSession(t, ts)

	// Write a document with a SERVER_TIMESTAMP transform.
	docName := wcTestDBPath + "/documents/sc-msgs/m1"
	doWritePOST(t, ts, writeSID, &firestorepb.WriteRequest{
		StreamToken: writeToken,
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Update{
				Update: &firestorepb.Document{
					Name:   docName,
					Fields: map[string]*firestorepb.Value{"text": {ValueType: &firestorepb.Value_StringValue{StringValue: "hello"}}},
				},
			},
			UpdateTransforms: []*firestorepb.DocumentTransform_FieldTransform{{
				FieldPath: "ts",
				TransformType: &firestorepb.DocumentTransform_FieldTransform_SetToServerValue{
					SetToServerValue: firestorepb.DocumentTransform_FieldTransform_REQUEST_TIME,
				},
			}},
		}},
	}, 1)

	// Wait for Write stream to acknowledge (ensures the ChangeEvent has fired).
	drainWriteChunks(t, ctx, ts, writeSID, writeSeq)

	// Collect Listen responses — use n=2 to catch any spurious second event.
	listenResps := collectResponses(t, ctx, ts, listenSID, &listenAID, 2)

	// Count DocumentChange events for sc-msgs/m1.
	var docChanges []*firestorepb.DocumentChange
	for _, r := range listenResps {
		if dc, ok := r.ResponseType.(*firestorepb.ListenResponse_DocumentChange); ok {
			if strings.HasSuffix(dc.DocumentChange.Document.GetName(), "/sc-msgs/m1") {
				docChanges = append(docChanges, dc.DocumentChange)
			}
		}
	}

	if len(docChanges) != 1 {
		t.Errorf("want exactly 1 DocumentChange for sc-msgs/m1, got %d (double-UpsertDoc regression)", len(docChanges))
	}
	if len(docChanges) >= 1 {
		ts := docChanges[0].Document.GetFields()["ts"]
		if ts == nil {
			t.Error("DocumentChange should have ts field set by SERVER_TIMESTAMP transform")
		} else if _, ok := ts.ValueType.(*firestorepb.Value_TimestampValue); !ok {
			t.Errorf("ts field should be a TimestampValue, got %T", ts.ValueType)
		}
	}
}

// ── Session GC tests ──────────────────────────────────────────────────────────

// TestWebChannel_SessionGC_IdleCleanup verifies that SweepIdleSessions cancels
// a session that has not been polled since config.SessionIdleTimeout ago.
func TestWebChannel_SessionGC_IdleCleanup(t *testing.T) {
	old := config.SessionIdleTimeout
	config.SessionIdleTimeout = 50 * time.Millisecond
	t.Cleanup(func() { config.SessionIdleTimeout = old })

	h, _ := newTestHandler(t)
	ts := httptest.NewServer(h)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Open a Listen session and poll once to set lastPollAt.
	body := buildAddTargetBody(wcTestDBPath, "gc-things", 1)
	sid := openSession(t, ts, body)
	aid := 0
	collectResponses(t, ctx, ts, sid, &aid, 3) // ADD + CURRENT + NO_CHANGE

	// Wait past the idle timeout.
	time.Sleep(100 * time.Millisecond)

	// Trigger the sweep.
	h.SweepIdleSessions()

	// Give the Listen goroutine time to exit and clean up.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.SessionCount() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if h.SessionCount() != 0 {
		t.Error("want session cleaned up after idle timeout, but it is still active")
	}
}

// TestWebChannel_SessionGC_ActiveNotCleaned verifies that a recently-polled
// session is NOT swept, even when SweepIdleSessions is called.
func TestWebChannel_SessionGC_ActiveNotCleaned(t *testing.T) {
	old := config.SessionIdleTimeout
	config.SessionIdleTimeout = 200 * time.Millisecond
	t.Cleanup(func() { config.SessionIdleTimeout = old })

	h, _ := newTestHandler(t)
	ts := httptest.NewServer(h)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := buildAddTargetBody(wcTestDBPath, "gc-active", 1)
	sid := openSession(t, ts, body)
	aid := 0
	// Poll to set lastPollAt to now.
	collectResponses(t, ctx, ts, sid, &aid, 3)

	// Immediately sweep: session was polled just now, well within 200ms timeout.
	h.SweepIdleSessions()

	// Small delay to allow any goroutine cleanup if cancellation accidentally ran.
	time.Sleep(20 * time.Millisecond)

	if h.SessionCount() == 0 {
		t.Error("recently-polled session should not be swept")
	}
	_ = sid // session stays alive until test cleanup closes the handler
}
