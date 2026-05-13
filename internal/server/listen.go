package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/magnus-rattlehead/hearthstore/internal/config"
	"github.com/magnus-rattlehead/hearthstore/internal/query"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// resumeTokenPayload is the JSON envelope stored in every resume token.
// Encoding is JSON for human readability.
type resumeTokenPayload struct {
	Seq      int64  `json:"s"`
	StreamID string `json:"i"`
}

// encodeResumeToken serialises a (streamID, seq) pair as an opaque token.
func encodeResumeToken(streamID string, seq int64) []byte {
	b, _ := json.Marshal(resumeTokenPayload{Seq: seq, StreamID: streamID})
	return b
}

// decodeResumeToken is the inverse of encodeResumeToken.
// Old proto-encoded tokens (or any malformed bytes) return ok=false.
func decodeResumeToken(token []byte) (streamID string, seq int64, ok bool) {
	var p resumeTokenPayload
	if err := json.Unmarshal(token, &p); err != nil {
		return "", 0, false
	}
	return p.StreamID, p.Seq, true
}

// tokenState describes how to handle an incoming resume token.
type tokenState int

const (
	tokenNone      tokenState = iota // no ResumeType → full initial snapshot
	tokenValidSeq                    // same stream, seq-based → delta via change log
	tokenValidTime                   // read_time → delta via timestamp (GetDocsSince)
	tokenStale                       // different stream or undecodable → full snapshot
)

// tokenStateForTarget classifies the resume type of an AddTarget request.
// Returns (state, sinceSeq, sinceTime, oldStreamID); oldStreamID is non-empty only
// for tokenStale and holds the streamID encoded in the stale token.
func tokenStateForTarget(target *firestorepb.Target, streamID string) (tokenState, int64, *timestamppb.Timestamp, string) {
	switch rt := target.ResumeType.(type) {
	case *firestorepb.Target_ResumeToken:
		sid, seq, ok := decodeResumeToken(rt.ResumeToken)
		if !ok {
			return tokenStale, 0, nil, ""
		}
		if sid != streamID {
			return tokenStale, 0, nil, sid
		}
		return tokenValidSeq, seq, nil, ""
	case *firestorepb.Target_ReadTime:
		return tokenValidTime, 0, rt.ReadTime, ""
	}
	return tokenNone, 0, nil, ""
}

// watchTarget tracks a single AddTarget from the client.
type watchTarget struct {
	id       int32
	project  string
	database string
	parent   string // collection parent path (empty = root)
	query    *firestorepb.StructuredQuery // nil = documents target
	docNames map[string]bool             // set for documents targets
	sent     map[string]bool             // doc names already sent to client
	once     bool                        // auto-remove after initial snapshot
}

// Listen implements the bidi streaming real-time watch protocol.
//
// Protocol per message received:
//   - AddTarget   → send ADD ack, stream initial snapshot, send CURRENT + NO_CHANGE
//   - RemoveTarget → send REMOVE ack, clean up target
//
// Change events from the store trigger DocumentChange / DocumentDelete / DocumentRemove
// for each registered target, followed by a NO_CHANGE heartbeat.
func (s *Server) Listen(stream firestorepb.Firestore_ListenServer) error {
	streamID := newStreamID()
	subID, events := s.store.Subscribe()
	defer s.store.Unsubscribe(subID)
	// scopes are registered lazily via UpdateScopes in handleListenRequest.

	s.streamsMu.Lock()
	s.streams[streamID] = make(map[int32]*watchTarget)
	s.streamsMu.Unlock()
	defer func() {
		s.streamsMu.Lock()
		delete(s.streams, streamID)
		s.streamsMu.Unlock()
	}()

	targets := make(map[int32]*watchTarget)

	// Periodic keep-alive: prevents idle long-poll connections from being dropped
	// by the browser or intermediate proxies before the next real event arrives.
	hb := time.NewTicker(config.HeartbeatInterval)
	defer hb.Stop()

	// Read incoming requests in a goroutine so we can select on both channels.
	type recvResult struct {
		req *firestorepb.ListenRequest
		err error
	}
	reqCh := make(chan recvResult, 8)
	go func() {
		for {
			req, err := stream.Recv()
			reqCh <- recvResult{req, err}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case rr := <-reqCh:
			if rr.err == io.EOF {
				return nil
			}
			if rr.err != nil {
				return rr.err
			}
			if err := s.handleListenRequest(stream, targets, rr.req, streamID, subID); err != nil {
				return err
			}

		case ev, ok := <-events:
			if !ok {
				return nil // store closed
			}
			if err := s.dispatchChangeEvent(stream, targets, ev, streamID); err != nil {
				return err
			}

		case <-hb.C:
			// Global NO_CHANGE with no TargetIds = keep-alive heartbeat.
			// The SDK ignores this unless it has pending changes, so it is safe to
			// send at any time and does not trigger spurious onSnapshot callbacks.
			if err := stream.Send(&firestorepb.ListenResponse{
				ResponseType: &firestorepb.ListenResponse_TargetChange{
					TargetChange: &firestorepb.TargetChange{
						TargetChangeType: firestorepb.TargetChange_NO_CHANGE,
						ReadTime:         timestamppb.Now(),
					},
				},
			}); err != nil {
				return err
			}
		}
	}
}

// handleListenRequest processes a single ListenRequest (AddTarget or RemoveTarget).
func (s *Server) handleListenRequest(
	stream firestorepb.Firestore_ListenServer,
	targets map[int32]*watchTarget,
	req *firestorepb.ListenRequest,
	streamID string,
	subID uint64,
) error {
	switch tc := req.TargetChange.(type) {
	case *firestorepb.ListenRequest_AddTarget:
		t, err := s.parseWatchTarget(req.Database, tc.AddTarget)
		if err != nil {
			return err
		}
		targets[t.id] = t
		s.streamsMu.Lock()
		if sm := s.streams[streamID]; sm != nil {
			sm[t.id] = t
		}
		s.streamsMu.Unlock()
		s.store.UpdateScopes(subID, subScopesFromTargets(targets))

		// Always acknowledge first.
		if err := sendTargetAdd(stream, t.id); err != nil {
			return err
		}

		state, sinceSeq, sinceTime, oldStreamID := tokenStateForTarget(tc.AddTarget, streamID)

		switch state {
		case tokenStale:
			// Different stream: full snapshot + forward any supplementary targets the
			// SDK sent to the old session concurrently with creating this new session.
			slog.Debug("listen: stale token → full snapshot", "stream", streamID, "target", t.id, "project", t.project, "db", t.database, "parent", t.parent)
			if err := s.sendInitialSnapshot(stream, t); err != nil {
				return err
			}
			if err := s.forwardOldStreamTargets(stream, targets, streamID, oldStreamID); err != nil {
				return err
			}
		case tokenValidSeq:
			slog.Debug("listen: seq token → diff snapshot", "stream", streamID, "target", t.id, "since_seq", sinceSeq)
			if err := s.sendDiffSnapshot(stream, t, sinceSeq); err != nil {
				return err
			}
		case tokenValidTime:
			slog.Debug("listen: read_time token → diff snapshot by time", "stream", streamID, "target", t.id)
			if err := s.sendDiffSnapshotByTime(stream, t, sinceTime); err != nil {
				return err
			}
		default: // tokenNone
			slog.Debug("listen: no token → initial snapshot", "stream", streamID, "target", t.id, "project", t.project, "db", t.database, "parent", t.parent)
			if err := s.sendInitialSnapshot(stream, t); err != nil {
				return err
			}
		}

		// Issue CURRENT with a token anchored to the latest seq.
		resumeToken, now := s.resumeTokenForTarget(t, streamID)
		if err := sendTargetCurrent(stream, t.id, resumeToken, now); err != nil {
			return err
		}

		// Send NO_CHANGE to signal a consistent view.
		if err := stream.Send(noChange(resumeToken, now)); err != nil {
			return err
		}

		// If once=true, auto-remove the target after the initial snapshot.
		if t.once {
			delete(targets, t.id)
			s.store.UpdateScopes(subID, subScopesFromTargets(targets))
			return sendTargetRemove(stream, t.id)
		}
		return nil

	case *firestorepb.ListenRequest_RemoveTarget:
		tid := tc.RemoveTarget
		if _, exists := targets[tid]; !exists {
			return nil // target not active; ignore duplicate or unknown remove
		}
		delete(targets, tid)
		s.store.UpdateScopes(subID, subScopesFromTargets(targets))
		s.streamsMu.Lock()
		if sm := s.streams[streamID]; sm != nil {
			delete(sm, tid)
		}
		s.streamsMu.Unlock()
		return sendTargetRemove(stream, tid)
	}
	return nil
}

// parseWatchTarget extracts a watchTarget from an AddTarget request.
func (s *Server) parseWatchTarget(database string, t *firestorepb.Target) (*watchTarget, error) {
	wt := &watchTarget{
		id:   t.TargetId,
		sent: make(map[string]bool),
		once: t.GetOnce(),
	}

	switch tt := t.TargetType.(type) {
	case *firestorepb.Target_Query:
		qt := tt.Query
		project, db, parent, err := parseParent(qt.Parent)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid target parent: %v", err)
		}
		wt.project = project
		wt.database = db
		wt.parent = parent
		wt.query = qt.GetStructuredQuery()

	case *firestorepb.Target_Documents:
		dt := tt.Documents
		wt.docNames = make(map[string]bool, len(dt.Documents))
		for _, name := range dt.Documents {
			project, db, _, err := parseName(name)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "invalid document name: %v", err)
			}
			if wt.project == "" {
				wt.project = project
				wt.database = db
			}
			wt.docNames[name] = true
		}

	default:
		if database != "" {
			// derive from database string if possible
			project, db, _, _ := parseParent(database + "/documents")
			wt.project = project
			wt.database = db
		}
	}
	return wt, nil
}

// sendInitialSnapshot fetches and streams all matching documents for target t.
func (s *Server) sendInitialSnapshot(stream firestorepb.Firestore_ListenServer, t *watchTarget) error {
	docs, err := s.fetchTargetDocs(t)
	if err != nil {
		return err
	}
	slog.Debug("listen: sendInitialSnapshot", "target", t.id, "docs", len(docs), "parent", t.parent)
	for _, doc := range docs {
		if err := sendDocChange(stream, t.id, doc); err != nil {
			return err
		}
		t.sent[doc.Name] = true
	}
	return nil
}

// sendDiffSnapshot sends only the documents that changed after sinceSeq, using the
// append-only change log. It also repopulates t.sent with all currently live docs
// so future dispatchChangeEvent calls correctly track the client's known set.
func (s *Server) sendDiffSnapshot(
	stream firestorepb.Firestore_ListenServer,
	t *watchTarget,
	sinceSeq int64,
) error {
	now := timestamppb.Now()

	if t.query != nil {
		for _, sel := range t.query.GetFrom() {
			changed, err := s.store.GetChangesSince(t.project, t.database, sinceSeq, t.parent, sel.CollectionId, sel.AllDescendants)
			if err != nil {
				return status.Errorf(codes.Internal, "get changes since: %v", err)
			}
			for _, c := range changed {
				if c.Deleted {
					if err := sendDocDelete(stream, t.id, c.Name, now); err != nil {
						return err
					}
					continue
				}
				if s.docMatchesTarget(c.Doc, t) {
					if err := sendDocChange(stream, t.id, c.Doc); err != nil {
						return err
					}
				}
			}
		}
	} else if t.docNames != nil {
		for name := range t.docNames {
			_, _, path, err := parseName(name)
			if err != nil {
				continue
			}
			c, err := s.store.GetDocChangeSince(t.project, t.database, path, sinceSeq)
			if err != nil || c == nil {
				continue
			}
			if c.Deleted {
				if err := sendDocDelete(stream, t.id, name, now); err != nil {
					return err
				}
			} else {
				if err := sendDocChange(stream, t.id, c.Doc); err != nil {
					return err
				}
			}
		}
	}

	// Repopulate t.sent with all currently live docs so dispatchChangeEvent can
	// correctly detect when a doc leaves the result set after this reconnect.
	docs, err := s.fetchTargetDocs(t)
	if err != nil {
		return err
	}
	for _, doc := range docs {
		t.sent[doc.Name] = true
	}
	return nil
}

// sendDiffSnapshotByTime sends only documents changed after since (timestamp path,
// used for AddTarget with read_time). This preserves the existing timestamp-based
// behaviour for clients that supply read_time rather than a resume_token.
func (s *Server) sendDiffSnapshotByTime(
	stream firestorepb.Firestore_ListenServer,
	t *watchTarget,
	since *timestamppb.Timestamp,
) error {
	sinceTime := since.AsTime()

	if t.query != nil {
		for _, sel := range t.query.GetFrom() {
			changed, err := s.store.GetDocsSince(t.project, t.database, sinceTime, t.parent, sel.CollectionId, sel.AllDescendants)
			if err != nil {
				return status.Errorf(codes.Internal, "get docs since: %v", err)
			}
			now := timestamppb.Now()
			for _, c := range changed {
				if c.Deleted {
					if err := sendDocDelete(stream, t.id, c.Name, now); err != nil {
						return err
					}
					continue
				}
				if s.docMatchesTarget(c.Doc, t) {
					if err := sendDocChange(stream, t.id, c.Doc); err != nil {
						return err
					}
				}
			}
		}
	}

	// Repopulate t.sent with all currently live docs.
	docs, err := s.fetchTargetDocs(t)
	if err != nil {
		return err
	}
	for _, doc := range docs {
		if t.docNames != nil {
			// Documents targets: re-stream docs since we have no per-doc timestamp diff.
			if err := sendDocChange(stream, t.id, doc); err != nil {
				return err
			}
		}
		t.sent[doc.Name] = true
	}
	return nil
}

// fetchTargetDocs returns all documents matching the target (query or explicit set).
func (s *Server) fetchTargetDocs(t *watchTarget) ([]*firestorepb.Document, error) {
	if t.docNames != nil {
		// Documents target: fetch each by name.
		var docs []*firestorepb.Document
		for name := range t.docNames {
			_, _, path, err := parseName(name)
			if err != nil {
				continue
			}
			doc, err := s.store.GetDoc(t.project, t.database, path)
			if err != nil {
				continue // missing → not included in snapshot
			}
			docs = append(docs, doc)
		}
		return docs, nil
	}

	// Query target: execute via the full query path so that OrderBy, Limit,
	// Offset, and cursors are respected (needed for limitToLast, pagination, etc.).
	q := t.query
	if q == nil {
		return nil, nil
	}
	var docs []*firestorepb.Document
	for _, sel := range q.GetFrom() {
		selQ := &firestorepb.StructuredQuery{
			From:    []*firestorepb.StructuredQuery_CollectionSelector{sel},
			Where:   q.Where,
			OrderBy: q.OrderBy,
			StartAt: q.StartAt,
			EndAt:   q.EndAt,
			Offset:  q.Offset,
			Limit:   q.Limit,
		}
		result, err := query.Build(t.project, t.database, t.parent, sel.AllDescendants, selQ)
		if err != nil {
			return nil, err
		}
		err = s.store.QueryDocs(result.SQL, result.Args, func(doc *firestorepb.Document) error {
			if result.NeedsGoFilter && !matchesFilter(doc, q.Where) {
				return nil
			}
			docs = append(docs, doc)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	// Sort using the effective ORDER BY (explicit fields plus implicit inequality
	// fields sorted lexicographically). This ensures snapshot listeners return docs
	// in the same order as query.get() for cross-type and multiple-inequality cases.
	if effectiveOrders := query.AddImplicitOrderBy(q.OrderBy, q.Where); len(effectiveOrders) > 0 {
		sortDocsByOrderBy(docs, effectiveOrders)
	}
	return docs, nil
}

// dispatchChangeEvent routes a storage ChangeEvent to all registered targets.
func (s *Server) dispatchChangeEvent(
	stream firestorepb.Firestore_ListenServer,
	targets map[int32]*watchTarget,
	ev storage.ChangeEvent,
	streamID string,
) error {
	now := timestamppb.Now()

	slog.Debug("dispatchChangeEvent", "name", ev.Name, "deleted", ev.Deleted, "seq", ev.Seq, "targets", len(targets))
	for _, t := range targets {
		wasSent := t.sent[ev.Name]

		if ev.Deleted {
			if wasSent {
				slog.Debug("dispatchChangeEvent: DocumentDelete", "name", ev.Name, "target", t.id)
				if err := sendDocDelete(stream, t.id, ev.Name, now); err != nil {
					return err
				}
				delete(t.sent, ev.Name)
			}
			continue
		}

		matchesNow := s.docMatchesTarget(ev.Doc, t)
		switch {
		case matchesNow:
			// Send DocumentChange whether new or updated.
			slog.Debug("dispatchChangeEvent: DocumentChange", "name", ev.Name, "target", t.id, "was_sent", wasSent, "doc_update_time", ev.Doc.GetUpdateTime().AsTime())
			if err := sendDocChange(stream, t.id, ev.Doc); err != nil {
				return err
			}
			t.sent[ev.Name] = true

		case !matchesNow && wasSent:
			// Document left the result set.
			slog.Debug("dispatchChangeEvent: DocumentRemove", "name", ev.Name, "target", t.id)
			if err := sendDocRemove(stream, t.id, ev.Name, now); err != nil {
				return err
			}
			delete(t.sent, ev.Name)
		}
	}

	// Heartbeat after every change batch (only when at least one target is active).
	if len(targets) == 0 {
		return nil
	}
	resumeToken := encodeResumeToken(streamID, ev.Seq)
	return stream.Send(noChange(resumeToken, now))
}

// subScopesFromTarget extracts storage.SubScope entries from a watchTarget.
func subScopesFromTarget(t *watchTarget) []storage.SubScope {
	if t.docNames != nil {
		scopes := make([]storage.SubScope, 0, len(t.docNames))
		for name := range t.docNames {
			_, _, path, _ := parseName(name)
			collection, parentPath, _ := splitDocPath(path)
			scopes = append(scopes, storage.SubScope{
				Project: t.project, Database: t.database,
				ParentPath: parentPath, Collection: collection,
			})
		}
		return scopes
	}
	if t.query == nil {
		return nil
	}
	from := t.query.GetFrom()
	scopes := make([]storage.SubScope, 0, len(from))
	for _, sel := range from {
		scopes = append(scopes, storage.SubScope{
			Project: t.project, Database: t.database,
			ParentPath: t.parent, Collection: sel.CollectionId,
			AllDescendants: sel.AllDescendants,
		})
	}
	return scopes
}

// subScopesFromTargets aggregates scopes from all active targets.
func subScopesFromTargets(targets map[int32]*watchTarget) []storage.SubScope {
	var all []storage.SubScope
	for _, t := range targets {
		all = append(all, subScopesFromTarget(t)...)
	}
	return all
}

// docMatchesTarget reports whether doc belongs to the watch target's result set.
func (s *Server) docMatchesTarget(doc *firestorepb.Document, t *watchTarget) bool {
	if doc == nil {
		return false
	}

	// Documents target: check explicit name set.
	if t.docNames != nil {
		return t.docNames[doc.Name]
	}

	// Query target: check collection + filter.
	q := t.query
	if q == nil {
		return false
	}
	_, _, docPath, err := parseName(doc.Name)
	if err != nil {
		return false
	}
	collection, parentPath, _ := splitDocPath(docPath)
	for _, sel := range q.GetFrom() {
		if sel.AllDescendants {
			if collection != sel.CollectionId {
				continue
			}
		} else {
			if collection != sel.CollectionId || parentPath != t.parent {
				continue
			}
		}
		if q.GetWhere() != nil && !matchesFilter(doc, q.GetWhere()) {
			return false
		}
		return true
	}
	return false
}

// forwardOldStreamTargets forwards targets that were active on oldStreamID but are not
// yet registered in the new session. Called in the tokenStale path: the SDK creates a
// new WebChannel session for the main target but concurrently sends supplementary targets
// (e.g. ratings) to the OLD session; we mirror them so the new session delivers all data.
func (s *Server) forwardOldStreamTargets(
	stream firestorepb.Firestore_ListenServer,
	targets map[int32]*watchTarget,
	newStreamID, oldStreamID string,
) error {
	if oldStreamID == "" {
		return nil
	}

	s.streamsMu.Lock()
	oldTargets := s.streams[oldStreamID]
	var toForward []*watchTarget
	for id, ot := range oldTargets {
		if _, already := targets[id]; !already {
			clone := &watchTarget{
				id:       ot.id,
				project:  ot.project,
				database: ot.database,
				parent:   ot.parent,
				query:    ot.query,
				docNames: ot.docNames,
				sent:     make(map[string]bool),
				once:     ot.once,
			}
			toForward = append(toForward, clone)
		}
	}
	s.streamsMu.Unlock()

	for _, ft := range toForward {
		slog.Debug("listen: forwarding target from old stream",
			"old_stream", oldStreamID, "new_stream", newStreamID, "target", ft.id)

		targets[ft.id] = ft
		s.streamsMu.Lock()
		if sm := s.streams[newStreamID]; sm != nil {
			sm[ft.id] = ft
		}
		s.streamsMu.Unlock()

		if err := sendTargetAdd(stream, ft.id); err != nil {
			return err
		}
		if err := s.sendInitialSnapshot(stream, ft); err != nil {
			return err
		}
		tok, now := s.resumeTokenForTarget(ft, newStreamID)
		if err := sendTargetCurrent(stream, ft.id, tok, now); err != nil {
			return err
		}
		if err := stream.Send(noChange(tok, now)); err != nil {
			return err
		}
	}
	return nil
}

func sendDocChange(stream firestorepb.Firestore_ListenServer, targetID int32, doc *firestorepb.Document) error {
	return stream.Send(&firestorepb.ListenResponse{
		ResponseType: &firestorepb.ListenResponse_DocumentChange{
			DocumentChange: &firestorepb.DocumentChange{
				Document:  doc,
				TargetIds: []int32{targetID},
			},
		},
	})
}

func sendDocDelete(stream firestorepb.Firestore_ListenServer, targetID int32, name string, now *timestamppb.Timestamp) error {
	return stream.Send(&firestorepb.ListenResponse{
		ResponseType: &firestorepb.ListenResponse_DocumentDelete{
			DocumentDelete: &firestorepb.DocumentDelete{
				Document:         name,
				RemovedTargetIds: []int32{targetID},
				ReadTime:         now,
			},
		},
	})
}

func sendDocRemove(stream firestorepb.Firestore_ListenServer, targetID int32, name string, now *timestamppb.Timestamp) error {
	return stream.Send(&firestorepb.ListenResponse{
		ResponseType: &firestorepb.ListenResponse_DocumentRemove{
			DocumentRemove: &firestorepb.DocumentRemove{
				Document:         name,
				RemovedTargetIds: []int32{targetID},
				ReadTime:         now,
			},
		},
	})
}

// noChange builds a NO_CHANGE TargetChange heartbeat.
func noChange(resumeToken []byte, readTime *timestamppb.Timestamp) *firestorepb.ListenResponse {
	return &firestorepb.ListenResponse{
		ResponseType: &firestorepb.ListenResponse_TargetChange{
			TargetChange: &firestorepb.TargetChange{
				TargetChangeType: firestorepb.TargetChange_NO_CHANGE,
				ResumeToken:      resumeToken,
				ReadTime:         readTime,
			},
		},
	}
}

func (s *Server) resumeTokenForTarget(t *watchTarget, streamID string) ([]byte, *timestamppb.Timestamp) {
	seq, _ := s.store.CurrentSeq(t.project, t.database)
	return encodeResumeToken(streamID, seq), timestamppb.Now()
}

func sendTargetAdd(stream firestorepb.Firestore_ListenServer, targetID int32) error {
	return stream.Send(&firestorepb.ListenResponse{
		ResponseType: &firestorepb.ListenResponse_TargetChange{
			TargetChange: &firestorepb.TargetChange{
				TargetChangeType: firestorepb.TargetChange_ADD,
				TargetIds:        []int32{targetID},
			},
		},
	})
}

func sendTargetCurrent(stream firestorepb.Firestore_ListenServer, targetID int32, token []byte, readTime *timestamppb.Timestamp) error {
	return stream.Send(&firestorepb.ListenResponse{
		ResponseType: &firestorepb.ListenResponse_TargetChange{
			TargetChange: &firestorepb.TargetChange{
				TargetChangeType: firestorepb.TargetChange_CURRENT,
				TargetIds:        []int32{targetID},
				ResumeToken:      token,
				ReadTime:         readTime,
			},
		},
	})
}

func sendTargetRemove(stream firestorepb.Firestore_ListenServer, targetID int32) error {
	return stream.Send(&firestorepb.ListenResponse{
		ResponseType: &firestorepb.ListenResponse_TargetChange{
			TargetChange: &firestorepb.TargetChange{
				TargetChangeType: firestorepb.TargetChange_REMOVE,
				TargetIds:        []int32{targetID},
			},
		},
	})
}
