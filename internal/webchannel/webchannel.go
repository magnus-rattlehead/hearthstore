package webchannel

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"unicode"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/magnus-rattlehead/hearthstore/internal/config"
)

// Handler serves the Firebase WebChannel transport for the Firestore Listen and
// Write streaming RPCs.
//
// The Firebase JS browser SDK uses XMLHTTP long-poll mode:
//   - Initial POST (no SID): creates session, returns handshake chunk only, closes.
//   - GET with SID: long-poll for buffered response chunks.
//   - Subsequent POST with SID: feeds new requests into the session.
type Handler struct {
	srv      firestoreSrv
	mu       sync.Mutex
	sessions map[string]*session
	ctx      context.Context    // root context; cancelled by Close to stop gcLoop
	cancel   context.CancelFunc
}

type firestoreSrv interface {
	Listen(firestorepb.Firestore_ListenServer) error
	Write(firestorepb.Firestore_WriteServer) error
}

type session struct {
	id          string
	ctx         context.Context
	cancel      context.CancelFunc
	recvCh      chan *firestorepb.ListenRequest // non-nil for Listen sessions
	writeRecvCh chan *firestorepb.WriteRequest  // non-nil for Write sessions

	// Response buffer — guarded by mu.
	// seqNo/chunks grow atomically so data and noop heartbeats never interleave.
	// notify is closed on each append to wake GET pollers without busy-waiting.
	// recvOfs deduplicates the SDK's retransmit-before-ACK: messages with
	// absolute offset < recvOfs have already been delivered and are dropped.
	// lastPollAt drives GC (config.SessionIdleTimeout).
	mu         sync.Mutex
	seqNo      int64
	chunks     [][]byte
	notify     chan struct{}
	done       bool      // true once the RPC goroutine has returned

	recvOfs    int       // next expected incoming message offset
	isWrite    bool      // true for Write stream sessions (noops must not be sent)
	lastPollAt time.Time // last client activity; initialised to creation time
}

// New creates a WebChannel Handler backed by srv and starts the idle-session
// GC background goroutine.
func New(srv firestoreSrv) *Handler {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Handler{
		srv:      srv,
		sessions: make(map[string]*session),
		ctx:      ctx,
		cancel:   cancel,
	}
	go h.gcLoop()
	return h
}

// Close cancels all active sessions and stops the GC goroutine.
// Call this when the server shuts down.
func (h *Handler) Close() {
	h.cancel() // stops gcLoop
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, sess := range h.sessions {
		sess.cancel()
	}
}

// SweepIdleSessions cancels sessions that have not been polled since
// config.SessionIdleTimeout ago. Called automatically by the GC loop and
// exported so tests can trigger a sweep without waiting for the timer.
func (h *Handler) SweepIdleSessions() {
	cutoff := time.Now().Add(-config.SessionIdleTimeout)
	h.mu.Lock()
	var toCancel []context.CancelFunc
	for _, sess := range h.sessions {
		sess.mu.Lock()
		if sess.lastPollAt.Before(cutoff) {
			toCancel = append(toCancel, sess.cancel)
		}
		sess.mu.Unlock()
	}
	h.mu.Unlock()
	// Call cancel outside the lock so the RPC goroutine's deferred cleanup
	// (which acquires h.mu to delete the session) cannot deadlock.
	for _, cancel := range toCancel {
		cancel()
	}
}

// SessionCount returns the number of active sessions. Used in tests.
func (h *Handler) SessionCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.sessions)
}

func (h *Handler) gcLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.SweepIdleSessions()
		}
	}
}

// ServeHTTP routes WebChannel requests for the Listen and Write streaming RPCs.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	isListen := strings.HasSuffix(r.URL.Path, "/Listen/channel")
	isWrite := strings.HasSuffix(r.URL.Path, "/Write/channel")
	if !isListen && !isWrite {
		http.NotFound(w, r)
		return
	}
	setCORSHeaders(w, r)
	hasSID := r.URL.Query().Get("SID") != ""
	switch r.Method {
	case http.MethodOptions:
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPost:
		if hasSID {
			h.handleSubsequentPOST(w, r, isWrite)
		} else if isWrite {
			h.handleWriteInitialPOST(w, r)
		} else {
			h.handleInitialPOST(w, r)
		}
	case http.MethodGet:
		h.handleGETPoll(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleInitialPOST creates a session, writes the handshake chunk, starts the
// Listen goroutine, and returns. All ListenResponse data flows through GET polls.
func (h *Handler) handleInitialPOST(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	rawData := r.FormValue("req0___data__")
	slog.Debug("webchannel: initial POST recv", "content_type", r.Header.Get("Content-Type"), "has_data", rawData != "")

	// req0___data__ may be absent: the Firebase SDK sometimes sends a
	// zero-message initial POST (just count=0) to obtain a SID, then delivers
	// the first AddTarget via a subsequent POST. Handle both forms gracefully.
	var req firestorepb.ListenRequest
	hasInitialReq := false
	if rawData != "" {
		umo := protojson.UnmarshalOptions{DiscardUnknown: true}
		if err := umo.Unmarshal([]byte(rawData), &req); err != nil {
			slog.Warn("webchannel: initial POST parse error", "err", err)
			http.Error(w, "invalid proto: "+err.Error(), http.StatusBadRequest)
			return
		}
		hasInitialReq = true
	}
	if req.Database == "" {
		req.Database = r.URL.Query().Get("database")
	}
	slog.Debug("webchannel: initial POST", "db", req.Database, "target", describeTarget(&req))

	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	sid := newSID()
	slog.Info("webchannel: session created", "sid", sid, "db", req.Database, "target", describeTarget(&req))
	// Session context is independent of the request context: the session must
	// survive after this POST handler returns.
	ctx, cancel := context.WithCancel(context.Background())
	sess := &session{
		id:         sid,
		ctx:        ctx,
		cancel:     cancel,
		recvCh:     make(chan *firestorepb.ListenRequest, 8),
		notify:     make(chan struct{}),
		lastPollAt: time.Now(),
	}
	h.mu.Lock()
	h.sessions[sid] = sess
	h.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	writeHandshake(w, f, sid)
	// Handler returns here — initial POST response is complete (handshake only).

	// Seed the initial request if the initial POST included one (req0___data__
	// present). Advance recvOfs past message 0 so any retransmit is discarded.
	// If the initial POST had no messages (count=0 handshake), leave recvOfs=0
	// so the first subsequent POST (ofs=0) is accepted and delivered.
	if hasInitialReq {
		sess.mu.Lock()
		sess.recvOfs = 1
		sess.mu.Unlock()
		sess.recvCh <- &req
	}
	stream := &listenStream{sess: sess}
	go func() {
		defer func() {
			cancel()
			sess.mu.Lock()
			sess.done = true
			old := sess.notify
			sess.notify = make(chan struct{})
			sess.mu.Unlock()
			close(old) // wake any waiting GET pollers to drain and return

			h.mu.Lock()
			delete(h.sessions, sid)
			h.mu.Unlock()
			slog.Info("webchannel: session closed", "sid", sid)
		}()
		if err := h.srv.Listen(stream); err != nil {
			slog.Warn("webchannel: listen session error", "sid", sid, "err", err)
		}
	}()
}

// handleWriteInitialPOST creates a Write stream session.
func (h *Handler) handleWriteInitialPOST(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	var req firestorepb.WriteRequest
	if rawData := r.FormValue("req0___data__"); rawData != "" {
		umo := protojson.UnmarshalOptions{DiscardUnknown: true}
		if err := umo.Unmarshal([]byte(rawData), &req); err != nil {
			slog.Warn("webchannel: write initial POST parse error", "err", err)
		}
	}
	if req.Database == "" {
		req.Database = r.URL.Query().Get("database")
	}

	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	sid := newSID()
	slog.Info("webchannel: write session created", "sid", sid, "db", req.Database)
	ctx, cancel := context.WithCancel(context.Background())
	sess := &session{
		id:          sid,
		ctx:         ctx,
		cancel:      cancel,
		writeRecvCh: make(chan *firestorepb.WriteRequest, 8),
		isWrite:     true,
		notify:      make(chan struct{}),
		lastPollAt:  time.Now(),
	}
	h.mu.Lock()
	h.sessions[sid] = sess
	h.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	writeHandshake(w, f, sid)

	// Advance recvOfs past the handshake message (ofs=0, req0) so any
	// retransmit of the same offset in a subsequent POST is discarded.
	sess.mu.Lock()
	sess.recvOfs = 1
	sess.mu.Unlock()
	sess.writeRecvCh <- &req
	stream := &writeStream{sess: sess}
	go func() {
		defer func() {
			// Mark done and wake GET pollers so they drain any buffered chunks
			// (including any error chunk appended below) and then return.
			sess.mu.Lock()
			sess.done = true
			old := sess.notify
			sess.notify = make(chan struct{})
			sess.mu.Unlock()
			close(old)

			h.mu.Lock()
			delete(h.sessions, sid)
			h.mu.Unlock()
			slog.Info("webchannel: write session closed", "sid", sid)
			cancel()
		}()
		if err := h.srv.Write(stream); err != nil {
			slog.Warn("webchannel: write session error", "sid", sid, "err", err)
			// Deliver the error as a WebChannel message chunk so the SDK
			// receives the gRPC status (e.g. NOT_FOUND) instead of seeing a
			// silent stream close. Without this, the SDK logs a transport error
			// and retries blindly — preventing "to top" row creation from working
			// when the collection is empty (exists:true precondition on missing doc).
			st, _ := status.FromError(err)
			statusStr := grpcCodeToRESTStatus(st.Code())
			errMsg, _ := json.Marshal(map[string]any{
				"error": map[string]any{
					"message": statusStr + ": " + st.Message(),
					"status":  statusStr,
				},
			})
			appendChunk(sess, []json.RawMessage{json.RawMessage(errMsg)})
		}
	}()
}

// handleGETPoll delivers buffered ListenResponse chunks to the client.
// AID (last acknowledged seqNo, 0 = none) selects where to start.
// If no new chunks are available, the handler long-polls until data arrives,
// the session ends, or the client disconnects.
func (h *Handler) handleGETPoll(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("SID")
	if sid == "" {
		http.Error(w, "missing SID", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	sess := h.sessions[sid]
	h.mu.Unlock()
	if sess == nil {
		// Write sessions are short-lived: the RPC goroutine may finish and delete
		// the session before the SDK's GET poll or subsequent POST arrives. Return
		// a clean empty 200 so the SDK reconnects without exponential backoff, rather
		// than a 400 which triggers the error path with longer retry delays.
		if strings.HasSuffix(r.URL.Path, "/Write/channel") {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "unknown session", http.StatusBadRequest)
		return
	}

	// Mark the session as actively polled for GC purposes.
	sess.mu.Lock()
	sess.lastPollAt = time.Now()
	sess.mu.Unlock()

	// AID = last acknowledged seqNo. chunks are stored at index seqNo-1, so
	// we deliver starting from index AID.
	aid := 0
	if s := r.URL.Query().Get("AID"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			aid = n
		}
	}

	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	// Heartbeat timer: if no chunk arrives within HeartbeatInterval we send a
	// WebChannel noop [[seqNo, []]] so the browser's long-poll does not time
	// out and close the connection.  We use NewTimer+Reset instead of
	// time.After so the timer is reused across loop iterations and does not
	// leak when woken by a real data chunk.
	hb := time.NewTimer(config.HeartbeatInterval)
	defer hb.Stop()

	// deliveredUpTo tracks how many chunks have been sent in this response.
	// Initialized from AID (client's last-acknowledged seqNo) so we resume
	// from the right position on reconnect. The connection stays open after
	// delivering chunks (streaming mode): the browser's WebChannel fires a
	// CLOSE event every time the server ends the GET response, which the
	// Firebase SDK logs as "transport errored: status:1". Streaming mode
	// eliminates those CLOSE events and the associated reconnect overhead.
	deliveredUpTo := aid

	for {
		sess.mu.Lock()
		notify := sess.notify
		nChunks := len(sess.chunks)
		done := sess.done
		sess.mu.Unlock()

		if deliveredUpTo < nChunks {
			// Deliver all new chunks. Reading outside the lock is safe:
			// existing elements are never modified; appends only extend the slice.
			for _, c := range sess.chunks[deliveredUpTo:nChunks] {
				w.Write(c) //nolint:errcheck
			}
			f.Flush()
			slog.Debug("webchannel: GET poll delivered", "sid", sid, "from", deliveredUpTo, "to", nChunks)
			deliveredUpTo = nChunks
		}
		if done {
			return
		}

		select {
		case <-notify:
			// New chunk arrived — reset the heartbeat timer and loop to deliver.
			if !hb.Stop() {
				select {
				case <-hb.C:
				default:
				}
			}
			// Mark the session as actively used so the GC doesn't sweep it
			// while this streaming GET connection is open and delivering data.
			sess.mu.Lock()
			sess.lastPollAt = time.Now()
			sess.mu.Unlock()
			hb.Reset(config.HeartbeatInterval)
		case <-r.Context().Done():
			return
		case <-sess.ctx.Done():
			return
		case <-hb.C:
			// Idle too long — for Listen sessions, append a noop so the
			// response is non-empty and the browser's long-poll does not time
			// out. Write sessions must NOT receive noops: the Write stream's
			// onMessage handler asserts every message has non-empty proto data
			// (msg[1][0] != undefined); an empty message array [[seqNo,[]]]
			// reaches that assertion and triggers "Unexpected state".
			//
			// Also refresh lastPollAt so the GC does not sweep a session whose
			// streaming GET is open but quiescent (no writes → no notify wakes).
			sess.mu.Lock()
			sess.lastPollAt = time.Now()
			sess.mu.Unlock()
			if !sess.isWrite {
				appendNoop(sess)
			}
			hb.Reset(config.HeartbeatInterval)
		}
	}
}

// handleSubsequentPOST feeds new requests into an existing session.
func (h *Handler) handleSubsequentPOST(w http.ResponseWriter, r *http.Request, isWrite bool) {
	sid := r.URL.Query().Get("SID")

	// TYPE=terminate is a WebChannel teardown signal. Don't cancel the session;
	// the browser closes its GET XHR, which fires r.Context().Done() naturally.
	if r.URL.Query().Get("TYPE") == "terminate" {
		w.WriteHeader(http.StatusOK)
		return
	}

	h.mu.Lock()
	sess := h.sessions[sid]
	h.mu.Unlock()
	if sess == nil {
		// Write sessions close when the RPC exits; the SDK may POST before processing
		// the close. Acknowledge silently so it opens a new stream rather than a 400.
		if isWrite {
			ack, _ := json.Marshal([]any{1, 0, 7})
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			fmt.Fprintf(w, "%d\n", len(ack))
			w.Write(ack) //nolint:errcheck
			return
		}
		slog.Warn("webchannel: subsequent POST for unknown session", "sid", sid)
		http.Error(w, "unknown session", http.StatusBadRequest)
		return
	}

	// Mark the session as active for GC purposes.
	sess.mu.Lock()
	sess.lastPollAt = time.Now()
	sess.mu.Unlock()

	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	postOfs := 0
	if s := r.FormValue("ofs"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			postOfs = n
		}
	}
	for i := 0; ; i++ {
		rawData := r.FormValue(fmt.Sprintf("req%d___data__", i))
		if rawData == "" {
			break
		}
		msgOfs := postOfs + i

		// Deduplicate: the SDK retransmits with the same ofs before receiving the ACK.
		sess.mu.Lock()
		alreadySeen := msgOfs < sess.recvOfs
		if !alreadySeen {
			sess.recvOfs = msgOfs + 1
		}
		sess.mu.Unlock()
		if alreadySeen {
			slog.Debug("webchannel: discarding duplicate message", "sid", sid, "msg_ofs", msgOfs)
			continue
		}

		umo := protojson.UnmarshalOptions{DiscardUnknown: true}
		if isWrite {
			var req firestorepb.WriteRequest
			if err := umo.Unmarshal([]byte(rawData), &req); err != nil {
				slog.Warn("webchannel: write subsequent POST parse error", "sid", sid, "i", i, "err", err)
				continue
			}
			select {
			case sess.writeRecvCh <- &req:
			case <-sess.ctx.Done():
			}
		} else {
			var req firestorepb.ListenRequest
			if err := umo.Unmarshal([]byte(rawData), &req); err != nil {
				slog.Warn("webchannel: subsequent POST parse error", "sid", sid, "i", i, "err", err)
				continue
			}
			slog.Info("webchannel: subsequent POST", "sid", sid, "target", describeTarget(&req))
			select {
			case sess.recvCh <- &req:
			case <-sess.ctx.Done():
			}
		}
	}

	// Return a framed WebChannel backchannel ACK: "<len>\n[1, lastSeqNo, 7]"
	// The SDK's uc() parser requires the "<len>\n" framing prefix — without it,
	// it returns fc (incomplete) and retransmits the messages indefinitely.
	// e[0]=1 ("has outstanding data") keeps the existing back-channel GET alive;
	// e[0]=0 would call zc() to cancel the GET whenever it is >3s old.
	// e[1]=lastSeqNo tells the SDK how far the server's outgoing stream has reached.
	// e[2]=7 is the protocol version (matches real Firebase emulator).
	sess.mu.Lock()
	lastSeqNo := sess.seqNo
	sess.mu.Unlock()
	ack, _ := json.Marshal([]any{1, lastSeqNo, 7})
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "%d\n", len(ack))
	w.Write(ack) //nolint:errcheck
}



// listenStream bridges the session buffer ↔ firestorepb.Firestore_ListenServer.
// Send appends each response as a WebChannel chunk to the session buffer.
// Recv blocks on the session's receive channel fed by handleSubsequentPOST.
type listenStream struct {
	sess *session
}

// Send encodes resp as proto3 JSON, frames it as a WebChannel chunk, and appends
// it to the session buffer. Waiting GET pollers are notified via the notify channel.
// Format: <decimal_byte_count>\n[[seqNo, [<proto3JSON>]]]
func (s *listenStream) Send(resp *firestorepb.ListenResponse) error {
	b, err := protojson.Marshal(resp)
	if err != nil {
		return err
	}
	n := appendChunk(s.sess, []json.RawMessage{b})
	slog.Debug("webchannel: Send", "sid", s.sess.id, "seqNo", n, "bytes", len(b))
	return nil
}

// Recv blocks until a ListenRequest is available or the session context is cancelled.
func (s *listenStream) Recv() (*firestorepb.ListenRequest, error) {
	select {
	case req, ok := <-s.sess.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return req, nil
	case <-s.sess.ctx.Done():
		return nil, io.EOF
	}
}

func (s *listenStream) Context() context.Context     { return s.sess.ctx }
func (s *listenStream) SetHeader(metadata.MD) error  { return nil }
func (s *listenStream) SendHeader(metadata.MD) error { return nil }
func (s *listenStream) SetTrailer(metadata.MD)        {}

func (s *listenStream) SendMsg(m any) error {
	if resp, ok := m.(*firestorepb.ListenResponse); ok {
		return s.Send(resp)
	}
	return nil
}

func (s *listenStream) RecvMsg(m any) error {
	req, err := s.Recv()
	if err != nil {
		return err
	}
	if dst, ok := m.(*firestorepb.ListenRequest); ok {
		proto.Merge(dst, req)
	}
	return nil
}

type writeStream struct {
	sess *session
}

func (s *writeStream) Send(resp *firestorepb.WriteResponse) error {
	b, err := protojson.Marshal(resp)
	if err != nil {
		return err
	}
	appendChunk(s.sess, []json.RawMessage{b})
	return nil
}

func (s *writeStream) Recv() (*firestorepb.WriteRequest, error) {
	select {
	case req, ok := <-s.sess.writeRecvCh:
		if !ok {
			return nil, io.EOF
		}
		return req, nil
	case <-s.sess.ctx.Done():
		return nil, io.EOF
	}
}

func (s *writeStream) Context() context.Context     { return s.sess.ctx }
func (s *writeStream) SetHeader(metadata.MD) error  { return nil }
func (s *writeStream) SendHeader(metadata.MD) error { return nil }
func (s *writeStream) SetTrailer(metadata.MD)        {}

func (s *writeStream) SendMsg(m any) error {
	if resp, ok := m.(*firestorepb.WriteResponse); ok {
		return s.Send(resp)
	}
	return nil
}

func (s *writeStream) RecvMsg(m any) error {
	req, err := s.Recv()
	if err != nil {
		return err
	}
	if dst, ok := m.(*firestorepb.WriteRequest); ok {
		proto.Merge(dst, req)
	}
	return nil
}


// writeHandshake sends the WebChannel session-open frame to the client.
// Format: [[0, ["c", "<SID>", "", 8, 12, 30000]]]
// The magic numbers match the real Firebase emulator:
//   - 8     = VER (client version hint)
//   - 12    = SVER (server version)
//   - 30000 = backChannelRequestTimeoutMs base (SDK multiplies by 1.5 → 45s GET timeout)
func writeHandshake(w http.ResponseWriter, f http.Flusher, sid string) {
	handshake, _ := json.Marshal([]any{[]any{0, []any{"c", sid, "", 8, 12, 30000}}})
	fmt.Fprintf(w, "%d\n", len(handshake))
	w.Write(handshake) //nolint:errcheck
	f.Flush()
}

// appendChunk increments sess.seqNo, builds the WebChannel frame
// <len>\n[[seqNo, msgs]], appends it to sess.chunks, and wakes GET pollers.
// It must be called with sess.mu NOT held; it acquires the lock internally.
// Returns the assigned sequence number.
func appendChunk(sess *session, msgs []json.RawMessage) int64 {
	sess.mu.Lock()
	sess.seqNo++
	n := sess.seqNo
	payload, _ := json.Marshal([]any{[]any{n, msgs}})
	chunk := append([]byte(fmt.Sprintf("%d\n", len(payload))), payload...)
	sess.chunks = append(sess.chunks, chunk)
	old := sess.notify
	sess.notify = make(chan struct{})
	sess.mu.Unlock()
	close(old)
	return n
}

// appendNoop appends a WebChannel noop frame [[seqNo, ["noop"]]].
// The client checks m[0] === "noop" and skips the message (no event dispatched).
func appendNoop(sess *session) {
	appendChunk(sess, []json.RawMessage{json.RawMessage(`"noop"`)})
}


// describeTarget returns a short human-readable description of the target in a
// ListenRequest for use in log messages.
func describeTarget(req *firestorepb.ListenRequest) string {
	if req == nil {
		return "nil"
	}
	switch tc := req.TargetChange.(type) {
	case *firestorepb.ListenRequest_AddTarget:
		t := tc.AddTarget
		switch tt := t.TargetType.(type) {
		case *firestorepb.Target_Query:
			q := tt.Query
			if q.GetStructuredQuery() != nil && len(q.GetStructuredQuery().From) > 0 {
				parent := q.GetParent()
				if i := strings.Index(parent, "/documents/"); i >= 0 {
					parent = parent[i+len("/documents/"):]
				} else {
					parent = ""
				}
				coll := q.GetStructuredQuery().From[0].CollectionId
				if parent != "" {
					return fmt.Sprintf("AddTarget(query, id=%d, parent=%s, collection=%s)", t.TargetId, parent, coll)
				}
				return fmt.Sprintf("AddTarget(query, id=%d, collection=%s)", t.TargetId, coll)
			}
			return fmt.Sprintf("AddTarget(query, id=%d)", t.TargetId)
		case *firestorepb.Target_Documents:
			first := ""
			if len(tt.Documents.Documents) > 0 {
				name := tt.Documents.Documents[0]
				// show only the path after /documents/
				if i := strings.Index(name, "/documents/"); i >= 0 {
					first = " path=" + name[i+len("/documents/"):]
				} else {
					first = " path=" + name
				}
			}
			return fmt.Sprintf("AddTarget(docs, id=%d, n=%d%s)", t.TargetId, len(tt.Documents.Documents), first)
		}
		return fmt.Sprintf("AddTarget(id=%d)", t.TargetId)
	case *firestorepb.ListenRequest_RemoveTarget:
		return fmt.Sprintf("RemoveTarget(id=%d)", tc.RemoveTarget)
	}
	return "unknown"
}

// setCORSHeaders sets CORS headers required by the Firebase JS SDK.
// The SDK sends credentials, so we must reflect the specific Origin rather than "*".
func setCORSHeaders(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		origin = "*"
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Goog-Api-Client, Authorization, X-Firebase-Client")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
}


// grpcCodeToRESTStatus converts a gRPC status code to the Firestore REST API
// error status format (UPPER_SNAKE_CASE). The Firebase JS SDK parses the status
// field by lowercasing and replacing underscores with dashes, so NOT_FOUND
// becomes "not-found" — matching the SDK's internal error code enum.
//
// gRPC codes use CamelCase (e.g. "NotFound"); we insert underscores before
// each uppercase letter that follows a lowercase letter, then uppercase all.
func grpcCodeToRESTStatus(c codes.Code) string {
	s := c.String()
	var out []rune
	runes := []rune(s)
	for i, r := range runes {
		if i > 0 && unicode.IsUpper(r) && unicode.IsLower(runes[i-1]) {
			out = append(out, '_')
		}
		out = append(out, unicode.ToUpper(r))
	}
	return string(out)
}

func newSID() string {
	b := make([]byte, 8)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}
