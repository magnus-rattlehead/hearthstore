package server

// RESTHandler translates Firestore REST API requests (used by the Firebase JS
// browser SDK) to the existing gRPC handler methods.
//
// The browser SDK sends:
//   - Content-Type: text/plain  (avoids CORS preflight for simple requests)
//   - Body: proto3 JSON
//   - URL: /v1/projects/{p}/databases/{d}/documents:{op}
//
// This is completely different from gRPC-Web (binary framing, /ServiceName/Method
// URLs), so it must be handled separately.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// RESTHandler is the HTTP handler for Firestore REST API requests.
type RESTHandler struct {
	srv *Server
}

// NewRESTHandler creates a REST handler wrapping srv.
func NewRESTHandler(srv *Server) *RESTHandler {
	return &RESTHandler{srv: srv}
}

// RESTHandler returns an http.Handler serving the Firestore v1 REST API.
// It routes both :{method} action URLs and document CRUD URLs.
func (s *Server) RESTHandler() http.Handler {
	mux := http.NewServeMux()
	h := &RESTHandler{srv: s}
	mux.Handle("/v1/", h)
	return mux
}

var (
	restUnmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}
	restMarshal   = protojson.MarshalOptions{}
)

type restOpEntry struct {
	fn        func(h *RESTHandler, ctx context.Context, w http.ResponseWriter, path string, body []byte)
	useParent bool
}

var restOps = map[string]restOpEntry{
	"commit":               {(*RESTHandler).handleCommit, false},
	"batchGet":             {(*RESTHandler).handleBatchGet, false},
	"batchWrite":           {(*RESTHandler).handleBatchWrite, false},
	"beginTransaction":     {(*RESTHandler).handleBeginTransaction, false},
	"rollback":             {(*RESTHandler).handleRollback, false},
	"runQuery":             {(*RESTHandler).handleRunQuery, true},
	"runAggregationQuery":  {(*RESTHandler).handleRunAggregationQuery, true},
	"listCollectionIds":    {(*RESTHandler).handleListCollectionIds, true},
	"partitionQuery":       {(*RESTHandler).handlePartitionQuery, true},
}

func (h *RESTHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS: browser SDK calls are cross-origin.
	origin := r.Header.Get("Origin")
	if origin == "" {
		origin = "*"
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Headers",
		"Content-Type, Authorization, X-Goog-Api-Client, X-Firebase-Client, X-Goog-Request-Params")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PATCH, DELETE, OPTIONS")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Strip leading /v1/.
	p := strings.TrimPrefix(r.URL.Path, "/v1/")

	// All Firestore operations are expressed as :op suffixes.
	ctx := r.Context()

	colonIdx := strings.LastIndex(p, ":")
	if colonIdx < 0 {
		// No colon suffix -> document CRUD (GET / PATCH / DELETE) or CreateDocument (POST).
		h.handleCRUD(w, r, p, ctx)
		return
	}
	base := p[:colonIdx] // e.g. "projects/p/databases/d/documents[/{path}]"
	op := p[colonIdx+1:] // e.g. "commit", "runQuery"

	// Derive the database ("projects/p/databases/d") and full parent path.
	docIdx := strings.Index(base, "/documents")
	if docIdx < 0 {
		restError(w, status.Error(codes.InvalidArgument, "path must contain /documents"))
		return
	}
	database := base[:docIdx] // "projects/p/databases/d"
	parent := base            // full Firestore parent, may extend past /documents

	body, err := io.ReadAll(r.Body)
	if err != nil {
		restError(w, status.Errorf(codes.Internal, "read body: %v", err))
		return
	}

	slog.Info("rest: request", "op", op, "db", database)

	if entry, ok := restOps[op]; ok {
		path := database
		if entry.useParent {
			path = parent
		}
		entry.fn(h, ctx, w, path, body)
	} else {
		slog.Warn("rest: unknown operation", "op", op, "path", r.URL.Path)
		http.Error(w, "unknown operation: "+op, http.StatusNotFound)
	}
}



func (h *RESTHandler) handleCommit(ctx context.Context, w http.ResponseWriter, database string, body []byte) {
	var req firestorepb.CommitRequest
	if err := restUnmarshal.Unmarshal(body, &req); err != nil {
		restError(w, status.Errorf(codes.InvalidArgument, "parse commit: %v", err))
		return
	}
	req.Database = database
	resp, err := h.srv.Commit(ctx, &req)
	if err != nil {
		restError(w, err)
		return
	}
	restJSON(w, resp)
}

func (h *RESTHandler) handleBatchGet(ctx context.Context, w http.ResponseWriter, database string, body []byte) {
	var req firestorepb.BatchGetDocumentsRequest
	if err := restUnmarshal.Unmarshal(body, &req); err != nil {
		restError(w, status.Errorf(codes.InvalidArgument, "parse batchGet: %v", err))
		return
	}
	req.Database = database
	col := &batchGetCollector{streamBase: streamBase{ctx: ctx}}
	if err := h.srv.BatchGetDocuments(&req, col); err != nil {
		restError(w, err)
		return
	}
	restJSONArray(w, col.msgs)
}

func (h *RESTHandler) handleBatchWrite(ctx context.Context, w http.ResponseWriter, database string, body []byte) {
	var req firestorepb.BatchWriteRequest
	if err := restUnmarshal.Unmarshal(body, &req); err != nil {
		restError(w, status.Errorf(codes.InvalidArgument, "parse batchWrite: %v", err))
		return
	}
	req.Database = database
	resp, err := h.srv.BatchWrite(ctx, &req)
	if err != nil {
		restError(w, err)
		return
	}
	restJSON(w, resp)
}

func (h *RESTHandler) handleRunQuery(ctx context.Context, w http.ResponseWriter, parent string, body []byte) {
	var req firestorepb.RunQueryRequest
	if err := restUnmarshal.Unmarshal(body, &req); err != nil {
		restError(w, status.Errorf(codes.InvalidArgument, "parse runQuery: %v", err))
		return
	}
	req.Parent = parent
	col := &runQueryCollector{streamBase: streamBase{ctx: ctx}}
	if err := h.srv.RunQuery(&req, col); err != nil {
		restError(w, err)
		return
	}
	restJSONArray(w, col.msgs)
}

func (h *RESTHandler) handleRunAggregationQuery(ctx context.Context, w http.ResponseWriter, parent string, body []byte) {
	var req firestorepb.RunAggregationQueryRequest
	if err := restUnmarshal.Unmarshal(body, &req); err != nil {
		restError(w, status.Errorf(codes.InvalidArgument, "parse runAggregationQuery: %v", err))
		return
	}
	req.Parent = parent
	col := &runAggregationQueryCollector{streamBase: streamBase{ctx: ctx}}
	if err := h.srv.RunAggregationQuery(&req, col); err != nil {
		restError(w, err)
		return
	}
	restJSONArray(w, col.msgs)
}

func (h *RESTHandler) handleBeginTransaction(ctx context.Context, w http.ResponseWriter, database string, body []byte) {
	var req firestorepb.BeginTransactionRequest
	if err := restUnmarshal.Unmarshal(body, &req); err != nil {
		restError(w, status.Errorf(codes.InvalidArgument, "parse beginTransaction: %v", err))
		return
	}
	req.Database = database
	resp, err := h.srv.BeginTransaction(ctx, &req)
	if err != nil {
		restError(w, err)
		return
	}
	restJSON(w, resp)
}

func (h *RESTHandler) handleRollback(ctx context.Context, w http.ResponseWriter, database string, body []byte) {
	var req firestorepb.RollbackRequest
	if err := restUnmarshal.Unmarshal(body, &req); err != nil {
		restError(w, status.Errorf(codes.InvalidArgument, "parse rollback: %v", err))
		return
	}
	req.Database = database
	if _, err := h.srv.Rollback(ctx, &req); err != nil {
		restError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("{}")) //nolint:errcheck
}

func (h *RESTHandler) handleListCollectionIds(ctx context.Context, w http.ResponseWriter, parent string, body []byte) {
	var req firestorepb.ListCollectionIdsRequest
	if err := restUnmarshal.Unmarshal(body, &req); err != nil {
		restError(w, status.Errorf(codes.InvalidArgument, "parse listCollectionIds: %v", err))
		return
	}
	req.Parent = parent
	resp, err := h.srv.ListCollectionIds(ctx, &req)
	if err != nil {
		restError(w, err)
		return
	}
	restJSON(w, resp)
}



// handleCRUD handles GET / PATCH / DELETE / POST on document/collection paths.
// p is the path after stripping the leading "/v1/" (no colon suffix).
func (h *RESTHandler) handleCRUD(w http.ResponseWriter, r *http.Request, p string, ctx context.Context) {
	// Parse "projects/{proj}/databases/{db}/documents[/{rest}]"
	project, database, rest, err := parseResourcePrefix(p)
	if err != nil {
		restError(w, status.Errorf(codes.InvalidArgument, "invalid path: %v", err))
		return
	}
	docPath := strings.TrimPrefix(rest, "/") // path after /documents/

	// Even segment count = document, odd = collection (or 0 = root collection set).
	var segCount int
	if docPath != "" {
		segCount = len(strings.Split(docPath, "/"))
	}
	isDoc := segCount > 0 && segCount%2 == 0

	dbStr := "projects/" + project + "/databases/" + database

	switch r.Method {
	case http.MethodGet:
		if isDoc {
			docName := dbStr + "/documents/" + docPath
			h.handleGetDocument(ctx, w, docName)
		} else {
			dbRoot := dbStr + "/documents"
			var collID, listParent string
			if docPath == "" {
				listParent = dbRoot
			} else {
				parts := strings.Split(docPath, "/")
				collID = parts[len(parts)-1]
				if len(parts) > 1 {
					listParent = dbRoot + "/" + strings.Join(parts[:len(parts)-1], "/")
				} else {
					listParent = dbRoot
				}
			}
			h.handleListDocuments(ctx, w, listParent, collID)
		}
	case http.MethodDelete:
		docName := dbStr + "/documents/" + docPath
		h.handleDeleteDocument(ctx, w, docName)
	case http.MethodPatch:
		docName := dbStr + "/documents/" + docPath
		body, err := io.ReadAll(r.Body)
		if err != nil {
			restError(w, status.Errorf(codes.Internal, "read body: %v", err))
			return
		}
		maskPaths := r.URL.Query()["updateMask.fieldPaths"]
		h.handleUpdateDocument(ctx, w, docName, body, maskPaths)
	case http.MethodPost:
		if !isDoc {
			dbRoot := dbStr + "/documents"
			parts := strings.Split(docPath, "/")
			collID := parts[len(parts)-1]
			var colParent string
			if len(parts) > 1 {
				colParent = dbRoot + "/" + strings.Join(parts[:len(parts)-1], "/")
			} else {
				colParent = dbRoot
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				restError(w, status.Errorf(codes.Internal, "read body: %v", err))
				return
			}
			h.handleCreateDocument(ctx, w, colParent, collID, r.URL.Query().Get("documentId"), body)
		} else {
			http.Error(w, "POST not allowed on a document path", http.StatusMethodNotAllowed)
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *RESTHandler) handleGetDocument(ctx context.Context, w http.ResponseWriter, name string) {
	resp, err := h.srv.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: name})
	if err != nil {
		restError(w, err)
		return
	}
	restJSON(w, resp)
}

func (h *RESTHandler) handleDeleteDocument(ctx context.Context, w http.ResponseWriter, name string) {
	if _, err := h.srv.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: name}); err != nil {
		restError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("{}")) //nolint:errcheck
}

func (h *RESTHandler) handleUpdateDocument(ctx context.Context, w http.ResponseWriter, name string, body []byte, maskPaths []string) {
	req := &firestorepb.UpdateDocumentRequest{Document: &firestorepb.Document{Name: name}}
	if len(body) > 0 {
		if err := restUnmarshal.Unmarshal(body, req); err != nil {
			restError(w, status.Errorf(codes.InvalidArgument, "parse updateDocument: %v", err))
			return
		}
		req.Document.Name = name // URL wins
	}
	if len(maskPaths) > 0 {
		req.UpdateMask = &firestorepb.DocumentMask{FieldPaths: maskPaths}
	}
	resp, err := h.srv.UpdateDocument(ctx, req)
	if err != nil {
		restError(w, err)
		return
	}
	restJSON(w, resp)
}

func (h *RESTHandler) handleCreateDocument(ctx context.Context, w http.ResponseWriter, parent, collectionID, docID string, body []byte) {
	req := &firestorepb.CreateDocumentRequest{
		Parent:       parent,
		CollectionId: collectionID,
		DocumentId:   docID,
		Document:     &firestorepb.Document{},
	}
	if len(body) > 0 {
		if err := restUnmarshal.Unmarshal(body, req); err != nil {
			restError(w, status.Errorf(codes.InvalidArgument, "parse createDocument: %v", err))
			return
		}
		req.Parent = parent
		req.CollectionId = collectionID
		req.DocumentId = docID
	}
	resp, err := h.srv.CreateDocument(ctx, req)
	if err != nil {
		restError(w, err)
		return
	}
	restJSON(w, resp)
}

func (h *RESTHandler) handlePartitionQuery(ctx context.Context, w http.ResponseWriter, parent string, body []byte) {
	var req firestorepb.PartitionQueryRequest
	if err := restUnmarshal.Unmarshal(body, &req); err != nil {
		restError(w, status.Errorf(codes.InvalidArgument, "parse partitionQuery: %v", err))
		return
	}
	req.Parent = parent
	resp, err := h.srv.PartitionQuery(ctx, &req)
	if err != nil {
		restError(w, err)
		return
	}
	restJSON(w, resp)
}

func (h *RESTHandler) handleListDocuments(ctx context.Context, w http.ResponseWriter, parent, collectionID string) {
	resp, err := h.srv.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{
		Parent:       parent,
		CollectionId: collectionID,
	})
	if err != nil {
		restError(w, err)
		return
	}
	restJSON(w, resp)
}

//
// These implement the gRPC server-stream interfaces (Send + grpc.ServerStream)
// so streaming RPCs can be called from an HTTP handler. Responses are collected
// in-memory and returned as a JSON array.

type streamBase struct {
	ctx context.Context
}

func (s *streamBase) Context() context.Context       { return s.ctx }
func (s *streamBase) SetHeader(_ metadata.MD) error  { return nil }
func (s *streamBase) SendHeader(_ metadata.MD) error { return nil }
func (s *streamBase) SetTrailer(_ metadata.MD)        {}
func (s *streamBase) SendMsg(_ interface{}) error    { return nil }
func (s *streamBase) RecvMsg(_ interface{}) error    { return nil }

type batchGetCollector struct {
	streamBase
	msgs []proto.Message
}

func (c *batchGetCollector) Send(r *firestorepb.BatchGetDocumentsResponse) error {
	c.msgs = append(c.msgs, r)
	return nil
}

type runQueryCollector struct {
	streamBase
	msgs []proto.Message
}

func (c *runQueryCollector) Send(r *firestorepb.RunQueryResponse) error {
	c.msgs = append(c.msgs, r)
	return nil
}

type runAggregationQueryCollector struct {
	streamBase
	msgs []proto.Message
}

func (c *runAggregationQueryCollector) Send(r *firestorepb.RunAggregationQueryResponse) error {
	c.msgs = append(c.msgs, r)
	return nil
}



func restJSON(w http.ResponseWriter, msg proto.Message) {
	b, err := restMarshal.Marshal(msg)
	if err != nil {
		http.Error(w, "marshal: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b) //nolint:errcheck
}

func restJSONArray(w http.ResponseWriter, msgs []proto.Message) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("[")) //nolint:errcheck
	for i, m := range msgs {
		if i > 0 {
			w.Write([]byte(",")) //nolint:errcheck
		}
		b, _ := restMarshal.Marshal(m)
		w.Write(b) //nolint:errcheck
	}
	w.Write([]byte("]")) //nolint:errcheck
}

func restError(w http.ResponseWriter, err error) {
	st, _ := status.FromError(err)
	httpCode := grpcCodeToHTTP(st.Code())
	type errDetail struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	}
	type errResp struct {
		Error errDetail `json:"error"`
	}
	b, _ := json.Marshal(errResp{Error: errDetail{
		Code:    httpCode,
		Message: st.Message(),
		Status:  st.Code().String(),
	}})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpCode)
	w.Write(b) //nolint:errcheck
}

func grpcCodeToHTTP(code codes.Code) int {
	switch code {
	case codes.OK:
		return http.StatusOK
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
