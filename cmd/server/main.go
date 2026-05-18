package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	grpcstatus "google.golang.org/grpc/status"

	adminpb "cloud.google.com/go/datastore/admin/apiv1/adminpb"
	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/magnus-rattlehead/hearthstore/internal/datastore"
	"github.com/magnus-rattlehead/hearthstore/internal/server"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"github.com/magnus-rattlehead/hearthstore/internal/webchannel"
)

func defaultDataDir() string {
	if v := os.Getenv("HEARTHSTORE_DATA_DIR"); v != "" {
		return v
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".hearthstore")
	}
	return "./data"
}

const logo = `

              ▒▒
              ▓▓
              ▓▓▓▓
              ████
            ░░████
            ██▓▓██
      ░░  ████▓▓▓▓    ▒▒
      ▓▓▒▒▓▓██▒▒▓▓  ██░░
    ▓▓██████▒▒▓▓▒▒████  ▒▒
    ██████▒▒▒▒▓▓▒▒██▒▒  ██
    ██▓▓▓▓░░▒▒████▓▓░░▒▒██  ░░
    ██▒▒▒▒░░▒▒▓▓██▓▓▓▓████▓▓░░
    ██▒▒▒▒░░▒▒████▓▓██▓▓████░░
    ██▒▒░░░░▒▒▓▓██▒▒██▒▒██▓▓░░  ██╗░░██╗███████╗░█████╗░██████╗░████████╗██╗░░██╗░██████╗████████╗░█████╗░██████╗░███████╗
░░  ▒▒▒▒░░░░░░▒▒██░░▒▒▒▒▓▓██    ██║░░██║██╔════╝██╔══██╗██╔══██╗╚══██╔══╝██║░░██║██╔════╝╚══██╔══╝██╔══██╗██╔══██╗██╔════╝
  ██▓▓▓▓░░  ░░▒▒▒▒░░░░▒▒▓▓▓▓    ███████║█████╗░░███████║██████╔╝░░░██║░░░███████║╚█████╗░░░░██║░░░██║░░██║██████╔╝█████╗░░
  ░░██▓▓▒▒░░  ░░░░░░░░░░▓▓░░    ██╔══██║██╔══╝░░██╔══██║██╔══██╗░░░██║░░░██╔══██║░╚═══██╗░░░██║░░░██║░░██║██╔══██╗██╔══╝░░
    ░░▓▓░░░░        ░░▒▒░░      ██║░░██║███████╗██║░░██║██║░░██║░░░██║░░░██║░░██║██████╔╝░░░██║░░░╚█████╔╝██║░░██║███████╗
        ░░▒▒░░      ░░          ╚═╝░░╚═╝╚══════╝╚═╝░░╚═╝╚═╝░░╚═╝░░░╚═╝░░░╚═╝░░╚═╝╚═════╝░░░░╚═╝░░░░╚════╝░╚═╝░░╚═╝╚══════╝

`

// prefixWriter prepends "[hearthstore] " to each log line, so the tag appears
// before the structured fields rather than inside msg=.
type prefixWriter struct{ w io.Writer }

func (pw prefixWriter) Write(p []byte) (int, error) {
	const prefix = "[hearthstore] "
	buf := make([]byte, len(prefix)+len(p))
	copy(buf, prefix)
	copy(buf[len(prefix):], p)
	_, err := pw.w.Write(buf)
	return len(p), err
}

func main() {
	startTime := time.Now()
	fmt.Fprint(os.Stderr, logo)

	port          := flag.Int("port", 8080, "gRPC listen port (Firestore Native)")
	webPort       := flag.Int("web-port", 0, "gRPC-Web HTTP listen port (0 = disabled)")
	datastoreAddr := flag.String("datastore-addr", ":8456", "HTTP listen address (Datastore REST API)")
	dataDir       := flag.String("data-dir", defaultDataDir(), "Directory for SQLite database files (default: $HEARTHSTORE_DATA_DIR or ~/.hearthstore)")
	mode          := flag.String("mode", "both", "Server mode: firestore | datastore | both")
	logLevel      := flag.String("log-level", "info", "Log level: debug | info | warn | error")
	indexConfig   := flag.String("index-config", "", "Path to index.yaml for Datastore composite index configuration")
	reindexDS     := flag.Bool("reindex-ds", false, "Rebuild ds_field_index from ds_documents then serve normally")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		log.Fatalf("invalid log level %q: %v", *logLevel, err)
	}
	logger := slog.New(slog.NewTextHandler(prefixWriter{os.Stderr}, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("failed to create data directory: %v", err)
	}

	store, err := storage.New(*dataDir)
	if err != nil {
		log.Fatalf("failed to open storage: %v", err)
	}
	defer store.Close()

	if *reindexDS {
		slog.Info("rebuilding ds_field_index from ds_documents...")
		if err := store.RebuildDsFieldIndex(); err != nil {
			log.Fatalf("reindex failed: %v", err)
		}
		slog.Info("ds_field_index rebuild complete")
	}

	ring := server.NewRecentRing(200)
	dash := server.NewDashboard(store, ring, startTime)
	unary, streaming := makeGRPCInterceptors(store, ring)

	switch *mode {
	case "firestore":
		serveFirestore(*port, *webPort, store, ring, dash, unary, streaming)
	case "datastore":
		serveDatastore(*datastoreAddr, store, *indexConfig, ring, dash, unary)
	case "both":
		go serveFirestore(*port, *webPort, store, ring, dash, unary, streaming)
		serveDatastore(*datastoreAddr, store, *indexConfig, ring, dash, unary)
	default:
		log.Fatalf("unknown mode %q: use firestore, datastore, or both", *mode)
	}
}

func serveFirestore(port, webPort int, store *storage.Store, ring *server.RecentRing, dash http.Handler, unary grpc.UnaryServerInterceptor, streaming grpc.StreamServerInterceptor) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("firestore: failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unary),
		grpc.ChainStreamInterceptor(streaming),
	)
	fsServer := server.New(store)
	firestorepb.RegisterFirestoreServer(grpcServer, fsServer)
	reflection.Register(grpcServer)
	slog.Info("hearthstore listening", "protocol", "grpc+rest", "port", port)
	slog.Info("set env var", "FIRESTORE_EMULATOR_HOST", fmt.Sprintf("localhost:%d", port))

	if webPort > 0 {
		go serveGRPCWeb(webPort, grpcServer, fsServer)
	}

	// Single port: gRPC, WebChannel (/channel paths), REST, and dashboard.
	restHandler := server.NewRESTHandler(fsServer)
	wcHandler := webchannel.New(fsServer)
	mixed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/_/") {
			dash.ServeHTTP(w, r)
			return
		}
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/channel") {
			// CORS: browser SDK is always cross-origin.
			origin := r.Header.Get("Origin")
			if origin == "" {
				origin = "*"
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers",
				"Content-Type, Authorization, X-Goog-Api-Client, X-Firebase-Client, X-Goog-Request-Params")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			wcHandler.ServeHTTP(w, r)
			return
		}
		restHandler.ServeHTTP(w, r)
	})
	httpSrv := &http.Server{Handler: h2c.NewHandler(mixed, &http2.Server{})}
	if err := httpSrv.Serve(lis); err != nil {
		log.Fatalf("firestore: serve error: %v", err)
	}
}

// serveGRPCWeb serves WebChannel, REST, and gRPC-Web on a separate port.
func serveGRPCWeb(port int, grpcServer *grpc.Server, fsServer *server.Server) {
	wrapped := grpcweb.WrapServer(grpcServer,
		grpcweb.WithOriginFunc(func(origin string) bool { return true }),
		grpcweb.WithWebsockets(true),
	)
	wcHandler := webchannel.New(fsServer)
	restHandler := server.NewRESTHandler(fsServer)
	httpSrv := &http.Server{
		Addr: fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// CORS: browser SDK is always cross-origin.
			origin := r.Header.Get("Origin")
			if origin == "" {
				origin = "*"
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PATCH, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers",
				"Content-Type, Authorization, X-Goog-Api-Client, X-Firebase-Client, X-Goog-Request-Params")

			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			if strings.HasSuffix(r.URL.Path, "/channel") {
				wcHandler.ServeHTTP(w, r)
				return
			}
			if strings.HasPrefix(r.URL.Path, "/v1/") {
				restHandler.ServeHTTP(w, r)
				return
			}
			if wrapped.IsGrpcWebRequest(r) || wrapped.IsAcceptableGrpcCorsRequest(r) || wrapped.IsGrpcWebSocketRequest(r) {
				wrapped.ServeHTTP(w, r)
				return
			}
			http.NotFound(w, r)
		}),
	}
	slog.Info("hearthstore listening", "protocol", "grpc-web+webchannel", "port", port)
	if err := httpSrv.ListenAndServe(); err != nil {
		log.Fatalf("grpc-web: serve error: %v", err)
	}
}

func makeGRPCInterceptors(store *storage.Store, ring *server.RecentRing) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		dur := time.Since(start)
		var details map[string]any
		if err == nil {
			details = mergeDetails(requestDetails(req), responseDetails(resp, dur))
		} else {
			details = requestDetails(req)
		}
		logRPC(store, ring, shortMethod(info.FullMethod), requestAttrs(req), details, 0, dur, err)
		return resp, err
	}
	streaming := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		cs := &countingStream{ServerStream: ss}
		err := handler(srv, cs)
		dur := time.Since(start)
		d := requestDetails(cs.firstReq)
		if cs.sent > 0 {
			if d == nil {
				d = map[string]any{}
			}
			d["docs"] = cs.sent
		}
		logRPC(store, ring, shortMethod(info.FullMethod), requestAttrs(cs.firstReq), d, cs.sent, dur, err)
		return err
	}
	return unary, streaming
}

// countingStream captures the first RecvMsg and counts SendMsg calls.
type countingStream struct {
	grpc.ServerStream
	sent     int
	firstReq any
}

func (s *countingStream) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil && s.firstReq == nil {
		s.firstReq = m
	}
	return err
}

func (s *countingStream) SendMsg(m any) error {
	err := s.ServerStream.SendMsg(m)
	if err == nil {
		s.sent++
	}
	return err
}

// logRPC emits a structured log line and records metrics.
func logRPC(store *storage.Store, ring *server.RecentRing, method string, attrs []any, details map[string]any, sent int, dur time.Duration, err error) {
	isErr := err != nil
	if store != nil {
		store.IncrRPC(isErr)
	}
	if ring != nil && !strings.HasPrefix(method, "_/") {
		e := server.RecentEntry{
			T:         time.Now(),
			Method:    method,
			LatencyMs: dur.Milliseconds(),
			Details:   details,
		}
		if err != nil {
			e.Err = grpcstatus.Convert(err).Message()
		}
		ring.Add(e)
	}

	base := []any{"method", method}
	base = append(base, attrs...)
	if sent > 0 {
		base = append(base, "docs", sent)
	}
	base = append(base, "duration", dur.Round(time.Microsecond))

	if isErr {
		code := grpcstatus.Code(err)
		msg := grpcstatus.Convert(err).Message()
		base = append(base, "code", code, "err", msg)
		if code == codes.Internal || code == codes.Unknown {
			slog.Error("rpc", base...)
		} else {
			slog.Warn("rpc", base...)
		}
		return
	}
	slog.Info("rpc", base...)
}

// shortMethod strips the gRPC service prefix, returning just the RPC name.
// e.g. "/google.firestore.v1.Firestore/RunQuery" -> "RunQuery"
func shortMethod(m string) string {
	if i := strings.LastIndex(m, "/"); i >= 0 {
		return m[i+1:]
	}
	return m
}

// docPath strips everything up to and including "/documents/" from a Firestore
// resource name. Returns "" for root-collection parents.
func docPath(s string) string {
	const sep = "/documents/"
	if i := strings.Index(s, sep); i >= 0 {
		return s[i+len(sep):]
	}
	return ""
}

// requestAttrs returns structured log attributes for a Firestore request proto.
func requestAttrs(req any) []any {
	if req == nil {
		return nil
	}
	switch r := req.(type) {
	case *firestorepb.GetDocumentRequest:
		return []any{"doc", docPath(r.GetName())}

	case *firestorepb.CreateDocumentRequest:
		if p := docPath(r.GetParent()); p != "" {
			return []any{"parent", p, "collection", r.GetCollectionId()}
		}
		return []any{"collection", r.GetCollectionId()}

	case *firestorepb.UpdateDocumentRequest:
		return []any{"doc", docPath(r.GetDocument().GetName())}

	case *firestorepb.DeleteDocumentRequest:
		return []any{"doc", docPath(r.GetName())}

	case *firestorepb.BatchGetDocumentsRequest:
		return []any{"docs", len(r.GetDocuments())}

	case *firestorepb.BatchWriteRequest:
		return []any{"writes", len(r.GetWrites())}

	case *firestorepb.ListDocumentsRequest:
		if p := docPath(r.GetParent()); p != "" {
			return []any{"parent", p, "collection", r.GetCollectionId()}
		}
		return []any{"collection", r.GetCollectionId()}

	case *firestorepb.ListCollectionIdsRequest:
		if p := docPath(r.GetParent()); p != "" {
			return []any{"parent", p}
		}
		return nil

	case *firestorepb.RunQueryRequest:
		attrs := queryAttrs(r.GetParent(), r.GetStructuredQuery())
		return attrs

	case *firestorepb.RunAggregationQueryRequest:
		aq := r.GetStructuredAggregationQuery()
		if aq == nil {
			return nil
		}
		attrs := queryAttrs(r.GetParent(), aq.GetStructuredQuery())
		var ops []string
		for _, a := range aq.GetAggregations() {
			switch a.Operator.(type) {
			case *firestorepb.StructuredAggregationQuery_Aggregation_Count_:
				ops = append(ops, "count")
			case *firestorepb.StructuredAggregationQuery_Aggregation_Sum_:
				ops = append(ops, "sum("+a.GetSum().GetField().GetFieldPath()+")")
			case *firestorepb.StructuredAggregationQuery_Aggregation_Avg_:
				ops = append(ops, "avg("+a.GetAvg().GetField().GetFieldPath()+")")
			}
		}
		if len(ops) > 0 {
			attrs = append(attrs, "agg", strings.Join(ops, ","))
		}
		return attrs

	case *firestorepb.CommitRequest:
		return []any{"writes", len(r.GetWrites())}

	case *firestorepb.BeginTransactionRequest:
		return nil

	case *firestorepb.RollbackRequest:
		return nil

	case *firestorepb.ListenRequest:
		if at := r.GetAddTarget(); at != nil {
			if q := at.GetQuery(); q != nil {
				return queryAttrs(q.GetParent(), q.GetStructuredQuery())
			}
		}
		return nil
	}
	return nil
}

// requestDetails returns structured detail fields for the RPC panel.
func requestDetails(req any) map[string]any {
	if req == nil {
		return nil
	}
	switch r := req.(type) {
	case *firestorepb.GetDocumentRequest:
		return map[string]any{"doc": docPath(r.GetName())}

	case *firestorepb.CreateDocumentRequest:
		d := map[string]any{"collection": r.GetCollectionId()}
		if p := docPath(r.GetParent()); p != "" {
			d["parent"] = p
		}
		return d

	case *firestorepb.UpdateDocumentRequest:
		d := map[string]any{"doc": docPath(r.GetDocument().GetName())}
		if r.UpdateMask != nil && len(r.UpdateMask.FieldPaths) > 0 {
			d["mask"] = strings.Join(r.UpdateMask.FieldPaths, ", ")
		}
		return d

	case *firestorepb.DeleteDocumentRequest:
		return map[string]any{"doc": docPath(r.GetName())}

	case *firestorepb.BatchGetDocumentsRequest:
		names := r.GetDocuments()
		paths := make([]string, 0, len(names))
		for _, n := range names {
			paths = append(paths, docPath(n))
		}
		return map[string]any{"docs": len(names), "paths": paths}

	case *firestorepb.BatchWriteRequest:
		return writeDetails(r.GetWrites(), nil)

	case *firestorepb.ListDocumentsRequest:
		d := map[string]any{"collection": r.GetCollectionId()}
		if p := docPath(r.GetParent()); p != "" {
			d["parent"] = p
		}
		if r.GetPageSize() > 0 {
			d["page_size"] = r.GetPageSize()
		}
		if r.GetOrderBy() != "" {
			d["order_by"] = r.GetOrderBy()
		}
		return d

	case *firestorepb.ListCollectionIdsRequest:
		d := map[string]any{}
		if p := docPath(r.GetParent()); p != "" {
			d["parent"] = p
		}
		return d

	case *firestorepb.RunQueryRequest:
		return queryDetails(r.GetParent(), r.GetStructuredQuery())

	case *firestorepb.RunAggregationQueryRequest:
		aq := r.GetStructuredAggregationQuery()
		if aq == nil {
			return nil
		}
		d := queryDetails(r.GetParent(), aq.GetStructuredQuery())
		var ops []string
		for _, a := range aq.GetAggregations() {
			switch a.Operator.(type) {
			case *firestorepb.StructuredAggregationQuery_Aggregation_Count_:
				ops = append(ops, "count")
			case *firestorepb.StructuredAggregationQuery_Aggregation_Sum_:
				ops = append(ops, "sum("+a.GetSum().GetField().GetFieldPath()+")")
			case *firestorepb.StructuredAggregationQuery_Aggregation_Avg_:
				ops = append(ops, "avg("+a.GetAvg().GetField().GetFieldPath()+")")
			}
		}
		if len(ops) > 0 {
			d["aggregations"] = strings.Join(ops, ", ")
		}
		return d

	case *firestorepb.CommitRequest:
		return writeDetails(r.GetWrites(), r.GetTransaction())

	case *firestorepb.BeginTransactionRequest:
		if opts := r.GetOptions(); opts != nil {
			switch opts.Mode.(type) {
			case *firestorepb.TransactionOptions_ReadOnly_:
				return map[string]any{"mode": "read_only"}
			case *firestorepb.TransactionOptions_ReadWrite_:
				return map[string]any{"mode": "read_write"}
			}
		}
		return nil

	case *firestorepb.ListenRequest:
		if at := r.GetAddTarget(); at != nil {
			if q := at.GetQuery(); q != nil {
				return queryDetails(q.GetParent(), q.GetStructuredQuery())
			}
		}
		return nil

	case *datastorepb.LookupRequest:
		return datastore.DSLookupDetails(r)

	case *datastorepb.RunQueryRequest:
		return datastore.DSQueryDetails(r)

	case *datastorepb.RunAggregationQueryRequest:
		return datastore.DSAggregationQueryDetails(r)

	case *datastorepb.CommitRequest:
		return datastore.DSMutationDetails(r)

	case *datastorepb.BeginTransactionRequest:
		return datastore.DSBeginTxDetails(r)

	case *datastorepb.AllocateIdsRequest:
		return datastore.DSAllocateIdsDetails(r)

	case *datastorepb.ReserveIdsRequest:
		return datastore.DSReserveIdsDetails(r)

	case *datastorepb.RollbackRequest:
		return datastore.DSRollbackDetails(r)
	}
	return nil
}

// responseDetails returns response-side detail fields for the RPC panel.
func responseDetails(resp any, elapsed time.Duration) map[string]any {
	if resp == nil {
		return nil
	}
	switch r := resp.(type) {
	case *firestorepb.Document:
		d := map[string]any{"field_count": len(r.GetFields()), "elapsed_ms": elapsed.Milliseconds()}
		if n := docPath(r.GetName()); n != "" {
			d["name"] = n
		}
		if r.UpdateTime != nil {
			d["update_time"] = r.UpdateTime.AsTime().UTC().Format("2006-01-02T15:04:05Z")
		}
		if r.CreateTime != nil {
			d["create_time"] = r.CreateTime.AsTime().UTC().Format("2006-01-02T15:04:05Z")
		}
		return d

	case *firestorepb.BeginTransactionResponse:
		d := map[string]any{}
		if len(r.Transaction) > 0 {
			d["txn_returned"] = true
			d["txn_id_prefix"] = mainHexPrefix(r.Transaction)
		}
		return d

	case *firestorepb.CommitResponse:
		d := map[string]any{"write_results": len(r.GetWriteResults()), "elapsed_ms": elapsed.Milliseconds()}
		if r.CommitTime != nil {
			d["commit_time"] = r.CommitTime.AsTime().UTC().Format("2006-01-02T15:04:05Z")
		}
		return d

	case *firestorepb.BatchWriteResponse:
		var successes, failures int
		for _, s := range r.GetStatus() {
			if s.GetCode() == 0 {
				successes++
			} else {
				failures++
			}
		}
		d := map[string]any{"writes": len(r.GetWriteResults()), "elapsed_ms": elapsed.Milliseconds()}
		if successes > 0 {
			d["successes"] = successes
		}
		if failures > 0 {
			d["failures"] = failures
		}
		return d

	case *datastorepb.LookupResponse:
		return datastore.DSLookupResponseDetails(r, elapsed)

	case *datastorepb.CommitResponse:
		return datastore.DSCommitResponseDetails(r, elapsed)

	case *datastorepb.RunQueryResponse:
		return datastore.DSQueryResponseDetails(r, elapsed)

	case *datastorepb.RunAggregationQueryResponse:
		return datastore.DSAggregationResponseDetails(r, elapsed)

	case *datastorepb.BeginTransactionResponse:
		return datastore.DSBeginTxResponseDetails(r)

	case *datastorepb.AllocateIdsResponse:
		return datastore.DSAllocateIdsResponseDetails(r)
	}
	return nil
}

// mergeDetails merges two detail maps; b overwrites a on collision.
func mergeDetails(a, b map[string]any) map[string]any {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	out := make(map[string]any, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// mainHexPrefix returns the first 8 hex chars of b.
func mainHexPrefix(b []byte) string {
	n := 4
	if len(b) < n {
		n = len(b)
	}
	h := make([]byte, n*2)
	const hx = "0123456789abcdef"
	for i := range n {
		h[i*2] = hx[b[i]>>4]
		h[i*2+1] = hx[b[i]&0xf]
	}
	return string(h)
}

func writeDetails(writes []*firestorepb.Write, txn []byte) map[string]any {
	d := map[string]any{"writes": len(writes)}
	var sets, merges, deletes, xforms int
	collSet := map[string]struct{}{}
	for _, w := range writes {
		switch op := w.Operation.(type) {
		case *firestorepb.Write_Update:
			if w.UpdateMask != nil && len(w.UpdateMask.FieldPaths) > 0 {
				merges++
			} else {
				sets++
			}
			if p := docPath(op.Update.GetName()); p != "" {
				if i := strings.Index(p, "/"); i >= 0 {
					collSet[p[:i]] = struct{}{}
				}
			}
		case *firestorepb.Write_Delete:
			deletes++
			if p := docPath(op.Delete); p != "" {
				if i := strings.Index(p, "/"); i >= 0 {
					collSet[p[:i]] = struct{}{}
				}
			}
		case *firestorepb.Write_Transform:
			xforms++
		}
	}
	if sets > 0 {
		d["sets"] = sets
	}
	if merges > 0 {
		d["merges"] = merges
	}
	if deletes > 0 {
		d["deletes"] = deletes
	}
	if xforms > 0 {
		d["transforms"] = xforms
	}
	if len(collSet) > 0 {
		colls := make([]string, 0, len(collSet))
		for c := range collSet {
			colls = append(colls, c)
		}
		sort.Strings(colls)
		d["collections"] = colls
	}
	if len(txn) > 0 {
		d["txn"] = true
	}
	return d
}

func queryDetails(parent string, q *firestorepb.StructuredQuery) map[string]any {
	d := map[string]any{}
	if q != nil && len(q.From) > 0 {
		sel := q.From[0]
		d["collection"] = sel.CollectionId
		if sel.AllDescendants {
			d["group"] = true
		}
	}
	if p := docPath(parent); p != "" {
		d["parent"] = p
	}
	if q != nil && q.Limit != nil {
		d["limit"] = q.Limit.Value
	}
	if q != nil && len(q.OrderBy) > 0 {
		orders := make([]string, 0, len(q.OrderBy))
		for _, o := range q.OrderBy {
			s := o.Field.FieldPath
			if o.Direction == firestorepb.StructuredQuery_DESCENDING {
				s += " desc"
			}
			orders = append(orders, s)
		}
		d["order_by"] = strings.Join(orders, ", ")
	}
	if q != nil && q.Where != nil {
		d["filter"] = fsFilterSummary(q.Where)
	}
	return d
}

func fsFilterSummary(f *firestorepb.StructuredQuery_Filter) string {
	if f == nil {
		return ""
	}
	switch ft := f.FilterType.(type) {
	case *firestorepb.StructuredQuery_Filter_CompositeFilter:
		cf := ft.CompositeFilter
		parts := make([]string, 0, len(cf.Filters))
		for _, sub := range cf.Filters {
			if s := fsFilterSummary(sub); s != "" {
				parts = append(parts, s)
			}
		}
		op := " AND "
		if cf.Op == firestorepb.StructuredQuery_CompositeFilter_OR {
			op = " OR "
		}
		joined := strings.Join(parts, op)
		if len(parts) > 1 {
			return "(" + joined + ")"
		}
		return joined
	case *firestorepb.StructuredQuery_Filter_FieldFilter:
		ff := ft.FieldFilter
		field := ff.Field.GetFieldPath()
		op := fsFieldFilterOp(ff.Op)
		return field + " " + op + " " + fsValueSummary(ff.Value)
	case *firestorepb.StructuredQuery_Filter_UnaryFilter:
		uf := ft.UnaryFilter
		field := ""
		if ref := uf.GetField(); ref != nil {
			field = ref.GetFieldPath()
		}
		switch uf.Op {
		case firestorepb.StructuredQuery_UnaryFilter_IS_NAN:
			return field + " IS NaN"
		case firestorepb.StructuredQuery_UnaryFilter_IS_NULL:
			return field + " IS NULL"
		case firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NAN:
			return field + " IS NOT NaN"
		case firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NULL:
			return field + " IS NOT NULL"
		}
	}
	return ""
}

func fsFieldFilterOp(op firestorepb.StructuredQuery_FieldFilter_Operator) string {
	switch op {
	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN:
		return "<"
	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
		return "<="
	case firestorepb.StructuredQuery_FieldFilter_GREATER_THAN:
		return ">"
	case firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
		return ">="
	case firestorepb.StructuredQuery_FieldFilter_EQUAL:
		return "="
	case firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL:
		return "!="
	case firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
		return "ARRAY_CONTAINS"
	case firestorepb.StructuredQuery_FieldFilter_IN:
		return "IN"
	case firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:
		return "ARRAY_CONTAINS_ANY"
	case firestorepb.StructuredQuery_FieldFilter_NOT_IN:
		return "NOT IN"
	}
	return "?"
}

func fsValueSummary(v *firestorepb.Value) string {
	if v == nil {
		return "null"
	}
	switch vt := v.ValueType.(type) {
	case *firestorepb.Value_NullValue:
		return "null"
	case *firestorepb.Value_BooleanValue:
		if vt.BooleanValue {
			return "true"
		}
		return "false"
	case *firestorepb.Value_IntegerValue:
		return fmt.Sprintf("%d", vt.IntegerValue)
	case *firestorepb.Value_DoubleValue:
		return fmt.Sprintf("%g", vt.DoubleValue)
	case *firestorepb.Value_TimestampValue:
		return vt.TimestampValue.AsTime().UTC().Format("2006-01-02T15:04:05Z")
	case *firestorepb.Value_StringValue:
		s := vt.StringValue
		if len(s) > 40 {
			s = s[:37] + "..."
		}
		return fmt.Sprintf("%q", s)
	case *firestorepb.Value_BytesValue:
		return fmt.Sprintf("<bytes:%d>", len(vt.BytesValue))
	case *firestorepb.Value_ReferenceValue:
		p := docPath(vt.ReferenceValue)
		if len(p) > 40 {
			p = "..." + p[len(p)-37:]
		}
		return p
	case *firestorepb.Value_ArrayValue:
		if av := vt.ArrayValue; av != nil {
			n := len(av.Values)
			if n == 0 {
				return "[]"
			}
			elems := make([]string, 0, min(n, 5))
			for i, elem := range av.Values {
				if i >= 5 {
					elems = append(elems, fmt.Sprintf("+%d more", n-5))
					break
				}
				elems = append(elems, fsValueSummary(elem))
			}
			return "[" + strings.Join(elems, ", ") + "]"
		}
		return "[]"
	case *firestorepb.Value_MapValue:
		if mv := vt.MapValue; mv != nil {
			return fmt.Sprintf("{%d fields}", len(mv.Fields))
		}
		return "{}"
	case *firestorepb.Value_GeoPointValue:
		if gp := vt.GeoPointValue; gp != nil {
			return fmt.Sprintf("(%g,%g)", gp.Latitude, gp.Longitude)
		}
	}
	return "?"
}

// queryAttrs extracts collection and optional parent from a StructuredQuery.
func queryAttrs(parent string, q *firestorepb.StructuredQuery) []any {
	var attrs []any
	if q != nil && len(q.From) > 0 {
		sel := q.From[0]
		if p := docPath(parent); p != "" {
			attrs = append(attrs, "parent", p)
		}
		attrs = append(attrs, "collection", sel.CollectionId)
		if sel.AllDescendants {
			attrs = append(attrs, "group", true)
		}
	}
	return attrs
}

func serveDatastore(addr string, store *storage.Store, indexConfig string, ring *server.RecentRing, dash http.Handler, unary grpc.UnaryServerInterceptor) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("datastore: failed to listen: %v", err)
	}
	ds := datastore.New(store)
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unary),
	)
	datastorepb.RegisterDatastoreServer(grpcServer, ds.NewGRPCServer())
	adminSrv, err := datastore.NewAdminServer(indexConfig)
	if err != nil {
		log.Fatalf("datastore: failed to load index config: %v", err)
	}
	adminpb.RegisterDatastoreAdminServer(grpcServer, adminSrv)
	reflection.Register(grpcServer)
	slog.Info("hearthstore listening", "protocol", "grpc+http", "addr", addr)
	slog.Info("set env var", "DATASTORE_EMULATOR_HOST", addr)

	mixed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/_/") {
			dash.ServeHTTP(w, r)
			return
		}
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
			return
		}
		loggingHTTPMiddleware(ds.Handler(), store, ring).ServeHTTP(w, r)
	})
	httpSrv := &http.Server{Handler: h2c.NewHandler(mixed, &http2.Server{})}
	if err := httpSrv.Serve(lis); err != nil {
		log.Fatalf("datastore: serve error: %v", err)
	}
}

// loggingResponseWriter captures the HTTP status code written by a handler.
type loggingResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *loggingResponseWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func loggingHTTPMiddleware(next http.Handler, store *storage.Store, ring *server.RecentRing) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lw := &loggingResponseWriter{ResponseWriter: w, status: http.StatusOK}
		ctx, detailSlot := datastore.WithHTTPDetails(r.Context())
		r = r.WithContext(ctx)
		next.ServeHTTP(lw, r)
		dur := time.Since(start).Round(time.Microsecond)

		method := r.URL.Path
		if i := strings.LastIndex(method, ":"); i >= 0 {
			method = method[i+1:]
		}

		isErr := lw.status >= 500
		if store != nil {
			store.IncrRPC(isErr)
		}
		if ring != nil && !strings.HasPrefix(r.URL.Path, "/_/") {
			ring.Add(server.RecentEntry{
				T:         start,
				Method:    method,
				Path:      r.URL.Path,
				LatencyMs: dur.Milliseconds(),
				Status:    lw.status,
				Details:   detailSlot.V,
			})
		}

		attrs := []any{"method", method, "status", lw.status, "duration", dur}
		if lw.status >= 500 {
			slog.Error("http", attrs...)
		} else if lw.status >= 400 {
			slog.Warn("http", attrs...)
		} else {
			slog.Info("http", attrs...)
		}
	})
}
