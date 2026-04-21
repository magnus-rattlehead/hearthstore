package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
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
	// Prefer $HEARTHSTORE_DATA_DIR, then ~/.hearthstore, then ./data as fallback.
	if v := os.Getenv("HEARTHSTORE_DATA_DIR"); v != "" {
		return v
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".hearthstore")
	}
	return "./data"
}

func main() {
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
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
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
		slog.Info("rebuilding ds_field_index from ds_documents…")
		if err := store.RebuildDsFieldIndex(); err != nil {
			log.Fatalf("reindex failed: %v", err)
		}
		slog.Info("ds_field_index rebuild complete")
	}

	switch *mode {
	case "firestore":
		serveFirestore(*port, *webPort, store)
	case "datastore":
		serveDatastore(*datastoreAddr, store, *indexConfig)
	case "both":
		go serveFirestore(*port, *webPort, store)
		serveDatastore(*datastoreAddr, store, *indexConfig)
	default:
		log.Fatalf("unknown mode %q — use firestore, datastore, or both", *mode)
	}
}

func serveFirestore(port, webPort int, store *storage.Store) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("firestore: failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(loggingUnaryInterceptor),
		grpc.ChainStreamInterceptor(loggingStreamInterceptor),
	)
	fsServer := server.New(store)
	firestorepb.RegisterFirestoreServer(grpcServer, fsServer)
	reflection.Register(grpcServer)
	slog.Info("hearthstore listening", "protocol", "grpc+rest", "port", port)
	slog.Info("set env var", "FIRESTORE_EMULATOR_HOST", fmt.Sprintf("localhost:%d", port))

	if webPort > 0 {
		go serveGRPCWeb(webPort, grpcServer, fsServer)
	}

	// Single port: gRPC, WebChannel (/channel paths), and REST. The Firebase JS
	// SDK sends all traffic to a single FIRESTORE_EMULATOR_HOST address.
	restHandler := server.NewRESTHandler(fsServer)
	wcHandler := webchannel.New(fsServer)
	mixed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
			return
		}
		// WebChannel: paths ending in /channel are Listen or Write long-poll streams.
		if strings.HasSuffix(r.URL.Path, "/channel") {
			// CORS — browser SDK is always cross-origin (e.g. localhost:3000 → localhost:8080).
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

// serveGRPCWeb serves WebChannel (/channel), REST (/v1/...), and gRPC-Web on a separate port.
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
			// CORS — all responses need this; browser SDK is always cross-origin.
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

			// WebChannel: Listen and Write RPCs use our custom handler.
			if strings.HasSuffix(r.URL.Path, "/channel") {
				wcHandler.ServeHTTP(w, r)
				return
			}
			// Firestore REST API: browser SDK sends JSON to /v1/... paths.
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

func loggingUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	logRPC(shortMethod(info.FullMethod), requestAttrs(req), 0, time.Since(start), err)
	return resp, err
}

// countingStream wraps a grpc.ServerStream to:
//   - capture the first RecvMsg (the client request proto) for attribute extraction
//   - count SendMsg calls (number of response messages streamed back)
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

func loggingStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	cs := &countingStream{ServerStream: ss}
	err := handler(srv, cs)
	logRPC(shortMethod(info.FullMethod), requestAttrs(cs.firstReq), cs.sent, time.Since(start), err)
	return err
}

// logRPC emits a structured log line; sent > 0 is included for streaming RPCs.
func logRPC(method string, attrs []any, sent int, dur time.Duration, err error) {
	base := []any{"method", method}
	base = append(base, attrs...)
	if sent > 0 {
		base = append(base, "docs", sent)
	}
	base = append(base, "duration", dur.Round(time.Microsecond))

	if err != nil {
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
// e.g. "/google.firestore.v1.Firestore/RunQuery" → "RunQuery"
func shortMethod(m string) string {
	if i := strings.LastIndex(m, "/"); i >= 0 {
		return m[i+1:]
	}
	return m
}

// docPath extracts the relative document/collection path from a full Firestore
// resource name by stripping everything up to and including "/documents/".
// Returns "" for root-collection parents (names ending at "/documents").
func docPath(s string) string {
	const sep = "/documents/"
	if i := strings.Index(s, sep); i >= 0 {
		return s[i+len(sep):]
	}
	return ""
}

// requestAttrs returns structured log attributes for a Firestore request proto.
// It handles both unary requests (passed directly) and streaming requests
// (captured from the first RecvMsg on the stream wrapper).
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
		// Bidirectional streaming — log the target type if available.
		if at := r.GetAddTarget(); at != nil {
			if q := at.GetQuery(); q != nil {
				return queryAttrs(q.GetParent(), q.GetStructuredQuery())
			}
		}
		return nil
	}
	return nil
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

func serveDatastore(addr string, store *storage.Store, indexConfig string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("datastore: failed to listen: %v", err)
	}
	ds := datastore.New(store)
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(loggingUnaryInterceptor),
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
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
			return
		}
		loggingHTTPMiddleware(ds.Handler()).ServeHTTP(w, r)
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

func loggingHTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lw := &loggingResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(lw, r)
		dur := time.Since(start).Round(time.Microsecond)

		// Extract the RPC method name from the URL path ("/v1/projects/P:method").
		method := r.URL.Path
		if i := strings.LastIndex(method, ":"); i >= 0 {
			method = method[i+1:]
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
