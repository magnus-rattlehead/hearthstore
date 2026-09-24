package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	adminpb "cloud.google.com/go/datastore/admin/apiv1/adminpb"
	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/magnus-rattlehead/hearthstore/internal/datastore"
	"github.com/magnus-rattlehead/hearthstore/internal/importexport"
	"github.com/magnus-rattlehead/hearthstore/internal/server"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

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
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	startTime := time.Now()

	datastoreAddr := flag.String("datastore-addr", ":8456", "HTTP listen address (Datastore REST API)")
	dataDir := flag.String("data-dir", "", "Directory for Badger data files (default: ~/.hearthstore)")
	logLevel := flag.String("log-level", "info", "Log level: debug | info | warn | error")
	indexConfig := flag.String("index-config", "", "Path to index.yaml for Datastore composite index configuration")
	indexCacheMaxMB := flag.Int64("index-cache-max-mb", storage.DefaultIndexCacheBytes>>20, "Badger index and Bloom-filter cache budget in MiB (provisional default)")
	blockCacheMaxMB := flag.Int64("block-cache-max-mb", storage.DefaultBlockCacheBytes>>20, "Badger block cache memory budget in MiB")
	exactQueryMemoryMaxMB := flag.Int64("exact-query-memory-max-mb", storage.DefaultExactQueryMemoryBytes>>20, "Maximum scratch-owned memory for exact queries in MiB")
	queryConcurrency := flag.Int("query-concurrency", storage.DefaultQueryConcurrency, "Maximum concurrent query and aggregation RPCs")
	importData := flag.String("import-data", "", "Datastore export metadata file or directory to import before serving")
	exportOnExit := flag.String("export-on-exit", "", "Directory to receive a Datastore export during graceful shutdown")
	projectID := flag.String("project-id", "", "Project ID used by import and export operations")
	databaseID := flag.String("database-id", "(default)", "Database ID used by import and export operations")
	prepareStorage := flag.Bool("prepare-storage", false, "Check storage compatibility, offer to delete incompatible data interactively, then exit")
	flag.Parse()
	if err := validateTransferFlags(*projectID, *importData, *exportOnExit); err != nil {
		return err
	}
	if *databaseID == "" {
		return fmt.Errorf("database ID must not be empty")
	}
	if *dataDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("failed to resolve default data directory: %v; use -data-dir", err)
		}
		*dataDir = filepath.Join(home, ".hearthstore")
	}
	if *indexCacheMaxMB <= 0 || *indexCacheMaxMB > math.MaxInt64/(1024*1024) {
		return fmt.Errorf("invalid index cache budget %d MiB", *indexCacheMaxMB)
	}
	if *blockCacheMaxMB <= 0 || *blockCacheMaxMB > math.MaxInt64/(1024*1024) {
		return fmt.Errorf("invalid block cache budget %d MiB", *blockCacheMaxMB)
	}
	if *exactQueryMemoryMaxMB <= 0 || *exactQueryMemoryMaxMB > math.MaxInt64/(1024*1024) {
		return fmt.Errorf("invalid exact-query memory maximum %d MiB", *exactQueryMemoryMaxMB)
	}
	if *queryConcurrency <= 0 {
		return fmt.Errorf("invalid query concurrency %d: must be positive", *queryConcurrency)
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("invalid log level %q: %v", *logLevel, err)
	}
	logger := slog.New(slog.NewTextHandler(prefixWriter{os.Stderr}, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %v", err)
	}
	if *prepareStorage {
		if err := storage.CheckCompatibility(*dataDir); err == nil {
			return
		} else if !errors.Is(err, storage.ErrIncompatibleData) {
			return fmt.Errorf("failed to check storage: %v", err)
		}
		stdinInfo, _ := os.Stdin.Stat()
		store, err := openStorageWithRecovery(
			*dataDir,
			os.Stdin,
			os.Stderr,
			stdinInfo != nil && stdinInfo.Mode()&os.ModeCharDevice != 0,
			storage.OpenOptions{BlockCacheBytes: *blockCacheMaxMB << 20, IndexCacheBytes: *indexCacheMaxMB << 20},
		)
		if err != nil {
			return fmt.Errorf("failed to prepare storage: %v", err)
		}
		if err := store.Close(); err != nil {
			return fmt.Errorf("failed to close prepared storage: %v", err)
		}
		return
	}
	fmt.Fprint(os.Stderr, logo)

	storageProgress := beginStartupProgress(
		"opening Badger storage",
		"data_dir", *dataDir,
	)
	stdinInfo, _ := os.Stdin.Stat()
	store, err := openStorageWithRecovery(
		*dataDir,
		os.Stdin,
		os.Stderr,
		stdinInfo != nil && stdinInfo.Mode()&os.ModeCharDevice != 0,
		storage.OpenOptions{BlockCacheBytes: *blockCacheMaxMB << 20, IndexCacheBytes: *indexCacheMaxMB << 20},
	)
	if err != nil {
		storageProgress.halt()
		return fmt.Errorf("failed to open storage: %v", err)
	}
	if err := store.ConfigureBlockCache(*blockCacheMaxMB * 1024 * 1024); err != nil {
		_ = store.Close()
		storageProgress.halt()
		return fmt.Errorf("failed to configure block cache: %v", err)
	}
	if err := store.ConfigureExactQueries(*exactQueryMemoryMaxMB*1024*1024, *queryConcurrency); err != nil {
		_ = store.Close()
		storageProgress.halt()
		return fmt.Errorf("failed to configure exact queries: %v", err)
	}
	storageProgress.complete(time.Now())
	defer func() { runErr = errors.Join(runErr, store.Close()) }()

	indexManager := datastore.NewIndexManager(store)
	indexProgress := beginStartupProgress("loading and building Datastore composite indexes")
	if err := indexManager.LoadIndexFiles(context.Background(), *indexConfig); err != nil {
		indexProgress.halt()
		return fmt.Errorf("failed to initialize Datastore indexes: %v", err)
	}
	indexProgress.complete(time.Now())
	if *importData != "" {
		importProgress := beginStartupProgress("importing Datastore export", "source", *importData)
		stats, err := importexport.Import(context.Background(), store, *importData, *projectID, *databaseID)
		if err != nil {
			importProgress.halt()
			return fmt.Errorf("failed to import Datastore export: %v", err)
		}
		importProgress.complete(time.Now())
		slog.Info("Datastore import complete", "entities", stats.Entities, "bytes", stats.Bytes)
	}

	ops := server.NewOperationLog(startTime.Format("20060102T150405.000000000Z0700"))
	dash := server.NewDashboard(store, ops)
	unary := makeGRPCInterceptor(ops)
	shutdownContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	if err := serveDatastore(shutdownContext, *datastoreAddr, store, indexManager, ops, dash, unary, *queryConcurrency); err != nil {
		return fmt.Errorf("datastore: serve error: %v", err)
	}
	if *exportOnExit != "" {
		slog.Info("exporting Datastore data before shutdown", "directory", *exportOnExit)
		metadata, stats, err := importexport.Export(context.Background(), store, *exportOnExit, *projectID, *databaseID, time.Now().UTC())
		if err != nil {
			return fmt.Errorf("failed to export Datastore data during shutdown: %v", err)
		}
		slog.Info("Datastore export complete", "metadata", metadata, "entities", stats.Entities, "bytes", stats.Bytes)
	}
	return nil
}

func validateTransferFlags(projectID, importData, exportOnExit string) error {
	if projectID == "" && (importData != "" || exportOnExit != "") {
		return fmt.Errorf("-project-id is required with -import-data or -export-on-exit")
	}
	return nil
}

func openStorageWithRecovery(dataDir string, in io.Reader, out io.Writer, canPrompt bool, options ...storage.OpenOptions) (*storage.Store, error) {
	var config storage.OpenOptions
	if len(options) > 0 {
		config = options[0]
	}
	store, err := storage.NewWithOptions(dataDir, config)
	if !errors.Is(err, storage.ErrIncompatibleData) {
		return store, err
	}
	if !canPrompt {
		_, writeErr := fmt.Fprintf(out, "Incompatible Hearthstore database in %q; it was left untouched. Run in a terminal to confirm deletion, or reimport the original Datastore export into a new -data-dir.\n", dataDir)
		return nil, errors.Join(err, writeErr)
	}
	if _, writeErr := fmt.Fprintf(out, "\nIncompatible Hearthstore database found in %q.\nThis permanently deletes all stored entities and indexes.\nDelete it and create a new empty database? [y/N] ", dataDir); writeErr != nil {
		return nil, errors.Join(err, fmt.Errorf("writing database reset confirmation: %w", writeErr))
	}
	answer, readErr := bufio.NewReader(in).ReadString('\n')
	if readErr != nil {
		return nil, errors.Join(err, fmt.Errorf("reading database reset confirmation: %w", readErr))
	}
	answer = strings.ToLower(strings.TrimSpace(answer))
	if answer != "y" && answer != "yes" {
		return nil, err
	}
	// Recheck after the prompt: never reset storage that is now compatible.
	if checkErr := storage.CheckCompatibility(dataDir); !errors.Is(checkErr, storage.ErrIncompatibleData) {
		if checkErr != nil {
			return nil, checkErr
		}
		return storage.NewWithOptions(dataDir, config)
	}
	// Delete only storage-owned paths, preserving exports and configuration.
	for _, name := range []string{"badger", "scratch", "hearthstore.db", "hearthstore.db-wal", "hearthstore.db-shm", "storage-format"} {
		if removeErr := os.RemoveAll(filepath.Join(dataDir, name)); removeErr != nil {
			return nil, fmt.Errorf("deleting incompatible database %s: %w", name, removeErr)
		}
	}
	return storage.NewWithOptions(dataDir, config)
}

func makeGRPCInterceptor(ops *server.OperationLog) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		dur := time.Since(start)
		details := requestDetails(req)
		if err == nil {
			details = mergeDetails(details, responseDetails(resp, dur))
		}
		logRPC(ops, shortMethod(info.FullMethod), details, dur, err)
		return resp, err
	}
}

func logRPC(ops *server.OperationLog, method string, details map[string]any, dur time.Duration, err error) {
	isErr := err != nil
	if ops != nil {
		entry := server.OperationEntry{
			T:         time.Now().Add(-dur),
			Source:    "grpc",
			Method:    method,
			LatencyMs: dur.Milliseconds(),
			Details:   details,
		}
		if err != nil {
			entry.Err = grpcstatus.Convert(err).Message()
		}
		ops.Add(entry)
	}

	attrs := []any{"method", method, "duration", dur.Round(time.Microsecond)}
	if isErr {
		code := grpcstatus.Code(err)
		attrs = append(attrs, "code", code, "err", grpcstatus.Convert(err).Message())
		if code == codes.Internal || code == codes.Unknown {
			slog.Error("rpc", attrs...)
		} else {
			slog.Warn("rpc", attrs...)
		}
		return
	}
	slog.Info("rpc", attrs...)
}

func shortMethod(method string) string {
	if i := strings.LastIndex(method, "/"); i >= 0 {
		return method[i+1:]
	}
	return method
}

func requestDetails(req any) map[string]any {
	switch typed := req.(type) {
	case *datastorepb.LookupRequest:
		return datastore.DSLookupDetails(typed)
	case *datastorepb.RunQueryRequest:
		return datastore.DSQueryDetails(typed)
	case *datastorepb.RunAggregationQueryRequest:
		return datastore.DSAggregationQueryDetails(typed)
	case *datastorepb.CommitRequest:
		return datastore.DSMutationDetails(typed)
	case *datastorepb.BeginTransactionRequest:
		return datastore.DSBeginTxDetails(typed)
	case *datastorepb.AllocateIdsRequest:
		return datastore.DSAllocateIdsDetails(typed)
	case *datastorepb.ReserveIdsRequest:
		return datastore.DSReserveIdsDetails(typed)
	case *datastorepb.RollbackRequest:
		return datastore.DSRollbackDetails(typed)
	default:
		return nil
	}
}

func responseDetails(resp any, elapsed time.Duration) map[string]any {
	switch typed := resp.(type) {
	case *datastorepb.LookupResponse:
		return datastore.DSLookupResponseDetails(typed, elapsed)
	case *datastorepb.CommitResponse:
		return datastore.DSCommitResponseDetails(typed, elapsed)
	case *datastorepb.RunQueryResponse:
		return datastore.DSQueryResponseDetails(typed, elapsed)
	case *datastorepb.RunAggregationQueryResponse:
		return datastore.DSAggregationResponseDetails(typed, elapsed)
	case *datastorepb.BeginTransactionResponse:
		return datastore.DSBeginTxResponseDetails(typed)
	case *datastorepb.AllocateIdsResponse:
		return datastore.DSAllocateIdsResponseDetails(typed)
	default:
		return nil
	}
}

func mergeDetails(request, response map[string]any) map[string]any {
	if len(request) == 0 && len(response) == 0 {
		return nil
	}
	merged := make(map[string]any, len(request)+len(response))
	for key, value := range request {
		merged[key] = value
	}
	for key, value := range response {
		merged[key] = value
	}
	return merged
}

func serveDatastore(ctx context.Context, addr string, store *storage.Store, indexManager *datastore.IndexManager, ops *server.OperationLog, dash http.Handler, unary grpc.UnaryServerInterceptor, queryConcurrency int) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", addr, err)
	}
	ds := datastore.NewWithOptions(store, indexManager, datastore.Options{QueryConcurrency: queryConcurrency})
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unary),
		grpc.MaxRecvMsgSize(10<<20),
	)
	defer grpcServer.Stop()
	datastorepb.RegisterDatastoreServer(grpcServer, ds.NewGRPCServer())
	adminSrv := datastore.NewAdminServer(store, indexManager)
	adminpb.RegisterDatastoreAdminServer(grpcServer, adminSrv)
	longrunningpb.RegisterOperationsServer(grpcServer, adminSrv.Operations())
	reflection.Register(grpcServer)
	slog.Info("hearthstore listening", "protocol", "grpc+http", "addr", lis.Addr().String())
	slog.Info("set env var", "DATASTORE_EMULATOR_HOST", lis.Addr().String())

	mixed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/_/") {
			dash.ServeHTTP(w, r)
			return
		}
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
			return
		}
		loggingHTTPMiddleware(ds.Handler(), ops).ServeHTTP(w, r)
	})
	gate := newRequestDrain(mixed)
	requestsCtx, cancelRequests := context.WithCancel(context.Background())
	defer cancelRequests()
	httpSrv := &http.Server{
		Handler:     h2c.NewHandler(gate, &http2.Server{}),
		BaseContext: func(net.Listener) context.Context { return requestsCtx },
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- httpSrv.Serve(lis) }()
	select {
	case err := <-serveResult:
		gate.beginDrain()
		cancelRequests()
		grpcServer.Stop()
		closeErr := httpSrv.Close()
		if drainErr := gate.wait(context.Background()); drainErr != nil {
			return errors.Join(err, closeErr, drainErr)
		}
		if errors.Is(err, http.ErrServerClosed) {
			return closeErr
		}
		return errors.Join(err, closeErr)
	case <-ctx.Done():
		slog.Info("shutdown signal received; draining requests")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		gate.beginDrain()
		shutdownErr := httpSrv.Shutdown(shutdownCtx)
		drainErr := gate.wait(shutdownCtx)
		if shutdownErr != nil || drainErr != nil {
			cancelRequests()
			grpcServer.Stop()
			closeErr := httpSrv.Close()
			// Never close Badger while canceled handlers are still using it.
			waitErr := gate.wait(context.Background())
			return fmt.Errorf("draining server: %w", errors.Join(shutdownErr, drainErr, closeErr, waitErr))
		}
		grpcServer.Stop()
		err := <-serveResult
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *loggingResponseWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func loggingHTTPMiddleware(next http.Handler, ops *server.OperationLog) http.Handler {
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

		if ops != nil && !strings.HasPrefix(r.URL.Path, "/_/") {
			e := server.OperationEntry{
				T:         start,
				Source:    "datastore_http",
				Method:    method,
				Path:      r.URL.Path,
				LatencyMs: dur.Milliseconds(),
				Status:    lw.status,
				Details:   detailSlot.V,
			}
			if lw.status >= 400 {
				e.Err = http.StatusText(lw.status)
			}
			ops.Add(e)
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
