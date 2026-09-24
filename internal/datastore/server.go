package datastore

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/magnus-rattlehead/hearthstore/internal/importexport"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

const (
	maxAPIRequestBytes = 10 << 20
	maxLookupKeys      = 1_000
	maxTransactionSize = 10 << 20
)

// Server is the HTTP adapter for the Datastore REST API v1.
// All business logic lives in the embedded GRPCServer.
type Server struct {
	grpc *GRPCServer
}

var dsOps = map[string]func(*Server, http.ResponseWriter, *http.Request, string){
	"lookup":              (*Server).handleLookup,
	"runQuery":            (*Server).handleRunQuery,
	"runAggregationQuery": (*Server).handleRunAggregationQuery,
	"beginTransaction":    (*Server).handleBeginTransaction,
	"commit":              (*Server).handleCommit,
	"rollback":            (*Server).handleRollback,
	"allocateIds":         (*Server).handleAllocateIds,
	"reserveIds":          (*Server).handleReserveIds,
}

type txReadKey struct {
	project, database, namespace, path string
}

type txEntry struct {
	project, database string
	queries           map[storage.QueryScope]struct{}
	readOnly          bool
	readTime          *timestamppb.Timestamp // snapshot time for read-only transactions
	reads             map[txReadKey]int64    // entity path -> version at read time, for OCC
	readBytes         int
	created           time.Time
	lastUsed          time.Time
}

// checkOCCConflicts rejects read-set entries whose stored version changed.
func (g *GRPCServer) checkOCCConflicts(tx *storage.Txn, reads map[txReadKey]int64) error {
	for key, version := range reads {
		current, err := g.store.DsVersionTx(tx, key.project, key.database, key.namespace, key.path)
		if status.Code(err) == codes.NotFound {
			current, err = 0, nil
		}
		if err != nil {
			return err
		}
		if current != version {
			return status.Error(codes.Aborted, "too much contention on these datastore entities. please try again.")
		}
	}
	return nil
}

// New returns a ready Server.
func New(store *storage.Store) *Server {
	return &Server{grpc: newGRPCServer(store, NewIndexManager(store))}
}

// NewWithIndexManager returns a Server using the shared query/admin index manager.
func NewWithIndexManager(store *storage.Store, indexes *IndexManager) *Server {
	return &Server{grpc: newGRPCServer(store, indexes)}
}

// NewWithOptions returns a Server with explicit query resource limits.
func NewWithOptions(store *storage.Store, indexes *IndexManager, options Options) *Server {
	return &Server{grpc: newGRPCServerWithOptions(store, indexes, options)}
}

// NewGRPCServer returns the underlying GRPCServer for direct gRPC registration.
func (s *Server) NewGRPCServer() *GRPCServer { return s.grpc }

// Handler returns an http.Handler that routes all Datastore API requests.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/", s.route)
	mux.HandleFunc("/emulator/v1/projects/", s.routeEmulatorOperation)
	return mux
}

type emulatorOperationRequest struct {
	Database        string `json:"database"`
	ExportDirectory string `json:"export_directory"`
}

func (s *Server) routeEmulatorOperation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxAPIRequestBytes)
	path := strings.TrimPrefix(r.URL.Path, "/emulator/v1/projects/")
	colon := strings.LastIndexByte(path, ':')
	if colon <= 0 {
		writeErr(w, http.StatusNotFound, "unknown endpoint")
		return
	}
	project, operation := path[:colon], path[colon+1:]
	if operation != "import" && operation != "export" {
		writeErr(w, http.StatusNotFound, "unknown method: "+operation)
		return
	}
	var request emulatorOperationRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	database, err := emulatorDatabaseID(project, request.Database)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if request.ExportDirectory == "" {
		writeErr(w, http.StatusBadRequest, "export_directory is required")
		return
	}

	switch operation {
	case "export":
		_, _, err = importexport.Export(r.Context(), s.grpc.store, request.ExportDirectory, project, database, time.Now().UTC())
	case "import":
		_, err = importexport.Import(r.Context(), s.grpc.store, request.ExportDirectory, project, database)
	}
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(struct{}{}); err != nil {
		slog.Warn("Emulator operation response delivery failed; committed data unchanged", "error", err)
	}
}

func emulatorDatabaseID(project, resource string) (string, error) {
	prefix := "projects/" + project + "/databases/"
	if !strings.HasPrefix(resource, prefix) {
		return "", fmt.Errorf("database must belong to project %q", project)
	}
	database := strings.TrimPrefix(resource, prefix)
	if database == "" {
		return defaultDatabase, nil
	}
	if strings.Contains(database, "/") {
		return "", fmt.Errorf("invalid database resource %q", resource)
	}
	return database, nil
}

// route dispatches /v1/projects/{project}:{method} requests.
func (s *Server) route(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxAPIRequestBytes)

	// Path: /v1/projects/{project}:{method}
	path := strings.TrimPrefix(r.URL.Path, "/v1/projects/")
	colon := strings.LastIndex(path, ":")
	if colon < 0 {
		writeErr(w, http.StatusNotFound, "unknown endpoint")
		return
	}
	project := path[:colon]
	method := path[colon+1:]

	if fn, ok := dsOps[method]; ok {
		fn(s, w, r, project)
	} else {
		writeErr(w, http.StatusNotFound, "unknown method: "+method)
	}
}

// errResp is the Google API error envelope.
type errResp struct {
	Error struct {
		Code    int    `json:"code"`
		Status  string `json:"status"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeStatusErr(w, code, httpStatusName(code), msg)
}

func writeStatusErr(w http.ResponseWriter, code int, status, msg string) {
	resp := errResp{}
	resp.Error.Code = code
	resp.Error.Status = status
	resp.Error.Message = msg
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Warn("Error response delivery failed", "error", err)
	}
}

var httpStatusNames = map[int]string{
	400: "INVALID_ARGUMENT",
	404: "NOT_FOUND",
	409: "ALREADY_EXISTS",
	412: "FAILED_PRECONDITION",
	500: "INTERNAL",
	501: "UNIMPLEMENTED",
}

func httpStatusName(code int) string {
	if s, ok := httpStatusNames[code]; ok {
		return s
	}
	return http.StatusText(code)
}
