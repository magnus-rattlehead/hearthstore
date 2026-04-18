package datastore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// Server is the HTTP adapter for the Datastore REST API v1.
// All business logic lives in the embedded GRPCServer.
type Server struct {
	grpc *GRPCServer
}

type txReadKey struct {
	project, database, namespace, path string
}

type txEntry struct {
	readOnly bool
	readTime *timestamppb.Timestamp // snapshot time for read-only transactions
	reads    map[txReadKey]int64    // entity path → version at read time, for OCC
}

// checkOCCConflicts verifies that no entity in the read set has been modified
// since it was read. Returns Aborted if any version has increased.
func checkOCCConflicts(tx *sql.Tx, reads map[txReadKey]int64) error {
	for k, readVer := range reads {
		var curVer int64
		err := tx.QueryRow(
			`SELECT version FROM ds_documents WHERE project=? AND database=? AND namespace=? AND path=? AND deleted=0`,
			k.project, k.database, k.namespace, k.path,
		).Scan(&curVer)
		if err == sql.ErrNoRows || curVer > readVer {
			return status.Error(codes.Aborted, "too much contention on these datastore entities. please try again.")
		}
		if err != nil {
			return fmt.Errorf("occ version check: %w", err)
		}
	}
	return nil
}

// New returns a ready Server.
func New(store *storage.Store) *Server {
	return &Server{grpc: newGRPCServer(store)}
}

// NewGRPCServer returns the underlying GRPCServer for direct gRPC registration.
func (s *Server) NewGRPCServer() *GRPCServer { return s.grpc }

// Handler returns an http.Handler that routes all Datastore API requests.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/", s.route)
	return mux
}

// route dispatches /v1/projects/{project}:{method} requests.
func (s *Server) route(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}

	// Path: /v1/projects/{project}:{method}
	path := strings.TrimPrefix(r.URL.Path, "/v1/projects/")
	colon := strings.LastIndex(path, ":")
	if colon < 0 {
		writeErr(w, http.StatusNotFound, "unknown endpoint")
		return
	}
	project := path[:colon]
	method := path[colon+1:]

	switch method {
	case "lookup":
		s.handleLookup(w, r, project)
	case "runQuery":
		s.handleRunQuery(w, r, project)
	case "runAggregationQuery":
		s.handleRunAggregationQuery(w, r, project)
	case "beginTransaction":
		s.handleBeginTransaction(w, r, project)
	case "commit":
		s.handleCommit(w, r, project)
	case "rollback":
		s.handleRollback(w, r, project)
	case "allocateIds":
		s.handleAllocateIds(w, r, project)
	case "reserveIds":
		s.handleReserveIds(w, r, project)
	default:
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
	status := httpStatusName(code)
	resp := errResp{}
	resp.Error.Code = code
	resp.Error.Status = status
	resp.Error.Message = msg
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func httpStatusName(code int) string {
	switch code {
	case 400:
		return "INVALID_ARGUMENT"
	case 404:
		return "NOT_FOUND"
	case 409:
		return "ALREADY_EXISTS"
	case 412:
		return "FAILED_PRECONDITION"
	case 500:
		return "INTERNAL"
	case 501:
		return "UNIMPLEMENTED"
	default:
		return http.StatusText(code)
	}
}
