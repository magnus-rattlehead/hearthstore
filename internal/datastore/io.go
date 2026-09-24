package datastore

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	codepb "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var pjsonUnmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}
var pjsonMarshal = protojson.MarshalOptions{EmitUnpopulated: false}

// readProtoJSON reads the HTTP body and unmarshals it into msg.
// Writes an error response and returns false on failure.
func readProtoJSON(w http.ResponseWriter, body io.Reader, msg proto.Message) bool {
	data, err := io.ReadAll(body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeStatusErr(w, http.StatusRequestEntityTooLarge, "RESOURCE_EXHAUSTED", "request exceeds the 10 MiB Datastore API limit")
			return false
		}
		writeErr(w, http.StatusInternalServerError, "read body: "+err.Error())
		return false
	}
	if len(data) == 0 {
		return true // empty body is valid (e.g. BeginTransaction)
	}
	if err := pjsonUnmarshal.Unmarshal(data, msg); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}

// writeProtoJSON marshals msg to JSON and writes it to w.
func writeProtoJSON(w http.ResponseWriter, msg proto.Message) {
	out, err := pjsonMarshal.Marshal(msg)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "marshal response: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(out); err != nil {
		slog.Warn("Response delivery failed; committed writes are unchanged", "error", err)
	}
}

var grpcHTTPStatus = map[codes.Code]int{
	codes.Canceled:           499,
	codes.DeadlineExceeded:   http.StatusGatewayTimeout,
	codes.Unavailable:        http.StatusServiceUnavailable,
	codes.ResourceExhausted:  http.StatusTooManyRequests,
	codes.PermissionDenied:   http.StatusForbidden,
	codes.Unauthenticated:    http.StatusUnauthorized,
	codes.OutOfRange:         http.StatusBadRequest,
	codes.NotFound:           http.StatusNotFound,
	codes.AlreadyExists:      http.StatusConflict,
	codes.Aborted:            http.StatusConflict,
	codes.InvalidArgument:    http.StatusBadRequest,
	codes.FailedPrecondition: http.StatusBadRequest,
	codes.Unimplemented:      http.StatusNotImplemented,
}

// grpcToHTTP converts a gRPC status error to an HTTP status code.
func grpcToHTTP(err error) int {
	if code, ok := grpcHTTPStatus[status.Code(err)]; ok {
		return code
	}
	return http.StatusInternalServerError
}

// writeGrpcErr converts a gRPC error to an HTTP error response.
func writeGrpcErr(w http.ResponseWriter, err error) {
	err = rpcError(err)
	grpcStatus := status.Convert(err)
	writeStatusErr(w, grpcToHTTP(err), codepb.Code(grpcStatus.Code()).String(), grpcStatus.Message())
}

// rpcFailure preserves errors.Is/As locally while exposing a canonical, safe wire error.
type rpcFailure struct {
	cause error
	wire  *status.Status
}

func (e *rpcFailure) Error() string              { return e.wire.Err().Error() }
func (e *rpcFailure) Unwrap() error              { return e.cause }
func (e *rpcFailure) GRPCStatus() *status.Status { return e.wire }

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	var mapped *rpcFailure
	if errors.As(err, &mapped) {
		return err
	}
	var imported *storage.ImportError
	if errors.As(err, &imported) {
		slog.Error("Import failed", "outcome", imported.Outcome, "error", err)
		code := codes.Internal
		message := "import outcome unresolved; restart Hearthstore for recovery before retrying"
		if imported.Outcome == "committed" {
			message = "import committed; journal cleanup failed; do not assume retry is needed"
		}
		if imported.Outcome == "not started" {
			message = "new import not started; previous committed import journal cleanup failed"
		}
		if imported.Outcome == "rolled back" {
			message = "import rolled back; consult server logs for the cause"
			if cause, ok := status.FromError(imported.Err); ok {
				code, message = cause.Code(), "import rolled back: "+cause.Message()
			} else if errors.Is(imported.Err, context.Canceled) || errors.Is(imported.Err, context.DeadlineExceeded) {
				cause := status.FromContextError(imported.Err)
				code, message = cause.Code(), "import rolled back: "+cause.Message()
			}
		}
		return &rpcFailure{cause: err, wire: status.New(code, message)}
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &rpcFailure{cause: err, wire: status.FromContextError(err)}
	}
	slog.Error("Datastore operation failed", "error", err)
	return &rpcFailure{cause: err, wire: status.New(codes.Internal, "storage operation failed; consult server logs; writes may have committed")}
}
