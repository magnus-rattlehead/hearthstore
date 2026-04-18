package datastore

import (
	"io"
	"net/http"

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
	_, _ = w.Write(out)
}

// grpcToHTTP converts a gRPC status error to an HTTP status code.
func grpcToHTTP(err error) int {
	switch status.Code(err) {
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.Aborted:
		return http.StatusConflict
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.FailedPrecondition:
		return http.StatusPreconditionFailed
	case codes.Unimplemented:
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}

// writeGrpcErr converts a gRPC error to an HTTP error response.
func writeGrpcErr(w http.ResponseWriter, err error) {
	writeErr(w, grpcToHTTP(err), status.Convert(err).Message())
}
