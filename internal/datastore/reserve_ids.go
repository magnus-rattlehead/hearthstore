package datastore

import (
	"context"
	"net/http"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// ReserveIds is a no-op. The emulator doesn't enforce ID reservations.
func (g *GRPCServer) ReserveIds(ctx context.Context, req *datastorepb.ReserveIdsRequest) (*datastorepb.ReserveIdsResponse, error) {
	return &datastorepb.ReserveIdsResponse{}, nil
}

func (s *Server) handleReserveIds(w http.ResponseWriter, r *http.Request, project string) {
	var req datastorepb.ReserveIdsRequest
	if !readProtoJSON(w, r.Body, &req) {
		return
	}
	resp, err := s.grpc.ReserveIds(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	writeProtoJSON(w, resp)
}
