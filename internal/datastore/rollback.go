package datastore

import (
	"context"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

func (g *GRPCServer) Rollback(ctx context.Context, req *datastorepb.RollbackRequest) (out *datastorepb.RollbackResponse, resultErr error) {
	defer func() { resultErr = rpcError(resultErr) }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.store.CheckAvailable(); err != nil {
		return nil, err
	}
	txID := string(req.Transaction)
	g.txMu.Lock()
	_, ok := g.txns[txID]
	if ok {
		delete(g.txns, txID)
	}
	g.txMu.Unlock()

	if !ok {
		return nil, status.Error(codes.NotFound, "transaction not found: "+txID)
	}

	return &datastorepb.RollbackResponse{}, nil
}

func (s *Server) handleRollback(w http.ResponseWriter, r *http.Request, project string) {
	var req datastorepb.RollbackRequest
	if !readProtoJSON(w, r.Body, &req) {
		return
	}
	SetHTTPDetails(r.Context(), DSRollbackDetails(&req))
	resp, err := s.grpc.Rollback(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	writeProtoJSON(w, resp)
}
