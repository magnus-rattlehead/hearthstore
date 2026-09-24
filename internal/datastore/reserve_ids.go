package datastore

import (
	"context"
	"net/http"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func (g *GRPCServer) ReserveIds(ctx context.Context, req *datastorepb.ReserveIdsRequest) (out *datastorepb.ReserveIdsResponse, resultErr error) {
	defer func() { resultErr = rpcError(resultErr) }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.store.CheckAvailable(); err != nil {
		return nil, err
	}
	if req.GetProjectId() == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id is required")
	}
	database := req.DatabaseId
	if database == "" {
		database = defaultDatabase
	}
	for _, key := range req.Keys {
		if err := validateKeyScope(key, req.ProjectId, database, false); err != nil {
			return nil, err
		}
	}
	err := g.store.RunBatchedTx(ctx, func(tx *storage.Txn) error {
		for _, key := range req.Keys {
			parts := key.GetPath()
			if len(parts) == 0 || isIncompleteKey(key) || parts[len(parts)-1].GetId() <= 0 {
				return status.Error(codes.InvalidArgument, "reserveIds requires complete numeric keys")
			}
			proj, db, ns, kind, _, _ := keyComponents(key)
			if proj == "" {
				proj = req.ProjectId
			}
			if db == "" {
				db = database
			}
			if err := g.store.DsReserveIDTx(tx, proj, db, ns, kind, parts[len(parts)-1].GetId()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &datastorepb.ReserveIdsResponse{}, nil
}

func (s *Server) handleReserveIds(w http.ResponseWriter, r *http.Request, project string) {
	var req datastorepb.ReserveIdsRequest
	if !readProtoJSON(w, r.Body, &req) {
		return
	}
	if req.ProjectId == "" {
		req.ProjectId = project
	}
	SetHTTPDetails(r.Context(), DSReserveIdsDetails(&req))
	resp, err := s.grpc.ReserveIds(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	writeProtoJSON(w, resp)
}
