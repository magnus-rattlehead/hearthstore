package datastore

import (
	"context"
	"net/http"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func (g *GRPCServer) AllocateIds(ctx context.Context, req *datastorepb.AllocateIdsRequest) (out *datastorepb.AllocateIdsResponse, resultErr error) {
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
		if err := validateKeyScope(key, req.ProjectId, database, true); err != nil {
			return nil, err
		}
	}

	var allocated []*datastorepb.Key
	err := g.store.RunBatchedTx(ctx, func(tx *storage.Txn) error {
		allocated = make([]*datastorepb.Key, 0, len(req.Keys))
		for _, key := range req.Keys {
			key = scopedKey(key, req.ProjectId, database)
			if !isIncompleteKey(key) {
				allocated = append(allocated, key)
				continue
			}
			proj, db, ns, kind, _, _ := keyComponents(key)
			if proj == "" {
				proj = req.ProjectId
			}
			if db == "" {
				db = database
			}
			allocatedKey, err := g.allocateUnusedKeyTx(tx, proj, db, ns, kind, key)
			if err != nil {
				return err
			}
			allocated = append(allocated, allocatedKey)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &datastorepb.AllocateIdsResponse{Keys: allocated}, nil
}

func (g *GRPCServer) allocateUnusedKeyTx(tx *storage.Txn, project, database, namespace, kind string, incomplete *datastorepb.Key) (*datastorepb.Key, error) {
	for {
		id, err := g.store.DsAllocateIDTx(tx, project, database, namespace, kind)
		if err != nil {
			return nil, err
		}
		key := withID(incomplete, id)
		_, _, _, _, _, path := keyComponents(key)
		_, err = g.store.DsVersionTx(tx, project, database, namespace, path)
		if status.Code(err) == codes.NotFound {
			return key, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func (s *Server) handleAllocateIds(w http.ResponseWriter, r *http.Request, project string) {
	var req datastorepb.AllocateIdsRequest
	if !readProtoJSON(w, r.Body, &req) {
		return
	}
	if req.ProjectId == "" {
		req.ProjectId = project
	}
	SetHTTPDetails(r.Context(), DSAllocateIdsDetails(&req))
	resp, err := s.grpc.AllocateIds(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	MergeHTTPDetails(r.Context(), DSAllocateIdsResponseDetails(resp))
	writeProtoJSON(w, resp)
}
