package datastore

import (
	"context"
	"net/http"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

func (g *GRPCServer) AllocateIds(ctx context.Context, req *datastorepb.AllocateIdsRequest) (*datastorepb.AllocateIdsResponse, error) {
	if req.ProjectId == "" {
		req.ProjectId = defaultProjectFromKey(req.Keys)
	}
	database := req.DatabaseId
	if database == "" {
		database = defaultDatabase
	}

	allocated := make([]*datastorepb.Key, 0, len(req.Keys))
	for _, key := range req.Keys {
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

		first, err := g.store.DsAllocateIds(proj, db, ns, kind, 1)
		if err != nil {
			return nil, err
		}
		allocated = append(allocated, withID(key, first))
	}

	return &datastorepb.AllocateIdsResponse{Keys: allocated}, nil
}

func (s *Server) handleAllocateIds(w http.ResponseWriter, r *http.Request, project string) {
	var req datastorepb.AllocateIdsRequest
	if !readProtoJSON(w, r.Body, &req) {
		return
	}
	if req.ProjectId == "" {
		req.ProjectId = project
	}
	resp, err := s.grpc.AllocateIds(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	writeProtoJSON(w, resp)
}
