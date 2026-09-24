package datastore

import (
	"context"
	"net/http"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

const maxLookupResponseBytes = 4 << 20

// Lookup fetches entities by key.
func (g *GRPCServer) Lookup(ctx context.Context, req *datastorepb.LookupRequest) (out *datastorepb.LookupResponse, resultErr error) {
	defer func() { resultErr = rpcError(resultErr) }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.store.CheckAvailable(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "lookup request is required")
	}
	if len(req.Keys) > maxLookupKeys {
		return nil, status.Errorf(codes.InvalidArgument, "lookup contains %d keys; maximum is %d", len(req.Keys), maxLookupKeys)
	}
	if req.ProjectId == "" {
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

	readAt, activeTxID, newTxIDForResp, err := g.resolveReadOptions(req.GetReadOptions(), req.ProjectId, database)
	if err != nil {
		return nil, err
	}
	if readAt == nil {
		snapshot := g.store.ReadTime()
		readAt = &snapshot
	}
	releaseSnapshot, err := g.store.PinReadTime(*readAt)
	if err != nil {
		return nil, err
	}
	defer releaseSnapshot()

	now := timestamppb.Now()
	if readAt != nil {
		now = timestamppb.New(*readAt)
	}
	resp := &datastorepb.LookupResponse{
		ReadTime:    now,
		Transaction: []byte(newTxIDForResp),
	}
	responseBytes := proto.Size(resp)
	appendResult := func(result *datastorepb.EntityResult, found bool) bool {
		key := result.GetEntity().GetKey()
		cost := messageFieldSize(1, proto.Size(result))
		reserved := messageFieldSize(3, proto.Size(key))
		if responseBytes-reserved+cost > maxLookupResponseBytes {
			resp.Deferred = append(resp.Deferred, key)
			return false
		}
		responseBytes += cost - reserved
		if found {
			resp.Found = append(resp.Found, result)
		} else {
			resp.Missing = append(resp.Missing, result)
		}
		return true
	}
	seen := make(map[string]bool, len(req.Keys))

	// Deduplicate keys and group by (project, database, namespace) for batch fetch.
	type nsKey struct{ project, database, namespace string }
	type keyMeta struct {
		key  *datastorepb.Key
		path string
	}
	groups := make(map[nsKey][]keyMeta)
	for _, key := range req.Keys {
		key = scopedKey(key, req.ProjectId, database)
		ks := keyString(key)
		if seen[ks] {
			continue
		}
		seen[ks] = true

		proj, db, ns, _, _, path := keyComponents(key)
		if proj == "" {
			proj = req.ProjectId
		}
		if db == "" {
			db = database
		}

		nk := nsKey{proj, db, ns}
		groups[nk] = append(groups[nk], keyMeta{key, path})
		responseBytes += messageFieldSize(3, proto.Size(key))
	}

	// Visit one entity at a time so decoded entities are bounded by the exact
	// serialized response ceiling rather than an arbitrary fetch batch.
	for nk, metas := range groups {
		paths := make([]string, len(metas))
		for i, meta := range metas {
			paths[i] = meta.path
		}
		visited := 0
		visit := func(row *storage.DsEntityRow, _ string) bool {
			meta := metas[visited]
			visited++
			if row == nil {
				return appendResult(&datastorepb.EntityResult{Entity: &datastorepb.Entity{Key: meta.key}}, false)
			}
			return appendResult(&datastorepb.EntityResult{Entity: row.Entity, Version: row.Version, CreateTime: row.CreateTime, UpdateTime: row.UpdateTime}, true)
		}
		var getErr error
		if readAt == nil {
			getErr = g.store.DsVisitManyWithTimes(ctx, nk.project, nk.database, nk.namespace, paths, visit)
		} else {
			getErr = g.store.DsVisitManyWithTimesAsOf(ctx, *readAt, nk.project, nk.database, nk.namespace, paths, visit)
		}
		if getErr != nil {
			return nil, getErr
		}
		for ; visited < len(metas); visited++ {
			resp.Deferred = append(resp.Deferred, metas[visited].key)
		}
	}

	// Record found entity versions into the transaction's read set for OCC.
	if activeTxID != "" {
		reads := make(map[txReadKey]int64, len(resp.Found))
		for _, er := range append(append([]*datastorepb.EntityResult{}, resp.Found...), resp.Missing...) {
			proj2, db2, ns2, _, _, path2 := keyComponents(er.Entity.Key)
			if proj2 == "" {
				proj2 = req.ProjectId
			}
			if db2 == "" {
				db2 = database
			}
			reads[txReadKey{proj2, db2, ns2, path2}] = er.Version
		}
		if err := g.recordTransactionReads(activeTxID, reads); err != nil {
			return nil, err
		}
	}

	if proto.Size(resp) > maxLookupResponseBytes {
		return nil, status.Error(codes.ResourceExhausted, "lookup keys exceed response size limit")
	}
	return resp, nil
}

func (s *Server) handleLookup(w http.ResponseWriter, r *http.Request, project string) {
	var req datastorepb.LookupRequest
	if !readProtoJSON(w, r.Body, &req) {
		return
	}
	if req.ProjectId == "" {
		req.ProjectId = project
	}
	start := time.Now()
	SetHTTPDetails(r.Context(), DSLookupDetails(&req))
	resp, err := s.grpc.Lookup(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	MergeHTTPDetails(r.Context(), DSLookupResponseDetails(resp, time.Since(start)))
	writeProtoJSON(w, resp)
}
