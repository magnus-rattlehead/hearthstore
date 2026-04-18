package datastore

import (
	"context"
	"net/http"

	"google.golang.org/protobuf/types/known/timestamppb"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// Lookup fetches entities by key.
func (g *GRPCServer) Lookup(ctx context.Context, req *datastorepb.LookupRequest) (*datastorepb.LookupResponse, error) {
	if req.ProjectId == "" {
		req.ProjectId = defaultProjectFromKey(req.Keys)
	}
	database := req.DatabaseId
	if database == "" {
		database = defaultDatabase
	}

	readAt, activeTxID, newTxIDForResp := g.resolveReadOptions(req.GetReadOptions())

	now := timestamppb.Now()
	resp := &datastorepb.LookupResponse{
		ReadTime:    now,
		Transaction: []byte(newTxIDForResp),
	}
	seen := make(map[string]bool, len(req.Keys))

	for _, key := range req.Keys {
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

		if readAt != nil {
			entity, err := g.store.DsGetAsOf(proj, db, ns, path, *readAt)
			if err != nil {
				resp.Missing = append(resp.Missing, &datastorepb.EntityResult{
					Entity: &datastorepb.Entity{Key: key},
				})
			} else {
				resp.Found = append(resp.Found, &datastorepb.EntityResult{
					Entity: entity,
				})
			}
		} else {
			entity, ver, ct, ut, err := g.store.DsGetWithTimes(proj, db, ns, path)
			if err != nil {
				resp.Missing = append(resp.Missing, &datastorepb.EntityResult{
					Entity: &datastorepb.Entity{Key: key},
				})
			} else {
				resp.Found = append(resp.Found, &datastorepb.EntityResult{
					Entity:     entity,
					Version:    ver,
					CreateTime: ct,
					UpdateTime: ut,
				})
			}
		}
	}

	// Record found entity versions into the transaction's read set for OCC.
	if activeTxID != "" {
		g.txMu.Lock()
		if entry, ok := g.txns[activeTxID]; ok {
			if entry.reads == nil {
				entry.reads = make(map[txReadKey]int64)
			}
			for _, er := range resp.Found {
				proj2, db2, ns2, _, _, path2 := keyComponents(er.Entity.Key)
				if proj2 == "" {
					proj2 = req.ProjectId
				}
				if db2 == "" {
					db2 = database
				}
				entry.reads[txReadKey{proj2, db2, ns2, path2}] = er.Version
			}
			g.txns[activeTxID] = entry
		}
		g.txMu.Unlock()
	}

	return resp, nil
}

// defaultProjectFromKey extracts the project ID from the first key, if any.
func defaultProjectFromKey(keys []*datastorepb.Key) string {
	if len(keys) > 0 {
		return keys[0].GetPartitionId().GetProjectId()
	}
	return ""
}

func (s *Server) handleLookup(w http.ResponseWriter, r *http.Request, project string) {
	var req datastorepb.LookupRequest
	if !readProtoJSON(w, r.Body, &req) {
		return
	}
	if req.ProjectId == "" {
		req.ProjectId = project
	}
	resp, err := s.grpc.Lookup(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	writeProtoJSON(w, resp)
}
