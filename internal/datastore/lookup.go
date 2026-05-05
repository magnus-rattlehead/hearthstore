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

	// Deduplicate keys and group by (project, database, namespace) for batch fetch.
	type nsKey struct{ project, database, namespace string }
	type keyMeta struct {
		key  *datastorepb.Key
		path string
	}
	groups := make(map[nsKey][]keyMeta)
	keyByPath := make(map[string]*datastorepb.Key, len(req.Keys))

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
			// Snapshot reads must be per-entity (time-based query).
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
			nk := nsKey{proj, db, ns}
			groups[nk] = append(groups[nk], keyMeta{key, path})
			keyByPath[path] = key
		}
	}

	// Batch fetch all non-snapshot keys per namespace group.
	for nk, metas := range groups {
		paths := make([]string, len(metas))
		for i, m := range metas {
			paths[i] = m.path
		}
		found, missing, err := g.store.DsGetManyWithTimes(nk.project, nk.database, nk.namespace, paths)
		if err != nil {
			return nil, err
		}
		for _, row := range found {
			resp.Found = append(resp.Found, &datastorepb.EntityResult{
				Entity:     row.Entity,
				Version:    row.Version,
				CreateTime: row.CreateTime,
				UpdateTime: row.UpdateTime,
			})
		}
		for _, path := range missing {
			key := keyByPath[path]
			resp.Missing = append(resp.Missing, &datastorepb.EntityResult{
				Entity: &datastorepb.Entity{Key: key},
			})
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
