package datastore

import (
	"context"
	"net/http"

	"google.golang.org/protobuf/types/known/timestamppb"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

func (g *GRPCServer) BeginTransaction(ctx context.Context, req *datastorepb.BeginTransactionRequest) (*datastorepb.BeginTransactionResponse, error) {
	var entry txEntry
	if opts := req.TransactionOptions; opts != nil {
		if ro, ok := opts.Mode.(*datastorepb.TransactionOptions_ReadOnly_); ok {
			entry.readOnly = true
			if rt := ro.ReadOnly.GetReadTime(); rt != nil {
				entry.readTime = rt
			} else {
				entry.readTime = timestamppb.Now()
			}
		}
	}

	id := newTxID()
	g.txMu.Lock()
	g.txns[id] = entry
	g.txMu.Unlock()

	return &datastorepb.BeginTransactionResponse{
		Transaction: []byte(id),
	}, nil
}

func (s *Server) handleBeginTransaction(w http.ResponseWriter, r *http.Request, project string) {
	var req datastorepb.BeginTransactionRequest
	if !readProtoJSON(w, r.Body, &req) {
		return
	}
	resp, err := s.grpc.BeginTransaction(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	writeProtoJSON(w, resp)
}
