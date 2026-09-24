package datastore

import (
	"context"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

func (g *GRPCServer) BeginTransaction(ctx context.Context, req *datastorepb.BeginTransactionRequest) (out *datastorepb.BeginTransactionResponse, resultErr error) {
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
	entry := txEntry{project: req.ProjectId, database: database, readTime: timestamppb.New(g.store.ReadTime())}
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
	if err := validateReadTime(entry.readTime.AsTime()); err != nil {
		return nil, err
	}

	id := newTxID()
	g.addTransaction(id, entry)

	return &datastorepb.BeginTransactionResponse{
		Transaction: []byte(id),
	}, nil
}

func (s *Server) handleBeginTransaction(w http.ResponseWriter, r *http.Request, project string) {
	var req datastorepb.BeginTransactionRequest
	if !readProtoJSON(w, r.Body, &req) {
		return
	}
	if req.ProjectId == "" {
		req.ProjectId = project
	}
	SetHTTPDetails(r.Context(), DSBeginTxDetails(&req))
	resp, err := s.grpc.BeginTransaction(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	MergeHTTPDetails(r.Context(), DSBeginTxResponseDetails(resp))
	writeProtoJSON(w, resp)
}
