package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func (s *Server) Rollback(ctx context.Context, req *firestorepb.RollbackRequest) (*emptypb.Empty, error) {
	txID := string(req.Transaction)

	s.txMu.Lock()
	_, ok := s.txns[txID]
	if ok {
		delete(s.txns, txID)
	}
	s.txMu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "transaction not found: %s", txID)
	}

	return &emptypb.Empty{}, nil
}
