package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func (s *Server) DeleteDocument(ctx context.Context, req *firestorepb.DeleteDocumentRequest) (*emptypb.Empty, error) {
	project, database, path, err := parseName(req.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid document name: %v", err)
	}

	if req.CurrentDocument != nil {
		if err := s.store.CheckPrecondition(project, database, path, req.CurrentDocument); err != nil {
			return nil, err
		}
	}

	if err := s.store.DeleteDoc(project, database, path); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
