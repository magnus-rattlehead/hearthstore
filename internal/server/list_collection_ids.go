package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func (s *Server) ListCollectionIds(ctx context.Context, req *firestorepb.ListCollectionIdsRequest) (*firestorepb.ListCollectionIdsResponse, error) {
	// Parent is a document path: "projects/{p}/databases/{d}/documents[/{doc_path}]"
	project, database, parentPath, err := parseParent(req.Parent)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent: %v", err)
	}

	ids, err := s.store.ListCollectionIDs(project, database, parentPath)
	if err != nil {
		return nil, err
	}

	return &firestorepb.ListCollectionIdsResponse{CollectionIds: ids}, nil
}
