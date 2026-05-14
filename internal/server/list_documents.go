package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func (s *Server) ListDocuments(ctx context.Context, req *firestorepb.ListDocumentsRequest) (*firestorepb.ListDocumentsResponse, error) {
	project, database, parentPath, err := parseParent(req.Parent)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent: %v", err)
	}

	docs, nextToken, err := s.store.ListDocs(project, database, parentPath, req.CollectionId, req.PageSize, req.PageToken, req.ShowMissing)
	if err != nil {
		return nil, err
	}

	if req.Mask != nil && len(req.Mask.FieldPaths) > 0 {
		for i, doc := range docs {
			docs[i] = applyFieldMask(doc, req.Mask.FieldPaths)
		}
	}

	return &firestorepb.ListDocumentsResponse{
		Documents:     docs,
		NextPageToken: nextToken,
	}, nil
}
