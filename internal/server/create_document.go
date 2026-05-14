package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func (s *Server) CreateDocument(ctx context.Context, req *firestorepb.CreateDocumentRequest) (*firestorepb.Document, error) {
	project, database, parentPath, err := parseParent(req.Parent)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid parent: %v", err)
	}
	if req.CollectionId == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_id is required")
	}

	docID := req.DocumentId
	if docID == "" {
		docID = newAutoID()
	}

	// Full document path within the database (e.g. "users/u1" or "a/b/users/u1").
	var docPath string
	if parentPath == "" {
		docPath = req.CollectionId + "/" + docID
	} else {
		docPath = parentPath + "/" + req.CollectionId + "/" + docID
	}

	// Set the canonical resource name before storage so it's persisted in the BLOB.
	stub := &firestorepb.Document{
		Name:   buildDocName(project, database, docPath),
		Fields: req.Document.GetFields(),
	}
	doc, err := s.store.InsertDoc(project, database, req.CollectionId, parentPath, docPath, stub)
	if err != nil {
		return nil, err
	}
	return doc, nil
}
