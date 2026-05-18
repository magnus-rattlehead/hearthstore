package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func (s *Server) UpdateDocument(ctx context.Context, req *firestorepb.UpdateDocumentRequest) (*firestorepb.Document, error) {
	project, database, path, err := parseName(req.Document.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid document name: %v", err)
	}

	if req.CurrentDocument != nil {
		if err := s.store.CheckPrecondition(project, database, path, req.CurrentDocument); err != nil {
			return nil, err
		}
	}

	collection, parentPath, _ := splitDocPath(path)

	name := buildDocName(project, database, path)

	// With an update mask: merge into the existing document. Requires the document to exist.
	if req.UpdateMask != nil && len(req.UpdateMask.FieldPaths) > 0 {
		existing, err := s.store.GetDoc(project, database, path)
		if err != nil {
			return nil, err // propagates NotFound
		}
		if existing.Fields == nil {
			existing.Fields = make(map[string]*firestorepb.Value)
		}
		for _, f := range req.UpdateMask.FieldPaths {
			if v, ok := req.Document.Fields[f]; ok {
				existing.Fields[f] = v
			} else {
				delete(existing.Fields, f) // field in mask but absent in request = delete it
			}
		}
		existing.Name = name
		return s.store.UpsertDoc(project, database, collection, parentPath, path, existing)
	}

	// Without an update mask: full replace (upsert - creates if document does not exist).
	stub := &firestorepb.Document{Name: name, Fields: req.Document.GetFields()}
	return s.store.UpsertDoc(project, database, collection, parentPath, path, stub)
}
