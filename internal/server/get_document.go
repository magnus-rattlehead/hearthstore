package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func (s *Server) GetDocument(ctx context.Context, req *firestorepb.GetDocumentRequest) (*firestorepb.Document, error) {
	project, database, path, err := parseName(req.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid document name: %v", err)
	}

	// Resolve a read-time snapshot if requested via transaction or direct read_time.
	var doc *firestorepb.Document
	switch cs := req.ConsistencySelector.(type) {
	case *firestorepb.GetDocumentRequest_Transaction:
		s.txMu.Lock()
		entry, ok := s.txns[string(cs.Transaction)]
		s.txMu.Unlock()
		if ok && entry.readTime != nil {
			doc, err = s.store.GetDocAsOf(project, database, path, entry.readTime.AsTime())
		} else {
			doc, err = s.store.GetDoc(project, database, path)
		}
	case *firestorepb.GetDocumentRequest_ReadTime:
		if cs.ReadTime != nil {
			doc, err = s.store.GetDocAsOf(project, database, path, cs.ReadTime.AsTime())
		} else {
			doc, err = s.store.GetDoc(project, database, path)
		}
	default:
		doc, err = s.store.GetDoc(project, database, path)
	}
	if err != nil {
		return nil, err
	}

	if req.Mask != nil && len(req.Mask.FieldPaths) > 0 {
		doc = applyFieldMask(doc, req.Mask.FieldPaths)
	}

	return doc, nil
}
