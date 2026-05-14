package server

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func (s *Server) BatchGetDocuments(req *firestorepb.BatchGetDocumentsRequest, stream firestorepb.Firestore_BatchGetDocumentsServer) error {
	project, database, err := parseDatabase(req.Database)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid database: %v", err)
	}

	// Handle inline transaction selector.
	var txID []byte
	var snapReadTime *timestamppb.Timestamp // non-nil → read as of this time
	switch cs := req.ConsistencySelector.(type) {
	case *firestorepb.BatchGetDocumentsRequest_NewTransaction:
		id := newAutoID()
		readOnly := false
		if cs.NewTransaction != nil {
			_, readOnly = cs.NewTransaction.Mode.(*firestorepb.TransactionOptions_ReadOnly_)
		}
		s.txMu.Lock()
		s.txns[id] = txEntry{readOnly: readOnly}
		s.txMu.Unlock()
		txID = []byte(id)
	case *firestorepb.BatchGetDocumentsRequest_Transaction:
		txID = cs.Transaction
		// If this is a read-time transaction, propagate the snapshot time.
		s.txMu.Lock()
		entry, ok := s.txns[string(txID)]
		s.txMu.Unlock()
		if ok && entry.readTime != nil {
			snapReadTime = entry.readTime
		}
	case *firestorepb.BatchGetDocumentsRequest_ReadTime:
		snapReadTime = cs.ReadTime
	}

	readTime := timestamppb.Now()
	sentFirst := false
	seen := make(map[string]bool, len(req.Documents))
	for _, name := range req.Documents {
		if seen[name] {
			continue
		}
		seen[name] = true

		_, _, path, err := parseName(name)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid document name %q: %v", name, err)
		}

		var resp *firestorepb.BatchGetDocumentsResponse
		var doc *firestorepb.Document
		if snapReadTime != nil {
			doc, err = s.store.GetDocAsOf(project, database, path, snapReadTime.AsTime())
		} else {
			doc, err = s.store.GetDoc(project, database, path)
		}
		if status.Code(err) == codes.NotFound {
			resp = &firestorepb.BatchGetDocumentsResponse{
				Result:   &firestorepb.BatchGetDocumentsResponse_Missing{Missing: name},
				ReadTime: readTime,
			}
		} else if err != nil {
			return err
		} else {
			if req.Mask != nil && len(req.Mask.FieldPaths) > 0 {
				doc = applyFieldMask(doc, req.Mask.FieldPaths)
			}
			resp = &firestorepb.BatchGetDocumentsResponse{
				Result:   &firestorepb.BatchGetDocumentsResponse_Found{Found: doc},
				ReadTime: readTime,
			}
		}
		if !sentFirst && len(txID) > 0 {
			resp.Transaction = txID
			sentFirst = true
		}
		if err = stream.Send(resp); err != nil {
			return err
		}
	}
	// If no documents were requested but a transaction was started, send one
	// response with just the transaction ID.
	if !sentFirst && len(txID) > 0 {
		return stream.Send(&firestorepb.BatchGetDocumentsResponse{
			Transaction: txID,
			ReadTime:    readTime,
		})
	}
	return nil
}
