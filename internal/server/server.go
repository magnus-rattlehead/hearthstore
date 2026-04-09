package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	firestorepb "google.golang.org/genproto/googleapis/firestore/v1"

	"github.com/stiviguranjaku/hearthstore/internal/storage"
)

// Server implements firestorepb.FirestoreServer backed by a disk Store.
type Server struct {
	firestorepb.UnimplementedFirestoreServer
	store *storage.Store
}

func New(store *storage.Store) *Server {
	return &Server{store: store}
}

func (s *Server) GetDocument(ctx context.Context, req *firestorepb.GetDocumentRequest) (*firestorepb.Document, error) {
	return nil, status.Error(codes.Unimplemented, "GetDocument not yet implemented")
}

func (s *Server) ListDocuments(req *firestorepb.ListDocumentsRequest, stream firestorepb.Firestore_ListDocumentsServer) error {
	return status.Error(codes.Unimplemented, "ListDocuments not yet implemented")
}

func (s *Server) CreateDocument(ctx context.Context, req *firestorepb.CreateDocumentRequest) (*firestorepb.Document, error) {
	return nil, status.Error(codes.Unimplemented, "CreateDocument not yet implemented")
}

func (s *Server) UpdateDocument(ctx context.Context, req *firestorepb.UpdateDocumentRequest) (*firestorepb.Document, error) {
	return nil, status.Error(codes.Unimplemented, "UpdateDocument not yet implemented")
}

func (s *Server) DeleteDocument(ctx context.Context, req *firestorepb.DeleteDocumentRequest) error {
	return status.Error(codes.Unimplemented, "DeleteDocument not yet implemented")
}

func (s *Server) BatchGetDocuments(req *firestorepb.BatchGetDocumentsRequest, stream firestorepb.Firestore_BatchGetDocumentsServer) error {
	return status.Error(codes.Unimplemented, "BatchGetDocuments not yet implemented")
}

func (s *Server) BatchWrite(ctx context.Context, req *firestorepb.BatchWriteRequest) (*firestorepb.BatchWriteResponse, error) {
	return nil, status.Error(codes.Unimplemented, "BatchWrite not yet implemented")
}

func (s *Server) BeginTransaction(ctx context.Context, req *firestorepb.BeginTransactionRequest) (*firestorepb.BeginTransactionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "BeginTransaction not yet implemented")
}

func (s *Server) Commit(ctx context.Context, req *firestorepb.CommitRequest) (*firestorepb.CommitResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Commit not yet implemented")
}

func (s *Server) Rollback(ctx context.Context, req *firestorepb.RollbackRequest) error {
	return status.Error(codes.Unimplemented, "Rollback not yet implemented")
}

func (s *Server) RunQuery(req *firestorepb.RunQueryRequest, stream firestorepb.Firestore_RunQueryServer) error {
	return status.Error(codes.Unimplemented, "RunQuery not yet implemented")
}

func (s *Server) RunAggregationQuery(req *firestorepb.RunAggregationQueryRequest, stream firestorepb.Firestore_RunAggregationQueryServer) error {
	return status.Error(codes.Unimplemented, "RunAggregationQuery not yet implemented")
}

func (s *Server) ListCollectionIds(ctx context.Context, req *firestorepb.ListCollectionIdsRequest) (*firestorepb.ListCollectionIdsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ListCollectionIds not yet implemented")
}
