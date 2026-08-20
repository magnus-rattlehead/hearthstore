package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	adminpb "cloud.google.com/go/datastore/admin/apiv1/adminpb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// AdminServer manages the same persisted indexes used by RunQuery.
type AdminServer struct {
	adminpb.UnimplementedDatastoreAdminServer
	store      *storage.Store
	manager    *IndexManager
	operations *IndexOperationsServer
}

func NewAdminServer(store *storage.Store, manager *IndexManager) *AdminServer {
	operations := &IndexOperationsServer{store: store, operations: make(map[string]*longrunningpb.Operation)}
	return &AdminServer{store: store, manager: manager, operations: operations}
}

func (s *AdminServer) Operations() *IndexOperationsServer { return s.operations }

func (s *AdminServer) ListIndexes(_ context.Context, req *adminpb.ListIndexesRequest) (*adminpb.ListIndexesResponse, error) {
	indexes, err := s.store.ListDsCompositeIndexes(req.ProjectId)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &adminpb.ListIndexesResponse{}
	for _, idx := range indexes {
		resp.Indexes = append(resp.Indexes, adminIndex(idx))
	}
	return resp, nil
}

func (s *AdminServer) GetIndex(_ context.Context, req *adminpb.GetIndexRequest) (*adminpb.Index, error) {
	idx, err := s.store.GetDsCompositeIndex(req.ProjectId, req.IndexId)
	if err == sql.ErrNoRows {
		return nil, status.Errorf(codes.NotFound, "index %q not found", req.IndexId)
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return adminIndex(idx), nil
}

func (s *AdminServer) CreateIndex(ctx context.Context, req *adminpb.CreateIndexRequest) (*longrunningpb.Operation, error) {
	if req.ProjectId == "" || req.Index == nil || req.Index.Kind == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id, index, and kind are required")
	}
	if len(req.Index.Properties) < 2 || len(req.Index.Properties) > 100 {
		return nil, status.Error(codes.InvalidArgument, "composite indexes require between 2 and 100 properties")
	}
	idx := storage.DsCompositeIndex{Project: req.ProjectId, Kind: req.Index.Kind, Ancestor: req.Index.Ancestor == adminpb.Index_ALL_ANCESTORS, Source: "admin"}
	for _, property := range req.Index.Properties {
		if property.Name == "" {
			return nil, status.Error(codes.InvalidArgument, "index property name is required")
		}
		idx.Properties = append(idx.Properties, storage.DsIndexProperty{Name: property.Name, Desc: property.Direction == adminpb.Index_DESCENDING})
	}
	idx.ID = storage.DsCompositeIndexID(idx.Kind, idx.Ancestor, idx.Properties)
	if _, err := s.store.GetDsCompositeIndex(idx.Project, idx.ID); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "index %q already exists", idx.ID)
	} else if err != sql.ErrNoRows {
		return nil, status.Error(codes.Internal, err.Error())
	}
	current, created, err := s.store.EnsureDsCompositeIndex(ctx, idx, false)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !created {
		return nil, status.Errorf(codes.AlreadyExists, "index %q already exists", idx.ID)
	}
	_ = s.manager.recordGenerated(idx)
	name := fmt.Sprintf("projects/%s/operations/index-create-%s", req.ProjectId, idx.ID)
	op := newIndexOperation(name, current, false, nil)
	s.operations.put(op)
	go s.operations.watch(name, req.ProjectId, idx.ID, false)
	return op, nil
}

func (s *AdminServer) DeleteIndex(ctx context.Context, req *adminpb.DeleteIndexRequest) (*longrunningpb.Operation, error) {
	idx, err := s.store.GetDsCompositeIndex(req.ProjectId, req.IndexId)
	if err == sql.ErrNoRows {
		return nil, status.Errorf(codes.NotFound, "index %q not found", req.IndexId)
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if idx.State != storage.DsIndexReady && idx.State != storage.DsIndexError {
		return nil, status.Errorf(codes.FailedPrecondition, "index %q is %s", idx.ID, idx.State)
	}
	name := fmt.Sprintf("projects/%s/operations/index-delete-%s", req.ProjectId, idx.ID)
	op := newIndexOperation(name, idx, false, nil)
	s.operations.put(op)
	go func() {
		err := s.store.DeleteDsCompositeIndex(context.Background(), req.ProjectId, req.IndexId)
		if err == nil {
			_ = s.manager.removeGenerated(req.IndexId)
		}
		s.operations.finishDelete(name, err)
	}()
	return op, nil
}

func adminIndex(idx storage.DsCompositeIndex) *adminpb.Index {
	out := &adminpb.Index{ProjectId: idx.Project, IndexId: idx.ID, Kind: idx.Kind, Ancestor: adminpb.Index_NONE, State: adminState(idx.State)}
	if idx.Ancestor {
		out.Ancestor = adminpb.Index_ALL_ANCESTORS
	}
	for _, property := range idx.Properties {
		direction := adminpb.Index_ASCENDING
		if property.Desc {
			direction = adminpb.Index_DESCENDING
		}
		out.Properties = append(out.Properties, &adminpb.Index_IndexedProperty{Name: property.Name, Direction: direction})
	}
	return out
}

func adminState(state string) adminpb.Index_State {
	switch state {
	case storage.DsIndexCreating:
		return adminpb.Index_CREATING
	case storage.DsIndexReady:
		return adminpb.Index_READY
	case storage.DsIndexDeleting:
		return adminpb.Index_DELETING
	default:
		return adminpb.Index_ERROR
	}
}

// IndexOperationsServer exposes index creation/deletion as pollable operations.
type IndexOperationsServer struct {
	longrunningpb.UnimplementedOperationsServer
	store      *storage.Store
	mu         sync.RWMutex
	operations map[string]*longrunningpb.Operation
}

func (s *IndexOperationsServer) put(op *longrunningpb.Operation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.operations[op.Name] = op
}
func (s *IndexOperationsServer) get(name string) (*longrunningpb.Operation, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	op, ok := s.operations[name]
	return op, ok
}

func newIndexOperation(name string, idx storage.DsCompositeIndex, done bool, operationErr error) *longrunningpb.Operation {
	metadata, _ := anypb.New(&adminpb.IndexOperationMetadata{IndexId: idx.ID, ProgressEntities: &adminpb.Progress{WorkCompleted: idx.ProcessedEntities, WorkEstimated: idx.TotalEntities}})
	op := &longrunningpb.Operation{Name: name, Metadata: metadata, Done: done}
	if done && operationErr == nil {
		response, _ := anypb.New(adminIndex(idx))
		op.Result = &longrunningpb.Operation_Response{Response: response}
	}
	if operationErr != nil {
		op.Done = true
		op.Result = &longrunningpb.Operation_Error{Error: status.Convert(operationErr).Proto()}
	}
	return op
}

func (s *IndexOperationsServer) watch(name, project, id string, deleting bool) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		idx, err := s.store.GetDsCompositeIndex(project, id)
		if err == sql.ErrNoRows && deleting {
			s.finishDelete(name, nil)
			return
		}
		if err != nil {
			continue
		}
		done := idx.State == storage.DsIndexReady || idx.State == storage.DsIndexError
		var operationErr error
		if idx.State == storage.DsIndexError {
			operationErr = status.Error(codes.Internal, idx.Error)
		}
		s.put(newIndexOperation(name, idx, done, operationErr))
		if done {
			return
		}
	}
}
func (s *IndexOperationsServer) finishDelete(name string, err error) {
	idx := storage.DsCompositeIndex{}
	s.put(newIndexOperation(name, idx, true, err))
}

func (s *IndexOperationsServer) GetOperation(_ context.Context, req *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
	if op, ok := s.get(req.Name); ok {
		return op, nil
	}
	return nil, status.Error(codes.NotFound, "operation not found")
}
func (s *IndexOperationsServer) ListOperations(_ context.Context, _ *longrunningpb.ListOperationsRequest) (*longrunningpb.ListOperationsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	resp := &longrunningpb.ListOperationsResponse{}
	for _, op := range s.operations {
		resp.Operations = append(resp.Operations, op)
	}
	return resp, nil
}
func (s *IndexOperationsServer) DeleteOperation(_ context.Context, req *longrunningpb.DeleteOperationRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.operations[req.Name]
	if !ok {
		return nil, status.Error(codes.NotFound, "operation not found")
	}
	if !op.Done {
		return nil, status.Error(codes.FailedPrecondition, "operation is not done")
	}
	delete(s.operations, req.Name)
	return &emptypb.Empty{}, nil
}
func (s *IndexOperationsServer) CancelOperation(context.Context, *longrunningpb.CancelOperationRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "index build cancellation is not supported")
}
func (s *IndexOperationsServer) WaitOperation(ctx context.Context, req *longrunningpb.WaitOperationRequest) (*longrunningpb.Operation, error) {
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	if req.Timeout != nil {
		deadline.Reset(req.Timeout.AsDuration())
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		op, err := s.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: req.Name})
		if err != nil {
			return nil, err
		}
		if op.Done {
			return op, nil
		}
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-deadline.C:
			return op, nil
		case <-ticker.C:
		}
	}
}
