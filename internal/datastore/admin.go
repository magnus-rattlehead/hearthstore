package datastore

import (
	"context"
	"errors"
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
	operations := &IndexOperationsServer{store: store, operations: make(map[string]*longrunningpb.Operation), changed: make(chan struct{})}
	return &AdminServer{store: store, manager: manager, operations: operations}
}

func (s *AdminServer) Operations() *IndexOperationsServer { return s.operations }

func (s *AdminServer) ListIndexes(_ context.Context, req *adminpb.ListIndexesRequest) (*adminpb.ListIndexesResponse, error) {
	indexes, err := s.store.ListDsCompositeIndexes(req.ProjectId)
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &adminpb.ListIndexesResponse{}
	for _, idx := range indexes {
		resp.Indexes = append(resp.Indexes, adminIndex(idx))
	}
	return resp, nil
}

func (s *AdminServer) GetIndex(_ context.Context, req *adminpb.GetIndexRequest) (*adminpb.Index, error) {
	idx, err := s.store.GetDsCompositeIndex(req.ProjectId, req.IndexId)
	if errors.Is(err, storage.ErrIndexNotFound) {
		return nil, status.Errorf(codes.NotFound, "index %q not found", req.IndexId)
	}
	if err != nil {
		return nil, rpcError(err)
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
	} else if !errors.Is(err, storage.ErrIndexNotFound) {
		return nil, rpcError(err)
	}
	current, created, err := s.store.EnsureDsCompositeIndex(ctx, idx, false)
	if err != nil {
		return nil, rpcError(err)
	}
	if !created {
		return nil, status.Errorf(codes.AlreadyExists, "index %q already exists", idx.ID)
	}
	name := fmt.Sprintf("projects/%s/operations/index-create-%s", req.ProjectId, idx.ID)
	op := newIndexOperation(name, current, false, nil)
	s.operations.put(op)
	go s.operations.watch(name, req.ProjectId, idx.ID, false)
	return op, nil
}

func (s *AdminServer) DeleteIndex(ctx context.Context, req *adminpb.DeleteIndexRequest) (*longrunningpb.Operation, error) {
	idx, err := s.store.GetDsCompositeIndex(req.ProjectId, req.IndexId)
	if errors.Is(err, storage.ErrIndexNotFound) {
		return nil, status.Errorf(codes.NotFound, "index %q not found", req.IndexId)
	}
	if err != nil {
		return nil, rpcError(err)
	}
	if idx.State != storage.DsIndexReady && idx.State != storage.DsIndexError {
		return nil, status.Errorf(codes.FailedPrecondition, "index %q is %s", idx.ID, idx.State)
	}
	name := fmt.Sprintf("projects/%s/operations/index-delete-%s", req.ProjectId, idx.ID)
	op := newIndexOperation(name, idx, false, nil)
	s.operations.put(op)
	go func() {
		err := s.store.DeleteDsCompositeIndex(context.Background(), req.ProjectId, req.IndexId)
		if err == nil && s.manager != nil {
			s.manager.invalidateTemplates(req.ProjectId)
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
	completed  []string
	changed    chan struct{}
}

const completedOperationLimit = 1_000

func (s *IndexOperationsServer) put(op *longrunningpb.Operation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.operations == nil {
		s.operations = make(map[string]*longrunningpb.Operation)
	}
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	previous := s.operations[op.Name]
	s.operations[op.Name] = op
	if op.Done && (previous == nil || !previous.Done) {
		s.completed = append(s.completed, op.Name)
		if len(s.completed) > completedOperationLimit {
			oldest := s.completed[0]
			s.completed = s.completed[1:]
			delete(s.operations, oldest)
		}
	}
	close(s.changed)
	s.changed = make(chan struct{})
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
		op.Result = &longrunningpb.Operation_Error{Error: status.Convert(rpcError(operationErr)).Proto()}
	}
	return op
}

func (s *IndexOperationsServer) watch(name, project, id string, _ bool) {
	err := s.store.WatchDsCompositeIndex(context.Background(), project, id, func(idx storage.DsCompositeIndex) {
		if idx.State != storage.DsIndexReady && idx.State != storage.DsIndexError {
			s.put(newIndexOperation(name, idx, false, nil))
		}
	})
	if err != nil {
		s.put(newIndexOperation(name, storage.DsCompositeIndex{}, true, rpcError(err)))
		return
	}
	idx, err := s.store.GetDsCompositeIndex(project, id)
	if err != nil {
		s.put(newIndexOperation(name, storage.DsCompositeIndex{}, true, rpcError(err)))
		return
	}
	var operationErr error
	if idx.State == storage.DsIndexError {
		operationErr = rpcError(errors.New(idx.Error))
	}
	s.put(newIndexOperation(name, idx, true, operationErr))
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
	var timeout <-chan time.Time
	var timer *time.Timer
	if req.Timeout != nil {
		timer = time.NewTimer(req.Timeout.AsDuration())
		timeout = timer.C
		defer timer.Stop()
	}
	for {
		s.mu.RLock()
		op, ok := s.operations[req.Name]
		changed := s.changed
		s.mu.RUnlock()
		if !ok {
			return nil, status.Error(codes.NotFound, "operation not found")
		}
		if op.Done {
			return op, nil
		}
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-timeout:
			return op, nil
		case <-changed:
		}
	}
}
