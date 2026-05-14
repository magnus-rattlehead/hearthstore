package server

import (
	"context"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// PartitionQuery returns an empty partition list, meaning "run as a single partition."
// This is correct emulator behavior — the caller falls back to running the full query.
func (s *Server) PartitionQuery(ctx context.Context, req *firestorepb.PartitionQueryRequest) (*firestorepb.PartitionQueryResponse, error) {
	return &firestorepb.PartitionQueryResponse{}, nil
}
