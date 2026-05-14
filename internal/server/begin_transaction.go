package server

import (
	"context"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func (s *Server) BeginTransaction(ctx context.Context, req *firestorepb.BeginTransactionRequest) (*firestorepb.BeginTransactionResponse, error) {
	id := newAutoID()

	entry := txEntry{}
	if opts := req.Options; opts != nil {
		if ro, ok := opts.Mode.(*firestorepb.TransactionOptions_ReadOnly_); ok {
			entry.readOnly = true
			if ro.ReadOnly != nil {
				entry.readTime = ro.ReadOnly.GetReadTime()
			}
		}
	}

	s.txMu.Lock()
	s.txns[id] = entry
	s.txMu.Unlock()

	return &firestorepb.BeginTransactionResponse{Transaction: []byte(id)}, nil
}
