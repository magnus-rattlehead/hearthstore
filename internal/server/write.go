package server

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"io"
	"log/slog"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// Write implements the bidi streaming write protocol.
//
// Protocol:
//  1. Client sends handshake: WriteRequest{Database, Writes: nil}
//  2. Server responds: WriteResponse{StreamId, StreamToken, CommitTime}
//  3. Client sends batches: WriteRequest{StreamToken: prev, Writes: [...]}
//  4. Server applies writes, responds: WriteResponse{StreamToken: new, WriteResults, CommitTime}
//  5. Client closes → server returns nil
func (s *Server) Write(stream firestorepb.Firestore_WriteServer) error {
	// Step 1: read handshake.
	first, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	if first.Database == "" {
		return status.Error(codes.InvalidArgument, "first WriteRequest must set database")
	}

	// Step 2: send handshake response.
	streamID := newStreamID()
	token := newStreamToken()
	if err := stream.Send(&firestorepb.WriteResponse{
		StreamId:    streamID,
		StreamToken: token,
		CommitTime:  timestamppb.Now(),
	}); err != nil {
		return err
	}

	// Step 3+: process write batches until client closes.
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if len(req.Writes) == 0 {
			// Empty batch: SDK is refreshing the stream or resuming after a token gap.
			// Respond with an updated token so the client's acknowledgment state advances.
			token = newStreamToken()
			if err := stream.Send(&firestorepb.WriteResponse{
				StreamToken: token,
				CommitTime:  timestamppb.Now(),
			}); err != nil {
				return err
			}
			continue
		}

		slog.Debug("write: batch received",
			"writes", len(req.Writes),
			"stream_token", string(req.StreamToken),
		)
		for i, w := range req.Writes {
			slog.Debug("write: request details",
				"index", i,
				"has_precondition", w.CurrentDocument != nil,
				"precondition", w.CurrentDocument,
			)
		}
		commitTime := timestamppb.Now()
		writeResults, err := s.applyWriteBatch(req.Writes)
		if err != nil {
			slog.Debug("write: applyWriteBatch error", "err", err)
			return err
		}

		token = newStreamToken()
		if err := stream.Send(&firestorepb.WriteResponse{
			StreamToken:  token,
			WriteResults: writeResults,
			CommitTime:   commitTime,
		}); err != nil {
			return err
		}
		slog.Debug("write: batch response sent", "write_results", len(writeResults))
	}
}

// applyWriteBatch applies a slice of writes atomically in one transaction.
// Fast path if all writes are simple upserts; slow path (single tx) otherwise.
func (s *Server) applyWriteBatch(writes []*firestorepb.Write) ([]*firestorepb.WriteResult, error) {
	results := make([]*firestorepb.WriteResult, 0, len(writes))

	// Fast path: all simple upserts → single tx via UpsertDocsManyTx.
	if rows, project, database, ok := s.collectSimpleUpsertWrites(writes); ok {
		acc := storage.NewFsCommitAccumulator()
		var docs map[string]*firestorepb.Document
		if err := s.store.RunInTx(func(tx *sql.Tx) error {
			var e error
			docs, e = s.store.UpsertDocsManyTx(tx, project, database, rows, acc)
			if e != nil {
				return e
			}
			return acc.Flush(tx)
		}); err != nil {
			return nil, err
		}
		s.store.NotifyBatch(acc.Events())
		for _, r := range rows {
			results = append(results, &firestorepb.WriteResult{UpdateTime: docs[r.Path].UpdateTime})
		}
		return results, nil
	}

	// Slow path: wrap all writes in one tx for atomicity.
	if err := s.store.RunInTx(func(tx *sql.Tx) error {
		results = results[:0]
		for _, w := range writes {
			wr, err := s.applyWriteTx(tx, w)
			if err != nil {
				return err
			}
			results = append(results, wr)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return results, nil
}

func newStreamID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func newStreamToken() []byte {
	return strconv.AppendInt(nil, time.Now().UnixNano(), 10)
}
