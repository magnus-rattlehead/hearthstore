//go:build integration

package tests

import (
	"context"
	"errors"
	"net"
	"testing"

	"cloud.google.com/go/datastore"
	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	dspkg "github.com/magnus-rattlehead/hearthstore/internal/datastore"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

type grpcEntity struct {
	Score int64
}

func TestDatastoreGRPCWorkflow(t *testing.T) {
	client := newDatastoreClient(t)
	ctx := context.Background()
	key := datastore.NameKey("GRPCSmoke", "entity", nil)

	if _, err := client.Put(ctx, key, &grpcEntity{Score: 42}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	var entity grpcEntity
	if err := client.Get(ctx, key, &entity); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if entity.Score != 42 {
		t.Fatalf("Get score = %d, want 42", entity.Score)
	}

	var results []grpcEntity
	query := datastore.NewQuery("GRPCSmoke").FilterField("Score", "=", int64(42))
	if _, err := client.GetAll(ctx, query, &results); err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("query results = %d, want 1", len(results))
	}

	if _, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var current grpcEntity
		if err := tx.Get(key, &current); err != nil {
			return err
		}
		current.Score++
		_, err := tx.Put(key, &current)
		return err
	}); err != nil {
		t.Fatalf("RunInTransaction: %v", err)
	}
	if err := client.Get(ctx, key, &entity); err != nil {
		t.Fatalf("Get after transaction: %v", err)
	}
	if entity.Score != 43 {
		t.Fatalf("score after transaction = %d, want 43", entity.Score)
	}

	if err := client.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := client.Get(ctx, key, &entity); !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Fatalf("Get after delete error = %v, want ErrNoSuchEntity", err)
	}
}

func newDatastoreClient(t *testing.T) *datastore.Client {
	t.Helper()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	datastorepb.RegisterDatastoreServer(server, dspkg.New(store).NewGRPCServer())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	client, err := datastore.NewClient(
		context.Background(),
		testProject,
		option.WithEndpoint(listener.Addr().String()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("datastore.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
