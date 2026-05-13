//go:build integration

package tests

// Integration tests for the Cloud Datastore gRPC layer using the official
// cloud.google.com/go/datastore SDK. Each test spins up a fresh in-process
// gRPC server backed by a temp SQLite store and connects via localhost.

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

type dsEntity struct {
	Name  string
	Score int64
}

// newDatastoreClient starts a hearthstore Datastore gRPC server and returns a
// connected *datastore.Client. Both are torn down at test end.
func newDatastoreClient(t *testing.T) *datastore.Client {
	t.Helper()
	ctx := context.Background()

	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	grpcSrv := grpc.NewServer()
	datastorepb.RegisterDatastoreServer(grpcSrv, dspkg.New(store).NewGRPCServer())
	go grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(grpcSrv.Stop)

	addr := lis.Addr().String()
	client, err := datastore.NewClient(ctx, testProject,
		option.WithEndpoint(addr),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("datastore.NewClient: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// -- basic CRUD -------------------------------------------------------------

func TestDSGRPC_PutAndGet(t *testing.T) {
	client := newDatastoreClient(t)
	ctx := context.Background()

	key := datastore.NameKey("User", "alice", nil)
	if _, err := client.Put(ctx, key, &dsEntity{Name: "Alice", Score: 42}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	var got dsEntity
	if err := client.Get(ctx, key, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "Alice" {
		t.Errorf("Name = %q, want Alice", got.Name)
	}
	if got.Score != 42 {
		t.Errorf("Score = %d, want 42", got.Score)
	}
}

func TestDSGRPC_Delete(t *testing.T) {
	client := newDatastoreClient(t)
	ctx := context.Background()

	key := datastore.NameKey("Item", "toDelete", nil)
	if _, err := client.Put(ctx, key, &dsEntity{Score: 1}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := client.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	var got dsEntity
	err := client.Get(ctx, key, &got)
	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("Get after Delete: want ErrNoSuchEntity, got %v", err)
	}
}

func TestDSGRPC_Upsert_Overwrites(t *testing.T) {
	client := newDatastoreClient(t)
	ctx := context.Background()

	key := datastore.NameKey("User", "bob", nil)
	if _, err := client.Put(ctx, key, &dsEntity{Name: "Bob", Score: 10}); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if _, err := client.Put(ctx, key, &dsEntity{Name: "Bob v2", Score: 99}); err != nil {
		t.Fatalf("second Put: %v", err)
	}

	var got dsEntity
	if err := client.Get(ctx, key, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Score != 99 {
		t.Errorf("Score = %d, want 99 after overwrite", got.Score)
	}
}

func TestDSGRPC_GetMulti(t *testing.T) {
	client := newDatastoreClient(t)
	ctx := context.Background()

	k1 := datastore.NameKey("Multi", "a", nil)
	k2 := datastore.NameKey("Multi", "b", nil)
	k3 := datastore.NameKey("Multi", "c", nil) // not written

	client.Put(ctx, k1, &dsEntity{Score: 1})  //nolint:errcheck
	client.Put(ctx, k2, &dsEntity{Score: 2})  //nolint:errcheck

	entities := make([]dsEntity, 3)
	err := client.GetMulti(ctx, []*datastore.Key{k1, k2, k3}, entities)
	// GetMulti returns a MultiError when some keys are missing.
	if err == nil {
		t.Fatal("expected MultiError for missing key, got nil")
	}
	me, ok := err.(datastore.MultiError)
	if !ok {
		t.Fatalf("expected datastore.MultiError, got %T: %v", err, err)
	}
	if me[0] != nil || me[1] != nil {
		t.Errorf("k1/k2 should be found, got errors: %v, %v", me[0], me[1])
	}
	if !errors.Is(me[2], datastore.ErrNoSuchEntity) {
		t.Errorf("k3 should be ErrNoSuchEntity, got %v", me[2])
	}
	if entities[0].Score != 1 || entities[1].Score != 2 {
		t.Errorf("entity scores = %d, %d; want 1, 2", entities[0].Score, entities[1].Score)
	}
}

// -- queries ----------------------------------------------------------------

func TestDSGRPC_Query_Equality(t *testing.T) {
	client := newDatastoreClient(t)
	ctx := context.Background()

	for name, score := range map[string]int64{"p1": 10, "p2": 20, "p3": 10} {
		client.Put(ctx, datastore.NameKey("Player", name, nil), &dsEntity{Score: score}) //nolint:errcheck
	}

	q := datastore.NewQuery("Player").FilterField("Score", "=", int64(10))
	var results []dsEntity
	if _, err := client.GetAll(ctx, q, &results); err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("score==10: want 2, got %d", len(results))
	}
}

func TestDSGRPC_Query_OrderByLimit(t *testing.T) {
	client := newDatastoreClient(t)
	ctx := context.Background()

	for name, score := range map[string]int64{"r1": 30, "r2": 10, "r3": 20} {
		client.Put(ctx, datastore.NameKey("Run", name, nil), &dsEntity{Score: score}) //nolint:errcheck
	}

	q := datastore.NewQuery("Run").Order("Score").Limit(2)
	var results []dsEntity
	if _, err := client.GetAll(ctx, q, &results); err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("want 2, got %d", len(results))
	}
	if results[0].Score != 10 {
		t.Errorf("first score = %d, want 10", results[0].Score)
	}
	if results[1].Score != 20 {
		t.Errorf("second score = %d, want 20", results[1].Score)
	}
}

// -- transactions -----------------------------------------------------------

func TestDSGRPC_Transaction_Commit(t *testing.T) {
	client := newDatastoreClient(t)
	ctx := context.Background()

	key := datastore.NameKey("Account", "acc1", nil)
	if _, err := client.Put(ctx, key, &dsEntity{Score: 100}); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var e dsEntity
		if err := tx.Get(key, &e); err != nil {
			return err
		}
		e.Score += 50
		_, err := tx.Put(key, &e)
		return err
	})
	if err != nil {
		t.Fatalf("RunInTransaction: %v", err)
	}

	var got dsEntity
	client.Get(ctx, key, &got) //nolint:errcheck
	if got.Score != 150 {
		t.Errorf("Score after tx = %d, want 150", got.Score)
	}
}

func TestDSGRPC_Transaction_Rollback(t *testing.T) {
	client := newDatastoreClient(t)
	ctx := context.Background()

	key := datastore.NameKey("Account", "acc2", nil)
	if _, err := client.Put(ctx, key, &dsEntity{Score: 200}); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	// Deliberately abort.
	client.RunInTransaction(ctx, func(tx *datastore.Transaction) error { //nolint:errcheck
		tx.Put(key, &dsEntity{Score: 0}) //nolint:errcheck
		return errors.New("abort")
	})

	var got dsEntity
	client.Get(ctx, key, &got) //nolint:errcheck
	if got.Score != 200 {
		t.Errorf("Score after rollback = %d, want 200 (unchanged)", got.Score)
	}
}

// -- aggregation ------------------------------------------------------------

func TestDSGRPC_Count(t *testing.T) {
	client := newDatastoreClient(t)
	ctx := context.Background()

	for i := int64(1); i <= 4; i++ {
		client.Put(ctx, datastore.NameKey("Widget", "w"+string(rune('0'+i)), nil), &dsEntity{Score: i}) //nolint:errcheck
	}

	n, err := client.Count(ctx, datastore.NewQuery("Widget"))
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 4 {
		t.Errorf("count = %d, want 4", n)
	}
}

// -- allocated IDs ----------------------------------------------------------

func TestDSGRPC_AllocatedID(t *testing.T) {
	client := newDatastoreClient(t)
	ctx := context.Background()

	key := datastore.IncompleteKey("Thing", nil)
	completedKey, err := client.Put(ctx, key, &dsEntity{Name: "auto"})
	if err != nil {
		t.Fatalf("Put with incomplete key: %v", err)
	}
	if completedKey.ID == 0 {
		t.Error("completed key should have non-zero numeric ID")
	}
}

// -- mixed operations -------------------------------------------------------

func TestDSGRPC_MixedOps(t *testing.T) {
	client := newDatastoreClient(t)
	ctx := context.Background()

	kA := datastore.NameKey("Actor", "alice", nil)
	kB := datastore.NameKey("Actor", "bob", nil)
	kC := datastore.NameKey("Actor", "carol", nil)

	client.Put(ctx, kA, &dsEntity{Score: 10}) //nolint:errcheck
	client.Put(ctx, kB, &dsEntity{Score: 20}) //nolint:errcheck
	client.Put(ctx, kC, &dsEntity{Score: 30}) //nolint:errcheck

	// Bump alice to 50.
	client.Put(ctx, kA, &dsEntity{Score: 50}) //nolint:errcheck

	// Delete bob.
	client.Delete(ctx, kB) //nolint:errcheck

	q := datastore.NewQuery("Actor").FilterField("Score", ">=", int64(20)).Order("Score")
	var results []dsEntity
	if _, err := client.GetAll(ctx, q, &results); err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("want 2, got %d", len(results))
	}
	if results[0].Score != 30 || results[1].Score != 50 {
		t.Errorf("scores = [%d %d], want [30 50]", results[0].Score, results[1].Score)
	}
}
