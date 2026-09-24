package storage

import (
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// QueryScope identifies a kind (or the whole partition when Kind is empty).
type QueryScope struct {
	Project, Database, Namespace, Kind string
	AllNamespaces                      bool
}

func kindFence(scope QueryScope) []byte {
	if scope.AllNamespaces {
		return []byte("meta/database-fence/" + enc(scope.Project) + "/" + enc(scope.Database))
	}
	return []byte("meta/fence/" + enc(scope.Project) + "/" + enc(scope.Database) + "/" + enc(scope.Namespace) + "/" + enc(scope.Kind))
}

func touchKind(tx *Txn, project, database, namespace, kind string) error {
	for _, name := range []string{kind, ""} {
		if err := tx.Set(kindFence(QueryScope{Project: project, Database: database, Namespace: namespace, Kind: name}), nil); err != nil {
			return err
		}
	}
	return tx.Set(kindFence(QueryScope{Project: project, Database: database, AllNamespaces: true}), nil)
}

// CheckQueryScopeTx adds an OCC dependency and detects changes since the snapshot.
// SHORTCUT: conflicts cover a whole kind; use range fences if unrelated writes
// produce excessive transaction aborts in measured Firespotter workloads.
func (s *Store) CheckQueryScopeTx(tx *Txn, scope QueryScope, snapshot time.Time) error {
	item, err := tx.Get(kindFence(scope))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if item.Version() > uint64(snapshot.UnixNano()) {
		return status.Error(codes.Aborted, "queried kind changed during transaction; retry the transaction")
	}
	return nil
}
