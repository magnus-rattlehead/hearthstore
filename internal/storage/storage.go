package storage

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	_ "github.com/mattn/go-sqlite3"
)

// dbExec is satisfied by both *sql.DB and *sql.Tx, enabling helpers to work
// in either a plain connection or a caller-supplied transaction.
type dbExec interface {
	QueryRow(query string, args ...any) *sql.Row
	Query(query string, args ...any) (*sql.Rows, error)
	Exec(query string, args ...any) (sql.Result, error)
}

// pragmaDSN applies these pragmas to every connection via DSN query params.
// journal_mode=WAL is omitted — it's a file-level setting, set once on wdb.
// wal_autocheckpoint=0 disables automatic checkpointing; a background goroutine
// checkpoints every 30 s via PASSIVE mode so write transactions never block on it.
// synchronous=OFF is safe for an emulator (developer tool; no crash-durability
// requirement). It removes the checkpoint fsync entirely, the dominant cost for
// write-heavy workloads. See: rqlite, phiresky sqlite-perf, ericdraken benchmarks.
const pragmaDSN = "_synchronous=OFF" +
	"&_busy_timeout=5000" +
	"&_cache_size=-524288" + // 512 MB — covers more B-tree hot pages
	"&_foreign_keys=on" +
	"&_stmt_cache_size=100" // per-connection prepared-statement LRU cache
// _wal_autocheckpoint and mmap_size are applied via explicit PRAGMA after open;
// mattn/go-sqlite3 silently ignores them in the DSN.

const schema = `
CREATE TABLE IF NOT EXISTS documents (
    project     TEXT NOT NULL,
    database    TEXT NOT NULL,
    path        TEXT NOT NULL,
    collection  TEXT NOT NULL,
    parent_path TEXT NOT NULL DEFAULT '',
    data        BLOB NOT NULL,
    create_time TEXT NOT NULL,
    update_time TEXT NOT NULL,
    deleted     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (project, database, path)
);

CREATE INDEX IF NOT EXISTS idx_collection ON documents (project, database, parent_path, collection);

-- Cloud Datastore API entities (separate from Firestore Native documents)
CREATE TABLE IF NOT EXISTS ds_documents (
    project     TEXT NOT NULL,
    database    TEXT NOT NULL DEFAULT '(default)',
    namespace   TEXT NOT NULL DEFAULT '',
    path        TEXT NOT NULL,
    kind        TEXT NOT NULL,
    parent_path TEXT NOT NULL DEFAULT '',
    data        BLOB NOT NULL,
    create_time TEXT NOT NULL,
    update_time TEXT NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1,
    deleted     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (project, database, namespace, path)
);

CREATE INDEX IF NOT EXISTS idx_ds_kind
    ON ds_documents (project, database, namespace, parent_path, kind);

-- Append-only change log for Datastore snapshot reads (readTime / read-only transactions).
-- Every entity write/delete appends a row; queries use MAX(change_time) <= readTime.
CREATE TABLE IF NOT EXISTS ds_document_changes (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    project     TEXT NOT NULL,
    database    TEXT NOT NULL DEFAULT '(default)',
    namespace   TEXT NOT NULL DEFAULT '',
    path        TEXT NOT NULL,
    kind        TEXT NOT NULL,
    parent_path TEXT NOT NULL DEFAULT '',
    change_time TEXT NOT NULL,
    deleted     INTEGER NOT NULL DEFAULT 0,
    data        BLOB   -- NULL for deletes
);

CREATE INDEX IF NOT EXISTS idx_ds_changes_doc ON ds_document_changes
    (project, database, namespace, path, seq);

CREATE INDEX IF NOT EXISTS idx_ds_changes_kind ON ds_document_changes
    (project, database, namespace, parent_path, kind, change_time);

-- Denormalized field values for Datastore entities — enables SQL filter pushdown.
-- Populated on every write; cleared on soft-delete.
CREATE TABLE IF NOT EXISTS ds_field_index (
    project      TEXT NOT NULL,
    database     TEXT NOT NULL DEFAULT '(default)',
    namespace    TEXT NOT NULL DEFAULT '',
    kind         TEXT NOT NULL,
    doc_path     TEXT NOT NULL,
    field_path   TEXT NOT NULL,
    value_string TEXT,
    value_int    INTEGER,
    value_double REAL,
    value_bool   INTEGER,
    value_bytes  BLOB,
    value_null   INTEGER,
    value_ref    TEXT,
    value_lat    REAL,
    value_lng    REAL,
    in_array     INTEGER NOT NULL DEFAULT 0
);

-- Filter lookups scoped to (namespace, kind, field).
CREATE INDEX IF NOT EXISTS idx_ds_field ON ds_field_index (
    project, database, namespace, kind, field_path,
    value_string, value_int, value_double
);

-- Per-document cleanup on re-index and soft-delete.
CREATE INDEX IF NOT EXISTS idx_ds_doc_fields ON ds_field_index
    (project, database, namespace, doc_path);

-- Covering indexes for single-sort keyset pagination. Each index places the sort
-- value column right after the equality prefix so SQLite can scan in ORDER BY order
-- and stop at LIMIT, making each page O(page-size) instead of O(kind-size).
CREATE INDEX IF NOT EXISTS idx_ds_field_sort_int ON ds_field_index
    (project, database, namespace, kind, field_path, value_int, doc_path)
    WHERE in_array = 0;

CREATE INDEX IF NOT EXISTS idx_ds_field_sort_str ON ds_field_index
    (project, database, namespace, kind, field_path, value_string, doc_path)
    WHERE in_array = 0;

CREATE INDEX IF NOT EXISTS idx_ds_field_sort_dbl ON ds_field_index
    (project, database, namespace, kind, field_path, value_double, doc_path)
    WHERE in_array = 0;

-- Covering index for no-sort / path-cursor pagination (ORDER BY path with cursor).
CREATE INDEX IF NOT EXISTS idx_ds_kind_path ON ds_documents
    (project, database, namespace, kind, path)
    WHERE deleted = 0;

-- Monotonic ID sequences for AllocateIds
CREATE TABLE IF NOT EXISTS ds_id_sequences (
    project   TEXT NOT NULL,
    database  TEXT NOT NULL DEFAULT '(default)',
    namespace TEXT NOT NULL DEFAULT '',
    kind      TEXT NOT NULL,
    next_id   INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (project, database, namespace, kind)
);

CREATE TABLE IF NOT EXISTS field_index (
    project         TEXT NOT NULL,
    database        TEXT NOT NULL,
    collection_path TEXT NOT NULL,
    doc_path        TEXT NOT NULL,
    field_path      TEXT NOT NULL,
    value_string    TEXT,
    value_int       INTEGER,
    value_double    REAL,
    value_bool      INTEGER,
    value_bytes     BLOB,
    value_null      INTEGER,
    value_ref       TEXT,
    in_array        INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_field ON field_index (
    project, database, collection_path, field_path,
    value_string, value_int, value_double
);

CREATE INDEX IF NOT EXISTS idx_doc_fields ON field_index (project, database, doc_path);

-- Partial index: covers only live documents; eliminates the per-row deleted=0
-- table lookup that idx_collection requires.
CREATE INDEX IF NOT EXISTS idx_collection_active ON documents
    (project, database, parent_path, collection)
    WHERE deleted=0;

-- Covering index for ORDER BY LEFT JOINs. The join condition leads with doc_path,
-- which is not in idx_field, causing full field_index scans per sort field.
CREATE INDEX IF NOT EXISTS idx_doc_join ON field_index
    (project, database, doc_path, field_path,
     value_string, value_int, value_double);

-- Covering index for SUM/AVG aggregations. Excludes array-element rows and
-- null/non-numeric values at the index level so the aggregation query needs no
-- per-row table lookups into field_index.
CREATE INDEX IF NOT EXISTS idx_field_numeric ON field_index
    (project, database, collection_path, field_path, value_int, value_double)
    WHERE in_array=0 AND (value_int IS NOT NULL OR value_double IS NOT NULL);

-- Per-type sort covering indexes for INNER JOIN-driven ORDER BY queries.
-- Driving FROM field_index INNER JOIN documents with these indexes yields
-- O(LIMIT) scans instead of O(collection-size) for single-field ORDER BY.
CREATE INDEX IF NOT EXISTS idx_field_sort_int ON field_index
    (project, database, collection_path, field_path, value_int, doc_path)
    WHERE in_array=0;

CREATE INDEX IF NOT EXISTS idx_field_sort_str ON field_index
    (project, database, collection_path, field_path, value_string, doc_path)
    WHERE in_array=0;

CREATE INDEX IF NOT EXISTS idx_field_sort_dbl ON field_index
    (project, database, collection_path, field_path, value_double, doc_path)
    WHERE in_array=0;

-- Append-only change log for Listen resumeToken support.
-- Every document write/delete appends a row; seq is the token value.
CREATE TABLE IF NOT EXISTS document_changes (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    project     TEXT NOT NULL,
    database    TEXT NOT NULL,
    path        TEXT NOT NULL,
    collection  TEXT NOT NULL,
    parent_path TEXT NOT NULL DEFAULT '',
    change_time TEXT NOT NULL,
    deleted     INTEGER NOT NULL DEFAULT 0,
    data        BLOB   -- NULL for deletes
);

-- Scoped range scan for Query-target diffs.
CREATE INDEX IF NOT EXISTS idx_changes_scope ON document_changes
    (project, database, parent_path, collection, seq);

-- Per-document diff for Documents-target reconnects.
CREATE INDEX IF NOT EXISTS idx_changes_doc ON document_changes
    (project, database, path, seq);
`

// Store is the disk-backed document store.
type Store struct {
	wdb *sql.DB // single write connection — serialises all mutations
	rdb *sql.DB // read connection pool — concurrent readers, never blocks writers (WAL)

	// pub/sub for real-time Listen delivery — sharded by (project, database, parent, collection)
	subMu      sync.RWMutex
	subEntries map[uint64]*subEntry
	byScope    map[subScopeKey]map[uint64]chan ChangeEvent
	byCollG    map[subCollGKey]map[uint64]chan ChangeEvent
	nextID     uint64

	done chan struct{} // closed by Close to stop the background checkpoint goroutine
}

// migrations holds ALTER TABLE statements that extend the schema after its
// initial creation. Each entry is run once; "duplicate column name" errors are
// silently swallowed so the same list is safe to run against any DB version.
var migrations = []string{
	`ALTER TABLE ds_field_index ADD COLUMN value_lat REAL`,
	`ALTER TABLE ds_field_index ADD COLUMN value_lng REAL`,
	// idx_ds_doc_join served correlated-EXISTS queries; replaced by IN-subquery
	// approach that drives from idx_ds_field. Dropping it cuts write overhead by ~33%.
	`DROP INDEX IF EXISTS idx_ds_doc_join`,
	// Per-type sort indexes enabling O(page-size) single-sort keyset pagination.
	`CREATE INDEX IF NOT EXISTS idx_ds_field_sort_int ON ds_field_index (project, database, namespace, kind, field_path, value_int, doc_path) WHERE in_array = 0`,
	`CREATE INDEX IF NOT EXISTS idx_ds_field_sort_str ON ds_field_index (project, database, namespace, kind, field_path, value_string, doc_path) WHERE in_array = 0`,
	`CREATE INDEX IF NOT EXISTS idx_ds_field_sort_dbl ON ds_field_index (project, database, namespace, kind, field_path, value_double, doc_path) WHERE in_array = 0`,
	// Covering index for path-cursor (no-sort) pagination.
	`CREATE INDEX IF NOT EXISTS idx_ds_kind_path ON ds_documents (project, database, namespace, kind, path) WHERE deleted = 0`,
	// Per-type sort covering indexes for Firestore INNER JOIN-driven ORDER BY.
	`CREATE INDEX IF NOT EXISTS idx_field_sort_int ON field_index (project, database, collection_path, field_path, value_int, doc_path) WHERE in_array=0`,
	`CREATE INDEX IF NOT EXISTS idx_field_sort_str ON field_index (project, database, collection_path, field_path, value_string, doc_path) WHERE in_array=0`,
	`CREATE INDEX IF NOT EXISTS idx_field_sort_dbl ON field_index (project, database, collection_path, field_path, value_double, doc_path) WHERE in_array=0`,
}

func applyMigrations(db *sql.DB) error {
	for _, m := range migrations {
		if _, err := db.Exec(m); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("migration %q: %w", m, err)
		}
	}
	return nil
}

// New opens (or creates) the SQLite database at dataDir/hearthstore.db.
// Uses two pools: one write connection (serialised) and up to 4 readers;
// WAL mode allows concurrent reads without blocking writes.
func New(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "hearthstore.db")

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}

	dsn := dbPath + "?" + pragmaDSN

	wdb, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite (write): %w", err)
	}
	wdb.SetMaxOpenConns(1)
	wdb.SetMaxIdleConns(1)
	if _, err := wdb.Exec("PRAGMA mmap_size=8589934592"); err != nil {
		return nil, fmt.Errorf("setting mmap_size: %w", err)
	}
	if _, err := wdb.Exec("PRAGMA wal_autocheckpoint=0"); err != nil {
		return nil, fmt.Errorf("disabling wal_autocheckpoint: %w", err)
	}

	// WAL mode persists in the file; read pool connections inherit it.
	if _, err := wdb.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return nil, fmt.Errorf("setting WAL mode: %w", err)
	}
	if _, err := wdb.Exec(schema); err != nil {
		return nil, fmt.Errorf("applying schema: %w", err)
	}
	if err := applyMigrations(wdb); err != nil {
		return nil, fmt.Errorf("applying migrations: %w", err)
	}
	// Refresh query planner statistics (non-fatal if unsupported).
	_, _ = wdb.Exec("PRAGMA optimize")

	rdb, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite (read): %w", err)
	}
	rdb.SetMaxOpenConns(4)
	rdb.SetMaxIdleConns(4)
	if _, err := rdb.Exec("PRAGMA mmap_size=8589934592"); err != nil {
		return nil, fmt.Errorf("setting mmap_size (read): %w", err)
	}

	s := &Store{
		wdb:        wdb,
		rdb:        rdb,
		subEntries: make(map[uint64]*subEntry),
		byScope:    make(map[subScopeKey]map[uint64]chan ChangeEvent),
		byCollG:    make(map[subCollGKey]map[uint64]chan ChangeEvent),
		done:       make(chan struct{}),
	}
	go s.checkpointLoop()
	return s, nil
}

// checkpointLoop runs a WAL checkpoint every 30 seconds so write transactions
// never stall on the auto-checkpoint threshold (disabled via wal_autocheckpoint=0).
// On Close it does a final TRUNCATE checkpoint to keep the WAL file small.
func (s *Store) checkpointLoop() {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			_, _ = s.wdb.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		case <-s.done:
			_, _ = s.wdb.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
			return
		}
	}
}

// Close stops the background checkpoint goroutine and closes both connections.
func (s *Store) Close() error {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	rerr := s.rdb.Close()
	werr := s.wdb.Close()
	if rerr != nil {
		return rerr
	}
	return werr
}

// DB returns the write connection. Used only by tests that need direct DB access.
func (s *Store) DB() *sql.DB {
	return s.wdb
}

// RunInTx executes fn inside a SQLite write transaction. If fn returns an
// error the transaction is rolled back; otherwise it is committed.
// SQLite BUSY errors (database locked) are mapped to codes.Aborted so that
// Datastore clients with retry logic will retry the transaction.
func (s *Store) RunInTx(fn func(tx *sql.Tx) error) error {
	tx, err := s.wdb.Begin()
	if err != nil {
		return wrapBusy(err)
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return wrapBusy(err)
	}
	return nil
}

// wrapBusy maps SQLite BUSY errors to codes.Aborted so clients will retry.
func wrapBusy(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "database is locked") || strings.Contains(msg, "SQLITE_BUSY") {
		return status.Errorf(codes.Aborted, "database busy, retry the transaction: %v", err)
	}
	return err
}
