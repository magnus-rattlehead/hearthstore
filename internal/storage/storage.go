package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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
// journal_mode=WAL is omitted - it's a file-level setting, set once on wdb.
// wal_autocheckpoint=0 disables automatic checkpointing; a background goroutine
// checkpoints every 30 s via TRUNCATE mode (resets WAL write position + shrinks
// the file) so the WAL does not grow unbounded. PASSIVE-only checkpointing moves
// frames to the DB but never resets the write pointer, so new writes always extend
// the file - eventually filling the disk.
// synchronous=OFF is safe for an emulator (developer tool; no crash-durability
// requirement). It removes the checkpoint fsync entirely, the dominant cost for
// write-heavy workloads. See: rqlite, phiresky sqlite-perf, ericdraken benchmarks.
const pragmaDSN = "_synchronous=OFF" +
	"&_busy_timeout=30000" +
	"&_cache_size=-524288" + // 512 MB - covers more B-tree hot pages
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

-- Denormalized field values for Datastore entities - enables SQL filter pushdown.
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

// batchJob is one write submitted to the group-commit loop.
type batchJob struct {
	ctx  context.Context
	fn   func(*sql.Tx) error
	done chan error // buffered(1); receives nil or error after commit
}

// Store is the disk-backed document store.
type Store struct {
	wdb  *sql.DB // single write connection - serialises all mutations
	rdb  *sql.DB // read connection pool - concurrent readers, never blocks writers (WAL)
	cpdb *sql.DB // dedicated checkpoint connection - never competes with wdb
	dbPath string  // filesystem path for WAL stat

	batchCh    chan batchJob // group-commit queue for non-transactional writes
	priorityCh chan batchJob // high-priority queue for isolated transactional commits

	// pub/sub for real-time Listen delivery - sharded by (project, database, parent, collection)
	subMu      sync.RWMutex
	subEntries map[uint64]*subEntry
	byScope    map[subScopeKey]map[uint64]chan ChangeEvent
	byCollG    map[subCollGKey]map[uint64]chan ChangeEvent
	nextID     uint64

	done chan struct{}   // closed by Close to stop background goroutines
	wg   sync.WaitGroup // tracks checkpointLoop + batchCommitLoop

	// atomic counters - incremented on hot paths, read by the dashboard
	commitTotal   atomic.Int64
	commitErrors  atomic.Int64
	batchJobsExec atomic.Int64
	batchPreempt  atomic.Int64
	occConflicts  atomic.Int64
	listenEvents  atomic.Int64
	rpcTotal      atomic.Int64
	rpcErrors     atomic.Int64

	// DBContentStats cache
	dbStatsMu      sync.Mutex
	dbStatsCached  DBContentStats
	dbStatsExpires time.Time
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
	rdb.SetMaxOpenConns(0) // unlimited; WAL allows arbitrary concurrent readers
	rdb.SetMaxIdleConns(max(4, runtime.GOMAXPROCS(0)))
	if _, err := rdb.Exec("PRAGMA mmap_size=8589934592"); err != nil {
		return nil, fmt.Errorf("setting mmap_size (read): %w", err)
	}

	// Dedicated checkpoint connection: keeps TRUNCATE checkpoint off wdb so the
	// write path never stalls waiting for the checkpoint's busy-timeout (up to 5s).
	cpdb, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite (checkpoint): %w", err)
	}
	cpdb.SetMaxOpenConns(1)
	cpdb.SetMaxIdleConns(1)
	// Cap WAL file at 256 MB: SQLite truncates to this limit after a successful
	// checkpoint, so the file can't grow beyond one checkpoint period's worth of writes.
	if _, err := cpdb.Exec("PRAGMA journal_size_limit=268435456"); err != nil {
		return nil, fmt.Errorf("setting journal_size_limit: %w", err)
	}

	s := &Store{
		wdb:        wdb,
		rdb:        rdb,
		cpdb:       cpdb,
		dbPath:     dbPath,
		batchCh:    make(chan batchJob, 1024),
		priorityCh: make(chan batchJob, 64),
		subEntries: make(map[uint64]*subEntry),
		byScope:    make(map[subScopeKey]map[uint64]chan ChangeEvent),
		byCollG:    make(map[subCollGKey]map[uint64]chan ChangeEvent),
		done:       make(chan struct{}),
	}
	s.wg.Add(2)
	go func() { defer s.wg.Done(); s.checkpointLoop() }()
	go func() { defer s.wg.Done(); s.batchCommitLoop() }()
	return s, nil
}

// checkpointLoop runs a TRUNCATE WAL checkpoint every 30 seconds.
// TRUNCATE (vs PASSIVE) resets the WAL write position and shrinks the WAL file
// after moving frames to the main DB. Without the write-position reset, new writes
// always extend the WAL at an ever-increasing offset - eventually filling the disk.
// The cpdb busy_timeout (30 s) bounds how long we wait for active readers; if a
// checkpoint times out the WAL grows by at most one interval's worth of writes.
func (s *Store) checkpointLoop() {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			_, _ = s.cpdb.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
		case <-s.done:
			_, _ = s.cpdb.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
			return
		}
	}
}

// Close stops background goroutines and closes all connections.
func (s *Store) Close() error {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	s.wg.Wait() // ensure goroutines exit before DB connections close
	rerr := s.rdb.Close()
	cperr := s.cpdb.Close()
	werr := s.wdb.Close()
	if rerr != nil {
		return rerr
	}
	if cperr != nil {
		return cperr
	}
	return werr
}

// DB returns the write connection. Used only by tests that need direct DB access.
func (s *Store) DB() *sql.DB { return s.wdb }

// RDB returns the read connection pool.
func (s *Store) RDB() *sql.DB { return s.rdb }

// CPDB returns the checkpoint connection.
func (s *Store) CPDB() *sql.DB { return s.cpdb }

// BatchStats returns current group-commit queue depths.
func (s *Store) BatchStats() (batchLen, batchCap, prioLen, prioCap int) {
	return len(s.batchCh), cap(s.batchCh), len(s.priorityCh), cap(s.priorityCh)
}

// SubCount returns the number of active Listen subscribers.
func (s *Store) SubCount() int {
	s.subMu.RLock()
	n := len(s.subEntries)
	s.subMu.RUnlock()
	return n
}

// ScopeCount returns the number of distinct scope shards.
func (s *Store) ScopeCount() int {
	s.subMu.RLock()
	n := len(s.byScope)
	s.subMu.RUnlock()
	return n
}

// CollGroupCount returns the number of distinct collection-group shards.
func (s *Store) CollGroupCount() int {
	s.subMu.RLock()
	n := len(s.byCollG)
	s.subMu.RUnlock()
	return n
}

// NextID returns the last assigned subscriber ID.
func (s *Store) NextID() uint64 {
	s.subMu.RLock()
	id := s.nextID
	s.subMu.RUnlock()
	return id
}

// --- counter accessors ---

func (s *Store) IncrCommitTotal()    { s.commitTotal.Add(1) }
func (s *Store) IncrCommitError()    { s.commitErrors.Add(1) }
func (s *Store) IncrBatchJob()       { s.batchJobsExec.Add(1) }
func (s *Store) IncrBatchPreempt()   { s.batchPreempt.Add(1) }
func (s *Store) IncrOCCConflict()    { s.occConflicts.Add(1) }
func (s *Store) IncrListenEvent()    { s.listenEvents.Add(1) }
func (s *Store) IncrRPC(isErr bool)  {
	s.rpcTotal.Add(1)
	if isErr {
		s.rpcErrors.Add(1)
	}
}

// CounterSnapshot holds a point-in-time read of all atomic counters.
type CounterSnapshot struct {
	CommitTotal   int64 `json:"commit_total"`
	CommitErrors  int64 `json:"commit_errors"`
	BatchJobsExec int64 `json:"batch_jobs_executed"`
	BatchPreempt  int64 `json:"batch_preempted"`
	OCCConflicts  int64 `json:"occ_conflicts"`
	ListenEvents  int64 `json:"listen_events"`
	RPCTotal      int64 `json:"rpc_total"`
	RPCErrors     int64 `json:"rpc_errors"`
}

func (s *Store) CounterSnapshot() CounterSnapshot {
	return CounterSnapshot{
		CommitTotal:   s.commitTotal.Load(),
		CommitErrors:  s.commitErrors.Load(),
		BatchJobsExec: s.batchJobsExec.Load(),
		BatchPreempt:  s.batchPreempt.Load(),
		OCCConflicts:  s.occConflicts.Load(),
		ListenEvents:  s.listenEvents.Load(),
		RPCTotal:      s.rpcTotal.Load(),
		RPCErrors:     s.rpcErrors.Load(),
	}
}

// DBContentStats holds cached row counts and size info from the database.
type DBContentStats struct {
	Documents      int64 `json:"documents"`
	DocChanges     int64 `json:"document_changes"`
	FieldIndex     int64 `json:"field_index"`
	DsDocuments    int64 `json:"ds_documents"`
	DsDocChanges   int64 `json:"ds_document_changes"`
	DsFieldIndex   int64 `json:"ds_field_index"`
	DsIDSequences  int64 `json:"ds_id_sequences"`
	DBSizeBytes    int64 `json:"db_size_bytes"`
	WALSizeBytes   int64 `json:"wal_size_bytes"`
	FreelistPages  int64 `json:"freelist_pages"`
}

// DBContentStats queries row counts and DB size, caching the result for 5s.
func (s *Store) DBContentStats() DBContentStats {
	s.dbStatsMu.Lock()
	defer s.dbStatsMu.Unlock()
	if time.Now().Before(s.dbStatsExpires) {
		return s.dbStatsCached
	}
	var st DBContentStats
	tables := []struct {
		dest  *int64
		query string
	}{
		{&st.Documents, `SELECT COUNT(*) FROM documents`},
		{&st.DocChanges, `SELECT COUNT(*) FROM document_changes`},
		{&st.FieldIndex, `SELECT COUNT(*) FROM field_index`},
		{&st.DsDocuments, `SELECT COUNT(*) FROM ds_documents`},
		{&st.DsDocChanges, `SELECT COUNT(*) FROM ds_document_changes`},
		{&st.DsFieldIndex, `SELECT COUNT(*) FROM ds_field_index`},
		{&st.DsIDSequences, `SELECT COUNT(*) FROM ds_id_sequences`},
	}
	for _, t := range tables {
		_ = s.rdb.QueryRow(t.query).Scan(t.dest)
	}
	var pageCount, pageSize int64
	_ = s.rdb.QueryRow(`PRAGMA page_count`).Scan(&pageCount)
	_ = s.rdb.QueryRow(`PRAGMA page_size`).Scan(&pageSize)
	_ = s.rdb.QueryRow(`PRAGMA freelist_count`).Scan(&st.FreelistPages)
	st.DBSizeBytes = pageCount * pageSize
	if fi, err := os.Stat(s.dbPath + "-wal"); err == nil {
		st.WALSizeBytes = fi.Size()
	}
	s.dbStatsCached = st
	s.dbStatsExpires = time.Now().Add(5 * time.Second)
	return st
}

// RunInTx executes fn inside a SQLite write transaction. If fn returns an
// error the transaction is rolled back; otherwise it is committed.
// SQLite BUSY errors (database locked) are mapped to codes.Aborted so that
// Datastore clients with retry logic will retry the transaction.
func (s *Store) RunInTx(fn func(tx *sql.Tx) error) error {
	return s.RunInTxCtx(context.Background(), fn)
}

// RunInTxCtx is like RunInTx but respects ctx cancellation. Sends through
// priorityCh so the batch commit loop runs it immediately after its current
// batch, ahead of any queued batch work.
func (s *Store) RunInTxCtx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	job := batchJob{ctx: ctx, fn: fn, done: make(chan error, 1)}
	select {
	case s.priorityCh <- job:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return status.Error(codes.Unavailable, "store closed")
	}
	select {
	case err := <-job.done:
		s.commitTotal.Add(1)
		if err != nil {
			s.commitErrors.Add(1)
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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

const batchMaxSize = 512

// batchTimeBudget caps how long one executeBatch cycle holds the write
// connection. Self-tuning: fast fns pack many into one TX; slow fns (large
// bulk upserts) pack fewer. Priority jobs still preempt between any two fns.
const batchTimeBudget = 50 * time.Millisecond

// RunBatchedTx submits fn to the group-commit loop. Concurrent callers are
// merged into one SQLite transaction (up to a 50ms time budget), amortising
// the WAL sync across all of them. Use this for
// non-transactional writes (BulkWriter, BatchWrite, streaming Write) where
// OCC isolation between concurrent RPCs is not required. For transactional
// commits with OCC conflict checks, use RunInTxCtx instead.
func (s *Store) RunBatchedTx(ctx context.Context, fn func(*sql.Tx) error) error {
	job := batchJob{ctx: ctx, fn: fn, done: make(chan error, 1)}
	select {
	case s.batchCh <- job:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-job.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// batchCommitLoop coalesces concurrent writes into one SQLite transaction.
// Priority jobs (RunInTxCtx) are drained before and after each batch so
// transactional commits never wait more than one batch execution cycle.
func (s *Store) batchCommitLoop() {
	for {
		s.drainPriorityCh()

		var first batchJob
		select {
		case j := <-s.priorityCh:
			s.runPriorityJob(j)
			continue
		case first = <-s.batchCh:
		case <-s.done:
			s.drainPriorityCh()
			s.drainBatchCh()
			return
		}

		batch := []batchJob{first}
	drain:
		for len(batch) < batchMaxSize {
			select {
			case job := <-s.batchCh:
				batch = append(batch, job)
			default:
				break drain
			}
		}

		select {
		case <-s.done:
			s.executeBatch(batch)
			s.drainPriorityCh()
			s.drainBatchCh()
			return
		default:
		}

		s.executeBatch(batch)
	}
}

func (s *Store) drainBatchCh() {
	for {
		select {
		case j := <-s.batchCh:
			j.done <- status.Error(codes.Unavailable, "store closing")
		default:
			return
		}
	}
}

// drainPriorityCh runs all currently-queued priority jobs (non-blocking).
func (s *Store) drainPriorityCh() {
	for {
		select {
		case j := <-s.priorityCh:
			s.runPriorityJob(j)
		default:
			return
		}
	}
}

// runPriorityJob executes a single isolated transaction for a priority job.
func (s *Store) runPriorityJob(j batchJob) {
	if j.ctx.Err() != nil {
		j.done <- j.ctx.Err()
		return
	}
	j.done <- s.runSingleTx(j.fn)
}

// executeBatch runs jobs in one transaction. Breaks early to yield to priority
// jobs if any arrive mid-batch, committing completed work and recursing for the
// remainder. On commit failure falls back to per-job individual transactions.
func (s *Store) executeBatch(batch []batchJob) {
	s.doExecuteBatch(batch, false)
}

// doExecuteBatch is the internal implementation of executeBatch.
// earlyPreempt=true allows priority preemption before the first fn runs;
// safe on recursive calls because priority was just drained by the caller.
// The first (non-recursive) call uses earlyPreempt=false so at least one fn
// always makes progress, preventing batch starvation under sustained priority load.
func (s *Store) doExecuteBatch(batch []batchJob, earlyPreempt bool) {
	// Drop jobs whose context already expired while waiting.
	active := make([]batchJob, 0, len(batch))
	for _, j := range batch {
		if j.ctx.Err() != nil {
			j.done <- j.ctx.Err()
		} else {
			active = append(active, j)
		}
	}
	if len(active) == 0 {
		return
	}

	tx, err := s.wdb.Begin()
	if err != nil {
		e := wrapBusy(err)
		for _, j := range active {
			j.done <- e
		}
		return
	}

	end := len(active)
	anyErr := false
	deadline := time.Now().Add(batchTimeBudget)
	for i, j := range active {
		canPreempt := earlyPreempt || i > 0
		if canPreempt && (len(s.priorityCh) > 0 || time.Now().After(deadline)) {
			s.batchPreempt.Add(1)
			end = i
			break
		}
		if err := j.fn(tx); err != nil {
			anyErr = true
			end = i + 1
			break
		}
		s.batchJobsExec.Add(1)
	}

	ran, deferred := active[:end], active[end:]

	if anyErr || func() bool { return wrapBusy(tx.Commit()) != nil }() {
		tx.Rollback()
		for _, j := range ran {
			j.done <- s.runSingleTx(j.fn)
		}
	} else {
		for _, j := range ran {
			j.done <- nil
		}
	}

	if len(deferred) > 0 {
		s.drainPriorityCh()
		s.doExecuteBatch(deferred, true)
	}
}

func (s *Store) runSingleTx(fn func(*sql.Tx) error) error {
	tx, err := s.wdb.Begin()
	if err != nil {
		return wrapBusy(err)
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return wrapBusy(err)
	}
	return wrapBusy(tx.Commit())
}
