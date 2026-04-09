package storage

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS documents (
    project     TEXT NOT NULL,
    database    TEXT NOT NULL,
    path        TEXT NOT NULL,
    collection  TEXT NOT NULL,
    data        BLOB NOT NULL,
    create_time TEXT NOT NULL,
    update_time TEXT NOT NULL,
    deleted     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (project, database, path)
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
    value_ref       TEXT
);

CREATE INDEX IF NOT EXISTS idx_field ON field_index (
    project, database, collection_path, field_path,
    value_string, value_int, value_double
);

CREATE INDEX IF NOT EXISTS idx_doc_fields ON field_index (project, database, doc_path);
`

// Store is the disk-backed document store.
type Store struct {
	db *sql.DB
}

// New opens (or creates) the SQLite database at dataDir/hearthstore.db.
func New(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "hearthstore.db")

	// Ensure the data directory exists.
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite: %w", err)
	}

	// WAL mode for concurrent reads + writes, mmap for OS-managed paging.
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA mmap_size=8589934592", // 8 GB — OS decides what stays in RAM
		"PRAGMA cache_size=-131072",   // 128 MB in-process page cache
		"PRAGMA foreign_keys=ON",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return nil, fmt.Errorf("setting pragma %q: %w", p, err)
		}
	}

	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("applying schema: %w", err)
	}

	return &Store{db: db}, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// DB returns the raw *sql.DB for use by sub-packages.
func (s *Store) DB() *sql.DB {
	return s.db
}
