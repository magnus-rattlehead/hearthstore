package storage

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const storageFormat = "datastore-badger-v5\n"
const snapshotRetention = time.Hour

// ErrIncompatibleData identifies storage that Hearthstore cannot open.
var ErrIncompatibleData = errors.New("incompatible storage data")

// Txn is the transaction type used by storage mutation helpers.
type Txn struct {
	*badger.Txn
	writtenBytes int64
}

func (tx *Txn) Set(key, value []byte) error {
	if err := tx.Txn.Set(key, value); err != nil {
		return err
	}
	tx.writtenBytes += int64(len(key) + len(value))
	return nil
}

func (tx *Txn) Delete(key []byte) error {
	if err := tx.Txn.Delete(key); err != nil {
		return err
	}
	tx.writtenBytes += int64(len(key))
	return nil
}

var (
	monoMu     sync.Mutex
	lastTimeNs int64
)

func monotonicNow() *timestamppb.Timestamp {
	monoMu.Lock()
	defer monoMu.Unlock()
	n := time.Now().UnixNano()
	if n <= lastTimeNs {
		n = lastTimeNs + 1
	}
	lastTimeNs = n
	return timestamppb.New(time.Unix(0, n))
}

func enc(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func dec(value string) string {
	decoded, _ := base64.RawURLEncoding.DecodeString(value)
	return string(decoded)
}

func itemValue(item *badger.Item) ([]byte, error) {
	var value []byte
	err := item.Value(func(encoded []byte) error {
		value = append(value, encoded...)
		return nil
	})
	return value, err
}

// Store is the Badger-backed Datastore entity store.
type Store struct {
	db                    *badger.DB
	dbPath                string
	scratchDir            string
	exactSlots            chan struct{}
	exactAccumulatorBytes int

	done     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	workerMu sync.Mutex
	closing  bool
	wg       sync.WaitGroup

	compositeBuilds     sync.Map
	compositeBuildSlots chan struct{}

	commitTotal   atomic.Int64
	txnTooBig     atomic.Int64
	latestTs      atomic.Uint64
	gcWriteBytes  atomic.Int64
	gcWake        chan struct{}
	commitMu      sync.Mutex
	maintenanceMu sync.RWMutex
	snapshotMu    sync.Mutex
	snapshots     map[uint64]int
	discardTs     uint64
	// recoveryErr is protected by maintenanceMu. Only reopening clears it.
	recoveryErr error
	commitTxn   func(*Txn, uint64) error
	syncDB      func() error
}

// New opens or creates a Badger store at dataDir/badger.
func New(dataDir string) (*Store, error) {
	return NewWithOptions(dataDir, OpenOptions{})
}

// NewWithOptions opens storage with explicit opening-time cache budgets.
func NewWithOptions(dataDir string, options OpenOptions) (*Store, error) {
	if options.BlockCacheBytes == 0 {
		options.BlockCacheBytes = DefaultBlockCacheBytes
	}
	if options.IndexCacheBytes == 0 {
		options.IndexCacheBytes = DefaultIndexCacheBytes
	}
	if options.BlockCacheBytes < 0 || options.IndexCacheBytes < 0 {
		return nil, fmt.Errorf("cache budgets must be positive")
	}
	if err := CheckCompatibility(dataDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}
	dbPath := filepath.Join(dataDir, "badger")
	scratchDir := filepath.Join(dataDir, "scratch")
	db, err := badger.OpenManaged(badgerOptions(dbPath).WithBlockCacheSize(options.BlockCacheBytes).WithIndexCacheSize(options.IndexCacheBytes))
	if err != nil {
		return nil, fmt.Errorf("opening Badger: %w", err)
	}
	if err := os.RemoveAll(scratchDir); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("clearing query scratch: %w", err)
	}
	if err := os.MkdirAll(scratchDir, 0o700); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("creating query scratch: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "storage-format"), []byte(storageFormat), 0o644); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("writing storage format: %w", err)
	}
	limits, err := deriveExactQueryLimits(DefaultExactQueryMemoryBytes, DefaultQueryConcurrency)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	storeCtx, cancel := context.WithCancel(context.Background())
	s := &Store{
		db:                    db,
		dbPath:                dbPath,
		scratchDir:            scratchDir,
		exactSlots:            make(chan struct{}, limits.maxAccumulators),
		exactAccumulatorBytes: limits.accumulatorBytes,
		done:                  make(chan struct{}),
		gcWake:                make(chan struct{}, 1),
		ctx:                   storeCtx,
		cancel:                cancel,
		compositeBuildSlots:   make(chan struct{}, 1),
	}
	s.latestTs.Store(db.MaxVersion())
	s.commitTxn = func(tx *Txn, ts uint64) error { return tx.CommitAt(ts, nil) }
	s.syncDB = db.Sync
	if err := s.recoverImport(); err != nil {
		cancel()
		_ = db.Close()
		return nil, fmt.Errorf("recovering interrupted import: %w", err)
	}
	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.valueLogGCLoop() }()
	return s, nil
}

// ConfigureExactQueries sets the scratch-memory ceiling and top-level query concurrency.
// It must be called before serving queries.
func (s *Store) ConfigureExactQueries(memoryBytes int64, concurrency int) error {
	limits, err := deriveExactQueryLimits(memoryBytes, concurrency)
	if err != nil {
		return err
	}
	s.exactSlots = make(chan struct{}, limits.maxAccumulators)
	s.exactAccumulatorBytes = limits.accumulatorBytes
	return nil
}

func badgerOptions(dbPath string) badger.Options {
	return badger.DefaultOptions(dbPath).
		WithValueThreshold(16 << 10).
		WithSyncWrites(false).
		WithNumVersionsToKeep(1).
		WithBlockCacheSize(DefaultBlockCacheBytes).
		WithIndexCacheSize(DefaultIndexCacheBytes).
		WithExternalMagic(1).
		WithLogger(badgerLogger{})
}

// CheckCompatibility validates the storage markers without opening the database.
func CheckCompatibility(dataDir string) error {
	if err := rejectIncompatibleDataDir(dataDir); err != nil {
		return err
	}
	formatPath := filepath.Join(dataDir, "storage-format")
	dbPath := filepath.Join(dataDir, "badger")
	if data, err := os.ReadFile(formatPath); err == nil {
		if string(data) != storageFormat {
			return fmt.Errorf("%w: unsupported storage format %q", ErrIncompatibleData, string(data))
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("reading storage format: %w", err)
	} else if entries, readErr := os.ReadDir(dbPath); readErr == nil && len(entries) > 0 {
		return fmt.Errorf("%w: unmarked Badger directory %s", ErrIncompatibleData, dbPath)
	}
	return nil
}

func rejectIncompatibleDataDir(dataDir string) error {
	for _, name := range []string{"hearthstore.db", "hearthstore.db-wal", "hearthstore.db-shm"} {
		if _, err := os.Stat(filepath.Join(dataDir, name)); err == nil {
			return fmt.Errorf("%w: file found at %s", ErrIncompatibleData, filepath.Join(dataDir, name))
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("checking data directory: %w", err)
		}
	}
	return nil
}

const valueLogGCDiscardRatio = 0.5

func valueLogGCReady(writtenBytes, valueLogFileBytes int64) bool {
	return valueLogFileBytes > 0 && writtenBytes >= valueLogFileBytes
}

func (s *Store) recordGCWrites(writtenBytes int64) {
	total := s.gcWriteBytes.Add(writtenBytes)
	if !valueLogGCReady(total, s.db.Opts().ValueLogFileSize) {
		return
	}
	s.gcWriteBytes.Store(0)
	select {
	case s.gcWake <- struct{}{}:
	default:
	}
}

func (s *Store) valueLogGCLoop() {
	for {
		select {
		case <-s.gcWake:
			s.advanceDiscardTime(time.Now().Add(-snapshotRetention))
			for {
				err := s.db.RunValueLogGC(valueLogGCDiscardRatio)
				if errors.Is(err, badger.ErrNoRewrite) {
					break
				}
				if err != nil {
					break
				}
			}
		case <-s.done:
			return
		}
	}
}

// Close stops maintenance, syncs acknowledged data, and closes Badger.
func (s *Store) Close() error {
	s.workerMu.Lock()
	if !s.closing {
		s.closing = true
		s.cancel()
		close(s.done)
	}
	s.workerMu.Unlock()
	s.wg.Wait()
	return errors.Join(s.Sync(), s.db.Close())
}

// Sync flushes acknowledged writes to durable storage.
func (s *Store) Sync() error { return s.syncDB() }

// CheckAvailable rejects access when recovery must complete before serving data.
func (s *Store) CheckAvailable() error {
	s.maintenanceMu.RLock()
	defer s.maintenanceMu.RUnlock()
	return s.checkAvailableUnlocked()
}

func (s *Store) checkAvailableUnlocked() error {
	if s.recoveryErr != nil {
		return status.Error(codes.FailedPrecondition, "storage recovery required; restart Hearthstore to recover before retrying")
	}
	return nil
}

func (s *Store) RunInTx(fn func(*Txn) error) error {
	return s.RunInTxCtx(context.Background(), fn)
}

func (s *Store) RunInTxCtx(ctx context.Context, fn func(*Txn) error) error {
	return s.runUpdate(ctx, false, fn)
}

// RunBatchedTx executes an independent concurrent Badger transaction with conflict retries.
func (s *Store) RunBatchedTx(ctx context.Context, fn func(*Txn) error) error {
	return s.runUpdate(ctx, true, fn)
}

func (s *Store) runUpdate(ctx context.Context, retry bool, fn func(*Txn) error) error {
	err := s.runUpdateRaw(ctx, retry, fn)
	if errors.Is(err, badger.ErrConflict) {
		return status.Error(codes.Aborted, "storage transaction conflicted; retry the operation")
	}
	if errors.Is(err, badger.ErrTxnTooBig) {
		s.txnTooBig.Add(1)
		return status.Error(codes.ResourceExhausted, "storage transaction exceeds Badger transaction limits")
	}
	return err
}

func (s *Store) runUpdateRaw(ctx context.Context, retry bool, fn func(*Txn) error) error {
	s.maintenanceMu.RLock()
	defer s.maintenanceMu.RUnlock()
	if err := s.checkAvailableUnlocked(); err != nil {
		return err
	}
	return s.runUpdateRawUnlocked(ctx, retry, fn)
}

func (s *Store) runUpdateRawUnlocked(ctx context.Context, retry bool, fn func(*Txn) error) error {
	if s.db.IsClosed() {
		return badger.ErrDBClosed
	}
	attempts := 1
	if retry {
		attempts = 8
	}
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		txn := &Txn{Txn: s.db.NewTransactionAt(s.latestTs.Load(), true)}
		err := fn(txn)
		if err == nil {
			err = ctx.Err()
		}
		if err == nil {
			s.commitMu.Lock()
			commitTs := uint64(time.Now().UnixNano())
			if latest := s.latestTs.Load(); commitTs <= latest {
				commitTs = latest + 1
			}
			err = ctx.Err()
			if err == nil {
				err = s.commitTxn(txn, commitTs)
			}
			if err == nil {
				s.latestTs.Store(commitTs)
				s.recordGCWrites(txn.writtenBytes)
			}
			s.commitMu.Unlock()
		}
		txn.Discard()
		s.commitTotal.Add(1)
		if errors.Is(err, badger.ErrConflict) {
			if attempt+1 < attempts {
				delay := (100 * time.Microsecond) << min(attempt, 6)
				delay = delay/2 + time.Duration(rand.Int64N(int64(delay/2)))
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
				continue
			}
			return err
		}
		return err
	}
	return badger.ErrConflict
}

func (s *Store) view(fn func(*Txn) error) error {
	s.maintenanceMu.RLock()
	defer s.maintenanceMu.RUnlock()
	if err := s.checkAvailableUnlocked(); err != nil {
		return err
	}
	return s.viewAtUnlocked(uint64(max(time.Now().UnixNano(), int64(s.latestTs.Load()))), fn)
}

// ReadTime captures a snapshot after any atomic import has finished publishing.
func (s *Store) ReadTime() time.Time {
	s.maintenanceMu.RLock()
	defer s.maintenanceMu.RUnlock()
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	return time.Unix(0, max(time.Now().UnixNano(), int64(s.latestTs.Load())))
}

func (s *Store) viewAt(readTs uint64, fn func(*Txn) error) error {
	s.maintenanceMu.RLock()
	defer s.maintenanceMu.RUnlock()
	if err := s.checkAvailableUnlocked(); err != nil {
		return err
	}
	return s.viewAtUnlocked(readTs, fn)
}

func (s *Store) viewAtUnlocked(readTs uint64, fn func(*Txn) error) error {
	if s.db.IsClosed() {
		return badger.ErrDBClosed
	}
	release, err := s.PinReadTime(time.Unix(0, int64(readTs)))
	if err != nil {
		return err
	}
	defer release()
	txn := &Txn{Txn: s.db.NewTransactionAt(readTs, false)}
	defer txn.Discard()
	return fn(txn)
}

type CounterSnapshot struct {
	CommitTotal int64
	TxnTooBig   int64
}

func (s *Store) CounterSnapshot() CounterSnapshot {
	return CounterSnapshot{
		CommitTotal: s.commitTotal.Load(),
		TxnTooBig:   s.txnTooBig.Load(),
	}
}

type badgerLogger struct{}

func (badgerLogger) Errorf(f string, a ...any) { slog.Error("Badger", "message", fmt.Sprintf(f, a...)) }
func (badgerLogger) Warningf(f string, a ...any) {
	slog.Warn("Badger", "message", fmt.Sprintf(f, a...))
}
func (badgerLogger) Infof(string, ...any)  {}
func (badgerLogger) Debugf(string, ...any) {}
