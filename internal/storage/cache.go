package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

const DefaultBlockCacheBytes = int64(256 << 20)

// DefaultIndexCacheBytes is a provisional finite budget, pending workload tuning.
const DefaultIndexCacheBytes = int64(64 << 20)

// OpenOptions controls caches that must be configured before Badger opens.
type OpenOptions struct{ BlockCacheBytes, IndexCacheBytes int64 }

// ConfigureBlockCache applies the exact caller-owned cache budget.
func (s *Store) ConfigureBlockCache(bytes int64) error {
	if bytes <= 0 {
		return fmt.Errorf("block cache budget must be positive")
	}
	if _, err := s.db.CacheMaxCost(badger.BlockCache, bytes); err != nil {
		return fmt.Errorf("setting block cache budget: %w", err)
	}
	return nil
}

// ConfigureAdaptiveBlockCache is retained for source compatibility. Cache
// sizing is now explicit so a workload-derived budget is applied immediately.
func (s *Store) ConfigureAdaptiveBlockCache(bytes int64) error {
	return s.ConfigureBlockCache(bytes)
}
