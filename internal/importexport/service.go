package importexport

import (
	"context"
	"fmt"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/exportformat"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"google.golang.org/protobuf/proto"
)

// Export writes one consistent project/database snapshot in Google's
// Datastore export format.
func Export(ctx context.Context, store *storage.Store, directory, project, database string, started time.Time) (string, exportformat.Stats, error) {
	return exportformat.WriteExport(ctx, directory, started, func(yield func(*datastorepb.Entity) error) error {
		return store.DsVisitAllEntities(ctx, project, database, func(_ string, row *storage.DsEntityRow) error {
			return yield(row.Entity)
		})
	})
}

// Import validates a complete export before atomically applying its entities.
func Import(ctx context.Context, store *storage.Store, source, project, database string) (exportformat.Stats, error) {
	paths, err := store.NewPathAccumulator(ctx)
	if err != nil {
		return exportformat.Stats{}, fmt.Errorf("creating import preflight workspace: %w", err)
	}
	preflightStats, preflightErr := exportformat.VisitExport(ctx, source, project, database, func(entity *datastorepb.Entity) error {
		encodedKey, err := proto.MarshalOptions{Deterministic: true}.Marshal(entity.GetKey())
		if err != nil {
			return fmt.Errorf("encoding import key: %w", err)
		}
		return paths.Add(string(encodedKey))
	})
	if preflightErr == nil {
		var unique int64
		unique, preflightErr = paths.Count()
		if preflightErr == nil && unique != preflightStats.Entities {
			preflightErr = fmt.Errorf("import contains duplicate entity keys")
		}
	}
	if closeErr := paths.Close(); preflightErr == nil {
		preflightErr = closeErr
	}
	if preflightErr != nil {
		return exportformat.Stats{}, fmt.Errorf("preflighting import: %w", preflightErr)
	}

	err = store.DsImportAtomic(ctx, project, database, func(yield func(*datastorepb.Entity) error) error {
		_, err := exportformat.VisitExport(ctx, source, project, database, yield)
		return err
	})
	if err != nil {
		return exportformat.Stats{}, err
	}
	return preflightStats, nil
}
