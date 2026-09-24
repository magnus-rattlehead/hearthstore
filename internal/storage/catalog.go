package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/dgraph-io/badger/v4"

	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
)

type kindCatalogEntry struct {
	Project, Database, Namespace, Kind string
	Count                              int64
}

func kindCatalogKey(project, database, namespace, kind string) []byte {
	return []byte("meta/kind/" + enc(project) + "/" + enc(database) + "/" + enc(namespace) + "/" + enc(kind))
}

func adjustKindCount(tx *Txn, project, database, namespace, kind string, delta int64) error {
	key := kindCatalogKey(project, database, namespace, kind)
	var count int64
	item, err := tx.Get(key)
	if err == nil {
		value, err := itemValue(item)
		if err != nil {
			return err
		}
		if len(value) != 8 {
			return fmt.Errorf("invalid kind catalog count")
		}
		count = int64(binary.BigEndian.Uint64(value))
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}
	count += delta
	if count < 0 {
		return fmt.Errorf("negative kind catalog count")
	}
	// Java retains metadata after the last entity is deleted.
	return tx.Set(key, binary.BigEndian.AppendUint64(nil, uint64(count)))
}

func (s *Store) kindCatalog(tx *Txn, project string) ([]kindCatalogEntry, error) {
	var entries []kindCatalogEntry
	err := s.visitKindCatalog(tx, project, func(entry kindCatalogEntry) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, err
}

func (s *Store) visitKindCatalog(tx *Txn, project string, visit func(kindCatalogEntry) error) error {
	prefix := []byte("meta/kind/")
	if project != "" {
		prefix = append(prefix, enc(project)+"/"...)
	}
	iterator := tx.NewIterator(badger.DefaultIteratorOptions)
	defer iterator.Close()
	for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
		parts := strings.Split(string(iterator.Item().Key()), "/")
		if len(parts) != 6 {
			return fmt.Errorf("invalid kind catalog key")
		}
		value, err := itemValue(iterator.Item())
		if err != nil {
			return err
		}
		if len(value) != 8 {
			return fmt.Errorf("invalid kind catalog count")
		}
		if err := visit(kindCatalogEntry{dec(parts[2]), dec(parts[3]), dec(parts[4]), dec(parts[5]), int64(binary.BigEndian.Uint64(value))}); err != nil {
			return err
		}
	}
	return nil
}

// DsVisitMetadataAsOf streams synthetic kinds/namespaces without scanning entities.
func (s *Store) DsVisitMetadataAsOf(ctx context.Context, snapshot time.Time, project, database, namespace, kind string, visit func(*DsEntityRow) error) (int64, error) {
	work := QueryWorkFromContext(ctx)
	var scanned int64
	err := s.viewAt(uint64(snapshot.UnixNano()), func(tx *Txn) error {
		if kind == "__property__" {
			var err error
			scanned, err = visitPropertyCatalog(ctx, tx, project, database, namespace, propertyCatalogPrefix(project, database, namespace), visit)
			return err
		}
		lastNamespace, haveNamespace := "", false
		return s.visitKindCatalog(tx, project, func(entry kindCatalogEntry) error {
			work.Charge(WorkIndexEntries, 1)
			if err := work.Checkpoint(ctx); err != nil {
				return err
			}
			if entry.Database != database {
				return nil
			}
			scanned++
			element := &datastorepb.Key_PathElement{Kind: kind}
			switch kind {
			case "__kind__":
				if entry.Namespace != namespace {
					return nil
				}
				element.IdType = &datastorepb.Key_PathElement_Name{Name: entry.Kind}
			case "__namespace__":
				if haveNamespace && lastNamespace == entry.Namespace {
					return nil
				}
				lastNamespace, haveNamespace = entry.Namespace, true
				if entry.Namespace == "" {
					element.IdType = &datastorepb.Key_PathElement_Id{Id: 1}
				} else {
					element.IdType = &datastorepb.Key_PathElement_Name{Name: entry.Namespace}
				}
			default:
				return fmt.Errorf("unsupported metadata kind %q", kind)
			}
			key := &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: project, DatabaseId: database, NamespaceId: namespace}, Path: []*datastorepb.Key_PathElement{element}}
			return visit(&DsEntityRow{Entity: &datastorepb.Entity{Key: key}, Path: keycodec.Path(key.Path)})
		})
	})
	return scanned, err
}
