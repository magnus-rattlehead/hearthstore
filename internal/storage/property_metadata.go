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
	"github.com/magnus-rattlehead/hearthstore/internal/propertypath"
)

type propertyRepresentation struct{ name, representation string }

func propertyCatalogPrefix(project, database, namespace string) []byte {
	return []byte("meta/property/" + enc(project) + "/" + enc(database) + "/" + enc(namespace) + "/")
}

// propertyRepresentations records one contribution per entity/property/type,
// not per repeated array element. Representation names are the documented
// Datastore metadata names, not protobuf or Hearthstore index tags.
func propertyRepresentations(entity *datastorepb.Entity) map[propertyRepresentation]bool {
	result := make(map[propertyRepresentation]bool)
	var add func(string, *datastorepb.Value)
	add = func(name string, value *datastorepb.Value) {
		if value == nil || value.ExcludeFromIndexes {
			return
		}
		representation := ""
		switch v := value.ValueType.(type) {
		case *datastorepb.Value_IntegerValue, *datastorepb.Value_TimestampValue:
			representation = "INT64"
		case *datastorepb.Value_DoubleValue:
			representation = "DOUBLE"
		case *datastorepb.Value_BooleanValue:
			representation = "BOOLEAN"
		case *datastorepb.Value_StringValue, *datastorepb.Value_BlobValue, *datastorepb.Value_EntityValue:
			representation = "STRING"
		case *datastorepb.Value_KeyValue:
			representation = "REFERENCE"
		case *datastorepb.Value_GeoPointValue:
			representation = "POINT"
		case *datastorepb.Value_NullValue:
			representation = "NULL"
		case *datastorepb.Value_ArrayValue:
			for _, element := range v.ArrayValue.GetValues() {
				add(name, element)
			}
		}
		if representation != "" {
			result[propertyRepresentation{name, representation}] = true
		}
	}
	var walk func(string, map[string]*datastorepb.Value)
	walk = func(prefix string, properties map[string]*datastorepb.Value) {
		for name, value := range properties {
			if value == nil || value.ExcludeFromIndexes {
				continue
			}
			path := name
			if prefix != "" {
				path = prefix + "." + name
			}
			if nested := value.GetEntityValue(); nested != nil {
				add(path, value)
				walk(path, nested.Properties)
			} else if prefix == "" || propertypath.QueryValue(entity, path, true) == value {
				add(path, value)
			}
		}
	}
	walk("", entity.GetProperties())
	return result
}

func maintainPropertyCatalog(tx *Txn, project, database, namespace, kind string, oldEntity, newEntity *datastorepb.Entity) error {
	before, after := propertyRepresentations(oldEntity), propertyRepresentations(newEntity)
	deltas := make(map[propertyRepresentation]int64)
	for entry := range before {
		if !after[entry] {
			deltas[entry] = -1
		}
	}
	for entry := range after {
		if !before[entry] {
			deltas[entry] = 1
		}
	}
	prefix := string(propertyCatalogPrefix(project, database, namespace)) + enc(kind) + "/"
	for entry, delta := range deltas {
		key := []byte(prefix + enc(entry.name) + "/" + entry.representation)
		var count int64
		item, err := tx.Get(key)
		if err == nil {
			value, err := itemValue(item)
			if err != nil {
				return err
			}
			if len(value) != 8 {
				return fmt.Errorf("invalid property catalog count")
			}
			count = int64(binary.BigEndian.Uint64(value))
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		count += delta
		if count < 0 {
			return fmt.Errorf("negative property catalog count")
		}
		if count == 0 {
			err = tx.Delete(key)
		} else {
			err = tx.Set(key, binary.BigEndian.AppendUint64(nil, uint64(count)))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func visitPropertyCatalog(ctx context.Context, tx *Txn, project, database, namespace string, prefix []byte, visit func(*DsEntityRow) error) (int64, error) {
	work := QueryWorkFromContext(ctx)
	base := propertyCatalogPrefix(project, database, namespace)
	iterator := tx.NewIterator(badger.DefaultIteratorOptions)
	defer iterator.Close()
	var current *DsEntityRow
	var previous string
	var scanned int64
	flush := func() error {
		if current != nil {
			return visit(current)
		}
		return nil
	}
	for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
		work.Charge(WorkIndexEntries, 1)
		if err := work.Checkpoint(ctx); err != nil {
			return scanned, err
		}
		scanned++
		parts := strings.Split(string(iterator.Item().Key()[len(base):]), "/")
		if len(parts) != 3 {
			return scanned, fmt.Errorf("invalid property catalog key")
		}
		value, err := itemValue(iterator.Item())
		if err != nil {
			return scanned, err
		}
		if len(value) != 8 || int64(binary.BigEndian.Uint64(value)) <= 0 {
			return scanned, fmt.Errorf("invalid property catalog count")
		}
		identity := parts[0] + "/" + parts[1]
		if current == nil || identity != previous {
			if err := flush(); err != nil {
				return scanned, err
			}
			key := &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: project, DatabaseId: database, NamespaceId: namespace}, Path: []*datastorepb.Key_PathElement{
				{Kind: "__kind__", IdType: &datastorepb.Key_PathElement_Name{Name: dec(parts[0])}},
				{Kind: "__property__", IdType: &datastorepb.Key_PathElement_Name{Name: dec(parts[1])}},
			}}
			entity := &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{"property_representation": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{}}}}}
			current = &DsEntityRow{Entity: entity, Path: keycodec.Path(key.Path)}
			previous = identity
		}
		array := current.Entity.Properties["property_representation"].GetArrayValue()
		array.Values = append(array.Values, &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: parts[2]}})
	}
	return scanned, flush()
}

// DsPropertyMetadataAsOf reconstructs one synthetic property entity at the
// query's pinned snapshot after its key has passed the disk-backed sort.
func (s *Store) DsPropertyMetadataAsOf(ctx context.Context, snapshot time.Time, key *datastorepb.Key) (*datastorepb.Entity, error) {
	if len(key.GetPath()) != 2 || key.Path[0].Kind != "__kind__" || key.Path[1].Kind != "__property__" {
		return nil, fmt.Errorf("invalid property metadata key")
	}
	partition := key.GetPartitionId()
	prefix := append(propertyCatalogPrefix(partition.ProjectId, partition.DatabaseId, partition.NamespaceId), enc(key.Path[0].GetName())+"/"+enc(key.Path[1].GetName())+"/"...)
	var entity *datastorepb.Entity
	err := s.viewAt(uint64(snapshot.UnixNano()), func(tx *Txn) error {
		_, err := visitPropertyCatalog(ctx, tx, partition.ProjectId, partition.DatabaseId, partition.NamespaceId, prefix, func(row *DsEntityRow) error { entity = row.Entity; return nil })
		return err
	})
	if err == nil && entity == nil {
		return nil, fmt.Errorf("property metadata missing at pinned snapshot")
	}
	return entity, err
}
