package storage

import (
	"context"
	"encoding/json"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/dgraph-io/badger/v4"
	"github.com/magnus-rattlehead/hearthstore/internal/propertypath"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxCompositeIndexBytes = 2 << 20

// compositeIndexesForKind reads the catalog in the entity write transaction.
// The optional cache belongs to that transaction, never to the store lifetime.
func compositeIndexesForKind(tx *Txn, project, kind string, acc *CommitAccumulator) ([]DsCompositeIndex, error) {
	cacheKey := compositeIndexCacheKey{project: project, kind: kind}
	if acc != nil {
		if indexes, ok := acc.compositeIndexes[cacheKey]; ok {
			return indexes, nil
		}
	}
	prefix := []byte("meta/index/" + enc(project) + "/")
	it := tx.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	var indexes []DsCompositeIndex
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		raw, err := itemValue(it.Item())
		if err != nil {
			return nil, err
		}
		var idx DsCompositeIndex
		if err := json.Unmarshal(raw, &idx); err != nil {
			return nil, err
		}
		if idx.Kind == kind && (idx.State == DsIndexReady || idx.State == DsIndexCreating) {
			indexes = append(indexes, idx)
		}
	}
	if acc != nil {
		acc.compositeIndexes[cacheKey] = indexes
	}
	return indexes, nil
}

// validateEntityIndexLimits counts logical entries, not physical generations,
// covering payloads, or the ascending/descending builtin copies. Cartesian
// counts and byte totals are computed from domains without expanding tuples.
func validateEntityIndexLimits(ctx context.Context, entity *datastorepb.Entity, path string, indexes []DsCompositeIndex) error {
	properties, err := indexedProperties(entity)
	if err != nil {
		return err
	}
	var count, size int64
	for _, values := range properties {
		count += int64(len(values))
	}
	for _, idx := range indexes {
		literal, err := logicalIndexDomains(ctx, idx, entity, false)
		if err != nil {
			return err
		}
		var canonical []map[string]int64
		if compositeHasDottedProperty(idx) {
			canonical, err = logicalIndexDomains(ctx, idx, entity, true)
			if err != nil {
				return err
			}
		}
		keyBytes := datastoreKeyBytes(entity.Key) + 32
		a, aBytes := logicalDomainTotals(literal, keyBytes)
		b, bBytes := logicalDomainTotals(canonical, keyBytes)
		// An oversized interpretation alone cannot be rescued by unioning it.
		if a > maxEntityIndexedValues || b > maxEntityIndexedValues {
			return indexCountLimitError()
		}
		var common []map[string]int64
		if literal != nil && canonical != nil {
			common = make([]map[string]int64, len(literal))
			for i, domain := range literal {
				common[i] = make(map[string]int64)
				for value, size := range domain {
					if _, ok := canonical[i][value]; ok {
						common[i][value] = size
					}
				}
			}
		}
		c, cBytes := logicalDomainTotals(common, keyBytes)
		scopes := int64(1)
		if idx.Ancestor {
			scopes = int64(len(indexScopes(path)) - 1)
		}
		count += (a + b - c) * scopes
		size += (aBytes + bBytes - cBytes) * scopes
		if count > maxEntityIndexedValues {
			return indexCountLimitError()
		}
		if size > maxCompositeIndexBytes {
			return status.Error(codes.InvalidArgument, "entity exceeds the Datastore 2 MiB composite index size limit")
		}
	}
	if count > maxEntityIndexedValues {
		return indexCountLimitError()
	}
	return ctx.Err()
}

func indexCountLimitError() error {
	return status.Errorf(codes.InvalidArgument, "entity exceeds %d indexed property values and composite index entries", maxEntityIndexedValues)
}

func logicalIndexDomains(ctx context.Context, idx DsCompositeIndex, entity *datastorepb.Entity, canonical bool) ([]map[string]int64, error) {
	domains := make([]map[string]int64, len(idx.Properties))
	work := QueryWorkFromContext(ctx)
	for i, property := range idx.Properties {
		value := propertypath.QueryValue(entity, property.Name, canonical)
		if value == nil || value.ExcludeFromIndexes {
			return nil, nil
		}
		values := []*datastorepb.Value{value}
		if array := value.GetArrayValue(); array != nil {
			values = array.Values
		}
		domains[i] = make(map[string]int64)
		for _, candidate := range values {
			if err := work.Checkpoint(ctx); err != nil {
				return nil, err
			}
			if candidate == nil || candidate.ExcludeFromIndexes || candidate.GetEntityValue() != nil {
				continue
			}
			raw, ok := orderedValue(candidate, false)
			if !ok {
				return nil, status.Errorf(codes.InvalidArgument, "cannot index property %q", property.Name)
			}
			domains[i][string(raw)] = datastoreValueBytes(candidate)
		}
		if len(domains[i]) == 0 {
			return nil, nil
		}
	}
	return domains, nil
}

func logicalDomainTotals(domains []map[string]int64, keyBytes int64) (int64, int64) {
	if domains == nil {
		return 0, 0
	}
	count := int64(1)
	for _, domain := range domains {
		if len(domain) == 0 {
			return 0, 0
		}
		if count > maxEntityIndexedValues/int64(len(domain)) {
			return maxEntityIndexedValues + 1, 0
		}
		count *= int64(len(domain))
	}
	size := keyBytes * count
	for _, domain := range domains {
		var sum int64
		for _, bytes := range domain {
			sum += bytes
		}
		size += sum * (count / int64(len(domain)))
	}
	return count, size
}

func datastoreKeyBytes(key *datastorepb.Key) int64 {
	size := int64(16)
	if namespace := key.GetPartitionId().GetNamespaceId(); namespace != "" {
		size += int64(len(namespace) + 1)
	}
	for _, element := range key.GetPath() {
		size += int64(len(element.Kind) + 1)
		if _, named := element.IdType.(*datastorepb.Key_PathElement_Name); named {
			size += int64(len(element.GetName()) + 1)
		} else {
			size += 8
		}
	}
	return size
}

func datastoreValueBytes(value *datastorepb.Value) int64 {
	switch v := value.GetValueType().(type) {
	case *datastorepb.Value_NullValue, *datastorepb.Value_BooleanValue:
		return 1
	case *datastorepb.Value_IntegerValue, *datastorepb.Value_DoubleValue, *datastorepb.Value_TimestampValue:
		return 8
	case *datastorepb.Value_StringValue:
		return int64(len(v.StringValue) + 1)
	case *datastorepb.Value_BlobValue:
		return int64(len(v.BlobValue))
	case *datastorepb.Value_GeoPointValue:
		return 16
	case *datastorepb.Value_KeyValue:
		return datastoreKeyBytes(v.KeyValue)
	}
	return 0 // Embedded entities and arrays are not scalar composite entries.
}
