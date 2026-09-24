package storage

import (
	"bytes"
	"context"
	"fmt"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
	"github.com/magnus-rattlehead/hearthstore/internal/propertypath"
)

// firstCompositeArrayEntry selects one representative independently of a page
// cursor. Choosing each component independently avoids a Cartesian allocation.
func firstCompositeArrayEntry(ctx context.Context, idx DsCompositeIndex, database, namespace, ancestor, path string, entity *datastorepb.Entity, prefix []byte, bounds compositeQueryBounds) ([]byte, error) {
	first, err := firstCompositeInterpretationEntry(ctx, idx, database, namespace, ancestor, path, entity, prefix, bounds, false)
	if err != nil {
		return nil, err
	}
	if !compositeHasDottedProperty(idx) {
		return first, nil
	}
	nested, err := firstCompositeInterpretationEntry(ctx, idx, database, namespace, ancestor, path, entity, prefix, bounds, true)
	if err != nil {
		return nil, err
	}
	if len(nested) > 0 && (len(first) == 0 || bytes.Compare(nested, first) < 0) {
		return nested, nil
	}
	return first, nil
}

func firstCompositeInterpretationEntry(ctx context.Context, idx DsCompositeIndex, database, namespace, ancestor, path string, entity *datastorepb.Entity, prefix []byte, bounds compositeQueryBounds, canonical bool) ([]byte, error) {
	work := QueryWorkFromContext(ctx)
	hasArray := compositeHasDottedProperty(idx)
	for _, property := range idx.Properties {
		hasArray = hasArray || propertypath.QueryValue(entity, property.Name, canonical).GetArrayValue() != nil
	}
	if !hasArray {
		return nil, nil
	}
	var equality [][]byte
	for len(prefix) > 0 {
		component, rest, ok := takeIndexComponent(prefix)
		if !ok {
			return []byte{}, nil
		}
		equality = append(equality, component)
		prefix = rest
	}
	key := append(compositeScanBase(idx, database, namespace, idx.ActiveGeneration, ancestor), '/')
	for i, property := range idx.Properties {
		value := propertypath.QueryValue(entity, property.Name, canonical)
		values := []*datastorepb.Value{value}
		if array := value.GetArrayValue(); array != nil {
			values = array.Values
		}
		var first []byte
		for _, candidate := range values {
			if err := work.Checkpoint(ctx); err != nil {
				return nil, err
			}
			if candidate == nil || candidate.ExcludeFromIndexes {
				continue
			}
			raw, ok := orderedValue(candidate, property.Desc)
			if !ok {
				continue
			}
			encoded := encodeIndexComponent(raw)
			if i < len(equality) && !bytes.Equal(encoded, equality[i]) {
				continue
			}
			if i == len(equality) && !spanContains(bounds.compositeScanBounds, encoded) {
				continue
			}
			if !spanContains(bounds.properties[property.Name], encoded) {
				continue
			}
			if first == nil || bytes.Compare(encoded, first) < 0 {
				first = encoded
			}
		}
		if first == nil {
			return []byte{}, nil
		}
		key = append(key, first...)
		key = append(key, 0, 0)
	}
	key = appendIndexComponent(key, keycodec.Ordered(entity.Key))
	return append(key, enc(path)...), nil
}

func compositeCoverValue(idx DsCompositeIndex, database, namespace, path string, key []byte, entity *datastorepb.Entity, record dsRecord) ([]byte, error) {
	scope := ""
	if idx.Ancestor {
		// The scope is encoded directly after the generation in the index prefix.
		for _, ancestor := range indexScopes(path)[1:] {
			base := append(compositeScanBase(idx, database, namespace, idx.ActiveGeneration, ancestor), '/')
			if bytes.HasPrefix(key, base) {
				scope = ancestor
				break
			}
		}
	}
	projected, err := compositeProjection(idx, database, namespace, scope, key, entity)
	if err != nil {
		return nil, err
	}
	return encodeIndexValue(path, record, projected)
}

// compositeProjection selects the actual scalar tuple represented by an index
// entry; expanding the source arrays again would destroy global index ordering.
func compositeProjection(idx DsCompositeIndex, database, namespace, ancestor string, key []byte, entity *datastorepb.Entity) (*datastorepb.Entity, error) {
	base := append(compositeScanBase(idx, database, namespace, idx.ActiveGeneration, ancestor), '/')
	if !bytes.HasPrefix(key, base) {
		return nil, fmt.Errorf("index entry outside projection scope")
	}
	rest := key[len(base):]
	properties := make(map[string]*datastorepb.Value, len(idx.Properties))
	for _, property := range idx.Properties {
		component, tail, ok := takeIndexComponent(rest)
		if !ok {
			return nil, fmt.Errorf("invalid composite projection entry")
		}
		rest = tail
		propertypath.QueryValues(entity, property.Name, func(candidate *datastorepb.Value) bool {
			encoded, ok := orderedValue(candidate, property.Desc)
			if ok && bytes.Equal(component, encodeIndexComponent(encoded)) {
				properties[property.Name] = candidate
				return false
			}
			return true
		})
		if properties[property.Name] == nil {
			return nil, fmt.Errorf("composite entry does not match property %q", property.Name)
		}
	}
	return &datastorepb.Entity{Key: entity.Key, Properties: properties}, nil
}
