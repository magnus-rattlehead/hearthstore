package storage

import (
	"bytes"
	"errors"
	"strings"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ValidateQueryCursor validates the physical position without reading the entity:
// deleting or updating the entity must not invalidate its old cursor position.
func (s *Store) ValidateQueryCursor(c CursorPayload, key *datastorepb.Key, ancestor string, logical map[string][]byte) error {
	invalid := func() error { return status.Error(codes.InvalidArgument, "invalid cursor position") }
	partition := key.GetPartitionId()
	kind := key.Path[len(key.Path)-1].Kind
	var base []byte
	var properties []DsIndexProperty
	if strings.HasPrefix(c.I, "builtin:") {
		if c.G != 1 {
			return invalid()
		}
		property := strings.TrimPrefix(c.I, "builtin:")
		if property == "" {
			return invalid()
		}
		base = builtinIndexBase(partition.ProjectId, partition.DatabaseId, partition.NamespaceId, kind, property, ancestor)
		properties = []DsIndexProperty{{Name: property}}
	} else {
		idx, err := s.GetDsCompositeIndex(partition.ProjectId, c.I)
		if errors.Is(err, ErrIndexNotFound) {
			return invalid()
		}
		if err != nil {
			return err
		}
		if idx.Kind != kind || c.G <= 0 || c.G != idx.ActiveGeneration {
			return invalid()
		}
		base = append(compositeScanBase(idx, partition.DatabaseId, partition.NamespaceId, c.G, ancestor), '/')
		properties = idx.Properties
	}
	if !bytes.HasPrefix(c.K, base) {
		return invalid()
	}
	tail := c.K[len(base):]
	for _, property := range properties {
		component, rest, ok := takeIndexComponent(tail)
		if !ok || len(component) == 0 {
			return invalid()
		}
		if value := logical[property.Name]; value != nil {
			raw := bytes.Clone(value)
			if property.Desc {
				for i := range raw {
					raw[i] = ^raw[i]
				}
			}
			if !bytes.Equal(component, encodeIndexComponent(raw)) {
				return invalid()
			}
		}
		tail = rest
	}
	expected := append(appendIndexComponent(nil, keycodec.Ordered(key)), enc(c.P)...)
	if !bytes.Equal(tail, expected) {
		return invalid()
	}
	return nil
}
