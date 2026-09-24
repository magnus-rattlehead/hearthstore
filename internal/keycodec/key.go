// Package keycodec defines collision-free Datastore key identities.
package keycodec

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

func component(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }

// Path encodes typed path elements. Its ASCII representation survives JSON cursors
// unchanged; ordering is defined by Ordered, not by this representation.
func Path(elements []*datastorepb.Key_PathElement) string {
	parts := make([]string, 0, len(elements)*2)
	for _, element := range elements {
		parts = append(parts, component(element.Kind))
		switch id := element.IdType.(type) {
		case *datastorepb.Key_PathElement_Id:
			var value [8]byte
			binary.BigEndian.PutUint64(value[:], uint64(id.Id)^(1<<63))
			parts = append(parts, "i"+base64.RawURLEncoding.EncodeToString(value[:]))
		case *datastorepb.Key_PathElement_Name:
			parts = append(parts, "n"+component(id.Name))
		default:
			parts = append(parts, "u")
		}
	}
	return strings.Join(parts, "/")
}

// AppendID appends a numeric path element to an encoded parent path.
func AppendID(parent, kind string, id int64) string {
	child := Path([]*datastorepb.Key_PathElement{{Kind: kind, IdType: &datastorepb.Key_PathElement_Id{Id: id}}})
	if parent == "" {
		return child
	}
	return parent + "/" + child
}

// ParsePath decodes a complete typed storage path, rejecting malformed components.
func ParsePath(path string) ([]*datastorepb.Key_PathElement, error) {
	parts := strings.Split(path, "/")
	if len(parts)%2 != 0 {
		return nil, fmt.Errorf("invalid key path")
	}
	out := make([]*datastorepb.Key_PathElement, 0, len(parts)/2)
	for i := 0; i < len(parts); i += 2 {
		kind, err := base64.RawURLEncoding.DecodeString(parts[i])
		if err != nil || len(kind) == 0 || len(parts[i+1]) < 2 {
			return nil, fmt.Errorf("invalid key path component")
		}
		value, err := base64.RawURLEncoding.DecodeString(parts[i+1][1:])
		if err != nil {
			return nil, fmt.Errorf("invalid key identifier: %w", err)
		}
		element := &datastorepb.Key_PathElement{Kind: string(kind)}
		switch parts[i+1][0] {
		case 'i':
			if len(value) != 8 {
				return nil, fmt.Errorf("invalid numeric key identifier")
			}
			element.IdType = &datastorepb.Key_PathElement_Id{Id: int64(binary.BigEndian.Uint64(value) ^ (1 << 63))}
		case 'n':
			if len(value) == 0 {
				return nil, fmt.Errorf("empty key name")
			}
			element.IdType = &datastorepb.Key_PathElement_Name{Name: string(value)}
		default:
			return nil, fmt.Errorf("invalid key identifier type")
		}
		out = append(out, element)
	}
	return out, nil
}

func appendString(out []byte, value string) []byte {
	for i := 0; i < len(value); i++ {
		out = append(out, value[i])
		if value[i] == 0 {
			out = append(out, 255)
		}
	}
	return append(out, 0, 0)
}

// Ordered returns a typed, byte-sortable identity including all partition fields.
func Ordered(key *datastorepb.Key) []byte {
	partition := key.GetPartitionId()
	database := partition.GetDatabaseId()
	if database == "" {
		database = "(default)"
	}
	out := appendString(nil, partition.GetProjectId())
	out = appendString(out, database)
	out = appendString(out, partition.GetNamespaceId())
	for _, element := range key.GetPath() {
		out = appendString(out, element.Kind)
		switch id := element.IdType.(type) {
		case *datastorepb.Key_PathElement_Id:
			out = append(out, 1)
			out = binary.BigEndian.AppendUint64(out, uint64(id.Id)^(1<<63))
		case *datastorepb.Key_PathElement_Name:
			out = appendString(append(out, 2), id.Name)
		default:
			out = append(out, 0)
		}
	}
	return out
}
