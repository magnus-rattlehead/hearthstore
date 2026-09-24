package storage

import (
	"encoding/binary"
	"fmt"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
)

// encodeIndexValue stores only the key, indexed scalar tuple, and result metadata.
func encodeIndexValue(path string, record dsRecord, entity *datastorepb.Entity) ([]byte, error) {
	data, err := proto.Marshal(entity)
	if err != nil {
		return nil, err
	}
	cover := dsRecord{Version: record.Version, Created: record.Created, Updated: record.Updated, Data: data}
	out := binary.AppendUvarint([]byte{1}, uint64(len(path)))
	out = append(out, path...)
	return append(out, encodeRecord(cover)...), nil
}

func splitIndexValue(value []byte) (string, []byte, error) {
	if len(value) < 2 || value[0] != 1 {
		return "", nil, fmt.Errorf("invalid index value header")
	}
	length, n := binary.Uvarint(value[1:])
	if n <= 0 || length > uint64(len(value)-1-n) {
		return "", nil, fmt.Errorf("invalid index path length")
	}
	start, end := 1+n, 1+n+int(length)
	if len(value)-end < 25 {
		return "", nil, fmt.Errorf("truncated index record")
	}
	return string(value[start:end]), value[end:], nil
}

func decodeIndexValue(value []byte) (string, dsRecord, *datastorepb.Entity, error) {
	path, data, err := splitIndexValue(value)
	if err != nil {
		return "", dsRecord{}, nil, err
	}
	record, entity, err := decodeDS(data)
	return path, record, entity, err
}
