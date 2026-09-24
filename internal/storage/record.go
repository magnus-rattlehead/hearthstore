package storage

import (
	"encoding/binary"
	"fmt"
)

// encodeRecord stores the protobuf payload directly, without JSON/base64 expansion.
func encodeRecord(record dsRecord) []byte {
	out := make([]byte, 1, len(record.Data)+len(record.Kind)+len(record.ParentPath)+64)
	out[0] = 3
	if record.Deleted {
		out[0] |= 0x80
	}
	for _, value := range []int64{record.Version, record.Created, record.Updated} {
		out = binary.BigEndian.AppendUint64(out, uint64(value))
	}
	for _, value := range [][]byte{[]byte(record.Kind), []byte(record.ParentPath), record.Data} {
		out = binary.AppendUvarint(out, uint64(len(value)))
		out = append(out, value...)
	}
	return out
}

func decodeRecord(data []byte) (dsRecord, error) {
	var record dsRecord
	if len(data) < 25 || data[0]&0x7f != 3 {
		return record, fmt.Errorf("invalid storage record header")
	}
	record.Deleted = data[0]&0x80 != 0
	record.Version = int64(binary.BigEndian.Uint64(data[1:9]))
	record.Created = int64(binary.BigEndian.Uint64(data[9:17]))
	record.Updated = int64(binary.BigEndian.Uint64(data[17:25]))
	data = data[25:]
	var fields [3][]byte
	for i := range fields {
		length, n := binary.Uvarint(data)
		if n <= 0 || length > uint64(len(data)-n) {
			return record, fmt.Errorf("invalid storage record field %d", i)
		}
		fields[i] = data[n : n+int(length)]
		data = data[n+int(length):]
	}
	if len(data) != 0 {
		return record, fmt.Errorf("unexpected storage record trailing bytes")
	}
	record.Kind, record.ParentPath, record.Data = string(fields[0]), string(fields[1]), fields[2]
	return record, nil
}
