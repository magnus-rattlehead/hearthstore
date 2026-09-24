package datastore

import (
	"encoding/binary"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

const defaultDatabase = "(default)"

// keyComponents extracts storage columns from a Datastore Key.
func keyComponents(key *datastorepb.Key) (project, database, namespace, kind, parentPath, path string) {
	pid := key.GetPartitionId()
	project = pid.GetProjectId()
	database = pid.GetDatabaseId()
	namespace = pid.GetNamespaceId()

	parts := key.GetPath()
	if len(parts) == 0 {
		return
	}

	path = keycodec.Path(parts)
	kind = parts[len(parts)-1].GetKind()
	parentPath = keycodec.Path(parts[:len(parts)-1])
	return
}

// keyString returns a canonical string for deduplication (used in Lookup).
func keyString(key *datastorepb.Key) string {
	return string(keycodec.Ordered(key))
}

// isIncompleteKey reports whether the last path element has no ID or name.
func isIncompleteKey(key *datastorepb.Key) bool {
	parts := key.GetPath()
	if len(parts) == 0 {
		return true
	}
	last := parts[len(parts)-1]
	switch last.GetIdType().(type) {
	case *datastorepb.Key_PathElement_Id:
		return last.GetId() == 0
	case *datastorepb.Key_PathElement_Name:
		return last.GetName() == ""
	}
	return true
}

// withID returns a copy of key with the last path element's ID set to id.
func withID(key *datastorepb.Key, id int64) *datastorepb.Key {
	parts := make([]*datastorepb.Key_PathElement, len(key.GetPath()))
	for i, p := range key.GetPath() {
		parts[i] = &datastorepb.Key_PathElement{Kind: p.GetKind(), IdType: p.GetIdType()}
	}
	last := parts[len(parts)-1]
	last.IdType = &datastorepb.Key_PathElement_Id{Id: id}
	return &datastorepb.Key{PartitionId: key.GetPartitionId(), Path: parts}
}

// encodeCursor encodes an entity path in the only supported cursor format.
func encodeCursor(path string) []byte {
	return encodeCursorFull(storage.CursorPayload{V: 4, P: path})
}

// decodeCursor decodes a cursor back to a path string.
func decodeCursor(cursor []byte) string {
	cp, ok := decodeCursorFull(cursor)
	if !ok {
		return ""
	}
	return cp.P
}

// encodeCursorFull writes a binary cursor; the protobuf REST adapter supplies base64.
func encodeCursorFull(cp storage.CursorPayload) []byte {
	out := []byte{'H', 'S', 4}
	out = binary.AppendVarint(out, cp.G)
	out = binary.AppendUvarint(out, uint64(cp.O))
	for _, value := range [][]byte{[]byte(cp.P), []byte(cp.I), cp.K, cp.H, []byte(cp.D), cp.B} {
		out = binary.AppendUvarint(out, uint64(len(value)))
		out = append(out, value...)
	}
	return out
}

// decodeCursorFull accepts only bounded v4 binary cursors.
func decodeCursorFull(data []byte) (storage.CursorPayload, bool) {
	var cp storage.CursorPayload
	if len(data) < 3 || len(data) > 64<<10 || string(data[:3]) != "HS\x04" {
		return cp, false
	}
	data = data[3:]
	generation, n := binary.Varint(data)
	if n <= 0 {
		return cp, false
	}
	data = data[n:]
	offset, n := binary.Uvarint(data)
	if n <= 0 || offset > uint64(^uint(0)>>1) {
		return cp, false
	}
	data = data[n:]
	var fields [6][]byte
	for i := range fields {
		length, n := binary.Uvarint(data)
		if n <= 0 || length > uint64(len(data)-n) {
			return cp, false
		}
		fields[i] = data[n : n+int(length)]
		data = data[n+int(length):]
	}
	if len(data) != 0 || len(fields[0]) == 0 {
		return cp, false
	}
	cp = storage.CursorPayload{V: 4, P: string(fields[0]), I: string(fields[1]), K: fields[2], H: fields[3], D: string(fields[4]), B: fields[5], O: int(offset), G: generation}
	return cp, true
}
