package datastore

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

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

	segments := make([]string, 0, len(parts)*2)
	for _, p := range parts {
		segments = append(segments, p.GetKind())
		switch id := p.GetIdType().(type) {
		case *datastorepb.Key_PathElement_Id:
			segments = append(segments, strconv.FormatInt(id.Id, 10))
		case *datastorepb.Key_PathElement_Name:
			segments = append(segments, id.Name)
		default:
			segments = append(segments, "") // incomplete key
		}
	}

	path = strings.Join(segments, "/")
	last := parts[len(parts)-1]
	kind = last.GetKind()

	if len(segments) >= 4 {
		parentPath = strings.Join(segments[:len(segments)-2], "/")
	}
	return
}


// keyString returns a canonical string for deduplication (used in Lookup).
func keyString(key *datastorepb.Key) string {
	_, _, namespace, _, _, path := keyComponents(key)
	pid := key.GetPartitionId()
	return fmt.Sprintf("%s|%s|%s|%s", pid.GetProjectId(), pid.GetDatabaseId(), namespace, path)
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

// encodeCursor encodes an entity path as an opaque cursor (URL-safe base64).
// Clients (e.g. the Dialpad ds library) validate cursors with urlsafe_b64decode,
// so URL-safe encoding is required for correct round-trip handling.
func encodeCursor(path string) []byte {
	return []byte(base64.URLEncoding.EncodeToString([]byte(path)))
}

// decodeCursor decodes a cursor back to a path string.
func decodeCursor(cursor []byte) string {
	if len(cursor) == 0 {
		return ""
	}
	b, err := base64.URLEncoding.DecodeString(string(cursor))
	if err != nil {
		// Fall back to standard encoding for cursors issued before this change.
		b, err = base64.StdEncoding.DecodeString(string(cursor))
		if err != nil {
			return ""
		}
	}
	return string(b)
}

// encodeCursorFull encodes a CursorPayload as JSON then URL-safe base64.
func encodeCursorFull(cp storage.CursorPayload) []byte {
	data, _ := json.Marshal(cp)
	return []byte(base64.URLEncoding.EncodeToString(data))
}

// decodeCursorFull decodes a cursor byte slice to a CursorPayload.
// New-format cursors are JSON objects; old-format cursors are plain paths.
// Returns (zero, false) if the cursor cannot be decoded at all.
func decodeCursorFull(b []byte) (storage.CursorPayload, bool) {
	if len(b) == 0 {
		return storage.CursorPayload{}, false
	}
	decoded, err := base64.URLEncoding.DecodeString(string(b))
	if err != nil {
		decoded, err = base64.StdEncoding.DecodeString(string(b))
		if err != nil {
			return storage.CursorPayload{}, false
		}
	}
	// Try JSON (new-format cursor).
	var cp storage.CursorPayload
	if jsonErr := json.Unmarshal(decoded, &cp); jsonErr == nil && cp.P != "" {
		return cp, true
	}
	// The REST/JSON transport double-encodes cursors: pjsonMarshal serialises the
	// EndCursor []byte field as StdB64, so clients receive StdB64(URLSafeB64(JSON)).
	// After one decode above we hold URLSafeB64(JSON); one more URL-safe decode reaches
	// the JSON payload. This depth is always exactly 2 - cursors are regenerated fresh
	// from entity data each page, so layers never accumulate across pages.
	if decoded2, err2 := base64.URLEncoding.DecodeString(string(decoded)); err2 == nil {
		if jsonErr := json.Unmarshal(decoded2, &cp); jsonErr == nil && cp.P != "" {
			return cp, true
		}
		// Plain path after double decode (REST plain-path cursor).
		return storage.CursorPayload{P: string(decoded2)}, true
	}
	// Fall back: treat decoded bytes as a plain path (old-format cursor).
	return storage.CursorPayload{P: string(decoded)}, true
}

// buildCursor constructs the per-entity cursor for keyset pagination.
// When sorts is non-empty it encodes a CursorPayload with sort field values;
// otherwise it falls back to the plain-path cursor.
func buildCursor(path string, sorts []storage.DsSortSpec, row *storage.DsEntityRow) []byte {
	if len(sorts) == 0 {
		return encodeCursor(path)
	}
	kvs := make([]storage.CursorSortKV, 0, len(sorts))
	for _, sp := range sorts {
		if sp.Col == "__path__" {
			// __key__ sort: path is the sort value, no field index involved.
			kvs = append(kvs, storage.CursorSortKV{Col: "__path__", V: path})
			continue
		}
		v := getProp(row.Entity, sp.FieldPath)
		kv := storage.CursorSortKV{Col: sp.Col}
		if v == nil {
			kv.Null = true
		} else if _, isNull := v.ValueType.(*datastorepb.Value_NullValue); isNull {
			kv.Null = true
		} else {
			kv.V = serializeSortValue(v)
		}
		kvs = append(kvs, kv)
	}
	return encodeCursorFull(storage.CursorPayload{P: path, S: kvs})
}

// serializeSortValue converts a Datastore property value to a string
// for storage in a CursorSortKV entry.
func serializeSortValue(v *datastorepb.Value) string {
	if v == nil {
		return ""
	}
	_, sqlVal, ok := dsValueColumn(v)
	if !ok {
		return ""
	}
	switch sv := sqlVal.(type) {
	case string:
		return sv
	case int64:
		return strconv.FormatInt(sv, 10)
	case float64:
		return strconv.FormatFloat(sv, 'f', -1, 64)
	default:
		return ""
	}
}
