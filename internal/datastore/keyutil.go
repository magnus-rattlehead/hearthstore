package datastore

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
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

// pathToKey reconstructs a Key from storage path components.
func pathToKey(project, database, namespace, path string) *datastorepb.Key {
	if database == defaultDatabase {
		database = ""
	}
	parts := strings.Split(path, "/")
	var pathElems []*datastorepb.Key_PathElement
	for i := 0; i+1 < len(parts); i += 2 {
		kind := parts[i]
		idOrName := parts[i+1]
		pe := &datastorepb.Key_PathElement{Kind: kind}
		if n, err := strconv.ParseInt(idOrName, 10, 64); err == nil {
			pe.IdType = &datastorepb.Key_PathElement_Id{Id: n}
		} else {
			pe.IdType = &datastorepb.Key_PathElement_Name{Name: idOrName}
		}
		pathElems = append(pathElems, pe)
	}
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{
			ProjectId:   project,
			DatabaseId:  database,
			NamespaceId: namespace,
		},
		Path: pathElems,
	}
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

// encodeCursor encodes an entity path as an opaque cursor (base64).
func encodeCursor(path string) []byte {
	return []byte(base64.StdEncoding.EncodeToString([]byte(path)))
}

// decodeCursor decodes a cursor back to a path string.
func decodeCursor(cursor []byte) string {
	if len(cursor) == 0 {
		return ""
	}
	b, err := base64.StdEncoding.DecodeString(string(cursor))
	if err != nil {
		return ""
	}
	return string(b)
}
