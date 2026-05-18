package server

import (
	"crypto/rand"
	"fmt"
	"strings"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// parseName parses "projects/{p}/databases/{d}/documents/{path}" into its components.
func parseName(name string) (project, database, path string, err error) {
	p, d, rest, e := parseResourcePrefix(name)
	if e != nil {
		return "", "", "", e
	}
	if rest == "" {
		return "", "", "", fmt.Errorf("missing document path in %q", name)
	}
	return p, d, rest, nil
}

// parseParent parses "projects/{p}/databases/{d}/documents[/{doc_path}]".
// Returns parentPath="" for root (when the name ends at /documents).
func parseParent(parent string) (project, database, parentPath string, err error) {
	return parseResourcePrefix(parent)
}

// parseDatabase parses "projects/{p}/databases/{d}".
func parseDatabase(db string) (project, database string, err error) {
	parts := strings.SplitN(db, "/databases/", 2)
	if len(parts) != 2 || !strings.HasPrefix(parts[0], "projects/") {
		return "", "", fmt.Errorf("invalid database name: %q", db)
	}
	return strings.TrimPrefix(parts[0], "projects/"), parts[1], nil
}

// buildDocName constructs the full Firestore document resource name.
func buildDocName(project, database, path string) string {
	return fmt.Sprintf("projects/%s/databases/%s/documents/%s", project, database, path)
}

// splitDocPath extracts (collection, parentPath, docID) from a document path.
//
//	"users/u1"        -> collection="users", parentPath="",    docID="u1"
//	"a/b/users/u1"   -> collection="users", parentPath="a/b", docID="u1"
func splitDocPath(path string) (collection, parentPath, docID string) {
	parts := strings.Split(path, "/")
	docID = parts[len(parts)-1]
	collection = parts[len(parts)-2]
	if len(parts) >= 4 {
		parentPath = strings.Join(parts[:len(parts)-2], "/")
	}
	return
}

// newAutoID generates a random 20-character Firestore-style document ID.
func newAutoID() string {
	const alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
	for i, v := range b {
		b[i] = alpha[int(v)%len(alpha)]
	}
	return string(b)
}

// applyFieldMask returns a copy of doc containing only the specified top-level fields.
func applyFieldMask(doc *firestorepb.Document, fields []string) *firestorepb.Document {
	keep := make(map[string]bool, len(fields))
	for _, f := range fields {
		keep[f] = true
	}
	out := &firestorepb.Document{
		Name:       doc.Name,
		CreateTime: doc.CreateTime,
		UpdateTime: doc.UpdateTime,
		Fields:     make(map[string]*firestorepb.Value, len(fields)),
	}
	for k, v := range doc.Fields {
		if keep[k] {
			out.Fields[k] = v
		}
	}
	return out
}

// parseResourcePrefix splits a Firestore resource name at the /documents boundary.
// Returns project, database, and the path segment after /documents (empty for root).
func parseResourcePrefix(name string) (project, database, rest string, err error) {
	const projPfx = "projects/"
	if !strings.HasPrefix(name, projPfx) {
		return "", "", "", fmt.Errorf("expected %q prefix in %q", projPfx, name)
	}
	after := name[len(projPfx):]

	dbSplit := strings.SplitN(after, "/databases/", 2)
	if len(dbSplit) != 2 {
		return "", "", "", fmt.Errorf("missing /databases/ in %q", name)
	}
	project = dbSplit[0]

	docSplit := strings.SplitN(dbSplit[1], "/documents", 2)
	if len(docSplit) != 2 {
		return "", "", "", fmt.Errorf("missing /documents in %q", name)
	}
	database = docSplit[0]
	rest = strings.TrimPrefix(docSplit[1], "/")
	return
}
