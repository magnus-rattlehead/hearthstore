package datastore

import (
	"bytes"
	"encoding/hex"
	"strings"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/keycodec"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (g *GRPCServer) queryCursor(data, fingerprint []byte, project, database, namespace string, q, schema *datastorepb.Query, internal bool) (*storage.CursorPayload, error) {
	if len(data) == 0 {
		return nil, nil
	}
	invalid := func() (*storage.CursorPayload, error) {
		return nil, status.Error(codes.InvalidArgument, "invalid cursor or query scope")
	}
	c, ok := decodeCursorFull(data)
	// Aggregation has already validated the external cursor before padding its
	// logical tuple with non-value sentinels for the aggregate index schema.
	if internal && ok && bytes.Equal(c.H, fingerprint) && c.I == "fallback" && (strings.HasSuffix(string(c.K), "|~") || strings.HasSuffix(string(c.K), "|!")) {
		c.Before = strings.HasSuffix(string(c.K), "|!")
		return &c, nil
	}
	if !ok || c.G < 0 || c.O != 0 && len(projectionFields(q)) == 0 {
		return invalid()
	}
	reversed := false
	if !bytes.Equal(c.H, fingerprint) {
		if len(q.Order) == 0 || q.Order[len(q.Order)-1].Property.GetName() != "__key__" {
			return invalid()
		}
		reverse := proto.Clone(q).(*datastorepb.Query)
		for _, order := range reverse.Order {
			if order.Direction == datastorepb.PropertyOrder_DESCENDING {
				order.Direction = datastorepb.PropertyOrder_ASCENDING
			} else {
				order.Direction = datastorepb.PropertyOrder_DESCENDING
			}
		}
		if !bytes.Equal(c.H, queryFingerprint(project, database, namespace, reverse)) {
			return invalid()
		}
		reversed = true
	}
	q = schema
	path, err := keycodec.ParsePath(c.P)
	if err != nil || len(path) == 0 || len(q.Kind) > 0 && path[len(path)-1].Kind != q.Kind[0].Name {
		return invalid()
	}
	key := &datastorepb.Key{PartitionId: &datastorepb.PartitionId{ProjectId: project, DatabaseId: database, NamespaceId: namespace}, Path: path}
	components := strings.Split(string(c.B), "/")
	if len(components) != len(q.Order)+1 {
		return invalid()
	}
	for _, component := range components {
		if decoded, err := hex.DecodeString(component); err != nil || len(decoded) == 0 {
			return invalid()
		}
	}
	if components[len(components)-1] != hex.EncodeToString(keycodec.Ordered(key)) {
		return invalid()
	}
	logical := map[string][]byte{"__key__": append([]byte{7}, keycodec.Ordered(key)...)}
	for i, order := range q.Order {
		raw, err := hex.DecodeString(components[i])
		if err != nil {
			return invalid()
		}
		if (order.Direction == datastorepb.PropertyOrder_DESCENDING) != reversed {
			for j := range raw {
				raw[j] = ^raw[j]
			}
		}
		if !storage.ValidOrderedQueryValue(raw) {
			return invalid()
		}
		if order.Property.Name == "__key__" && !bytes.Equal(raw, logical["__key__"]) {
			return invalid()
		}
		// Physical dotted entries and selected logical witnesses can differ.
		// Validate structure independently; do not pretend this authenticates
		// arbitrary well-formed edits to an unsigned cursor.
		if !strings.Contains(order.Property.Name, ".") {
			logical[order.Property.Name] = raw
		}
	}
	switch c.I {
	case "fallback":
		parts := strings.Split(string(c.K), "|")
		if c.G != 0 || c.O != 0 || len(parts) != 3 || parts[0] != string(c.B) || parts[1] != c.P {
			return invalid()
		}
		if _, err := hex.DecodeString(parts[2]); err != nil {
			return invalid()
		}
	case "":
		if c.G != 0 || len(c.K) != 0 {
			return invalid()
		}
	default:
		ancestor := extractAncestorPath(q.Filter, project, database, namespace)
		if err := g.store.ValidateQueryCursor(c, key, ancestor, logical); err != nil {
			return nil, err
		}
	}
	if reversed {
		if len(components) < len(q.Order)+1 {
			return invalid()
		}
		for i := range q.Order {
			raw, _ := hex.DecodeString(components[i]) // Validated above.
			for j := range raw {
				raw[j] = ^raw[j]
			}
			components[i] = hex.EncodeToString(raw)
		}
		c.B = []byte(strings.Join(components, "/"))
		c.I, c.G, c.O, c.D = "fallback", 0, 0, ""
		c.K = []byte(string(c.B) + "|" + c.P + "|")
		// CursorModernizer.computeBefore XORs the direction with the stored
		// beforeAscending bit. This is request-local, not a new wire format.
		c.Before = true
	}
	return &c, nil
}
