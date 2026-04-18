package datastore

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"

	adminpb "cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

// AdminServer implements adminpb.DatastoreAdminServer.
// Only ListIndexes and GetIndex are implemented; all other methods return Unimplemented.
type AdminServer struct {
	adminpb.UnimplementedDatastoreAdminServer
	indexes []*adminpb.Index
}

// NewAdminServer creates an AdminServer. If indexConfigPath is non-empty, it
// loads and parses an index.yaml file. If the path is empty or the file does
// not exist, the server starts with zero indexes (tests that don't require
// index pre-configuration will still work).
func NewAdminServer(indexConfigPath string) (*AdminServer, error) {
	srv := &AdminServer{}
	if indexConfigPath == "" {
		return srv, nil
	}
	data, err := os.ReadFile(indexConfigPath)
	if err != nil {
		if os.IsNotExist(err) {
			return srv, nil
		}
		return nil, fmt.Errorf("read index config: %w", err)
	}
	if err := srv.parseIndexYAML(data); err != nil {
		return nil, fmt.Errorf("parse index config: %w", err)
	}
	return srv, nil
}

// indexYAML is the on-disk format used by index.yaml.
type indexYAML struct {
	Indexes []indexYAMLEntry `yaml:"indexes"`
}

type indexYAMLEntry struct {
	Kind       string              `yaml:"kind"`
	Ancestor   string              `yaml:"ancestor"`
	Properties []indexYAMLProperty `yaml:"properties"`
}

type indexYAMLProperty struct {
	Name      string `yaml:"name"`
	Direction string `yaml:"direction"`
}

func (s *AdminServer) parseIndexYAML(data []byte) error {
	var cfg indexYAML
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return err
	}
	for _, entry := range cfg.Indexes {
		idx := &adminpb.Index{
			IndexId: indexID(entry),
			Kind:    entry.Kind,
			State:   adminpb.Index_READY,
		}

		switch entry.Ancestor {
		case "yes", "true", "1", "all":
			idx.Ancestor = adminpb.Index_ALL_ANCESTORS
		default:
			idx.Ancestor = adminpb.Index_NONE
		}

		for _, p := range entry.Properties {
			dir := adminpb.Index_ASCENDING
			switch p.Direction {
			case "desc", "DESCENDING":
				dir = adminpb.Index_DESCENDING
			}
			idx.Properties = append(idx.Properties, &adminpb.Index_IndexedProperty{
				Name:      p.Name,
				Direction: dir,
			})
		}
		s.indexes = append(s.indexes, idx)
	}
	return nil
}

// indexID derives a stable, short ID from the index definition so that
// GetIndex (which receives an ID from ListIndexes) always resolves correctly.
func indexID(entry indexYAMLEntry) string {
	h := md5.New()
	fmt.Fprintf(h, "%s\x00%s", entry.Kind, entry.Ancestor)
	for _, p := range entry.Properties {
		fmt.Fprintf(h, "\x00%s\x00%s", p.Name, p.Direction)
	}
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}

// ListIndexes returns all known indexes.
func (s *AdminServer) ListIndexes(_ context.Context, req *adminpb.ListIndexesRequest) (*adminpb.ListIndexesResponse, error) {
	return &adminpb.ListIndexesResponse{Indexes: s.indexes}, nil
}

// GetIndex returns a single index by ID.
func (s *AdminServer) GetIndex(_ context.Context, req *adminpb.GetIndexRequest) (*adminpb.Index, error) {
	for _, idx := range s.indexes {
		if idx.IndexId == req.IndexId {
			return idx, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "index %q not found", req.IndexId)
}
