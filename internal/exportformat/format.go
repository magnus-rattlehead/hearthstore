package exportformat

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
)

const (
	exportVersion   = "3"
	entitiesPerFile = 5000
)

type Stats struct {
	Entities int64
	Bytes    int64
}

func WriteExport(ctx context.Context, parentDirectory string, started time.Time, visit func(func(*datastorepb.Entity) error) error) (metadataPath string, stats Stats, err error) {
	if visit == nil {
		return "", Stats{}, fmt.Errorf("export entity visitor is nil")
	}
	if err := os.MkdirAll(parentDirectory, 0o755); err != nil {
		return "", Stats{}, fmt.Errorf("creating export parent directory: %w", err)
	}
	exportName := fmt.Sprintf("datastore_export_%d", started.Unix())
	finalDirectory := filepath.Join(parentDirectory, exportName)
	if _, statErr := os.Stat(finalDirectory); statErr == nil {
		return "", Stats{}, fmt.Errorf("export directory already exists: %s", finalDirectory)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", Stats{}, fmt.Errorf("checking export directory: %w", statErr)
	}
	temporaryDirectory, err := os.MkdirTemp(parentDirectory, ".hearthstore-export-")
	if err != nil {
		return "", Stats{}, fmt.Errorf("creating temporary export directory: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(temporaryDirectory)
		}
	}()

	setDirectory := filepath.Join(temporaryDirectory, "all_namespaces", "all_kinds")
	if err := os.MkdirAll(setDirectory, 0o755); err != nil {
		return "", Stats{}, fmt.Errorf("creating entity-set directory: %w", err)
	}

	shards := &exportShards{directory: setDirectory}
	if err := shards.openNext(); err != nil {
		return "", Stats{}, err
	}
	visitErr := visit(func(entity *datastorepb.Entity) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		legacy, err := ToExportEntity(entity)
		if err != nil {
			return err
		}
		encoded, err := proto.Marshal(legacy)
		if err != nil {
			return fmt.Errorf("encoding export entity: %w", err)
		}
		if shards.currentCount == entitiesPerFile {
			if err := shards.openNext(); err != nil {
				return err
			}
		}
		if err := shards.writer.WriteRecord(encoded); err != nil {
			return err
		}
		shards.currentCount++
		stats.Entities++
		return nil
	})
	closeErr := shards.close()
	if visitErr != nil {
		return "", Stats{}, visitErr
	}
	if closeErr != nil {
		return "", Stats{}, closeErr
	}
	stats.Bytes = shards.bytes

	finished := time.Now().UTC()
	backup := &Backup{
		BackupInfo: &BackupInfo{BackupName: proto.String(exportName), StartTimestamp: proto.Int64(started.UnixMicro()), EndTimestamp: proto.Int64(finished.UnixMicro())},
		Kind:       []*KindBackupInfo{{Kind: proto.String(""), File: shards.names}},
	}
	backupBytes, err := proto.Marshal(backup)
	if err != nil {
		return "", Stats{}, fmt.Errorf("encoding entity-set metadata: %w", err)
	}
	setMetadataName := "all_namespaces_all_kinds.export_metadata"
	if err := os.WriteFile(filepath.Join(setDirectory, setMetadataName), backupBytes, 0o644); err != nil {
		return "", Stats{}, fmt.Errorf("writing entity-set metadata: %w", err)
	}

	allKinds := KindType_ALL_KINDS
	allNamespaces := NamespaceType_ALL_NAMESPACES
	relativeSetMetadata := filepath.ToSlash(filepath.Join("all_namespaces", "all_kinds", setMetadataName))
	overall := &OverallExportMetadata{EntitySet: []*SingleExportMetadataPointer{{
		EntitySetSpec:      &EntitySetSpec{KindType: &allKinds, NamespaceType: &allNamespaces},
		ExportMetadataFile: proto.String(relativeSetMetadata),
		NumEntities:        proto.Int64(stats.Entities),
		NumBytes:           proto.Int64(stats.Bytes),
	}}}
	overallBytes, err := proto.Marshal(overall)
	if err != nil {
		return "", Stats{}, fmt.Errorf("encoding overall export metadata: %w", err)
	}
	overallName := exportName + ".overall_export_metadata"
	overallFile, err := os.OpenFile(filepath.Join(temporaryDirectory, overallName), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return "", Stats{}, fmt.Errorf("creating overall export metadata: %w", err)
	}
	logWriter := NewLogWriter(overallFile)
	writeErr := logWriter.WriteRecord([]byte(exportVersion))
	if writeErr == nil {
		writeErr = logWriter.WriteRecord(overallBytes)
	}
	if closeFileErr := overallFile.Close(); writeErr == nil {
		writeErr = closeFileErr
	}
	if writeErr != nil {
		return "", Stats{}, fmt.Errorf("writing overall export metadata: %w", writeErr)
	}

	if err := os.Rename(temporaryDirectory, finalDirectory); err != nil {
		return "", Stats{}, fmt.Errorf("committing export directory: %w", err)
	}
	committed = true
	return filepath.Join(finalDirectory, overallName), stats, nil
}

type exportShards struct {
	directory    string
	file         *os.File
	writer       *LogWriter
	currentCount int
	names        []string
	bytes        int64
}

func (s *exportShards) openNext() error {
	if err := s.close(); err != nil {
		return err
	}
	name := fmt.Sprintf("output-%d", len(s.names))
	file, err := os.OpenFile(filepath.Join(s.directory, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("creating export shard %s: %w", name, err)
	}
	s.file = file
	s.writer = NewLogWriter(file)
	s.currentCount = 0
	s.names = append(s.names, name)
	return nil
}

func (s *exportShards) close() error {
	if s.file == nil {
		return nil
	}
	info, statErr := s.file.Stat()
	closeErr := s.file.Close()
	s.file = nil
	s.writer = nil
	if statErr != nil {
		return fmt.Errorf("reading export shard size: %w", statErr)
	}
	s.bytes += info.Size()
	if closeErr != nil {
		return fmt.Errorf("closing export shard: %w", closeErr)
	}
	return nil
}

// VisitExport reads every entity from a Datastore-compatible export.
func VisitExport(ctx context.Context, source, project, database string, visit func(*datastorepb.Entity) error) (Stats, error) {
	metadataPath, err := resolveOverallMetadata(source)
	if err != nil {
		return Stats{}, err
	}
	file, err := os.Open(metadataPath)
	if err != nil {
		return Stats{}, fmt.Errorf("opening overall export metadata: %w", err)
	}
	reader := NewLogReader(file)
	version, err := reader.ReadRecord()
	if err != nil {
		_ = file.Close()
		return Stats{}, fmt.Errorf("reading export version: %w", err)
	}
	if string(version) != exportVersion {
		_ = file.Close()
		return Stats{}, fmt.Errorf("unsupported Datastore export version %q", version)
	}
	payload, err := reader.ReadRecord()
	closeErr := file.Close()
	if err != nil {
		return Stats{}, fmt.Errorf("reading overall export metadata: %w", err)
	}
	if closeErr != nil {
		return Stats{}, fmt.Errorf("closing overall export metadata: %w", closeErr)
	}
	var overall OverallExportMetadata
	if err := proto.Unmarshal(payload, &overall); err != nil {
		return Stats{}, fmt.Errorf("decoding overall export metadata: %w", err)
	}

	root := filepath.Dir(metadataPath)
	var stats Stats
	for _, pointer := range overall.EntitySet {
		if err := ctx.Err(); err != nil {
			return Stats{}, err
		}
		setMetadataPath, err := safeExistingPath(root, pointer.GetExportMetadataFile())
		if err != nil {
			return Stats{}, fmt.Errorf("invalid entity-set metadata path: %w", err)
		}
		setBytes, err := os.ReadFile(setMetadataPath)
		if err != nil {
			return Stats{}, fmt.Errorf("reading entity-set metadata: %w", err)
		}
		var backup Backup
		if err := proto.Unmarshal(setBytes, &backup); err != nil {
			return Stats{}, fmt.Errorf("decoding entity-set metadata: %w", err)
		}
		var setStats Stats
		for _, kind := range backup.Kind {
			for _, shardName := range kind.File {
				shardPath, err := safeExistingPath(filepath.Dir(setMetadataPath), shardName)
				if err != nil {
					return Stats{}, fmt.Errorf("invalid export shard path: %w", err)
				}
				shardStats, err := visitShard(ctx, shardPath, project, database, visit)
				if err != nil {
					return Stats{}, err
				}
				setStats.Entities += shardStats.Entities
				setStats.Bytes += shardStats.Bytes
			}
		}
		if pointer.NumEntities != nil && pointer.GetNumEntities() != setStats.Entities {
			return Stats{}, fmt.Errorf("entity-set metadata declares %d entities but contains %d", pointer.GetNumEntities(), setStats.Entities)
		}
		if pointer.NumBytes != nil && pointer.GetNumBytes() != setStats.Bytes {
			return Stats{}, fmt.Errorf("entity-set metadata declares %d bytes but contains %d", pointer.GetNumBytes(), setStats.Bytes)
		}
		stats.Entities += setStats.Entities
		stats.Bytes += setStats.Bytes
	}
	return stats, nil
}

func visitShard(ctx context.Context, path, project, database string, visit func(*datastorepb.Entity) error) (Stats, error) {
	file, err := os.Open(path)
	if err != nil {
		return Stats{}, fmt.Errorf("opening export shard %s: %w", path, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return Stats{}, fmt.Errorf("reading export shard size: %w", err)
	}
	stats := Stats{Bytes: info.Size()}
	reader := NewLogReader(file)
	for {
		if err := ctx.Err(); err != nil {
			return Stats{}, err
		}
		payload, err := reader.ReadRecord()
		if errors.Is(err, io.EOF) {
			return stats, nil
		}
		if err != nil {
			return Stats{}, fmt.Errorf("reading export shard %s: %w", path, err)
		}
		var legacy EntityProto
		if err := proto.Unmarshal(payload, &legacy); err != nil {
			return Stats{}, fmt.Errorf("decoding entity in export shard %s: %w", path, err)
		}
		entity, err := FromExportEntity(&legacy, project, database)
		if err != nil {
			return Stats{}, fmt.Errorf("converting entity in export shard %s: %w", path, err)
		}
		if err := visit(entity); err != nil {
			return Stats{}, err
		}
		stats.Entities++
	}
}

func resolveOverallMetadata(source string) (string, error) {
	info, err := os.Stat(source)
	if err != nil {
		return "", fmt.Errorf("reading import source: %w", err)
	}
	if !info.IsDir() {
		if !strings.HasSuffix(source, ".overall_export_metadata") {
			return "", fmt.Errorf("import source is not an overall export metadata file: %s", source)
		}
		return source, nil
	}
	matches, err := filepath.Glob(filepath.Join(source, "*.overall_export_metadata"))
	if err != nil {
		return "", fmt.Errorf("searching import directory: %w", err)
	}
	if len(matches) == 0 {
		matches, err = filepath.Glob(filepath.Join(source, "*", "*.overall_export_metadata"))
		if err != nil {
			return "", fmt.Errorf("searching import directory: %w", err)
		}
	}
	sort.Strings(matches)
	if len(matches) != 1 {
		return "", fmt.Errorf("import directory must contain exactly one overall export metadata file; found %d", len(matches))
	}
	return matches[0], nil
}

func safeExistingPath(root, relative string) (string, error) {
	if relative == "" || !filepath.IsLocal(filepath.FromSlash(relative)) {
		return "", fmt.Errorf("path %q is not local", relative)
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	candidate := filepath.Join(root, filepath.FromSlash(relative))
	resolvedCandidate, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(resolvedRoot, resolvedCandidate)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q escapes export directory", relative)
	}
	return resolvedCandidate, nil
}
