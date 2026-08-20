package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
)

var compositeBuilds sync.Map

const (
	DsIndexCreating = "CREATING"
	DsIndexReady    = "READY"
	DsIndexDeleting = "DELETING"
	DsIndexError    = "ERROR"
)

// DsIndexProperty is one ordered property in a Datastore composite index.
type DsIndexProperty struct {
	Name string `json:"name" yaml:"name"`
	Desc bool   `json:"desc" yaml:"-"`
}

// DsCompositeIndex describes a persisted Datastore composite index.
type DsCompositeIndex struct {
	Project            string            `json:"project"`
	ID                 string            `json:"id"`
	Kind               string            `json:"kind"`
	Ancestor           bool              `json:"ancestor"`
	Properties         []DsIndexProperty `json:"properties"`
	State              string            `json:"state"`
	Source             string            `json:"source"`
	ActiveGeneration   int64             `json:"active_generation"`
	BuildingGeneration int64             `json:"building_generation"`
	ProcessedEntities  int64             `json:"processed_entities"`
	TotalEntities      int64             `json:"total_entities"`
	Error              string            `json:"error,omitempty"`
}

// DsCompositeIndexID returns a stable ID for an index definition.
func DsCompositeIndexID(kind string, ancestor bool, properties []DsIndexProperty) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%t", kind, ancestor)
	for _, p := range properties {
		fmt.Fprintf(h, "\x00%s\x00%t", p.Name, p.Desc)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// EnsureDsCompositeIndex persists a definition and optionally builds it before returning.
func (s *Store) EnsureDsCompositeIndex(ctx context.Context, idx DsCompositeIndex, wait bool) (DsCompositeIndex, bool, error) {
	if idx.ID == "" {
		idx.ID = DsCompositeIndexID(idx.Kind, idx.Ancestor, idx.Properties)
	}
	if idx.Source == "" {
		idx.Source = "generated"
	}
	properties, err := json.Marshal(idx.Properties)
	if err != nil {
		return DsCompositeIndex{}, false, fmt.Errorf("marshal composite index: %w", err)
	}
	now := time.Now().UTC().Format(timeLayout)
	created := false
	err = s.RunInTxCtx(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(`INSERT OR IGNORE INTO ds_composite_indexes
			(project,index_id,kind,ancestor,properties,state,source,building_generation,created_at,updated_at)
			VALUES (?,?,?,?,?,?,?,1,?,?)`, idx.Project, idx.ID, idx.Kind, boolInt(idx.Ancestor), properties,
			DsIndexCreating, idx.Source, now, now)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		created = n > 0
		return nil
	})
	if err != nil {
		return DsCompositeIndex{}, false, err
	}
	current, err := s.GetDsCompositeIndex(idx.Project, idx.ID)
	if err != nil {
		return current, created, err
	}
	if current.State != DsIndexCreating {
		return current, created, nil
	}
	key := fmt.Sprintf("%p/%s/%s", s, idx.Project, idx.ID)
	if _, running := compositeBuilds.LoadOrStore(key, struct{}{}); !running {
		build := func(buildCtx context.Context) error {
			defer compositeBuilds.Delete(key)
			return s.BuildDsCompositeIndex(buildCtx, idx.Project, idx.ID)
		}
		if wait {
			err = build(ctx)
		} else {
			go func() { _ = build(context.Background()) }()
		}
	} else if wait {
		for current.State == DsIndexCreating {
			select {
			case <-ctx.Done():
				return current, created, ctx.Err()
			case <-time.After(25 * time.Millisecond):
			}
			current, err = s.GetDsCompositeIndex(idx.Project, idx.ID)
			if err != nil {
				return current, created, err
			}
		}
	}
	current, getErr := s.GetDsCompositeIndex(idx.Project, idx.ID)
	if err != nil {
		return current, true, err
	}
	return current, true, getErr
}

// ListDsCompositeIndexes returns project indexes, optionally across all projects.
func (s *Store) ListDsCompositeIndexes(project string) ([]DsCompositeIndex, error) {
	q := `SELECT project,index_id,kind,ancestor,properties,state,source,active_generation,
		building_generation,processed_entities,total_entities,error FROM ds_composite_indexes`
	var args []any
	if project != "" {
		q += ` WHERE project=?`
		args = append(args, project)
	}
	q += ` ORDER BY project,kind,index_id`
	rows, err := s.rdb.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DsCompositeIndex
	for rows.Next() {
		idx, err := scanCompositeIndex(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, idx)
	}
	return out, rows.Err()
}

// ListDsProjects returns projects currently represented in Datastore storage.
func (s *Store) ListDsProjects() ([]string, error) {
	rows, err := s.rdb.Query(`SELECT DISTINCT project FROM ds_documents ORDER BY project`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var projects []string
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			return nil, err
		}
		projects = append(projects, project)
	}
	return projects, rows.Err()
}

// GetDsCompositeIndex returns one project index.
func (s *Store) GetDsCompositeIndex(project, id string) (DsCompositeIndex, error) {
	row := s.rdb.QueryRow(`SELECT project,index_id,kind,ancestor,properties,state,source,active_generation,
		building_generation,processed_entities,total_entities,error
		FROM ds_composite_indexes WHERE project=? AND index_id=?`, project, id)
	idx, err := scanCompositeIndex(row)
	if err == sql.ErrNoRows {
		return DsCompositeIndex{}, sql.ErrNoRows
	}
	return idx, err
}

type rowScanner interface{ Scan(...any) error }

func scanCompositeIndex(row rowScanner) (DsCompositeIndex, error) {
	var idx DsCompositeIndex
	var ancestor int
	var props []byte
	err := row.Scan(&idx.Project, &idx.ID, &idx.Kind, &ancestor, &props, &idx.State, &idx.Source,
		&idx.ActiveGeneration, &idx.BuildingGeneration, &idx.ProcessedEntities, &idx.TotalEntities, &idx.Error)
	if err != nil {
		return idx, err
	}
	idx.Ancestor = ancestor != 0
	if err := json.Unmarshal(props, &idx.Properties); err != nil {
		return idx, fmt.Errorf("decode composite index %s: %w", idx.ID, err)
	}
	return idx, nil
}

// BuildDsCompositeIndex backfills one generation in bounded, restart-safe batches.
func (s *Store) BuildDsCompositeIndex(ctx context.Context, project, id string) error {
	idx, err := s.GetDsCompositeIndex(project, id)
	if err != nil {
		return err
	}
	if idx.State == DsIndexReady {
		return nil
	}
	var total int64
	if err := s.rdb.QueryRow(`SELECT COUNT(*) FROM ds_documents WHERE project=? AND kind=? AND deleted=0`, project, idx.Kind).Scan(&total); err != nil {
		return err
	}
	if err := s.RunInTxCtx(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE ds_composite_indexes SET state=?,total_entities=?,error='',updated_at=?
			WHERE project=? AND index_id=?`, DsIndexCreating, total, time.Now().UTC().Format(timeLayout), project, id)
		return err
	}); err != nil {
		return err
	}
	slog.Info("building Datastore composite index", "project", project, "index", id, "kind", idx.Kind, "entities", total)
	started := time.Now()
	const batchSize = 200
	var lastRowID, processed int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return context.Canceled
		default:
		}
		rows, err := s.rdb.Query(`SELECT rowid FROM ds_documents
			WHERE project=? AND kind=? AND deleted=0 AND rowid>? ORDER BY rowid LIMIT ?`, project, idx.Kind, lastRowID, batchSize)
		if err != nil {
			return s.failCompositeBuild(project, id, err)
		}
		var ids []int64
		for rows.Next() {
			var rowID int64
			if err := rows.Scan(&rowID); err != nil {
				rows.Close()
				return s.failCompositeBuild(project, id, err)
			}
			ids = append(ids, rowID)
			lastRowID = rowID
		}
		rows.Close()
		if len(ids) == 0 {
			break
		}
		err = s.RunInTxCtx(ctx, func(tx *sql.Tx) error {
			for _, rowID := range ids {
				var database, namespace, path, kind, parentPath string
				var data []byte
				err := tx.QueryRow(`SELECT database,namespace,path,kind,parent_path,data FROM ds_documents
					WHERE rowid=? AND project=? AND deleted=0`, rowID, project).Scan(&database, &namespace, &path, &kind, &parentPath, &data)
				if err == sql.ErrNoRows {
					continue
				}
				if err != nil {
					return err
				}
				var entity datastorepb.Entity
				if err := proto.Unmarshal(data, &entity); err != nil {
					return err
				}
				if err := dsReplaceOneCompositeIndex(tx, idx, database, namespace, path, parentPath, &entity, idx.BuildingGeneration); err != nil {
					return err
				}
			}
			processed += int64(len(ids))
			_, err := tx.Exec(`UPDATE ds_composite_indexes SET processed_entities=?,updated_at=? WHERE project=? AND index_id=?`,
				processed, time.Now().UTC().Format(timeLayout), project, id)
			return err
		})
		if err != nil {
			return s.failCompositeBuild(project, id, err)
		}
		if processed%2000 == 0 || processed >= total {
			slog.Info("Datastore composite index progress", "project", project, "index", id, "processed", processed, "total", total, "elapsed", time.Since(started).Round(time.Millisecond))
		}
	}
	err = s.RunInTxCtx(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE ds_composite_indexes SET state=?,active_generation=building_generation,
			processed_entities=?,updated_at=? WHERE project=? AND index_id=?`, DsIndexReady, processed,
			time.Now().UTC().Format(timeLayout), project, id)
		return err
	})
	if err != nil {
		return s.failCompositeBuild(project, id, err)
	}
	slog.Info("Datastore composite index ready", "project", project, "index", id, "kind", idx.Kind, "entities", processed, "elapsed", time.Since(started).Round(time.Millisecond))
	return nil
}

func (s *Store) failCompositeBuild(project, id string, cause error) error {
	_ = s.RunInTx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE ds_composite_indexes SET state=?,error=?,updated_at=? WHERE project=? AND index_id=?`,
			DsIndexError, cause.Error(), time.Now().UTC().Format(timeLayout), project, id)
		return err
	})
	slog.Error("Datastore composite index build failed", "project", project, "index", id, "error", cause)
	return cause
}

// DeleteDsCompositeIndex marks and removes an index and all physical entries.
func (s *Store) DeleteDsCompositeIndex(ctx context.Context, project, id string) error {
	return s.RunInTxCtx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`UPDATE ds_composite_indexes SET state=?,updated_at=? WHERE project=? AND index_id=?`,
			DsIndexDeleting, time.Now().UTC().Format(timeLayout), project, id); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM ds_composite_index_entries WHERE project=? AND index_id=?`, project, id); err != nil {
			return err
		}
		res, err := tx.Exec(`DELETE FROM ds_composite_indexes WHERE project=? AND index_id=?`, project, id)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return sql.ErrNoRows
		}
		return nil
	})
}

// DsQueryComposite scans a ready composite index and returns entities in index order.
func (s *Store) DsQueryComposite(project, database, namespace, id, ancestorPath string, prefix []byte, cursor *CursorPayload, limit int) ([]*DsEntityRow, int64, error) {
	idx, err := s.GetDsCompositeIndex(project, id)
	if err != nil || idx.State != DsIndexReady {
		return nil, 0, err
	}
	q := `SELECT index_key,doc_path FROM ds_composite_index_entries WHERE project=? AND database=? AND namespace=? AND index_id=? AND generation=? AND ancestor_path=?`
	args := []any{project, database, namespace, id, idx.ActiveGeneration, ancestorPath}
	if cursor != nil {
		if cursor.I != id || cursor.G != idx.ActiveGeneration || len(cursor.K) == 0 {
			return nil, 0, fmt.Errorf("invalid composite cursor")
		}
		q += ` AND (index_key>? OR (index_key=? AND doc_path>?))`
		args = append(args, cursor.K, cursor.K, cursor.P)
	}
	if len(prefix) > 0 {
		q += ` AND index_key>=?`
		args = append(args, prefix)
		if upper := prefixUpperBound(prefix); upper != nil {
			q += ` AND index_key<?`
			args = append(args, upper)
		}
	}
	q += ` ORDER BY index_key,doc_path`
	rows, err := s.rdb.Query(q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	seen := map[string]struct{}{}
	pathKeys := map[string][]byte{}
	var paths []string
	var scanned int64
	for rows.Next() {
		var key []byte
		var path string
		if err := rows.Scan(&key, &path); err != nil {
			return nil, scanned, err
		}
		scanned++
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
		pathKeys[path] = append([]byte(nil), key...)
		if limit > 0 && len(paths) >= limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, scanned, err
	}
	found, _, err := s.DsGetManyWithTimes(project, database, namespace, paths)
	if err != nil {
		return nil, scanned, err
	}
	byPath := make(map[string]*DsEntityRow, len(found))
	for _, row := range found {
		byPath[row.Path] = row
	}
	out := make([]*DsEntityRow, 0, len(paths))
	for _, path := range paths {
		if row := byPath[path]; row != nil {
			row.IndexKey = pathKeys[path]
			out = append(out, row)
		}
	}
	return out, scanned, nil
}

// DsCompositePrefix encodes an equality prefix in definition order.
func DsCompositePrefix(idx DsCompositeIndex, values map[string]*datastorepb.Value) []byte {
	var out []byte
	for _, p := range idx.Properties {
		v, ok := values[p.Name]
		if !ok {
			break
		}
		component, ok := encodeIndexValue(v)
		if !ok {
			break
		}
		if p.Desc {
			invert(component)
		}
		out = append(out, component...)
	}
	return out
}

func dsMaintainCompositeIndexes(exec dbExec, project, database, namespace, kind, path, parentPath string, entity *datastorepb.Entity) error {
	if _, err := exec.Exec(`DELETE FROM ds_composite_index_entries WHERE project=? AND database=? AND namespace=? AND doc_path=?`, project, database, namespace, path); err != nil {
		return fmt.Errorf("composite index cleanup: %w", err)
	}
	if entity == nil {
		return nil
	}
	rows, err := exec.Query(`SELECT project,index_id,kind,ancestor,properties,state,source,active_generation,
		building_generation,processed_entities,total_entities,error FROM ds_composite_indexes
		WHERE project=? AND kind=? AND state IN (?,?)`, project, kind, DsIndexReady, DsIndexCreating)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		idx, err := scanCompositeIndex(rows)
		if err != nil {
			return err
		}
		generation := idx.ActiveGeneration
		if idx.State == DsIndexCreating {
			generation = idx.BuildingGeneration
		}
		if err := dsReplaceOneCompositeIndex(exec, idx, database, namespace, path, parentPath, entity, generation); err != nil {
			return err
		}
	}
	return rows.Err()
}

func dsReplaceOneCompositeIndex(exec dbExec, idx DsCompositeIndex, database, namespace, path, parentPath string, entity *datastorepb.Entity, generation int64) error {
	if _, err := exec.Exec(`DELETE FROM ds_composite_index_entries WHERE project=? AND database=? AND namespace=? AND index_id=? AND generation=? AND doc_path=?`,
		idx.Project, database, namespace, idx.ID, generation, path); err != nil {
		return err
	}
	keys, err := compositeKeys(idx, entity)
	if err != nil {
		return err
	}
	ancestors := []string{""}
	if idx.Ancestor {
		ancestors = entityAncestors(path)
		if len(ancestors) == 0 && parentPath != "" {
			ancestors = []string{parentPath}
		}
	}
	if len(keys)*len(ancestors) > 20000 {
		return fmt.Errorf("index %s would create more than 20000 entries for entity %s", idx.ID, path)
	}
	var totalBytes int
	for _, key := range keys {
		totalBytes += len(key) * len(ancestors)
	}
	if totalBytes > 2*1024*1024 {
		return fmt.Errorf("index %s entries exceed 2 MiB for entity %s", idx.ID, path)
	}
	for _, ancestor := range ancestors {
		for _, key := range keys {
			if _, err := exec.Exec(`INSERT OR IGNORE INTO ds_composite_index_entries
				(project,database,namespace,index_id,generation,ancestor_path,index_key,doc_path) VALUES (?,?,?,?,?,?,?,?)`,
				idx.Project, database, namespace, idx.ID, generation, ancestor, key, path); err != nil {
				return err
			}
		}
	}
	return nil
}

func compositeKeys(idx DsCompositeIndex, entity *datastorepb.Entity) ([][]byte, error) {
	parts := make([][][]byte, 0, len(idx.Properties)+1)
	for _, property := range idx.Properties {
		values := propertyValues(entity, property.Name)
		if len(values) == 0 {
			return nil, nil
		}
		var encoded [][]byte
		seen := map[string]struct{}{}
		for _, value := range values {
			part, ok := encodeIndexValue(value)
			if !ok {
				continue
			}
			if property.Desc {
				invert(part)
			}
			if _, exists := seen[string(part)]; exists {
				continue
			}
			seen[string(part)] = struct{}{}
			encoded = append(encoded, part)
		}
		if len(encoded) == 0 {
			return nil, nil
		}
		parts = append(parts, encoded)
	}
	keyPart, ok := encodeIndexValue(&datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: entity.Key}})
	if !ok {
		return nil, nil
	}
	if len(idx.Properties) > 0 && idx.Properties[len(idx.Properties)-1].Desc {
		invert(keyPart)
	}
	parts = append(parts, [][]byte{keyPart})
	keys := [][]byte{{}}
	for _, choices := range parts {
		if len(keys)*len(choices) > 20000 {
			return nil, fmt.Errorf("index %s has an exploding array", idx.ID)
		}
		next := make([][]byte, 0, len(keys)*len(choices))
		for _, prefix := range keys {
			for _, choice := range choices {
				next = append(next, append(append([]byte{}, prefix...), choice...))
			}
		}
		keys = next
	}
	return keys, nil
}

func propertyValues(entity *datastorepb.Entity, name string) []*datastorepb.Value {
	if name == "__key__" {
		return []*datastorepb.Value{{ValueType: &datastorepb.Value_KeyValue{KeyValue: entity.Key}}}
	}
	parts := strings.Split(name, ".")
	var walk func(*datastorepb.Value, int) []*datastorepb.Value
	walk = func(v *datastorepb.Value, pos int) []*datastorepb.Value {
		if v == nil || v.GetExcludeFromIndexes() {
			return nil
		}
		if array := v.GetArrayValue(); array != nil {
			var out []*datastorepb.Value
			for _, child := range array.Values {
				out = append(out, walk(child, pos)...)
			}
			return out
		}
		if pos == len(parts) {
			return []*datastorepb.Value{v}
		}
		if embedded := v.GetEntityValue(); embedded != nil {
			return walk(embedded.Properties[parts[pos]], pos+1)
		}
		return nil
	}
	return walk(entity.Properties[parts[0]], 1)
}

func encodeIndexValue(v *datastorepb.Value) ([]byte, bool) {
	if v == nil || v.GetExcludeFromIndexes() {
		return nil, false
	}
	var raw []byte
	var tag byte
	switch x := v.GetValueType().(type) {
	case *datastorepb.Value_NullValue:
		tag = 0x10
	case *datastorepb.Value_IntegerValue:
		tag = 0x20
		raw = make([]byte, 8)
		binary.BigEndian.PutUint64(raw, uint64(x.IntegerValue)^1<<63)
	case *datastorepb.Value_TimestampValue:
		tag = 0x21
		raw = make([]byte, 12)
		binary.BigEndian.PutUint64(raw, uint64(x.TimestampValue.Seconds)^1<<63)
		binary.BigEndian.PutUint32(raw[8:], uint32(x.TimestampValue.Nanos))
	case *datastorepb.Value_BooleanValue:
		tag = 0x30
		if x.BooleanValue {
			raw = []byte{1}
		} else {
			raw = []byte{0}
		}
	case *datastorepb.Value_BlobValue:
		tag = 0x40
		raw = x.BlobValue
	case *datastorepb.Value_StringValue:
		tag = 0x50
		raw = []byte(x.StringValue)
	case *datastorepb.Value_DoubleValue:
		tag = 0x60
		bits := math.Float64bits(x.DoubleValue)
		if math.IsNaN(x.DoubleValue) {
			bits = 0
		} else if bits&(1<<63) != 0 {
			bits = ^bits
		} else {
			bits ^= 1 << 63
		}
		raw = make([]byte, 8)
		binary.BigEndian.PutUint64(raw, bits)
	case *datastorepb.Value_GeoPointValue:
		if x.GeoPointValue == nil {
			return nil, false
		}
		tag = 0x70
		raw = append(sortableFloat(x.GeoPointValue.Latitude), sortableFloat(x.GeoPointValue.Longitude)...)
	case *datastorepb.Value_KeyValue:
		if x.KeyValue == nil {
			return nil, false
		}
		tag = 0x80
		raw = encodeDatastoreKey(x.KeyValue)
	case *datastorepb.Value_EntityValue:
		tag = 0x90
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(x.EntityValue)
		if err != nil {
			return nil, false
		}
		raw = b
	default:
		return nil, false
	}
	out := []byte{tag}
	for _, b := range raw {
		if b == 0 {
			out = append(out, 0, 0xff)
		} else {
			out = append(out, b)
		}
	}
	return append(out, 0, 0), true
}

func sortableFloat(f float64) []byte {
	bits := math.Float64bits(f)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, bits)
	return b
}

func encodeDatastoreKey(key *datastorepb.Key) []byte {
	var out []byte
	for _, p := range key.Path {
		out = appendEscaped(out, []byte(p.Kind))
		switch id := p.IdType.(type) {
		case *datastorepb.Key_PathElement_Id:
			out = append(out, 1)
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(id.Id)^1<<63)
			out = append(out, b...)
		case *datastorepb.Key_PathElement_Name:
			out = append(out, 2)
			out = appendEscaped(out, []byte(id.Name))
		default:
			out = append(out, 0)
		}
	}
	return out
}

func appendEscaped(dst, src []byte) []byte {
	for _, b := range src {
		if b == 0 {
			dst = append(dst, 0, 0xff)
		} else {
			dst = append(dst, b)
		}
	}
	return append(dst, 0, 0)
}
func invert(b []byte) {
	for i := range b {
		b[i] = ^b[i]
	}
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
func prefixUpperBound(prefix []byte) []byte {
	out := append([]byte{}, prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
func entityAncestors(path string) []string {
	parts := strings.Split(path, "/")
	var out []string
	for i := 2; i <= len(parts); i += 2 {
		out = append(out, strings.Join(parts[:i], "/"))
	}
	return out
}

// SortDsCompositeIndexes stabilizes generated configuration output.
func SortDsCompositeIndexes(indexes []DsCompositeIndex) {
	sort.Slice(indexes, func(i, j int) bool {
		if indexes[i].Kind != indexes[j].Kind {
			return indexes[i].Kind < indexes[j].Kind
		}
		return indexes[i].ID < indexes[j].ID
	})
}

// CompareDsIndexKeys is exposed for focused ordering tests.
func CompareDsIndexKeys(a, b []byte) int { return bytes.Compare(a, b) }
