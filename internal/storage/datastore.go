package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// DsSortSpec describes one ORDER BY field for SQL-level keyset pagination.
type DsSortSpec struct {
	FieldPath string // property name
	Col       string // auto-detected ds_field_index column (value_string/value_int/etc.)
	Desc      bool
}

// CursorSortKV holds the sort-field value for one ORDER BY field in a keyset cursor.
type CursorSortKV struct {
	Col string `json:"c"` // ds_field_index column name
	V   string `json:"v"` // sort value serialized as string
}

// CursorPayload is the structured cursor for keyset pagination; JSON-marshaled
// and URL-safe base64-encoded before transmission.
type CursorPayload struct {
	P string         `json:"p"`           // entity path (tie-breaker)
	S []CursorSortKV `json:"s,omitempty"` // one entry per ORDER BY field
}

// ErrColumnNotDetected is returned by DsQueryKindLimited when a sort field has
// no indexed data, preventing SQL ORDER BY column auto-detection.
var ErrColumnNotDetected = errors.New("sort column not found in field index")

// dbExec is defined in storage.go (shared with document.go).

// DsGet fetches a single active entity. Returns (nil, 0, codes.NotFound) if absent or deleted.
func (s *Store) DsGet(project, database, namespace, path string) (*datastorepb.Entity, int64, error) {
	var data []byte
	var version int64
	err := s.rdb.QueryRow(
		`SELECT data, version FROM ds_documents
		 WHERE project=? AND database=? AND namespace=? AND path=? AND deleted=0`,
		project, database, namespace, path,
	).Scan(&data, &version)
	if err == sql.ErrNoRows {
		return nil, 0, status.Errorf(codes.NotFound, "entity not found: %s", path)
	}
	if err != nil {
		return nil, 0, fmt.Errorf("ds get: %w", err)
	}
	var e datastorepb.Entity
	if err := proto.Unmarshal(data, &e); err != nil {
		return nil, 0, fmt.Errorf("ds unmarshal: %w", err)
	}
	return &e, version, nil
}

// DsGetWithTimes fetches an entity plus its create/update timestamps.
func (s *Store) DsGetWithTimes(project, database, namespace, path string) (*datastorepb.Entity, int64, *timestamppb.Timestamp, *timestamppb.Timestamp, error) {
	var data []byte
	var version int64
	var createStr, updateStr string
	err := s.rdb.QueryRow(
		`SELECT data, version, create_time, update_time FROM ds_documents
		 WHERE project=? AND database=? AND namespace=? AND path=? AND deleted=0`,
		project, database, namespace, path,
	).Scan(&data, &version, &createStr, &updateStr)
	if err == sql.ErrNoRows {
		return nil, 0, nil, nil, status.Errorf(codes.NotFound, "entity not found: %s", path)
	}
	if err != nil {
		return nil, 0, nil, nil, fmt.Errorf("ds get: %w", err)
	}
	var e datastorepb.Entity
	if err := proto.Unmarshal(data, &e); err != nil {
		return nil, 0, nil, nil, fmt.Errorf("ds unmarshal: %w", err)
	}
	ct, _ := time.Parse(timeLayout, createStr)
	ut, _ := time.Parse(timeLayout, updateStr)
	return &e, version, timestamppb.New(ct), timestamppb.New(ut), nil
}

// DsInsert creates a new entity, returning codes.AlreadyExists if one is active.
func (s *Store) DsInsert(project, database, namespace, path, kind, parentPath string, entity *datastorepb.Entity) (*datastorepb.Entity, int64, *timestamppb.Timestamp, *timestamppb.Timestamp, error) {
	return dsInsertExec(s.wdb, project, database, namespace, path, kind, parentPath, entity)
}

// DsInsertTx is like DsInsert but runs within the provided transaction.
func (s *Store) DsInsertTx(tx *sql.Tx, project, database, namespace, path, kind, parentPath string, entity *datastorepb.Entity) (*datastorepb.Entity, int64, *timestamppb.Timestamp, *timestamppb.Timestamp, error) {
	return dsInsertExec(tx, project, database, namespace, path, kind, parentPath, entity)
}

func dsInsertExec(exec dbExec, project, database, namespace, path, kind, parentPath string, entity *datastorepb.Entity) (*datastorepb.Entity, int64, *timestamppb.Timestamp, *timestamppb.Timestamp, error) {
	var dummy int
	err := exec.QueryRow(
		`SELECT 1 FROM ds_documents WHERE project=? AND database=? AND namespace=? AND path=? AND deleted=0`,
		project, database, namespace, path,
	).Scan(&dummy)
	if err == nil {
		return nil, 0, nil, nil, status.Errorf(codes.AlreadyExists, "entity already exists: %s", path)
	}
	now := timestamppb.Now()
	e := proto.Clone(entity).(*datastorepb.Entity)
	ver, err := dsSaveExec(exec, project, database, namespace, path, kind, parentPath, e, 0, now, now)
	return e, ver, now, now, err
}

// DsUpdate merges into an existing entity. Returns codes.NotFound if absent.
// If baseVersion > 0 and current version differs, returns conflictDetected=true without writing.
func (s *Store) DsUpdate(project, database, namespace, path string, entity *datastorepb.Entity, baseVersion int64) (*datastorepb.Entity, int64, *timestamppb.Timestamp, *timestamppb.Timestamp, bool, error) {
	return dsUpdateExec(s.wdb, project, database, namespace, path, entity, baseVersion)
}

// DsUpdateTx is like DsUpdate but runs within the provided transaction.
func (s *Store) DsUpdateTx(tx *sql.Tx, project, database, namespace, path string, entity *datastorepb.Entity, baseVersion int64) (*datastorepb.Entity, int64, *timestamppb.Timestamp, *timestamppb.Timestamp, bool, error) {
	return dsUpdateExec(tx, project, database, namespace, path, entity, baseVersion)
}

func dsUpdateExec(exec dbExec, project, database, namespace, path string, entity *datastorepb.Entity, baseVersion int64) (*datastorepb.Entity, int64, *timestamppb.Timestamp, *timestamppb.Timestamp, bool, error) {
	var data []byte
	var curVersion int64
	var createStr string
	err := exec.QueryRow(
		`SELECT data, version, create_time FROM ds_documents
		 WHERE project=? AND database=? AND namespace=? AND path=? AND deleted=0`,
		project, database, namespace, path,
	).Scan(&data, &curVersion, &createStr)
	if err == sql.ErrNoRows {
		return nil, 0, nil, nil, false, status.Errorf(codes.NotFound, "entity not found: %s", path)
	}
	if err != nil {
		return nil, 0, nil, nil, false, fmt.Errorf("ds update read: %w", err)
	}

	if baseVersion > 0 && curVersion != baseVersion {
		return nil, curVersion, nil, nil, true, nil // conflict, no write
	}

	ct, _ := time.Parse(timeLayout, createStr)
	createTime := timestamppb.New(ct)
	now := timestamppb.Now()

	// Fetch existing entity to recover kind/parentPath from row metadata.
	var kind, parentPath string
	_ = exec.QueryRow(`SELECT kind, parent_path FROM ds_documents WHERE project=? AND database=? AND namespace=? AND path=?`,
		project, database, namespace, path).Scan(&kind, &parentPath)

	e := proto.Clone(entity).(*datastorepb.Entity)
	newVer, err := dsSaveExec(exec, project, database, namespace, path, kind, parentPath, e, curVersion, createTime, now)
	return e, newVer, createTime, now, false, err
}

// DsUpsert creates or replaces an entity, preserving create_time for existing docs.
// If baseVersion > 0 and the entity exists with a different version, returns conflictDetected=true.
func (s *Store) DsUpsert(project, database, namespace, path, kind, parentPath string, entity *datastorepb.Entity, baseVersion int64) (*datastorepb.Entity, int64, *timestamppb.Timestamp, *timestamppb.Timestamp, bool, error) {
	return dsUpsertExec(s.wdb, project, database, namespace, path, kind, parentPath, entity, baseVersion)
}

// DsUpsertTx is like DsUpsert but runs within the provided transaction.
func (s *Store) DsUpsertTx(tx *sql.Tx, project, database, namespace, path, kind, parentPath string, entity *datastorepb.Entity, baseVersion int64) (*datastorepb.Entity, int64, *timestamppb.Timestamp, *timestamppb.Timestamp, bool, error) {
	return dsUpsertExec(tx, project, database, namespace, path, kind, parentPath, entity, baseVersion)
}

func dsUpsertExec(exec dbExec, project, database, namespace, path, kind, parentPath string, entity *datastorepb.Entity, baseVersion int64) (*datastorepb.Entity, int64, *timestamppb.Timestamp, *timestamppb.Timestamp, bool, error) {
	now := timestamppb.Now()
	createTime := now

	var curVersion int64
	var createStr string
	err := exec.QueryRow(
		`SELECT version, create_time FROM ds_documents
		 WHERE project=? AND database=? AND namespace=? AND path=? AND deleted=0`,
		project, database, namespace, path,
	).Scan(&curVersion, &createStr)
	if err == nil {
		// Existing active entity.
		if baseVersion > 0 && curVersion != baseVersion {
			return nil, curVersion, nil, nil, true, nil // conflict
		}
		ct, _ := time.Parse(timeLayout, createStr)
		createTime = timestamppb.New(ct)
	}

	e := proto.Clone(entity).(*datastorepb.Entity)
	newVer, err := dsSaveExec(exec, project, database, namespace, path, kind, parentPath, e, curVersion, createTime, now)
	return e, newVer, createTime, now, false, err
}

// DsDelete soft-deletes an entity. No-ops silently if absent.
func (s *Store) DsDelete(project, database, namespace, path string) error {
	return dsDeleteExec(s.wdb, project, database, namespace, path)
}

// DsDeleteTx is like DsDelete but runs within the provided transaction.
func (s *Store) DsDeleteTx(tx *sql.Tx, project, database, namespace, path string) error {
	return dsDeleteExec(tx, project, database, namespace, path)
}

func dsDeleteExec(exec dbExec, project, database, namespace, path string) error {
	now := time.Now().UTC().Format(timeLayout)
	_, err := exec.Exec(
		`UPDATE ds_documents SET deleted=1, update_time=?
		 WHERE project=? AND database=? AND namespace=? AND path=? AND deleted=0`,
		now, project, database, namespace, path,
	)
	if err != nil {
		return err
	}
	if _, err := exec.Exec(
		`DELETE FROM ds_field_index WHERE project=? AND database=? AND namespace=? AND doc_path=?`,
		project, database, namespace, path,
	); err != nil {
		return fmt.Errorf("ds_field_index delete: %w", err)
	}
	// Append tombstone to change log.
	_, err = exec.Exec(`
		INSERT INTO ds_document_changes
			(project, database, namespace, path, kind, parent_path, change_time, deleted, data)
		SELECT ?, ?, ?, ?, kind, parent_path, ?, 1, NULL
		FROM ds_documents WHERE project=? AND database=? AND namespace=? AND path=?`,
		project, database, namespace, path, now,
		project, database, namespace, path,
	)
	return err
}

// DsQueryKind returns all active entities of a given kind under parentPath.
// If parentPath is empty, returns all entities of that kind in the namespace.
// If ancestorPath is non-empty, further filters to entities whose path starts with ancestorPath.
// filterSQL / filterArgs are optional SQL conditions generated by buildDsWhereClause;
// they are correlated against table alias "d" and ANDed into the WHERE clause.
func (s *Store) DsQueryKind(project, database, namespace, kind, ancestorPath string, filterSQL string, filterArgs []any) ([]*DsEntityRow, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if ancestorPath != "" {
		// HAS_ANCESTOR: path == ancestorPath OR path LIKE ancestorPath + '/%'
		q := `SELECT data, version, create_time, update_time, path FROM ds_documents d
			  WHERE project=? AND database=? AND namespace=? AND kind=? AND deleted=0
			    AND (path=? OR path LIKE ?)`
		qArgs := []any{project, database, namespace, kind, ancestorPath, ancestorPath + "/%"}
		if filterSQL != "" {
			q += " AND " + filterSQL
			qArgs = append(qArgs, filterArgs...)
		}
		rows, err = s.rdb.Query(q, qArgs...)
	} else {
		q := `SELECT data, version, create_time, update_time, path FROM ds_documents d
			  WHERE project=? AND database=? AND namespace=? AND kind=? AND deleted=0`
		qArgs := []any{project, database, namespace, kind}
		if filterSQL != "" {
			q += " AND " + filterSQL
			qArgs = append(qArgs, filterArgs...)
		}
		rows, err = s.rdb.Query(q, qArgs...)
	}
	if err != nil {
		return nil, fmt.Errorf("ds query kind: %w", err)
	}
	defer rows.Close()

	var results []*DsEntityRow
	for rows.Next() {
		var data []byte
		var version int64
		var createStr, updateStr, path string
		if err := rows.Scan(&data, &version, &createStr, &updateStr, &path); err != nil {
			return nil, fmt.Errorf("ds scan: %w", err)
		}
		var e datastorepb.Entity
		if err := proto.Unmarshal(data, &e); err != nil {
			return nil, fmt.Errorf("ds unmarshal: %w", err)
		}
		ct, _ := time.Parse(timeLayout, createStr)
		ut, _ := time.Parse(timeLayout, updateStr)
		results = append(results, &DsEntityRow{
			Entity:     &e,
			Version:    version,
			CreateTime: timestamppb.New(ct),
			UpdateTime: timestamppb.New(ut),
			Path:       path,
		})
	}
	return results, rows.Err()
}

// DsEntityRow holds a fetched entity with metadata.
type DsEntityRow struct {
	Entity     *datastorepb.Entity
	Version    int64
	CreateTime *timestamppb.Timestamp
	UpdateTime *timestamppb.Timestamp
	Path       string
}

// DsQueryKindLimited runs a paginated kind query with ORDER BY, keyset cursor,
// and LIMIT pushed into SQLite. Returns the filled-in sort specs (with Col populated)
// alongside the entity rows.
//
// If a sort field has no indexed data, ErrColumnNotDetected is returned and the
// caller should fall back to DsQueryKind + Go-side processing.
func (s *Store) DsQueryKindLimited(
	project, database, namespace, kind, ancestorPath string,
	filterSQL string, filterArgs []any,
	sorts []DsSortSpec,
	cursor *CursorPayload,
	limit int,
) ([]DsSortSpec, []*DsEntityRow, error) {
	// Make a local copy so we can fill in Col without mutating the caller's slice.
	localSorts := make([]DsSortSpec, len(sorts))
	copy(localSorts, sorts)
	for i := range localSorts {
		if localSorts[i].Col != "" {
			continue
		}
		col, err := s.detectSortColumn(project, database, namespace, kind, localSorts[i].FieldPath)
		if err != nil {
			return nil, nil, err
		}
		localSorts[i].Col = col
	}
	rows, err := s.runLimitedQuery(project, database, namespace, kind, ancestorPath,
		filterSQL, filterArgs, localSorts, cursor, limit)
	return localSorts, rows, err
}

// detectSortColumn probes ds_field_index to find which value column a field uses.
func (s *Store) detectSortColumn(project, database, namespace, kind, fieldPath string) (string, error) {
	var vStr *string
	var vInt *int64
	var vDouble *float64
	var vBool *int64
	var vNull *int64
	var vRef *string
	err := s.rdb.QueryRow(
		`SELECT value_string, value_int, value_double, value_bool, value_null, value_ref
		 FROM ds_field_index
		 WHERE project=? AND database=? AND namespace=? AND kind=? AND field_path=? AND in_array=0
		 LIMIT 1`,
		project, database, namespace, kind, fieldPath,
	).Scan(&vStr, &vInt, &vDouble, &vBool, &vNull, &vRef)
	if err == sql.ErrNoRows {
		return "", ErrColumnNotDetected
	}
	if err != nil {
		return "", fmt.Errorf("detect sort column: %w", err)
	}
	switch {
	case vInt != nil:
		return "value_int", nil
	case vDouble != nil:
		return "value_double", nil
	case vStr != nil:
		return "value_string", nil
	case vBool != nil:
		return "value_bool", nil
	case vNull != nil:
		return "value_null", nil
	case vRef != nil:
		return "value_ref", nil
	default:
		return "", ErrColumnNotDetected
	}
}

// runLimitedQuery builds and executes the paginated SQL query.
func (s *Store) runLimitedQuery(
	project, database, namespace, kind, ancestorPath string,
	filterSQL string, filterArgs []any,
	sorts []DsSortSpec,
	cursor *CursorPayload,
	limit int,
) ([]*DsEntityRow, error) {
	var sb strings.Builder
	var args []any

	sb.WriteString(`SELECT d.data, d.version, d.create_time, d.update_time, d.path`)
	sb.WriteString(` FROM ds_documents d`)

	// LEFT JOIN one ds_field_index alias per sort field.
	for i, sp := range sorts {
		fmt.Fprintf(&sb,
			` LEFT JOIN ds_field_index fi_%d ON fi_%d.project=d.project AND fi_%d.database=d.database`+
				` AND fi_%d.namespace=d.namespace AND fi_%d.doc_path=d.path`+
				` AND fi_%d.field_path=? AND fi_%d.in_array=0`,
			i, i, i, i, i, i, i)
		args = append(args, sp.FieldPath)
	}

	sb.WriteString(` WHERE d.project=? AND d.database=? AND d.namespace=? AND d.kind=? AND d.deleted=0`)
	args = append(args, project, database, namespace, kind)

	if ancestorPath != "" {
		sb.WriteString(` AND (d.path=? OR d.path LIKE ?)`)
		args = append(args, ancestorPath, ancestorPath+"/%")
	}
	if filterSQL != "" {
		sb.WriteString(` AND `)
		sb.WriteString(filterSQL)
		args = append(args, filterArgs...)
	}

	if cursor != nil {
		ks, ksArgs := buildKeysetCondition(sorts, cursor)
		if ks != "" {
			sb.WriteString(` AND (`)
			sb.WriteString(ks)
			sb.WriteByte(')')
			args = append(args, ksArgs...)
		}
	}

	// ORDER BY sort fields then d.path as tie-breaker.
	if len(sorts) > 0 {
		sb.WriteString(` ORDER BY `)
		for i, sp := range sorts {
			if i > 0 {
				sb.WriteString(`, `)
			}
			fmt.Fprintf(&sb, `fi_%d.%s`, i, sp.Col)
			if sp.Desc {
				sb.WriteString(` DESC`)
			} else {
				sb.WriteString(` ASC`)
			}
		}
		sb.WriteString(`, d.path ASC`)
	} else {
		sb.WriteString(` ORDER BY d.path ASC`)
	}

	sb.WriteString(` LIMIT ?`)
	args = append(args, limit)

	rows, err := s.rdb.Query(sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("ds limited query: %w", err)
	}
	defer rows.Close()

	var results []*DsEntityRow
	for rows.Next() {
		var data []byte
		var version int64
		var createStr, updateStr, path string
		if err := rows.Scan(&data, &version, &createStr, &updateStr, &path); err != nil {
			return nil, fmt.Errorf("ds limited scan: %w", err)
		}
		var e datastorepb.Entity
		if err := proto.Unmarshal(data, &e); err != nil {
			return nil, fmt.Errorf("ds limited unmarshal: %w", err)
		}
		ct, _ := time.Parse(timeLayout, createStr)
		ut, _ := time.Parse(timeLayout, updateStr)
		results = append(results, &DsEntityRow{
			Entity:     &e,
			Version:    version,
			CreateTime: timestamppb.New(ct),
			UpdateTime: timestamppb.New(ut),
			Path:       path,
		})
	}
	return results, rows.Err()
}

// buildKeysetCondition constructs the SQL OR-chain for keyset pagination.
// Each OR-branch advances one position in the sort key hierarchy, e.g.:
//
//	(f0>v0) OR (f0=v0 AND f1<v1) OR (f0=v0 AND f1=v1 AND path>cursor_path)
//
// The direction of inequality follows each field's Desc flag.
func buildKeysetCondition(sorts []DsSortSpec, cursor *CursorPayload) (string, []any) {
	if cursor == nil {
		return "", nil
	}
	if len(sorts) == 0 || len(cursor.S) == 0 {
		return "d.path > ?", []any{cursor.P}
	}

	n := len(sorts)
	if len(cursor.S) < n {
		n = len(cursor.S)
	}

	var parts []string
	var allArgs []any

	for outerIdx := 0; outerIdx <= n; outerIdx++ {
		var branch strings.Builder
		var branchArgs []any

		// Equality for all sort fields before outerIdx.
		for i := 0; i < outerIdx; i++ {
			if i > 0 {
				branch.WriteString(" AND ")
			}
			fmt.Fprintf(&branch, "fi_%d.%s = ?", i, sorts[i].Col)
			branchArgs = append(branchArgs, parseCursorValue(cursor.S[i].Col, cursor.S[i].V))
		}

		if outerIdx < n {
			// Inequality on field outerIdx: > for ASC, < for DESC.
			if outerIdx > 0 {
				branch.WriteString(" AND ")
			}
			op := ">"
			if sorts[outerIdx].Desc {
				op = "<"
			}
			fmt.Fprintf(&branch, "fi_%d.%s %s ?", outerIdx, sorts[outerIdx].Col, op)
			branchArgs = append(branchArgs, parseCursorValue(cursor.S[outerIdx].Col, cursor.S[outerIdx].V))
		} else {
			// Tie-breaker: path is always ascending.
			if outerIdx > 0 {
				branch.WriteString(" AND ")
			}
			branch.WriteString("d.path > ?")
			branchArgs = append(branchArgs, cursor.P)
		}

		parts = append(parts, "("+branch.String()+")")
		allArgs = append(allArgs, branchArgs...)
	}

	if len(parts) == 1 {
		return parts[0], allArgs
	}
	return strings.Join(parts, " OR "), allArgs
}

// parseCursorValue converts a string cursor value back to the Go type appropriate
// for the given ds_field_index column.
func parseCursorValue(col, v string) any {
	switch col {
	case "value_int", "value_bool", "value_null":
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	case "value_double":
		f, _ := strconv.ParseFloat(v, 64)
		return f
	default: // value_string, value_ref
		return v
	}
}

// DsAllocateIds atomically reserves count IDs for the given (project, database, namespace, kind).
// Returns the first allocated ID; caller uses [first, first+count).
func (s *Store) DsAllocateIds(project, database, namespace, kind string, count int) (int64, error) {
	var first int64
	err := s.RunInTx(func(tx *sql.Tx) error {
		var err error
		first, err = dsAllocateIdsExec(tx, project, database, namespace, kind, count)
		return err
	})
	return first, err
}

// DsAllocateIdsTx reserves IDs within an already-open transaction.
func (s *Store) DsAllocateIdsTx(tx *sql.Tx, project, database, namespace, kind string, count int) (int64, error) {
	return dsAllocateIdsExec(tx, project, database, namespace, kind, count)
}

func dsAllocateIdsExec(exec dbExec, project, database, namespace, kind string, count int) (int64, error) {
	if _, err := exec.Exec(
		`INSERT OR IGNORE INTO ds_id_sequences (project, database, namespace, kind, next_id)
		 VALUES (?, ?, ?, ?, 1)`,
		project, database, namespace, kind,
	); err != nil {
		return 0, fmt.Errorf("ds sequence init: %w", err)
	}

	var first int64
	if err := exec.QueryRow(
		`SELECT next_id FROM ds_id_sequences WHERE project=? AND database=? AND namespace=? AND kind=?`,
		project, database, namespace, kind,
	).Scan(&first); err != nil {
		return 0, fmt.Errorf("ds sequence read: %w", err)
	}

	if _, err := exec.Exec(
		`UPDATE ds_id_sequences SET next_id=next_id+? WHERE project=? AND database=? AND namespace=? AND kind=?`,
		count, project, database, namespace, kind,
	); err != nil {
		return 0, fmt.Errorf("ds sequence update: %w", err)
	}
	return first, nil
}

// dsSaveExec marshals and persists an entity using the given executor.
// nextVersion is the new version assigned (curVersion+1).
func dsSaveExec(exec dbExec, project, database, namespace, path, kind, parentPath string, entity *datastorepb.Entity, curVersion int64, createTime, updateTime *timestamppb.Timestamp) (int64, error) {
	data, err := proto.Marshal(entity)
	if err != nil {
		return 0, fmt.Errorf("ds marshal: %w", err)
	}
	newVersion := curVersion + 1
	createStr := createTime.AsTime().UTC().Format(timeLayout)
	updateStr := updateTime.AsTime().UTC().Format(timeLayout)
	_, err = exec.Exec(`
		INSERT INTO ds_documents
			(project, database, namespace, path, kind, parent_path, data, create_time, update_time, version, deleted)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
		ON CONFLICT (project, database, namespace, path) DO UPDATE SET
			kind        = excluded.kind,
			parent_path = excluded.parent_path,
			data        = excluded.data,
			create_time = excluded.create_time,
			update_time = excluded.update_time,
			version     = excluded.version,
			deleted     = 0`,
		project, database, namespace, path, kind, parentPath, data, createStr, updateStr, newVersion,
	)
	if err != nil {
		return 0, err
	}
	// Append to change log for snapshot read support.
	_, err = exec.Exec(`
		INSERT INTO ds_document_changes
			(project, database, namespace, path, kind, parent_path, change_time, deleted, data)
		VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)`,
		project, database, namespace, path, kind, parentPath, updateStr, data,
	)
	if err != nil {
		return 0, err
	}
	// Populate field index for SQL filter pushdown.
	if err := dsIndexDocFields(exec, project, database, namespace, kind, path, entity); err != nil {
		return 0, err
	}
	return newVersion, nil
}

// DsGetAsOf returns the entity state at or before asOf. Returns nil (not found) if no row exists.
func (s *Store) DsGetAsOf(project, database, namespace, path string, asOf time.Time) (*datastorepb.Entity, error) {
	asOfStr := asOf.UTC().Format(timeLayout)
	var data []byte
	var deleted int
	err := s.rdb.QueryRow(`
		SELECT data, deleted FROM ds_document_changes
		WHERE project=? AND database=? AND namespace=? AND path=? AND change_time <= ?
		ORDER BY seq DESC LIMIT 1`,
		project, database, namespace, path, asOfStr,
	).Scan(&data, &deleted)
	if err == sql.ErrNoRows || deleted == 1 {
		return nil, status.Errorf(codes.NotFound, "entity not found at readTime: %s", path)
	}
	if err != nil {
		return nil, fmt.Errorf("ds get as of: %w", err)
	}
	var e datastorepb.Entity
	if err := proto.Unmarshal(data, &e); err != nil {
		return nil, fmt.Errorf("ds unmarshal: %w", err)
	}
	return &e, nil
}

// DsQueryKindAsOf returns all entities of a given kind as of asOf.
func (s *Store) DsQueryKindAsOf(project, database, namespace, kind, ancestorPath string, asOf time.Time) ([]*DsEntityRow, error) {
	asOfStr := asOf.UTC().Format(timeLayout)
	var rows *sql.Rows
	var err error
	if ancestorPath != "" {
		rows, err = s.rdb.Query(`
			SELECT path, data FROM (
				SELECT path, data, deleted,
					ROW_NUMBER() OVER (PARTITION BY path ORDER BY seq DESC) rn
				FROM ds_document_changes
				WHERE project=? AND database=? AND namespace=? AND kind=? AND change_time <= ?
				  AND (path=? OR path LIKE ?)
			) WHERE rn=1 AND deleted=0`,
			project, database, namespace, kind, asOfStr, ancestorPath, ancestorPath+"/%",
		)
	} else {
		rows, err = s.rdb.Query(`
			SELECT path, data FROM (
				SELECT path, data, deleted,
					ROW_NUMBER() OVER (PARTITION BY path ORDER BY seq DESC) rn
				FROM ds_document_changes
				WHERE project=? AND database=? AND namespace=? AND kind=? AND change_time <= ?
			) WHERE rn=1 AND deleted=0`,
			project, database, namespace, kind, asOfStr,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("ds query kind as of: %w", err)
	}
	defer rows.Close()

	var results []*DsEntityRow
	for rows.Next() {
		var path string
		var data []byte
		if err := rows.Scan(&path, &data); err != nil {
			return nil, fmt.Errorf("ds scan as of: %w", err)
		}
		var e datastorepb.Entity
		if err := proto.Unmarshal(data, &e); err != nil {
			return nil, fmt.Errorf("ds unmarshal as of: %w", err)
		}
		results = append(results, &DsEntityRow{Entity: &e, Path: path})
	}
	return results, rows.Err()
}

// dsFiRow represents one row to insert into ds_field_index.
// Nil pointer fields are stored as SQL NULL.
type dsFiRow struct {
	fieldPath string
	vStr      *string
	vInt      *int64
	vDouble   *float64
	vBool     *int64 // 0 or 1
	vNull     *int64 // always 1 when set
	vBytes    []byte
	vRef      *string
	vLat      *float64
	vLng      *float64
	inArray   bool
}

// dsKeyToPath builds a storage path string from a *datastorepb.Key.
// Path format: Kind/id[/Kind/id...] — mirrors keyComponents in the datastore package.
func dsKeyToPath(key *datastorepb.Key) string {
	if key == nil {
		return ""
	}
	var parts []string
	for _, pe := range key.GetPath() {
		switch id := pe.GetIdType().(type) {
		case *datastorepb.Key_PathElement_Id:
			parts = append(parts, pe.Kind+"/"+strconv.FormatInt(id.Id, 10))
		case *datastorepb.Key_PathElement_Name:
			parts = append(parts, pe.Kind+"/"+id.Name)
		default:
			parts = append(parts, pe.Kind+"/")
		}
	}
	return strings.Join(parts, "/")
}

// dsCollectValue extracts indexable values from a Datastore property value recursively.
// inArray=true means we are already inside an array — nested arrays are skipped.
func dsCollectValue(fieldPath string, v *datastorepb.Value, inArray bool, rows *[]dsFiRow) {
	if v == nil || v.GetExcludeFromIndexes() {
		return
	}
	switch vt := v.GetValueType().(type) {
	case *datastorepb.Value_NullValue:
		one := int64(1)
		*rows = append(*rows, dsFiRow{fieldPath: fieldPath, vNull: &one, inArray: inArray})
	case *datastorepb.Value_BooleanValue:
		b := int64(0)
		if vt.BooleanValue {
			b = 1
		}
		*rows = append(*rows, dsFiRow{fieldPath: fieldPath, vBool: &b, inArray: inArray})
	case *datastorepb.Value_IntegerValue:
		n := vt.IntegerValue
		*rows = append(*rows, dsFiRow{fieldPath: fieldPath, vInt: &n, inArray: inArray})
	case *datastorepb.Value_DoubleValue:
		f := vt.DoubleValue
		*rows = append(*rows, dsFiRow{fieldPath: fieldPath, vDouble: &f, inArray: inArray})
	case *datastorepb.Value_StringValue:
		s := vt.StringValue
		*rows = append(*rows, dsFiRow{fieldPath: fieldPath, vStr: &s, inArray: inArray})
	case *datastorepb.Value_BlobValue:
		*rows = append(*rows, dsFiRow{fieldPath: fieldPath, vBytes: vt.BlobValue, inArray: inArray})
	case *datastorepb.Value_TimestampValue:
		s := vt.TimestampValue.AsTime().UTC().Format(timeLayout)
		*rows = append(*rows, dsFiRow{fieldPath: fieldPath, vStr: &s, inArray: inArray})
	case *datastorepb.Value_KeyValue:
		p := dsKeyToPath(vt.KeyValue)
		*rows = append(*rows, dsFiRow{fieldPath: fieldPath, vRef: &p, inArray: inArray})
	case *datastorepb.Value_GeoPointValue:
		if vt.GeoPointValue != nil {
			lat := vt.GeoPointValue.Latitude
			lng := vt.GeoPointValue.Longitude
			*rows = append(*rows, dsFiRow{fieldPath: fieldPath, vLat: &lat, vLng: &lng, inArray: inArray})
		}
	case *datastorepb.Value_EntityValue:
		if !inArray {
			// Sentinel row carries canonical proto bytes for EQUAL pushdown;
			// sub-properties are indexed separately below via recursion.
			opts := proto.MarshalOptions{Deterministic: true}
			b, _ := opts.Marshal(vt.EntityValue)
			*rows = append(*rows, dsFiRow{fieldPath: fieldPath, vBytes: b, inArray: false})
		}
		if vt.EntityValue != nil {
			for k, child := range vt.EntityValue.Properties {
				dsCollectValue(fieldPath+"."+k, child, inArray, rows)
			}
		}
	case *datastorepb.Value_ArrayValue:
		if inArray {
			return // no nested arrays
		}
		if vt.ArrayValue == nil || len(vt.ArrayValue.Values) == 0 {
			*rows = append(*rows, dsFiRow{fieldPath: fieldPath, inArray: false}) // empty-array sentinel
			return
		}
		// Canonical bytes sentinel for EQUAL filter on the whole array.
		opts := proto.MarshalOptions{Deterministic: true}
		b, _ := opts.Marshal(vt.ArrayValue)
		*rows = append(*rows, dsFiRow{fieldPath: fieldPath, vBytes: b, inArray: false})
		for _, elem := range vt.ArrayValue.Values {
			dsCollectValue(fieldPath, elem, true, rows)
		}
	}
}

// dsIndexDocFields rebuilds the ds_field_index rows for a single entity within exec.
func dsIndexDocFields(exec dbExec, project, database, namespace, kind, docPath string, entity *datastorepb.Entity) error {
	if _, err := exec.Exec(
		`DELETE FROM ds_field_index WHERE project=? AND database=? AND namespace=? AND doc_path=?`,
		project, database, namespace, docPath,
	); err != nil {
		return fmt.Errorf("ds_field_index delete: %w", err)
	}
	if entity == nil || len(entity.Properties) == 0 {
		return nil
	}
	var rows []dsFiRow
	for propName, v := range entity.Properties {
		dsCollectValue(propName, v, false, &rows)
	}
	return dsBatchInsertFI(exec, project, database, namespace, kind, docPath, rows)
}

// dsBatchInsertFI inserts field index rows in chunks of 500 to stay within
// SQLite's bound-variable limit.
// RebuildDsFieldIndex rebuilds ds_field_index from all active entities in ds_documents.
// Run once after a schema change or manual truncation of the index table.
// Processes entities in batches of 200 to bound transaction size.
func (s *Store) RebuildDsFieldIndex() error {
	const batchSize = 200
	type erow struct{ project, database, namespace, path, kind string; data []byte }

	offset := 0
	for {
		rows, err := s.rdb.Query(
			`SELECT project, database, namespace, path, kind, data
			 FROM ds_documents WHERE deleted=0
			 ORDER BY rowid LIMIT ? OFFSET ?`,
			batchSize, offset,
		)
		if err != nil {
			return fmt.Errorf("reindex query: %w", err)
		}
		var batch []erow
		for rows.Next() {
			var r erow
			if err := rows.Scan(&r.project, &r.database, &r.namespace, &r.path, &r.kind, &r.data); err != nil {
				rows.Close()
				return fmt.Errorf("reindex scan: %w", err)
			}
			batch = append(batch, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("reindex rows: %w", err)
		}
		if len(batch) == 0 {
			break
		}
		if err := s.RunInTx(func(tx *sql.Tx) error {
			for _, r := range batch {
				var entity datastorepb.Entity
				if err := proto.Unmarshal(r.data, &entity); err != nil {
					return fmt.Errorf("reindex unmarshal %s: %w", r.path, err)
				}
				if err := dsIndexDocFields(tx, r.project, r.database, r.namespace, r.kind, r.path, &entity); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		offset += len(batch)
		if len(batch) < batchSize {
			break
		}
	}
	return nil
}

func dsBatchInsertFI(exec dbExec, project, database, namespace, kind, docPath string, rows []dsFiRow) error {
	if len(rows) == 0 {
		return nil
	}
	const batchSize = 500
	const prefix = `INSERT INTO ds_field_index` +
		` (project, database, namespace, kind, doc_path, field_path,` +
		`  value_string, value_int, value_double, value_bool, value_null, value_ref, value_bytes, value_lat, value_lng, in_array)` +
		` VALUES `
	const placeholder = `(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`

	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]

		var sb strings.Builder
		sb.WriteString(prefix)
		args := make([]any, 0, len(batch)*16)
		for i, r := range batch {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(placeholder)
			inArrayInt := 0
			if r.inArray {
				inArrayInt = 1
			}
			args = append(args,
				project, database, namespace, kind, docPath, r.fieldPath,
				r.vStr, r.vInt, r.vDouble, r.vBool, r.vNull, r.vRef, nilIfEmpty(r.vBytes),
				r.vLat, r.vLng,
				inArrayInt,
			)
		}
		if _, err := exec.Exec(sb.String(), args...); err != nil {
			return fmt.Errorf("ds_field_index insert: %w", err)
		}
	}
	return nil
}
