package datastore

import (
	"context"
	"database/sql"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func (g *GRPCServer) Commit(ctx context.Context, req *datastorepb.CommitRequest) (*datastorepb.CommitResponse, error) {
	if req.ProjectId == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id is required")
	}
	database := req.DatabaseId
	if database == "" {
		database = defaultDatabase
	}

	var entry txEntry
	if tx := req.GetTransaction(); len(tx) > 0 {
		txID := string(tx)
		g.txMu.Lock()
		var ok bool
		entry, ok = g.txns[txID]
		if ok {
			delete(g.txns, txID)
		}
		g.txMu.Unlock()
		if !ok {
			return nil, status.Error(codes.NotFound, "transaction not found: "+txID)
		}
		if entry.readOnly && len(req.Mutations) > 0 {
			return nil, status.Error(codes.FailedPrecondition, "read-only transaction cannot contain writes")
		}
	}

	commitTime := timestamppb.Now()

	// Wrap all mutations in a single SQLite transaction for atomicity and
	// performance (one WAL commit instead of two per mutation).
	// The accumulator defers change-log and field-index writes so they can be
	// flushed in bulk at the end, reducing SQL round-trips from O(n) to O(1).
	var results []*datastorepb.MutationResult
	if err := g.store.RunInTx(func(tx *sql.Tx) error {
		if err := checkOCCConflicts(tx, entry.reads); err != nil {
			return err
		}
		acc := storage.NewCommitAccumulator()
		results = make([]*datastorepb.MutationResult, 0, len(req.Mutations))

		// Fast path: all mutations are simple upserts (complete key, baseVersion=0,
		// no property mask, no transforms) - execute as one batch INSERT.
		if bulkRows, ok := g.collectSimpleUpserts(req.ProjectId, database, req.Mutations); ok {
			versions, err := g.store.DsUpsertManyTx(tx, req.ProjectId, database, bulkRows, commitTime, acc)
			if err != nil {
				return err
			}
			for _, r := range bulkRows {
				results = append(results, &datastorepb.MutationResult{
					Version:    versions[r.Path],
					UpdateTime: commitTime,
				})
			}
			return acc.Flush(tx)
		}

		for _, m := range req.Mutations {
			mr, err := g.applyMutationTx(tx, req.ProjectId, database, m, commitTime, acc)
			if err != nil {
				return err
			}
			results = append(results, mr)
		}
		return acc.Flush(tx)
	}); err != nil {
		return nil, err
	}

	return &datastorepb.CommitResponse{
		MutationResults: results,
		CommitTime:      commitTime,
	}, nil
}

func (s *Server) handleCommit(w http.ResponseWriter, r *http.Request, project string) {
	var req datastorepb.CommitRequest
	if !readProtoJSON(w, r.Body, &req) {
		return
	}
	if req.ProjectId == "" {
		req.ProjectId = project
	}
	resp, err := s.grpc.Commit(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	writeProtoJSON(w, resp)
}

// collectSimpleUpserts checks whether every mutation is a plain upsert (complete key,
// baseVersion=0, no property mask, no transforms). If so, it returns the batch rows
// and true so the caller can use a single multi-row INSERT instead of N round-trips.
func (g *GRPCServer) collectSimpleUpserts(project, database string, mutations []*datastorepb.Mutation) ([]storage.UpsertManyRow, bool) {
	if len(mutations) == 0 {
		return nil, false
	}
	rows := make([]storage.UpsertManyRow, 0, len(mutations))
	for _, m := range mutations {
		op, ok := m.Operation.(*datastorepb.Mutation_Upsert)
		if !ok {
			return nil, false
		}
		if m.GetBaseVersion() != 0 || m.GetPropertyMask() != nil || len(m.PropertyTransforms) != 0 {
			return nil, false
		}
		e := op.Upsert
		if isIncompleteKey(e.Key) {
			return nil, false
		}
		proj, db, ns, kind, parentPath, path := keyComponents(e.Key)
		if proj == "" {
			proj = project
		}
		if db == "" {
			db = database
		}
		if proj != project || db != database {
			return nil, false // cross-project/database mutations fall back to per-mutation path
		}
		rows = append(rows, storage.UpsertManyRow{
			Namespace:  ns,
			Path:       path,
			Kind:       kind,
			ParentPath: parentPath,
			Entity:     e,
		})
	}
	return rows, true
}

// applyMutationTx executes a single Mutation within the given transaction and returns its MutationResult.
func (g *GRPCServer) applyMutationTx(tx *sql.Tx, project, database string, m *datastorepb.Mutation, commitTime *timestamppb.Timestamp, acc *storage.CommitAccumulator) (*datastorepb.MutationResult, error) {
	var (
		resultKey        *datastorepb.Key
		version          int64
		updateTime       *timestamppb.Timestamp
		conflictDetected bool
		entity           *datastorepb.Entity
	)

	switch op := m.Operation.(type) {

	case *datastorepb.Mutation_Insert:
		e := op.Insert
		proj, db, ns, kind, parentPath, _ := keyComponents(e.Key)
		if proj == "" {
			proj = project
		}
		if db == "" {
			db = database
		}
		wasIncomplete := isIncompleteKey(e.Key)
		if wasIncomplete {
			allocID, err := g.store.DsAllocateIdsTx(tx, proj, db, ns, kind, 1)
			if err != nil {
				return nil, err
			}
			e = proto.Clone(e).(*datastorepb.Entity)
			e.Key = withID(e.Key, allocID)
		}
		_, _, _, _, parentPath, path := keyComponents(e.Key)
		var ct *timestamppb.Timestamp
		var err error
		entity, version, ct, updateTime, err = g.store.DsInsertTx(tx, proj, db, ns, path, kind, parentPath, e, acc)
		if err != nil {
			return nil, err
		}
		_ = ct
		if wasIncomplete {
			resultKey = entity.Key
		}

	case *datastorepb.Mutation_Update:
		e := op.Update
		proj, db, ns, _, _, path := keyComponents(e.Key)
		if proj == "" {
			proj = project
		}
		if db == "" {
			db = database
		}
		e = applyPropertyMask(e, m.GetPropertyMask(), func() (*datastorepb.Entity, error) {
			existing, _, err := g.store.DsGet(proj, db, ns, path)
			return existing, err
		})
		var err error
		entity, version, _, updateTime, conflictDetected, err = g.store.DsUpdateTx(tx, proj, db, ns, path, e, m.GetBaseVersion(), acc)
		if err != nil {
			return nil, err
		}

	case *datastorepb.Mutation_Upsert:
		e := op.Upsert
		proj, db, ns, kind, parentPath, _ := keyComponents(e.Key)
		if proj == "" {
			proj = project
		}
		if db == "" {
			db = database
		}
		wasIncomplete := isIncompleteKey(e.Key)
		if wasIncomplete {
			allocID, err := g.store.DsAllocateIdsTx(tx, proj, db, ns, kind, 1)
			if err != nil {
				return nil, err
			}
			e = proto.Clone(e).(*datastorepb.Entity)
			e.Key = withID(e.Key, allocID)
		}
		_, _, _, _, parentPath, path := keyComponents(e.Key)
		e = applyPropertyMask(e, m.GetPropertyMask(), func() (*datastorepb.Entity, error) {
			existing, _, err := g.store.DsGet(proj, db, ns, path)
			return existing, err
		})
		var err error
		entity, version, _, updateTime, conflictDetected, err = g.store.DsUpsertTx(tx, proj, db, ns, path, kind, parentPath, e, m.GetBaseVersion(), acc)
		if err != nil {
			return nil, err
		}
		if wasIncomplete && entity != nil {
			resultKey = entity.Key
		}

	case *datastorepb.Mutation_Delete:
		proj, db, ns, _, _, path := keyComponents(op.Delete)
		if proj == "" {
			proj = project
		}
		if db == "" {
			db = database
		}
		if err := g.store.DsDeleteTx(tx, proj, db, ns, path, acc); err != nil {
			return nil, err
		}
		updateTime = commitTime
		return &datastorepb.MutationResult{UpdateTime: updateTime}, nil

	default:
		return nil, status.Error(codes.InvalidArgument, "unknown mutation operation")
	}

	if conflictDetected {
		return &datastorepb.MutationResult{
			Version:          version,
			UpdateTime:       updateTime,
			ConflictDetected: true,
		}, nil
	}

	var transformResults []*datastorepb.Value
	if len(m.PropertyTransforms) > 0 && entity != nil {
		var err error
		entity, transformResults, err = applyPropertyTransforms(entity, m.PropertyTransforms, commitTime)
		if err != nil {
			return nil, err
		}
		if err := g.resaveEntityTx(tx, project, database, entity, acc); err != nil {
			return nil, err
		}
	}

	return &datastorepb.MutationResult{
		Key:              resultKey,
		Version:          version,
		UpdateTime:       updateTime,
		TransformResults: transformResults,
	}, nil
}

// resaveEntityTx writes the transformed entity back to storage within the given transaction.
func (g *GRPCServer) resaveEntityTx(tx *sql.Tx, project, database string, entity *datastorepb.Entity, acc *storage.CommitAccumulator) error {
	proj, db, ns, kind, parentPath, path := keyComponents(entity.Key)
	if proj == "" {
		proj = project
	}
	if db == "" {
		db = database
	}
	_, _, _, _, _, err := g.store.DsUpdateTx(tx, proj, db, ns, path, entity, 0, acc)
	_ = kind
	_ = parentPath
	return err
}

// applyPropertyTransforms applies server-side transforms to an entity in memory.
// Returns the modified entity and a per-transform result value slice.
// Scalar transforms (setToServerValue, increment, max, min) return the new value.
// Array transforms (append, remove) return null (per Datastore spec).
func applyPropertyTransforms(entity *datastorepb.Entity, transforms []*datastorepb.PropertyTransform, commitTime *timestamppb.Timestamp) (*datastorepb.Entity, []*datastorepb.Value, error) {
	if entity.Properties == nil {
		entity.Properties = make(map[string]*datastorepb.Value)
	}
	results := make([]*datastorepb.Value, 0, len(transforms))
	nullVal := &datastorepb.Value{ValueType: &datastorepb.Value_NullValue{}}

	for _, t := range transforms {
		prop := t.Property
		current := getNestedProp(entity, prop)

		switch tt := t.TransformType.(type) {
		case *datastorepb.PropertyTransform_SetToServerValue:
			if tt.SetToServerValue == datastorepb.PropertyTransform_REQUEST_TIME {
				v := &datastorepb.Value{
					ValueType: &datastorepb.Value_TimestampValue{TimestampValue: commitTime},
				}
				setNestedProp(entity, prop, v)
				results = append(results, v)
			} else {
				results = append(results, nullVal)
			}

		case *datastorepb.PropertyTransform_Increment:
			v := dsNumericOp(current, tt.Increment, func(a, b float64) float64 { return a + b })
			setNestedProp(entity, prop, v)
			results = append(results, v)

		case *datastorepb.PropertyTransform_Maximum:
			v := dsNumericOp(current, tt.Maximum, func(a, b float64) float64 {
				if a >= b {
					return a
				}
				return b
			})
			setNestedProp(entity, prop, v)
			results = append(results, v)

		case *datastorepb.PropertyTransform_Minimum:
			v := dsNumericOp(current, tt.Minimum, func(a, b float64) float64 {
				if a <= b {
					return a
				}
				return b
			})
			setNestedProp(entity, prop, v)
			results = append(results, v)

		case *datastorepb.PropertyTransform_AppendMissingElements:
			v := appendMissing(current, tt.AppendMissingElements)
			setNestedProp(entity, prop, v)
			results = append(results, nullVal)

		case *datastorepb.PropertyTransform_RemoveAllFromArray:
			v := removeFromArray(current, tt.RemoveAllFromArray)
			setNestedProp(entity, prop, v)
			results = append(results, nullVal)

		default:
			results = append(results, nullVal)
		}
	}
	return entity, results, nil
}

// getNestedProp retrieves a property value by dot-separated path.
// E.g. "nested.p1" fetches entity.Properties["nested"].EntityValue.Properties["p1"].
func getNestedProp(entity *datastorepb.Entity, path string) *datastorepb.Value {
	dot := strings.IndexByte(path, '.')
	if dot < 0 {
		if entity.Properties == nil {
			return nil
		}
		return entity.Properties[path]
	}
	head, tail := path[:dot], path[dot+1:]
	if entity.Properties == nil {
		return nil
	}
	parent := entity.Properties[head]
	if parent == nil {
		return nil
	}
	ev := parent.GetEntityValue()
	if ev == nil {
		return nil
	}
	return getNestedProp(ev, tail)
}

// setNestedProp sets a property value by dot-separated path, creating intermediate
// EntityValue wrappers as needed.
func setNestedProp(entity *datastorepb.Entity, path string, v *datastorepb.Value) {
	if entity.Properties == nil {
		entity.Properties = make(map[string]*datastorepb.Value)
	}
	dot := strings.IndexByte(path, '.')
	if dot < 0 {
		entity.Properties[path] = v
		return
	}
	head, tail := path[:dot], path[dot+1:]
	parent := entity.Properties[head]
	var ev *datastorepb.Entity
	if parent != nil {
		ev = parent.GetEntityValue()
	}
	if ev == nil {
		ev = &datastorepb.Entity{}
	}
	setNestedProp(ev, tail, v)
	entity.Properties[head] = &datastorepb.Value{
		ValueType: &datastorepb.Value_EntityValue{EntityValue: ev},
	}
}

func dsNumericOp(current, operand *datastorepb.Value, op func(a, b float64) float64) *datastorepb.Value {
	var a float64
	if current != nil {
		a = dsNumericFloat(current)
	}
	b := dsNumericFloat(operand)
	result := op(a, b)

	_, aIsInt := current.GetValueType().(*datastorepb.Value_IntegerValue)
	_, bIsInt := operand.GetValueType().(*datastorepb.Value_IntegerValue)
	if (current == nil || aIsInt) && bIsInt {
		return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: int64(result)}}
	}
	return &datastorepb.Value{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: result}}
}

func appendMissing(current *datastorepb.Value, toAdd *datastorepb.ArrayValue) *datastorepb.Value {
	existing := []*datastorepb.Value{}
	if av := current.GetArrayValue(); av != nil {
		existing = av.Values
	}
	out := make([]*datastorepb.Value, len(existing))
	copy(out, existing)
	for _, add := range toAdd.GetValues() {
		found := false
		for _, e := range existing {
			if compareValues(e, add) == 0 {
				found = true
				break
			}
		}
		if !found {
			out = append(out, add)
		}
	}
	return &datastorepb.Value{ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: out}}}
}

func removeFromArray(current *datastorepb.Value, toRemove *datastorepb.ArrayValue) *datastorepb.Value {
	if av := current.GetArrayValue(); av == nil {
		return current
	}
	var out []*datastorepb.Value
	for _, e := range current.GetArrayValue().GetValues() {
		keep := true
		for _, rem := range toRemove.GetValues() {
			if compareValues(e, rem) == 0 {
				keep = false
				break
			}
		}
		if keep {
			out = append(out, e)
		}
	}
	return &datastorepb.Value{ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: out}}}
}

// applyPropertyMask applies a PropertyMask to a partial entity for Update/Upsert mutations.
// If mask is nil or has no paths, the entity is returned unchanged (full-replace semantics).
// Otherwise, the existing entity is fetched via fetchExisting, and for each path in the mask:
//   - if the path is present in incoming, copy its value onto existing
//   - if the path is absent in incoming, delete it from existing
//
// The returned entity is always a clone; the originals are not modified.
func applyPropertyMask(incoming *datastorepb.Entity, mask *datastorepb.PropertyMask, fetchExisting func() (*datastorepb.Entity, error)) *datastorepb.Entity {
	if mask == nil || len(mask.GetPaths()) == 0 {
		return incoming
	}
	existing, err := fetchExisting()
	if err != nil {
		// Entity does not exist yet - write only the masked fields from incoming.
		merged := proto.Clone(incoming).(*datastorepb.Entity)
		if merged.Properties == nil {
			merged.Properties = make(map[string]*datastorepb.Value)
		}
		for k := range merged.Properties {
			masked := false
			for _, p := range mask.GetPaths() {
				if p == k {
					masked = true
					break
				}
			}
			if !masked {
				delete(merged.Properties, k)
			}
		}
		return merged
	}
	merged := proto.Clone(existing).(*datastorepb.Entity)
	if merged.Properties == nil {
		merged.Properties = make(map[string]*datastorepb.Value)
	}
	for _, p := range mask.GetPaths() {
		if v, ok := incoming.GetProperties()[p]; ok {
			merged.Properties[p] = v
		} else {
			delete(merged.Properties, p)
		}
	}
	merged.Key = incoming.Key
	return merged
}
