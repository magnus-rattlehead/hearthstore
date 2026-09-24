package datastore

import (
	"context"
	"math"
	"net/http"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/magnus-rattlehead/hearthstore/internal/propertypath"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

func (g *GRPCServer) Commit(ctx context.Context, req *datastorepb.CommitRequest) (out *datastorepb.CommitResponse, resultErr error) {
	defer func() { resultErr = rpcError(resultErr) }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.store.CheckAvailable(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "commit request is required")
	}
	if proto.Size(req) > maxTransactionSize {
		return nil, status.Error(codes.ResourceExhausted, "commit exceeds the 10 MiB Datastore API limit")
	}
	if err := validateCommitLimits(req); err != nil {
		return nil, err
	}
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
			now := time.Now()
			if now.Sub(entry.created) >= transactionMaxAge || now.Sub(entry.lastUsed) >= transactionIdle {
				ok = false
			}
			delete(g.txns, txID)
		}
		g.txMu.Unlock()
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "the referenced transaction has expired or is no longer valid")
		}
		if entry.project != req.ProjectId || entry.database != database {
			return nil, status.Error(codes.InvalidArgument, "transaction belongs to another project or database")
		}
		if entry.readOnly && len(req.Mutations) > 0 {
			return nil, status.Error(codes.FailedPrecondition, "read-only transaction cannot contain writes")
		}
	}

	commitTime := timestamppb.Now()

	// Explicit transactions keep their OCC read set; independent writes retry conflicts.
	atomic := len(req.GetTransaction()) > 0 || req.GetSingleUseTransaction() != nil || req.Mode == datastorepb.CommitRequest_TRANSACTIONAL
	runTx := g.store.RunBatchedTx
	if atomic {
		runTx = g.store.RunInTxCtx
	}

	// Classify simple upserts before opening the transaction.
	bulkRows, isBulk := g.collectSimpleUpserts(req.ProjectId, database, req.Mutations)

	var results []*datastorepb.MutationResult

	// Independent bulk upserts adapt to Badger's transaction limit.
	if isBulk && !atomic {
		bulkResults, err := g.store.DsUpsertMany(ctx, req.ProjectId, database, bulkRows, commitTime)
		if err != nil {
			return nil, err
		}
		results = make([]*datastorepb.MutationResult, 0, len(bulkRows))
		for _, result := range bulkResults {
			results = append(results, &datastorepb.MutationResult{
				Key:        result.Key,
				Version:    result.Version,
				UpdateTime: result.UpdateTime,
			})
		}
		return &datastorepb.CommitResponse{
			MutationResults: results,
			CommitTime:      commitTime,
		}, nil
	}

	if err := runTx(ctx, func(tx *storage.Txn) error {
		if err := g.checkOCCConflicts(tx, entry.reads); err != nil {
			return err
		}
		for scope := range entry.queries {
			if err := g.store.CheckQueryScopeTx(tx, scope, entry.readTime.AsTime()); err != nil {
				return err
			}
		}
		acc := storage.NewCommitAccumulator()
		results = make([]*datastorepb.MutationResult, 0, len(req.Mutations))

		if isBulk {
			bulkResults, err := g.store.DsUpsertManyTx(tx, req.ProjectId, database, bulkRows, commitTime, acc)
			if err != nil {
				return err
			}
			for _, result := range bulkResults {
				results = append(results, &datastorepb.MutationResult{
					Key:        result.Key,
					Version:    result.Version,
					UpdateTime: result.UpdateTime,
				})
			}
			return nil
		}

		for _, m := range req.Mutations {
			mr, err := g.applyMutationTx(tx, req.ProjectId, database, m, commitTime, acc)
			if err != nil {
				return err
			}
			results = append(results, mr)
		}
		return nil
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
	start := time.Now()
	SetHTTPDetails(r.Context(), DSMutationDetails(&req))
	resp, err := s.grpc.Commit(r.Context(), &req)
	if err != nil {
		writeGrpcErr(w, err)
		return
	}
	MergeHTTPDetails(r.Context(), DSCommitResponseDetails(resp, time.Since(start)))
	writeProtoJSON(w, resp)
}

// collectSimpleUpserts returns rows eligible for the batched Badger write path.
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
		e := &datastorepb.Entity{Key: scopedKey(op.Upsert.Key, project, database), Properties: op.Upsert.Properties}
		allocateID := isIncompleteKey(e.Key)
		proj, db, ns, kind, parentPath, path := keyComponents(e.Key)
		if len(e.Key.GetPath()) == 0 || kind == "" {
			return nil, false
		}
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
			AllocateID: allocateID,
		})
	}
	return rows, true
}

// applyMutationTx applies one mutation within tx.
func (g *GRPCServer) applyMutationTx(tx *storage.Txn, project, database string, m *datastorepb.Mutation, commitTime *timestamppb.Timestamp, acc *storage.CommitAccumulator) (*datastorepb.MutationResult, error) {
	var incoming *datastorepb.Entity
	switch op := m.Operation.(type) {
	case *datastorepb.Mutation_Insert:
		incoming = op.Insert
	case *datastorepb.Mutation_Update:
		incoming = op.Update
	case *datastorepb.Mutation_Upsert:
		incoming = op.Upsert
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
		return &datastorepb.MutationResult{UpdateTime: commitTime}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown mutation operation")
	}
	if incoming == nil || incoming.Key == nil {
		return nil, status.Error(codes.InvalidArgument, "entity key is required")
	}
	e := proto.Clone(incoming).(*datastorepb.Entity)
	e.Key = scopedKey(e.Key, project, database)
	proj, db, ns, kind, _, _ := keyComponents(e.Key)
	if proj == "" {
		proj = project
	}
	if db == "" {
		db = database
	}
	wasIncomplete := isIncompleteKey(e.Key)
	if wasIncomplete {
		if _, update := m.Operation.(*datastorepb.Mutation_Update); update {
			return nil, status.Error(codes.InvalidArgument, "update requires a complete key")
		}
		key, err := g.allocateUnusedKeyTx(tx, proj, db, ns, kind, e.Key)
		if err != nil {
			return nil, err
		}
		e.Key = key
	}
	_, _, _, _, parent, path := keyComponents(e.Key)
	var err error
	e, err = applyPropertyMask(e, m.PropertyMask, func() (*datastorepb.Entity, error) {
		existing, _, err := g.store.DsGetTx(tx, proj, db, ns, path)
		return existing, err
	})
	if err != nil {
		return nil, err
	}
	var transforms []*datastorepb.Value
	if len(m.PropertyTransforms) > 0 {
		e, transforms, err = applyPropertyTransforms(e, m.PropertyTransforms, commitTime)
		if err != nil {
			return nil, err
		}
	}
	if err := validateEntityLimits(e); err != nil {
		return nil, err
	}
	write := storage.EntityWrite{
		Project: proj, Database: db, Namespace: ns, Path: path,
		Kind: kind, ParentPath: parent, Entity: e, BaseVersion: m.GetBaseVersion(),
	}
	var written storage.WriteResult
	switch m.Operation.(type) {
	case *datastorepb.Mutation_Insert:
		written, err = g.store.DsInsertTx(tx, write, acc)
	case *datastorepb.Mutation_Update:
		written, err = g.store.DsUpdateTx(tx, write, acc)
	case *datastorepb.Mutation_Upsert:
		written, err = g.store.DsUpsertTx(tx, write, acc)
	}
	if err != nil {
		return nil, err
	}
	result := &datastorepb.MutationResult{Version: written.Version, UpdateTime: written.UpdateTime, ConflictDetected: written.Conflict}
	if !written.Conflict {
		result.TransformResults = transforms
		if wasIncomplete {
			result.Key = e.Key
		}
	}
	return result, nil
}

// applyPropertyTransforms returns the transformed entity and protocol result values.
// Scalar transforms return their new value; array transforms return null.
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
					ValueType: &datastorepb.Value_TimestampValue{TimestampValue: &timestamppb.Timestamp{Seconds: commitTime.Seconds, Nanos: commitTime.Nanos / 1_000_000 * 1_000_000}},
				}
				setNestedProp(entity, prop, v)
				results = append(results, v)
			} else {
				results = append(results, nullVal)
			}

		case *datastorepb.PropertyTransform_Increment:
			v := dsNumericOp(current, tt.Increment, "increment")
			setNestedProp(entity, prop, v)
			results = append(results, v)

		case *datastorepb.PropertyTransform_Maximum:
			v := dsNumericOp(current, tt.Maximum, "maximum")
			setNestedProp(entity, prop, v)
			results = append(results, v)

		case *datastorepb.PropertyTransform_Minimum:
			v := dsNumericOp(current, tt.Minimum, "minimum")
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

func getNestedProp(entity *datastorepb.Entity, path string) *datastorepb.Value {
	return propertypath.Get(entity, path)
}

func setNestedProp(entity *datastorepb.Entity, path string, value *datastorepb.Value) {
	propertypath.Set(entity, path, value)
}

func dsNumericOp(current, operand *datastorepb.Value, operation string) *datastorepb.Value {
	if !isDsNumeric(current) {
		return proto.Clone(operand).(*datastorepb.Value)
	}
	if operation != "increment" {
		if math.IsNaN(dsNumericFloat(current)) || math.IsNaN(dsNumericFloat(operand)) {
			return &datastorepb.Value{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: math.NaN()}}
		}
		comparison := compareTransformValues(current, operand)
		if operation == "maximum" && comparison >= 0 || operation == "minimum" && comparison <= 0 {
			return current
		}
		return proto.Clone(operand).(*datastorepb.Value)
	}
	a, aInt := current.GetValueType().(*datastorepb.Value_IntegerValue)
	b, bInt := operand.GetValueType().(*datastorepb.Value_IntegerValue)
	if aInt && bInt {
		result := a.IntegerValue + b.IntegerValue
		if b.IntegerValue > 0 && a.IntegerValue > math.MaxInt64-b.IntegerValue {
			result = math.MaxInt64
		}
		if b.IntegerValue < 0 && a.IntegerValue < math.MinInt64-b.IntegerValue {
			result = math.MinInt64
		}
		return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: result}}
	}
	return &datastorepb.Value{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: dsNumericFloat(current) + dsNumericFloat(operand)}}
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
		for _, e := range out {
			if compareTransformValues(e, add) == 0 {
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
			if compareTransformValues(e, rem) == 0 {
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

// applyPropertyMask applies only explicitly masked fields; an empty mask preserves existing properties.
func applyPropertyMask(incoming *datastorepb.Entity, mask *datastorepb.PropertyMask, fetchExisting func() (*datastorepb.Entity, error)) (*datastorepb.Entity, error) {
	if mask == nil {
		return incoming, nil
	}
	existing, err := fetchExisting()
	if err != nil && status.Code(err) != codes.NotFound {
		return nil, err
	}
	merged := &datastorepb.Entity{Key: incoming.Key}
	if existing != nil {
		merged = proto.Clone(existing).(*datastorepb.Entity)
		merged.Key = incoming.Key
	}
	for _, path := range mask.Paths {
		parts, err := parseMaskPath(path)
		if err != nil {
			return nil, err
		}
		if len(parts) == 1 && parts[0] == "__key__" {
			continue
		}
		value := maskedValue(incoming, parts)
		if value != nil {
			value = proto.Clone(value).(*datastorepb.Value)
		}
		setMaskedValue(merged, incoming, parts, value)
	}
	return merged, nil
}
