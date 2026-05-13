package server

import (
	"database/sql"
	"log/slog"
	"math"
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// virtualDocIDRe matches emulator-internal IDs like __id7__ and __id-7__ that
// the Firestore SDK synthesises for virtual documents used in integration tests.
// These are allowed through even though they use double-underscore delimiters.
var virtualDocIDRe = regexp.MustCompile(`^__id-?[0-9]+__$`)

// checkDocID rejects document IDs that use the reserved __*__ namespace,
// unless they are virtual numeric IDs (__id<N>__) used by the SDK test suite.
func checkDocID(path string) error {
	_, _, docID := splitDocPath(path)
	if strings.HasPrefix(docID, "__") && strings.HasSuffix(docID, "__") && !virtualDocIDRe.MatchString(docID) {
		return status.Errorf(codes.InvalidArgument, "Document ID %q is reserved", docID)
	}
	return nil
}

// writeStoreOps abstracts the four storage methods that differ between the
// direct (non-transactional) and Tx-scoped write paths.
type writeStoreOps interface {
	checkPrecondition(project, database, path string, pre *firestorepb.Precondition) error
	getDoc(project, database, path string) (*firestorepb.Document, error)
	upsertDoc(project, database, collection, parentPath, path string, doc *firestorepb.Document) (*firestorepb.Document, error)
	deleteDoc(project, database, path string) error
}

type directOps struct{ s *storage.Store }

func (o *directOps) checkPrecondition(p, db, path string, pre *firestorepb.Precondition) error {
	return o.s.CheckPrecondition(p, db, path, pre)
}
func (o *directOps) getDoc(p, db, path string) (*firestorepb.Document, error) {
	return o.s.GetDoc(p, db, path)
}
func (o *directOps) upsertDoc(p, db, col, pp, path string, doc *firestorepb.Document) (*firestorepb.Document, error) {
	return o.s.UpsertDoc(p, db, col, pp, path, doc)
}
func (o *directOps) deleteDoc(p, db, path string) error { return o.s.DeleteDoc(p, db, path) }

type txOps struct {
	s  *storage.Store
	tx *sql.Tx
}

func (o *txOps) checkPrecondition(p, db, path string, pre *firestorepb.Precondition) error {
	return o.s.CheckPreconditionTx(o.tx, p, db, path, pre)
}
func (o *txOps) getDoc(p, db, path string) (*firestorepb.Document, error) {
	return o.s.GetDocTx(o.tx, p, db, path)
}
func (o *txOps) upsertDoc(p, db, col, pp, path string, doc *firestorepb.Document) (*firestorepb.Document, error) {
	return o.s.UpsertDocTx(o.tx, p, db, col, pp, path, doc)
}
func (o *txOps) deleteDoc(p, db, path string) error { return o.s.DeleteDocTx(o.tx, p, db, path) }

type txAccOps struct {
	s   *storage.Store
	tx  *sql.Tx
	acc *storage.FsCommitAccumulator
}

func (o *txAccOps) checkPrecondition(p, db, path string, pre *firestorepb.Precondition) error {
	return o.s.CheckPreconditionTx(o.tx, p, db, path, pre)
}
func (o *txAccOps) getDoc(p, db, path string) (*firestorepb.Document, error) {
	return o.s.GetDocTx(o.tx, p, db, path)
}
func (o *txAccOps) upsertDoc(p, db, col, pp, path string, doc *firestorepb.Document) (*firestorepb.Document, error) {
	return o.s.UpsertDocTxAcc(o.tx, p, db, col, pp, path, doc, o.acc)
}
func (o *txAccOps) deleteDoc(p, db, path string) error {
	return o.s.DeleteDocTxAcc(o.tx, p, db, path, o.acc)
}

// applyWrite executes a single Write using the non-transactional storage path.
func (s *Server) applyWrite(w *firestorepb.Write) (*firestorepb.WriteResult, error) {
	return s.applyWriteWith(&directOps{s.store}, w)
}

// applyWriteTx executes a single Write inside tx so all writes in a Commit
// are rolled back atomically if any one fails.
func (s *Server) applyWriteTx(tx *sql.Tx, w *firestorepb.Write) (*firestorepb.WriteResult, error) {
	return s.applyWriteWith(&txOps{s.store, tx}, w)
}

// applyWriteTxAcc executes a single Write inside tx, collecting ChangeEvents in acc
// for post-tx fan-out. Use in place of applyWriteTx when an accumulator is available.
func (s *Server) applyWriteTxAcc(tx *sql.Tx, w *firestorepb.Write, acc *storage.FsCommitAccumulator) (*firestorepb.WriteResult, error) {
	return s.applyWriteWith(&txAccOps{s.store, tx, acc}, w)
}

// applyWriteWith is the shared implementation used by applyWrite and applyWriteTx.
func (s *Server) applyWriteWith(ops writeStoreOps, w *firestorepb.Write) (*firestorepb.WriteResult, error) {
	if w.CurrentDocument != nil {
		switch op := w.Operation.(type) {
		case *firestorepb.Write_Update:
			project, database, path, err := parseName(op.Update.Name)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "invalid document name: %v", err)
			}
			if err := ops.checkPrecondition(project, database, path, w.CurrentDocument); err != nil {
				return nil, err
			}
		case *firestorepb.Write_Delete:
			project, database, path, err := parseName(op.Delete)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "invalid document name: %v", err)
			}
			if err := ops.checkPrecondition(project, database, path, w.CurrentDocument); err != nil {
				return nil, err
			}
		}
	}

	switch op := w.Operation.(type) {
	case *firestorepb.Write_Update:
		project, database, path, err := parseName(op.Update.Name)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid document name: %v", err)
		}
		if err := checkDocID(path); err != nil {
			return nil, err
		}
		collection, parentPath, _ := splitDocPath(path)
		name := buildDocName(project, database, path)

		// A Set without a mask (full replace) must write even if the fields map
		// is empty or all fields are transform sentinels, so that
		// doc.Set(ctx, {time: serverTimestamp}) correctly overwrites the document
		// starting from an empty base before applying the transforms.
		isFullReplace := w.UpdateMask == nil
		hasFieldUpdates := isFullReplace ||
			(w.UpdateMask != nil && len(w.UpdateMask.FieldPaths) > 0) ||
			(w.UpdateMask == nil && len(op.Update.GetFields()) > 0)

		slog.Debug("applyWrite: Update",
			"path", path,
			"has_field_updates", hasFieldUpdates,
			"update_mask", w.UpdateMask.GetFieldPaths(),
			"transforms", len(w.UpdateTransforms),
		)

		var doc *firestorepb.Document
		if hasFieldUpdates {
			if w.UpdateMask != nil && len(w.UpdateMask.FieldPaths) > 0 {
				existing, err := ops.getDoc(project, database, path)
				if err != nil {
					if status.Code(err) != codes.NotFound {
						return nil, err
					}
					// Document doesn't exist — set({}, merge:true) should create it.
					existing = &firestorepb.Document{Name: name, Fields: make(map[string]*firestorepb.Value)}
				}
				existing.Fields = applyUpdateMask(existing.Fields, op.Update.GetFields(), w.UpdateMask.FieldPaths)
				existing.Name = name
				doc = existing
			} else {
				doc = &firestorepb.Document{Name: name, Fields: op.Update.GetFields()}
			}
		} else if len(w.UpdateTransforms) > 0 {
			// Transform-only: load existing doc so transforms see the current state.
			// If the doc doesn't exist yet (e.g. doc.create() with only server timestamps),
			// start from an empty document.
			doc, err = ops.getDoc(project, database, path)
			if err != nil {
				if status.Code(err) == codes.NotFound {
					doc = &firestorepb.Document{
						Name:   buildDocName(project, database, path),
						Fields: make(map[string]*firestorepb.Value),
					}
					err = nil
				} else {
					return nil, err
				}
			}
		}

		if len(w.UpdateTransforms) > 0 {
			doc = applyFieldTransforms(doc, w.UpdateTransforms, timestamppb.Now())
		}

		if hasFieldUpdates || len(w.UpdateTransforms) > 0 {
			doc, err = ops.upsertDoc(project, database, collection, parentPath, path, doc)
			if err != nil {
				return nil, err
			}
			slog.Debug("applyWrite: upserted", "path", path, "transforms", len(w.UpdateTransforms), "update_time", doc.GetUpdateTime().AsTime())
		}

		var transformResults []*firestorepb.Value
		for _, t := range w.UpdateTransforms {
			val := getNestedField(doc.GetFields(), t.FieldPath)
			if val == nil {
				val = &firestorepb.Value{}
			}
			transformResults = append(transformResults, val)
		}
		var updateTime *timestamppb.Timestamp
		if doc != nil {
			updateTime = doc.UpdateTime
		} else {
			updateTime = timestamppb.Now() // no-op write (e.g. Set({}, MergeAll) with empty mask)
		}
		return &firestorepb.WriteResult{UpdateTime: updateTime, TransformResults: transformResults}, nil

	case *firestorepb.Write_Delete:
		project, database, path, err := parseName(op.Delete)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid document name: %v", err)
		}
		if err := ops.deleteDoc(project, database, path); err != nil {
			return nil, err
		}
		return &firestorepb.WriteResult{}, nil

	case *firestorepb.Write_Transform:
		// Legacy whole-document transform — apply field transforms to existing doc.
		dt := op.Transform
		project, database, path, err := parseName(dt.Document)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid document name: %v", err)
		}
		existing, err := ops.getDoc(project, database, path)
		if err != nil {
			if status.Code(err) != codes.NotFound {
				return nil, err
			}
			existing = &firestorepb.Document{
				Name:   buildDocName(project, database, path),
				Fields: make(map[string]*firestorepb.Value),
			}
		}
		collection, parentPath, _ := splitDocPath(path)
		existing = applyFieldTransforms(existing, dt.FieldTransforms, timestamppb.Now())
		existing, err = ops.upsertDoc(project, database, collection, parentPath, path, existing)
		if err != nil {
			return nil, err
		}
		var transformResults []*firestorepb.Value
		for _, t := range dt.FieldTransforms {
			val := getNestedField(existing.GetFields(), t.FieldPath)
			if val == nil {
				val = &firestorepb.Value{}
			}
			transformResults = append(transformResults, val)
		}
		return &firestorepb.WriteResult{UpdateTime: existing.UpdateTime, TransformResults: transformResults}, nil

	default:
		return nil, status.Error(codes.InvalidArgument, "unknown write operation type")
	}
}

// applyFieldTransforms applies a list of FieldTransforms to doc in-place (on a copy).
func applyFieldTransforms(doc *firestorepb.Document, transforms []*firestorepb.DocumentTransform_FieldTransform, now *timestamppb.Timestamp) *firestorepb.Document {
	if doc.Fields == nil {
		doc.Fields = make(map[string]*firestorepb.Value)
	}
	for _, t := range transforms {
		fp := t.FieldPath
		current := getNestedField(doc.Fields, fp)

		var newVal *firestorepb.Value
		switch tt := t.TransformType.(type) {
		case *firestorepb.DocumentTransform_FieldTransform_SetToServerValue:
			if tt.SetToServerValue == firestorepb.DocumentTransform_FieldTransform_REQUEST_TIME {
				newVal = &firestorepb.Value{
					ValueType: &firestorepb.Value_TimestampValue{TimestampValue: now},
				}
			}

		case *firestorepb.DocumentTransform_FieldTransform_Increment:
			newVal = fsNumericOp(current, tt.Increment, func(a, b float64) float64 { return a + b })

		case *firestorepb.DocumentTransform_FieldTransform_Maximum:
			newVal = fsNumericOp(current, tt.Maximum, func(a, b float64) float64 {
				if a >= b {
					return a
				}
				return b
			})

		case *firestorepb.DocumentTransform_FieldTransform_Minimum:
			newVal = fsNumericOp(current, tt.Minimum, func(a, b float64) float64 {
				if a <= b {
					return a
				}
				return b
			})

		case *firestorepb.DocumentTransform_FieldTransform_AppendMissingElements:
			newVal = fsAppendMissing(current, tt.AppendMissingElements)

		case *firestorepb.DocumentTransform_FieldTransform_RemoveAllFromArray:
			newVal = fsRemoveFromArray(current, tt.RemoveAllFromArray)
		}

		if newVal != nil {
			setNestedField(doc.Fields, fp, newVal)
		}
	}
	return doc
}

// splitFieldPath splits a Firestore field path at its first segment boundary,
// respecting backtick-quoted segments (which may contain literal dots).
// Returns (head, tail, nested) where head is the unescaped first segment,
// tail is the remainder, and nested is true when there are more segments.
func splitFieldPath(path string) (head, tail string, nested bool) {
	if len(path) == 0 {
		return "", "", false
	}
	if path[0] == '`' {
		// Backtick-quoted segment: scan for closing backtick, honouring \\ and \` escapes.
		// The Firestore SDK encodes: \ → \\ and ` → \` inside backtick segments.
		i := 1
		for i < len(path) {
			if path[i] == '\\' && i+1 < len(path) {
				if path[i+1] == '`' || path[i+1] == '\\' {
					i += 2 // skip escape sequence (\` or \\)
					continue
				}
			}
			if path[i] == '`' {
				break
			}
			i++
		}
		rawName := path[1:i]
		// Un-escape: process left-to-right, handling \\ → \ and \` → `
		var unescBuf strings.Builder
		unescBuf.Grow(len(rawName))
		for j := 0; j < len(rawName); {
			if rawName[j] == '\\' && j+1 < len(rawName) {
				switch rawName[j+1] {
				case '\\':
					unescBuf.WriteByte('\\')
					j += 2
				case '`':
					unescBuf.WriteByte('`')
					j += 2
				default:
					unescBuf.WriteByte(rawName[j])
					j++
				}
			} else {
				unescBuf.WriteByte(rawName[j])
				j++
			}
		}
		head = unescBuf.String()
		rest := ""
		if i+1 < len(path) {
			rest = path[i+1:]
		}
		if len(rest) > 0 && rest[0] == '.' {
			return head, rest[1:], true
		}
		return head, rest, false
	}
	// Unquoted segment: split on first dot.
	idx := strings.IndexByte(path, '.')
	if idx < 0 {
		return path, "", false
	}
	return path[:idx], path[idx+1:], true
}

// getNestedField reads the value at a dotted field path, traversing into MapValues.
func getNestedField(fields map[string]*firestorepb.Value, path string) *firestorepb.Value {
	head, tail, nested := splitFieldPath(path)
	v := fields[head]
	if !nested {
		return v
	}
	mv := v.GetMapValue()
	if mv == nil {
		return nil
	}
	return getNestedField(mv.Fields, tail)
}

// setNestedField writes val at a dotted field path, creating intermediate MapValues as needed.
func setNestedField(fields map[string]*firestorepb.Value, path string, val *firestorepb.Value) {
	head, tail, nested := splitFieldPath(path)
	if !nested {
		fields[head] = val
		return
	}
	// Clone the intermediate map so we don't mutate stored protos.
	var subFields map[string]*firestorepb.Value
	if mv := fields[head].GetMapValue(); mv != nil {
		subFields = make(map[string]*firestorepb.Value, len(mv.Fields))
		for k, v := range mv.Fields {
			subFields[k] = v
		}
	} else {
		subFields = make(map[string]*firestorepb.Value)
	}
	setNestedField(subFields, tail, val)
	fields[head] = &firestorepb.Value{
		ValueType: &firestorepb.Value_MapValue{
			MapValue: &firestorepb.MapValue{Fields: subFields},
		},
	}
}

func fsNumericOp(current, operand *firestorepb.Value, op func(a, b float64) float64) *firestorepb.Value {
	var a float64
	if current != nil {
		a = numericFloat(current)
	}
	b := numericFloat(operand)
	result := op(a, b)

	_, aIsInt := current.GetValueType().(*firestorepb.Value_IntegerValue)
	_, bIsInt := operand.GetValueType().(*firestorepb.Value_IntegerValue)
	if (current == nil || aIsInt) && bIsInt && !math.IsInf(result, 0) && !math.IsNaN(result) {
		return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: int64(result)}}
	}
	return &firestorepb.Value{ValueType: &firestorepb.Value_DoubleValue{DoubleValue: result}}
}

func fsAppendMissing(current *firestorepb.Value, toAdd *firestorepb.ArrayValue) *firestorepb.Value {
	existing := []*firestorepb.Value{}
	if av := current.GetArrayValue(); av != nil {
		existing = av.Values
	}
	out := make([]*firestorepb.Value, len(existing))
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
	return &firestorepb.Value{ValueType: &firestorepb.Value_ArrayValue{ArrayValue: &firestorepb.ArrayValue{Values: out}}}
}

func fsRemoveFromArray(current *firestorepb.Value, toRemove *firestorepb.ArrayValue) *firestorepb.Value {
	av := current.GetArrayValue()
	if av == nil {
		return current
	}
	var out []*firestorepb.Value
	for _, e := range av.Values {
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
	return &firestorepb.Value{ValueType: &firestorepb.Value_ArrayValue{ArrayValue: &firestorepb.ArrayValue{Values: out}}}
}

// applyMaskField merges a single field-path (possibly dot-separated) from src into dst.
// If the path is absent in src it is deleted from dst instead.
func applyMaskField(dst, src map[string]*firestorepb.Value, fieldPath string) {
	head, tail, nested := splitFieldPath(fieldPath)
	if !nested {
		// Top-level field: set or delete directly.
		if v, ok := src[head]; ok {
			dst[head] = v
		} else {
			delete(dst, head)
		}
		return
	}
	// Nested: ensure dst has a MapValue at head, then recurse.
	dstMap := dst[head].GetMapValue()
	if dstMap == nil {
		dstMap = &firestorepb.MapValue{Fields: make(map[string]*firestorepb.Value)}
	} else {
		// Clone so we don't mutate the stored proto.
		fields := make(map[string]*firestorepb.Value, len(dstMap.Fields))
		for k, v := range dstMap.Fields {
			fields[k] = v
		}
		dstMap = &firestorepb.MapValue{Fields: fields}
	}
	srcMap := src[head].GetMapValue()
	var srcFields map[string]*firestorepb.Value
	if srcMap != nil {
		srcFields = srcMap.Fields
	}
	applyMaskField(dstMap.Fields, srcFields, tail)
	dst[head] = &firestorepb.Value{ValueType: &firestorepb.Value_MapValue{MapValue: dstMap}}
}

// applyUpdateMask applies the UpdateMask field paths from update into existing, returning the merged fields.
func applyUpdateMask(existing, update map[string]*firestorepb.Value, maskPaths []string) map[string]*firestorepb.Value {
	out := make(map[string]*firestorepb.Value, len(existing))
	for k, v := range existing {
		out[k] = v
	}
	for _, fp := range maskPaths {
		applyMaskField(out, update, fp)
	}
	return out
}
