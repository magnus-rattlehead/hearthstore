package server

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/status"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func (s *Server) recordOperation(method, source, path string, start time.Time, details map[string]any, err error) {
	if s == nil || s.ops == nil {
		return
	}
	e := OperationEntry{
		T:         start,
		Source:    source,
		Method:    method,
		Path:      path,
		LatencyMs: time.Since(start).Milliseconds(),
		Details:   details,
	}
	if err != nil {
		e.Err = status.Convert(err).Message()
	}
	s.ops.Add(e)
}

func (s *Server) recordWriteBatch(req *firestorepb.WriteRequest, start time.Time, err error) {
	if req == nil || len(req.GetWrites()) == 0 {
		return
	}
	s.recordOperation("WriteBatch", "firestore_stream", "", start, streamWriteDetails(req.GetWrites()), err)
}

func (s *Server) recordListenOperation(req *firestorepb.ListenRequest, start time.Time, err error) {
	if req == nil {
		return
	}
	method := "Listen"
	details := map[string]any{}
	switch tc := req.TargetChange.(type) {
	case *firestorepb.ListenRequest_AddTarget:
		method = "Listen.AddTarget"
		details = listenTargetDetails(req.GetDatabase(), tc.AddTarget)
	case *firestorepb.ListenRequest_RemoveTarget:
		method = "Listen.RemoveTarget"
		details["database"] = req.GetDatabase()
		details["target_id"] = tc.RemoveTarget
	}
	s.recordOperation(method, "firestore_stream", "", start, details, err)
}

func streamWriteDetails(writes []*firestorepb.Write) map[string]any {
	d := map[string]any{"writes": len(writes)}
	var sets, merges, deletes, transforms int
	colls := map[string]struct{}{}
	for _, w := range writes {
		switch op := w.Operation.(type) {
		case *firestorepb.Write_Update:
			if w.UpdateMask != nil && len(w.UpdateMask.FieldPaths) > 0 {
				merges++
			} else {
				sets++
			}
			addDocCollection(colls, op.Update.GetName())
		case *firestorepb.Write_Delete:
			deletes++
			addDocCollection(colls, op.Delete)
		case *firestorepb.Write_Transform:
			transforms++
			addDocCollection(colls, op.Transform.GetDocument())
		}
	}
	if sets > 0 {
		d["sets"] = sets
	}
	if merges > 0 {
		d["merges"] = merges
	}
	if deletes > 0 {
		d["deletes"] = deletes
	}
	if transforms > 0 {
		d["transforms"] = transforms
	}
	if len(colls) > 0 {
		out := make([]string, 0, len(colls))
		for c := range colls {
			out = append(out, c)
		}
		d["collections"] = out
		if len(out) == 1 {
			d["collection"] = out[0]
		}
	}
	return d
}

func listenTargetDetails(database string, target *firestorepb.Target) map[string]any {
	d := map[string]any{
		"database":  database,
		"target_id": target.GetTargetId(),
	}
	switch tt := target.GetTargetType().(type) {
	case *firestorepb.Target_Query:
		d["kind"] = "query"
		q := tt.Query
		if p := docPathFromParent(q.GetParent()); p != "" {
			d["parent"] = p
		}
		if sq := q.GetStructuredQuery(); sq != nil {
			if len(sq.From) > 0 {
				d["collection"] = sq.From[0].CollectionId
				if sq.From[0].AllDescendants {
					d["group"] = true
				}
			}
			if sq.Limit != nil {
				d["limit"] = sq.Limit.Value
			}
			if len(sq.OrderBy) > 0 {
				orders := make([]string, 0, len(sq.OrderBy))
				for _, o := range sq.OrderBy {
					part := o.Field.GetFieldPath()
					if o.Direction == firestorepb.StructuredQuery_DESCENDING {
						part += " desc"
					}
					orders = append(orders, part)
				}
				d["order_by"] = strings.Join(orders, ", ")
			}
			if sq.Where != nil {
				d["filter"] = streamFilterSummary(sq.Where)
			}
		}
	case *firestorepb.Target_Documents:
		d["kind"] = "documents"
		docs := tt.Documents.GetDocuments()
		d["docs"] = len(docs)
		paths := make([]string, 0, len(docs))
		for _, name := range docs {
			paths = append(paths, docPathFromName(name))
		}
		d["paths"] = paths
	}
	return d
}

func addDocCollection(colls map[string]struct{}, name string) {
	p := docPathFromName(name)
	if p == "" {
		return
	}
	parts := strings.Split(p, "/")
	if len(parts) >= 2 {
		colls[parts[len(parts)-2]] = struct{}{}
	}
}

func docPathFromName(name string) string {
	if _, _, path, err := parseName(name); err == nil {
		return path
	}
	return ""
}

func docPathFromParent(parent string) string {
	if _, _, path, err := parseParent(parent); err == nil {
		return path
	}
	return ""
}

func streamFilterSummary(f *firestorepb.StructuredQuery_Filter) string {
	if f == nil {
		return ""
	}
	switch ft := f.FilterType.(type) {
	case *firestorepb.StructuredQuery_Filter_CompositeFilter:
		parts := make([]string, 0, len(ft.CompositeFilter.Filters))
		for _, sub := range ft.CompositeFilter.Filters {
			if s := streamFilterSummary(sub); s != "" {
				parts = append(parts, s)
			}
		}
		op := " AND "
		if ft.CompositeFilter.Op == firestorepb.StructuredQuery_CompositeFilter_OR {
			op = " OR "
		}
		return strings.Join(parts, op)
	case *firestorepb.StructuredQuery_Filter_FieldFilter:
		ff := ft.FieldFilter
		return fmt.Sprintf("%s %s %s", ff.Field.GetFieldPath(), streamFieldFilterOp(ff.Op), streamValueSummary(ff.Value))
	case *firestorepb.StructuredQuery_Filter_UnaryFilter:
		uf := ft.UnaryFilter
		field := uf.GetField().GetFieldPath()
		return field + " " + uf.Op.String()
	}
	return ""
}

func streamFieldFilterOp(op firestorepb.StructuredQuery_FieldFilter_Operator) string {
	switch op {
	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN:
		return "<"
	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
		return "<="
	case firestorepb.StructuredQuery_FieldFilter_GREATER_THAN:
		return ">"
	case firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
		return ">="
	case firestorepb.StructuredQuery_FieldFilter_EQUAL:
		return "="
	case firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL:
		return "!="
	case firestorepb.StructuredQuery_FieldFilter_IN:
		return "IN"
	case firestorepb.StructuredQuery_FieldFilter_NOT_IN:
		return "NOT IN"
	case firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
		return "ARRAY_CONTAINS"
	case firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:
		return "ARRAY_CONTAINS_ANY"
	}
	return op.String()
}

func streamValueSummary(v *firestorepb.Value) string {
	if v == nil {
		return "null"
	}
	switch vt := v.ValueType.(type) {
	case *firestorepb.Value_NullValue:
		return "null"
	case *firestorepb.Value_BooleanValue:
		return fmt.Sprintf("%t", vt.BooleanValue)
	case *firestorepb.Value_IntegerValue:
		return fmt.Sprintf("%d", vt.IntegerValue)
	case *firestorepb.Value_DoubleValue:
		return fmt.Sprintf("%g", vt.DoubleValue)
	case *firestorepb.Value_StringValue:
		if len(vt.StringValue) > 40 {
			return fmt.Sprintf("%q...", vt.StringValue[:37])
		}
		return fmt.Sprintf("%q", vt.StringValue)
	case *firestorepb.Value_ArrayValue:
		return fmt.Sprintf("[%d values]", len(vt.ArrayValue.GetValues()))
	case *firestorepb.Value_MapValue:
		return fmt.Sprintf("{%d fields}", len(vt.MapValue.GetFields()))
	}
	return "?"
}
