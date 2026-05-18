package server

import (
	"database/sql"
	"errors"
	"math"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/magnus-rattlehead/hearthstore/internal/query"
)

func (s *Server) RunAggregationQuery(req *firestorepb.RunAggregationQueryRequest, stream firestorepb.Firestore_RunAggregationQueryServer) error {
	sq, ok := req.QueryType.(*firestorepb.RunAggregationQueryRequest_StructuredAggregationQuery)
	if !ok {
		return status.Error(codes.Unimplemented, "only StructuredAggregationQuery is supported")
	}
	aq := sq.StructuredAggregationQuery
	if aq == nil {
		return status.Error(codes.InvalidArgument, "structured_aggregation_query is required")
	}

	inner, ok := aq.QueryType.(*firestorepb.StructuredAggregationQuery_StructuredQuery)
	if !ok {
		return status.Error(codes.Unimplemented, "only StructuredQuery is supported inside aggregation")
	}
	q := inner.StructuredQuery

	project, database, parentPath, err := parseParent(req.Parent)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid parent: %v", err)
	}

	// Handle inline transaction: register a new transaction if requested and
	// include the transaction ID in the response.
	var txID []byte
	var readAt *time.Time
	switch cs := req.ConsistencySelector.(type) {
	case *firestorepb.RunAggregationQueryRequest_NewTransaction:
		id := newAutoID()
		entry := txEntry{}
		if cs.NewTransaction != nil {
			_, entry.readOnly = cs.NewTransaction.Mode.(*firestorepb.TransactionOptions_ReadOnly_)
		}
		s.txMu.Lock()
		s.txns[id] = entry
		s.txMu.Unlock()
		txID = []byte(id)
	case *firestorepb.RunAggregationQueryRequest_Transaction:
		txID = cs.Transaction
		s.txMu.Lock()
		if entry, ok := s.txns[string(cs.Transaction)]; ok && entry.readTime != nil {
			t := entry.readTime.AsTime()
			readAt = &t
		}
		s.txMu.Unlock()
	case *firestorepb.RunAggregationQueryRequest_ReadTime:
		if cs.ReadTime != nil {
			t := cs.ReadTime.AsTime()
			readAt = &t
		}
	}

	explainOpts := req.GetExplainOptions()

	// Plan-only explain: return plan without executing. Per spec, no readTime is sent.
	if explainOpts != nil && !explainOpts.GetAnalyze() {
		effectiveOrders := query.AddImplicitOrderBy(q.GetOrderBy(), q.GetWhere())
		plan := buildPlanSummary(effectiveOrders)
		return stream.Send(&firestorepb.RunAggregationQueryResponse{
			ExplainMetrics: &firestorepb.ExplainMetrics{PlanSummary: plan},
		})
	}

	start := time.Now()

	// Read-time path: use streaming aggregation over historical snapshot.
	if readAt != nil {
		return s.streamingAggregationAsOf(project, database, parentPath, q, aq.Aggregations, txID, stream, explainOpts, start, *readAt)
	}

	// Attempt SQL push-down. Falls back to streaming if any filter requires Go
	// evaluation (IS_NAN, GeoPoint).
	fields, err := s.sqlAggregation(project, database, parentPath, q, aq.Aggregations)
	if err != nil {
		return s.streamingAggregation(project, database, parentPath, q, aq.Aggregations, txID, stream, explainOpts, start)
	}

	resp := &firestorepb.RunAggregationQueryResponse{
		Result:      &firestorepb.AggregationResult{AggregateFields: fields},
		ReadTime:    timestamppb.Now(),
		Transaction: txID,
	}
	if explainOpts != nil && explainOpts.GetAnalyze() {
		effectiveOrders := query.AddImplicitOrderBy(q.GetOrderBy(), q.GetWhere())
		plan := buildPlanSummary(effectiveOrders)
		resp.ExplainMetrics = buildExplainMetrics(plan, 1, time.Since(start))
	}
	return stream.Send(resp)
}

// sqlAggregation runs all aggregations via SQL. Returns an error if any
// aggregation's filter has NeedsGoFilter set (caller should fall back to streaming).
func (s *Server) sqlAggregation(project, database, parentPath string, q *firestorepb.StructuredQuery, aggs []*firestorepb.StructuredAggregationQuery_Aggregation) (map[string]*firestorepb.Value, error) {
	fields := make(map[string]*firestorepb.Value, len(aggs))

	// Collect all field paths referenced by sum/avg aggregations. Firestore requires
	// every document in a multi-aggregation query to have numeric values for ALL such
	// fields - count() and other aggregations use the same filtered document set.
	var requiredFields []string
	seen := make(map[string]bool)
	for _, agg := range aggs {
		var fp string
		switch op := agg.Operator.(type) {
		case *firestorepb.StructuredAggregationQuery_Aggregation_Sum_:
			fp = op.Sum.GetField().GetFieldPath()
		case *firestorepb.StructuredAggregationQuery_Aggregation_Avg_:
			fp = op.Avg.GetField().GetFieldPath()
		}
		if fp != "" && !seen[fp] {
			requiredFields = append(requiredFields, fp)
			seen[fp] = true
		}
	}

	for _, sel := range q.GetFrom() {
		// selQ mirrors the base query with a single From selector. All constraints
		// (OrderBy, cursors, Limit, Offset) are propagated so that COUNT, SUM, and AVG
		// all operate on the same filtered document set.
		selQ := &firestorepb.StructuredQuery{
			From:    []*firestorepb.StructuredQuery_CollectionSelector{sel},
			Where:   q.GetWhere(),
			OrderBy: q.GetOrderBy(),
			Limit:   q.GetLimit(),
			Offset:  q.GetOffset(),
			StartAt: q.GetStartAt(),
			EndAt:   q.GetEndAt(),
		}

		for _, agg := range aggs {
			alias := agg.Alias

			switch op := agg.Operator.(type) {

			case *firestorepb.StructuredAggregationQuery_Aggregation_Count_:
				upTo := op.Count.GetUpTo().GetValue()
				r, err := query.BuildCount(project, database, parentPath, sel.AllDescendants, selQ, upTo, requiredFields)
				if err != nil {
					return nil, err
				}
				if r.NeedsGoFilter {
					return nil, errors.New("NeedsGoFilter")
				}
				var n int64
				if err := s.store.QueryRow(r.SQL, r.Args, &n); err != nil {
					return nil, err
				}
				if upTo > 0 && n > upTo {
					n = upTo
				}
				if existing, ok := fields[alias]; ok {
					n += existing.GetIntegerValue()
				}
				fields[alias] = aggInt(n)

			case *firestorepb.StructuredAggregationQuery_Aggregation_Sum_:
				fieldPath := op.Sum.GetField().GetFieldPath()
				r, err := query.BuildNumericAgg(project, database, parentPath, sel.AllDescendants, selQ, fieldPath, requiredFields)
				if err != nil {
					return nil, err
				}
				if r.NeedsGoFilter {
					return nil, errors.New("NeedsGoFilter")
				}
				var res query.NumericAggResult
				if err := s.store.QueryRow(r.SQL, r.Args, &res.TotalSum, &res.NumericCount, &res.DoubleCount, &res.NaNCount); err != nil && !errors.Is(err, sql.ErrNoRows) {
					return nil, err
				}
				if res.NaNCount > 0 {
					fields[alias] = aggDouble(math.NaN())
					continue
				}
				// Accumulate across multiple From selectors.
				if existing, ok := fields[alias]; ok {
					res.TotalSum += numericFloat(existing)
					if _, isInt := existing.ValueType.(*firestorepb.Value_IntegerValue); isInt && res.DoubleCount == 0 {
						res.DoubleCount = 0 // still all-int
					} else {
						res.DoubleCount = 1 // force double result
					}
				}
				fields[alias] = numericAggToSum(&res)

			case *firestorepb.StructuredAggregationQuery_Aggregation_Avg_:
				fieldPath := op.Avg.GetField().GetFieldPath()
				r, err := query.BuildNumericAgg(project, database, parentPath, sel.AllDescendants, selQ, fieldPath, requiredFields)
				if err != nil {
					return nil, err
				}
				if r.NeedsGoFilter {
					return nil, errors.New("NeedsGoFilter")
				}
				var res query.NumericAggResult
				if err := s.store.QueryRow(r.SQL, r.Args, &res.TotalSum, &res.NumericCount, &res.DoubleCount, &res.NaNCount); err != nil && !errors.Is(err, sql.ErrNoRows) {
					return nil, err
				}
				if res.NaNCount > 0 {
					fields[alias] = aggDouble(math.NaN())
					continue
				}
				// For AVG across multiple selectors we accumulate sum+count.
				if existing, ok := fields[alias]; ok {
					// fields[alias] is a MapValue encoding {sum, count} - but since we
					// can't store intermediate state in a *Value cleanly, re-query is
					// simpler. In practice From always has one entry.
					_ = existing
				}
				fields[alias] = numericAggToAvg(&res)

			default:
				return nil, status.Error(codes.Unimplemented, "unsupported aggregation operator")
			}
		}
	}
	return fields, nil
}

func numericAggToSum(r *query.NumericAggResult) *firestorepb.Value {
	if r.NumericCount == 0 {
		return aggInt(0)
	}
	if r.DoubleCount == 0 {
		return aggInt(int64(r.TotalSum))
	}
	return aggDouble(r.TotalSum)
}

func numericAggToAvg(r *query.NumericAggResult) *firestorepb.Value {
	if r.NumericCount == 0 {
		return aggNull()
	}
	avg := r.TotalSum / float64(r.NumericCount)
	if math.IsNaN(avg) {
		return aggNull()
	}
	return aggDouble(avg)
}

// streamingAggregation is the fallback O(n) path for queries whose filters
// cannot be fully expressed in SQL (IS_NAN, GeoPoint).
func (s *Server) streamingAggregation(project, database, parentPath string, q *firestorepb.StructuredQuery, aggs []*firestorepb.StructuredAggregationQuery_Aggregation, txID []byte, stream firestorepb.Firestore_RunAggregationQueryServer, explainOpts *firestorepb.ExplainOptions, start time.Time) error {
	type updateFn func(*firestorepb.Document)
	type resultFn func() *firestorepb.Value

	updates := make([]updateFn, len(aggs))
	getResults := make([]resultFn, len(aggs))
	aliases := make([]string, len(aggs))

	for i, agg := range aggs {
		aliases[i] = agg.Alias
		switch op := agg.Operator.(type) {

		case *firestorepb.StructuredAggregationQuery_Aggregation_Count_:
			var n int64
			max := op.Count.GetUpTo().GetValue()
			updates[i] = func(_ *firestorepb.Document) {
				if max == 0 || n < max {
					n++
				}
			}
			getResults[i] = func() *firestorepb.Value { return aggInt(n) }

		case *firestorepb.StructuredAggregationQuery_Aggregation_Sum_:
			field := op.Sum.GetField().GetFieldPath()
			var sum float64
			allInt, hasAny := true, false
			updates[i] = func(doc *firestorepb.Document) {
				v := getField(doc, field)
				if v == nil || !isNumeric(v) {
					return
				}
				if _, ok := v.ValueType.(*firestorepb.Value_IntegerValue); !ok {
					allInt = false
				}
				sum += numericFloat(v)
				hasAny = true
			}
			getResults[i] = func() *firestorepb.Value {
				if !hasAny {
					return aggInt(0)
				}
				if allInt {
					return aggInt(int64(sum))
				}
				return aggDouble(sum)
			}

		case *firestorepb.StructuredAggregationQuery_Aggregation_Avg_:
			field := op.Avg.GetField().GetFieldPath()
			var sum float64
			var count int
			updates[i] = func(doc *firestorepb.Document) {
				v := getField(doc, field)
				if v == nil || !isNumeric(v) {
					return
				}
				sum += numericFloat(v)
				count++
			}
			getResults[i] = func() *firestorepb.Value {
				if count == 0 {
					return aggNull()
				}
				return aggDouble(sum / float64(count)) // NaN propagates if any field was NaN
			}

		default:
			return status.Error(codes.Unimplemented, "unsupported aggregation operator")
		}
	}

	for _, sel := range q.GetFrom() {
		selQ := &firestorepb.StructuredQuery{
			From:    []*firestorepb.StructuredQuery_CollectionSelector{sel},
			Where:   q.GetWhere(),
			OrderBy: q.GetOrderBy(),
			Limit:   q.GetLimit(),
			Offset:  q.GetOffset(),
			StartAt: q.GetStartAt(),
			EndAt:   q.GetEndAt(),
		}
		result, err := query.Build(project, database, parentPath, sel.AllDescendants, selQ)
		if err != nil {
			return status.Errorf(codes.Internal, "query build: %v", err)
		}
		err = s.store.QueryDocs(result.SQL, result.Args, func(doc *firestorepb.Document) error {
			if result.NeedsGoFilter && !matchesFilter(doc, q.GetWhere()) {
				return nil
			}
			for _, update := range updates {
				update(doc)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	aggregateFields := make(map[string]*firestorepb.Value, len(aliases))
	for i, alias := range aliases {
		aggregateFields[alias] = getResults[i]()
	}

	resp := &firestorepb.RunAggregationQueryResponse{
		Result:      &firestorepb.AggregationResult{AggregateFields: aggregateFields},
		ReadTime:    timestamppb.Now(),
		Transaction: txID,
	}
	if explainOpts != nil && explainOpts.GetAnalyze() {
		effectiveOrders := query.AddImplicitOrderBy(q.GetOrderBy(), q.GetWhere())
		plan := buildPlanSummary(effectiveOrders)
		resp.ExplainMetrics = buildExplainMetrics(plan, 1, time.Since(start))
	}
	return stream.Send(resp)
}

// streamingAggregationAsOf runs aggregation over a historical snapshot (readAt path).
func (s *Server) streamingAggregationAsOf(project, database, parentPath string, q *firestorepb.StructuredQuery, aggs []*firestorepb.StructuredAggregationQuery_Aggregation, txID []byte, stream firestorepb.Firestore_RunAggregationQueryServer, explainOpts *firestorepb.ExplainOptions, start time.Time, readAt time.Time) error {
	type updateFn func(*firestorepb.Document)
	type resultFn func() *firestorepb.Value

	updates := make([]updateFn, len(aggs))
	getResults := make([]resultFn, len(aggs))
	aliases := make([]string, len(aggs))

	for i, agg := range aggs {
		aliases[i] = agg.Alias
		switch op := agg.Operator.(type) {
		case *firestorepb.StructuredAggregationQuery_Aggregation_Count_:
			var n int64
			max := op.Count.GetUpTo().GetValue()
			updates[i] = func(_ *firestorepb.Document) {
				if max == 0 || n < max {
					n++
				}
			}
			getResults[i] = func() *firestorepb.Value { return aggInt(n) }
		case *firestorepb.StructuredAggregationQuery_Aggregation_Sum_:
			field := op.Sum.GetField().GetFieldPath()
			var sum float64
			allInt, hasAny := true, false
			updates[i] = func(doc *firestorepb.Document) {
				v := getField(doc, field)
				if v == nil || !isNumeric(v) {
					return
				}
				if _, ok := v.ValueType.(*firestorepb.Value_IntegerValue); !ok {
					allInt = false
				}
				sum += numericFloat(v)
				hasAny = true
			}
			getResults[i] = func() *firestorepb.Value {
				if !hasAny {
					return aggInt(0)
				}
				if allInt {
					return aggInt(int64(sum))
				}
				return aggDouble(sum)
			}
		case *firestorepb.StructuredAggregationQuery_Aggregation_Avg_:
			field := op.Avg.GetField().GetFieldPath()
			var sum float64
			var count int
			updates[i] = func(doc *firestorepb.Document) {
				v := getField(doc, field)
				if v == nil || !isNumeric(v) {
					return
				}
				sum += numericFloat(v)
				count++
			}
			getResults[i] = func() *firestorepb.Value {
				if count == 0 {
					return aggNull()
				}
				return aggDouble(sum / float64(count))
			}
		default:
			return status.Error(codes.Unimplemented, "unsupported aggregation operator")
		}
	}

	for _, sel := range q.GetFrom() {
		docs, err := s.store.ListDocsAsOf(project, database, sel.CollectionId, parentPath, sel.AllDescendants, readAt)
		if err != nil {
			return err
		}
		for _, doc := range docs {
			if !matchesFilter(doc, q.GetWhere()) {
				continue
			}
			for _, update := range updates {
				update(doc)
			}
		}
	}

	aggregateFields := make(map[string]*firestorepb.Value, len(aliases))
	for i, alias := range aliases {
		aggregateFields[alias] = getResults[i]()
	}

	resp := &firestorepb.RunAggregationQueryResponse{
		Result:      &firestorepb.AggregationResult{AggregateFields: aggregateFields},
		ReadTime:    timestamppb.Now(),
		Transaction: txID,
	}
	if explainOpts != nil && explainOpts.GetAnalyze() {
		effectiveOrders := query.AddImplicitOrderBy(q.GetOrderBy(), q.GetWhere())
		plan := buildPlanSummary(effectiveOrders)
		resp.ExplainMetrics = buildExplainMetrics(plan, 1, time.Since(start))
	}
	return stream.Send(resp)
}

func aggInt(n int64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: n}}
}

func aggDouble(f float64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_DoubleValue{DoubleValue: f}}
}

func aggNull() *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_NullValue{}}
}
