package datastore

import (
	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"context"
	"github.com/magnus-rattlehead/hearthstore/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"slices"
	"unicode/utf8"
)

func validateKeyScope(key *datastorepb.Key, project, database string, incomplete bool) error {
	if key == nil || len(key.Path) == 0 {
		return status.Error(codes.InvalidArgument, "key path is required")
	}
	if proto.Size(key) > maxKeyBytes {
		return status.Error(codes.InvalidArgument, "key exceeds the Datastore 6 KiB limit")
	}
	if len(key.Path) > maxKeyPathElements {
		return status.Error(codes.InvalidArgument, "key path exceeds 100 elements")
	}
	partition := key.GetPartitionId()
	if partition.GetProjectId() != "" && partition.GetProjectId() != project {
		return status.Error(codes.InvalidArgument, "key belongs to another project")
	}
	if partition.GetDatabaseId() != "" && partition.GetDatabaseId() != database {
		return status.Error(codes.InvalidArgument, "key belongs to another database")
	}
	for i, part := range key.Path {
		if part.GetKind() == "" || len(part.GetKind()) > maxNameBytes || !utf8.ValidString(part.GetKind()) {
			return status.Error(codes.InvalidArgument, "key kind must be valid UTF-8 and contain 1 to 1500 bytes")
		}
		switch id := part.IdType.(type) {
		case *datastorepb.Key_PathElement_Name:
			if id.Name == "" || len(id.Name) > maxNameBytes || !utf8.ValidString(id.Name) {
				return status.Error(codes.InvalidArgument, "key name must be valid UTF-8 and contain 1 to 1500 bytes")
			}
		case *datastorepb.Key_PathElement_Id:
			if id.Id == 0 {
				return status.Error(codes.InvalidArgument, "key ID must not be zero")
			}
		}
		if part.GetId() == 0 && part.GetName() == "" && !(incomplete && i == len(key.Path)-1) {
			return status.Error(codes.InvalidArgument, "complete key path is required")
		}
	}
	return nil
}

// scopedKey never changes the caller's protobuf message.
func scopedKey(key *datastorepb.Key, project, database string) *datastorepb.Key {
	copyKey := proto.Clone(key).(*datastorepb.Key)
	if copyKey.PartitionId == nil {
		copyKey.PartitionId = &datastorepb.PartitionId{}
	}
	if copyKey.PartitionId.ProjectId == "" {
		copyKey.PartitionId.ProjectId = project
	}
	if copyKey.PartitionId.DatabaseId == "" && database != defaultDatabase {
		copyKey.PartitionId.DatabaseId = database
	}
	return copyKey
}

func validateQuery(ctx context.Context, query *datastorepb.Query, namespace string) error {
	if query.Offset < 0 || query.GetLimit().GetValue() < 0 {
		return status.Error(codes.InvalidArgument, "query offset and limit must be non-negative")
	}
	if len(query.Kind) > 1 {
		return status.Error(codes.InvalidArgument, "only one query kind is supported")
	}
	for _, kind := range query.Kind {
		if kind.GetName() == "" {
			return status.Error(codes.InvalidArgument, "query kind name is required")
		}
	}
	for _, order := range query.Order {
		if order.GetProperty().GetName() == "" {
			return status.Error(codes.InvalidArgument, "order property is required")
		}
	}
	projected := make(map[string]bool, len(query.Projection))
	for _, field := range query.Projection {
		if field.GetProperty().GetName() == "" {
			return status.Error(codes.InvalidArgument, "projection property is required")
		}
		if projected[field.Property.Name] {
			return status.Error(codes.InvalidArgument, "the same property cannot be projected more than once")
		}
		projected[field.Property.Name] = true
	}
	distinct := make(map[string]bool, len(query.DistinctOn))
	for _, field := range query.DistinctOn {
		if field.GetName() == "" {
			return status.Error(codes.InvalidArgument, "distinct property is required")
		}
		distinct[field.Name] = true
	}
	if len(query.Order) > 0 && len(distinct) > 0 {
		remaining := len(distinct)
		for _, order := range query.Order {
			if remaining == 0 {
				break
			}
			name := order.Property.Name
			if !distinct[name] {
				return status.Error(codes.InvalidArgument, "distinct properties must precede other order properties")
			}
			delete(distinct, name)
			remaining--
		}
		if remaining != 0 {
			return status.Error(codes.InvalidArgument, "all distinct properties must appear first in query order")
		}
	}
	if query.Filter != nil {
		if err := validateQueryFilter(ctx, query.Filter, namespace, projected); err != nil {
			return err
		}
	}
	if err := validateMetadataQuery(ctx, query); err != nil {
		return err
	}
	return validateQueryComplexity(ctx, query)
}

func validateMetadataQuery(ctx context.Context, query *datastorepb.Query) error {
	if len(query.Kind) == 0 || !isMetadataKind(query.Kind[0].Name) {
		return nil
	}
	for _, order := range query.Order {
		if order.Property.Name != "__key__" || order.Direction == datastorepb.PropertyOrder_DESCENDING {
			return status.Error(codes.InvalidArgument, "metadata queries support only ascending key order")
		}
	}
	work := storage.QueryWorkFromContext(ctx)
	stack := []*datastorepb.Filter{query.Filter}
	for len(stack) > 0 {
		if err := work.Checkpoint(ctx); err != nil {
			return err
		}
		filter := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if property := filter.GetPropertyFilter(); property != nil {
			if property.Property.Name != "__key__" {
				return status.Error(codes.InvalidArgument, "metadata queries support only key filters")
			}
			if property.Op == datastorepb.PropertyFilter_HAS_ANCESTOR && query.Kind[0].Name != "__property__" {
				return status.Error(codes.InvalidArgument, "ancestor metadata queries require __property__")
			}
		} else if composite := filter.GetCompositeFilter(); composite != nil {
			stack = append(stack, composite.Filters...)
		}
	}
	return nil
}

// prepareQuery validates limits and removes repeated operands before access
// planning as well as execution. Membership operators retain their array semantics.
func prepareQuery(query *datastorepb.Query, namespace string) (*datastorepb.Query, error) {
	prepared, _, err := prepareQueryCondition(query, namespace)
	return prepared, err
}

func prepareQueryCondition(query *datastorepb.Query, namespace string) (*datastorepb.Query, *compiledCondition, error) {
	return prepareQueryConditionContext(context.Background(), query, namespace)
}

func prepareQueryConditionContext(ctx context.Context, query *datastorepb.Query, namespace string) (*datastorepb.Query, *compiledCondition, error) {
	if err := validateQuery(ctx, query, namespace); err != nil {
		return nil, nil, err
	}
	condition, err := compileConditionGraph(ctx, query.Filter, 30, false)
	if err != nil {
		return nil, nil, err
	}
	prepared := proto.Clone(query).(*datastorepb.Query)
	prepared.Filter = condition.filter()
	return prepared, condition, nil
}

func validateQueryFilter(ctx context.Context, filter *datastorepb.Filter, namespace string, projected map[string]bool) error {
	work := storage.QueryWorkFromContext(ctx)
	stack := []*datastorepb.Filter{filter}
	var exclusions map[string]struct{}
	hasIN, hasNOTIN, hasOR := false, false, false
	for len(stack) > 0 {
		if err := work.Checkpoint(ctx); err != nil {
			return err
		}
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if err := validateQueryFilterNode(current, namespace); err != nil {
			return err
		}
		if property := current.GetPropertyFilter(); property != nil {
			if projected[property.Property.Name] && (property.Op == datastorepb.PropertyFilter_EQUAL || property.Op == datastorepb.PropertyFilter_IN) {
				return status.Error(codes.InvalidArgument, "Cannot use projection on a property with an equality filter.")
			}
			hasIN = hasIN || property.Op == datastorepb.PropertyFilter_IN
			hasNOTIN = hasNOTIN || property.Op == datastorepb.PropertyFilter_NOT_IN
			if property.Op == datastorepb.PropertyFilter_NOT_IN || property.Op == datastorepb.PropertyFilter_NOT_EQUAL {
				canonical, err := canonicalMembership(property)
				if err != nil {
					return status.Error(codes.InvalidArgument, "invalid query filter encoding")
				}
				encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(canonical)
				if err != nil {
					return status.Error(codes.InvalidArgument, "invalid query filter encoding")
				}
				if exclusions == nil {
					exclusions = make(map[string]struct{})
				}
				exclusions[string(encoded)] = struct{}{}
			}
		}
		hasOR = hasOR || current.GetCompositeFilter().GetOp() == datastorepb.CompositeFilter_OR
		children := current.GetCompositeFilter().GetFilters()
		for i := len(children) - 1; i >= 0; i-- {
			stack = append(stack, children[i])
		}
	}
	if len(exclusions) > 1 {
		return status.Error(codes.InvalidArgument, "only one NOT_EQUAL or NOT_IN filter is allowed")
	}
	if hasNOTIN && (hasIN || hasOR) {
		return status.Error(codes.InvalidArgument, "NOT_IN cannot be combined with IN or OR")
	}
	return nil
}

func validateQueryFilterNode(filter *datastorepb.Filter, namespace string) error {
	switch typed := filter.GetFilterType().(type) {
	case *datastorepb.Filter_PropertyFilter:
		property := typed.PropertyFilter
		if property.GetProperty().GetName() == "" || property.GetValue() == nil || property.GetValue().ValueType == nil {
			return status.Error(codes.InvalidArgument, "filter property and value are required")
		}
		if property.Value.GetEntityValue() != nil {
			return status.Error(codes.InvalidArgument, "an entity value is not allowed in a filter")
		}
		switch property.Op {
		case datastorepb.PropertyFilter_LESS_THAN, datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL, datastorepb.PropertyFilter_GREATER_THAN, datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL, datastorepb.PropertyFilter_EQUAL, datastorepb.PropertyFilter_NOT_EQUAL:
			if property.Value.GetArrayValue() != nil {
				return status.Error(codes.InvalidArgument, "a list value is not allowed for this operator")
			}
		case datastorepb.PropertyFilter_HAS_ANCESTOR:
			if property.Property.Name != "__key__" || property.Value.GetKeyValue() == nil {
				return status.Error(codes.InvalidArgument, "ancestor filter requires a key")
			}
		case datastorepb.PropertyFilter_IN, datastorepb.PropertyFilter_NOT_IN:
			if len(property.Value.GetArrayValue().GetValues()) == 0 {
				return status.Error(codes.InvalidArgument, "IN and NOT_IN require a nonempty array")
			}
			limit := 30
			if property.Op == datastorepb.PropertyFilter_NOT_IN {
				limit = 10
			}
			if len(property.Value.GetArrayValue().GetValues()) > limit {
				return status.Errorf(codes.InvalidArgument, "%s supports up to %d values", property.Op, limit)
			}
		default:
			return status.Error(codes.InvalidArgument, "invalid property filter operator")
		}
		if property.Property.Name == "__key__" {
			values := []*datastorepb.Value{property.Value}
			if property.Op == datastorepb.PropertyFilter_IN || property.Op == datastorepb.PropertyFilter_NOT_IN {
				values = property.Value.GetArrayValue().GetValues()
			}
			for _, value := range values {
				if key := value.GetKeyValue(); key != nil && key.GetPartitionId().GetNamespaceId() != namespace {
					return status.Errorf(codes.InvalidArgument, "The query namespace is '%s' but __key__ filter namespace is '%s'.", namespace, key.GetPartitionId().GetNamespaceId())
				}
			}
		}
	case *datastorepb.Filter_CompositeFilter:
		composite := typed.CompositeFilter
		if composite.GetOp() != datastorepb.CompositeFilter_AND && composite.GetOp() != datastorepb.CompositeFilter_OR || len(composite.GetFilters()) == 0 {
			return status.Error(codes.InvalidArgument, "invalid composite filter")
		}
	default:
		return status.Error(codes.InvalidArgument, "filter is required")
	}
	return nil
}

// Java validates components before DNF expansion, but disjunctions afterwards.
// Node counts preserve shared subexpression multiplicity without visiting all
// paths through the graph or repeatedly serializing raw filter subtrees.
func validateQueryComplexity(ctx context.Context, query *datastorepb.Query) error {
	condition, err := compileConditionGraph(ctx, query.Filter, 0, true)
	if err != nil {
		return err
	}
	components := len(query.Order)
	inequality := make(map[string]bool)
	ancestors := make(map[string]bool)
	counts := make([]int, len(condition.nodes))
	for _, node := range condition.nodes {
		if pf := node.property; pf != nil {
			if pf.Op == datastorepb.PropertyFilter_HAS_ANCESTOR {
				ancestors[pf.Value.String()] = true
				continue
			}
			counts[node.id] = 1
			if pf.Op != datastorepb.PropertyFilter_EQUAL {
				inequality[pf.Property.Name] = true
			}
		} else {
			for _, child := range node.children {
				// Only the <=100 boundary matters. Saturation avoids overflow
				// for heavily shared graphs without affecting that boundary.
				counts[node.id] = min(101, counts[node.id]+counts[child.id])
			}
		}
	}
	if condition.root != nil {
		components += counts[condition.root.id]
	}
	if components+len(ancestors) > 100 {
		return status.Error(codes.InvalidArgument, "The query may not have more than 100 filters + sort orders + ancestor total")
	}
	if len(inequality) > 10 {
		return status.Error(codes.InvalidArgument, "A query may not have more than 10 distinct inequality fields")
	}
	filter, err := queryFilterDNF(condition.filter())
	if err != nil {
		return err
	}
	if cf := filter.GetCompositeFilter(); cf.GetOp() == datastorepb.CompositeFilter_OR && len(cf.Filters) > 30 {
		return status.Error(codes.InvalidArgument, "Too many disjunctions after normalization, the maximum is 30")
	}
	return nil
}

// queryFilterSet uses sorted deterministic encodings to deduplicate operands,
// including composites whose children arrived in a different order.
func queryFilterSet(op datastorepb.CompositeFilter_Operator, children []*datastorepb.Filter) (*datastorepb.Filter, error) {
	byKey := make(map[string]*datastorepb.Filter, len(children))
	var keys []string
	for _, child := range children {
		encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(child)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid query filter encoding")
		}
		key := string(encoded)
		if _, exists := byKey[key]; !exists {
			keys = append(keys, key)
			byKey[key] = child
		}
	}
	slices.Sort(keys)
	result := make([]*datastorepb.Filter, 0, len(keys))
	for _, key := range keys {
		result = append(result, byKey[key])
	}
	return &datastorepb.Filter{FilterType: &datastorepb.Filter_CompositeFilter{CompositeFilter: &datastorepb.CompositeFilter{Op: op, Filters: result}}}, nil
}

// queryFilterDNF mirrors Java ConditionNormalizer's set flattening and its 2*30
// intermediate product guard. Check BEFORE multiplying/allocating any product.
func queryFilterDNF(filter *datastorepb.Filter) (*datastorepb.Filter, error) {
	cf := filter.GetCompositeFilter()
	if cf == nil {
		return filter, nil
	}
	var children []*datastorepb.Filter
	for _, child := range cf.Filters {
		normalized, err := queryFilterDNF(child)
		if err != nil {
			return nil, err
		}
		if nested := normalized.GetCompositeFilter(); nested.GetOp() == cf.Op {
			children = append(children, nested.Filters...)
		} else {
			children = append(children, normalized)
		}
	}
	flat, err := queryFilterSet(cf.Op, children)
	if err != nil || cf.Op != datastorepb.CompositeFilter_AND {
		return flat, err
	}
	children = flat.GetCompositeFilter().Filters
	product := 1
	for _, child := range children {
		if nested := child.GetCompositeFilter(); nested.GetOp() == datastorepb.CompositeFilter_OR {
			if len(nested.Filters) > 60/product {
				return nil, status.Error(codes.InvalidArgument, "Too many disjunctions after normalization, the maximum is 30")
			}
			product *= len(nested.Filters)
		}
	}
	if product == 1 {
		return flat, nil
	}
	branches := [][]*datastorepb.Filter{nil}
	for _, child := range children {
		alternatives := []*datastorepb.Filter{child}
		if nested := child.GetCompositeFilter(); nested.GetOp() == datastorepb.CompositeFilter_OR {
			alternatives = nested.Filters
		}
		var next [][]*datastorepb.Filter
		for _, branch := range branches {
			for _, alternative := range alternatives {
				terms := append([]*datastorepb.Filter(nil), branch...)
				if nested := alternative.GetCompositeFilter(); nested.GetOp() == datastorepb.CompositeFilter_AND {
					terms = append(terms, nested.Filters...)
				} else {
					terms = append(terms, alternative)
				}
				next = append(next, terms)
			}
		}
		branches = next
	}
	var result []*datastorepb.Filter
	for _, branch := range branches {
		conjunction, err := queryFilterSet(datastorepb.CompositeFilter_AND, branch)
		if err != nil {
			return nil, err
		}
		if len(conjunction.GetCompositeFilter().Filters) == 1 {
			conjunction = conjunction.GetCompositeFilter().Filters[0]
		}
		result = append(result, conjunction)
	}
	return queryFilterSet(datastorepb.CompositeFilter_OR, result)
}
