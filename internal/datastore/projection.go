package datastore

import (
	"context"
	"slices"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"

	"github.com/magnus-rattlehead/hearthstore/internal/storage"
)

// queryTuplePredicate deduplicates adjacent ordered groups before page limits.
func queryTuplePredicate(query *datastorepb.Query, cursor *storage.CursorPayload, matches func(*datastorepb.Entity) bool) func(*datastorepb.Entity) bool {
	previous, havePrevious := "", false
	if cursor != nil && cursor.D != "" {
		previous, havePrevious = cursor.D, true
	}
	return func(entity *datastorepb.Entity) bool {
		if matches != nil && !matches(entity) {
			return false
		}
		if len(query.DistinctOn) == 0 {
			return true
		}
		group := distinctKey(entity, query.DistinctOn)
		if havePrevious && previous == group {
			return false
		}
		previous, havePrevious = group, true
		return true
	}
}

// visitProjection expands the Cartesian product lazily and stops when emit does.
func visitProjection(ctx context.Context, entity *datastorepb.Entity, fields []string, emit func(*datastorepb.Entity, bool) bool) error {
	return visitProjectionSelection(ctx, entity, fields, func(projected *datastorepb.Entity, _ []int, last bool) bool { return emit(projected, last) })
}

// visitProjectionSelection also exposes original array offsets for compact spill records.
// Offsets are only valid for the fixed entity snapshot being visited.
func visitProjectionSelection(ctx context.Context, entity *datastorepb.Entity, fields []string, emit func(*datastorepb.Entity, []int, bool) bool) error {
	work := storage.QueryWorkFromContext(ctx)
	var comparisonErr error
	compare := func(a, b *datastorepb.Value) int {
		if comparisonErr == nil {
			comparisonErr = work.Checkpoint(ctx)
		}
		if comparisonErr != nil {
			return 0
		}
		work.Charge(storage.WorkComparisons, 1)
		return compareValues(a, b)
	}
	values := make([][]*datastorepb.Value, len(fields))
	selections := make([][]int, len(fields))
	for i, field := range fields {
		if err := work.Checkpoint(ctx); err != nil {
			return err
		}
		value := getProp(entity, field)
		if value == nil {
			return nil
		}
		values[i] = []*datastorepb.Value{value}
		selections[i] = []int{0}
		if array := value.GetArrayValue(); array != nil {
			values[i] = array.Values
			selections[i] = make([]int, 0, len(array.Values))
			for j, candidate := range array.Values {
				if err := work.Checkpoint(ctx); err != nil {
					return err
				}
				if candidate != nil && !candidate.ExcludeFromIndexes && candidate.GetEntityValue() == nil {
					selections[i] = append(selections[i], j)
				}
			}
			slices.SortStableFunc(selections[i], func(a, b int) int { return compare(array.Values[a], array.Values[b]) })
			if comparisonErr != nil {
				return comparisonErr
			}
			selections[i] = slices.CompactFunc(selections[i], func(a, b int) bool { return compare(array.Values[a], array.Values[b]) == 0 })
			if comparisonErr != nil {
				return comparisonErr
			}
		}
		if len(selections[i]) == 0 {
			return nil
		}
	}
	positions := make([]int, len(fields))
	selected := make([]int, len(fields))
	for {
		if err := work.Checkpoint(ctx); err != nil {
			return err
		}
		work.Charge(storage.WorkProjectionTuples, 1)
		properties := make(map[string]*datastorepb.Value, len(fields))
		last := true
		for i, field := range fields {
			selected[i] = selections[i][positions[i]]
			projected := proto.Clone(values[i][selected[i]]).(*datastorepb.Value)
			projected.Meaning = 18 // Java emulator's INDEX_VALUE marker for projections.
			properties[field] = projected
			last = last && positions[i] == len(selections[i])-1
		}
		if !emit(&datastorepb.Entity{Key: entity.Key, Properties: properties}, selected, last) || last {
			return nil
		}
		for i := len(fields) - 1; i >= 0; i-- {
			positions[i]++
			if positions[i] < len(selections[i]) {
				break
			}
			positions[i] = 0
		}
	}
}
