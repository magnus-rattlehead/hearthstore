package server

import (
	"cmp"
	"math"
	"slices"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// extractVector extracts float64 coordinates from a Firestore vector Value.
// Firestore vectors are encoded as a MapValue with:
//
//	fields["__type__"] = StringValue("__vector__")
//	fields["value"]    = ArrayValue([DoubleValue, ...])
func extractVector(v *firestorepb.Value) ([]float64, bool) {
	if v == nil {
		return nil, false
	}
	m := v.GetMapValue()
	if m == nil || m.Fields == nil {
		return nil, false
	}
	typeField, ok := m.Fields["__type__"]
	if !ok || typeField.GetStringValue() != "__vector__" {
		return nil, false
	}
	valField, ok := m.Fields["value"]
	if !ok {
		return nil, false
	}
	av := valField.GetArrayValue()
	if av == nil {
		return nil, false
	}
	coords := make([]float64, len(av.Values))
	for i, elem := range av.Values {
		coords[i] = numericFloat(elem)
	}
	return coords, true
}

func euclideanDist(a, b []float64) float64 {
	var sum float64
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return math.Sqrt(sum)
}

func cosineDist(a, b []float64) float64 {
	var dot, magA, magB float64
	for i := range a {
		dot += a[i] * b[i]
		magA += a[i] * a[i]
		magB += b[i] * b[i]
	}
	if magA == 0 || magB == 0 {
		return 1.0 // undefined → maximum distance
	}
	sim := dot / (math.Sqrt(magA) * math.Sqrt(magB))
	if sim > 1.0 {
		sim = 1.0
	}
	if sim < -1.0 {
		sim = -1.0
	}
	return 1.0 - sim
}

func dotProductSim(a, b []float64) float64 {
	var sum float64
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

type docWithDist struct {
	doc  *firestorepb.Document
	dist float64
}

// applyFindNearest post-processes a doc set according to StructuredQuery.find_nearest.
// It filters docs to those with a matching vector field, computes distances,
// applies distanceThreshold, sorts, applies limit, and optionally adds a
// distanceResultField to each returned document.
func applyFindNearest(docs []*firestorepb.Document, fn *firestorepb.StructuredQuery_FindNearest) []*firestorepb.Document {
	if fn == nil {
		return docs
	}
	queryVec, ok := extractVector(fn.QueryVector)
	if !ok {
		return nil
	}

	isDotProduct := fn.DistanceMeasure == firestorepb.StructuredQuery_FindNearest_DOT_PRODUCT
	fieldPath := fn.VectorField.GetFieldPath()

	var candidates []docWithDist
	for _, doc := range docs {
		fieldVal := getField(doc, fieldPath)
		docVec, ok := extractVector(fieldVal)
		if !ok || len(docVec) != len(queryVec) {
			continue // skip docs with missing or dimension-mismatched vectors
		}

		var dist float64
		switch fn.DistanceMeasure {
		case firestorepb.StructuredQuery_FindNearest_EUCLIDEAN:
			dist = euclideanDist(queryVec, docVec)
		case firestorepb.StructuredQuery_FindNearest_COSINE:
			dist = cosineDist(queryVec, docVec)
		case firestorepb.StructuredQuery_FindNearest_DOT_PRODUCT:
			dist = dotProductSim(queryVec, docVec)
		default:
			dist = euclideanDist(queryVec, docVec)
		}

		// Apply distance threshold:
		// EUCLIDEAN/COSINE: keep where distance <= threshold
		// DOT_PRODUCT:      keep where dot_product >= threshold
		if fn.DistanceThreshold != nil {
			threshold := fn.DistanceThreshold.Value
			if isDotProduct {
				if dist < threshold {
					continue
				}
			} else {
				if dist > threshold {
					continue
				}
			}
		}

		candidates = append(candidates, docWithDist{doc: doc, dist: dist})
	}

	// Sort: ascending distance for EUCLIDEAN/COSINE, descending (highest similarity) for DOT_PRODUCT.
	slices.SortStableFunc(candidates, func(a, b docWithDist) int {
		if isDotProduct {
			return cmp.Compare(b.dist, a.dist)
		}
		return cmp.Compare(a.dist, b.dist)
	})

	// Apply limit.
	limit := int(fn.Limit.GetValue())
	if limit > 0 && len(candidates) > limit {
		candidates = candidates[:limit]
	}

	result := make([]*firestorepb.Document, len(candidates))
	for i, c := range candidates {
		doc := c.doc
		if fn.DistanceResultField != "" {
			// Clone the doc and inject the distance field.
			newFields := make(map[string]*firestorepb.Value, len(doc.Fields)+1)
			for k, v := range doc.Fields {
				newFields[k] = v
			}
			newFields[fn.DistanceResultField] = &firestorepb.Value{
				ValueType: &firestorepb.Value_DoubleValue{DoubleValue: c.dist},
			}
			doc = &firestorepb.Document{
				Name:       doc.Name,
				Fields:     newFields,
				CreateTime: doc.CreateTime,
				UpdateTime: doc.UpdateTime,
			}
		}
		result[i] = doc
	}
	return result
}
