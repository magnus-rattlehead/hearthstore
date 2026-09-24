package datastore

import (
	"math"
	"math/big"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// aggregationSum follows Java's BigIntegerAndDoubleSummation: retain exact
// integer totals separately from compensated doubles and IEEE special values.
// Most sums stay in int64; allocate a wide integer only on actual overflow.
type aggregationSum struct {
	integer                     int64
	wide                        *big.Int
	hasDouble                   bool
	finite, correction, special float64
}

func (s *aggregationSum) add(value *datastorepb.Value) bool {
	var integer int64
	switch v := value.GetValueType().(type) {
	case *datastorepb.Value_IntegerValue:
		integer = v.IntegerValue
	case *datastorepb.Value_TimestampValue:
		integer = v.TimestampValue.GetSeconds()*1_000_000 + int64(v.TimestampValue.GetNanos())/1_000
	case *datastorepb.Value_DoubleValue:
		s.hasDouble = true
		s.special += v.DoubleValue
		if !math.IsNaN(v.DoubleValue) && !math.IsInf(v.DoubleValue, 0) {
			value := v.DoubleValue + s.correction
			total := s.finite + value
			previous := total - value
			increment := total - previous
			s.correction = (s.finite - previous) + (value - increment)
			s.finite = total
		}
		return true
	default:
		return false
	}
	if s.wide == nil && (integer > 0 && s.integer > math.MaxInt64-integer || integer < 0 && s.integer < math.MinInt64-integer) {
		s.wide = big.NewInt(s.integer)
	}
	if s.wide != nil {
		var operand big.Int
		s.wide.Add(s.wide, operand.SetInt64(integer))
	} else {
		s.integer += integer
	}
	return true
}

func (s *aggregationSum) integerValue() (int64, bool) {
	if s.hasDouble {
		return 0, false
	}
	if s.wide != nil {
		return s.wide.Int64(), s.wide.IsInt64()
	}
	return s.integer, true
}

func (s *aggregationSum) doubleValue() float64 {
	if math.IsNaN(s.special) || math.IsInf(s.special, 0) {
		return s.special
	}
	integer := float64(s.integer)
	if s.wide != nil {
		integer, _ = s.wide.Float64()
	} // Accuracy is intentionally rounded to the API's double result.
	return integer + s.finite
}
