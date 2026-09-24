// Package valuecodec contains shared Datastore value ordering primitives.
package valuecodec

import (
	"encoding/binary"
	"math"
	"math/bits"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// Number produces the same sortable representation for mathematically equal
// integers and doubles without rounding integers through float64.
func Number(value *datastorepb.Value) []byte {
	var negative bool
	var exponent int
	var significand uint64
	if integer, ok := value.GetValueType().(*datastorepb.Value_IntegerValue); ok {
		negative = integer.IntegerValue < 0
		magnitude := uint64(integer.IntegerValue)
		if negative {
			magnitude = -magnitude
		}
		if magnitude == 0 {
			return []byte{2, 3}
		}
		exponent = bits.Len64(magnitude)
		significand = magnitude << (64 - exponent)
	} else {
		number := value.GetDoubleValue()
		switch {
		case math.IsNaN(number):
			return []byte{2, 0}
		case math.IsInf(number, -1):
			return []byte{2, 1}
		case math.IsInf(number, 1):
			return []byte{2, 5}
		case number == 0:
			return []byte{2, 3}
		}
		negative = number < 0
		fraction, exp := math.Frexp(math.Abs(number))
		exponent = exp
		significand = uint64(math.Ldexp(fraction, 64))
	}
	out := make([]byte, 12)
	out[0], out[1] = 2, 4
	binary.BigEndian.PutUint16(out[2:], uint16(exponent+2048))
	binary.BigEndian.PutUint64(out[4:], significand)
	if negative {
		out[1] = 2
		for i := 2; i < len(out); i++ {
			out[i] = ^out[i]
		}
	}
	return out
}
