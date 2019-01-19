package testing

import (
	"fmt"
	"math"

	"github.com/stretchr/testify/assert"
)

func toFloat(x interface{}) (float64, bool) {
	var xf float64
	xok := true

	switch xn := x.(type) {
	case uint8:
		xf = float64(xn)
	case uint16:
		xf = float64(xn)
	case uint32:
		xf = float64(xn)
	case uint64:
		xf = float64(xn)
	case int:
		xf = float64(xn)
	case int8:
		xf = float64(xn)
	case int16:
		xf = float64(xn)
	case int32:
		xf = float64(xn)
	case int64:
		xf = float64(xn)
	case float32:
		xf = float64(xn)
	case float64:
		xf = float64(xn)
	default:
		xok = false
	}

	return xf, xok
}

func castToFloats(
	t assert.TestingT,
	expected, actual interface{},
) (float64, float64, bool) {
	af, aok := toFloat(expected)
	bf, bok := toFloat(actual)

	if !aok || !bok {
		return 0, 0, assert.Fail(t, "expected or actual are unexpected types")
	}

	return af, bf, true
}

// EqualWithNaNs compares two numbers for equality, accounting for NaNs.
func EqualWithNaNs(
	t assert.TestingT,
	expected, actual interface{},
	msgAndArgs ...interface{},
) bool {
	af, bf, ok := castToFloats(t, expected, actual)
	if !ok {
		return ok
	}

	if math.IsNaN(af) && math.IsNaN(bf) {
		return true
	}

	return assert.Equal(t, af, bf, msgAndArgs)
}

// InDeltaWithNaNs compares two floats for equality within a delta,
// accounting for NaNs.
func InDeltaWithNaNs(
	t assert.TestingT,
	expected, actual interface{},
	delta float64,
	msgAndArgs ...interface{},
) bool {
	af, bf, ok := castToFloats(t, expected, actual)
	if !ok {
		return ok
	}

	if math.IsNaN(af) && math.IsNaN(bf) {
		return true
	}

	return assert.InDelta(t, expected, actual, delta)
}

// Equalish asserts that two objects are equal. Looser than assert.Equal since
// it checks for equality of printing expected vs printing actual.
//
//    assert.Equal(t, 123, 123, "123 and 123 should be equal")
//
// Returns whether the assertion was successful (true) or not (false).
func Equalish(
	t assert.TestingT,
	expected, actual interface{},
	msgAndArgs ...interface{},
) bool {
	if assert.ObjectsAreEqual(expected, actual) {
		return true
	}

	// Last ditch effort
	if fmt.Sprintf("%#v", expected) == fmt.Sprintf("%#v", actual) {
		return true
	}

	return assert.Fail(t, fmt.Sprintf("Not equal: %#v (expected)\n"+
		"        != %#v (actual)", expected, actual), msgAndArgs...)
}
