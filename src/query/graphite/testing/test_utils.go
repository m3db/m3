// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
		xf = xn
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

	dt := af - bf
	if dt < -delta || dt > delta {
		return assert.Fail(t,
			fmt.Sprintf(
				"Max difference between %v and %v allowed is %v, but difference was %v",
				expected, actual, delta, dt), msgAndArgs...)
	}

	return true
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
