// Copyright (c) 2018 Uber Technologies, Inc.
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

package binary

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testFunction(t *testing.T, op string, expected float64) {
	fn, err := ArithmeticFunction(op, false)
	assert.NoError(t, err)
	actual := fn(12, 15)
	if math.IsNaN(expected) {
		assert.True(t, math.IsNaN(actual))
	} else {
		assert.Equal(t, expected, actual)
	}
}

func testFunctionReturnBool(t *testing.T, op string, expected float64) {
	fn, err := ArithmeticFunction(op, true)
	assert.NoError(t, err)
	assert.Equal(t, expected, fn(12, 15))
}

func TestArithmeticFunction(t *testing.T) {
	testFunction(t, PlusType, 27)
	testFunction(t, MinusType, -3)
	testFunction(t, MultiplyType, 180)
	testFunction(t, DivType, 0.8)
	testFunction(t, ExpType, 15407021574586368)
	testFunction(t, ModType, 12)

	testFunction(t, EqType, math.NaN())
	testFunction(t, NotEqType, 12)
	testFunction(t, GreaterType, math.NaN())
	testFunction(t, LesserType, 12)
	testFunction(t, GreaterEqType, math.NaN())
	testFunction(t, LesserEqType, 12)

	testFunctionReturnBool(t, EqType, 0)
	testFunctionReturnBool(t, NotEqType, 1)
	testFunctionReturnBool(t, GreaterType, 0)
	testFunctionReturnBool(t, LesserType, 1)
	testFunctionReturnBool(t, GreaterEqType, 0)
	testFunctionReturnBool(t, LesserEqType, 1)
}
