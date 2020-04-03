// Copyright (c) 2020 Uber Technologies, Inc.
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

package scalar

import "github.com/m3db/m3/src/query/functions/binary"

// TimeValueGenerator generates values for times at steps.
// NB: this must return a new slice every time, as the underlying bytes
// may be mutated by function application.
type TimeValueGenerator func() []float64

// ValueForTimeFn calculates the scalar value for
// the given time value generator.
type ValueForTimeFn func(generator TimeValueGenerator) []float64

// Value describes a scalar value that can be
// computed without function application. This covers
// scalar arithmetic and `time()` function.
type Value struct {
	// HasTimeValues is set if this value requires time value
	// application to return the computed values.
	HasTimeValues bool
	// Value is the value if time values do not need to be applied.
	Scalar float64
	// TimeValueFn is a function to apply a time value generator, which
	// must return a new slice as this may mutate underlying values.
	TimeValueFn ValueForTimeFn
}

// ApplyFunc combines two Values given a binary function.
func ApplyFunc(l, r Value, fn binary.Function) Value {
	if !l.HasTimeValues {
		lVal := l.Scalar
		if !r.HasTimeValues {
			// NB: both left and right here are scalar.
			return Value{Scalar: fn(lVal, r.Scalar)}
		}

		// NB: left is constant and right has time dependent values.
		innerFn := r.TimeValueFn
		applyFunc := func(generator TimeValueGenerator) []float64 {
			inner := innerFn(generator)
			for i, val := range inner {
				inner[i] = fn(lVal, val)
			}

			return inner
		}

		r.TimeValueFn = applyFunc
		return r
	}

	if !r.HasTimeValues {
		rVal := r.Scalar
		// NB: left has time dependent values and right is constant.
		innerFn := l.TimeValueFn
		applyFunc := func(generator TimeValueGenerator) []float64 {
			inner := innerFn(generator)
			for i, val := range inner {
				inner[i] = fn(val, rVal)
			}

			return inner
		}

		l.TimeValueFn = applyFunc
		return l
	}

	innerFnL := l.TimeValueFn
	innerFnR := r.TimeValueFn
	applyFunc := func(generator TimeValueGenerator) []float64 {
		innerLeft := innerFnL(generator)
		innerRight := innerFnR(generator)
		for i, leftVal := range innerLeft {
			innerLeft[i] = fn(leftVal, innerRight[i])
		}

		return innerLeft
	}

	l.TimeValueFn = applyFunc
	return l
}
