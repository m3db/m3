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

package transformation

import (
	"math"
	"time"
)

var (
	emptyDatapoint = Datapoint{Value: math.NaN()}
)

// Datapoint is a metric data point containing a timestamp in
// Unix nanoseconds since epoch and a value.
type Datapoint struct {
	TimeNanos int64
	Value     float64
}

// IsEmpty returns whether this is an empty datapoint.
func (dp Datapoint) IsEmpty() bool { return math.IsNaN(dp.Value) }

// UnaryTransform is a unary transformation that takes a single
// datapoint as input and transforms it into a datapoint as output.
// It can keep state if it requires.
type UnaryTransform interface {
	Evaluate(dp Datapoint) Datapoint
}

// UnaryTransformFn implements UnaryTransform as a function.
type UnaryTransformFn func(dp Datapoint) Datapoint

// Evaluate implements UnaryTransform as a function.
func (fn UnaryTransformFn) Evaluate(dp Datapoint) Datapoint {
	return fn(dp)
}

// BinaryTransform is a binary transformation that takes the
// previous and the current datapoint as input and produces
// a single datapoint as the transformation result.
// It can keep state if it requires.
type BinaryTransform interface {
	Evaluate(prev, curr Datapoint) Datapoint
}

// BinaryTransformFn implements BinaryTransform as a function.
type BinaryTransformFn func(prev, curr Datapoint) Datapoint

// Evaluate implements BinaryTransform as a function.
func (fn BinaryTransformFn) Evaluate(prev, curr Datapoint) Datapoint {
	return fn(prev, curr)
}

// UnaryMultiOutputTransform is like UnaryTransform, but can output an additional datapoint.
// The additional datapoint is not passed to subsequent transforms.
type UnaryMultiOutputTransform interface {
	// Evaluate applies the transform on the provided datapoint.
	Evaluate(dp Datapoint, resolution time.Duration) (Datapoint, Datapoint)
}

// UnaryMultiOutputTransformFn implements UnaryMultiOutputTransform as a function.
type UnaryMultiOutputTransformFn func(dp Datapoint, resolution time.Duration) (Datapoint, Datapoint)

// Evaluate applies the transform on the provided datapoint.
func (fn UnaryMultiOutputTransformFn) Evaluate(dp Datapoint, resolution time.Duration) (Datapoint, Datapoint) {
	return fn(dp, resolution)
}
