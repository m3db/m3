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

import "math"

var (
	// allows to use a single transform fn ref (instead of
	// taking reference to it each time when converting to iface).
	transformAbsoluteFn = UnaryTransformFn(absolute)
)

func transformAbsolute() UnaryTransform {
	return transformAbsoluteFn
}

func absolute(dp Datapoint) Datapoint {
	var res Datapoint
	res.TimeNanos = dp.TimeNanos
	res.Value = math.Abs(dp.Value)
	return res
}

// add will add add a datapoint to a running count and return the result, useful
// for computing a running sum of values (like a monotonic increasing counter).
// Note:
// * It treats NaN as zero value, i.e. 42 + NaN = 42.
func transformAdd() UnaryTransform {
	var curr float64
	return UnaryTransformFn(func(dp Datapoint) Datapoint {
		if !math.IsNaN(dp.Value) {
			curr += dp.Value
		}
		return Datapoint{TimeNanos: dp.TimeNanos, Value: curr}
	})
}
