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

const (
	nanosPerSecond = time.Second / time.Nanosecond
)

var (
	// allows to use a single transform fn ref (instead of
	// taking reference to it each time when converting to iface).
	transformPerSecondFn = BinaryTransformFn(perSecond)
	transformIncreaseFn  = BinaryTransformFn(increase)
)

func transformPerSecond() BinaryTransform {
	return transformPerSecondFn
}

// perSecond computes the derivative between consecutive datapoints, taking into
// account the time interval between the values.
// Note:
// * It skips NaN values.
// * It assumes the timestamps are monotonically increasing, and values are non-decreasing.
//   If either of the two conditions is not met, an empty datapoint is returned.
func perSecond(prev, curr Datapoint, flags TransformationFeatureFlags) Datapoint {
	if prev.TimeNanos >= curr.TimeNanos || math.IsNaN(prev.Value) || math.IsNaN(curr.Value) {
		return emptyDatapoint
	}
	diff := curr.Value - prev.Value
	if diff < 0 {
		return emptyDatapoint
	}
	rate := diff * float64(nanosPerSecond) / float64(curr.TimeNanos-prev.TimeNanos)
	return Datapoint{TimeNanos: curr.TimeNanos, Value: rate}
}

func transformIncrease() BinaryTransform {
	return transformIncreaseFn
}

// increase computes the difference between consecutive datapoints, unlike
// perSecond it does not account for the time interval between the values.
// Note:
// * It skips NaN values.
// * It assumes the timestamps are monotonically increasing, and values are non-decreasing.
//   If either of the two conditions is not met, an empty datapoint is returned.
func increase(prev, curr Datapoint, flags TransformationFeatureFlags) Datapoint {
	if prev.TimeNanos >= curr.TimeNanos {
		return emptyDatapoint
	}
	if math.IsNaN(curr.Value) {
		return emptyDatapoint
	}

	if math.IsNaN(prev.Value) {
		if !flags.IncreaseWithPrevNaNTranslatesToCurrValueIncrease {
			return emptyDatapoint
		}
		prev.Value = 0
	}

	diff := curr.Value - prev.Value
	if diff < 0 {
		return emptyDatapoint
	}
	return Datapoint{TimeNanos: curr.TimeNanos, Value: diff}
}

// [nan,1,nan,nan,nan,5,nan,nan,nan,nan,7]
// call increase(...) only when something changes and only passed the changed values in
// ideally:
// [(nan,1),(1,5),(5,7)]
