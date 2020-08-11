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

package tile

import (
	"math"
)

// DownsampleCounterResets downsamples datapoints in a way that preserves counter invariants:
// The the last (and the first, if necessary) values stay the same as original (to transfer counter state from tile to tile).
// Also, after applying counter reset adjustment logics, all the values would be the same as
// after applying this logics to the original values.
// As an optimization (to reduce the amount of datapoints), prevFrameLastValue can be passed in (if available),
// and then in some cases the first value of this tile may be omitted.
// If the value for prevFrameLastValue is not available, pass math.Nan() instead.
func DownsampleCounterResets(
	prevFrameLastValue float64,
	frame SeriesBlockFrame,
	indices *[]int,
	results *[]float64,
) {
	frameValues := frame.Values()

	if len(frameValues) == 0 {
		return
	}

	firstValue := frameValues[0]
	if math.IsNaN(prevFrameLastValue) || prevFrameLastValue > firstValue {
		// include the first original datapoint to handle resets right before this frame
		*indices = append(*indices, 0)
		*results = append(*results, firstValue)
	}

	var (
		previous                     = firstValue
		accumulated                  = previous
		lastResetPosition            = math.MinInt64
		adjustedValueBeforeLastReset = 0.0
	)
	for i := 1; i < len(frameValues); i++ {
		current := frameValues[i]
		delta := current - previous
		if delta >= 0 { // monotonic non-decrease
			accumulated += delta
		} else { // a reset
			adjustedValueBeforeLastReset = accumulated
			lastResetPosition = i - 1
			accumulated += current
		}
		previous = current
	}

	if lastResetPosition >= 0 && (len(*results) == 0 || (*results)[0] != adjustedValueBeforeLastReset) {
		// include the adjusted value right before the last reset (if it is not equal to the included first value)
		*indices = append(*indices, lastResetPosition)
		*results = append(*results, adjustedValueBeforeLastReset)
	}

	lastPosition := len(frameValues) - 1
	lastValue := frameValues[lastPosition]

	positionAfterLastReset := lastResetPosition + 1
	if lastResetPosition >= 0 && adjustedValueBeforeLastReset <= lastValue {
		// include the original value right after the last reset (unless it is the last value, which is always included)
		*indices = append(*indices, positionAfterLastReset)
		*results = append(*results, frameValues[positionAfterLastReset])
	}

	if len(*results) == 1 && (*results)[0] == lastValue {
		// if only the first value was included until now, and it's equal to the last value, it can be discarded
		*results = (*results)[:0]
		*indices = (*indices)[:0]
	}

	// always include the last original datapoint
	*indices = append(*indices, lastPosition)
	*results = append(*results, lastValue)

	return
}
