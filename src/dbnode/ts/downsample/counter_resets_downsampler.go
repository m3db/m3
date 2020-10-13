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

package downsample

import (
	"math"
)

// Value represents a datapoint value after downsampling.
type Value struct {
	// FrameIndex is the index to the original datapoint within source frame (before downsampling).
	FrameIndex int
	// Value is a downsampled datapoint value.
	Value float64
}

// DownsampleCounterResets downsamples datapoints in a way that preserves counter invariants:
// The last (and the first, if necessary) values stay the same as original (to transfer counter state from tile to tile).
// Also, after applying counter reset adjustment logics, all the values would be the same as
// after applying this logics to the original values.
// As an optimization (to reduce the amount of datapoints), prevFrameLastValue can be passed in (if available),
// and then in some cases the first value of this tile may be omitted.
// If the value for prevFrameLastValue is not available, pass math.NaN() instead.
// Pass a slice of capacity 4 as results to avoid potential allocations.
//
// Examples:
//
// 1. Frame without resets, prevFrameLastValue not provided (x):
// ... x | 5 6 7 8 9 | ...
// downsamples to:
// ... x | 5       9 | ...
// - we always keep the last value (9), and also need to keep the first value (5) because there might have been
// a reset between the previous frame and the current one (x -> 5).
//
// 2. Frame without resets, prevFrameLastValue provided (2):
// ... 2 | 5 6 7 8 9 | ...
// downsamples to:
// ... 2 |         9 | ...
// - we always keep the last value (9), and we can drop the first value (5) because we know there was no reset
// between the previous frame and the current one (2 -> 5).
//
// 3. Frame with one reset (6 -> 1):
// ... 2 | 5 6 1 3 9 | ...
// downsamples to:
// ... 2 |   6 1   9 | ...
// - we always keep the last value (9), and also include two datapoints around the reset (6 -> 1).
//
// 4. Frame with two resets (5 -> 1, 3 -> 0):
// ... 2 | 5 1 3 0 9 | ...
// downsamples to:
// ... 2 |     8 0 9 | ...
// - we always keep the last value (9), and also include two datapoints around the last reset (8 -> 0),
// where 8 is the value accumulated from all resets within the frame (5 + 3 = 8).

func DownsampleCounterResets(
	prevFrameLastValue float64,
	frameValues []float64,
	results []Value,
) []Value {
	results = results[:0]

	if len(frameValues) == 0 {
		return results
	}

	firstValue := frameValues[0]
	if math.IsNaN(prevFrameLastValue) || prevFrameLastValue > firstValue {
		// include the first original datapoint to handle resets right before this frame
		results = append(results, Value{0, firstValue})
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
		previous = current
		if delta >= 0 { 
			// Monotonic non-decrease.
			accumulated += delta
			continue
		}
		// A reset.
		adjustedValueBeforeLastReset = accumulated
		lastResetPosition = i - 1
		accumulated += current
	}

	if lastResetPosition >= 0 && (len(results) == 0 || results[0].Value != adjustedValueBeforeLastReset) {
		// include the adjusted value right before the last reset (if it is not equal to the included first value)
		results = append(results, Value{lastResetPosition, adjustedValueBeforeLastReset})
	}

	lastPosition := len(frameValues) - 1
	lastValue := frameValues[lastPosition]

	positionAfterLastReset := lastResetPosition + 1
	if lastResetPosition >= 0 && adjustedValueBeforeLastReset <= lastValue {
		// include the original value right after the last reset (unless it is the last value, which is always included)
		results = append(results, Value{positionAfterLastReset, frameValues[positionAfterLastReset]})
	}

	if len(results) == 1 && results[0].Value == lastValue {
		// if only the first value was included until now, and it's equal to the last value, it can be discarded
		results = results[:0]
	}

	// always include the last original datapoint
	return append(results, Value{lastPosition, lastValue})
}
