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

package consolidators

import (
	"math"

	"github.com/m3db/m3/src/dbnode/ts"
)

// ConsolidationFunc consolidates a bunch of datapoints into a single float value
type ConsolidationFunc func(datapoints []ts.Datapoint) float64

// TakeLast is a consolidation function which takes the last datapoint
func TakeLast(values []ts.Datapoint) float64 {
	for i := len(values) - 1; i >= 0; i-- {
		value := values[i].Value
		if !math.IsNaN(value) {
			return value
		}
	}

	return math.NaN()
}

const initLength = 10

// Set NaN to a variable makes tests easier.
var nan = math.NaN()
