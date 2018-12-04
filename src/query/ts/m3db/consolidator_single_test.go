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

package m3db

import (
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"

	"github.com/stretchr/testify/assert"
)

func TestSingleConsolidator(t *testing.T) {
	lookback := time.Minute
	start := time.Now().Truncate(time.Hour)
	fn := TakeLast

	consolidator := buildSingleConsolidator(
		lookback,
		lookback,
		start,
		fn,
	)

	// NB: lookback limit: start-1
	actual := consolidator.consolidate()
	assert.True(t, math.IsNaN(actual))

	consolidator.addPoint(ts.Datapoint{Timestamp: start, Value: 1})
	// NB: lookback limit: start
	actual = consolidator.consolidate()
	assert.Equal(t, float64(1), actual)

	// NB: lookback limit: start+1
	actual = consolidator.consolidate()
	assert.True(t, math.IsNaN(actual))

	// NB: lookback limit: start+2
	actual = consolidator.consolidate()
	assert.True(t, math.IsNaN(actual))

	consolidator.addPoint(ts.Datapoint{Timestamp: start.Add(2*time.Minute + time.Second*30), Value: 2})
	consolidator.addPoint(ts.Datapoint{Timestamp: start.Add(3*time.Minute + time.Second), Value: 3})

	// NB: lookback limit: start+3
	actual = consolidator.consolidate()
	assert.Equal(t, float64(3), actual)
}
