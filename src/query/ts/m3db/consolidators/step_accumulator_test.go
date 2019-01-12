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

package consolidators

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	xts "github.com/m3db/m3/src/query/ts"

	"github.com/stretchr/testify/assert"
)

func TestAccumulator(t *testing.T) {
	lookback := time.Minute
	start := time.Now().Truncate(time.Hour)
	iterCount := 2

	acc := NewStepLookbackAccumulator(
		lookback,
		lookback,
		start,
		iterCount,
	)

	// NB: lookback limit: start-1
	actual := acc.AccumulateAndMoveToNext()
	expected := []xts.Datapoints{{}, {}}
	assert.Equal(t, expected, actual)

	acc.AddPointForIterator(ts.Datapoint{Timestamp: start, Value: 1}, 0)
	acc.AddPointForIterator(ts.Datapoint{Timestamp: start.Add(time.Minute), Value: 10}, 1)

	// NB: lookback limit: start
	actual = acc.AccumulateAndMoveToNext()
	expected[0] = xts.Datapoints{xts.Datapoint{Timestamp: start, Value: 1}}
	expected[1] = xts.Datapoints{xts.Datapoint{Timestamp: start.Add(time.Minute), Value: 10}}
	assert.Equal(t, expected, actual)

	// NB: lookback limit: start+1, point 1 is outside of the lookback period
	actual = acc.AccumulateAndMoveToNext()
	expected[0] = xts.Datapoints{}
	assert.Equal(t, expected, actual)

	// NB: lookback limit: start+2 both points outside of the lookback period
	actual = acc.AccumulateAndMoveToNext()
	expected[1] = xts.Datapoints{}
	assert.Equal(t, expected, actual)

	acc.AddPointForIterator(ts.Datapoint{Timestamp: start.Add(2*time.Minute + time.Second*30), Value: 2}, 0)
	acc.AddPointForIterator(ts.Datapoint{Timestamp: start.Add(3*time.Minute + time.Second), Value: 3}, 0)

	// NB: lookback limit: start+3, both points in lookback period
	actual = acc.AccumulateAndMoveToNext()
	expected[0] = xts.Datapoints{xts.Datapoint{Timestamp: start.Add(3*time.Minute + time.Second), Value: 3}}
	assert.Equal(t, expected, actual)
}
