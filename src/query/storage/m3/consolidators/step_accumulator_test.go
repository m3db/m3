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
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
)

func TestAccumulator(t *testing.T) {
	lookback := time.Minute
	start := xtime.Now().Truncate(time.Hour)
	acc := NewStepLookbackAccumulator(
		lookback,
		lookback,
		start,
	)

	// NB: lookback limit: start-1
	actual := acc.AccumulateAndMoveToNext()
	assert.Nil(t, actual)

	acc.AddPoint(ts.Datapoint{TimestampNanos: start, Value: 1})
	acc.AddPoint(ts.Datapoint{TimestampNanos: start.Add(time.Minute), Value: 10})
	acc.BufferStep()
	acc.BufferStep()
	acc.BufferStep()
	acc.AddPoint(ts.Datapoint{
		TimestampNanos: start.Add(2*time.Minute + time.Second*30),
		Value:          2,
	})
	acc.AddPoint(ts.Datapoint{
		TimestampNanos: start.Add(3*time.Minute + time.Second),
		Value:          3,
	})
	acc.BufferStep()

	// NB: lookback limit: start
	actual = acc.AccumulateAndMoveToNext()
	assert.Equal(t, []xts.Datapoint{
		{Timestamp: start, Value: 1},
		{Timestamp: start.Add(time.Minute), Value: 10},
	}, actual)

	// NB: lookback limit: start+1, should be reset
	actual = acc.AccumulateAndMoveToNext()
	assert.Nil(t, actual)

	// NB: lookback limit: start+2, should be reset
	actual = acc.AccumulateAndMoveToNext()
	assert.Nil(t, actual)

	// NB: lookback limit: start+3, both points in lookback period
	actual = acc.AccumulateAndMoveToNext()
	assert.Equal(t, []xts.Datapoint{
		{Timestamp: start.Add(2*time.Minute + time.Second*30), Value: 2},
		{Timestamp: start.Add(3*time.Minute + time.Second), Value: 3},
	}, actual)
}
