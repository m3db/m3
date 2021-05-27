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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
)

func TestConsolidator(t *testing.T) {
	lookback := time.Minute
	start := xtime.Now().Truncate(time.Hour)
	fn := TakeLast
	consolidator := NewStepLookbackConsolidator(
		lookback,
		lookback,
		start,
		fn,
	)

	// NB: lookback limit: start-1
	consolidator.BufferStep()

	consolidator.AddPoint(ts.Datapoint{TimestampNanos: start, Value: 1})
	consolidator.AddPoint(ts.Datapoint{
		TimestampNanos: start.Add(time.Minute),
		Value:          10,
	})
	consolidator.BufferStep()
	consolidator.BufferStep()
	consolidator.BufferStep()
	consolidator.AddPoint(ts.Datapoint{
		TimestampNanos: start.Add(2*time.Minute + time.Second*30),
		Value:          2},
	)
	consolidator.AddPoint(ts.Datapoint{
		TimestampNanos: start.Add(3*time.Minute + time.Second),
		Value:          3},
	)
	consolidator.BufferStep()

	actual := consolidator.ConsolidateAndMoveToNext()
	test.EqualsWithNans(t, nan, actual)

	// NB: lookback limit: start
	actual = consolidator.ConsolidateAndMoveToNext()
	assert.Equal(t, 10.0, actual)

	// NB: lookback limit: start+1, point 1 is outside of the lookback period
	actual = consolidator.ConsolidateAndMoveToNext()
	assert.Equal(t, 10.0, actual)

	// NB: lookback limit: start+2 both points outside of the lookback period
	actual = consolidator.ConsolidateAndMoveToNext()
	assert.True(t, math.IsNaN(actual), fmt.Sprintf("%f should be nan", actual))

	// NB: lookback limit: start+3, both points in lookback period
	actual = consolidator.ConsolidateAndMoveToNext()
	assert.Equal(t, 3.0, actual)
}
