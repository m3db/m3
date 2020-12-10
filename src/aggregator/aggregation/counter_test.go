// Copyright (c) 2016 Uber Technologies, Inc.
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

package aggregation

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/require"
)

func TestCounterDefaultAggregationType(t *testing.T) {
	c := NewCounter(NewOptions(instrument.NewOptions()))
	require.False(t, c.HasExpensiveAggregations)
	for i := 1; i <= 100; i++ {
		c.Update(time.Now(), int64(i))
	}
	require.Equal(t, int64(5050), c.Sum())
	require.Equal(t, 5050.0, c.ValueOf(aggregation.Sum))
	require.Equal(t, 100.0, c.ValueOf(aggregation.Count))
	require.Equal(t, 50.5, c.ValueOf(aggregation.Mean))
}

func TestCounterCustomAggregationType(t *testing.T) {
	opts := NewOptions(instrument.NewOptions())
	opts.HasExpensiveAggregations = true

	c := NewCounter(opts)
	require.True(t, c.HasExpensiveAggregations)

	for i := 1; i <= 100; i++ {
		c.Update(time.Now(), int64(i))
	}
	require.Equal(t, int64(5050), c.Sum())
	for aggType := range aggregation.ValidTypes {
		v := c.ValueOf(aggType)
		switch aggType {
		case aggregation.Min:
			require.Equal(t, float64(1), v)
		case aggregation.Max:
			require.Equal(t, float64(100), v)
		case aggregation.Mean:
			require.Equal(t, 50.5, v)
		case aggregation.Count:
			require.Equal(t, float64(100), v)
		case aggregation.Sum:
			require.Equal(t, float64(5050), v)
		case aggregation.SumSq:
			require.Equal(t, float64(338350), v)
		case aggregation.Stdev:
			require.InDelta(t, 29.01149, v, 0.001)
		case aggregation.Last:
			require.Equal(t, 0.0, v)
		default:
			require.Equal(t, float64(0), v)
			require.False(t, aggType.IsValidForCounter())
		}
	}
}
