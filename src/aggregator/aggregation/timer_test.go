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
	"math"
	"testing"

	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

var (
	testQuantiles = []float64{0.5, 0.95, 0.99}
	testAggTypes  = aggregation.Types{
		aggregation.Sum,
		aggregation.SumSq,
		aggregation.Mean,
		aggregation.Min,
		aggregation.Max,
		aggregation.Count,
		aggregation.Stdev,
		aggregation.Median,
		aggregation.P50,
		aggregation.P95,
		aggregation.P99,
	}
)

func TestCreateTimerResetStream(t *testing.T) {
	poolOpts := pool.NewObjectPoolOptions().SetSize(1)
	streamPool := cm.NewStreamPool(poolOpts)
	streamOpts := cm.NewOptions().SetStreamPool(streamPool)
	streamPool.Init(func() cm.Stream { return cm.NewStream(nil, streamOpts) })

	// Add a value to the timer and close the timer, which returns the
	// underlying stream to the pool.
	timer := NewTimer(testQuantiles, streamOpts, NewOptions())
	timer.Add(1.0)
	require.Equal(t, 1.0, timer.Min())
	timer.Close()

	// Create a new timer and assert the underlying stream has been closed.
	timer = NewTimer(testQuantiles, streamOpts, NewOptions())
	timer.Add(1.0)
	require.Equal(t, 1.0, timer.Min())
	timer.Close()
	require.Equal(t, 0.0, timer.stream.Min())
}

func TestTimerAggregations(t *testing.T) {
	opts := NewOptions()
	opts.ResetSetData(testAggTypes)

	timer := NewTimer(testQuantiles, cm.NewOptions(), opts)

	// Assert the state of an empty timer.
	require.True(t, timer.HasExpensiveAggregations)
	require.Equal(t, int64(0), timer.Count())
	require.Equal(t, 0.0, timer.Sum())
	require.Equal(t, 0.0, timer.SumSq())
	require.Equal(t, 0.0, timer.Min())
	require.Equal(t, 0.0, timer.Max())
	require.Equal(t, 0.0, timer.Mean())
	require.Equal(t, 0.0, timer.Stdev())
	require.Equal(t, 0.0, timer.Quantile(0.5))
	require.Equal(t, 0.0, timer.Quantile(0.95))
	require.Equal(t, 0.0, timer.Quantile(0.99))

	// Add values.
	for i := 1; i <= 100; i++ {
		timer.Add(float64(i))
	}

	// Validate the timer values match expectations.
	require.Equal(t, int64(100), timer.Count())
	require.Equal(t, 5050.0, timer.Sum())
	require.Equal(t, 338350.0, timer.SumSq())
	require.Equal(t, 1.0, timer.Min())
	require.Equal(t, 100.0, timer.Max())
	require.Equal(t, 50.5, timer.Mean())
	require.Equal(t, 29.011, math.Trunc(timer.Stdev()*1000+0.5)/1000.0)
	require.Equal(t, 50.0, timer.Quantile(0.5))
	require.True(t, timer.Quantile(0.95) >= 94 && timer.Quantile(0.95) <= 96)
	require.True(t, timer.Quantile(0.99) >= 98 && timer.Quantile(0.99) <= 100)

	for aggType := range aggregation.ValidTypes {
		v := timer.ValueOf(aggType)
		switch aggType {
		case aggregation.Last:
			require.Equal(t, 0.0, v)
		case aggregation.Min:
			require.Equal(t, 1.0, v)
		case aggregation.Max:
			require.Equal(t, 100.0, v)
		case aggregation.Mean:
			require.Equal(t, 50.5, v)
		case aggregation.Median:
			require.Equal(t, 50.0, v)
		case aggregation.Count:
			require.Equal(t, 100.0, v)
		case aggregation.Sum:
			require.Equal(t, 5050.0, v)
		case aggregation.SumSq:
			require.Equal(t, 338350.0, v)
		case aggregation.Stdev:
			require.InDelta(t, 29.01149, v, 0.001)
		case aggregation.P50:
			require.Equal(t, 50.0, v)
		case aggregation.P95:
			require.Equal(t, 95.0, v)
		case aggregation.P99:
			require.True(t, v >= 99 && v <= 100)
		}
	}
	// Closing the timer should close the underlying stream.
	timer.Close()
	require.Equal(t, 0.0, timer.stream.Quantile(0.5))

	// Closing the timer a second time should be a no op.
	timer.Close()
}

func TestTimerAggregationsNotExpensive(t *testing.T) {
	opts := NewOptions()
	opts.ResetSetData(aggregation.Types{aggregation.Sum})

	timer := NewTimer(testQuantiles, cm.NewOptions(), opts)

	// Assert the state of an empty timer.
	require.False(t, timer.HasExpensiveAggregations)

	// Add values.
	for i := 1; i <= 100; i++ {
		timer.Add(float64(i))
	}

	// All Non expensive calculations should be performed.
	require.Equal(t, int64(100), timer.Count())
	require.Equal(t, 5050.0, timer.Sum())
	require.Equal(t, 1.0, timer.Min())
	require.Equal(t, 100.0, timer.Max())
	require.Equal(t, 50.5, timer.Mean())

	// Expensive calculations are not performed.
	require.Equal(t, 0.0, timer.SumSq())

	// Closing the timer a second time should be a no op.
	timer.Close()
}
