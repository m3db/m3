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
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"

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

func testStreamOptions() cm.Options {
	return cm.NewOptions()
}

func getTimerSamples(
	num int,
	generator func(*rand.Rand) float64,
	q []float64,
) ([]float64, []float64) {
	if generator == nil {
		generator = func(r *rand.Rand) float64 {
			return r.Float64()
		}
	}
	var (
		quantiles = make([]float64, len(q))
		samples   = make([]float64, num)
		sorted    = make([]float64, num)
		rnd       = rand.New(rand.NewSource(0)) //nolint:gosec
	)

	for i := 0; i < len(samples); i++ {
		samples[i] = generator(rnd)
	}

	copy(sorted, samples)
	sort.Float64s(sorted)
	n := float64(len(sorted) - 1)
	for i, quantile := range q {
		quantiles[i] = sorted[int(n*quantile)]
	}

	return samples, quantiles
}

func TestCreateTimerResetStream(t *testing.T) {
	floatsPool := pool.NewFloatsPool([]pool.Bucket{{Capacity: 2048, Count: 100}}, nil)
	floatsPool.Init()
	streamOpts := cm.NewOptions()
	// Add a value to the timer and close the timer, which returns the
	// underlying stream to the pool.
	timer := NewTimer(testQuantiles, streamOpts, NewOptions(instrument.NewOptions()))
	timer.Add(time.Now(), 1.0, nil)
	require.Equal(t, 1.0, timer.Min())
	timer.Close()

	// Create a new timer and assert the underlying stream has been closed.
	timer = NewTimer(testQuantiles, streamOpts, NewOptions(instrument.NewOptions()))
	timer.Add(time.Now(), 1.0, nil)
	require.Equal(t, 1.0, timer.Min())
	timer.Close()
	require.Equal(t, 0.0, timer.stream.Min())
}

func TestTimerAggregations(t *testing.T) {
	opts := NewOptions(instrument.NewOptions())
	opts.ResetSetData(testAggTypes)

	timer := NewTimer(testQuantiles, testStreamOptions(), opts)

	// Assert the state of an empty timer.
	require.True(t, timer.hasExpensiveAggregations)
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
	at := time.Now()
	for i := 1; i <= 100; i++ {
		timer.Add(at, float64(i), nil)
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
	opts := NewOptions(instrument.NewOptions())
	opts.ResetSetData(aggregation.Types{aggregation.Sum})

	timer := NewTimer(testQuantiles, testStreamOptions(), opts)

	// Assert the state of an empty timer.
	require.False(t, timer.hasExpensiveAggregations)

	// Add values.
	at := time.Now()
	for i := 1; i <= 100; i++ {
		timer.Add(at, float64(i), nil)
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

func TestTimerReturnsLastNonEmptyAnnotation(t *testing.T) {
	opts := NewOptions(instrument.NewOptions())
	opts.ResetSetData(testAggTypes)
	timer := NewTimer(testQuantiles, cm.NewOptions(), opts)

	timer.Add(time.Now(), 1.1, []byte("first"))
	timer.Add(time.Now(), 2.1, []byte("second"))
	timer.Add(time.Now(), 3.1, nil)

	require.Equal(t, []byte("second"), timer.Annotation())
}
