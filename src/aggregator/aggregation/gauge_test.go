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
	"github.com/uber-go/tally"
)

func TestGaugeDefaultAggregationType(t *testing.T) {
	g := NewGauge(NewOptions(instrument.NewOptions()))
	require.False(t, g.HasExpensiveAggregations)
	for i := 1.0; i <= 100.0; i++ {
		g.Update(time.Now(), i)
	}
	require.Equal(t, 100.0, g.Last())
	require.Equal(t, 100.0, g.ValueOf(aggregation.Last))
	require.Equal(t, 100.0, g.ValueOf(aggregation.Count))
	require.Equal(t, 50.5, g.ValueOf(aggregation.Mean))
	require.Equal(t, 0.0, g.ValueOf(aggregation.SumSq))
}

func TestGaugeCustomAggregationType(t *testing.T) {
	opts := NewOptions(instrument.NewOptions())
	opts.HasExpensiveAggregations = true

	g := NewGauge(opts)
	require.True(t, g.HasExpensiveAggregations)

	for i := 1; i <= 100; i++ {
		g.Update(time.Now(), float64(i))
	}

	require.Equal(t, 100.0, g.Last())
	for aggType := range aggregation.ValidTypes {
		v := g.ValueOf(aggType)
		switch aggType {
		case aggregation.Last:
			require.Equal(t, float64(100), v)
		case aggregation.Min:
			require.Equal(t, float64(1), v)
		case aggregation.Max:
			require.Equal(t, float64(100), v)
		case aggregation.Mean:
			require.Equal(t, float64(50.5), v)
		case aggregation.Count:
			require.Equal(t, float64(100), v)
		case aggregation.Sum:
			require.Equal(t, float64(5050), v)
		case aggregation.SumSq:
			require.Equal(t, float64(338350), v)
		case aggregation.Stdev:
			require.InDelta(t, 29.01149, v, 0.001)
		default:
			require.Equal(t, float64(0), v)
			require.False(t, aggType.IsValidForGauge())
		}
	}
}

func TestGaugeLastOutOfOrderValues(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	g := NewGauge(NewOptions(instrument.NewOptions().SetMetricsScope(scope)))

	timeMid := time.Now().Add(time.Minute)
	timePre := timeMid.Add(-1 * time.Second)
	timePrePre := timeMid.Add(-1 * time.Second)
	timeAfter := timeMid.Add(time.Second)

	g.Update(timeMid, 42)
	g.Update(timePre, 41)
	g.Update(timeAfter, 43)
	g.Update(timePrePre, 40)

	require.Equal(t, 43.0, g.Last())
	snap := scope.Snapshot()
	counters := snap.Counters()
	counter, ok := counters["aggregation.gauges.values-out-of-order+"]
	require.True(t, ok)
	require.Equal(t, int64(2), counter.Value())
}
