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

	"github.com/m3db/m3metrics/policy"

	"github.com/stretchr/testify/require"
)

func TestGaugeDefaultAggregationType(t *testing.T) {
	g := NewGauge(NewOptions())
	for i := 1.0; i <= 100.0; i++ {
		g.Update(i)
	}
	require.Equal(t, 100.0, g.Last())
	require.Equal(t, 100.0, g.ValueOf(policy.Last))
	require.Equal(t, 0.0, g.ValueOf(policy.Count))
	require.Equal(t, 0.0, g.ValueOf(policy.Mean))
}

func TestGaugeCustomAggregationType(t *testing.T) {
	opts := NewOptions()
	opts.UseDefaultAggregation = false
	opts.HasExpensiveAggregations = true

	g := NewGauge(opts)

	for i := 1; i <= 100; i++ {
		g.Update(float64(i))
	}

	require.Equal(t, 100.0, g.Last())
	for aggType := range policy.ValidAggregationTypes {
		v := g.ValueOf(aggType)
		switch aggType {
		case policy.Last:
			require.Equal(t, float64(100), v)
		case policy.Lower:
			require.Equal(t, float64(1), v)
		case policy.Upper:
			require.Equal(t, float64(100), v)
		case policy.Mean:
			require.Equal(t, float64(50.5), v)
		case policy.Count:
			require.Equal(t, float64(100), v)
		case policy.Sum:
			require.Equal(t, float64(5050), v)
		case policy.SumSq:
			require.Equal(t, float64(338350), v)
		case policy.Stdev:
			require.InDelta(t, 29.01149, v, 0.001)
		default:
			require.Equal(t, float64(0), v)
			require.False(t, aggType.IsValidForGauge())
		}
	}
}
