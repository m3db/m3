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
	"github.com/stretchr/testify/require"
)

func TestTimer(t *testing.T) {
	timer := Timer{stream: cm.NewStream(cm.NewOptions())}

	// Assert the state of an empty timer
	require.Equal(t, int64(0), timer.Count())
	require.Equal(t, 0.0, timer.Sum())
	require.Equal(t, 0.0, timer.SumSq())
	require.Equal(t, 0.0, timer.Min())
	require.Equal(t, 0.0, timer.Max())
	require.Equal(t, 0.0, timer.Mean())
	require.Equal(t, 0.0, timer.Stddev())
	require.Equal(t, 0.0, timer.Quantile(0.5))
	require.Equal(t, 0.0, timer.Quantile(0.95))
	require.Equal(t, 0.0, timer.Quantile(0.99))

	// Add values
	for i := 1; i <= 100; i++ {
		timer.Add(float64(i))
	}

	// Validate the timer values match expectations
	require.Equal(t, int64(100), timer.Count())
	require.Equal(t, 5050.0, timer.Sum())
	require.Equal(t, 338350.0, timer.SumSq())
	require.Equal(t, 1.0, timer.Min())
	require.Equal(t, 100.0, timer.Max())
	require.Equal(t, 50.5, timer.Mean())
	require.Equal(t, 29.011, math.Trunc(timer.Stddev()*1000+0.5)/1000.0)
	require.Equal(t, 50.0, timer.Quantile(0.5))
	require.True(t, timer.Quantile(0.95) >= 94 && timer.Quantile(0.95) <= 96)
	require.True(t, timer.Quantile(0.99) >= 98 && timer.Quantile(0.99) <= 100)

	// Closing the timer should close the underlying stream
	timer.Close()
	require.Equal(t, 0.0, timer.stream.Quantile(0.5))

	// Closing the timer a second time should be a no op
	timer.Close()

	// Resetting the timer should reset the underlying stream
	timer.Reset()

	var batch []float64
	for i := 1; i <= 100; i++ {
		batch = append(batch, float64(i))
	}
	timer.AddBatch(batch)
	require.Equal(t, 5050.0, timer.Sum())
	require.Equal(t, 50.0, timer.Quantile(0.5))
}
