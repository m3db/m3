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

package aggregator

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregation"
	"github.com/m3db/m3/src/aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/require"
)

var (
	testAggregationValues = []float64{1.2, 5, 789, 4.0}
	testAggregationUnions = []unaggregated.MetricUnion{
		{
			Type:       metric.CounterType,
			ID:         testCounterID,
			CounterVal: 1234,
		},
		{
			Type:          metric.TimerType,
			ID:            testBatchTimerID,
			BatchTimerVal: []float64{1.0, 3.5, 2.2, 6.5, 4.8},
		},
		{
			Type:     metric.GaugeType,
			ID:       testGaugeID,
			GaugeVal: 123.456,
		},
	}
)

func TestCounterAggregationAdd(t *testing.T) {
	c := newCounterAggregation(aggregation.NewCounter(aggregation.NewOptions(instrument.NewOptions())))
	for _, v := range testAggregationValues {
		c.Add(time.Now(), v, nil)
	}
	require.Equal(t, int64(4), c.Count())
	require.Equal(t, int64(799), c.Sum())
}

func TestCounterAggregationAddUnion(t *testing.T) {
	c := newCounterAggregation(aggregation.NewCounter(aggregation.NewOptions(instrument.NewOptions())))
	for _, v := range testAggregationUnions {
		c.AddUnion(time.Now(), v)
	}
	require.Equal(t, int64(3), c.Count())
	require.Equal(t, int64(1234), c.Sum())
}

func TestTimerAggregationAdd(t *testing.T) {
	tm := newTimerAggregation(aggregation.NewTimer([]float64{0.5}, cm.NewOptions(), aggregation.NewOptions(instrument.NewOptions())))
	for _, v := range testAggregationValues {
		tm.Add(time.Now(), v, nil)
	}
	require.Equal(t, int64(4), tm.Count())
	require.Equal(t, 799.2, tm.Sum())
}

func TestTimerAggregationAddUnion(t *testing.T) {
	tm := newTimerAggregation(aggregation.NewTimer([]float64{0.5}, cm.NewOptions(), aggregation.NewOptions(instrument.NewOptions())))
	for _, v := range testAggregationUnions {
		tm.AddUnion(time.Now(), v)
	}
	require.Equal(t, int64(5), tm.Count())
	require.Equal(t, 18.0, tm.Sum())
}

func TestGaugeAggregationAdd(t *testing.T) {
	g := newGaugeAggregation(aggregation.NewGauge(aggregation.NewOptions(instrument.NewOptions())))
	for _, v := range testAggregationValues {
		g.Add(time.Now(), v, nil)
	}
	require.Equal(t, int64(4), g.Count())
	require.Equal(t, 799.2, g.Sum())
}

func TestGaugeAggregationAddUnion(t *testing.T) {
	g := newGaugeAggregation(aggregation.NewGauge(aggregation.NewOptions(instrument.NewOptions())))
	for _, v := range testAggregationUnions {
		g.AddUnion(time.Now(), v)
	}
	require.Equal(t, int64(3), g.Count())
	require.Equal(t, 123.456, g.Sum())
}
