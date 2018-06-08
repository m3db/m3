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
	"github.com/m3db/m3aggregator/aggregation"
	"github.com/m3db/m3metrics/metric/unaggregated"
)

// counterAggregation is a counter aggregation.
type counterAggregation struct {
	aggregation.Counter
}

func newCounterAggregation(c aggregation.Counter) counterAggregation {
	return counterAggregation{Counter: c}
}

func (c *counterAggregation) Add(value float64)                    { c.Counter.Update(int64(value)) }
func (c *counterAggregation) AddUnion(mu unaggregated.MetricUnion) { c.Counter.Update(mu.CounterVal) }

// timerAggregation is a timer aggregation.
type timerAggregation struct {
	aggregation.Timer
}

func newTimerAggregation(t aggregation.Timer) timerAggregation   { return timerAggregation{Timer: t} }
func (t *timerAggregation) Add(value float64)                    { t.Timer.Add(value) }
func (t *timerAggregation) AddUnion(mu unaggregated.MetricUnion) { t.Timer.AddBatch(mu.BatchTimerVal) }

// gaugeAggregation is a gauge aggregation.
type gaugeAggregation struct {
	aggregation.Gauge
}

func newGaugeAggregation(g aggregation.Gauge) gaugeAggregation   { return gaugeAggregation{Gauge: g} }
func (g *gaugeAggregation) Add(value float64)                    { g.Gauge.Update(value) }
func (g *gaugeAggregation) AddUnion(mu unaggregated.MetricUnion) { g.Gauge.Update(mu.GaugeVal) }
