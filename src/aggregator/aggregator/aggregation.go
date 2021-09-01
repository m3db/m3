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
	"errors"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregation"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
)

// counterAggregation is a counter aggregation.
type counterAggregation struct {
	aggregation.Counter
}

func newCounterAggregation(c aggregation.Counter) counterAggregation {
	return counterAggregation{Counter: c}
}

func (a *counterAggregation) Add(t time.Time, value float64, annotation []byte) {
	a.Counter.Update(t, int64(value), annotation)
}

func (a *counterAggregation) UpdateVal(t time.Time, value float64, prevValue float64) error {
	return errors.New("counters do not support updating values")
}

func (a *counterAggregation) AddUnion(t time.Time, mu unaggregated.MetricUnion) {
	a.Counter.Update(t, mu.CounterVal, mu.Annotation)
}

// timerAggregation is a timer aggregation.
type timerAggregation struct {
	aggregation.Timer
}

func newTimerAggregation(t aggregation.Timer) timerAggregation {
	return timerAggregation{Timer: t}
}

func (a *timerAggregation) Add(timestamp time.Time, value float64, annotation []byte) {
	a.Timer.Add(timestamp, value, annotation)
}

func (a *timerAggregation) UpdateVal(t time.Time, value float64, prevValue float64) error {
	return errors.New("timers do not support updating values")
}

func (a *timerAggregation) AddUnion(timestamp time.Time, mu unaggregated.MetricUnion) {
	a.Timer.AddBatch(timestamp, mu.BatchTimerVal, mu.Annotation)
}

// gaugeAggregation is a gauge aggregation.
type gaugeAggregation struct {
	aggregation.Gauge
}

func newGaugeAggregation(g aggregation.Gauge) gaugeAggregation {
	return gaugeAggregation{Gauge: g}
}

func (a *gaugeAggregation) Add(t time.Time, value float64, annotation []byte) {
	a.Gauge.Update(t, value, annotation)
}

func (a *gaugeAggregation) UpdateVal(t time.Time, value float64, prevValue float64) error {
	a.Gauge.UpdatePrevious(t, value, prevValue)
	return nil
}

func (a *gaugeAggregation) AddUnion(t time.Time, mu unaggregated.MetricUnion) {
	a.Gauge.Update(t, mu.GaugeVal, mu.Annotation)
}
