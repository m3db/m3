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
	"sync"

	"github.com/m3db/m3aggregator/aggregation"
	"github.com/m3db/m3metrics/metric/unaggregated"
)

// lockedCounter is a locked counter.
type lockedCounter struct {
	sync.Mutex
	aggregation.Counter
}

func newLockedCounter(c aggregation.Counter) *lockedCounter { return &lockedCounter{Counter: c} }
func (c *lockedCounter) Add(mu unaggregated.MetricUnion)    { c.Counter.Update(mu.CounterVal) }

// lockedTimer is a locked timer.
type lockedTimer struct {
	sync.Mutex
	aggregation.Timer
}

func newLockedTimer(t aggregation.Timer) *lockedTimer  { return &lockedTimer{Timer: t} }
func (t *lockedTimer) Add(mu unaggregated.MetricUnion) { t.Timer.AddBatch(mu.BatchTimerVal) }

// lockedGauge is a locked gauge.
type lockedGauge struct {
	sync.Mutex
	aggregation.Gauge
}

func newLockedGauge(g aggregation.Gauge) *lockedGauge  { return &lockedGauge{Gauge: g} }
func (g *lockedGauge) Add(mu unaggregated.MetricUnion) { g.Gauge.Update(mu.GaugeVal) }
