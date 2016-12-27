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

package unaggregated

import (
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
)

// Type is a metric type
type Type int8

// List of supported metric types
const (
	UnknownType Type = iota
	CounterType
	BatchTimerType
	GaugeType
)

// Counter is a counter containing the counter ID and the counter value
type Counter struct {
	ID    metric.ID
	Value int64
}

// BatchTimer is a timer containing the timer ID and a list of timer values
type BatchTimer struct {
	ID     metric.ID
	Values []float64
}

// Gauge is a gauge containing the gauge ID and the value at certain time
type Gauge struct {
	ID    metric.ID
	Value float64
}

// CounterWithPolicies is a counter with applicable policies
type CounterWithPolicies struct {
	Counter
	policy.VersionedPolicies
}

// BatchTimerWithPolicies is a batch timer with applicable policies
type BatchTimerWithPolicies struct {
	BatchTimer
	policy.VersionedPolicies
}

// GaugeWithPolicies is a gauge with applicable policies
type GaugeWithPolicies struct {
	Gauge
	policy.VersionedPolicies
}

// MetricUnion is a union of different types of metrics, only one of which is valid
// at any given time. The actual type of the metric depends on the type field,
// which determines which value field is valid. We intentionally do not use value
// pointers and nil checks to determine which type is valid in order to avoid the GC
// overhead of marking and sweeping the metrics.
type MetricUnion struct {
	Type          Type
	ID            metric.ID
	CounterVal    int64
	BatchTimerVal []float64
	GaugeVal      float64
}

var emptyMetricUnion MetricUnion

// Reset resets the metric union
func (m *MetricUnion) Reset() { *m = emptyMetricUnion }

// Counter returns the counter metric
func (m *MetricUnion) Counter() Counter { return Counter{ID: m.ID, Value: m.CounterVal} }

// BatchTimer returns the batch timer metric
func (m *MetricUnion) BatchTimer() BatchTimer { return BatchTimer{ID: m.ID, Values: m.BatchTimerVal} }

// Gauge returns the gauge metric
func (m *MetricUnion) Gauge() Gauge { return Gauge{ID: m.ID, Value: m.GaugeVal} }
