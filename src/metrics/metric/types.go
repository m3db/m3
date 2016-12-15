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

package metric

import "time"

// Type is a metric type
type Type int8

// List of supported metric types
const (
	UnknownType Type = iota
	CounterType
	BatchTimerType
	GaugeType
)

// IDType is the metric id type
type IDType []byte

// String is the string representation of an id
func (id IDType) String() string { return string(id) }

// Counter is a counter containing the counter ID and the counter value
type Counter struct {
	ID    IDType
	Value int64
}

// BatchTimer is a timer containing the timer ID and a list of timer values
type BatchTimer struct {
	ID     IDType
	Values []float64
}

// Gauge is a gauge containing the gauge ID and the value at certain time
type Gauge struct {
	ID    IDType
	Value float64
}

// RawMetric represents an unaggregated raw metric, which consists of a union
// of different types of metrics, only one of which is valid at any given time.
// The actual type of the metric depends on the type field, which determines
// which value field is valid. We intentionally do not use pointers to avoid
// the GC overhead of marking and sweeping the raw metrics.
type RawMetric struct {
	Type          Type
	ID            IDType
	CounterVal    int64
	BatchTimerVal []float64
	GaugeVal      float64
}

var emptyRawMetric RawMetric

// Reset resets the raw metric
func (m *RawMetric) Reset() { *m = emptyRawMetric }

// Counter returns the counter metric
func (m *RawMetric) Counter() Counter { return Counter{ID: m.ID, Value: m.CounterVal} }

// BatchTimer returns the batch timer metric
func (m *RawMetric) BatchTimer() BatchTimer { return BatchTimer{ID: m.ID, Values: m.BatchTimerVal} }

// Gauge returns the gauge metric
func (m *RawMetric) Gauge() Gauge { return Gauge{ID: m.ID, Value: m.GaugeVal} }

// Metric is the metric interface
type Metric interface {
	// ID is the metric id
	ID() IDType

	// Timestamp is the metric timestamp
	Timestamp() time.Time

	// Value is the metric value
	Value() float64
}
