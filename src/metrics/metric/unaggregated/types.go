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
	"errors"
	"fmt"

	"github.com/m3db/m3metrics/generated/proto/metricpb"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/pool"
)

var (
	errNilCounterWithMetadatasProto    = errors.New("nil counter with metadatas proto message")
	errNilBatchTimerWithMetadatasProto = errors.New("nil batch timer with metadatas proto message")
	errNilGaugeWithMetadatasProto      = errors.New("nil gauge with metadatas proto message")
)

// Type is a metric type.
type Type int8

// List of supported metric types.
const (
	UnknownType Type = iota
	CounterType
	BatchTimerType
	GaugeType
)

func (t Type) String() string {
	switch t {
	case CounterType:
		return "counter"
	case BatchTimerType:
		return "batchTimer"
	case GaugeType:
		return "gauge"
	default:
		return "unknown"
	}
}

// Counter is a counter containing the counter ID and the counter value.
type Counter struct {
	ID    id.RawID
	Value int64
}

// ToProto converts the counter to a protobuf message in place.
func (c Counter) ToProto(pb *metricpb.Counter) {
	pb.Id = c.ID
	pb.Value = c.Value
}

// FromProto converts the protobuf message to a counter in place.
func (c *Counter) FromProto(pb metricpb.Counter) {
	c.ID = pb.Id
	c.Value = pb.Value
}

// BatchTimer is a timer containing the timer ID and a list of timer values.
type BatchTimer struct {
	ID     id.RawID
	Values []float64
}

// ToProto converts the batch timer to a protobuf message in place.
func (t BatchTimer) ToProto(pb *metricpb.BatchTimer) {
	pb.Id = t.ID
	pb.Values = t.Values
}

// FromProto converts the protobuf message to a batch timer in place.
func (t *BatchTimer) FromProto(pb metricpb.BatchTimer) {
	t.ID = pb.Id
	t.Values = pb.Values
}

// Gauge is a gauge containing the gauge ID and the value at certain time.
type Gauge struct {
	ID    id.RawID
	Value float64
}

// ToProto converts the gauge to a protobuf message in place.
func (g Gauge) ToProto(pb *metricpb.Gauge) {
	pb.Id = g.ID
	pb.Value = g.Value
}

// FromProto converts the protobuf message to a gauge in place.
func (g *Gauge) FromProto(pb metricpb.Gauge) {
	g.ID = pb.Id
	g.Value = pb.Value
}

// CounterWithPoliciesList is a counter with applicable policies list.
type CounterWithPoliciesList struct {
	Counter
	policy.PoliciesList
}

// BatchTimerWithPoliciesList is a batch timer with applicable policies list.
type BatchTimerWithPoliciesList struct {
	BatchTimer
	policy.PoliciesList
}

// GaugeWithPoliciesList is a gauge with applicable policies list.
type GaugeWithPoliciesList struct {
	Gauge
	policy.PoliciesList
}

// CounterWithMetadatas is a counter with applicable metadatas.
type CounterWithMetadatas struct {
	Counter
	metadata.StagedMetadatas
}

// ToProto converts the counter with metadatas to a protobuf message in place.
func (cm CounterWithMetadatas) ToProto(pb *metricpb.CounterWithMetadatas) error {
	if err := cm.StagedMetadatas.ToProto(&pb.Metadatas); err != nil {
		return err
	}
	cm.Counter.ToProto(&pb.Counter)
	return nil
}

// FromProto converts the protobuf message to a counter with metadatas in place.
func (cm *CounterWithMetadatas) FromProto(pb *metricpb.CounterWithMetadatas) error {
	if pb == nil {
		return errNilCounterWithMetadatasProto
	}
	if err := cm.StagedMetadatas.FromProto(pb.Metadatas); err != nil {
		return err
	}
	cm.Counter.FromProto(pb.Counter)
	return nil
}

// BatchTimerWithMetadatas is a batch timer with applicable metadatas.
type BatchTimerWithMetadatas struct {
	BatchTimer
	metadata.StagedMetadatas
}

// ToProto converts the batch timer with metadatas to a protobuf message in place.
func (bm BatchTimerWithMetadatas) ToProto(pb *metricpb.BatchTimerWithMetadatas) error {
	if err := bm.StagedMetadatas.ToProto(&pb.Metadatas); err != nil {
		return errNilBatchTimerWithMetadatasProto
	}
	bm.BatchTimer.ToProto(&pb.BatchTimer)
	return nil
}

// FromProto converts the protobuf message to a batch timer with metadatas in place.
func (bm *BatchTimerWithMetadatas) FromProto(pb *metricpb.BatchTimerWithMetadatas) error {
	if pb == nil {
		return errNilBatchTimerWithMetadatasProto
	}
	if err := bm.StagedMetadatas.FromProto(pb.Metadatas); err != nil {
		return err
	}
	bm.BatchTimer.FromProto(pb.BatchTimer)
	return nil
}

// GaugeWithMetadatas is a gauge with applicable metadatas.
type GaugeWithMetadatas struct {
	Gauge
	metadata.StagedMetadatas
}

// ToProto converts the gauge with metadatas to a protobuf message in place.
func (gm GaugeWithMetadatas) ToProto(pb *metricpb.GaugeWithMetadatas) error {
	if err := gm.StagedMetadatas.ToProto(&pb.Metadatas); err != nil {
		return errNilGaugeWithMetadatasProto
	}
	gm.Gauge.ToProto(&pb.Gauge)
	return nil
}

// FromProto converts the protobuf message to a gauge with metadatas in place.
func (gm *GaugeWithMetadatas) FromProto(pb *metricpb.GaugeWithMetadatas) error {
	if pb == nil {
		return errNilGaugeWithMetadatasProto
	}
	if err := gm.StagedMetadatas.FromProto(pb.Metadatas); err != nil {
		return err
	}
	gm.Gauge.FromProto(pb.Gauge)
	return nil
}

// MetricUnion is a union of different types of metrics, only one of which is valid
// at any given time. The actual type of the metric depends on the type field,
// which determines which value field is valid. Note that if the timer values are
// allocated from a pool, the TimerValPool should be set to the originating pool,
// and the caller is responsible for returning the timer values to the pool.
// NB(xichen): possibly use refcounting to replace explicit ownership tracking.
type MetricUnion struct {
	Type          Type
	OwnsID        bool
	ID            id.RawID
	CounterVal    int64
	BatchTimerVal []float64
	GaugeVal      float64
	TimerValPool  pool.FloatsPool
}

var emptyMetricUnion MetricUnion

// String is the string representation of a metric union.
func (m *MetricUnion) String() string {
	switch m.Type {
	case CounterType:
		return fmt.Sprintf("{type:%s,id:%s,value:%d}", m.Type, m.ID.String(), m.CounterVal)
	case BatchTimerType:
		return fmt.Sprintf("{type:%s,id:%s,value:%v}", m.Type, m.ID.String(), m.BatchTimerVal)
	case GaugeType:
		return fmt.Sprintf("{type:%s,id:%s,value:%f}", m.Type, m.ID.String(), m.GaugeVal)
	default:
		return fmt.Sprintf(
			"{type:%d,id:%s,counterVal:%d,batchTimerVal:%v,gaugeVal:%f}",
			m.Type, m.ID.String(), m.CounterVal, m.BatchTimerVal, m.GaugeVal,
		)
	}
}

// Reset resets the metric union.
func (m *MetricUnion) Reset() { *m = emptyMetricUnion }

// Counter returns the counter metric.
func (m *MetricUnion) Counter() Counter { return Counter{ID: m.ID, Value: m.CounterVal} }

// BatchTimer returns the batch timer metric.
func (m *MetricUnion) BatchTimer() BatchTimer { return BatchTimer{ID: m.ID, Values: m.BatchTimerVal} }

// Gauge returns the gauge metric.
func (m *MetricUnion) Gauge() Gauge { return Gauge{ID: m.ID, Value: m.GaugeVal} }
