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

	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	errNilCounterWithMetadatasProto    = errors.New("nil counter with metadatas proto message")
	errNilBatchTimerWithMetadatasProto = errors.New("nil batch timer with metadatas proto message")
	errNilGaugeWithMetadatasProto      = errors.New("nil gauge with metadatas proto message")
)

// Counter is a counter containing the counter ID and the counter value.
type Counter struct {
	ID              id.RawID
	Annotation      []byte
	Value           int64
	ClientTimeNanos xtime.UnixNano
}

// ToUnion converts the counter to a metric union.
func (c Counter) ToUnion() MetricUnion {
	return MetricUnion{
		Type:            metric.CounterType,
		ID:              c.ID,
		CounterVal:      c.Value,
		Annotation:      c.Annotation,
		ClientTimeNanos: c.ClientTimeNanos,
	}
}

// ToProto converts the counter to a protobuf message in place.
func (c Counter) ToProto(pb *metricpb.Counter) {
	pb.Id = c.ID
	pb.Value = c.Value
	pb.Annotation = c.Annotation
	pb.ClientTimeNanos = int64(c.ClientTimeNanos)
}

// FromProto converts the protobuf message to a counter in place.
func (c *Counter) FromProto(pb metricpb.Counter) {
	c.ID = pb.Id
	c.Value = pb.Value
	c.Annotation = pb.Annotation
	c.ClientTimeNanos = xtime.UnixNano(pb.ClientTimeNanos)
}

// BatchTimer is a timer containing the timer ID and a list of timer values.
type BatchTimer struct {
	ID              id.RawID
	Values          []float64
	Annotation      []byte
	ClientTimeNanos xtime.UnixNano
}

// ToUnion converts the batch timer to a metric union.
func (t BatchTimer) ToUnion() MetricUnion {
	return MetricUnion{
		Type:            metric.TimerType,
		ID:              t.ID,
		BatchTimerVal:   t.Values,
		Annotation:      t.Annotation,
		ClientTimeNanos: t.ClientTimeNanos,
	}
}

// ToProto converts the batch timer to a protobuf message in place.
func (t BatchTimer) ToProto(pb *metricpb.BatchTimer) {
	pb.Id = t.ID
	pb.Values = t.Values
	pb.Annotation = t.Annotation
	pb.ClientTimeNanos = int64(t.ClientTimeNanos)
}

// FromProto converts the protobuf message to a batch timer in place.
func (t *BatchTimer) FromProto(pb metricpb.BatchTimer) {
	t.ID = pb.Id
	t.Values = pb.Values
	t.Annotation = pb.Annotation
}

// Gauge is a gauge containing the gauge ID and the value at certain time.
type Gauge struct {
	ID              id.RawID
	Annotation      []byte
	Value           float64
	ClientTimeNanos xtime.UnixNano
}

// ToUnion converts the gauge to a metric union.
func (g Gauge) ToUnion() MetricUnion {
	return MetricUnion{
		Type:            metric.GaugeType,
		ID:              g.ID,
		GaugeVal:        g.Value,
		Annotation:      g.Annotation,
		ClientTimeNanos: g.ClientTimeNanos,
	}
}

// ToProto converts the gauge to a protobuf message in place.
func (g Gauge) ToProto(pb *metricpb.Gauge) {
	pb.Id = g.ID
	pb.Value = g.Value
	pb.Annotation = g.Annotation
	pb.ClientTimeNanos = int64(g.ClientTimeNanos)
}

// FromProto converts the protobuf message to a gauge in place.
func (g *Gauge) FromProto(pb metricpb.Gauge) {
	g.ID = pb.Id
	g.Value = pb.Value
	g.Annotation = pb.Annotation
	g.ClientTimeNanos = xtime.UnixNano(pb.ClientTimeNanos)
}

// CounterWithPoliciesList is a counter with applicable policies list.
type CounterWithPoliciesList struct {
	policy.PoliciesList
	Counter
}

// BatchTimerWithPoliciesList is a batch timer with applicable policies list.
type BatchTimerWithPoliciesList struct {
	policy.PoliciesList
	BatchTimer
}

// GaugeWithPoliciesList is a gauge with applicable policies list.
type GaugeWithPoliciesList struct {
	policy.PoliciesList
	Gauge
}

// CounterWithMetadatas is a counter with applicable metadatas.
type CounterWithMetadatas struct {
	metadata.StagedMetadatas
	Counter
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
	metadata.StagedMetadatas
	BatchTimer
}

// ToProto converts the batch timer with metadatas to a protobuf message in place.
func (bm BatchTimerWithMetadatas) ToProto(pb *metricpb.BatchTimerWithMetadatas) error {
	if err := bm.StagedMetadatas.ToProto(&pb.Metadatas); err != nil {
		return err
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
	metadata.StagedMetadatas
	Gauge
}

// ToProto converts the gauge with metadatas to a protobuf message in place.
func (gm GaugeWithMetadatas) ToProto(pb *metricpb.GaugeWithMetadatas) error {
	if err := gm.StagedMetadatas.ToProto(&pb.Metadatas); err != nil {
		return err
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
type MetricUnion struct {
	TimerValPool    pool.FloatsPool
	Annotation      []byte
	ID              id.RawID
	BatchTimerVal   []float64
	CounterVal      int64
	GaugeVal        float64
	Type            metric.Type
	ClientTimeNanos xtime.UnixNano
}

var emptyMetricUnion MetricUnion

// String is the string representation of a metric union.
func (m *MetricUnion) String() string {
	switch m.Type {
	case metric.CounterType:
		return fmt.Sprintf("{type:%s,id:%s,value:%d}", m.Type, m.ID.String(), m.CounterVal)
	case metric.TimerType:
		return fmt.Sprintf("{type:%s,id:%s,value:%v}", m.Type, m.ID.String(), m.BatchTimerVal)
	case metric.GaugeType:
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
func (m *MetricUnion) Counter() Counter {
	return Counter{ID: m.ID, Value: m.CounterVal, Annotation: m.Annotation, ClientTimeNanos: m.ClientTimeNanos}
}

// BatchTimer returns the batch timer metric.
func (m *MetricUnion) BatchTimer() BatchTimer {
	return BatchTimer{ID: m.ID, Values: m.BatchTimerVal, Annotation: m.Annotation, ClientTimeNanos: m.ClientTimeNanos}
}

// Gauge returns the gauge metric.
func (m *MetricUnion) Gauge() Gauge {
	return Gauge{ID: m.ID, Value: m.GaugeVal, Annotation: m.Annotation, ClientTimeNanos: m.ClientTimeNanos}
}
