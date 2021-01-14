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

package aggregated

import (
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"
)

var (
	errNilForwardedMetricWithMetadataProto   = errors.New("nil forwarded metric with metadata proto message")
	errNilTimedMetricWithMetadataProto       = errors.New("nil timed metric with metadata proto message")
	errNilPassthroughMetricWithMetadataProto = errors.New("nil passthrough metric with metadata proto message")
)

// Metric is a metric, which is essentially a named value at certain time.
type Metric struct {
	Type      metric.Type
	ID        id.RawID
	TimeNanos int64
	Value     float64
}

// ToProto converts the metric to a protobuf message in place.
func (m Metric) ToProto(pb *metricpb.TimedMetric) error {
	if err := m.Type.ToProto(&pb.Type); err != nil {
		return err
	}
	pb.Id = m.ID
	pb.TimeNanos = m.TimeNanos
	pb.Value = m.Value
	return nil
}

// FromProto converts the protobuf message to a metric in place.
func (m *Metric) FromProto(pb metricpb.TimedMetric) error {
	if err := m.Type.FromProto(pb.Type); err != nil {
		return err
	}
	m.ID = pb.Id
	m.TimeNanos = pb.TimeNanos
	m.Value = pb.Value
	return nil
}

// String is the string representation of a metric.
func (m Metric) String() string {
	return fmt.Sprintf(
		"{id:%s,timestamp:%s,value:%f}",
		m.ID.String(),
		time.Unix(0, m.TimeNanos).String(),
		m.Value,
	)
}

// ChunkedMetric is a metric with a chunked ID.
type ChunkedMetric struct {
	id.ChunkedID
	Type      metric.Type
	TimeNanos int64
	Value     float64
}

// RawMetric is a metric in its raw form (e.g., encoded bytes associated with
// a metric object).
type RawMetric interface {
	// ID is the metric identifier.
	ID() (id.RawID, error)

	// TimeNanos is the metric timestamp in nanoseconds.
	TimeNanos() (int64, error)

	// Value is the metric value.
	Value() (float64, error)

	// Metric is the metric object represented by the raw metric.
	Metric() (Metric, error)

	// Bytes are the bytes backing this raw metric.
	Bytes() []byte

	// Reset resets the raw data.
	Reset(data []byte)
}

// MetricWithStoragePolicy is a metric with applicable storage policy.
type MetricWithStoragePolicy struct {
	Metric
	policy.StoragePolicy
}

// ToProto converts the chunked metric with storage policy to a protobuf message in place.
func (m MetricWithStoragePolicy) ToProto(pb *metricpb.TimedMetricWithStoragePolicy) error {
	if err := m.Metric.ToProto(&pb.TimedMetric); err != nil {
		return err
	}

	return m.StoragePolicy.ToProto(&pb.StoragePolicy)
}

// FromProto converts the protobuf message to a chunked metric with storage policy in place.
func (m *MetricWithStoragePolicy) FromProto(pb metricpb.TimedMetricWithStoragePolicy) error {
	if err := m.Metric.FromProto(pb.TimedMetric); err != nil {
		return err
	}
	return m.StoragePolicy.FromProto(pb.StoragePolicy)
}

// String is the string representation of a metric with storage policy.
func (m MetricWithStoragePolicy) String() string {
	return fmt.Sprintf("{metric:%s,policy:%s}", m.Metric.String(), m.StoragePolicy.String())
}

// ChunkedMetricWithStoragePolicy is a chunked metric with applicable storage policy.
type ChunkedMetricWithStoragePolicy struct {
	ChunkedMetric
	policy.StoragePolicy
}

// ForwardedMetric is a forwarded metric.
type ForwardedMetric struct {
	Type      metric.Type
	ID        id.RawID
	TimeNanos int64
	Values    []float64
}

// ToProto converts the forwarded metric to a protobuf message in place.
func (m ForwardedMetric) ToProto(pb *metricpb.ForwardedMetric) error {
	if err := m.Type.ToProto(&pb.Type); err != nil {
		return err
	}
	pb.Id = m.ID
	pb.TimeNanos = m.TimeNanos
	pb.Values = m.Values
	return nil
}

// FromProto converts the protobuf message to a forwarded metric in place.
func (m *ForwardedMetric) FromProto(pb metricpb.ForwardedMetric) error {
	if err := m.Type.FromProto(pb.Type); err != nil {
		return err
	}
	m.ID = pb.Id
	m.TimeNanos = pb.TimeNanos
	m.Values = pb.Values
	return nil
}

// String is a string representation of the forwarded metric.
func (m ForwardedMetric) String() string {
	return fmt.Sprintf(
		"{id:%s,timestamp:%s,values:%v}",
		m.ID.String(),
		time.Unix(0, m.TimeNanos).String(),
		m.Values,
	)
}

// ForwardedMetricWithMetadata is a forwarded metric with metadata.
type ForwardedMetricWithMetadata struct {
	ForwardedMetric
	metadata.ForwardMetadata
}

// ToProto converts the forwarded metric with metadata to a protobuf message in place.
func (fm ForwardedMetricWithMetadata) ToProto(pb *metricpb.ForwardedMetricWithMetadata) error {
	if err := fm.ForwardedMetric.ToProto(&pb.Metric); err != nil {
		return err
	}
	return fm.ForwardMetadata.ToProto(&pb.Metadata)
}

// FromProto converts the protobuf message to a forwarded metric with metadata in place.
func (fm *ForwardedMetricWithMetadata) FromProto(pb *metricpb.ForwardedMetricWithMetadata) error {
	if pb == nil {
		return errNilForwardedMetricWithMetadataProto
	}
	if err := fm.ForwardedMetric.FromProto(pb.Metric); err != nil {
		return err
	}
	return fm.ForwardMetadata.FromProto(pb.Metadata)
}

// TimedMetricWithMetadata is a timed metric with metadata.
type TimedMetricWithMetadata struct {
	Metric
	metadata.TimedMetadata
}

// ToProto converts the timed metric with metadata to a protobuf message in place.
func (tm TimedMetricWithMetadata) ToProto(pb *metricpb.TimedMetricWithMetadata) error {
	if err := tm.Metric.ToProto(&pb.Metric); err != nil {
		return err
	}
	return tm.TimedMetadata.ToProto(&pb.Metadata)
}

// FromProto converts the protobuf message to a timed metric with metadata in place.
func (tm *TimedMetricWithMetadata) FromProto(pb *metricpb.TimedMetricWithMetadata) error {
	if pb == nil {
		return errNilTimedMetricWithMetadataProto
	}
	if err := tm.Metric.FromProto(pb.Metric); err != nil {
		return err
	}
	return tm.TimedMetadata.FromProto(pb.Metadata)
}

// TimedMetricWithMetadatas is a timed metric with staged metadatas.
type TimedMetricWithMetadatas struct {
	Metric
	metadata.StagedMetadatas
}

// ToProto converts the timed metric with metadata to a protobuf message in place.
func (tm TimedMetricWithMetadatas) ToProto(pb *metricpb.TimedMetricWithMetadatas) error {
	if err := tm.Metric.ToProto(&pb.Metric); err != nil {
		return err
	}
	return tm.StagedMetadatas.ToProto(&pb.Metadatas)
}

// FromProto converts the protobuf message to a timed metric with metadata in place.
func (tm *TimedMetricWithMetadatas) FromProto(pb *metricpb.TimedMetricWithMetadatas) error {
	if pb == nil {
		return errNilTimedMetricWithMetadataProto
	}
	if err := tm.Metric.FromProto(pb.Metric); err != nil {
		return err
	}
	return tm.StagedMetadatas.FromProto(pb.Metadatas)
}

// PassthroughMetricWithMetadata is a passthrough metric with metadata.
type PassthroughMetricWithMetadata struct {
	Metric
	policy.StoragePolicy
}

// ToProto converts the passthrough metric with metadata to a protobuf message in place.
func (pm PassthroughMetricWithMetadata) ToProto(pb *metricpb.TimedMetricWithStoragePolicy) error {
	if err := pm.Metric.ToProto(&pb.TimedMetric); err != nil {
		return err
	}
	return pm.StoragePolicy.ToProto(&pb.StoragePolicy)
}

// FromProto converts the protobuf message to a timed metric with metadata in place.
func (pm *PassthroughMetricWithMetadata) FromProto(pb *metricpb.TimedMetricWithStoragePolicy) error {
	if pb == nil {
		return errNilPassthroughMetricWithMetadataProto
	}
	if err := pm.Metric.FromProto(pb.TimedMetric); err != nil {
		return err
	}
	return pm.StoragePolicy.FromProto(pb.StoragePolicy)
}
