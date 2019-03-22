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

package protobuf

import (
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
)

func resetAggregatedMetricProto(pb *metricpb.AggregatedMetric) {
	if pb == nil {
		return
	}
	resetTimedMetricWithStoragePolicyProto(&pb.Metric)
	pb.EncodeNanos = 0
}

// resetMetricWithMetadatasProto resets the metric with metadatas proto, and
// in particular message fields that are slices because the `Unmarshal` generated
// from gogoprotobuf simply append a new entry at the end of the slice, and
// as such, the fields with slice types need to be reset to be zero-length.
// NB: reset only needs to be done to the top-level slice fields as the individual
// items in the slice are created afresh during unmarshaling.
func resetMetricWithMetadatasProto(pb *metricpb.MetricWithMetadatas) {
	if pb == nil {
		return
	}
	pb.Type = metricpb.MetricWithMetadatas_UNKNOWN
	resetCounterWithMetadatasProto(pb.CounterWithMetadatas)
	resetBatchTimerWithMetadatasProto(pb.BatchTimerWithMetadatas)
	resetGaugeWithMetadatasProto(pb.GaugeWithMetadatas)
	resetForwardedMetricWithMetadataProto(pb.ForwardedMetricWithMetadata)
	resetTimedMetricWithMetadataProto(pb.TimedMetricWithMetadata)
}

func resetCounterWithMetadatasProto(pb *metricpb.CounterWithMetadatas) {
	if pb == nil {
		return
	}
	resetCounter(&pb.Counter)
	resetMetadatas(&pb.Metadatas)
}

func resetBatchTimerWithMetadatasProto(pb *metricpb.BatchTimerWithMetadatas) {
	if pb == nil {
		return
	}
	resetBatchTimer(&pb.BatchTimer)
	resetMetadatas(&pb.Metadatas)
}

func resetGaugeWithMetadatasProto(pb *metricpb.GaugeWithMetadatas) {
	if pb == nil {
		return
	}
	resetGauge(&pb.Gauge)
	resetMetadatas(&pb.Metadatas)
}

func resetForwardedMetricWithMetadataProto(pb *metricpb.ForwardedMetricWithMetadata) {
	if pb == nil {
		return
	}
	resetForwardedMetric(&pb.Metric)
	resetForwardMetadata(&pb.Metadata)
}

func resetTimedMetricWithMetadataProto(pb *metricpb.TimedMetricWithMetadata) {
	if pb == nil {
		return
	}
	resetTimedMetric(&pb.Metric)
	resetTimedMetadata(&pb.Metadata)
}

func resetTimedMetricWithStoragePolicyProto(pb *metricpb.TimedMetricWithStoragePolicy) {
	if pb == nil {
		return
	}
	resetTimedMetric(&pb.TimedMetric)
	pb.StoragePolicy.Reset()
}

func resetCounter(pb *metricpb.Counter) {
	if pb == nil {
		return
	}
	pb.Id = pb.Id[:0]
	pb.Value = 0
}

func resetBatchTimer(pb *metricpb.BatchTimer) {
	if pb == nil {
		return
	}
	pb.Id = pb.Id[:0]
	pb.Values = pb.Values[:0]
}

func resetGauge(pb *metricpb.Gauge) {
	if pb == nil {
		return
	}
	pb.Id = pb.Id[:0]
	pb.Value = 0.0
}

func resetForwardedMetric(pb *metricpb.ForwardedMetric) {
	if pb == nil {
		return
	}
	pb.Type = metricpb.MetricType_UNKNOWN
	pb.Id = pb.Id[:0]
	pb.TimeNanos = 0
	pb.Values = pb.Values[:0]
}

func resetTimedMetric(pb *metricpb.TimedMetric) {
	if pb == nil {
		return
	}
	pb.Type = metricpb.MetricType_UNKNOWN
	pb.Id = pb.Id[:0]
	pb.TimeNanos = 0
	pb.Value = 0
}

func resetMetadatas(pb *metricpb.StagedMetadatas) {
	if pb == nil {
		return
	}
	pb.Metadatas = pb.Metadatas[:0]
}

func resetForwardMetadata(pb *metricpb.ForwardMetadata) {
	if pb == nil {
		return
	}
	pb.AggregationId.Reset()
	pb.StoragePolicy.Reset()
	pb.Pipeline.Ops = pb.Pipeline.Ops[:0]
	pb.SourceId = 0
	pb.NumForwardedTimes = 0
}

func resetTimedMetadata(pb *metricpb.TimedMetadata) {
	if pb == nil {
		return
	}
	pb.AggregationId.Reset()
	pb.StoragePolicy.Reset()
}
