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
	"github.com/m3db/m3/src/metrics/policy"
)

// AggregatedDecoder is a decoder for decoding aggregated metrics.
type AggregatedDecoder struct {
	pool AggregatedDecoderPool
	pb   metricpb.AggregatedMetric
}

// NewAggregatedDecoder creates an aggregated decoder.
func NewAggregatedDecoder(p AggregatedDecoderPool) *AggregatedDecoder {
	return &AggregatedDecoder{
		pool: p,
	}
}

// Decode decodes the aggregated metric from the given bytes.
func (d *AggregatedDecoder) Decode(b []byte) error {
	return d.pb.Unmarshal(b)
}

// ID returns the decoded id.
func (d AggregatedDecoder) ID() []byte {
	return d.pb.Metric.TimedMetric.Id
}

// TimeNanos returns the decoded timestamp.
func (d AggregatedDecoder) TimeNanos() int64 {
	return d.pb.Metric.TimedMetric.TimeNanos
}

// Value returns the decoded value.
func (d AggregatedDecoder) Value() float64 {
	return d.pb.Metric.TimedMetric.Value
}

// StoragePolicy returns the decoded storage policy.
func (d AggregatedDecoder) StoragePolicy() (policy.StoragePolicy, error) {
	return policy.NewStoragePolicyFromProto(&d.pb.Metric.StoragePolicy)
}

// EncodeNanos returns the decoded encodeNanos.
func (d AggregatedDecoder) EncodeNanos() int64 {
	return d.pb.EncodeNanos
}

// Close closes the decoder.
func (d *AggregatedDecoder) Close() {
	resetAggregatedMetricProto(&d.pb)
	if d.pool != nil {
		d.pool.Put(d)
	}
}
