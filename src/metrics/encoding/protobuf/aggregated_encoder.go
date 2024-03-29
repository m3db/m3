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
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/x/pool"
)

// AggregatedEncoder is an encoder for encoding aggregated metrics.
type AggregatedEncoder struct {
	pool pool.BytesPool
	buf  []byte
	pb   metricpb.AggregatedMetric
}

// NewAggregatedEncoder creates a new aggregated encoder.
func NewAggregatedEncoder(p pool.BytesPool) *AggregatedEncoder {
	return &AggregatedEncoder{
		pool: p,
	}
}

// Encode encodes a metric with an applicable storage policy.
func (enc *AggregatedEncoder) Encode(
	m aggregated.MetricWithStoragePolicy,
) error {
	ReuseAggregatedMetricProto(&enc.pb)
	if err := m.ToProto(&enc.pb.Metric); err != nil {
		return err
	}
	// Always allocate a new byte slice to avoid modifying the existing one which may still being used.
	enc.buf = allocate(enc.pool, enc.pb.Size())
	n, err := enc.pb.MarshalTo(enc.buf)
	enc.buf = enc.buf[:n]
	return err
}

// Buffer returns the encoded buffer
func (enc *AggregatedEncoder) Buffer() Buffer {
	var fn PoolReleaseFn
	if enc.pool != nil {
		fn = enc.pool.Put
	}
	return NewBuffer(enc.buf, fn)
}
