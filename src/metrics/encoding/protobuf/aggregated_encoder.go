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
type AggregatedEncoder interface {
	// Encode encodes a metric with an applicable storage policy,
	// alongside the time at which encoding happens.
	Encode(m aggregated.MetricWithStoragePolicy, encodedAtNanos int64) error

	// Buffer returns the encoded buffer.
	Buffer() Buffer
}

type aggregatedEncoder struct {
	pool pool.BytesPool

	pb  metricpb.AggregatedMetric
	buf []byte
}

// NewAggregatedEncoder creates a new aggregated encoder.
func NewAggregatedEncoder(p pool.BytesPool) AggregatedEncoder {
	e := &aggregatedEncoder{
		pool: p,
	}
	return e
}

func (enc *aggregatedEncoder) Encode(
	m aggregated.MetricWithStoragePolicy,
	encodedAtNanos int64,
) error {
	ReuseAggregatedMetricProto(&enc.pb)
	if err := m.ToProto(&enc.pb.Metric); err != nil {
		return err
	}
	enc.pb.EncodeNanos = encodedAtNanos
	// Always allocate a new byte slice to avoid modifying the existing one which may still being used.
	enc.buf = allocate(enc.pool, enc.pb.Size())
	n, err := enc.pb.MarshalTo(enc.buf)
	enc.buf = enc.buf[:n]
	return err
}

func (enc *aggregatedEncoder) Buffer() Buffer {
	var fn PoolReleaseFn
	if enc.pool != nil {
		fn = enc.pool.Put
	}
	return NewBuffer(enc.buf, fn)
}
