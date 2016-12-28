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

package msgpack

import (
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/policy"
)

type encodeRawMetricWithPolicyFn func(data []byte, p policy.Policy)
type encodeRawMetricFn func(data []byte)
type encodeMetricAsRawFn func(m aggregated.Metric) []byte

// aggregatedEncoder uses MessagePack for encoding aggregated metrics.
// It is not thread-safe.
type aggregatedEncoder struct {
	encoderBase

	buf                         encoderBase                 // buffer for encoding metrics
	encodeRootObjectFn          encodeRootObjectFn          // root object encoding function
	encodeRawMetricWithPolicyFn encodeRawMetricWithPolicyFn // raw metric with policy encoding function
	encodeRawMetricFn           encodeRawMetricFn           // raw metric encoding function
	encodeMetricAsRawFn         encodeMetricAsRawFn         // metric to raw metric conversion function
}

// NewAggregatedEncoder creates an aggregated encoder
func NewAggregatedEncoder(encoder BufferedEncoder) AggregatedEncoder {
	enc := &aggregatedEncoder{
		encoderBase: newBaseEncoder(encoder),
		buf:         newBaseEncoder(newBufferedEncoder()),
	}

	enc.encodeRootObjectFn = enc.encodeRootObject
	enc.encodeRawMetricWithPolicyFn = enc.encodeRawMetricWithPolicy
	enc.encodeRawMetricFn = enc.encodeRawMetric
	enc.encodeMetricAsRawFn = enc.encodeMetricAsRaw

	return enc
}

func (enc *aggregatedEncoder) Encoder() BufferedEncoder      { return enc.encoder() }
func (enc *aggregatedEncoder) Reset(encoder BufferedEncoder) { enc.reset(encoder) }

// NB(xichen): we encode metric as a raw metric so the decoder can inspect the encoded byte stream
// and apply filters to the encode bytes as needed without fully decoding the entire payload
func (enc *aggregatedEncoder) EncodeMetricWithPolicy(mp aggregated.MetricWithPolicy) error {
	if err := enc.err(); err != nil {
		return err
	}
	enc.encodeRootObjectFn(rawMetricWithPolicyType)
	data := enc.encodeMetricAsRawFn(mp.Metric)
	enc.encodeRawMetricWithPolicyFn(data, mp.Policy)
	return enc.err()
}

func (enc *aggregatedEncoder) EncodeRawMetricWithPolicy(rp aggregated.RawMetricWithPolicy) error {
	if err := enc.err(); err != nil {
		return err
	}
	enc.encodeRootObjectFn(rawMetricWithPolicyType)
	enc.encodeRawMetricWithPolicyFn(rp.RawMetric.Bytes(), rp.Policy)
	return enc.err()
}

func (enc *aggregatedEncoder) encodeRootObject(objType objectType) {
	enc.encodeVersion(aggregatedVersion)
	enc.encodeNumObjectFields(numFieldsForType(rootObjectType))
	enc.encodeObjectType(objType)
}

func (enc *aggregatedEncoder) encodeMetricAsRaw(m aggregated.Metric) []byte {
	enc.buf.resetData()
	enc.buf.encodeVersion(metricVersion)
	enc.buf.encodeNumObjectFields(numFieldsForType(metricType))
	enc.buf.encodeID(m.ID)
	enc.buf.encodeTime(m.Timestamp)
	enc.buf.encodeFloat64(m.Value)
	return enc.buf.encoder().Bytes()
}

func (enc *aggregatedEncoder) encodeRawMetricWithPolicy(data []byte, p policy.Policy) {
	enc.encodeNumObjectFields(numFieldsForType(rawMetricWithPolicyType))
	enc.encodeRawMetricFn(data)
	enc.encodePolicy(p)
}

func (enc *aggregatedEncoder) encodeRawMetric(data []byte) {
	enc.encodeBytes(data)
}
