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
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/policy"
)

type encodeRawMetricWithStoragePolicyFn func(data []byte, p policy.StoragePolicy)
type encodeRawMetricWithStoragePolicyAndEncodeTimeFn func(data []byte, p policy.StoragePolicy, encodedAtNanos int64)
type encodeRawMetricFn func(data []byte)
type encodeMetricAsRawFn func(m aggregated.Metric) []byte
type encodeChunkedMetricAsRawFn func(m aggregated.ChunkedMetric) []byte

// aggregatedEncoder uses MessagePack for encoding aggregated metrics.
// It is not thread-safe.
type aggregatedEncoder struct {
	encoderBase

	buf                                             encoderBase
	encodeRootObjectFn                              encodeRootObjectFn
	encodeRawMetricWithStoragePolicyFn              encodeRawMetricWithStoragePolicyFn
	encodeRawMetricWithStoragePolicyAndEncodeTimeFn encodeRawMetricWithStoragePolicyAndEncodeTimeFn
	encodeRawMetricFn                               encodeRawMetricFn
	encodeMetricAsRawFn                             encodeMetricAsRawFn
	encodeChunkedMetricAsRawFn                      encodeChunkedMetricAsRawFn
}

// NewAggregatedEncoder creates an aggregated encoder.
func NewAggregatedEncoder(encoder BufferedEncoder) AggregatedEncoder {
	enc := &aggregatedEncoder{
		encoderBase: newBaseEncoder(encoder),
		buf:         newBaseEncoder(NewBufferedEncoder()),
	}

	enc.encodeRootObjectFn = enc.encodeRootObject
	enc.encodeRawMetricWithStoragePolicyFn = enc.encodeRawMetricWithStoragePolicy
	enc.encodeRawMetricWithStoragePolicyAndEncodeTimeFn = enc.encodeRawMetricWithStoragePolicyAndEncodeTime
	enc.encodeRawMetricFn = enc.encodeRawMetric
	enc.encodeMetricAsRawFn = enc.encodeMetricAsRaw
	enc.encodeChunkedMetricAsRawFn = enc.encodeChunkedMetricAsRaw

	return enc
}

func (enc *aggregatedEncoder) Encoder() BufferedEncoder      { return enc.encoder() }
func (enc *aggregatedEncoder) Reset(encoder BufferedEncoder) { enc.reset(encoder) }

func (enc *aggregatedEncoder) EncodeChunkedMetricWithStoragePolicy(
	cmp aggregated.ChunkedMetricWithStoragePolicy,
) error {
	if err := enc.err(); err != nil {
		return err
	}
	enc.encodeRootObjectFn(rawMetricWithStoragePolicyType)
	data := enc.encodeChunkedMetricAsRawFn(cmp.ChunkedMetric)
	enc.encodeRawMetricWithStoragePolicyFn(data, cmp.StoragePolicy)
	return enc.err()
}

func (enc *aggregatedEncoder) EncodeChunkedMetricWithStoragePolicyAndEncodeTime(
	cmp aggregated.ChunkedMetricWithStoragePolicy,
	encodedAtNanos int64,
) error {
	if err := enc.err(); err != nil {
		return err
	}
	enc.encodeRootObjectFn(rawMetricWithStoragePolicyAndEncodeTimeType)
	data := enc.encodeChunkedMetricAsRawFn(cmp.ChunkedMetric)
	enc.encodeRawMetricWithStoragePolicyAndEncodeTimeFn(data, cmp.StoragePolicy, encodedAtNanos)
	return enc.err()
}

func (enc *aggregatedEncoder) encodeRootObject(objType objectType) {
	enc.encodeVersion(aggregatedVersion)
	enc.encodeNumObjectFields(numFieldsForType(rootObjectType))
	enc.encodeObjectType(objType)
}

func (enc *aggregatedEncoder) encodeMetricAsRaw(m aggregated.Metric) []byte {
	enc.buf.resetData()
	enc.encodeMetricProlog()
	enc.buf.encodeRawID(m.ID)
	enc.buf.encodeVarint(m.TimeNanos)
	enc.buf.encodeFloat64(m.Value)
	return enc.buf.encoder().Bytes()
}

func (enc *aggregatedEncoder) encodeChunkedMetricAsRaw(m aggregated.ChunkedMetric) []byte {
	enc.buf.resetData()
	enc.encodeMetricProlog()
	enc.buf.encodeChunkedID(m.ChunkedID)
	enc.buf.encodeVarint(m.TimeNanos)
	enc.buf.encodeFloat64(m.Value)
	return enc.buf.encoder().Bytes()
}

func (enc *aggregatedEncoder) encodeMetricProlog() {
	enc.buf.encodeVersion(metricVersion)
	enc.buf.encodeNumObjectFields(numFieldsForType(metricType))
}

func (enc *aggregatedEncoder) encodeRawMetricWithStoragePolicy(
	data []byte,
	p policy.StoragePolicy,
) {
	enc.encodeNumObjectFields(numFieldsForType(rawMetricWithStoragePolicyType))
	enc.encodeRawMetricFn(data)
	enc.encodeStoragePolicy(p)
}

func (enc *aggregatedEncoder) encodeRawMetricWithStoragePolicyAndEncodeTime(
	data []byte,
	p policy.StoragePolicy,
	encodedAtNanos int64,
) {
	enc.encodeNumObjectFields(numFieldsForType(rawMetricWithStoragePolicyAndEncodeTimeType))
	enc.encodeRawMetricFn(data)
	enc.encodeStoragePolicy(p)
	enc.encodeVarint(encodedAtNanos)
}

func (enc *aggregatedEncoder) encodeRawMetric(data []byte) {
	enc.encodeBytes(data)
}
