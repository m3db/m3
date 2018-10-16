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
	"io"
	"testing"

	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/policy"

	"github.com/stretchr/testify/require"
)

func validateAggregatedDecodeResults(
	t *testing.T,
	it AggregatedIterator,
	expectedResults []metricWithPolicyAndEncodeTime,
	expectedErr error,
) {
	var results []metricWithPolicyAndEncodeTime
	for it.Next() {
		value, policy, encodedAtNanos := it.Value()
		m, err := value.Metric()
		require.NoError(t, err)
		results = append(results, metricWithPolicyAndEncodeTime{
			metric:         m,
			policy:         policy,
			encodedAtNanos: encodedAtNanos,
		})
	}
	require.Equal(t, expectedErr, it.Err())
	require.Equal(t, expectedResults, results)
}

func TestAggregatedIteratorDecodeNewerVersionThanSupported(t *testing.T) {
	input := metricWithPolicyAndEncodeTime{
		metric: testMetric,
		policy: testPolicy,
	}
	enc := testAggregatedEncoder().(*aggregatedEncoder)

	// Version encoded is higher than supported version.
	enc.encodeRootObjectFn = func(objType objectType) {
		enc.encodeVersion(aggregatedVersion + 1)
		enc.encodeNumObjectFields(numFieldsForType(rootObjectType))
		enc.encodeObjectType(objType)
	}
	require.NoError(t, testAggregatedEncodeMetricWithPolicy(enc, input.metric.(aggregated.Metric), input.policy))

	// Now restore the encode top-level function and encode another metric.
	enc.encodeRootObjectFn = enc.encodeRootObject
	require.NoError(t, testAggregatedEncodeMetricWithPolicy(enc, input.metric.(aggregated.Metric), input.policy))

	it := testAggregatedIterator(enc.Encoder().Buffer())
	it.(*aggregatedIterator).ignoreHigherVersion = true

	// Check that we skipped the first metric and successfully decoded the second metric.
	validateAggregatedDecodeResults(t, it, []metricWithPolicyAndEncodeTime{input}, io.EOF)
}

func TestAggregatedIteratorDecodeRootObjectMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicyAndEncodeTime{
		metric: testMetric,
		policy: testPolicy,
	}
	enc := testAggregatedEncoder().(*aggregatedEncoder)

	// Pretend we added an extra int field to the root object.
	enc.encodeRootObjectFn = func(objType objectType) {
		enc.encodeVersion(unaggregatedVersion)
		enc.encodeNumObjectFields(numFieldsForType(rootObjectType) + 1)
		enc.encodeObjectType(objType)
	}
	err := testAggregatedEncodeMetricWithPolicy(enc, input.metric.(aggregated.Metric), input.policy)
	require.NoError(t, err)
	enc.encodeVarint(0)
	require.NoError(t, enc.err())

	it := testAggregatedIterator(enc.Encoder().Buffer())

	// Check that we successfully decoded the metric.
	validateAggregatedDecodeResults(t, it, []metricWithPolicyAndEncodeTime{input}, io.EOF)
}

func TestAggregatedIteratorDecodeRawMetricMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicyAndEncodeTime{
		metric: testMetric,
		policy: testPolicy,
	}
	enc := testAggregatedEncoder().(*aggregatedEncoder)

	// Pretend we added an extra int field to the raw metric with policy object.
	enc.encodeRawMetricWithStoragePolicyFn = func(data []byte, p policy.StoragePolicy) {
		enc.encodeNumObjectFields(numFieldsForType(rawMetricWithStoragePolicyType) + 1)
		enc.encodeRawMetricFn(data)
		enc.encodeStoragePolicy(p)
	}
	err := testAggregatedEncodeMetricWithPolicy(enc, input.metric.(aggregated.Metric), input.policy)
	require.NoError(t, err)
	enc.encodeVarint(0)
	require.NoError(t, enc.err())

	it := testAggregatedIterator(enc.Encoder().Buffer())

	// Check that we successfully decoded the metric.
	validateAggregatedDecodeResults(t, it, []metricWithPolicyAndEncodeTime{input}, io.EOF)
}

func TestAggregatedIteratorDecodeRawMetricWithEncodeTimeMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicyAndEncodeTime{
		metric:         testMetric,
		policy:         testPolicy,
		encodedAtNanos: testEncodedAtNanos,
	}
	enc := testAggregatedEncoder().(*aggregatedEncoder)

	// Pretend we added an extra int field to the raw metric with policy object.
	enc.encodeRawMetricWithStoragePolicyAndEncodeTimeFn = func(data []byte, p policy.StoragePolicy, encodedAtNanos int64) {
		enc.encodeNumObjectFields(numFieldsForType(rawMetricWithStoragePolicyAndEncodeTimeType) + 1)
		enc.encodeRawMetricFn(data)
		enc.encodeStoragePolicy(p)
		enc.encodeVarint(encodedAtNanos)
	}
	err := testAggregatedEncodeMetricWithPolicyAndEncodeTime(enc, input.metric.(aggregated.Metric), input.policy, input.encodedAtNanos)
	require.NoError(t, err)
	enc.encodeVarint(0)
	require.NoError(t, enc.err())

	it := testAggregatedIterator(enc.Encoder().Buffer())

	// Check that we successfully decoded the metric.
	validateAggregatedDecodeResults(t, it, []metricWithPolicyAndEncodeTime{input}, io.EOF)
}

func TestAggregatedIteratorDecodeMetricHigherVersionThanSupported(t *testing.T) {
	input := metricWithPolicyAndEncodeTime{
		metric: testMetric,
		policy: testPolicy,
	}
	enc := testAggregatedEncoder().(*aggregatedEncoder)

	// Pretend we added an extra int field to the raw metric object.
	enc.encodeMetricAsRawFn = func(m aggregated.Metric) []byte {
		enc.buf.resetData()
		enc.buf.encodeVersion(metricVersion + 1)
		return enc.buf.encoder().Bytes()
	}
	err := testAggregatedEncodeMetricWithPolicy(enc, input.metric.(aggregated.Metric), input.policy)
	require.NoError(t, err)
	require.NoError(t, enc.err())

	it := testAggregatedIterator(enc.Encoder().Buffer())
	require.True(t, it.Next())
	rawMetric, _, _ := it.Value()
	_, err = rawMetric.Value()
	require.Error(t, err)
}

func TestAggregatedIteratorDecodeMetricMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicyAndEncodeTime{
		metric: testMetric,
		policy: testPolicy,
	}
	enc := testAggregatedEncoder().(*aggregatedEncoder)

	// Pretend we added an extra int field to the raw metric object.
	enc.encodeMetricAsRawFn = func(m aggregated.Metric) []byte {
		enc.encodeMetricAsRaw(m)
		enc.buf.encodeVarint(0)
		return enc.buf.encoder().Bytes()
	}
	err := testAggregatedEncodeMetricWithPolicy(enc, input.metric.(aggregated.Metric), input.policy)
	require.NoError(t, err)
	require.NoError(t, enc.err())

	it := testAggregatedIterator(enc.Encoder().Buffer())

	// Check that we successfully decoded the metric.
	validateAggregatedDecodeResults(t, it, []metricWithPolicyAndEncodeTime{input}, io.EOF)
}

func TestAggregatedIteratorClose(t *testing.T) {
	it := NewAggregatedIterator(nil, nil)
	it.Close()
	require.False(t, it.Next())
	require.NoError(t, it.Err())
	require.True(t, it.(*aggregatedIterator).closed)
}
