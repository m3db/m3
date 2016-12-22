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
	"testing"

	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/policy"

	"github.com/stretchr/testify/require"
)

func testCapturingBaseEncoder(encoder encoderBase) *[]interface{} {
	baseEncoder := encoder.(*baseEncoder)

	var result []interface{}
	baseEncoder.encodeVarintFn = func(value int64) {
		result = append(result, value)
	}
	baseEncoder.encodeFloat64Fn = func(value float64) {
		result = append(result, value)
	}
	baseEncoder.encodeBytesFn = func(value []byte) {
		result = append(result, value)
	}
	baseEncoder.encodeArrayLenFn = func(value int) {
		result = append(result, value)
	}

	return &result
}

func testCapturingAggregatedEncoder(t *testing.T) (AggregatedEncoder, *[]interface{}) {
	encoder := testAggregatedEncoder(t).(*aggregatedEncoder)
	result := testCapturingBaseEncoder(encoder.encoderBase)
	return encoder, result
}

func expectedResultsForRawMetricWithPolicy(t *testing.T, m aggregated.RawMetric, p policy.Policy) []interface{} {
	results := []interface{}{
		numFieldsForType(rawMetricWithPolicyType),
		m.Bytes(),
	}
	results = append(results, expectedResultsForPolicy(t, p)...)
	return results
}

func expectedResultsForAggregatedMetricWithPolicy(t *testing.T, m interface{}, p policy.Policy) []interface{} {
	results := []interface{}{
		int64(aggregatedVersion),
		numFieldsForType(rootObjectType),
		int64(rawMetricWithPolicyType),
	}
	switch m := m.(type) {
	case aggregated.Metric:
		rm := toRawMetric(t, m)
		results = append(results, expectedResultsForRawMetricWithPolicy(t, rm, p)...)
	case aggregated.RawMetric:
		results = append(results, expectedResultsForRawMetricWithPolicy(t, m, p)...)
	default:
		require.Fail(t, "unrecognized input type %T", m)
	}
	return results
}

func TestAggregatedEncodeMetric(t *testing.T) {
	encoder := testAggregatedEncoder(t).(*aggregatedEncoder)
	result := testCapturingBaseEncoder(encoder.buf)
	encoder.encodeMetricAsRaw(testMetric)
	expected := []interface{}{
		int64(metricVersion),
		[]byte(testMetric.ID),
		int64(testMetric.Timestamp.UnixNano()),
		testMetric.Value,
	}
	require.Equal(t, expected, *result)
}

func TestAggregatedEncodeMetricWithPolicy(t *testing.T) {
	encoder, results := testCapturingAggregatedEncoder(t)
	require.NoError(t, encoder.EncodeMetricWithPolicy(testMetric, testPolicy))
	expected := expectedResultsForAggregatedMetricWithPolicy(t, testMetric, testPolicy)
	require.Equal(t, expected, *results)
}

func TestAggregatedEncodeRawMetricWithPolicy(t *testing.T) {
	encoder, results := testCapturingAggregatedEncoder(t)
	rawMetric := toRawMetric(t, testMetric)
	require.NoError(t, encoder.EncodeRawMetricWithPolicy(rawMetric, testPolicy))
	expected := expectedResultsForAggregatedMetricWithPolicy(t, rawMetric, testPolicy)
	require.Equal(t, expected, *results)
}

func TestAggregatedEncodeError(t *testing.T) {
	// Intentionally return an error when encoding varint
	encoder := testAggregatedEncoder(t).(*aggregatedEncoder)
	baseEncoder := encoder.encoderBase.(*baseEncoder)
	baseEncoder.encodeVarintFn = func(value int64) {
		baseEncoder.encodeErr = errTestVarint
	}

	// Assert the error is expected
	require.Equal(t, errTestVarint, encoder.EncodeMetricWithPolicy(testMetric, testPolicy))

	// Assert re-encoding doesn't change the error
	require.Equal(t, errTestVarint, encoder.EncodeMetricWithPolicy(testMetric, testPolicy))
}
