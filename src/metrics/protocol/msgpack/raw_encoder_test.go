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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	errTestVarint   = errors.New("test varint error")
	errTestFloat64  = errors.New("test float64 error")
	errTestBytes    = errors.New("test bytes error")
	errTestArrayLen = errors.New("test array len error")
)

func testGoodCapturingRawEncoder(t *testing.T) (RawEncoder, *[]interface{}) {
	rawEncoder := testRawEncoder(t).(*rawEncoder)

	var result []interface{}
	rawEncoder.encodeVarintFn = func(value int64) {
		result = append(result, value)
	}
	rawEncoder.encodeFloat64Fn = func(value float64) {
		result = append(result, value)
	}
	rawEncoder.encodeBytesFn = func(value []byte) {
		result = append(result, value)
	}
	rawEncoder.encodeArrayLenFn = func(value int) {
		result = append(result, value)
	}
	return rawEncoder, &result
}

func getExpectedResults(t *testing.T, m *metric.RawMetric, p policy.VersionedPolicies) []interface{} {
	results := []interface{}{
		int64(supportedVersion),
		int64(m.Type),
		[]byte(m.ID),
	}

	switch m.Type {
	case metric.CounterType:
		results = append(results, m.CounterVal)
	case metric.BatchTimerType:
		results = append(results, len(m.BatchTimerVal))
		for _, v := range m.BatchTimerVal {
			results = append(results, v)
		}
	case metric.GaugeType:
		results = append(results, m.GaugeVal)
	default:
		require.Fail(t, fmt.Sprintf("unrecognized metric type %v", m.Type))
	}

	results = append(results, int64(p.Version))
	if p.Version != policy.DefaultPolicyVersion {
		results = append(results, len(p.Policies))
		for _, p := range p.Policies {
			resolutionValue, err := policy.ValueFromResolution(p.Resolution)
			require.NoError(t, err)
			results = append(results, int64(resolutionValue))

			retentionValue, err := policy.ValueFromRetention(p.Retention)
			require.NoError(t, err)
			results = append(results, int64(retentionValue))
		}
	}

	return results
}

func TestRawEncodeCounterWithDefaultPolicies(t *testing.T) {
	policies := policy.DefaultVersionedPolicies
	encoder, results := testGoodCapturingRawEncoder(t)
	require.NoError(t, encoder.Encode(&testCounter, policies))
	expected := getExpectedResults(t, &testCounter, policies)
	require.Equal(t, expected, *results)
}

func TestRawEncodeBatchTimerWithDefaultPolicies(t *testing.T) {
	policies := policy.DefaultVersionedPolicies
	encoder, results := testGoodCapturingRawEncoder(t)
	require.NoError(t, encoder.Encode(&testBatchTimer, policies))
	expected := getExpectedResults(t, &testBatchTimer, policies)
	require.Equal(t, expected, *results)
}

func TestRawEncodeGaugeWithDefaultPolicies(t *testing.T) {
	policies := policy.DefaultVersionedPolicies
	encoder, results := testGoodCapturingRawEncoder(t)
	require.NoError(t, encoder.Encode(&testGauge, policies))
	expected := getExpectedResults(t, &testGauge, policies)
	require.Equal(t, expected, *results)
}

func TestRawEncodeAllTypesWithDefaultPolicies(t *testing.T) {
	var expected []interface{}
	encoder, results := testGoodCapturingRawEncoder(t)
	for _, input := range testInputWithAllTypesAndDefaultPolicies {
		require.NoError(t, encoder.Encode(&input.metric, input.policies))
		expected = append(expected, getExpectedResults(t, &input.metric, input.policies)...)
	}

	require.Equal(t, expected, *results)
}

func TestRawEncodeAllTypesWithCustomPolicies(t *testing.T) {
	var expected []interface{}
	encoder, results := testGoodCapturingRawEncoder(t)
	for _, input := range testInputWithAllTypesAndCustomPolicies {
		require.NoError(t, encoder.Encode(&input.metric, input.policies))
		expected = append(expected, getExpectedResults(t, &input.metric, input.policies)...)
	}

	require.Equal(t, expected, *results)
}

func TestRawEncodeCounterError(t *testing.T) {
	counter := testCounter
	policies := policy.DefaultVersionedPolicies

	// Intentionally return an error when encoding varint
	rawEncoder := testRawEncoder(t).(*rawEncoder)
	rawEncoder.encodeVarintFn = func(value int64) {
		rawEncoder.err = errTestVarint
	}

	// Assert the error is expected
	require.Equal(t, errTestVarint, rawEncoder.Encode(&counter, policies))

	// Assert re-encoding doesn't change the error
	require.Equal(t, errTestVarint, rawEncoder.Encode(&counter, policies))
}

func TestRawEncodeBatchTimerError(t *testing.T) {
	timer := testBatchTimer
	policies := policy.DefaultVersionedPolicies

	// Intentionally return an error when encoding array length
	rawEncoder := testRawEncoder(t).(*rawEncoder)
	rawEncoder.encodeArrayLenFn = func(value int) {
		rawEncoder.err = errTestArrayLen
	}

	// Assert the error is expected
	require.Equal(t, errTestArrayLen, rawEncoder.Encode(&timer, policies))

	// Assert re-encoding doesn't change the error
	require.Equal(t, errTestArrayLen, rawEncoder.Encode(&timer, policies))
}

func TestRawEncodeGaugeError(t *testing.T) {
	gauge := testGauge
	policies := policy.DefaultVersionedPolicies

	// Intentionally return an error when encoding float64
	rawEncoder := testRawEncoder(t).(*rawEncoder)
	rawEncoder.encodeFloat64Fn = func(value float64) {
		rawEncoder.err = errTestFloat64
	}

	// Assert the error is expected
	require.Equal(t, errTestFloat64, rawEncoder.Encode(&gauge, policies))

	// Assert re-encoding doesn't change the error
	require.Equal(t, errTestFloat64, rawEncoder.Encode(&gauge, policies))
}

func TestRawEncodePolicyError(t *testing.T) {
	gauge := testGauge
	policies := policy.VersionedPolicies{
		Version: 1,
		Policies: []policy.Policy{
			{
				Resolution: policy.Resolution{Window: time.Duration(1), Precision: xtime.Second},
				Retention:  policy.Retention(time.Hour),
			},
		},
	}

	// Intentionally return an error when encoding array length
	rawEncoder := testRawEncoder(t).(*rawEncoder)
	rawEncoder.encodeArrayLenFn = func(value int) {
		rawEncoder.err = errTestArrayLen
	}

	// Assert the error is expected
	require.Equal(t, errTestArrayLen, rawEncoder.Encode(&gauge, policies))

	// Assert re-encoding doesn't change the error
	require.Equal(t, errTestArrayLen, rawEncoder.Encode(&gauge, policies))
}
