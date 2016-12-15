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

func testCapturingMultiTypedEncoder(t *testing.T) (MultiTypedEncoder, *[]interface{}) {
	encoder := testMultiTypedEncoder(t).(*multiTypedEncoder)

	var result []interface{}
	encoder.encodeVarintFn = func(value int64) {
		result = append(result, value)
	}
	encoder.encodeFloat64Fn = func(value float64) {
		result = append(result, value)
	}
	encoder.encodeBytesFn = func(value []byte) {
		result = append(result, value)
	}
	encoder.encodeArrayLenFn = func(value int) {
		result = append(result, value)
	}
	return encoder, &result
}

func getExpectedResults(t *testing.T, m *metric.OneOf, p policy.VersionedPolicies) []interface{} {
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

func TestMultiTypedEncodeCounterWithDefaultPolicies(t *testing.T) {
	policies := policy.DefaultVersionedPolicies
	encoder, results := testCapturingMultiTypedEncoder(t)
	require.NoError(t, testEncode(t, encoder, &testCounter, policies))
	expected := getExpectedResults(t, &testCounter, policies)
	require.Equal(t, expected, *results)
}

func TestMultiTypedEncodeBatchTimerWithDefaultPolicies(t *testing.T) {
	policies := policy.DefaultVersionedPolicies
	encoder, results := testCapturingMultiTypedEncoder(t)
	require.NoError(t, testEncode(t, encoder, &testBatchTimer, policies))
	expected := getExpectedResults(t, &testBatchTimer, policies)
	require.Equal(t, expected, *results)
}

func TestMultiTypedEncodeGaugeWithDefaultPolicies(t *testing.T) {
	policies := policy.DefaultVersionedPolicies
	encoder, results := testCapturingMultiTypedEncoder(t)
	require.NoError(t, testEncode(t, encoder, &testGauge, policies))
	expected := getExpectedResults(t, &testGauge, policies)
	require.Equal(t, expected, *results)
}

func TestMultiTypedEncodeAllTypesWithDefaultPolicies(t *testing.T) {
	var expected []interface{}
	encoder, results := testCapturingMultiTypedEncoder(t)
	for _, input := range testInputWithAllTypesAndDefaultPolicies {
		require.NoError(t, testEncode(t, encoder, &input.metric, input.policies))
		expected = append(expected, getExpectedResults(t, &input.metric, input.policies)...)
	}

	require.Equal(t, expected, *results)
}

func TestMultiTypedEncodeAllTypesWithCustomPolicies(t *testing.T) {
	var expected []interface{}
	encoder, results := testCapturingMultiTypedEncoder(t)
	for _, input := range testInputWithAllTypesAndCustomPolicies {
		require.NoError(t, testEncode(t, encoder, &input.metric, input.policies))
		expected = append(expected, getExpectedResults(t, &input.metric, input.policies)...)
	}

	require.Equal(t, expected, *results)
}

func TestMultiTypedEncodeCounterError(t *testing.T) {
	counter := testCounter
	policies := policy.DefaultVersionedPolicies

	// Intentionally return an error when encoding varint
	encoder := testMultiTypedEncoder(t).(*multiTypedEncoder)
	encoder.encodeVarintFn = func(value int64) {
		encoder.err = errTestVarint
	}

	// Assert the error is expected
	require.Equal(t, errTestVarint, testEncode(t, encoder, &counter, policies))

	// Assert re-encoding doesn't change the error
	require.Equal(t, errTestVarint, testEncode(t, encoder, &counter, policies))
}

func TestMultiTypedEncodeBatchTimerError(t *testing.T) {
	timer := testBatchTimer
	policies := policy.DefaultVersionedPolicies

	// Intentionally return an error when encoding array length
	encoder := testMultiTypedEncoder(t).(*multiTypedEncoder)
	encoder.encodeArrayLenFn = func(value int) {
		encoder.err = errTestArrayLen
	}

	// Assert the error is expected
	require.Equal(t, errTestArrayLen, testEncode(t, encoder, &timer, policies))

	// Assert re-encoding doesn't change the error
	require.Equal(t, errTestArrayLen, testEncode(t, encoder, &timer, policies))
}

func TestMultiTypedEncodeGaugeError(t *testing.T) {
	gauge := testGauge
	policies := policy.DefaultVersionedPolicies

	// Intentionally return an error when encoding float64
	encoder := testMultiTypedEncoder(t).(*multiTypedEncoder)
	encoder.encodeFloat64Fn = func(value float64) {
		encoder.err = errTestFloat64
	}

	// Assert the error is expected
	require.Equal(t, errTestFloat64, testEncode(t, encoder, &gauge, policies))

	// Assert re-encoding doesn't change the error
	require.Equal(t, errTestFloat64, testEncode(t, encoder, &gauge, policies))
}

func TestMultiTypedEncodePolicyError(t *testing.T) {
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
	encoder := testMultiTypedEncoder(t).(*multiTypedEncoder)
	encoder.encodeArrayLenFn = func(value int) {
		encoder.err = errTestArrayLen
	}

	// Assert the error is expected
	require.Equal(t, errTestArrayLen, testEncode(t, encoder, &gauge, policies))

	// Assert re-encoding doesn't change the error
	require.Equal(t, errTestArrayLen, testEncode(t, encoder, &gauge, policies))
}
