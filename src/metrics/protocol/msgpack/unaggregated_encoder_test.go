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

	"github.com/m3db/m3metrics/metric/unaggregated"
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

func TestUnaggregatedEncodeCounter(t *testing.T) {
	encoder, results := testCapturingUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncodeMetric(t, encoder, testCounter))
	expected := expectedResultsForUnaggregatedMetric(t, testCounter)
	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeBatchTimer(t *testing.T) {
	encoder, results := testCapturingUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncodeMetric(t, encoder, testBatchTimer))
	expected := expectedResultsForUnaggregatedMetric(t, testBatchTimer)
	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeGauge(t *testing.T) {
	encoder, results := testCapturingUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncodeMetric(t, encoder, testGauge))
	expected := expectedResultsForUnaggregatedMetric(t, testGauge)
	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeCounterWithDefaultPoliciesList(t *testing.T) {
	policies := testDefaultStagedPoliciesList
	encoder, results := testCapturingUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, testCounter, policies))
	expected := expectedResultsForUnaggregatedMetricWithPoliciesList(t, testCounter, policies)
	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeBatchTimerWithDefaultPoliciesList(t *testing.T) {
	policies := testDefaultStagedPoliciesList
	encoder, results := testCapturingUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, testBatchTimer, policies))
	expected := expectedResultsForUnaggregatedMetricWithPoliciesList(t, testBatchTimer, policies)
	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeGaugeWithDefaultPoliciesList(t *testing.T) {
	policies := testDefaultStagedPoliciesList
	encoder, results := testCapturingUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, testGauge, policies))
	expected := expectedResultsForUnaggregatedMetricWithPoliciesList(t, testGauge, policies)
	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeAllMetricTypes(t *testing.T) {
	inputs := []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge}
	var expected []interface{}
	encoder, results := testCapturingUnaggregatedEncoder(t)
	for _, input := range inputs {
		require.NoError(t, testUnaggregatedEncodeMetric(t, encoder, input))
		expected = append(expected, expectedResultsForUnaggregatedMetric(t, input)...)
	}
	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeAllTypesWithDefaultPoliciesList(t *testing.T) {
	var expected []interface{}
	encoder, results := testCapturingUnaggregatedEncoder(t)
	for _, input := range testInputWithAllTypesAndDefaultPoliciesList {
		require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, input.metric, input.policiesList))
		expected = append(expected, expectedResultsForUnaggregatedMetricWithPoliciesList(t, input.metric, input.policiesList)...)
	}

	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeAllTypesWithSingleCustomPoliciesList(t *testing.T) {
	var expected []interface{}
	encoder, results := testCapturingUnaggregatedEncoder(t)
	for _, input := range testInputWithAllTypesAndSingleCustomPoliciesList {
		require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, input.metric, input.policiesList))
		expected = append(expected, expectedResultsForUnaggregatedMetricWithPoliciesList(t, input.metric, input.policiesList)...)
	}

	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeAllTypesWithMultiCustomPolicies(t *testing.T) {
	var expected []interface{}
	encoder, results := testCapturingUnaggregatedEncoder(t)
	for _, input := range testInputWithAllTypesAndMultiCustomPoliciesList {
		require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, input.metric, input.policiesList))
		expected = append(expected, expectedResultsForUnaggregatedMetricWithPoliciesList(t, input.metric, input.policiesList)...)
	}

	require.Equal(t, expected, *results)
}

func TestUnaggregatedEncodeVarintError(t *testing.T) {
	counter := testCounter
	policies := testDefaultStagedPoliciesList

	// Intentionally return an error when encoding varint.
	encoder := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	baseEncoder := encoder.encoderBase.(*baseEncoder)
	baseEncoder.encodeVarintFn = func(value int64) {
		baseEncoder.encodeErr = errTestVarint
	}

	// Assert the error is expected.
	require.Equal(t, errTestVarint, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, counter, policies))

	// Assert re-encoding doesn't change the error.
	require.Equal(t, errTestVarint, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, counter, policies))
}

func TestUnaggregatedEncodeFloat64Error(t *testing.T) {
	gauge := testGauge
	policies := testDefaultStagedPoliciesList

	// Intentionally return an error when encoding float64.
	encoder := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	baseEncoder := encoder.encoderBase.(*baseEncoder)
	baseEncoder.encodeFloat64Fn = func(value float64) {
		baseEncoder.encodeErr = errTestFloat64
	}

	// Assert the error is expected.
	require.Equal(t, errTestFloat64, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, gauge, policies))

	// Assert re-encoding doesn't change the error.
	require.Equal(t, errTestFloat64, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, gauge, policies))
}

func TestUnaggregatedEncodeBytesError(t *testing.T) {
	timer := testBatchTimer
	policies := testDefaultStagedPoliciesList

	// Intentionally return an error when encoding array length.
	encoder := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	baseEncoder := encoder.encoderBase.(*baseEncoder)
	baseEncoder.encodeBytesFn = func(value []byte) {
		baseEncoder.encodeErr = errTestBytes
	}

	// Assert the error is expected.
	require.Equal(t, errTestBytes, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, timer, policies))

	// Assert re-encoding doesn't change the error.
	require.Equal(t, errTestBytes, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, timer, policies))
}

func TestUnaggregatedEncodeArrayLenError(t *testing.T) {
	gauge := testGauge
	policies := policy.PoliciesList{
		policy.NewStagedPolicies(
			time.Now().UnixNano(),
			false,
			[]policy.Policy{
				policy.NewPolicy(time.Second, xtime.Second, time.Hour),
			},
		),
	}

	// Intentionally return an error when encoding array length.
	encoder := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	baseEncoder := encoder.encoderBase.(*baseEncoder)
	baseEncoder.encodeArrayLenFn = func(value int) {
		baseEncoder.encodeErr = errTestArrayLen
	}

	// Assert the error is expected.
	require.Equal(t, errTestArrayLen, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, gauge, policies))

	// Assert re-encoding doesn't change the error.
	require.Equal(t, errTestArrayLen, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, gauge, policies))
}

func TestUnaggregatedEncoderReset(t *testing.T) {
	metric := testCounter
	policies := testDefaultStagedPoliciesList

	encoder := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	baseEncoder := encoder.encoderBase.(*baseEncoder)
	baseEncoder.encodeErr = errTestVarint
	require.Equal(t, errTestVarint, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, metric, policies))

	encoder.Reset(NewBufferedEncoder())
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, encoder, metric, policies))
}

func testCapturingUnaggregatedEncoder(t *testing.T) (UnaggregatedEncoder, *[]interface{}) {
	encoder := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	result := testCapturingBaseEncoder(encoder.encoderBase)
	return encoder, result
}

func expectedResultsForPolicy(t *testing.T, p policy.Policy) []interface{} {
	results := []interface{}{numFieldsForType(policyType)}

	resolutionValue, err := policy.ValueFromResolution(p.Resolution())
	if err == nil {
		results = append(results, []interface{}{
			numFieldsForType(knownResolutionType),
			int64(knownResolutionType),
			int64(resolutionValue),
		}...)
	} else {
		results = append(results, []interface{}{
			numFieldsForType(unknownResolutionType),
			int64(unknownResolutionType),
			int64(p.Resolution().Window),
			int64(p.Resolution().Precision),
		}...)
	}

	retentionValue, err := policy.ValueFromRetention(p.Retention())
	if err == nil {
		results = append(results, []interface{}{
			numFieldsForType(knownRetentionType),
			int64(knownRetentionType),
			int64(retentionValue),
		}...)
	} else {
		results = append(results, []interface{}{
			numFieldsForType(unknownRetentionType),
			int64(unknownRetentionType),
			int64(p.Retention()),
		}...)
	}

	return results
}

func expectedResultsForStagedPolicies(t *testing.T, sp policy.StagedPolicies) []interface{} {
	policies, _ := sp.Policies()
	results := []interface{}{
		numFieldsForType(stagedPoliciesType),
		sp.CutoverNanos,
		sp.Tombstoned,
		len(policies),
	}
	for _, p := range policies {
		results = append(results, expectedResultsForPolicy(t, p)...)
	}
	return results
}

func expectedResultsForPoliciesList(t *testing.T, pl policy.PoliciesList) []interface{} {
	if pl.IsDefault() {
		return []interface{}{
			numFieldsForType(defaultPoliciesListType),
			int64(defaultPoliciesListType),
		}
	}
	results := []interface{}{
		numFieldsForType(customPoliciesListType),
		int64(customPoliciesListType),
		len(pl),
	}
	for _, sp := range pl {
		results = append(results, expectedResultsForStagedPolicies(t, sp)...)
	}
	return results
}

func expectedResultsForUnaggregatedMetric(t *testing.T, m unaggregated.MetricUnion) []interface{} {
	results := []interface{}{
		int64(unaggregatedVersion),
		numFieldsForType(rootObjectType),
	}

	switch m.Type {
	case unaggregated.CounterType:
		results = append(results, []interface{}{
			int64(counterType),
			numFieldsForType(counterType),
			[]byte(m.ID),
			m.CounterVal,
		}...)
	case unaggregated.BatchTimerType:
		results = append(results, []interface{}{
			int64(batchTimerType),
			numFieldsForType(batchTimerType),
			[]byte(m.ID),
			len(m.BatchTimerVal),
		}...)
		for _, v := range m.BatchTimerVal {
			results = append(results, v)
		}
	case unaggregated.GaugeType:
		results = append(results, []interface{}{
			int64(gaugeType),
			numFieldsForType(gaugeType),
			[]byte(m.ID),
			m.GaugeVal,
		}...)
	default:
		require.Fail(t, fmt.Sprintf("unrecognized metric type %v", m.Type))
	}

	return results
}

func expectedResultsForUnaggregatedMetricWithPoliciesList(
	t *testing.T,
	m unaggregated.MetricUnion,
	pl policy.PoliciesList,
) []interface{} {
	results := []interface{}{
		int64(unaggregatedVersion),
		numFieldsForType(rootObjectType),
	}

	switch m.Type {
	case unaggregated.CounterType:
		results = append(results, []interface{}{
			int64(counterWithPoliciesListType),
			numFieldsForType(counterWithPoliciesListType),
			numFieldsForType(counterType),
			[]byte(m.ID),
			m.CounterVal,
		}...)
	case unaggregated.BatchTimerType:
		results = append(results, []interface{}{
			int64(batchTimerWithPoliciesListType),
			numFieldsForType(batchTimerWithPoliciesListType),
			numFieldsForType(batchTimerType),
			[]byte(m.ID),
			len(m.BatchTimerVal),
		}...)
		for _, v := range m.BatchTimerVal {
			results = append(results, v)
		}
	case unaggregated.GaugeType:
		results = append(results, []interface{}{
			int64(gaugeWithPoliciesListType),
			numFieldsForType(gaugeWithPoliciesListType),
			numFieldsForType(gaugeType),
			[]byte(m.ID),
			m.GaugeVal,
		}...)
	default:
		require.Fail(t, fmt.Sprintf("unrecognized metric type %v", m.Type))
	}

	plRes := expectedResultsForPoliciesList(t, pl)
	results = append(results, plRes...)

	return results
}
