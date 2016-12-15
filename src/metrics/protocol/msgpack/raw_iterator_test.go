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
	"io"
	"testing"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"

	"github.com/stretchr/testify/require"
)

func testGoodRawIterator(
	t *testing.T,
	varintValues []int64,
	float64Values []float64,
	bytesValues [][]byte,
	arrayLenValues []int,
) *rawIterator {
	it, err := NewRawIterator(nil, nil)
	require.NoError(t, err)
	rawIt := it.(*rawIterator)

	var (
		varintIdx   int
		float64Idx  int
		bytesIdx    int
		arrayLenIdx int
	)
	rawIt.decodeVarintFn = func() int64 {
		if varintIdx >= len(varintValues) {
			rawIt.err = io.EOF
			return 0
		}
		v := varintValues[varintIdx]
		varintIdx++
		return v
	}
	rawIt.decodeFloat64Fn = func() float64 {
		if float64Idx >= len(float64Values) {
			rawIt.err = io.EOF
			return 0.0
		}
		v := float64Values[float64Idx]
		float64Idx++
		return v
	}
	rawIt.decodeBytesFn = func() []byte {
		if bytesIdx >= len(bytesValues) {
			rawIt.err = io.EOF
			return nil
		}
		v := bytesValues[bytesIdx]
		bytesIdx++
		return v
	}
	rawIt.decodeArrayLenFn = func() int {
		if arrayLenIdx >= len(arrayLenValues) {
			rawIt.err = io.EOF
			return 0
		}
		v := arrayLenValues[arrayLenIdx]
		arrayLenIdx++
		return v
	}
	return rawIt
}

func getMockValuesFor(
	t *testing.T,
	m *metric.OneOf,
	p policy.VersionedPolicies,
) ([]int64, []float64, [][]byte, []int) {
	var (
		varintValues = []int64{
			int64(supportedVersion),
			int64(m.Type),
		}
		float64Values []float64
		bytesValues   = [][]byte{
			[]byte(m.ID),
		}
		arrayLenValues []int
	)
	switch m.Type {
	case metric.CounterType:
		varintValues = append(varintValues, m.CounterVal)
	case metric.BatchTimerType:
		arrayLenValues = append(arrayLenValues, len(m.BatchTimerVal))
		float64Values = m.BatchTimerVal
	case metric.GaugeType:
		float64Values = []float64{m.GaugeVal}
	default:
		require.Fail(t, fmt.Sprintf("unrecognized metric type %v", m.Type))
	}

	varintValues = append(varintValues, int64(p.Version))
	if p.Version != policy.DefaultPolicyVersion {
		arrayLenValues = append(arrayLenValues, len(p.Policies))
		for _, p := range p.Policies {
			resolutionValue, err := policy.ValueFromResolution(p.Resolution)
			require.NoError(t, err)
			varintValues = append(varintValues, int64(resolutionValue))

			retentionValue, err := policy.ValueFromRetention(p.Retention)
			require.NoError(t, err)
			varintValues = append(varintValues, int64(retentionValue))
		}
	}

	return varintValues, float64Values, bytesValues, arrayLenValues
}

func validateDecodeResults(t *testing.T, inputs ...metricWithPolicies) {
	var (
		varintValues   []int64
		float64Values  []float64
		bytesValues    [][]byte
		arrayLenValues []int
	)
	for _, input := range inputs {
		vi, f, b, al := getMockValuesFor(t, &input.metric, input.policies)
		varintValues = append(varintValues, vi...)
		float64Values = append(float64Values, f...)
		bytesValues = append(bytesValues, b...)
		arrayLenValues = append(arrayLenValues, al...)
	}
	rawIt := testGoodRawIterator(t, varintValues, float64Values, bytesValues, arrayLenValues)

	var results []metricWithPolicies
	for rawIt.Next() {
		value, policies := rawIt.Value()
		results = append(results, metricWithPolicies{
			metric:   *value,
			policies: policies,
		})
	}

	require.Equal(t, io.EOF, rawIt.Err())
	require.Equal(t, inputs, results)
}

func TestRawIteratorDecodeCounter(t *testing.T) {
	input := metricWithPolicies{
		metric:   testCounter,
		policies: policy.DefaultVersionedPolicies,
	}
	validateDecodeResults(t, input)
}

func TestRawIteratorDecodeBatchTimer(t *testing.T) {
	input := metricWithPolicies{
		metric:   testBatchTimer,
		policies: policy.DefaultVersionedPolicies,
	}
	validateDecodeResults(t, input)
}

func TestRawIteratorDecodeGauge(t *testing.T) {
	input := metricWithPolicies{
		metric:   testGauge,
		policies: policy.DefaultVersionedPolicies,
	}
	validateDecodeResults(t, input)
}

func TestRawIteratorDecodeAllTypesWithDefaultPolicies(t *testing.T) {
	validateDecodeResults(t, testInputWithAllTypesAndDefaultPolicies...)
}

func TestRawIteratorDecodeAllTypesWithCustomPolicies(t *testing.T) {
	validateDecodeResults(t, testInputWithAllTypesAndCustomPolicies...)
}

func TestRawIteratorDecodeError(t *testing.T) {
	it, err := NewRawIterator(nil, nil)
	require.NoError(t, err)
	err = errors.New("foo")
	it.(*rawIterator).err = err

	require.False(t, it.Next())
	require.Equal(t, err, it.Err())
}

func TestRawIteratorReset(t *testing.T) {
	it, err := NewRawIterator(nil, nil)
	require.NoError(t, err)
	err = errors.New("foo")
	it.(*rawIterator).err = err

	it.Reset(nil)
	require.NoError(t, it.(*rawIterator).err)
}
