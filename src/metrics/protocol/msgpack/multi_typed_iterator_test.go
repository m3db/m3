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
	"io"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func validateDecodeResults(
	t *testing.T,
	it MultiTypedIterator,
	expectedResults []metricWithPolicies,
	expectedErr error,
) {
	var results []metricWithPolicies
	for it.Next() {
		value, policies := it.Value()
		results = append(results, metricWithPolicies{
			metric:            *value,
			versionedPolicies: policies,
		})
	}
	require.Equal(t, expectedErr, it.Err())
	require.Equal(t, expectedResults, results)
}

func TestMultiTypedIteratorDecodeNewerVersionThanSupported(t *testing.T) {
	input := metricWithPolicies{
		metric:            testCounter,
		versionedPolicies: policy.DefaultVersionedPolicies,
	}
	enc := testMultiTypedEncoder(t).(*multiTypedEncoder)

	// Version encoded is higher than supported version
	enc.encodeTopLevelFn = func(objType objectType) {
		enc.encodeVersion(supportedVersion + 1)
		enc.encodeObjectType(objType)
		enc.encodeNumObjectFields(numFieldsForType(objType))
	}
	require.NoError(t, enc.EncodeCounterWithPolicies(input.metric.Counter(), input.versionedPolicies))

	// Now restore the encode top-level function and encode another counter
	enc.encodeTopLevelFn = enc.encodeTopLevel
	require.NoError(t, enc.EncodeCounterWithPolicies(input.metric.Counter(), input.versionedPolicies))

	it := testMultiTypedIterator(t, enc.Encoder().Buffer)

	// Check that we skipped the first counter and normally decoded the second counter
	validateDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestMultiTypedIteratorDecodeTopLevelMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric:            testCounter,
		versionedPolicies: policy.DefaultVersionedPolicies,
	}
	enc := testMultiTypedEncoder(t).(*multiTypedEncoder)

	// Pretend we added an extra int field to the top-level object
	enc.encodeTopLevelFn = func(objType objectType) {
		enc.encodeVersion(supportedVersion)
		enc.encodeObjectType(objType)
		enc.encodeNumObjectFields(numFieldsForType(objType) + 1)
	}
	enc.EncodeCounterWithPolicies(input.metric.Counter(), input.versionedPolicies)
	enc.encodeVarintFn(0)
	require.NoError(t, enc.err)

	it := testMultiTypedIterator(t, enc.Encoder().Buffer)

	// Check that we normally decoded the counter
	validateDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestMultiTypedIteratorDecodeCounterMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric:            testCounter,
		versionedPolicies: policy.DefaultVersionedPolicies,
	}
	enc := testMultiTypedEncoder(t).(*multiTypedEncoder)

	// Pretend we added an extra int field to the counter object
	enc.encodeCounterFn = func(c metric.Counter) {
		enc.encodeNumObjectFields(numFieldsForType(counterType) + 1)
		enc.encodeID(c.ID)
		enc.encodeVarintFn(int64(c.Value))
		enc.encodeVarintFn(0)
	}
	require.NoError(t, enc.EncodeCounterWithPolicies(input.metric.Counter(), input.versionedPolicies))

	it := testMultiTypedIterator(t, enc.Encoder().Buffer)

	// Check that we normally decoded the counter
	validateDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestMultiTypedIteratorDecodeBatchTimerMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric:            testBatchTimer,
		versionedPolicies: policy.DefaultVersionedPolicies,
	}
	enc := testMultiTypedEncoder(t).(*multiTypedEncoder)

	// Pretend we added an extra int field to the batch timer object
	enc.encodeBatchTimerFn = func(bt metric.BatchTimer) {
		enc.encodeNumObjectFields(numFieldsForType(batchTimerType) + 1)
		enc.encodeID(bt.ID)
		enc.encodeArrayLenFn(len(bt.Values))
		for _, v := range bt.Values {
			enc.encodeFloat64Fn(v)
		}
		enc.encodeVarintFn(0)
	}
	require.NoError(t, enc.EncodeBatchTimerWithPolicies(
		input.metric.BatchTimer(),
		input.versionedPolicies,
	))

	it := testMultiTypedIterator(t, enc.Encoder().Buffer)

	// Check that we normally decoded the batch timer
	validateDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestMultiTypedIteratorDecodeGaugeMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric:            testGauge,
		versionedPolicies: policy.DefaultVersionedPolicies,
	}
	enc := testMultiTypedEncoder(t).(*multiTypedEncoder)

	// Pretend we added an extra int field to the gauge object
	enc.encodeGaugeFn = func(g metric.Gauge) {
		enc.encodeNumObjectFields(numFieldsForType(gaugeType) + 1)
		enc.encodeID(g.ID)
		enc.encodeFloat64Fn(g.Value)
		enc.encodeVarintFn(0)
	}
	require.NoError(t, enc.EncodeGaugeWithPolicies(input.metric.Gauge(), input.versionedPolicies))

	it := testMultiTypedIterator(t, enc.Encoder().Buffer)

	// Check that we normally decoded the gauge
	validateDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestMultiTypedIteratorDecodePolicyMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric: testGauge,
		versionedPolicies: policy.VersionedPolicies{
			Version: 1,
			Policies: []policy.Policy{
				{
					Resolution: policy.Resolution{Window: time.Duration(1), Precision: xtime.Second},
					Retention:  policy.Retention(time.Hour),
				},
			},
		},
	}
	enc := testMultiTypedEncoder(t).(*multiTypedEncoder)

	// Pretend we added an extra int field to the policy object
	enc.encodePolicyFn = func(p policy.Policy) {
		enc.encodeNumObjectFields(numFieldsForType(policyType) + 1)
		enc.encodeResolution(p.Resolution)
		enc.encodeRetention(p.Retention)
		enc.encodeVarintFn(0)
	}
	require.NoError(t, enc.EncodeGaugeWithPolicies(input.metric.Gauge(), input.versionedPolicies))

	it := testMultiTypedIterator(t, enc.Encoder().Buffer)

	// Check that we normally decoded the policy
	validateDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestMultiTypedIteratorDecodeVersionedPoliciesMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric: testGauge,
		versionedPolicies: policy.VersionedPolicies{
			Version: 1,
			Policies: []policy.Policy{
				{
					Resolution: policy.Resolution{Window: time.Duration(1), Precision: xtime.Second},
					Retention:  policy.Retention(time.Hour),
				},
			},
		},
	}
	enc := testMultiTypedEncoder(t).(*multiTypedEncoder)

	// Pretend we added an extra int field to the policy object
	enc.encodeVersionedPoliciesFn = func(vp policy.VersionedPolicies) {
		enc.encodeNumObjectFields(numFieldsForType(customVersionedPolicyType) + 1)
		enc.encodeVersion(vp.Version)
		enc.encodeArrayLenFn(len(vp.Policies))
		for _, policy := range vp.Policies {
			enc.encodePolicyFn(policy)
		}
		enc.encodeVarintFn(0)
	}
	require.NoError(t, enc.EncodeGaugeWithPolicies(input.metric.Gauge(), input.versionedPolicies))

	it := testMultiTypedIterator(t, enc.Encoder().Buffer)

	// Check that we normally decoded the policy
	validateDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestMultiTypedIteratorDecodeCounterFewerFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric:            testCounter,
		versionedPolicies: policy.DefaultVersionedPolicies,
	}
	enc := testMultiTypedEncoder(t).(*multiTypedEncoder)

	// Pretend we added an extra int field to the counter object
	enc.encodeCounterFn = func(c metric.Counter) {
		enc.encodeNumObjectFields(numFieldsForType(counterType) - 1)
		enc.encodeID(c.ID)
	}
	require.NoError(t, enc.EncodeCounterWithPolicies(input.metric.Counter(), input.versionedPolicies))

	it := testMultiTypedIterator(t, enc.Encoder().Buffer)

	// Check that we normally decoded the counter
	validateDecodeResults(t, it, nil, errors.New("number of fields mismatch: expected 2 actual 1"))
}

func TestMultiTypedIteratorDecodeError(t *testing.T) {
	it, err := NewMultiTypedIterator(nil, nil)
	require.NoError(t, err)
	err = errors.New("foo")
	it.(*multiTypedIterator).err = err

	require.False(t, it.Next())
	require.Equal(t, err, it.Err())
}

func TestMultiTypedIteratorReset(t *testing.T) {
	it, err := NewMultiTypedIterator(nil, nil)
	require.NoError(t, err)
	err = errors.New("foo")
	it.(*multiTypedIterator).err = err

	it.Reset(nil)
	require.NoError(t, it.(*multiTypedIterator).err)
}
