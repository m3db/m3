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
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func validateUnaggregatedDecodeResults(
	t *testing.T,
	it UnaggregatedIterator,
	expectedResults []metricWithPolicies,
	expectedErr error,
) {
	var results []metricWithPolicies
	for it.Next() {
		value, policies := it.Value()
		results = append(results, metricWithPolicies{
			metric:            value,
			versionedPolicies: policies,
		})
	}
	require.Equal(t, expectedErr, it.Err())
	require.Equal(t, expectedResults, results)
}

func TestUnaggregatedIteratorDecodeNewerVersionThanSupported(t *testing.T) {
	input := metricWithPolicies{
		metric:            testCounter,
		versionedPolicies: policy.DefaultVersionedPolicies,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Version encoded is higher than supported version
	enc.encodeRootObjectFn = func(objType objectType) {
		enc.encodeVersion(unaggregatedVersion + 1)
		enc.encodeNumObjectFields(numFieldsForType(rootObjectType))
		enc.encodeObjectType(objType)
	}
	require.NoError(t, testUnaggregatedEncode(t, enc, input.metric, input.versionedPolicies))

	// Now restore the encode top-level function and encode another counter
	enc.encodeRootObjectFn = enc.encodeRootObject
	require.NoError(t, testUnaggregatedEncode(t, enc, input.metric, input.versionedPolicies))

	// Check that we skipped the first counter and successfully decoded the second counter
	it := testUnaggregatedIterator(t, bytes.NewBuffer(enc.Encoder().Buffer.Bytes()))
	it.(*unaggregatedIterator).ignoreHigherVersion = true
	validateUnaggregatedDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)

	it.Reset(bytes.NewBuffer(enc.Encoder().Buffer.Bytes()))
	it.(*unaggregatedIterator).ignoreHigherVersion = false
	validateUnaggregatedDecodeResults(t, it, nil, errors.New("received version 2 is higher than supported version 1"))
}

func TestUnaggregatedIteratorDecodeRootObjectMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric:            testCounter,
		versionedPolicies: policy.DefaultVersionedPolicies,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Pretend we added an extra int field to the root object
	enc.encodeRootObjectFn = func(objType objectType) {
		enc.encodeVersion(unaggregatedVersion)
		enc.encodeNumObjectFields(numFieldsForType(rootObjectType) + 1)
		enc.encodeObjectType(objType)
	}
	testUnaggregatedEncode(t, enc, input.metric, input.versionedPolicies)
	enc.encodeVarint(0)
	require.NoError(t, enc.err())

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer)

	// Check that we successfully decoded the counter
	validateUnaggregatedDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodeCounterWithPoliciesMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric:            testCounter,
		versionedPolicies: policy.DefaultVersionedPolicies,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Pretend we added an extra int field to the counter with policies object
	enc.encodeCounterWithPoliciesFn = func(cp unaggregated.CounterWithPolicies) {
		enc.encodeNumObjectFields(numFieldsForType(counterWithPoliciesType) + 1)
		enc.encodeCounterFn(cp.Counter)
		enc.encodeVersionedPoliciesFn(cp.VersionedPolicies)
	}
	testUnaggregatedEncode(t, enc, input.metric, input.versionedPolicies)
	enc.encodeVarint(0)
	require.NoError(t, enc.err())

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer)

	// Check that we successfully decoded the counter
	validateUnaggregatedDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodeCounterMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric:            testCounter,
		versionedPolicies: policy.DefaultVersionedPolicies,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Pretend we added an extra int field to the counter object
	enc.encodeCounterFn = func(c unaggregated.Counter) {
		enc.encodeNumObjectFields(numFieldsForType(counterType) + 1)
		enc.encodeID(c.ID)
		enc.encodeVarint(int64(c.Value))
		enc.encodeVarint(0)
	}
	require.NoError(t, testUnaggregatedEncode(t, enc, input.metric, input.versionedPolicies))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer)

	// Check that we successfully decoded the counter
	validateUnaggregatedDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodeBatchTimerMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric:            testBatchTimer,
		versionedPolicies: policy.DefaultVersionedPolicies,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Pretend we added an extra int field to the batch timer object
	enc.encodeBatchTimerFn = func(bt unaggregated.BatchTimer) {
		enc.encodeNumObjectFields(numFieldsForType(batchTimerType) + 1)
		enc.encodeID(bt.ID)
		enc.encodeArrayLen(len(bt.Values))
		for _, v := range bt.Values {
			enc.encodeFloat64(v)
		}
		enc.encodeVarint(0)
	}
	require.NoError(t, testUnaggregatedEncode(t, enc, input.metric, input.versionedPolicies))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer)

	// Check that we successfully decoded the batch timer
	validateUnaggregatedDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodeGaugeMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric:            testGauge,
		versionedPolicies: policy.DefaultVersionedPolicies,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Pretend we added an extra int field to the gauge object
	enc.encodeGaugeFn = func(g unaggregated.Gauge) {
		enc.encodeNumObjectFields(numFieldsForType(gaugeType) + 1)
		enc.encodeID(g.ID)
		enc.encodeFloat64(g.Value)
		enc.encodeVarint(0)
	}
	require.NoError(t, testUnaggregatedEncode(t, enc, input.metric, input.versionedPolicies))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer)

	// Check that we successfully decoded the gauge
	validateUnaggregatedDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodePolicyWithCustomResolution(t *testing.T) {
	input := metricWithPolicies{
		metric: testGauge,
		versionedPolicies: policy.VersionedPolicies{
			Version: 1,
			Cutover: time.Now(),
			Policies: []policy.Policy{
				{
					Resolution: policy.Resolution{Window: 3 * time.Second, Precision: xtime.Second},
					Retention:  policy.Retention(time.Hour),
				},
			},
		},
	}
	enc := testUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncode(t, enc, input.metric, input.versionedPolicies))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer)

	// Check that we successfully decoded the policy
	validateUnaggregatedDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodePolicyWithCustomRetention(t *testing.T) {
	input := metricWithPolicies{
		metric: testGauge,
		versionedPolicies: policy.VersionedPolicies{
			Version: 1,
			Cutover: time.Now(),
			Policies: []policy.Policy{
				{
					Resolution: policy.Resolution{Window: time.Second, Precision: xtime.Second},
					Retention:  policy.Retention(289 * time.Hour),
				},
			},
		},
	}
	enc := testUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncode(t, enc, input.metric, input.versionedPolicies))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer)

	// Check that we successfully decoded the policy
	validateUnaggregatedDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodePolicyMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric: testGauge,
		versionedPolicies: policy.VersionedPolicies{
			Version: 1,
			Cutover: time.Now(),
			Policies: []policy.Policy{
				{
					Resolution: policy.Resolution{Window: time.Second, Precision: xtime.Second},
					Retention:  policy.Retention(time.Hour),
				},
			},
		},
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	baseEncoder := enc.encoderBase.(*baseEncoder)

	// Pretend we added an extra int field to the policy object
	baseEncoder.encodePolicyFn = func(p policy.Policy) {
		baseEncoder.encodeNumObjectFields(numFieldsForType(policyType) + 1)
		baseEncoder.encodeResolution(p.Resolution)
		baseEncoder.encodeRetention(p.Retention)
		baseEncoder.encodeVarint(0)
	}
	require.NoError(t, testUnaggregatedEncode(t, enc, input.metric, input.versionedPolicies))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer)

	// Check that we successfully decoded the policy
	validateUnaggregatedDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodeVersionedPoliciesMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric: testGauge,
		versionedPolicies: policy.VersionedPolicies{
			Version: 1,
			Cutover: time.Now(),
			Policies: []policy.Policy{
				{
					Resolution: policy.Resolution{Window: time.Second, Precision: xtime.Second},
					Retention:  policy.Retention(time.Hour),
				},
			},
		},
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Pretend we added an extra int field to the policy object
	enc.encodeVersionedPoliciesFn = func(vp policy.VersionedPolicies) {
		enc.encodeNumObjectFields(numFieldsForType(customVersionedPoliciesType) + 1)
		enc.encodeObjectType(customVersionedPoliciesType)
		enc.encodeVersion(vp.Version)
		enc.encodeTime(vp.Cutover)
		enc.encodeArrayLen(len(vp.Policies))
		for _, policy := range vp.Policies {
			enc.encodePolicy(policy)
		}
		enc.encodeVarint(0)
	}
	require.NoError(t, testUnaggregatedEncode(t, enc, input.metric, input.versionedPolicies))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer)

	// Check that we successfully decoded the policy
	validateUnaggregatedDecodeResults(t, it, []metricWithPolicies{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodeCounterFewerFieldsThanExpected(t *testing.T) {
	input := metricWithPolicies{
		metric:            testCounter,
		versionedPolicies: policy.DefaultVersionedPolicies,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Pretend we added an extra int field to the counter object
	enc.encodeCounterFn = func(c unaggregated.Counter) {
		enc.encodeNumObjectFields(numFieldsForType(counterType) - 1)
		enc.encodeID(c.ID)
	}
	require.NoError(t, testUnaggregatedEncode(t, enc, input.metric, input.versionedPolicies))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer)

	// Check that we successfully decoded the counter
	validateUnaggregatedDecodeResults(t, it, nil, errors.New("number of fields mismatch: expected 2 actual 1"))
}

func TestUnaggregatedIteratorDecodeError(t *testing.T) {
	it := NewUnaggregatedIterator(nil, nil)
	err := errors.New("foo")
	it.(*unaggregatedIterator).setErr(err)

	require.False(t, it.Next())
	require.Equal(t, err, it.Err())
}

func TestUnaggregatedIteratorReset(t *testing.T) {
	it := NewUnaggregatedIterator(nil, nil)
	err := errors.New("foo")
	it.(*unaggregatedIterator).setErr(err)

	it.Reset(nil)
	require.NoError(t, it.(*unaggregatedIterator).Err())
	require.False(t, it.(*unaggregatedIterator).closed)
}

func TestUnaggregatedIteratorClose(t *testing.T) {
	it := NewUnaggregatedIterator(nil, nil)
	it.Close()
	require.False(t, it.Next())
	require.NoError(t, it.Err())
	require.True(t, it.(*unaggregatedIterator).closed)
}

func TestUnaggregatedIteratorDecodeInvalidTimeUnit(t *testing.T) {
	input := metricWithPolicies{
		metric:            testCounter,
		versionedPolicies: testVersionedPoliciesWithInvalidTimeUnit,
	}
	enc := testUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncode(t, enc, input.metric, input.versionedPolicies))
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer)
	validateUnaggregatedDecodeResults(t, it, nil, errors.New("invalid precision unknown"))
}
