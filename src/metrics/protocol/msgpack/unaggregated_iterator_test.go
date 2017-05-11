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
	"bufio"
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestUnaggregatedIteratorDecodeDefaultPoliciesList(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	enc.encodePoliciesList(testDefaultStagedPoliciesList)
	require.NoError(t, enc.err())
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.decodePoliciesList()
	require.NoError(t, it.Err())
	pl := it.PoliciesList()
	require.Equal(t, testDefaultStagedPoliciesList, pl)
}

func TestUnaggregatedIteratorDecodeSingleCustomPoliciesListWithAlloc(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	enc.encodePoliciesList(testSingleCustomStagedPoliciesList)
	require.NoError(t, enc.err())
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.decodePoliciesList()
	require.NoError(t, it.Err())
	pl := it.PoliciesList()
	require.Equal(t, testSingleCustomStagedPoliciesList, pl)
	require.True(t, len(it.cachedPolicies) >= len(testSingleCustomStagedPoliciesList))
	policies, _ := testSingleCustomStagedPoliciesList[0].Policies()
	require.Equal(t, it.cachedPolicies[0], policies)
}

func TestUnaggregatedIteratorDecodeSingleCustomPoliciesListNoPoliciesListAlloc(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	enc.encodePoliciesList(testSingleCustomStagedPoliciesList)
	require.NoError(t, enc.err())
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.cachedPoliciesList = make(policy.PoliciesList, len(testSingleCustomStagedPoliciesList)*3)
	it.decodePoliciesList()
	require.NoError(t, it.Err())
	pl := it.PoliciesList()
	require.Equal(t, testSingleCustomStagedPoliciesList, pl)
	require.True(t, len(it.cachedPolicies) >= len(testSingleCustomStagedPoliciesList))
	policies, _ := testSingleCustomStagedPoliciesList[0].Policies()
	require.Equal(t, it.cachedPolicies[0], policies)
}

func TestUnaggregatedIteratorDecodeSingleCustomPoliciesListNoAlloc(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	enc.encodePoliciesList(testSingleCustomStagedPoliciesList)
	require.NoError(t, enc.err())
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.cachedPoliciesList = make(policy.PoliciesList, len(testSingleCustomStagedPoliciesList)*3)
	it.cachedPolicies = make([][]policy.Policy, len(testSingleCustomStagedPoliciesList)*3)
	it.cachedPolicies[0] = make([]policy.Policy, 32)
	it.decodePoliciesList()
	require.NoError(t, it.Err())
	pl := it.PoliciesList()
	require.Equal(t, testSingleCustomStagedPoliciesList, pl)
	require.True(t, len(it.cachedPolicies) >= len(testSingleCustomStagedPoliciesList))
	policies, _ := testSingleCustomStagedPoliciesList[0].Policies()
	require.Equal(t, it.cachedPolicies[0], policies)
}

func TestUnaggregatedIteratorDecodeMultiCustomPoliciesListWithAlloc(t *testing.T) {
	input := testMultiCustomStagedPoliciesList
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	enc.encodePoliciesList(input)
	require.NoError(t, enc.err())
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.decodePoliciesList()
	require.NoError(t, it.Err())
	pl := it.PoliciesList()
	require.Equal(t, input, pl)
	require.True(t, len(it.cachedPolicies) >= len(input))
	for i := 0; i < len(input); i++ {
		policies, _ := input[i].Policies()
		require.Equal(t, it.cachedPolicies[i], policies)
	}
}

func TestUnaggregatedIteratorDecodeIDDecodeBytesLenError(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	enc.encodeFloat64(1.0)
	require.NoError(t, enc.err())
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	require.Equal(t, 0, len(it.decodeID()))
	require.Error(t, it.Err())
}

func TestUnaggregatedIteratorDecodeIDNilBytes(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	enc.encodeBytes(nil)
	require.NoError(t, enc.err())
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	require.Equal(t, 0, len(it.decodeID()))
	require.NoError(t, it.Err())
}

func TestUnaggregatedIteratorDecodeIDWithAlloc(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	data := []byte("foobarbaz")
	enc.encodeBytes(data)
	require.NoError(t, enc.err())
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	require.Equal(t, id.RawID(data), it.decodeID())
	require.NoError(t, it.Err())
}

func TestUnaggregatedIteratorDecodeIDNoAlloc(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	data := []byte("foobarbaz")
	enc.encodeBytes(data)
	require.NoError(t, enc.err())
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.id = make([]byte, len(data)*3)
	require.Equal(t, id.RawID(data), it.decodeID())
	require.NoError(t, it.Err())
}

func TestUnaggregatedIteratorDecodeIDReadError(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	data := []byte("foobarbazasierasekr")
	enc.encodeBytesLen(len(data) + 1)
	require.NoError(t, enc.err())

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	require.Equal(t, 0, len(it.decodeID()))
	require.Error(t, it.Err())
}

func TestUnaggregatedIteratorDecodeIDReadSuccess(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	data := []byte("foobarbazasierasekr")
	enc.encodeBytes(data)
	require.NoError(t, enc.err())

	// Intentionally buffer some data in the buffered reader.
	buf := append([]byte{1}, enc.Encoder().Bytes()...)
	reader := bufio.NewReaderSize(bytes.NewBuffer(buf), 16)
	reader.Read(make([]byte, 1))

	it := testUnaggregatedIterator(t, reader).(*unaggregatedIterator)
	it.id = make([]byte, len(data)/2)
	require.Equal(t, id.RawID(data), it.decodeID())
	require.NoError(t, it.Err())
}

func TestUnaggregatedIteratorDecodeBatchTimerDecodeArrayLenError(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	enc.encodeFloat64(1.0)
	require.NoError(t, enc.err())
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.decodeBatchTimer()
	require.Error(t, it.Err())
}

func TestUnaggregatedIteratorDecodeBatchTimerNoValues(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	bt := unaggregated.BatchTimer{
		ID:     []byte("foo"),
		Values: nil,
	}
	enc.encodeBatchTimer(bt)
	require.NoError(t, enc.err())

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.decodeBatchTimer()

	require.NoError(t, it.Err())
	mu := it.Metric()
	require.Equal(t, unaggregated.BatchTimerType, mu.Type)
	require.Equal(t, id.RawID("foo"), mu.ID)
	require.Equal(t, 0, len(mu.BatchTimerVal))
	require.False(t, mu.OwnsID)
	require.Nil(t, mu.TimerValPool)
}

func TestUnaggregatedIteratorDecodeBatchTimerDecodeFloat64Error(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	enc.encodeBatchTimerFn = func(bt unaggregated.BatchTimer) {
		enc.encodeNumObjectFields(numFieldsForType(batchTimerType))
		enc.encodeRawID(bt.ID)
		enc.encodeArrayLen(len(bt.Values))
		enc.encodeBytes([]byte("foo"))
	}
	require.NoError(t, enc.err())

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.decodeBatchTimer()

	require.Error(t, it.Err())
}

func TestUnaggregatedIteratorDecodeBatchTimerNoAlloc(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	bt := unaggregated.BatchTimer{
		ID:     []byte("foo"),
		Values: []float64{1.0, 2.0, 3.0, 4.0},
	}
	enc.encodeBatchTimer(bt)
	require.NoError(t, enc.err())

	// Allocate a large enough buffer to avoid triggering an allocation.
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.timerValues = make([]float64, 1000)
	it.decodeBatchTimer()

	require.NoError(t, it.Err())
	mu := it.Metric()
	require.Equal(t, unaggregated.BatchTimerType, mu.Type)
	require.Equal(t, id.RawID("foo"), mu.ID)
	require.Equal(t, bt.Values, mu.BatchTimerVal)
	require.Equal(t, cap(it.timerValues), cap(mu.BatchTimerVal))
	require.False(t, mu.OwnsID)
	require.Nil(t, mu.TimerValPool)
}

func TestUnaggregatedIteratorDecodeBatchTimerWithAllocNonPoolAlloc(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	bt := unaggregated.BatchTimer{
		ID:     []byte("foo"),
		Values: []float64{1.0, 2.0, 3.0, 4.0},
	}
	enc.encodeBatchTimer(bt)
	require.NoError(t, enc.err())

	// Allocate a large enough buffer to avoid triggering an allocation.
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.decodeBatchTimer()

	require.NoError(t, it.Err())
	mu := it.Metric()
	require.Equal(t, unaggregated.BatchTimerType, mu.Type)
	require.Equal(t, id.RawID("foo"), mu.ID)
	require.Equal(t, bt.Values, mu.BatchTimerVal)
	require.Equal(t, cap(it.timerValues), cap(mu.BatchTimerVal))
	require.False(t, mu.OwnsID)
	require.Nil(t, mu.TimerValPool)
}

func TestUnaggregatedIteratorDecodeBatchTimerWithAllocPoolAlloc(t *testing.T) {
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	bt := unaggregated.BatchTimer{
		ID:     []byte("foo"),
		Values: []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0},
	}
	enc.encodeBatchTimer(bt)
	require.NoError(t, enc.err())

	// Allocate a large enough buffer to avoid triggering an allocation.
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer()).(*unaggregatedIterator)
	it.timerValues = nil
	it.largeFloatsSize = 2
	it.decodeBatchTimer()

	require.NoError(t, it.Err())
	mu := it.Metric()
	require.Equal(t, unaggregated.BatchTimerType, mu.Type)
	require.Equal(t, id.RawID("foo"), mu.ID)
	require.Equal(t, bt.Values, mu.BatchTimerVal)
	require.True(t, cap(mu.BatchTimerVal) >= len(bt.Values))
	require.Nil(t, it.timerValues)
	require.False(t, mu.OwnsID)
	require.NotNil(t, mu.TimerValPool)
	require.Equal(t, it.largeFloatsPool, mu.TimerValPool)
}

func TestUnaggregatedIteratorDecodeNewerVersionThanSupported(t *testing.T) {
	input := metricWithPoliciesList{
		metric:       testCounter,
		policiesList: testDefaultStagedPoliciesList,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Version encoded is higher than supported version.
	enc.encodeRootObjectFn = func(objType objectType) {
		enc.encodeVersion(unaggregatedVersion + 1)
		enc.encodeNumObjectFields(numFieldsForType(rootObjectType))
		enc.encodeObjectType(objType)
	}
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, enc, input.metric, input.policiesList))

	// Now restore the encode top-level function and encode another counter.
	enc.encodeRootObjectFn = enc.encodeRootObject
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, enc, input.metric, input.policiesList))

	// Check that we skipped the first counter and successfully decoded the second counter.
	it := testUnaggregatedIterator(t, bytes.NewBuffer(enc.Encoder().Bytes()))
	it.(*unaggregatedIterator).ignoreHigherVersion = true
	validateUnaggregatedDecodeResults(t, it, []metricWithPoliciesList{input}, io.EOF)

	it.Reset(bytes.NewBuffer(enc.Encoder().Bytes()))
	it.(*unaggregatedIterator).ignoreHigherVersion = false
	validateUnaggregatedDecodeResults(t, it, nil, errors.New("received version 2 is higher than supported version 1"))
}

func TestUnaggregatedIteratorDecodeRootObjectMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPoliciesList{
		metric:       testCounter,
		policiesList: testDefaultStagedPoliciesList,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Pretend we added an extra int field to the root object.
	enc.encodeRootObjectFn = func(objType objectType) {
		enc.encodeVersion(unaggregatedVersion)
		enc.encodeNumObjectFields(numFieldsForType(rootObjectType) + 1)
		enc.encodeObjectType(objType)
	}
	testUnaggregatedEncodeMetricWithPoliciesList(t, enc, input.metric, input.policiesList)
	enc.encodeVarint(0)
	require.NoError(t, enc.err())

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer())

	// Check that we successfully decoded the counter.
	validateUnaggregatedDecodeResults(t, it, []metricWithPoliciesList{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodeCounterWithPoliciesMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPoliciesList{
		metric:       testCounter,
		policiesList: testDefaultStagedPoliciesList,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Pretend we added an extra int field to the counter with policies object.
	enc.encodeCounterWithPoliciesListFn = func(cp unaggregated.CounterWithPoliciesList) {
		enc.encodeNumObjectFields(numFieldsForType(counterWithPoliciesListType) + 1)
		enc.encodeCounterFn(cp.Counter)
		enc.encodePoliciesListFn(cp.PoliciesList)
		enc.encodeVarint(0)
	}
	testUnaggregatedEncodeMetricWithPoliciesList(t, enc, input.metric, input.policiesList)
	require.NoError(t, enc.err())

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer())

	// Check that we successfully decoded the counter.
	validateUnaggregatedDecodeResults(t, it, []metricWithPoliciesList{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodeCounterMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPoliciesList{
		metric:       testCounter,
		policiesList: testDefaultStagedPoliciesList,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Pretend we added an extra int field to the counter object.
	enc.encodeCounterFn = func(c unaggregated.Counter) {
		enc.encodeNumObjectFields(numFieldsForType(counterType) + 1)
		enc.encodeRawID(c.ID)
		enc.encodeVarint(int64(c.Value))
		enc.encodeVarint(0)
	}
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, enc, input.metric, input.policiesList))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer())

	// Check that we successfully decoded the counter.
	validateUnaggregatedDecodeResults(t, it, []metricWithPoliciesList{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodeBatchTimerMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPoliciesList{
		metric:       testBatchTimer,
		policiesList: testDefaultStagedPoliciesList,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Pretend we added an extra int field to the batch timer object.
	enc.encodeBatchTimerFn = func(bt unaggregated.BatchTimer) {
		enc.encodeNumObjectFields(numFieldsForType(batchTimerType) + 1)
		enc.encodeRawID(bt.ID)
		enc.encodeArrayLen(len(bt.Values))
		for _, v := range bt.Values {
			enc.encodeFloat64(v)
		}
		enc.encodeVarint(0)
	}
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, enc, input.metric, input.policiesList))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer())

	// Check that we successfully decoded the batch timer.
	validateUnaggregatedDecodeResults(t, it, []metricWithPoliciesList{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodeGaugeMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPoliciesList{
		metric:       testGauge,
		policiesList: testDefaultStagedPoliciesList,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Pretend we added an extra int field to the gauge object.
	enc.encodeGaugeFn = func(g unaggregated.Gauge) {
		enc.encodeNumObjectFields(numFieldsForType(gaugeType) + 1)
		enc.encodeRawID(g.ID)
		enc.encodeFloat64(g.Value)
		enc.encodeVarint(0)
	}
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, enc, input.metric, input.policiesList))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer())

	// Check that we successfully decoded the gauge.
	validateUnaggregatedDecodeResults(t, it, []metricWithPoliciesList{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodePolicyWithCustomResolution(t *testing.T) {
	input := metricWithPoliciesList{
		metric: testGauge,
		policiesList: policy.PoliciesList{
			policy.NewStagedPolicies(
				time.Now().UnixNano(),
				false,
				[]policy.Policy{
					policy.NewPolicy(3*time.Second, xtime.Second, time.Hour),
				},
			),
		},
	}
	enc := testUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, enc, input.metric, input.policiesList))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer())

	// Check that we successfully decoded the policy.
	validateUnaggregatedDecodeResults(t, it, []metricWithPoliciesList{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodePolicyWithCustomRetention(t *testing.T) {
	input := metricWithPoliciesList{
		metric: testGauge,
		policiesList: policy.PoliciesList{
			policy.NewStagedPolicies(
				time.Now().UnixNano(),
				false,
				[]policy.Policy{
					policy.NewPolicy(time.Second, xtime.Second, 289*time.Hour),
				},
			),
		},
	}
	enc := testUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, enc, input.metric, input.policiesList))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer())

	// Check that we successfully decoded the policy.
	validateUnaggregatedDecodeResults(t, it, []metricWithPoliciesList{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodePolicyMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPoliciesList{
		metric: testGauge,
		policiesList: policy.PoliciesList{
			policy.NewStagedPolicies(
				time.Now().UnixNano(),
				true,
				[]policy.Policy{
					policy.NewPolicy(time.Second, xtime.Second, time.Hour),
				},
			),
		},
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)
	baseEncoder := enc.encoderBase.(*baseEncoder)

	// Pretend we added an extra int field to the policy object.
	baseEncoder.encodePolicyFn = func(p policy.Policy) {
		baseEncoder.encodeNumObjectFields(numFieldsForType(policyType) + 1)
		baseEncoder.encodeResolution(p.Resolution())
		baseEncoder.encodeRetention(p.Retention())
		baseEncoder.encodeVarint(0)
	}
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, enc, input.metric, input.policiesList))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer())

	// Check that we successfully decoded the policy.
	validateUnaggregatedDecodeResults(t, it, []metricWithPoliciesList{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodePoliciesListMoreFieldsThanExpected(t *testing.T) {
	input := metricWithPoliciesList{
		metric:       testGauge,
		policiesList: testSingleCustomStagedPoliciesList,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Pretend we added an extra int field to the policy object.
	enc.encodePoliciesListFn = func(pl policy.PoliciesList) {
		enc.encodeNumObjectFields(numFieldsForType(customPoliciesListType) + 1)
		enc.encodeObjectType(customPoliciesListType)
		enc.encodeArrayLen(len(pl))
		for _, sp := range pl {
			enc.encodeStagedPolicies(sp)
		}
		enc.encodeVarint(0)
	}
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, enc, input.metric, input.policiesList))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer())

	// Check that we successfully decoded the policy.
	validateUnaggregatedDecodeResults(t, it, []metricWithPoliciesList{input}, io.EOF)
}

func TestUnaggregatedIteratorDecodeCounterFewerFieldsThanExpected(t *testing.T) {
	input := metricWithPoliciesList{
		metric:       testCounter,
		policiesList: testDefaultStagedPoliciesList,
	}
	enc := testUnaggregatedEncoder(t).(*unaggregatedEncoder)

	// Pretend we added an extra int field to the counter object.
	enc.encodeCounterFn = func(c unaggregated.Counter) {
		enc.encodeNumObjectFields(numFieldsForType(counterType) - 1)
		enc.encodeRawID(c.ID)
	}
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, enc, input.metric, input.policiesList))

	it := testUnaggregatedIterator(t, enc.Encoder().Buffer())

	// Check that we encountered an error during decoding.
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
	input := metricWithPoliciesList{
		metric:       testCounter,
		policiesList: testStagedPoliciesWithInvalidTimeUnit,
	}
	enc := testUnaggregatedEncoder(t)
	require.NoError(t, testUnaggregatedEncodeMetricWithPoliciesList(t, enc, input.metric, input.policiesList))
	it := testUnaggregatedIterator(t, enc.Encoder().Buffer())
	validateUnaggregatedDecodeResults(t, it, nil, errors.New("invalid precision unknown"))
}

func validateUnaggregatedDecodeResults(
	t *testing.T,
	it UnaggregatedIterator,
	expectedResults []metricWithPoliciesList,
	expectedErr error,
) {
	var results []metricWithPoliciesList
	for it.Next() {
		metric, policiesList := it.Metric(), it.PoliciesList()
		policiesList = toPoliciesList(t, policiesList)
		results = append(results, metricWithPoliciesList{
			metric:       metric,
			policiesList: policiesList,
		})
	}
	require.Equal(t, expectedErr, it.Err())
	validateMetricsWithPoliciesList(t, expectedResults, results)
}
