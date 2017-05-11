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

	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"

	"github.com/stretchr/testify/require"
)

var (
	testRawMetricData      = []byte("foodg")
	errTestDecodeRawMetric = errors.New("foo")
)

func TestRawMetricDecodeIDExistingError(t *testing.T) {
	m := testRawMetric()
	m.it.setErr(errTestDecodeRawMetric)
	_, err := m.ID()
	require.Equal(t, errTestDecodeRawMetric, err)
}

func TestRawMetricDecodeIDVersionError(t *testing.T) {
	m := testRawMetric()
	m.it.(*mockBaseIterator).decodeVersionFn = func() int {
		return metricVersion + 1
	}
	_, err := m.ID()
	require.Error(t, err)
}

func TestRawMetricDecodeIDBytesLenDecodeError(t *testing.T) {
	m := testRawMetric()
	m.it.(*mockBaseIterator).decodeBytesLenFn = func() int {
		m.it.setErr(errTestDecodeRawMetric)
		return 0
	}
	_, err := m.ID()
	require.Equal(t, errTestDecodeRawMetric, err)
}

func TestRawMetricDecodeIDBytesLenOutOfRange(t *testing.T) {
	m := testRawMetric()
	m.it.(*mockBaseIterator).decodeBytesLenFn = func() int { return -100 }
	_, err := m.ID()
	require.Error(t, err)

	m = testRawMetric()
	m.it.(*mockBaseIterator).decodeBytesLenFn = func() int {
		return len(testRawMetricData) + 1
	}
	_, err = m.ID()
	require.Error(t, err)
}

func TestRawMetricDecodeIDSuccess(t *testing.T) {
	m := testRawMetric()
	id, err := m.ID()
	require.NoError(t, err)
	require.Equal(t, testMetric.ID, id)
	require.True(t, m.idDecoded)

	// Get ID again to make sure we don't re-decode the ID.
	id, err = m.ID()
	require.NoError(t, err)
	require.Equal(t, testMetric.ID, id)
}

func TestRawMetricDecodeTimestampExistingError(t *testing.T) {
	m := testRawMetric()
	m.it.setErr(errTestDecodeRawMetric)
	_, err := m.TimeNanos()
	require.Equal(t, errTestDecodeRawMetric, err)
}

func TestRawMetricDecodeTimestampDecodeError(t *testing.T) {
	m := testRawMetric()
	m.it.(*mockBaseIterator).decodeVarintFn = func() int64 {
		m.it.setErr(errTestDecodeRawMetric)
		return 0
	}
	_, err := m.TimeNanos()
	require.Equal(t, errTestDecodeRawMetric, err)
}

func TestRawMetricDecodeTimestampSuccess(t *testing.T) {
	m := testRawMetric()
	timeNanos, err := m.TimeNanos()
	require.NoError(t, err)
	require.Equal(t, testMetric.TimeNanos, timeNanos)
	require.True(t, m.timeDecoded)

	// Get timestamp again to make sure we don't re-decode the timestamp.
	require.NoError(t, err)
	require.Equal(t, testMetric.TimeNanos, timeNanos)
}

func TestRawMetricDecodeValueExistingError(t *testing.T) {
	m := testRawMetric()
	m.it.setErr(errTestDecodeRawMetric)
	_, err := m.Value()
	require.Equal(t, errTestDecodeRawMetric, err)
}

func TestRawMetricDecodeValueDecodeError(t *testing.T) {
	m := testRawMetric()
	m.it.(*mockBaseIterator).decodeFloat64Fn = func() float64 {
		m.it.setErr(errTestDecodeRawMetric)
		return 0
	}
	_, err := m.Value()
	require.Equal(t, errTestDecodeRawMetric, err)
}

func TestRawMetricDecodeValueSuccess(t *testing.T) {
	m := testRawMetric()
	value, err := m.Value()
	require.NoError(t, err)
	require.Equal(t, testMetric.Value, value)
	require.True(t, m.valueDecoded)

	value, err = m.Value()
	require.NoError(t, err)
	require.Equal(t, testMetric.Value, value)
}

func TestRawMetricDecodeMetricExistingError(t *testing.T) {
	m := testRawMetric()
	m.it.setErr(errTestDecodeRawMetric)
	_, err := m.Metric()
	require.Equal(t, errTestDecodeRawMetric, err)
}

func TestRawMetricDecodeMetricSuccess(t *testing.T) {
	m := testRawMetric()
	metric, err := m.Metric()
	require.NoError(t, err)
	require.Equal(t, testMetric, metric)
	require.True(t, m.idDecoded)
	require.True(t, m.timeDecoded)
	require.True(t, m.valueDecoded)

	// Get metric again to make sure we don't re-decode the metric.
	require.NoError(t, err)
	require.Equal(t, testMetric, metric)
}

func TestRawMetricBytes(t *testing.T) {
	m := testRawMetric()
	require.Equal(t, m.data, m.Bytes())
}

func TestRawMetricNilID(t *testing.T) {
	r := NewRawMetric(nil, 16)
	r.Reset(toRawMetric(t, emptyMetric).Bytes())
	decoded, err := r.ID()
	require.NoError(t, err)
	require.Nil(t, decoded)
	require.True(t, r.(*rawMetric).idDecoded)
}

func TestRawMetricReset(t *testing.T) {
	metrics := []aggregated.Metric{
		{ID: id.RawID("foo"), TimeNanos: testMetric.TimeNanos, Value: 1.0},
		{ID: id.RawID("bar"), TimeNanos: testMetric.TimeNanos, Value: 2.3},
		{ID: id.RawID("baz"), TimeNanos: testMetric.TimeNanos, Value: 4234.234},
	}
	rawMetric := NewRawMetric(nil, 16)
	for i := 0; i < len(metrics); i++ {
		rawMetric.Reset(toRawMetric(t, metrics[i]).Bytes())
		decoded, err := rawMetric.Metric()
		require.NoError(t, err)
		require.Equal(t, metrics[i], decoded)
	}
}

func TestRawMetricRoundtripStress(t *testing.T) {
	metrics := []aggregated.Metric{
		{ID: id.RawID("foo"), TimeNanos: testMetric.TimeNanos, Value: 1.0},
		{ID: id.RawID("bar"), TimeNanos: testMetric.TimeNanos, Value: 2.3},
		{ID: id.RawID("baz"), TimeNanos: testMetric.TimeNanos, Value: 4234.234},
	}
	var (
		inputs  []aggregated.Metric
		results []aggregated.Metric
		numIter = 2
	)
	for i := 0; i < numIter; i++ {
		input := metrics[i%len(metrics)]
		inputs = append(inputs, input)
		rawMetric := toRawMetric(t, input)
		decoded, err := rawMetric.Metric()
		require.NoError(t, err)
		results = append(results, decoded)
	}
	require.Equal(t, inputs, results)
}

type decodeVersionFn func() int
type decodeBytesLenFn func() int
type decodeVarintFn func() int64
type decodeFloat64Fn func() float64

type mockBaseIterator struct {
	bufReader        bufReader
	itErr            error
	decodeVersionFn  decodeVersionFn
	decodeBytesLenFn decodeBytesLenFn
	decodeVarintFn   decodeVarintFn
	decodeFloat64Fn  decodeFloat64Fn
}

func (it *mockBaseIterator) reset(reader io.Reader)       {}
func (it *mockBaseIterator) err() error                   { return it.itErr }
func (it *mockBaseIterator) setErr(err error)             { it.itErr = err }
func (it *mockBaseIterator) reader() bufReader            { return it.bufReader }
func (it *mockBaseIterator) decodePolicy() policy.Policy  { return policy.EmptyPolicy }
func (it *mockBaseIterator) decodeVersion() int           { return it.decodeVersionFn() }
func (it *mockBaseIterator) decodeObjectType() objectType { return unknownType }
func (it *mockBaseIterator) decodeNumObjectFields() int   { return 0 }
func (it *mockBaseIterator) decodeRawID() id.RawID        { return nil }
func (it *mockBaseIterator) decodeVarint() int64          { return it.decodeVarintFn() }
func (it *mockBaseIterator) decodeBool() bool             { return false }
func (it *mockBaseIterator) decodeFloat64() float64       { return it.decodeFloat64Fn() }
func (it *mockBaseIterator) decodeBytes() []byte          { return nil }
func (it *mockBaseIterator) decodeBytesLen() int          { return it.decodeBytesLenFn() }
func (it *mockBaseIterator) decodeArrayLen() int          { return 0 }
func (it *mockBaseIterator) skip(numFields int)           {}

func (it *mockBaseIterator) checkNumFieldsForType(objType objectType) (int, int, bool) {
	return 0, 0, true
}

func (it *mockBaseIterator) checkExpectedNumFieldsForType(
	objType objectType,
	numActualFields int,
) (int, bool) {
	return 0, true
}

func testRawMetric() *rawMetric {
	mockIt := &mockBaseIterator{}
	mockIt.decodeVersionFn = func() int { return metricVersion }
	mockIt.decodeBytesLenFn = func() int { return len(testMetric.ID) }
	mockIt.decodeVarintFn = func() int64 { return testMetric.TimeNanos }
	mockIt.decodeFloat64Fn = func() float64 { return testMetric.Value }
	mockIt.bufReader = bytes.NewReader(testRawMetricData)

	m := NewRawMetric(testRawMetricData, 16).(*rawMetric)
	m.it = mockIt

	return m
}
