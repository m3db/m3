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
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/aggregated"

	"github.com/stretchr/testify/require"
)

var (
	testRawMetricData      = []byte("foodg")
	errTestDecodeRawMetric = errors.New("foo")
)

func testRawMetric() *rawMetric {
	m := NewRawMetric(testRawMetricData).(*rawMetric)

	m.decodeVersionFn = func() int { return metricVersion }
	m.decodeBytesLenFn = func() int { return len(testMetric.ID) }
	m.decodeTimeFn = func() time.Time { return testMetric.Timestamp }
	m.decodeFloat64Fn = func() float64 { return testMetric.Value }
	m.readBytesFn = func(n int) []byte { return testRawMetricData[:n] }

	return m
}

func TestRawMetricDecodeIDExistingError(t *testing.T) {
	m := testRawMetric()
	m.it.setErr(errTestDecodeRawMetric)
	_, err := m.ID()
	require.Equal(t, errTestDecodeRawMetric, err)
}

func TestRawMetricDecodeIDVersionError(t *testing.T) {
	m := testRawMetric()
	m.decodeVersionFn = func() int { return metricVersion + 1 }
	_, err := m.ID()
	require.Error(t, err)
}

func TestRawMetricDecodeIDBytesLenDecodeError(t *testing.T) {
	m := testRawMetric()
	m.decodeBytesLenFn = func() int {
		m.it.setErr(errTestDecodeRawMetric)
		return 0
	}
	_, err := m.ID()
	require.Equal(t, errTestDecodeRawMetric, err)
}

func TestRawMetricDecodeIDBytesLenOutOfRange(t *testing.T) {
	m := testRawMetric()
	m.decodeBytesLenFn = func() int { return -1 }
	_, err := m.ID()
	require.Error(t, err)

	m = testRawMetric()
	m.decodeBytesLenFn = func() int { return len(testRawMetricData) + 1 }
	_, err = m.ID()
	require.Error(t, err)
}

func TestRawMetricDecodeIDSuccess(t *testing.T) {
	m := testRawMetric()
	id, err := m.ID()
	require.NoError(t, err)
	require.Equal(t, testMetric.ID, id)
	require.True(t, m.idDecoded)

	// Get ID again to make sure we don't re-decode the ID
	id, err = m.ID()
	require.NoError(t, err)
	require.Equal(t, testMetric.ID, id)
}

func TestRawMetricDecodeTimestampExistingError(t *testing.T) {
	m := testRawMetric()
	m.it.setErr(errTestDecodeRawMetric)
	_, err := m.Timestamp()
	require.Equal(t, errTestDecodeRawMetric, err)
}

func TestRawMetricDecodeTimestampDecodeError(t *testing.T) {
	m := testRawMetric()
	m.decodeTimeFn = func() time.Time {
		m.it.setErr(errTestDecodeRawMetric)
		return time.Time{}
	}
	_, err := m.Timestamp()
	require.Equal(t, errTestDecodeRawMetric, err)
}

func TestRawMetricDecodeTimestampSuccess(t *testing.T) {
	m := testRawMetric()
	timestamp, err := m.Timestamp()
	require.NoError(t, err)
	require.Equal(t, testMetric.Timestamp, timestamp)
	require.True(t, m.timestampDecoded)

	// Get timestamp again to make sure we don't re-decode the timestamp
	require.NoError(t, err)
	require.Equal(t, testMetric.Timestamp, timestamp)
}

func TestRawMetricDecodeValueExistingError(t *testing.T) {
	m := testRawMetric()
	m.it.setErr(errTestDecodeRawMetric)
	_, err := m.Value()
	require.Equal(t, errTestDecodeRawMetric, err)
}

func TestRawMetricDecodeValueDecodeError(t *testing.T) {
	m := testRawMetric()
	m.decodeFloat64Fn = func() float64 {
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
	require.True(t, m.timestampDecoded)
	require.True(t, m.valueDecoded)

	// Get metric again to make sure we don't re-decode the metric
	require.NoError(t, err)
	require.Equal(t, testMetric, metric)
}

func TestRawMetricBytes(t *testing.T) {
	m := testRawMetric()
	require.Equal(t, m.data, m.Bytes())
}

func TestRawMetricReset(t *testing.T) {
	metrics := []aggregated.Metric{
		{ID: metric.ID("foo"), Timestamp: testMetric.Timestamp, Value: 1.0},
		{ID: metric.ID("bar"), Timestamp: testMetric.Timestamp, Value: 2.3},
		{ID: metric.ID("baz"), Timestamp: testMetric.Timestamp, Value: 4234.234},
	}
	rawMetric := NewRawMetric(nil)
	for i := 0; i < len(metrics); i++ {
		rawMetric.Reset(toRawMetric(t, metrics[i]).Bytes())
		decoded, err := rawMetric.Metric()
		require.NoError(t, err)
		require.Equal(t, metrics[i], decoded)
	}
}

func TestRawMetricRoundtripStress(t *testing.T) {
	metrics := []aggregated.Metric{
		{ID: metric.ID("foo"), Timestamp: testMetric.Timestamp, Value: 1.0},
		{ID: metric.ID("bar"), Timestamp: testMetric.Timestamp, Value: 2.3},
		{ID: metric.ID("baz"), Timestamp: testMetric.Timestamp, Value: 4234.234},
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
