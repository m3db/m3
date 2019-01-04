// Copyright (c) 2018 Uber Technologies, Inc.
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

package protobuf

import (
	"testing"

	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/policy"

	"github.com/stretchr/testify/require"
)

var (
	testAggregatedMetric1 = aggregated.MetricWithStoragePolicy{
		Metric: aggregated.Metric{
			Type:      metric.CounterType,
			ID:        []byte("foo"),
			TimeNanos: 1234,
			Value:     100,
		},
		StoragePolicy: policy.MustParseStoragePolicy("10s:2d"),
	}
	testAggregatedMetric2 = aggregated.MetricWithStoragePolicy{
		Metric: aggregated.Metric{
			Type:  metric.CounterType,
			ID:    []byte("bar"),
			Value: 200,
		},
		StoragePolicy: policy.MustParseStoragePolicy("1m:2d"),
	}
)

func TestAggregatedEncoderDecoder_RoundTrip(t *testing.T) {
	enc := NewAggregatedEncoder(nil)
	dec := NewAggregatedDecoder(nil)
	require.NoError(t, enc.Encode(testAggregatedMetric1, 2000))
	require.NoError(t, dec.Decode(enc.Buffer().Bytes()))
	require.Equal(t, int64(2000), dec.EncodeNanos())
	sp, err := dec.StoragePolicy()
	require.NoError(t, err)
	require.Equal(t, testAggregatedMetric1.StoragePolicy, sp)
	require.Equal(t, string(testAggregatedMetric1.ID), string(dec.ID()))
	require.Equal(t, testAggregatedMetric1.TimeNanos, dec.TimeNanos())
	require.Equal(t, testAggregatedMetric1.Value, dec.Value())
}

func TestAggregatedEncoderDecoder_ResetProtobuf(t *testing.T) {
	enc := NewAggregatedEncoder(nil)
	dec := NewAggregatedDecoder(nil)
	require.NoError(t, enc.Encode(testAggregatedMetric1, 2000))
	require.NoError(t, dec.Decode(enc.Buffer().Bytes()))
	require.Equal(t, int64(2000), dec.EncodeNanos())
	sp, err := dec.StoragePolicy()
	require.NoError(t, err)
	require.Equal(t, testAggregatedMetric1.StoragePolicy, sp)
	require.Equal(t, string(testAggregatedMetric1.ID), string(dec.ID()))
	require.Equal(t, testAggregatedMetric1.TimeNanos, dec.TimeNanos())
	require.Equal(t, testAggregatedMetric1.Value, dec.Value())

	// Must close the decoder to reset all the fields in the protobuf object
	// to avoid previously decoded value being applied in the next decoding.
	dec.Close()

	require.NoError(t, enc.Encode(testAggregatedMetric2, 3000))
	require.NoError(t, dec.Decode(enc.Buffer().Bytes()))
	require.Equal(t, int64(3000), dec.EncodeNanos())
	sp, err = dec.StoragePolicy()
	require.NoError(t, err)
	require.Equal(t, testAggregatedMetric2.StoragePolicy, sp)
	require.Equal(t, string(testAggregatedMetric2.ID), string(dec.ID()))
	require.Equal(t, testAggregatedMetric2.TimeNanos, dec.TimeNanos())
	require.Equal(t, testAggregatedMetric2.Value, dec.Value())
}
