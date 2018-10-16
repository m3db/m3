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
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testMetric = aggregated.Metric{
		ID:        id.RawID("foo"),
		TimeNanos: time.Now().UnixNano(),
		Value:     123.45,
	}
	testChunkedMetric = aggregated.ChunkedMetric{
		ChunkedID: id.ChunkedID{
			Prefix: []byte("foo."),
			Data:   []byte("bar"),
			Suffix: []byte(".baz"),
		},
		TimeNanos: time.Now().UnixNano(),
		Value:     123.45,
	}
	testPolicy         = policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour)
	testEncodedAtNanos = time.Now().UnixNano()
)

type metricWithPolicyAndEncodeTime struct {
	metric         interface{}
	policy         policy.StoragePolicy
	encodedAtNanos int64
}

func testAggregatedEncoder() AggregatedEncoder {
	return NewAggregatedEncoder(NewBufferedEncoder())
}

func testAggregatedIterator(reader io.Reader) AggregatedIterator {
	return NewAggregatedIterator(reader, NewAggregatedIteratorOptions())
}

func testAggregatedEncodeMetricWithPolicy(
	encoder AggregatedEncoder,
	m interface{},
	p policy.StoragePolicy,
) error {
	switch m := m.(type) {
	case aggregated.Metric:
		return encoder.EncodeMetricWithStoragePolicy(aggregated.MetricWithStoragePolicy{
			Metric:        m,
			StoragePolicy: p,
		})
	case aggregated.ChunkedMetric:
		return encoder.EncodeChunkedMetricWithStoragePolicy(aggregated.ChunkedMetricWithStoragePolicy{
			ChunkedMetric: m,
			StoragePolicy: p,
		})
	default:
		return fmt.Errorf("unrecognized metric type: %T", m)
	}
}

func testAggregatedEncodeMetricWithPolicyAndEncodeTime(
	encoder AggregatedEncoder,
	m interface{},
	p policy.StoragePolicy,
	encodedAtNanos int64,
) error {
	switch m := m.(type) {
	case aggregated.Metric:
		input := aggregated.MetricWithStoragePolicy{
			Metric:        m,
			StoragePolicy: p,
		}
		return encoder.EncodeMetricWithStoragePolicyAndEncodeTime(input, encodedAtNanos)
	case aggregated.ChunkedMetric:
		input := aggregated.ChunkedMetricWithStoragePolicy{
			ChunkedMetric: m,
			StoragePolicy: p,
		}
		return encoder.EncodeChunkedMetricWithStoragePolicyAndEncodeTime(input, encodedAtNanos)
	default:
		return fmt.Errorf("unrecognized metric type: %T", m)
	}
}

func toRawMetric(t *testing.T, m interface{}) aggregated.RawMetric {
	encoder := NewAggregatedEncoder(NewBufferedEncoder()).(*aggregatedEncoder)
	var data []byte
	switch m := m.(type) {
	case aggregated.Metric:
		data = encoder.encodeMetricAsRaw(m)
	case aggregated.ChunkedMetric:
		data = encoder.encodeChunkedMetricAsRaw(m)
	default:
		require.Fail(t, "unrecognized metric type %T", m)
	}
	require.NoError(t, encoder.err())
	return NewRawMetric(data, 16)
}

func validateAggregatedRoundtrip(t *testing.T, inputs ...metricWithPolicyAndEncodeTime) {
	encoder := testAggregatedEncoder()
	it := testAggregatedIterator(nil)
	validateAggregatedRoundtripWithEncoderAndIterator(t, encoder, it, inputs...)
}

func validateAggregatedRoundtripWithEncoderAndIterator(
	t *testing.T,
	encoder AggregatedEncoder,
	it AggregatedIterator,
	inputs ...metricWithPolicyAndEncodeTime,
) {
	var (
		expected []metricWithPolicyAndEncodeTime
		results  []metricWithPolicyAndEncodeTime
	)

	// Encode the batch of metrics.
	encoder.Reset(NewBufferedEncoder())
	for _, input := range inputs {
		switch inputMetric := input.metric.(type) {
		case aggregated.Metric:
			expected = append(expected, metricWithPolicyAndEncodeTime{
				metric:         inputMetric,
				policy:         input.policy,
				encodedAtNanos: input.encodedAtNanos,
			})
			if input.encodedAtNanos == 0 {
				require.NoError(t, testAggregatedEncodeMetricWithPolicy(encoder, inputMetric, input.policy))
			} else {
				require.NoError(t, testAggregatedEncodeMetricWithPolicyAndEncodeTime(encoder, inputMetric, input.policy, input.encodedAtNanos))
			}
		case aggregated.ChunkedMetric:
			var id id.RawID
			id = append(id, inputMetric.ChunkedID.Prefix...)
			id = append(id, inputMetric.ChunkedID.Data...)
			id = append(id, inputMetric.ChunkedID.Suffix...)
			expected = append(expected, metricWithPolicyAndEncodeTime{
				metric: aggregated.Metric{
					ID:        id,
					TimeNanos: inputMetric.TimeNanos,
					Value:     inputMetric.Value,
				},
				policy:         input.policy,
				encodedAtNanos: input.encodedAtNanos,
			})
			if input.encodedAtNanos == 0 {
				require.NoError(t, testAggregatedEncodeMetricWithPolicy(encoder, inputMetric, input.policy))
			} else {
				require.NoError(t, testAggregatedEncodeMetricWithPolicyAndEncodeTime(encoder, inputMetric, input.policy, input.encodedAtNanos))
			}
		default:
			require.Fail(t, "unrecognized input type %T", inputMetric)
		}
	}

	// Decode the batch of metrics.
	encodedBytes := bytes.NewBuffer(encoder.Encoder().Bytes())
	it.Reset(encodedBytes)
	for it.Next() {
		metric, p, encodedAtNanos := it.Value()
		m, err := metric.Metric()
		require.NoError(t, err)
		results = append(results, metricWithPolicyAndEncodeTime{
			metric:         m,
			policy:         p,
			encodedAtNanos: encodedAtNanos,
		})
	}

	// Assert the results match expectations.
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, expected, results)
}

func TestAggregatedEncodeDecodeMetricWithPolicy(t *testing.T) {
	validateAggregatedRoundtrip(t, metricWithPolicyAndEncodeTime{
		metric: testMetric,
		policy: testPolicy,
	})
}

func TestAggregatedEncodeDecodeMetricWithPolicyAndEncodeTime(t *testing.T) {
	validateAggregatedRoundtrip(t, metricWithPolicyAndEncodeTime{
		metric:         testMetric,
		policy:         testPolicy,
		encodedAtNanos: testEncodedAtNanos,
	})
}

func TestAggregatedEncodeDecodeChunkedMetricWithPolicy(t *testing.T) {
	validateAggregatedRoundtrip(t, metricWithPolicyAndEncodeTime{
		metric: testChunkedMetric,
		policy: testPolicy,
	})
}

func TestAggregatedEncodeDecodeChunkedMetricWithPolicyAndEncodeTime(t *testing.T) {
	validateAggregatedRoundtrip(t, metricWithPolicyAndEncodeTime{
		metric:         testChunkedMetric,
		policy:         testPolicy,
		encodedAtNanos: testEncodedAtNanos,
	})
}

func TestAggregatedEncodeDecodeStress(t *testing.T) {
	var (
		numIter    = 10
		numMetrics = 10000
		encoder    = testAggregatedEncoder()
		iterator   = testAggregatedIterator(nil)
	)

	for i := 0; i < numIter; i++ {
		var inputs []metricWithPolicyAndEncodeTime
		for j := 0; j < numMetrics; j++ {
			switch j % 4 {
			case 0:
				inputs = append(inputs, metricWithPolicyAndEncodeTime{
					metric: testMetric,
					policy: testPolicy,
				})
			case 1:
				inputs = append(inputs, metricWithPolicyAndEncodeTime{
					metric:         testMetric,
					policy:         testPolicy,
					encodedAtNanos: testEncodedAtNanos,
				})
			case 2:
				inputs = append(inputs, metricWithPolicyAndEncodeTime{
					metric: testChunkedMetric,
					policy: testPolicy,
				})
			case 3:
				inputs = append(inputs, metricWithPolicyAndEncodeTime{
					metric:         testChunkedMetric,
					policy:         testPolicy,
					encodedAtNanos: testEncodedAtNanos,
				})
			}
		}
		validateAggregatedRoundtripWithEncoderAndIterator(t, encoder, iterator, inputs...)
	}
}
