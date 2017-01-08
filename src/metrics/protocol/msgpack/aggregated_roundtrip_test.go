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

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testMetric = aggregated.Metric{
		ID:        metric.ID("foo"),
		Timestamp: time.Now(),
		Value:     123.45,
	}
	testChunkedMetric = aggregated.ChunkedMetric{
		ChunkedID: metric.ChunkedID{
			Prefix: []byte("foo."),
			Data:   []byte("bar"),
			Suffix: []byte(".baz"),
		},
		Timestamp: time.Now(),
		Value:     123.45,
	}
	testMetric2 = aggregated.Metric{
		ID:        metric.ID("bar"),
		Timestamp: time.Now(),
		Value:     678.90,
	}
	testPolicy = policy.Policy{
		Resolution: policy.Resolution{Window: time.Second, Precision: xtime.Second},
		Retention:  policy.Retention(time.Hour),
	}
)

type metricWithPolicy struct {
	metric interface{}
	policy policy.Policy
}

func testAggregatedEncoder(t *testing.T) AggregatedEncoder {
	return NewAggregatedEncoder(newBufferedEncoder())
}

func testAggregatedIterator(t *testing.T, reader io.Reader) AggregatedIterator {
	return NewAggregatedIterator(reader, NewAggregatedIteratorOptions())
}

func testAggregatedEncode(t *testing.T, encoder AggregatedEncoder, m interface{}, p policy.Policy) error {
	switch m := m.(type) {
	case aggregated.Metric:
		return encoder.EncodeMetricWithPolicy(aggregated.MetricWithPolicy{
			Metric: m,
			Policy: p,
		})
	case aggregated.ChunkedMetric:
		return encoder.EncodeChunkedMetricWithPolicy(aggregated.ChunkedMetricWithPolicy{
			ChunkedMetric: m,
			Policy:        p,
		})
	case aggregated.RawMetric:
		return encoder.EncodeRawMetricWithPolicy(aggregated.RawMetricWithPolicy{
			RawMetric: m,
			Policy:    p,
		})
	default:
		return fmt.Errorf("unrecognized metric type: %T", m)
	}
}

func toRawMetric(t *testing.T, m interface{}) aggregated.RawMetric {
	encoder := NewAggregatedEncoder(newBufferedEncoder()).(*aggregatedEncoder)
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
	return NewRawMetric(data)
}

func validateAggregatedRoundtrip(t *testing.T, inputs ...metricWithPolicy) {
	encoder := testAggregatedEncoder(t)
	it := testAggregatedIterator(t, nil)
	validateAggregatedRoundtripWithEncoderAndIterator(t, encoder, it, inputs...)
}

func validateAggregatedRoundtripWithEncoderAndIterator(
	t *testing.T,
	encoder AggregatedEncoder,
	it AggregatedIterator,
	inputs ...metricWithPolicy,
) {
	var (
		expected []metricWithPolicy
		results  []metricWithPolicy
	)

	// Encode the batch of metrics
	encoder.Reset(newBufferedEncoder())
	for _, input := range inputs {
		switch inputMetric := input.metric.(type) {
		case aggregated.Metric:
			expected = append(expected, metricWithPolicy{
				metric: inputMetric,
				policy: input.policy,
			})
			require.NoError(t, testAggregatedEncode(t, encoder, inputMetric, input.policy))
		case aggregated.ChunkedMetric:
			var id metric.ID
			id = append(id, inputMetric.ChunkedID.Prefix...)
			id = append(id, inputMetric.ChunkedID.Data...)
			id = append(id, inputMetric.ChunkedID.Suffix...)
			expected = append(expected, metricWithPolicy{
				metric: aggregated.Metric{
					ID:        id,
					Timestamp: inputMetric.Timestamp,
					Value:     inputMetric.Value,
				},
				policy: input.policy,
			})
			require.NoError(t, testAggregatedEncode(t, encoder, inputMetric, input.policy))
		case aggregated.RawMetric:
			m, err := inputMetric.Metric()
			require.NoError(t, err)
			expected = append(expected, metricWithPolicy{
				metric: m,
				policy: input.policy,
			})
			require.NoError(t, testAggregatedEncode(t, encoder, inputMetric, input.policy))
		default:
			require.Fail(t, "unrecognized input type %T", inputMetric)
		}
	}

	// Decode the batch of metrics
	encodedBytes := bytes.NewBuffer(encoder.Encoder().Bytes())
	it.Reset(encodedBytes)
	for it.Next() {
		metric, p := it.Value()
		m, err := metric.Metric()
		require.NoError(t, err)
		results = append(results, metricWithPolicy{
			metric: m,
			policy: p,
		})
	}

	// Assert the results match expectations
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, expected, results)
}

func TestAggregatedEncodeDecodeMetricWithPolicy(t *testing.T) {
	validateAggregatedRoundtrip(t, metricWithPolicy{
		metric: testMetric,
		policy: testPolicy,
	})
}

func TestAggregatedEncodeDecodeChunkedMetricWithPolicy(t *testing.T) {
	validateAggregatedRoundtrip(t, metricWithPolicy{
		metric: testChunkedMetric,
		policy: testPolicy,
	})
}

func TestAggregatedEncodeDecodeRawMetricWithPolicy(t *testing.T) {
	validateAggregatedRoundtrip(t, metricWithPolicy{
		metric: toRawMetric(t, testMetric),
		policy: testPolicy,
	})
}

func TestAggregatedEncodeDecodeStress(t *testing.T) {
	var (
		numIter    = 10
		numMetrics = 10000
		encoder    = testAggregatedEncoder(t)
		iterator   = testAggregatedIterator(t, nil)
	)

	for i := 0; i < numIter; i++ {
		var inputs []metricWithPolicy
		for j := 0; j < numMetrics; j++ {
			if j%3 == 0 {
				inputs = append(inputs, metricWithPolicy{
					metric: testMetric,
					policy: testPolicy,
				})
			} else if j%3 == 1 {
				inputs = append(inputs, metricWithPolicy{
					metric: testChunkedMetric,
					policy: testPolicy,
				})
			} else {
				inputs = append(inputs, metricWithPolicy{
					metric: toRawMetric(t, testMetric2),
					policy: testPolicy,
				})
			}
		}
		validateAggregatedRoundtripWithEncoderAndIterator(t, encoder, iterator, inputs...)
	}
}
