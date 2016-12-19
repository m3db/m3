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
	"math/rand"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testCounter = metric.OneOf{
		Type:       metric.CounterType,
		ID:         []byte("foo"),
		CounterVal: 1234,
	}

	testBatchTimer = metric.OneOf{
		Type:          metric.BatchTimerType,
		ID:            []byte("foo"),
		BatchTimerVal: []float64{222.22, 345.67, 901.23345},
	}

	testGauge = metric.OneOf{
		Type:     metric.GaugeType,
		ID:       []byte("foo"),
		GaugeVal: 123.456,
	}

	testInputWithAllTypesAndDefaultPolicies = []metricWithPolicies{
		{
			metric:            testCounter,
			versionedPolicies: policy.DefaultVersionedPolicies,
		},
		{
			metric:            testBatchTimer,
			versionedPolicies: policy.DefaultVersionedPolicies,
		},
		{
			metric:            testGauge,
			versionedPolicies: policy.DefaultVersionedPolicies,
		},
	}

	testInputWithAllTypesAndCustomPolicies = []metricWithPolicies{
		// Retain this metric at 1 second resolution for 1 hour
		{
			metric: testCounter,
			versionedPolicies: policy.VersionedPolicies{
				Version: 1,
				Policies: []policy.Policy{
					{
						Resolution: policy.Resolution{Window: time.Duration(1), Precision: xtime.Second},
						Retention:  policy.Retention(time.Hour),
					},
				},
			},
		},
		// Retain this metric at 10 second resolution for 6 hours,
		// then 1 minute resolution for 2 days
		{
			metric: testBatchTimer,
			versionedPolicies: policy.VersionedPolicies{
				Version: 2,
				Policies: []policy.Policy{
					{
						Resolution: policy.Resolution{Window: time.Duration(10), Precision: xtime.Second},
						Retention:  policy.Retention(6 * time.Hour),
					},
					{
						Resolution: policy.Resolution{Window: time.Duration(1), Precision: xtime.Minute},
						Retention:  policy.Retention(2 * 24 * time.Hour),
					},
				},
			},
		},
		// Retain this metric at 10 minute resolution for 45 days
		{
			metric: testGauge,
			versionedPolicies: policy.VersionedPolicies{
				Version: 2,
				Policies: []policy.Policy{
					{
						Resolution: policy.Resolution{Window: time.Duration(10), Precision: xtime.Minute},
						Retention:  policy.Retention(45 * 24 * time.Hour),
					},
				},
			},
		},
	}
)

type metricWithPolicies struct {
	metric            metric.OneOf
	versionedPolicies policy.VersionedPolicies
}

func testMultiTypedEncoder(t *testing.T) MultiTypedEncoder {
	encoder, err := NewMultiTypedEncoder(newBufferedEncoder())
	require.NoError(t, err)
	return encoder
}

func testMultiTypedIterator(t *testing.T, reader io.Reader) MultiTypedIterator {
	opts := NewMultiTypedIteratorOptions()
	iterator, err := NewMultiTypedIterator(reader, opts)
	require.NoError(t, err)
	return iterator
}

func testEncode(t *testing.T, encoder MultiTypedEncoder, m *metric.OneOf, p policy.VersionedPolicies) error {
	switch m.Type {
	case metric.CounterType:
		return encoder.EncodeCounterWithPolicies(m.Counter(), p)
	case metric.BatchTimerType:
		return encoder.EncodeBatchTimerWithPolicies(m.BatchTimer(), p)
	case metric.GaugeType:
		return encoder.EncodeGaugeWithPolicies(m.Gauge(), p)
	default:
		return fmt.Errorf("unrecognized metric type %v", m.Type)
	}
}

func compareMetric(t *testing.T, expected metric.OneOf, actual metric.OneOf) {
	require.Equal(t, expected.Type, actual.Type)
	switch expected.Type {
	case metric.CounterType:
		require.Equal(t, expected.Counter(), actual.Counter())
	case metric.BatchTimerType:
		require.Equal(t, expected.BatchTimer(), actual.BatchTimer())
	case metric.GaugeType:
		require.Equal(t, expected.Gauge(), actual.Gauge())
	default:
		require.Fail(t, fmt.Sprintf("unrecognized metric type %v", expected.Type))
	}
}

func validateRoundtrip(t *testing.T, inputs ...metricWithPolicies) {
	encoder := testMultiTypedEncoder(t)
	it := testMultiTypedIterator(t, nil)
	validateRoundtripWithEncoderAndIterator(t, encoder, it, inputs...)
}

func validateRoundtripWithEncoderAndIterator(
	t *testing.T,
	encoder MultiTypedEncoder, it MultiTypedIterator, inputs ...metricWithPolicies,
) {
	var results []metricWithPolicies

	// Encode the batch of metrics
	encoder.Reset(newBufferedEncoder())
	for _, input := range inputs {
		testEncode(t, encoder, &input.metric, input.versionedPolicies)
	}
	buffer := encoder.Encoder().Buffer

	// Decode the batch of metrics
	byteStream := bytes.NewBuffer(buffer.Bytes())
	it.Reset(byteStream)
	for it.Next() {
		m, p := it.Value()
		results = append(results, metricWithPolicies{
			metric:            *m,
			versionedPolicies: p,
		})
	}

	// Assert the results match expectations
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs), len(results))
	for i := 0; i < len(inputs); i++ {
		compareMetric(t, inputs[i].metric, results[i].metric)
		require.Equal(t, inputs[i].versionedPolicies, results[i].versionedPolicies)
	}
}

func TestEncodeDecodeCounterWithDefaultPolicies(t *testing.T) {
	validateRoundtrip(t, metricWithPolicies{
		metric: metric.OneOf{
			Type:       metric.CounterType,
			ID:         []byte("foo"),
			CounterVal: 1234,
		},
		versionedPolicies: policy.DefaultVersionedPolicies,
	})
}

func TestEncodeDecodeBatchTimerWithDefaultPolicies(t *testing.T) {
	validateRoundtrip(t, metricWithPolicies{
		metric: metric.OneOf{
			Type:          metric.BatchTimerType,
			ID:            []byte("foo"),
			BatchTimerVal: []float64{222.22, 345.67, 901.23345},
		},
		versionedPolicies: policy.DefaultVersionedPolicies,
	})
}

func TestEncodeDecodeGaugeWithDefaultPolicies(t *testing.T) {
	validateRoundtrip(t, metricWithPolicies{
		metric: metric.OneOf{
			Type:     metric.GaugeType,
			ID:       []byte("foo"),
			GaugeVal: 123.456,
		},
		versionedPolicies: policy.DefaultVersionedPolicies,
	})
}

func TestEncodeDecodeAllTypesWithDefaultPolicies(t *testing.T) {
	validateRoundtrip(t, testInputWithAllTypesAndDefaultPolicies...)
}

func TestEncodeDecodeAllTypesWithCustomPolicies(t *testing.T) {
	validateRoundtrip(t, testInputWithAllTypesAndCustomPolicies...)
}

func TestEncodeDecodeStress(t *testing.T) {
	numIter := 10
	numMetrics := 10000
	allMetrics := []metric.OneOf{testCounter, testBatchTimer, testGauge}
	allPolicies := []policy.VersionedPolicies{
		policy.DefaultVersionedPolicies,
		{
			Version: 2,
			Policies: []policy.Policy{
				{
					Resolution: policy.Resolution{Window: time.Duration(10), Precision: xtime.Second},
					Retention:  policy.Retention(6 * time.Hour),
				},
				{
					Resolution: policy.Resolution{Window: time.Duration(1), Precision: xtime.Minute},
					Retention:  policy.Retention(2 * 24 * time.Hour),
				},
			},
		},
	}

	encoder := testMultiTypedEncoder(t)
	iterator := testMultiTypedIterator(t, nil)
	for i := 0; i < numIter; i++ {
		var inputs []metricWithPolicies
		for j := 0; j < numMetrics; j++ {
			m := allMetrics[rand.Int63n(int64(len(allMetrics)))]
			p := allPolicies[rand.Int63n(int64(len(allPolicies)))]
			inputs = append(inputs, metricWithPolicies{metric: m, versionedPolicies: p})
		}
		validateRoundtripWithEncoderAndIterator(t, encoder, iterator, inputs...)
	}
}
