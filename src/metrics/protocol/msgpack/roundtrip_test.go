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

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testCounter = unaggregated.MetricUnion{
		Type:       unaggregated.CounterType,
		ID:         []byte("foo"),
		CounterVal: 1234,
	}

	testBatchTimer = unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            []byte("foo"),
		BatchTimerVal: []float64{222.22, 345.67, 901.23345},
	}

	testGauge = unaggregated.MetricUnion{
		Type:     unaggregated.GaugeType,
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
		// Retain this metric at 100 second resolution for 6 hours,
		// then custom resolution for 2 days, then 1 minute for 25 days
		{
			metric: testBatchTimer,
			versionedPolicies: policy.VersionedPolicies{
				Version: 2,
				Policies: []policy.Policy{
					{
						Resolution: policy.Resolution{Window: time.Duration(100), Precision: xtime.Second},
						Retention:  policy.Retention(6 * time.Hour),
					},
					{
						Resolution: policy.Resolution{Window: time.Duration(1), Precision: xtime.Unit(100)},
						Retention:  policy.Retention(2 * 24 * time.Hour),
					},
					{
						Resolution: policy.Resolution{Window: time.Duration(1), Precision: xtime.Minute},
						Retention:  policy.Retention(25 * 24 * time.Hour),
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
	metric            unaggregated.MetricUnion
	versionedPolicies policy.VersionedPolicies
}

func testUnaggregatedEncoder(t *testing.T) UnaggregatedEncoder {
	encoder, err := NewUnaggregatedEncoder(newBufferedEncoder())
	require.NoError(t, err)
	return encoder
}

func testUnaggregatedIterator(t *testing.T, reader io.Reader) UnaggregatedIterator {
	opts := NewUnaggregatedIteratorOptions()
	iterator, err := NewUnaggregatedIterator(reader, opts)
	require.NoError(t, err)
	return iterator
}

func testEncode(t *testing.T, encoder UnaggregatedEncoder, m *unaggregated.MetricUnion, p policy.VersionedPolicies) error {
	switch m.Type {
	case unaggregated.CounterType:
		return encoder.EncodeCounterWithPolicies(m.Counter(), p)
	case unaggregated.BatchTimerType:
		return encoder.EncodeBatchTimerWithPolicies(m.BatchTimer(), p)
	case unaggregated.GaugeType:
		return encoder.EncodeGaugeWithPolicies(m.Gauge(), p)
	default:
		return fmt.Errorf("unrecognized metric type %v", m.Type)
	}
}

func compareMetric(t *testing.T, expected unaggregated.MetricUnion, actual unaggregated.MetricUnion) {
	require.Equal(t, expected.Type, actual.Type)
	switch expected.Type {
	case unaggregated.CounterType:
		require.Equal(t, expected.Counter(), actual.Counter())
	case unaggregated.BatchTimerType:
		require.Equal(t, expected.BatchTimer(), actual.BatchTimer())
	case unaggregated.GaugeType:
		require.Equal(t, expected.Gauge(), actual.Gauge())
	default:
		require.Fail(t, fmt.Sprintf("unrecognized metric type %v", expected.Type))
	}
}

func validateRoundtrip(t *testing.T, inputs ...metricWithPolicies) {
	encoder := testUnaggregatedEncoder(t)
	it := testUnaggregatedIterator(t, nil)
	validateRoundtripWithEncoderAndIterator(t, encoder, it, inputs...)
}

func validateRoundtripWithEncoderAndIterator(
	t *testing.T,
	encoder UnaggregatedEncoder, it UnaggregatedIterator, inputs ...metricWithPolicies,
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
		metric: unaggregated.MetricUnion{
			Type:       unaggregated.CounterType,
			ID:         []byte("foo"),
			CounterVal: 1234,
		},
		versionedPolicies: policy.DefaultVersionedPolicies,
	})
}

func TestEncodeDecodeBatchTimerWithDefaultPolicies(t *testing.T) {
	validateRoundtrip(t, metricWithPolicies{
		metric: unaggregated.MetricUnion{
			Type:          unaggregated.BatchTimerType,
			ID:            []byte("foo"),
			BatchTimerVal: []float64{222.22, 345.67, 901.23345},
		},
		versionedPolicies: policy.DefaultVersionedPolicies,
	})
}

func TestEncodeDecodeGaugeWithDefaultPolicies(t *testing.T) {
	validateRoundtrip(t, metricWithPolicies{
		metric: unaggregated.MetricUnion{
			Type:     unaggregated.GaugeType,
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
	allMetrics := []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge}
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

	encoder := testUnaggregatedEncoder(t)
	iterator := testUnaggregatedIterator(t, nil)
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
