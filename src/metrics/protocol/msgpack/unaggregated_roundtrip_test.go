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

	testDefaultStagedPoliciesList = policy.DefaultPoliciesList

	testSingleCustomStagedPoliciesList = policy.PoliciesList{
		policy.NewStagedPolicies(
			time.Now().UnixNano(),
			false,
			[]policy.Policy{
				policy.NewPolicy(20*time.Second, xtime.Second, 6*time.Hour),
				policy.NewPolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
				policy.NewPolicy(10*time.Minute, xtime.Minute, 25*24*time.Hour),
			},
		),
	}

	testMultiCustomStagedPoliciesList = policy.PoliciesList{
		policy.NewStagedPolicies(
			time.Now().UnixNano(),
			false,
			[]policy.Policy{
				policy.NewPolicy(20*time.Second, xtime.Second, 6*time.Hour),
				policy.NewPolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
				policy.NewPolicy(10*time.Minute, xtime.Minute, 25*24*time.Hour),
			},
		),
		policy.NewStagedPolicies(
			time.Now().Add(time.Minute).UnixNano(),
			true,
			[]policy.Policy{
				policy.NewPolicy(time.Second, xtime.Second, time.Hour),
			},
		),
	}

	testStagedPoliciesWithInvalidTimeUnit = policy.PoliciesList{
		policy.NewStagedPolicies(
			time.Now().UnixNano(),
			true,
			[]policy.Policy{
				policy.NewPolicy(time.Second, xtime.Unit(100), time.Hour),
			},
		),
	}

	testInputWithAllTypesAndDefaultPoliciesList = []metricWithPoliciesList{
		{
			metric:       testCounter,
			policiesList: testDefaultStagedPoliciesList,
		},
		{
			metric:       testBatchTimer,
			policiesList: testDefaultStagedPoliciesList,
		},
		{
			metric:       testGauge,
			policiesList: testDefaultStagedPoliciesList,
		},
	}

	testInputWithAllTypesAndSingleCustomPoliciesList = []metricWithPoliciesList{
		// Retain this metric at 20 second resolution for 6 hours,
		// then 1 minute for 2 days, then 10 minutes for 25 days.
		{
			metric: testBatchTimer,
			policiesList: policy.PoliciesList{
				policy.NewStagedPolicies(
					time.Now().UnixNano(),
					false,
					[]policy.Policy{
						policy.NewPolicy(20*time.Second, xtime.Second, 6*time.Hour),
						policy.NewPolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
						policy.NewPolicy(10*time.Minute, xtime.Minute, 25*24*time.Hour),
					},
				),
			},
		},
		// Retain this metric at 1 second resolution for 1 hour.
		{
			metric: testCounter,
			policiesList: policy.PoliciesList{
				policy.NewStagedPolicies(
					time.Now().UnixNano(),
					true,
					[]policy.Policy{
						policy.NewPolicy(time.Second, xtime.Second, time.Hour),
					},
				),
			},
		},
		// Retain this metric at 10 minute resolution for 45 days.
		{
			metric: testGauge,
			policiesList: policy.PoliciesList{
				policy.NewStagedPolicies(
					time.Now().UnixNano(),
					false,
					[]policy.Policy{
						policy.NewPolicy(10*time.Minute, xtime.Minute, 45*24*time.Hour),
					},
				),
			},
		},
	}

	testInputWithAllTypesAndMultiCustomPoliciesList = []metricWithPoliciesList{
		{
			metric: testBatchTimer,
			policiesList: policy.PoliciesList{
				policy.NewStagedPolicies(
					time.Now().UnixNano(),
					false,
					[]policy.Policy{
						policy.NewPolicy(20*time.Second, xtime.Second, 6*time.Hour),
						policy.NewPolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
						policy.NewPolicy(10*time.Minute, xtime.Minute, 25*24*time.Hour),
					},
				),
				policy.NewStagedPolicies(
					time.Now().Add(time.Minute).UnixNano(),
					true,
					[]policy.Policy{
						policy.NewPolicy(time.Second, xtime.Second, time.Hour),
					},
				),
			},
		},
		{
			metric: testCounter,
			policiesList: policy.PoliciesList{
				policy.NewStagedPolicies(
					time.Now().UnixNano(),
					true,
					[]policy.Policy{
						policy.NewPolicy(time.Second, xtime.Second, time.Hour),
					},
				),
				policy.NewStagedPolicies(
					time.Now().Add(time.Hour).UnixNano(),
					false,
					[]policy.Policy{
						policy.NewPolicy(10*time.Minute, xtime.Minute, 45*24*time.Hour),
					},
				),
			},
		},
		{
			metric: testGauge,
			policiesList: policy.PoliciesList{
				policy.NewStagedPolicies(
					time.Now().UnixNano(),
					false,
					[]policy.Policy{
						policy.NewPolicy(10*time.Minute, xtime.Minute, 45*24*time.Hour),
					},
				),
				policy.NewStagedPolicies(
					time.Now().Add(time.Nanosecond).UnixNano(),
					false,
					[]policy.Policy{
						policy.NewPolicy(5*time.Minute, xtime.Minute, 36*time.Hour),
					},
				),
			},
		},
	}
)

func TestUnaggregatedEncodeDecodeCounterWithDefaultPoliciesList(t *testing.T) {
	validateUnaggregatedRoundtrip(t, metricWithPoliciesList{
		metric:       testCounter,
		policiesList: testDefaultStagedPoliciesList,
	})
}

func TestUnaggregatedEncodeDecodeBatchTimerWithDefaultPoliciesList(t *testing.T) {
	validateUnaggregatedRoundtrip(t, metricWithPoliciesList{
		metric:       testBatchTimer,
		policiesList: testDefaultStagedPoliciesList,
	})
}

func TestUnaggregatedEncodeDecodeGaugeWithDefaultPoliciesList(t *testing.T) {
	validateUnaggregatedRoundtrip(t, metricWithPoliciesList{
		metric:       testGauge,
		policiesList: testDefaultStagedPoliciesList,
	})
}

func TestUnaggregatedEncodeDecodeAllTypesWithDefaultPoliciesList(t *testing.T) {
	validateUnaggregatedRoundtrip(t, testInputWithAllTypesAndDefaultPoliciesList...)
}

func TestUnaggregatedEncodeDecodeAllTypesWithSingleCustomPoliciesList(t *testing.T) {
	validateUnaggregatedRoundtrip(t, testInputWithAllTypesAndSingleCustomPoliciesList...)
}

func TestUnaggregatedEncodeDecodeAllTypesWithMultiCustomPoliciesList(t *testing.T) {
	validateUnaggregatedRoundtrip(t, testInputWithAllTypesAndMultiCustomPoliciesList...)
}

func TestUnaggregatedEncodeDecodeStress(t *testing.T) {
	numIter := 10
	numMetrics := 10000
	allMetrics := []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge}
	allPolicies := []policy.PoliciesList{
		testDefaultStagedPoliciesList,
		policy.PoliciesList{
			policy.NewStagedPolicies(
				time.Now().UnixNano(),
				false,
				[]policy.Policy{
					policy.NewPolicy(time.Second, xtime.Second, 6*time.Hour),
					policy.NewPolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
				},
			),
		},
		policy.PoliciesList{
			policy.NewStagedPolicies(
				time.Now().UnixNano(),
				false,
				[]policy.Policy{
					policy.NewPolicy(20*time.Second, xtime.Second, 6*time.Hour),
					policy.NewPolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
					policy.NewPolicy(10*time.Minute, xtime.Minute, 25*24*time.Hour),
				},
			),
			policy.NewStagedPolicies(
				time.Now().Add(time.Minute).UnixNano(),
				true,
				[]policy.Policy{
					policy.NewPolicy(time.Second, xtime.Second, time.Hour),
				},
			),
			policy.NewStagedPolicies(
				time.Now().Add(time.Minute).UnixNano(),
				false,
				[]policy.Policy{},
			),
		},
	}

	encoder := testUnaggregatedEncoder(t)
	iterator := testUnaggregatedIterator(t, nil)
	for i := 0; i < numIter; i++ {
		var inputs []metricWithPoliciesList
		for j := 0; j < numMetrics; j++ {
			m := allMetrics[rand.Int63n(int64(len(allMetrics)))]
			p := allPolicies[rand.Int63n(int64(len(allPolicies)))]
			inputs = append(inputs, metricWithPoliciesList{metric: m, policiesList: p})
		}
		validateUnaggregatedRoundtripWithEncoderAndIterator(t, encoder, iterator, inputs...)
	}
}

type metricWithPoliciesList struct {
	metric       unaggregated.MetricUnion
	policiesList policy.PoliciesList
}

func testCapturingBaseEncoder(encoder encoderBase) *[]interface{} {
	baseEncoder := encoder.(*baseEncoder)

	var result []interface{}
	baseEncoder.encodeVarintFn = func(value int64) {
		result = append(result, value)
	}
	baseEncoder.encodeBoolFn = func(value bool) {
		result = append(result, value)
	}
	baseEncoder.encodeFloat64Fn = func(value float64) {
		result = append(result, value)
	}
	baseEncoder.encodeBytesFn = func(value []byte) {
		result = append(result, value)
	}
	baseEncoder.encodeBytesLenFn = func(value int) {
		result = append(result, value)
	}
	baseEncoder.encodeArrayLenFn = func(value int) {
		result = append(result, value)
	}

	return &result
}

func testUnaggregatedEncoder(t *testing.T) UnaggregatedEncoder {
	return NewUnaggregatedEncoder(NewBufferedEncoder())
}

func testUnaggregatedIterator(t *testing.T, reader io.Reader) UnaggregatedIterator {
	opts := NewUnaggregatedIteratorOptions()
	return NewUnaggregatedIterator(reader, opts)
}

func testUnaggregatedEncode(t *testing.T, encoder UnaggregatedEncoder, m unaggregated.MetricUnion, pl policy.PoliciesList) error {
	switch m.Type {
	case unaggregated.CounterType:
		return encoder.EncodeCounterWithPoliciesList(unaggregated.CounterWithPoliciesList{
			Counter:      m.Counter(),
			PoliciesList: pl,
		})
	case unaggregated.BatchTimerType:
		return encoder.EncodeBatchTimerWithPoliciesList(unaggregated.BatchTimerWithPoliciesList{
			BatchTimer:   m.BatchTimer(),
			PoliciesList: pl,
		})
	case unaggregated.GaugeType:
		return encoder.EncodeGaugeWithPoliciesList(unaggregated.GaugeWithPoliciesList{
			Gauge:        m.Gauge(),
			PoliciesList: pl,
		})
	default:
		return fmt.Errorf("unrecognized metric type %v", m.Type)
	}
}

func compareUnaggregatedMetric(t *testing.T, expected unaggregated.MetricUnion, actual unaggregated.MetricUnion) {
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

func comparedPoliciesList(t *testing.T, expected policy.PoliciesList, actual policy.PoliciesList) {
	require.Equal(t, len(expected), len(actual))
	for i := 0; i < len(expected); i++ {
		require.Equal(t, expected[i].CutoverNanos, actual[i].CutoverNanos)
		require.Equal(t, expected[i].Tombstoned, actual[i].Tombstoned)
		expectedPolicies, expectedIsDefault := expected[i].Policies()
		actualPolicies, actualIsDefault := actual[i].Policies()
		require.Equal(t, expectedIsDefault, actualIsDefault)
		require.Equal(t, expectedPolicies, actualPolicies)
	}
}

func validateUnaggregatedRoundtrip(t *testing.T, inputs ...metricWithPoliciesList) {
	encoder := testUnaggregatedEncoder(t)
	it := testUnaggregatedIterator(t, nil)
	validateUnaggregatedRoundtripWithEncoderAndIterator(t, encoder, it, inputs...)
}

func validateUnaggregatedRoundtripWithEncoderAndIterator(
	t *testing.T,
	encoder UnaggregatedEncoder,
	it UnaggregatedIterator,
	inputs ...metricWithPoliciesList,
) {
	var results []metricWithPoliciesList

	// Encode the batch of metrics.
	encoder.Reset(NewBufferedEncoder())
	for _, input := range inputs {
		testUnaggregatedEncode(t, encoder, input.metric, input.policiesList)
	}

	// Decode the batch of metrics.
	byteStream := bytes.NewBuffer(encoder.Encoder().Bytes())
	it.Reset(byteStream)
	for it.Next() {
		m, pl := it.Value()

		// Make a copy of cached policies list because it becomes invalid
		// on the next Next() call.
		pl = toPoliciesList(t, pl)

		results = append(results, metricWithPoliciesList{
			metric:       m,
			policiesList: pl,
		})
	}

	// Assert the results match expectations.
	require.Equal(t, io.EOF, it.Err())
	validateMetricsWithPoliciesList(t, inputs, results)
}

func validateMetricsWithPoliciesList(t *testing.T, inputs, results []metricWithPoliciesList) {
	require.Equal(t, len(inputs), len(results))
	for i := 0; i < len(inputs); i++ {
		compareUnaggregatedMetric(t, inputs[i].metric, results[i].metric)
		comparedPoliciesList(t, inputs[i].policiesList, results[i].policiesList)
	}
}

func toStagedPolicies(t *testing.T, p policy.StagedPolicies) policy.StagedPolicies {
	srcPolicies, _ := p.Policies()
	destPolicies := make([]policy.Policy, len(srcPolicies))
	for i, policy := range srcPolicies {
		destPolicies[i] = policy
	}
	return policy.NewStagedPolicies(p.CutoverNanos, p.Tombstoned, destPolicies)
}

func toPoliciesList(t *testing.T, pl policy.PoliciesList) policy.PoliciesList {
	if pl.IsDefault() {
		return policy.DefaultPoliciesList
	}
	policiesList := make(policy.PoliciesList, 0, len(pl))
	for i := 0; i < len(pl); i++ {
		policiesList = append(policiesList, toStagedPolicies(t, pl[i]))
	}
	return policiesList
}
