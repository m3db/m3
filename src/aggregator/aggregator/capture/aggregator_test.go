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

package capture

import (
	"fmt"
	"testing"

	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"

	"github.com/stretchr/testify/require"
)

var (
	testCounter = unaggregated.MetricUnion{
		Type:       unaggregated.CounterType,
		ID:         id.RawID("testCounter"),
		CounterVal: 1234,
	}
	testBatchTimer = unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            id.RawID("testCounter"),
		BatchTimerVal: []float64{1.0, 3.5, 2.2, 6.5, 4.8},
	}
	testGauge = unaggregated.MetricUnion{
		Type:     unaggregated.GaugeType,
		ID:       id.RawID("testCounter"),
		GaugeVal: 123.456,
	}
	testInvalid = unaggregated.MetricUnion{
		Type: unaggregated.UnknownType,
		ID:   id.RawID("invalid"),
	}
	testDefaultPoliciesList = policy.DefaultPoliciesList
)

func TestAggregator(t *testing.T) {
	agg := NewAggregator()

	// Adding an invalid metric should result in an error.
	policies := testDefaultPoliciesList
	require.Error(t, agg.AddMetricWithPoliciesList(testInvalid, policies))

	// Add valid metrics with policies.
	var expected SnapshotResult
	for _, mu := range []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge} {
		switch mu.Type {
		case unaggregated.CounterType:
			expected.CountersWithPoliciesList = append(
				expected.CountersWithPoliciesList,
				unaggregated.CounterWithPoliciesList{
					Counter:      mu.Counter(),
					PoliciesList: policies,
				})
		case unaggregated.BatchTimerType:
			expected.BatchTimersWithPoliciesList = append(
				expected.BatchTimersWithPoliciesList,
				unaggregated.BatchTimerWithPoliciesList{
					BatchTimer:   mu.BatchTimer(),
					PoliciesList: policies,
				})
		case unaggregated.GaugeType:
			expected.GaugesWithPoliciesList = append(
				expected.GaugesWithPoliciesList,
				unaggregated.GaugeWithPoliciesList{
					Gauge:        mu.Gauge(),
					PoliciesList: policies,
				})
		default:
			require.Fail(t, fmt.Sprintf("unknown metric type %v", mu.Type))
		}
		require.NoError(t, agg.AddMetricWithPoliciesList(mu, policies))
	}

	require.Equal(t, 3, agg.NumMetricsAdded())

	res := agg.Snapshot()
	require.Equal(t, expected, res)
}
