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

package mock

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"

	"github.com/stretchr/testify/require"
)

var (
	testCounter = unaggregated.MetricUnion{
		Type:       unaggregated.CounterType,
		ID:         metric.ID("testCounter"),
		CounterVal: 1234,
	}
	testBatchTimer = unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            metric.ID("testCounter"),
		BatchTimerVal: []float64{1.0, 3.5, 2.2, 6.5, 4.8},
	}
	testGauge = unaggregated.MetricUnion{
		Type:     unaggregated.GaugeType,
		ID:       metric.ID("testCounter"),
		GaugeVal: 123.456,
	}
	testInvalid = unaggregated.MetricUnion{
		Type: unaggregated.UnknownType,
		ID:   metric.ID("invalid"),
	}
	testDefaultVersionedPolicies = policy.DefaultVersionedPolicies(
		1,
		time.Now(),
	)
)

func TestMockAggregator(t *testing.T) {
	agg := NewAggregator()

	// Adding an invalid metric should result in an error
	policies := testDefaultVersionedPolicies
	require.Error(t, agg.AddMetricWithPolicies(testInvalid, policies))

	// Add valid metrics with policies
	var expected SnapshotResult
	for _, mu := range []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge} {
		switch mu.Type {
		case unaggregated.CounterType:
			expected.CountersWithPolicies = append(
				expected.CountersWithPolicies,
				unaggregated.CounterWithPolicies{
					Counter:           mu.Counter(),
					VersionedPolicies: policies,
				})
		case unaggregated.BatchTimerType:
			expected.BatchTimersWithPolicies = append(
				expected.BatchTimersWithPolicies,
				unaggregated.BatchTimerWithPolicies{
					BatchTimer:        mu.BatchTimer(),
					VersionedPolicies: policies,
				})
		case unaggregated.GaugeType:
			expected.GaugesWithPolicies = append(
				expected.GaugesWithPolicies,
				unaggregated.GaugeWithPolicies{
					Gauge:             mu.Gauge(),
					VersionedPolicies: policies,
				})
		default:
			require.Fail(t, fmt.Sprintf("unknown metric type %v", mu.Type))
		}
		require.NoError(t, agg.AddMetricWithPolicies(mu, policies))
	}

	require.Equal(t, 3, agg.NumMetricsAdded())

	res := agg.Snapshot()
	require.Equal(t, expected, res)
}
