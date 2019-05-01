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
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var (
	testCounter = unaggregated.MetricUnion{
		Type:       metric.CounterType,
		ID:         id.RawID("testCounter"),
		CounterVal: 1234,
	}
	testBatchTimer = unaggregated.MetricUnion{
		Type:          metric.TimerType,
		ID:            id.RawID("testBatch"),
		BatchTimerVal: []float64{1.0, 3.5, 2.2, 6.5, 4.8},
	}
	testGauge = unaggregated.MetricUnion{
		Type:     metric.GaugeType,
		ID:       id.RawID("testGauge"),
		GaugeVal: 123.456,
	}
	testTimed = aggregated.Metric{
		Type:      metric.CounterType,
		ID:        []byte("testTimed"),
		TimeNanos: 12345,
		Value:     -13.5,
	}
	testForwarded = aggregated.ForwardedMetric{
		Type:      metric.CounterType,
		ID:        []byte("testForwarded"),
		TimeNanos: 12345,
		Values:    []float64{908, -13.5},
	}
	testPassThrough = aggregated.Metric{
		Type:      metric.GaugeType,
		ID:        []byte("testPassThrough"),
		TimeNanos: 12345,
		Value:     -13.5,
	}
	testInvalid = unaggregated.MetricUnion{
		Type: metric.UnknownType,
		ID:   id.RawID("invalid"),
	}
	testDefaultMetadatas = metadata.DefaultStagedMetadatas
	testTimedMetadata    = metadata.TimedMetadata{
		AggregationID: aggregation.DefaultID,
		StoragePolicy: policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
	}
	testPassThroughMetadata = metadata.TimedMetadata{
		AggregationID: aggregation.DefaultID,
		StoragePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
	}
	testForwardMetadata = metadata.ForwardMetadata{
		AggregationID: aggregation.DefaultID,
		StoragePolicy: policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
		Pipeline: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("foo"),
					AggregationID: aggregation.MustCompressTypes(aggregation.Count),
				},
			},
		}),
		SourceID:          1234,
		NumForwardedTimes: 3,
	}
)

func TestAggregator(t *testing.T) {
	agg := NewAggregator()

	// Adding an invalid metric should result in an error.
	metadatas := testDefaultMetadatas
	require.Error(t, agg.AddUntimed(testInvalid, metadatas))

	// Add valid untimed metrics with policies.
	var expected SnapshotResult
	for _, mu := range []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge} {
		switch mu.Type {
		case metric.CounterType:
			expected.CountersWithMetadatas = append(
				expected.CountersWithMetadatas,
				unaggregated.CounterWithMetadatas{
					Counter:         mu.Counter(),
					StagedMetadatas: metadatas,
				})
		case metric.TimerType:
			expected.BatchTimersWithMetadatas = append(
				expected.BatchTimersWithMetadatas,
				unaggregated.BatchTimerWithMetadatas{
					BatchTimer:      mu.BatchTimer(),
					StagedMetadatas: metadatas,
				})
		case metric.GaugeType:
			expected.GaugesWithMetadatas = append(
				expected.GaugesWithMetadatas,
				unaggregated.GaugeWithMetadatas{
					Gauge:           mu.Gauge(),
					StagedMetadatas: metadatas,
				})
		default:
			require.Fail(t, fmt.Sprintf("unknown metric type %v", mu.Type))
		}
		require.NoError(t, agg.AddUntimed(mu, metadatas))
	}

	// Add valid timed metrics with metadata.
	expected.TimedMetricWithMetadata = append(
		expected.TimedMetricWithMetadata,
		aggregated.TimedMetricWithMetadata{
			Metric:        testTimed,
			TimedMetadata: testTimedMetadata,
		},
	)
	require.NoError(t, agg.AddTimed(testTimed, testTimedMetadata))

	require.Equal(t, 4, agg.NumMetricsAdded())

	// Add valid forwarded metrics with metadata.
	expected.ForwardedMetricsWithMetadata = append(
		expected.ForwardedMetricsWithMetadata,
		aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: testForwarded,
			ForwardMetadata: testForwardMetadata,
		},
	)
	require.NoError(t, agg.AddForwarded(testForwarded, testForwardMetadata))

	require.Equal(t, 5, agg.NumMetricsAdded())

	// add valid pass-through metrics with metadata
	expected.PassThroughMetricWithMetadata = append(
		expected.PassThroughMetricWithMetadata,
		aggregated.TimedMetricWithMetadata{
			Metric:        testPassThrough,
			TimedMetadata: testPassThroughMetadata,
		},
	)
	require.NoError(t, agg.AddPassThrough(testPassThrough, testPassThroughMetadata))
	require.Equal(t, 6, agg.NumMetricsAdded())

	res := agg.Snapshot()
	require.Equal(t, expected, res)
}
