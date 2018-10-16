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

package migration

import (
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testConvertCounterUnion = unaggregated.MetricUnion{
		Type:       metric.CounterType,
		ID:         []byte("testConvertCounter"),
		CounterVal: 1234,
	}
	testConvertBatchTimerUnion = unaggregated.MetricUnion{
		Type:          metric.TimerType,
		ID:            []byte("testConvertBatchTimer"),
		BatchTimerVal: []float64{222.22, 345.67, 901.23345},
	}
	testConvertGaugeUnion = unaggregated.MetricUnion{
		Type:     metric.GaugeType,
		ID:       []byte("testConvertGauge"),
		GaugeVal: 123.456,
	}
	testConvertPoliciesList = policy.PoliciesList{
		// Default staged policies.
		policy.DefaultStagedPolicies,

		// Staged policies with default policies lists.
		policy.NewStagedPolicies(
			123,
			false,
			nil,
		),

		// Single pipeline.
		policy.NewStagedPolicies(
			1234,
			false,
			[]policy.Policy{
				policy.NewPolicy(
					policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
					aggregation.DefaultID,
				),
				policy.NewPolicy(
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 6*time.Hour),
					aggregation.DefaultID,
				),
				// A duplicate policy.
				policy.NewPolicy(
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 6*time.Hour),
					aggregation.DefaultID,
				),
			},
		),

		// Multiple pipelines.
		policy.NewStagedPolicies(
			5678,
			true,
			[]policy.Policy{
				policy.NewPolicy(
					policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
					aggregation.DefaultID,
				),
				policy.NewPolicy(
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 6*time.Hour),
					aggregation.MustCompressTypes(aggregation.Count, aggregation.Last),
				),
				policy.NewPolicy(
					policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 24*time.Hour),
					aggregation.MustCompressTypes(aggregation.Count, aggregation.Last),
				),
				policy.NewPolicy(
					policy.NewStoragePolicy(time.Hour, xtime.Hour, 30*24*time.Hour),
					aggregation.MustCompressTypes(aggregation.Sum),
				),
			},
		),
	}
	testConvertCounter = unaggregated.Counter{
		ID:    []byte("testConvertCounter"),
		Value: 1234,
	}
	testConvertBatchTimer = unaggregated.BatchTimer{
		ID:     []byte("testConvertBatchTimer"),
		Values: []float64{222.22, 345.67, 901.23345},
	}
	testConvertGauge = unaggregated.Gauge{
		ID:    []byte("testConvertGauge"),
		Value: 123.456,
	}
	testConvertStagedMetadatas = metadata.StagedMetadatas{
		metadata.DefaultStagedMetadata,
		metadata.StagedMetadata{
			CutoverNanos: 123,
			Tombstoned:   false,
			Metadata:     metadata.DefaultMetadata,
		},
		metadata.StagedMetadata{
			CutoverNanos: 1234,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 6*time.Hour),
						},
					},
				},
			},
		},
		metadata.StagedMetadata{
			CutoverNanos: 5678,
			Tombstoned:   true,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
						},
					},
					{
						AggregationID: aggregation.MustCompressTypes(aggregation.Count, aggregation.Last),
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 6*time.Hour),
							policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 24*time.Hour),
						},
					},
					{
						AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Hour, xtime.Hour, 30*24*time.Hour),
						},
					},
				},
			},
		},
	}
)

func TestToStagedMetadatas(t *testing.T) {
	require.Equal(t, testConvertStagedMetadatas, ToStagedMetadatas(testConvertPoliciesList))
}

func TestToUnaggregatedMessageUnion(t *testing.T) {
	inputs := []struct {
		metricUnion  unaggregated.MetricUnion
		policiesList policy.PoliciesList
	}{
		{
			metricUnion:  testConvertCounterUnion,
			policiesList: testConvertPoliciesList,
		},
		{
			metricUnion:  testConvertBatchTimerUnion,
			policiesList: testConvertPoliciesList,
		},
		{
			metricUnion:  testConvertGaugeUnion,
			policiesList: testConvertPoliciesList,
		},
	}
	expected := []encoding.UnaggregatedMessageUnion{
		{
			Type: encoding.CounterWithMetadatasType,
			CounterWithMetadatas: unaggregated.CounterWithMetadatas{
				Counter:         testConvertCounter,
				StagedMetadatas: testConvertStagedMetadatas,
			},
		},
		{
			Type: encoding.BatchTimerWithMetadatasType,
			BatchTimerWithMetadatas: unaggregated.BatchTimerWithMetadatas{
				BatchTimer:      testConvertBatchTimer,
				StagedMetadatas: testConvertStagedMetadatas,
			},
		},
		{
			Type: encoding.GaugeWithMetadatasType,
			GaugeWithMetadatas: unaggregated.GaugeWithMetadatas{
				Gauge:           testConvertGauge,
				StagedMetadatas: testConvertStagedMetadatas,
			},
		},
	}

	for i, input := range inputs {
		res, err := toUnaggregatedMessageUnion(input.metricUnion, input.policiesList)
		require.NoError(t, err)
		require.Equal(t, expected[i], res)
	}
}

func TestToUnaggregatedMessageUnionError(t *testing.T) {
	invalidMetric := unaggregated.MetricUnion{
		Type: metric.UnknownType,
	}
	_, err := toUnaggregatedMessageUnion(invalidMetric, testConvertPoliciesList)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "unknown metric type"))
}
