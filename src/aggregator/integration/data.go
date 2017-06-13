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

package integration

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3aggregator/aggregation"
	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3metrics/metric/aggregated"
	metricID "github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testCounterVal     = int64(123)
	testBatchTimerVals = []float64{1.5, 2.5, 3.5, 4.5, 5.5}
	testGaugeVal       = 456.789
	testPoliciesList   = policy.PoliciesList{
		policy.NewStagedPolicies(
			0,
			false,
			[]policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), policy.DefaultAggregationID),
				policy.NewPolicy(policy.NewStoragePolicy(2*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
			},
		),
	}
	testUpdatedPoliciesList = policy.PoliciesList{
		policy.NewStagedPolicies(
			0,
			false,
			[]policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), policy.DefaultAggregationID),
				policy.NewPolicy(policy.NewStoragePolicy(3*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
			},
		),
	}

	testDefaultTimerAggregationTypes = policy.AggregationTypes{
		policy.Sum,
		policy.SumSq,
		policy.Mean,
		policy.Lower,
		policy.Upper,
		policy.Count,
		policy.Stdev,
		policy.Median,
		policy.P50,
		policy.P95,
		policy.P99,
	}
)

type byTimeIDPolicyAscending []aggregated.MetricWithStoragePolicy

func (a byTimeIDPolicyAscending) Len() int      { return len(a) }
func (a byTimeIDPolicyAscending) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byTimeIDPolicyAscending) Less(i, j int) bool {
	if a[i].TimeNanos != a[j].TimeNanos {
		return a[i].TimeNanos < a[j].TimeNanos
	}
	id1, id2 := string(a[i].ID), string(a[j].ID)
	if id1 != id2 {
		return id1 < id2
	}
	resolution1, resolution2 := a[i].Resolution().Window, a[j].Resolution().Window
	if resolution1 != resolution2 {
		return resolution1 < resolution2
	}
	retention1, retention2 := a[i].Retention(), a[j].Retention()
	return retention1 < retention2
}

type metricKey struct {
	id  string
	typ unaggregated.Type
}
type valuesByTime map[int64]interface{}
type datapointsByID map[metricKey]valuesByTime
type dataForPolicy struct {
	aggTypes policy.AggregationTypes
	data     datapointsByID
}
type metricsByPolicy map[policy.Policy]*dataForPolicy

type metricTypeFn func(ts time.Time, idx int) unaggregated.Type

type testData struct {
	timestamp time.Time
	metrics   []unaggregated.MetricUnion
}

type testDatasetWithPoliciesList struct {
	dataset      []testData
	policiesList policy.PoliciesList
}

func roundRobinMetricTypeFn(_ time.Time, idx int) unaggregated.Type {
	switch idx % 3 {
	case 0:
		return unaggregated.CounterType
	case 1:
		return unaggregated.BatchTimerType
	default:
		return unaggregated.GaugeType
	}
}

func constantMetryTypeFnFactory(typ unaggregated.Type) metricTypeFn {
	return func(time.Time, int) unaggregated.Type { return typ }
}

func generateTestIDs(prefix string, numIDs int) []string {
	ids := make([]string, numIDs)
	for i := 0; i < numIDs; i++ {
		ids[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return ids
}

func generateTestData(
	t *testing.T,
	start, stop time.Time,
	interval time.Duration,
	ids []string,
	typeFn metricTypeFn,
	policiesList policy.PoliciesList,
) testDatasetWithPoliciesList {
	var (
		testDataset []testData
		intervalIdx int
	)
	for timestamp := start; timestamp.Before(stop); timestamp = timestamp.Add(interval) {
		mp := make([]unaggregated.MetricUnion, 0, len(ids))
		for i := 0; i < len(ids); i++ {
			// Randomly generate metrics with slightly pertubrations to the values
			var mu unaggregated.MetricUnion
			metricType := typeFn(timestamp, i)
			switch metricType {
			case unaggregated.CounterType:
				mu = unaggregated.MetricUnion{
					Type:       metricType,
					ID:         metricID.RawID(ids[i]),
					CounterVal: testCounterVal + int64(intervalIdx),
				}
			case unaggregated.BatchTimerType:
				vals := make([]float64, len(testBatchTimerVals))
				for idx, v := range testBatchTimerVals {
					vals[idx] = v + float64(intervalIdx)
				}
				mu = unaggregated.MetricUnion{
					Type:          metricType,
					ID:            metricID.RawID(ids[i]),
					BatchTimerVal: vals,
				}
			case unaggregated.GaugeType:
				mu = unaggregated.MetricUnion{
					Type:     metricType,
					ID:       metricID.RawID(ids[i]),
					GaugeVal: testGaugeVal + float64(intervalIdx),
				}
			}
			mp = append(mp, mu)
		}
		testDataset = append(testDataset, testData{
			timestamp: timestamp,
			metrics:   mp,
		})
		intervalIdx++
	}
	return testDatasetWithPoliciesList{
		dataset:      testDataset,
		policiesList: policiesList,
	}
}

func toExpectedResults(
	t *testing.T,
	now time.Time,
	dsp testDatasetWithPoliciesList,
	opts aggregator.Options,
) []aggregated.MetricWithStoragePolicy {
	var (
		nowNanos       = now.UnixNano()
		policiesList   = dsp.policiesList
		activePolicies policy.StagedPolicies
		found          bool
	)

	require.True(t, len(policiesList) > 0)
	for i := len(policiesList) - 1; i >= 0; i-- {
		if policiesList[i].CutoverNanos <= nowNanos {
			found = true
			activePolicies = policiesList[i]
			break
		}
	}
	require.True(t, found)

	policies, useDefault := activePolicies.Policies()
	if useDefault {
		policies = opts.DefaultPolicies()
	}

	byPolicy := make(metricsByPolicy)
	for _, p := range policies {
		byPolicy[p] = &dataForPolicy{aggTypes: policy.DefaultAggregationTypes, data: make(datapointsByID)}
	}

	decompressor := policy.NewAggregationIDDecompressor()
	// Aggregate metrics by policies
	for _, dataValues := range dsp.dataset {
		for _, mu := range dataValues.metrics {
			for policy, metrics := range byPolicy {
				key := metricKey{id: string(mu.ID), typ: mu.Type}
				datapoints, exists := metrics.data[key]
				if !exists {
					datapoints = make(valuesByTime)
					metrics.data[key] = datapoints
				}
				alignedStartNanos := dataValues.timestamp.Truncate(policy.Resolution().Window).UnixNano()
				values, exists := datapoints[alignedStartNanos]

				aggTypes, err := decompressor.Decompress(policy.AggregationID)
				require.NoError(t, err)

				metrics.aggTypes = aggTypes
				aggregationOpts := aggregation.NewOptions()

				if !exists {
					switch mu.Type {
					case unaggregated.CounterType:
						// TODO(cw) Get default aggregation types for Counter.
						aggregationOpts.ResetSetData(aggTypes)
						values = aggregation.NewCounter(aggregationOpts)
					case unaggregated.BatchTimerType:
						if aggTypes.IsDefault() {
							aggTypes = opts.DefaultTimerAggregationTypes()
						}
						aggregationOpts.ResetSetData(aggTypes)
						values = aggregation.NewTimer(opts.TimerQuantiles(), opts.StreamOptions(), aggregationOpts)
					case unaggregated.GaugeType:
						// TODO(cw) Get default aggregation types for Gauge.
						aggregationOpts.ResetSetData(aggTypes)
						values = aggregation.NewGauge(aggregationOpts)
					default:
						require.Fail(t, fmt.Sprintf("unrecognized metric type %v", mu.Type))
					}
				}
				// Add current metric to the value
				switch mu.Type {
				case unaggregated.CounterType:
					v := values.(aggregation.Counter)
					v.Update(mu.CounterVal)
					datapoints[alignedStartNanos] = v
				case unaggregated.BatchTimerType:
					v := values.(aggregation.Timer)
					v.AddBatch(mu.BatchTimerVal)
					datapoints[alignedStartNanos] = v
				case unaggregated.GaugeType:
					v := values.(aggregation.Gauge)
					v.Update(mu.GaugeVal)
					datapoints[alignedStartNanos] = v
				default:
					require.Fail(t, fmt.Sprintf("unrecognized metric type %v", mu.Type))
				}
			}
		}
	}

	// Convert metrics by policy to sorted aggregated metrics slice
	var expected []aggregated.MetricWithStoragePolicy
	for policy, metrics := range byPolicy {
		alignedCutoffNanos := now.Truncate(policy.Resolution().Window).UnixNano()
		for key, datapoints := range metrics.data {
			for timeNanos, values := range datapoints {
				endAtNanos := timeNanos + int64(policy.Resolution().Window)
				// The end time must be no later than the aligned cutoff time
				// for the data to be flushed
				if endAtNanos <= alignedCutoffNanos {
					expected = append(expected, toAggregatedMetrics(t, key, endAtNanos, values, policy.StoragePolicy, metrics.aggTypes, opts)...)
				}
			}
		}
	}

	// Sort the aggregated metrics
	sort.Sort(byTimeIDPolicyAscending(expected))

	return expected
}

func toAggregatedMetrics(
	t *testing.T,
	key metricKey,
	timeNanos int64,
	values interface{},
	sp policy.StoragePolicy,
	aggTypes policy.AggregationTypes,
	opts aggregator.Options,
) []aggregated.MetricWithStoragePolicy {
	var result []aggregated.MetricWithStoragePolicy
	fn := func(prefix []byte, id string, suffix []byte, timeNanos int64, value float64, sp policy.StoragePolicy) {
		result = append(result, aggregated.MetricWithStoragePolicy{
			Metric: aggregated.Metric{
				ID:        metricID.RawID(string(prefix) + id + string(suffix)),
				TimeNanos: timeNanos,
				Value:     value,
			},
			StoragePolicy: sp,
		})
	}

	id := key.id
	switch values := values.(type) {
	case aggregation.Counter:
		var fullCounterPrefix = opts.FullCounterPrefix()
		if aggTypes.IsDefault() {
			fn(fullCounterPrefix, id, nil, timeNanos, float64(values.Sum()), sp)
			break
		}

		for _, aggType := range aggTypes {
			fn(fullCounterPrefix, id, opts.Suffix(aggType), timeNanos, values.ValueOf(aggType), sp)
		}
	case aggregation.Timer:
		var fullTimerPrefix = opts.FullTimerPrefix()
		if aggTypes.IsDefault() {
			aggTypes = opts.DefaultTimerAggregationTypes()
		}

		for _, aggType := range aggTypes {
			fn(fullTimerPrefix, id, opts.Suffix(aggType), timeNanos, values.ValueOf(aggType), sp)
		}
	case aggregation.Gauge:
		var fullGaugePrefix = opts.FullGaugePrefix()
		if aggTypes.IsDefault() {
			fn(fullGaugePrefix, id, nil, timeNanos, values.Last(), sp)
			break
		}

		for _, aggType := range aggTypes {
			fn(fullGaugePrefix, id, opts.Suffix(aggType), timeNanos, values.ValueOf(aggType), sp)
		}
	default:
		require.Fail(t, fmt.Sprintf("unrecognized aggregation type %T", values))
	}

	return result
}
