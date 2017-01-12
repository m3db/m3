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
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"
)

var (
	testCounterVal        = int64(123)
	testBatchTimerVals    = []float64{1.5, 2.5, 3.5, 4.5, 5.5}
	testGaugeVal          = 456.789
	testVersionedPolicies = policy.VersionedPolicies{
		Version: 1,
		Policies: []policy.Policy{
			{
				Resolution: policy.Resolution{Window: time.Second, Precision: xtime.Second},
				Retention:  policy.Retention(time.Hour),
			},
			{
				Resolution: policy.Resolution{Window: 2 * time.Second, Precision: xtime.Second},
				Retention:  policy.Retention(6 * time.Hour),
			},
		},
	}
	testUpdatedVersionedPolicies = policy.VersionedPolicies{
		Version: 2,
		Policies: []policy.Policy{
			{
				Resolution: policy.Resolution{Window: time.Second, Precision: xtime.Second},
				Retention:  policy.Retention(time.Hour),
			},
			{
				Resolution: policy.Resolution{Window: 3 * time.Second, Precision: xtime.Second},
				Retention:  policy.Retention(24 * time.Hour),
			},
		},
	}
)

type byTimeIDPolicyAscending []aggregated.MetricWithPolicy

func (a byTimeIDPolicyAscending) Len() int      { return len(a) }
func (a byTimeIDPolicyAscending) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byTimeIDPolicyAscending) Less(i, j int) bool {
	if a[i].Timestamp != a[j].Timestamp {
		return a[i].Timestamp.Before(a[j].Timestamp)
	}
	id1, id2 := string(a[i].ID), string(a[j].ID)
	if id1 != id2 {
		return id1 < id2
	}
	resolution1, resolution2 := a[i].Policy.Resolution.Window, a[j].Policy.Resolution.Window
	if resolution1 != resolution2 {
		return resolution1 < resolution2
	}
	retention1, retention2 := a[i].Policy.Retention, a[j].Policy.Retention
	return retention1 < retention2
}

type valuesByTime map[time.Time]interface{}
type datapointsByID map[string]valuesByTime
type metricsByPolicy map[policy.Policy]datapointsByID

type metricTypeFn func(idx int) unaggregated.Type

type testData struct {
	timestamp time.Time
	metrics   []unaggregated.MetricUnion
}

type testDatasetWithPolicies struct {
	dataset  []testData
	policies policy.VersionedPolicies
}

func roundRobinMetricTypeFn(idx int) unaggregated.Type {
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
	return func(int) unaggregated.Type { return typ }
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
	policies policy.VersionedPolicies,
) testDatasetWithPolicies {
	var (
		testDataset []testData
		intervalIdx int
	)
	for timestamp := start; timestamp.Before(stop); timestamp = timestamp.Add(interval) {
		mp := make([]unaggregated.MetricUnion, 0, len(ids))
		for i := 0; i < len(ids); i++ {
			// Randomly generate metrics with slightly pertubrations to the values
			var mu unaggregated.MetricUnion
			metricType := typeFn(i)
			switch metricType {
			case unaggregated.CounterType:
				mu = unaggregated.MetricUnion{
					Type:       metricType,
					ID:         metric.ID(ids[i]),
					CounterVal: testCounterVal + int64(intervalIdx),
				}
			case unaggregated.BatchTimerType:
				vals := make([]float64, len(testBatchTimerVals))
				for idx, v := range testBatchTimerVals {
					vals[idx] = v + float64(intervalIdx)
				}
				mu = unaggregated.MetricUnion{
					Type:          metricType,
					ID:            metric.ID(ids[i]),
					BatchTimerVal: vals,
				}
			case unaggregated.GaugeType:
				mu = unaggregated.MetricUnion{
					Type:     metricType,
					ID:       metric.ID(ids[i]),
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
	return testDatasetWithPolicies{dataset: testDataset, policies: policies}
}

func toExpectedResults(
	t *testing.T,
	now time.Time,
	dsp testDatasetWithPolicies,
	opts aggregator.Options,
) []aggregated.MetricWithPolicy {
	byPolicy := make(metricsByPolicy)
	for _, p := range dsp.policies.Policies {
		byPolicy[p] = make(datapointsByID)
	}

	// Aggregate metrics by policies
	for _, dataValues := range dsp.dataset {
		for _, mu := range dataValues.metrics {
			for policy, metrics := range byPolicy {
				datapoints, exists := metrics[string(mu.ID)]
				if !exists {
					datapoints = make(valuesByTime)
					metrics[string(mu.ID)] = datapoints
				}
				alignedStart := dataValues.timestamp.Truncate(policy.Resolution.Window)
				values, exists := datapoints[alignedStart]
				if !exists {
					switch mu.Type {
					case unaggregated.CounterType:
						values = aggregation.NewCounter()
					case unaggregated.BatchTimerType:
						values = aggregation.NewTimer(nil)
					case unaggregated.GaugeType:
						values = aggregation.NewGauge()
					default:
						require.Fail(t, fmt.Sprintf("unrecognized metric type %v", mu.Type))
					}
				}
				// Add current metric to the value
				switch mu.Type {
				case unaggregated.CounterType:
					v := values.(aggregation.Counter)
					v.Add(mu.CounterVal)
					datapoints[alignedStart] = v
				case unaggregated.BatchTimerType:
					v := values.(aggregation.Timer)
					v.AddBatch(mu.BatchTimerVal)
					datapoints[alignedStart] = v
				case unaggregated.GaugeType:
					v := values.(aggregation.Gauge)
					v.Add(mu.GaugeVal)
					datapoints[alignedStart] = v
				default:
					require.Fail(t, fmt.Sprintf("unrecognized metric type %v", mu.Type))
				}
			}
		}
	}

	// Convert metrics by policy to sorted aggregated metrics slice
	var expected []aggregated.MetricWithPolicy
	for policy, metrics := range byPolicy {
		alignedCutoff := now.Truncate(policy.Resolution.Window)
		for id, datapoints := range metrics {
			for time, values := range datapoints {
				endAt := time.Add(policy.Resolution.Window)
				// The end time must be no later than the aligned cutoff time
				// for the data to be flushed
				if !endAt.After(alignedCutoff) {
					expected = append(expected, toAggregatedMetrics(t, id, endAt, values, policy, opts)...)
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
	id string,
	timestamp time.Time,
	values interface{},
	p policy.Policy,
	opts aggregator.Options,
) []aggregated.MetricWithPolicy {
	var result []aggregated.MetricWithPolicy
	fn := func(prefix []byte, id string, suffix []byte, timestamp time.Time, value float64, p policy.Policy) {
		result = append(result, aggregated.MetricWithPolicy{
			Metric: aggregated.Metric{
				ID:        metric.ID(string(prefix) + id + string(suffix)),
				Timestamp: timestamp,
				Value:     value,
			},
			Policy: p,
		})
	}

	switch values := values.(type) {
	case aggregation.Counter:
		fn(opts.FullCounterPrefix(), id, nil, timestamp, float64(values.Sum()), p)
	case aggregation.Timer:
		var (
			fullTimerPrefix   = opts.FullTimerPrefix()
			timerSumSuffix    = opts.TimerSumSuffix()
			timerSumSqSuffix  = opts.TimerSumSqSuffix()
			timerMeanSuffix   = opts.TimerMeanSuffix()
			timerLowerSuffix  = opts.TimerLowerSuffix()
			timerUpperSuffix  = opts.TimerUpperSuffix()
			timerCountSuffix  = opts.TimerCountSuffix()
			timerStdevSuffix  = opts.TimerStdevSuffix()
			timerMedianSuffix = opts.TimerMedianSuffix()
			quantiles         = opts.TimerQuantiles()
			quantileSuffixes  = opts.TimerQuantileSuffixes()
		)
		fn(fullTimerPrefix, id, timerSumSuffix, timestamp, values.Sum(), p)
		fn(fullTimerPrefix, id, timerSumSqSuffix, timestamp, values.SumSq(), p)
		fn(fullTimerPrefix, id, timerMeanSuffix, timestamp, values.Mean(), p)
		fn(fullTimerPrefix, id, timerLowerSuffix, timestamp, values.Min(), p)
		fn(fullTimerPrefix, id, timerUpperSuffix, timestamp, values.Max(), p)
		fn(fullTimerPrefix, id, timerCountSuffix, timestamp, float64(values.Count()), p)
		fn(fullTimerPrefix, id, timerStdevSuffix, timestamp, values.Stdev(), p)
		for idx, q := range quantiles {
			v := values.Quantile(q)
			if q == 0.5 {
				fn(fullTimerPrefix, id, timerMedianSuffix, timestamp, v, p)
			}
			fn(fullTimerPrefix, id, quantileSuffixes[idx], timestamp, v, p)
		}
	case aggregation.Gauge:
		fn(opts.FullGaugePrefix(), id, nil, timestamp, values.Value(), p)
	default:
		require.Fail(t, fmt.Sprintf("unrecognized aggregation type %T", values))
	}

	return result
}
