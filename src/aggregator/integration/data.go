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
	maggregation "github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/aggregated"
	metricid "github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/op/applied"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

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
				policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), maggregation.DefaultID),
				policy.NewPolicy(policy.NewStoragePolicy(2*time.Second, xtime.Second, 6*time.Hour), maggregation.DefaultID),
			},
		),
	}
	testUpdatedPoliciesList = policy.PoliciesList{
		policy.NewStagedPolicies(
			0,
			false,
			[]policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), maggregation.DefaultID),
				policy.NewPolicy(policy.NewStoragePolicy(3*time.Second, xtime.Second, 24*time.Hour), maggregation.DefaultID),
			},
		),
	}
	testPoliciesListWithCustomAggregation1 = policy.PoliciesList{
		policy.NewStagedPolicies(
			0,
			false,
			[]policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), maggregation.MustCompressTypes(maggregation.Min)),
				policy.NewPolicy(policy.NewStoragePolicy(2*time.Second, xtime.Second, 6*time.Hour), maggregation.MustCompressTypes(maggregation.Min)),
			},
		),
	}
	testPoliciesListWithCustomAggregation2 = policy.PoliciesList{
		policy.NewStagedPolicies(
			0,
			false,
			[]policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), maggregation.MustCompressTypes(maggregation.Min, maggregation.Max)),
				policy.NewPolicy(policy.NewStoragePolicy(3*time.Second, xtime.Second, 24*time.Hour), maggregation.MustCompressTypes(maggregation.Min, maggregation.Max)),
			},
		),
	}
	testStagedMetadatas = metadata.StagedMetadatas{
		{
			CutoverNanos: 0,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: maggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
							policy.NewStoragePolicy(2*time.Second, xtime.Second, 6*time.Hour),
						},
					},
					{
						AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Second, xtime.Second, 2*time.Hour),
						},
					},
				},
			},
		},
	}
	testUpdatedStagedMetadatas = metadata.StagedMetadatas{
		{
			CutoverNanos: 0,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: maggregation.MustCompressTypes(maggregation.Mean),
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
							policy.NewStoragePolicy(3*time.Second, xtime.Second, 6*time.Hour),
						},
					},
					{
						AggregationID: maggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(2*time.Second, xtime.Second, 2*time.Hour),
						},
					},
				},
			},
		},
	}
)

func generateTestIDs(prefix string, numIDs int) []string {
	ids := make([]string, numIDs)
	for i := 0; i < numIDs; i++ {
		ids[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return ids
}

func generateTestDataset(
	start, stop time.Time,
	interval time.Duration,
	ids []string,
	typeFn metricTypeFn,
) testDataset {
	var (
		testDataset []testData
		intervalIdx int
	)
	for timestamp := start; timestamp.Before(stop); timestamp = timestamp.Add(interval) {
		mp := make([]unaggregated.MetricUnion, 0, len(ids))
		for i := 0; i < len(ids); i++ {
			// Randomly generate metrics with slight pertubrations to the values.
			var mu unaggregated.MetricUnion
			metricType := typeFn(timestamp, i)
			switch metricType {
			case metric.CounterType:
				mu = unaggregated.MetricUnion{
					Type:       metricType,
					ID:         metricid.RawID(ids[i]),
					CounterVal: testCounterVal + int64(intervalIdx),
				}
			case metric.TimerType:
				vals := make([]float64, len(testBatchTimerVals))
				for idx, v := range testBatchTimerVals {
					vals[idx] = v + float64(intervalIdx)
				}
				mu = unaggregated.MetricUnion{
					Type:          metricType,
					ID:            metricid.RawID(ids[i]),
					BatchTimerVal: vals,
				}
			case metric.GaugeType:
				mu = unaggregated.MetricUnion{
					Type:     metricType,
					ID:       metricid.RawID(ids[i]),
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
	return testDataset
}

func computeExpectedResults(
	t *testing.T,
	now time.Time,
	dataset testDataset,
	metadata metadataUnion,
	opts aggregator.Options,
) []aggregated.MetricWithStoragePolicy {
	keys := metadata.expectedAggregationKeys(t, now, opts.DefaultStoragePolicies())
	buckets := computeExpectedAggregationBuckets(t, dataset, keys, opts)
	return computeExpectedAggregationOutput(t, now, buckets, opts)
}

// computeExpectedAggregationKeysFromPoliciesList computes the expected set of aggregation keys
// from the given time and the policies list.
func computeExpectedAggregationKeysFromPoliciesList(
	t *testing.T,
	now time.Time,
	policiesList policy.PoliciesList,
	defaultStoragePolices []policy.StoragePolicy,
) aggregationKeys {
	// Find the staged policy that is currently active.
	nowNanos := now.UnixNano()
	i := len(policiesList) - 1
	for i >= 0 {
		if policiesList[i].CutoverNanos <= nowNanos {
			break
		}
		i--
	}
	require.True(t, i >= 0)

	// If the active policies are the default policies, create the aggregation keys
	// from them.
	policies, useDefault := policiesList[i].Policies()
	if useDefault {
		res := make(aggregationKeys, 0, len(defaultStoragePolices))
		for _, sp := range defaultStoragePolices {
			key := aggregationKey{storagePolicy: sp}
			res = append(res, key)
		}
		return res
	}

	// Otherwise create the aggregation keys from the staged policies.
	res := make(aggregationKeys, 0, len(policies))
	for _, p := range policies {
		newKey := aggregationKey{
			aggregationID: p.AggregationID,
			storagePolicy: p.StoragePolicy,
		}
		res.add(newKey)
	}
	return res
}

func computeExpectedAggregationKeysFromStagedMetadatas(
	t *testing.T,
	now time.Time,
	metadatas metadata.StagedMetadatas,
	defaultStoragePolices []policy.StoragePolicy,
) aggregationKeys {
	// Find the staged policy that is currently active.
	nowNanos := now.UnixNano()
	i := len(metadatas) - 1
	for i >= 0 {
		if metadatas[i].CutoverNanos <= nowNanos {
			break
		}
		i--
	}
	require.True(t, i >= 0)

	res := make(aggregationKeys, 0, len(metadatas[i].Pipelines))
	for _, pipeline := range metadatas[i].Pipelines {
		storagePolicies := pipeline.StoragePolicies
		if policy.IsDefaultStoragePolicies(storagePolicies) {
			storagePolicies = defaultStoragePolices
		}
		for _, sp := range storagePolicies {
			newKey := aggregationKey{
				aggregationID: pipeline.AggregationID,
				storagePolicy: sp,
				pipeline:      pipeline.Pipeline,
			}
			res.add(newKey)
		}
	}
	return res
}

// computeExpectedAggregationBuckets computes the expected aggregation buckets for the given
// dataset and the aggregation keys, assuming each metric in the given dataset is associated
// with the full set of aggregation keys passed in.
func computeExpectedAggregationBuckets(
	t *testing.T,
	dataset testDataset,
	keys aggregationKeys,
	opts aggregator.Options,
) []aggregationBucket {
	buckets := make([]aggregationBucket, 0, len(keys))
	for _, k := range keys {
		bucket := aggregationBucket{key: k, data: make(datapointsByID)}
		buckets = append(buckets, bucket)
	}

	for _, dataValues := range dataset {
		for _, mu := range dataValues.metrics {
			for _, bucket := range buckets {
				// Add metric to the list of metrics aggregated by the aggregation bucket if necessary.
				key := metricKey{id: string(mu.ID), typ: mu.Type}
				datapoints, metricExists := bucket.data[key]
				if !metricExists {
					datapoints = make(valuesByTime)
					bucket.data[key] = datapoints
				}

				// Add metric to the time bucket associated with the aggregation bucket if necessary.
				resolution := bucket.key.storagePolicy.Resolution()
				alignedStartNanos := dataValues.timestamp.Truncate(resolution.Window).UnixNano()
				values, timeBucketExists := datapoints[alignedStartNanos]
				if !timeBucketExists {
					var (
						aggTypeOpts = opts.AggregationTypesOptions()
						aggTypes    = maggregation.NewIDDecompressor().MustDecompress(bucket.key.aggregationID)
						//metrics.aggTypes = aggTypes
						aggregationOpts = aggregation.NewOptions()
					)
					switch mu.Type {
					case metric.CounterType:
						if aggTypes.IsDefault() {
							aggTypes = aggTypeOpts.DefaultCounterAggregationTypes()
						}
						aggregationOpts.ResetSetData(aggTypes)
						values = aggregation.NewCounter(aggregationOpts)
					case metric.TimerType:
						if aggTypes.IsDefault() {
							aggTypes = aggTypeOpts.DefaultTimerAggregationTypes()
						}
						aggregationOpts.ResetSetData(aggTypes)
						values = aggregation.NewTimer(aggTypeOpts.TimerQuantiles(), opts.StreamOptions(), aggregationOpts)
					case metric.GaugeType:
						if aggTypes.IsDefault() {
							aggTypes = aggTypeOpts.DefaultGaugeAggregationTypes()
						}
						aggregationOpts.ResetSetData(aggTypes)
						values = aggregation.NewGauge(aggregationOpts)
					default:
						require.Fail(t, fmt.Sprintf("unrecognized metric type %v", mu.Type))
					}
				}

				// Add metric value to the corresponding time bucket.
				switch mu.Type {
				case metric.CounterType:
					v := values.(aggregation.Counter)
					v.Update(mu.CounterVal)
					datapoints[alignedStartNanos] = v
				case metric.TimerType:
					v := values.(aggregation.Timer)
					v.AddBatch(mu.BatchTimerVal)
					datapoints[alignedStartNanos] = v
				case metric.GaugeType:
					v := values.(aggregation.Gauge)
					v.Update(mu.GaugeVal)
					datapoints[alignedStartNanos] = v
				default:
					require.Fail(t, fmt.Sprintf("unrecognized metric type %v", mu.Type))
				}
			}
		}
	}

	return buckets
}

// computeExpectedAggregationOutput computes the expected aggregation output given
// the current time and the populated aggregation buckets.
func computeExpectedAggregationOutput(
	t *testing.T,
	now time.Time,
	buckets []aggregationBucket,
	opts aggregator.Options,
) []aggregated.MetricWithStoragePolicy {
	var expected []aggregated.MetricWithStoragePolicy
	for _, bucket := range buckets {
		var (
			aggregationTypes   = maggregation.NewIDDecompressor().MustDecompress(bucket.key.aggregationID)
			storagePolicy      = bucket.key.storagePolicy
			resolutionWindow   = storagePolicy.Resolution().Window
			alignedCutoffNanos = now.Truncate(resolutionWindow).UnixNano()
		)
		for key, datapoints := range bucket.data {
			for timeNanos, values := range datapoints {
				endAtNanos := timeNanos + int64(resolutionWindow)
				// The end time must be no later than the aligned cutoff time
				// for the data to be flushed.
				if endAtNanos > alignedCutoffNanos {
					continue
				}
				outputs := computeExpectedAggregatedMetrics(
					t,
					key,
					endAtNanos,
					values,
					storagePolicy,
					aggregationTypes,
					opts,
				)
				expected = append(expected, outputs...)
			}
		}
	}

	// Sort the aggregated metrics.
	sort.Sort(byTimeIDPolicyAscending(expected))

	return expected
}

// computeExpectedAggregatedMetrics computes the expected set of aggregated metrics
// given the metric key, timestamp, metric aggregation, and related aggregation metadata.
func computeExpectedAggregatedMetrics(
	t *testing.T,
	key metricKey,
	timeNanos int64,
	metricAgg interface{},
	sp policy.StoragePolicy,
	aggTypes maggregation.Types,
	opts aggregator.Options,
) []aggregated.MetricWithStoragePolicy {
	var results []aggregated.MetricWithStoragePolicy
	fn := func(
		prefix []byte,
		id string,
		suffix []byte,
		timeNanos int64,
		value float64,
		sp policy.StoragePolicy,
	) {
		results = append(results, aggregated.MetricWithStoragePolicy{
			Metric: aggregated.Metric{
				ID:        metricid.RawID(string(prefix) + id + string(suffix)),
				TimeNanos: timeNanos,
				Value:     value,
			},
			StoragePolicy: sp,
		})
	}

	id := key.id
	aggTypeOpts := opts.AggregationTypesOptions()
	switch metricAgg := metricAgg.(type) {
	case aggregation.Counter:
		var fullCounterPrefix = opts.FullCounterPrefix()
		if aggTypes.IsDefault() {
			aggTypes = aggTypeOpts.DefaultCounterAggregationTypes()
		}

		for _, aggType := range aggTypes {
			fn(fullCounterPrefix, id, aggTypeOpts.TypeStringForCounter(aggType), timeNanos, metricAgg.ValueOf(aggType), sp)
		}
	case aggregation.Timer:
		var fullTimerPrefix = opts.FullTimerPrefix()
		if aggTypes.IsDefault() {
			aggTypes = aggTypeOpts.DefaultTimerAggregationTypes()
		}

		for _, aggType := range aggTypes {
			fn(fullTimerPrefix, id, aggTypeOpts.TypeStringForTimer(aggType), timeNanos, metricAgg.ValueOf(aggType), sp)
		}
	case aggregation.Gauge:
		var fullGaugePrefix = opts.FullGaugePrefix()
		if aggTypes.IsDefault() {
			aggTypes = aggTypeOpts.DefaultGaugeAggregationTypes()
		}

		for _, aggType := range aggTypes {
			fn(fullGaugePrefix, id, aggTypeOpts.TypeStringForGauge(aggType), timeNanos, metricAgg.ValueOf(aggType), sp)
		}
	default:
		require.Fail(t, fmt.Sprintf("unrecognized aggregation type %T", metricAgg))
	}

	return results
}

func roundRobinMetricTypeFn(_ time.Time, idx int) metric.Type {
	switch idx % 3 {
	case 0:
		return metric.CounterType
	case 1:
		return metric.TimerType
	default:
		return metric.GaugeType
	}
}

func constantMetricTypeFnFactory(typ metric.Type) metricTypeFn {
	return func(time.Time, int) metric.Type { return typ }
}

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

type metricTypeFn func(ts time.Time, idx int) metric.Type

type metricKey struct {
	id  string
	typ metric.Type
}

type valuesByTime map[int64]interface{}
type datapointsByID map[metricKey]valuesByTime

type aggregationKey struct {
	aggregationID maggregation.ID
	storagePolicy policy.StoragePolicy
	pipeline      applied.Pipeline
}

func (k aggregationKey) Equal(other aggregationKey) bool {
	return k.aggregationID == other.aggregationID &&
		k.storagePolicy == other.storagePolicy &&
		k.pipeline.Equal(other.pipeline)
}

type aggregationKeys []aggregationKey

func (keys *aggregationKeys) add(newKey aggregationKey) {
	for _, k := range *keys {
		if k.Equal(newKey) {
			return
		}
	}
	*keys = append(*keys, newKey)
}

type aggregationBucket struct {
	key  aggregationKey
	data datapointsByID
}

type testData struct {
	timestamp time.Time
	metrics   []unaggregated.MetricUnion
}

type testDataset []testData

type metadataType int

const (
	policiesListType metadataType = iota
	stagedMetadatasType
)

type metadataUnion struct {
	mType           metadataType
	policiesList    policy.PoliciesList
	stagedMetadatas metadata.StagedMetadatas
}

func (mu metadataUnion) expectedAggregationKeys(
	t *testing.T,
	now time.Time,
	defaultStoragePolicies []policy.StoragePolicy,
) aggregationKeys {
	switch mu.mType {
	case policiesListType:
		return computeExpectedAggregationKeysFromPoliciesList(t, now, mu.policiesList, defaultStoragePolicies)
	case stagedMetadatasType:
		return computeExpectedAggregationKeysFromStagedMetadatas(t, now, mu.stagedMetadatas, defaultStoragePolicies)
	}
	panic("should not reach here")
}
