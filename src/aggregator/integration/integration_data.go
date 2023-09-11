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
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregation"
	"github.com/m3db/m3/src/aggregator/aggregator"
	maggregation "github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	metricid "github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

var (
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
	testStagedMetadatasWithCustomAggregation1 = metadata.StagedMetadatas{
		{
			CutoverNanos: 0,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: maggregation.MustCompressTypes(maggregation.Min),
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
							policy.NewStoragePolicy(2*time.Second, xtime.Second, 6*time.Hour),
						},
					},
				},
			},
		},
	}
	testStagedMetadatasWithCustomAggregation2 = metadata.StagedMetadatas{
		{
			CutoverNanos: 0,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: maggregation.MustCompressTypes(maggregation.Min, maggregation.Max),
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
							policy.NewStoragePolicy(3*time.Second, xtime.Second, 24*time.Hour),
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
	testCmpOpts = []cmp.Option{
		cmpopts.EquateEmpty(),
		cmpopts.EquateNaNs(),
		cmp.AllowUnexported(policy.StoragePolicy{}),
	}
)

func generateTestIDs(prefix string, numIDs int) []string {
	ids := make([]string, numIDs)
	for i := 0; i < numIDs; i++ {
		ids[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return ids
}

func mustGenerateTestDataset(t *testing.T, opts datasetGenOpts) testDataset {
	ds, err := generateTestDataset(opts)
	require.NoError(t, err)
	return ds
}

func generateTestDataset(opts datasetGenOpts) (testDataset, error) {
	var (
		testDataset []testData
		intervalIdx int
	)
	for timestamp := opts.start; timestamp.Before(opts.stop); timestamp = timestamp.Add(opts.interval) {
		metricWithMetadatas := make([]metricWithMetadataUnion, 0, len(opts.ids))
		for i := 0; i < len(opts.ids); i++ {
			var (
				metricType = opts.typeFn(timestamp, i)
				mu         metricUnion
			)
			switch opts.category {
			case untimedMetric:
				var err error
				mu, err = generateTestUntimedMetric(metricType, opts.ids[i], xtime.ToUnixNano(timestamp), intervalIdx,
					i, opts.valueGenOpts.untimed)
				if err != nil {
					return nil, err
				}
			case forwardedMetric:
				mu = generateTestForwardedMetric(metricType, opts.ids[i], timestamp.UnixNano(), intervalIdx,
					i, opts.valueGenOpts.forwarded)
			case timedMetric:
				mu = generateTestTimedMetric(metricType, opts.ids[i], timestamp.UnixNano(), intervalIdx,
					i, opts.valueGenOpts.timed)
			case passthroughMetric:
				mu = generateTestPassthroughMetric(metricType, opts.ids[i], timestamp.UnixNano(), intervalIdx,
					i, opts.valueGenOpts.passthrough)
			default:
				return nil, fmt.Errorf("unrecognized metric category: %v", opts.category)
			}
			metricWithMetadatas = append(metricWithMetadatas, metricWithMetadataUnion{
				metric:   mu,
				metadata: opts.metadataFn(i),
			})
		}
		testDataset = append(testDataset, testData{
			timestamp:           timestamp,
			metricWithMetadatas: metricWithMetadatas,
		})
		intervalIdx++
	}
	return testDataset, nil
}

func generateTestUntimedMetric(
	metricType metric.Type,
	id string,
	timestamp xtime.UnixNano,
	intervalIdx, idIdx int,
	valueGenOpts untimedValueGenOpts,
) (metricUnion, error) {
	mu := metricUnion{category: untimedMetric}
	annotation := generateAnnotation(metricType, idIdx)
	switch metricType {
	case metric.CounterType:
		mu.untimed = unaggregated.MetricUnion{
			Type:            metricType,
			ID:              metricid.RawID(id),
			CounterVal:      valueGenOpts.counterValueGenFn(intervalIdx, idIdx),
			Annotation:      annotation,
			ClientTimeNanos: timestamp,
		}
	case metric.TimerType:
		mu.untimed = unaggregated.MetricUnion{
			Type:            metricType,
			ID:              metricid.RawID(id),
			BatchTimerVal:   valueGenOpts.timerValueGenFn(intervalIdx, idIdx),
			Annotation:      annotation,
			ClientTimeNanos: timestamp,
		}
	case metric.GaugeType:
		mu.untimed = unaggregated.MetricUnion{
			Type:            metricType,
			ID:              metricid.RawID(id),
			GaugeVal:        valueGenOpts.gaugeValueGenFn(intervalIdx, idIdx),
			Annotation:      annotation,
			ClientTimeNanos: timestamp,
		}
	default:
		return metricUnion{}, fmt.Errorf("unrecognized untimed metric type: %v", metricType)
	}
	return mu, nil
}

func generateTestTimedMetric(
	metricType metric.Type,
	id string,
	timeNanos int64,
	intervalIdx, idIdx int,
	valueGenOpts timedValueGenOpts,
) metricUnion {
	return metricUnion{
		category: timedMetric,
		timed: aggregated.Metric{
			Type:       metricType,
			ID:         metricid.RawID(id),
			TimeNanos:  timeNanos,
			Value:      valueGenOpts.timedValueGenFn(intervalIdx, idIdx),
			Annotation: generateAnnotation(metricType, idIdx),
		},
	}
}

func generateTestPassthroughMetric(
	metricType metric.Type,
	id string,
	timeNanos int64,
	intervalIdx, idIdx int,
	valueGenOpts passthroughValueGenOpts,
) metricUnion {
	return metricUnion{
		category: passthroughMetric,
		passthrough: aggregated.Metric{
			Type:       metricType,
			ID:         metricid.RawID(id),
			TimeNanos:  timeNanos,
			Value:      valueGenOpts.passthroughValueGenFn(intervalIdx, idIdx),
			Annotation: generateAnnotation(metricType, idIdx),
		},
	}
}

func generateTestForwardedMetric(
	metricType metric.Type,
	id string,
	timeNanos int64,
	intervalIdx, idIdx int,
	valueGenOpts forwardedValueGenOpts,
) metricUnion {
	return metricUnion{
		category: forwardedMetric,
		forwarded: aggregated.ForwardedMetric{
			Type:       metricType,
			ID:         metricid.RawID(id),
			TimeNanos:  timeNanos,
			Values:     valueGenOpts.forwardedValueGenFn(intervalIdx, idIdx),
			Annotation: generateAnnotation(metricType, idIdx),
		},
	}
}

func generateAnnotation(typ metric.Type, idx int) []byte {
	return []byte(fmt.Sprintf("%v annotation, idx=%v", typ.String(), idx))
}

func mustComputeExpectedResults(
	t *testing.T,
	now time.Time,
	dataset testDataset,
	opts aggregator.Options,
) []aggregated.MetricWithStoragePolicy {
	res, err := computeExpectedResults(now, dataset, opts)
	require.NoError(t, err)
	return res
}

func computeExpectedResults(
	now time.Time,
	dataset testDataset,
	opts aggregator.Options,
) ([]aggregated.MetricWithStoragePolicy, error) {
	buckets, err := computeExpectedAggregationBuckets(now, dataset, opts)
	if err != nil {
		return nil, err
	}
	return computeExpectedAggregationOutput(now, buckets, opts)
}

// computeExpectedAggregationBuckets computes the expected aggregation buckets for the given
// dataset and the aggregation keys, assuming each metric in the given dataset is associated
// with the full set of aggregation keys passed in.
func computeExpectedAggregationBuckets(
	now time.Time,
	dataset testDataset,
	opts aggregator.Options,
) ([]aggregationBucket, error) {
	var (
		buckets                = make([]aggregationBucket, 0)
		defaultStoragePolicies = opts.DefaultStoragePolicies()
	)
	for _, dataValues := range dataset {
		for _, mm := range dataValues.metricWithMetadatas {
			keys, err := mm.metadata.expectedAggregationKeys(now, defaultStoragePolicies)
			if err != nil {
				return nil, err
			}
			for _, key := range keys {
				// Find or create the corresponding bucket.
				var bucket *aggregationBucket
				for _, b := range buckets {
					if b.key.Equal(key) {
						bucket = &b
						break
					}
				}
				if bucket == nil {
					buckets = append(buckets, aggregationBucket{key: key, data: make(datapointsByID)})
					bucket = &buckets[len(buckets)-1]
				}

				// Add metric to the list of metrics aggregated by the aggregation bucket if necessary.
				mu := mm.metric
				key := metricKey{category: mu.category, typ: mu.Type(), id: string(mu.ID()), storagePolicy: key.storagePolicy}
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
						aggTypeOpts     = opts.AggregationTypesOptions()
						aggTypes        = maggregation.NewIDDecompressor().MustDecompress(bucket.key.aggregationID)
						aggregationOpts = aggregation.NewOptions(opts.InstrumentOptions())
					)
					switch mu.Type() {
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
						values = aggregation.NewTimer(aggTypeOpts.Quantiles(), opts.StreamOptions(), aggregationOpts)
					case metric.GaugeType:
						if aggTypes.IsDefault() {
							aggTypes = aggTypeOpts.DefaultGaugeAggregationTypes()
						}
						aggregationOpts.ResetSetData(aggTypes)
						values = aggregation.NewGauge(aggregationOpts)
					default:
						return nil, fmt.Errorf("unrecognized metric type %v", mu.Type())
					}
				}

				// Add metric value to the corresponding time bucket.
				var err error
				switch mu.category {
				case untimedMetric:
					values, err = addUntimedMetricToAggregation(values, mu.untimed)
				case forwardedMetric:
					values, err = addForwardedMetricToAggregation(values, mu.forwarded)
				case timedMetric:
					values, err = addTimedMetricToAggregation(values, mu.timed)
				case passthroughMetric:
					// Passthrough metrics need no aggregation.
					err = nil
				default:
					err = fmt.Errorf("unrecognized metric category: %v", mu.category)
				}
				if err != nil {
					return nil, err
				}
				datapoints[alignedStartNanos] = values
			}
		}
	}

	return buckets, nil
}

func addUntimedMetricToAggregation(
	values interface{},
	mu unaggregated.MetricUnion,
) (interface{}, error) {
	switch mu.Type {
	case metric.CounterType:
		v := values.(aggregation.Counter)
		v.Update(time.Now(), mu.CounterVal, mu.Annotation)
		return v, nil
	case metric.TimerType:
		v := values.(aggregation.Timer)
		v.AddBatch(time.Now(), mu.BatchTimerVal, mu.Annotation)
		return v, nil
	case metric.GaugeType:
		v := values.(aggregation.Gauge)
		v.Update(time.Now(), mu.GaugeVal, mu.Annotation)
		return v, nil
	default:
		return nil, fmt.Errorf("unrecognized untimed metric type %v", mu.Type)
	}
}

func addTimedMetricToAggregation(
	values interface{},
	mu aggregated.Metric,
) (interface{}, error) {
	switch mu.Type {
	case metric.CounterType:
		v := values.(aggregation.Counter)
		v.Update(time.Now(), int64(mu.Value), mu.Annotation)
		return v, nil
	case metric.TimerType:
		v := values.(aggregation.Timer)
		v.AddBatch(time.Now(), []float64{mu.Value}, mu.Annotation)
		return v, nil
	case metric.GaugeType:
		v := values.(aggregation.Gauge)
		v.Update(time.Now(), mu.Value, mu.Annotation)
		return v, nil
	default:
		return nil, fmt.Errorf("unrecognized timed metric type %v", mu.Type)
	}
}

func addForwardedMetricToAggregation(
	values interface{},
	mu aggregated.ForwardedMetric,
) (interface{}, error) {
	switch mu.Type {
	case metric.CounterType:
		v := values.(aggregation.Counter)
		for _, val := range mu.Values {
			v.Update(time.Now(), int64(val), mu.Annotation)
		}
		return v, nil
	case metric.TimerType:
		v := values.(aggregation.Timer)
		v.AddBatch(time.Now(), mu.Values, mu.Annotation)
		return v, nil
	case metric.GaugeType:
		v := values.(aggregation.Gauge)
		for _, val := range mu.Values {
			v.Update(time.Now(), val, mu.Annotation)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("unrecognized forwarded metric type %v", mu.Type)
	}
}

// computeExpectedAggregationOutput computes the expected aggregation output given
// the current time and the populated aggregation buckets.
func computeExpectedAggregationOutput(
	now time.Time,
	buckets []aggregationBucket,
	opts aggregator.Options,
) ([]aggregated.MetricWithStoragePolicy, error) {
	var expected []aggregated.MetricWithStoragePolicy
	for _, bucket := range buckets {
		var (
			aggregationTypes   = maggregation.NewIDDecompressor().MustDecompress(bucket.key.aggregationID)
			storagePolicy      = bucket.key.storagePolicy
			resolutionWindow   = storagePolicy.Resolution().Window
			alignedCutoffNanos = now.Truncate(resolutionWindow).UnixNano()
		)
		for key, datapoints := range bucket.data {
			timestampNanosFn := key.category.TimestampNanosFn()
			for windowStartAtNanos, values := range datapoints {
				timestampNanos := timestampNanosFn(windowStartAtNanos, resolutionWindow)
				// The end time must be no later than the aligned cutoff time
				// for the data to be flushed.
				if timestampNanos > alignedCutoffNanos {
					continue
				}
				outputs, err := computeExpectedAggregatedMetrics(
					key,
					timestampNanos,
					values,
					storagePolicy,
					aggregationTypes,
					opts,
				)
				if err != nil {
					return nil, err
				}
				expected = append(expected, outputs...)
			}
		}
	}

	// Sort the aggregated metrics.
	sort.Sort(byTimeIDPolicyAscending(expected))

	return expected, nil
}

// computeExpectedAggregatedMetrics computes the expected set of aggregated metrics
// given the metric key, timestamp, metric aggregation, and related aggregation metadata.
func computeExpectedAggregatedMetrics(
	key metricKey,
	timeNanos int64,
	metricAgg interface{},
	sp policy.StoragePolicy,
	aggTypes maggregation.Types,
	opts aggregator.Options,
) ([]aggregated.MetricWithStoragePolicy, error) {
	var results []aggregated.MetricWithStoragePolicy
	fn := func(
		prefix []byte,
		id string,
		suffix []byte,
		timeNanos int64,
		value float64,
		annotation []byte,
		sp policy.StoragePolicy,
	) {
		results = append(results, aggregated.MetricWithStoragePolicy{
			Metric: aggregated.Metric{
				ID:         metricid.RawID(string(prefix) + id + string(suffix)),
				TimeNanos:  timeNanos,
				Value:      value,
				Annotation: annotation,
			},
			StoragePolicy: sp,
		})
	}

	id := key.id
	aggTypeOpts := opts.AggregationTypesOptions()
	switch metricAgg := metricAgg.(type) {
	case aggregation.Counter:
		if aggTypes.IsDefault() {
			aggTypes = aggTypeOpts.DefaultCounterAggregationTypes()
		}

		for _, aggType := range aggTypes {
			if key.category == timedMetric {
				fn(nil, id, nil, timeNanos, metricAgg.ValueOf(aggType), metricAgg.Annotation(), sp)
				continue
			}
			fn(opts.FullCounterPrefix(), id, aggTypeOpts.TypeStringForCounter(aggType), timeNanos,
				metricAgg.ValueOf(aggType), metricAgg.Annotation(), sp)
		}
	case aggregation.Timer:
		if aggTypes.IsDefault() {
			aggTypes = aggTypeOpts.DefaultTimerAggregationTypes()
		}

		for _, aggType := range aggTypes {
			if key.category == timedMetric {
				fn(nil, id, nil, timeNanos, metricAgg.ValueOf(aggType), metricAgg.Annotation(), sp)
				continue
			}
			fn(opts.FullTimerPrefix(), id, aggTypeOpts.TypeStringForTimer(aggType), timeNanos,
				metricAgg.ValueOf(aggType), metricAgg.Annotation(), sp)
		}
	case aggregation.Gauge:
		if aggTypes.IsDefault() {
			aggTypes = aggTypeOpts.DefaultGaugeAggregationTypes()
		}

		for _, aggType := range aggTypes {
			if key.category == timedMetric {
				fn(nil, id, nil, timeNanos, metricAgg.ValueOf(aggType), metricAgg.Annotation(), sp)
				continue
			}
			fn(opts.FullGaugePrefix(), id, aggTypeOpts.TypeStringForGauge(aggType), timeNanos,
				metricAgg.ValueOf(aggType), metricAgg.Annotation(), sp)
		}
	default:
		return nil, fmt.Errorf("unrecognized aggregation type %T", metricAgg)
	}

	return results, nil
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
	category      metricCategory
	typ           metric.Type
	id            string
	storagePolicy policy.StoragePolicy
}

type (
	valuesByTime   map[int64]interface{}
	datapointsByID map[metricKey]valuesByTime
)

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

// timestampNanosFn computes the timestamp in nanoseconds of metrics in a given time window.
type timestampNanosFn func(windowStartAtNanos int64, resolution time.Duration) int64

type metricCategory int

const (
	untimedMetric metricCategory = iota
	forwardedMetric
	timedMetric
	passthroughMetric
)

func (c metricCategory) TimestampNanosFn() timestampNanosFn {
	switch c {
	case untimedMetric:
		return func(windowStartAtNanos int64, resolution time.Duration) int64 {
			return windowStartAtNanos + resolution.Nanoseconds()
		}
	case forwardedMetric:
		return func(windowStartAtNanos int64, _ time.Duration) int64 {
			return windowStartAtNanos
		}
	case timedMetric:
		return func(windowStartAtNanos int64, resolution time.Duration) int64 {
			return windowStartAtNanos + resolution.Nanoseconds()
		}
	case passthroughMetric:
		return func(windowStartAtNanos int64, _ time.Duration) int64 {
			return windowStartAtNanos
		}
	default:
		panic(fmt.Errorf("unknown category type: %v", c))
	}
}

type metricUnion struct {
	category    metricCategory
	untimed     unaggregated.MetricUnion
	forwarded   aggregated.ForwardedMetric
	timed       aggregated.Metric
	passthrough aggregated.Metric
}

func (mu metricUnion) Type() metric.Type {
	switch mu.category {
	case untimedMetric:
		return mu.untimed.Type
	case forwardedMetric:
		return mu.forwarded.Type
	case timedMetric:
		return mu.timed.Type
	case passthroughMetric:
		return mu.passthrough.Type
	default:
		panic(fmt.Errorf("unknown category type: %v", mu.category))
	}
}

func (mu metricUnion) ID() metricid.RawID {
	switch mu.category {
	case untimedMetric:
		return mu.untimed.ID
	case forwardedMetric:
		return mu.forwarded.ID
	case timedMetric:
		return mu.timed.ID
	case passthroughMetric:
		return mu.passthrough.ID
	default:
		panic(fmt.Errorf("unknown category type: %v", mu.category))
	}
}

type metadataType int

const (
	stagedMetadatasType metadataType = iota
	forwardMetadataType
	timedMetadataType
	passthroughMetadataType
)

type metadataFn func(idx int) metadataUnion

type metadataUnion struct {
	mType               metadataType
	stagedMetadatas     metadata.StagedMetadatas
	forwardMetadata     metadata.ForwardMetadata
	timedMetadata       metadata.TimedMetadata
	passthroughMetadata policy.StoragePolicy
}

func (mu metadataUnion) expectedAggregationKeys(
	now time.Time,
	defaultStoragePolicies []policy.StoragePolicy,
) (aggregationKeys, error) {
	switch mu.mType {
	case stagedMetadatasType:
		return computeExpectedAggregationKeysFromStagedMetadatas(now, mu.stagedMetadatas, defaultStoragePolicies)
	case forwardMetadataType:
		return computeExpectedAggregationKeysFromForwardMetadata(mu.forwardMetadata), nil
	case timedMetadataType:
		return computeExpectedAggregationKeysFromTimedMetadata(mu.timedMetadata), nil
	case passthroughMetadataType:
		return computeExpectedAggregationKeysFromPassthroughMetadata(mu.passthroughMetadata), nil
	default:
		return nil, fmt.Errorf("unexpected metadata type: %v", mu.mType)
	}
}

func computeExpectedAggregationKeysFromStagedMetadatas(
	now time.Time,
	metadatas metadata.StagedMetadatas,
	defaultStoragePolices []policy.StoragePolicy,
) (aggregationKeys, error) {
	// Find the staged policy that is currently active.
	nowNanos := now.UnixNano()
	i := len(metadatas) - 1
	for i >= 0 {
		if metadatas[i].CutoverNanos <= nowNanos {
			break
		}
		i--
	}
	if i < 0 {
		return nil, errors.New("no active staged metadata")
	}

	res := make(aggregationKeys, 0, len(metadatas[i].Pipelines))
	for _, pipeline := range metadatas[i].Pipelines {
		storagePolicies := pipeline.StoragePolicies
		if storagePolicies.IsDefault() {
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
	return res, nil
}

func computeExpectedAggregationKeysFromTimedMetadata(
	metadata metadata.TimedMetadata,
) aggregationKeys {
	return aggregationKeys{
		{
			aggregationID: metadata.AggregationID,
			storagePolicy: metadata.StoragePolicy,
		},
	}
}

func computeExpectedAggregationKeysFromPassthroughMetadata(
	metadata policy.StoragePolicy,
) aggregationKeys {
	return aggregationKeys{
		{
			aggregationID: maggregation.DefaultID,
			storagePolicy: metadata,
		},
	}
}

func computeExpectedAggregationKeysFromForwardMetadata(
	metadata metadata.ForwardMetadata,
) aggregationKeys {
	return aggregationKeys{
		{
			aggregationID: metadata.AggregationID,
			storagePolicy: metadata.StoragePolicy,
			pipeline:      metadata.Pipeline,
		},
	}
}

type metricWithMetadataUnion struct {
	metric   metricUnion
	metadata metadataUnion
}

type testData struct {
	timestamp           time.Time
	metricWithMetadatas []metricWithMetadataUnion
}

type testDataset []testData

type (
	counterValueGenFn func(intervalIdx, idIdx int) int64
	timerValueGenFn   func(intervalIdx, idIdx int) []float64
	gaugeValueGenFn   func(intervalIdx, idIdx int) float64
)

func defaultCounterValueGenFn(intervalIdx, _ int) int64 {
	testCounterVal := int64(123)
	return testCounterVal + int64(intervalIdx)
}

func defaultTimerValueGenFn(intervalIdx, _ int) []float64 {
	testBatchTimerVals := []float64{1.5, 2.5, 3.5, 4.5, 5.5}
	vals := make([]float64, len(testBatchTimerVals))
	for idx, v := range testBatchTimerVals {
		vals[idx] = v + float64(intervalIdx)
	}
	return vals
}

func defaultGaugeValueGenFn(intervalIdx, _ int) float64 {
	testGaugeVal := 456.789
	return testGaugeVal + float64(intervalIdx)
}

type untimedValueGenOpts struct {
	counterValueGenFn counterValueGenFn
	timerValueGenFn   timerValueGenFn
	gaugeValueGenFn   gaugeValueGenFn
}

var defaultUntimedValueGenOpts = untimedValueGenOpts{
	counterValueGenFn: defaultCounterValueGenFn,
	timerValueGenFn:   defaultTimerValueGenFn,
	gaugeValueGenFn:   defaultGaugeValueGenFn,
}

type timedValueGenFn func(intervalIdx, idIdx int) float64

func defaultTimedValueGenFn(intervalIdx, _ int) float64 {
	testVal := 456.789
	return testVal + float64(intervalIdx)
}

type timedValueGenOpts struct {
	timedValueGenFn timedValueGenFn
}

var defaultTimedValueGenOpts = timedValueGenOpts{
	timedValueGenFn: defaultTimedValueGenFn,
}

type passthroughValueGenFn func(intervalIdx, idIdx int) float64

func defaultPassthroughValueGenFn(intervalIdx, _ int) float64 {
	testVal := 123.456
	return testVal + float64(intervalIdx)
}

type passthroughValueGenOpts struct {
	passthroughValueGenFn passthroughValueGenFn
}

var defaultPassthroughValueGenOpts = passthroughValueGenOpts{
	passthroughValueGenFn: defaultPassthroughValueGenFn,
}

type forwardedValueGenFn func(intervalIdx, idIdx int) []float64

func defaultForwardedValueGenFn(intervalIdx, _ int) []float64 {
	testForwardedVals := []float64{1.2, 3.4, 5.6}
	vals := make([]float64, len(testForwardedVals))
	for idx, v := range testForwardedVals {
		vals[idx] = v + float64(intervalIdx)
	}
	return vals
}

type forwardedValueGenOpts struct {
	forwardedValueGenFn forwardedValueGenFn
}

var defaultForwardedValueGenOpts = forwardedValueGenOpts{
	forwardedValueGenFn: defaultForwardedValueGenFn,
}

type valueGenOpts struct {
	untimed     untimedValueGenOpts
	timed       timedValueGenOpts
	forwarded   forwardedValueGenOpts
	passthrough passthroughValueGenOpts
}

var defaultValueGenOpts = valueGenOpts{
	untimed:     defaultUntimedValueGenOpts,
	timed:       defaultTimedValueGenOpts,
	forwarded:   defaultForwardedValueGenOpts,
	passthrough: defaultPassthroughValueGenOpts,
}

type datasetGenOpts struct {
	start        time.Time
	stop         time.Time
	interval     time.Duration
	ids          []string
	category     metricCategory
	typeFn       metricTypeFn
	valueGenOpts valueGenOpts
	metadataFn   metadataFn
}
