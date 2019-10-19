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

package downsample

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	testAggregationType            = aggregation.Sum
	testAggregationStoragePolicies = []policy.StoragePolicy{
		policy.MustParseStoragePolicy("2s:1d"),
	}
)

const (
	nameTag = "__name__"
)

func TestDownsamplerAggregationWithAutoMappingRules(t *testing.T) {
	testDownsampler, err := NewTestDownsampler(TestDownsamplerOptions{
		AutoMappingRules: []MappingRule{
			{
				Aggregations: []aggregation.Type{testAggregationType},
				Policies:     testAggregationStoragePolicies,
			},
		},
	})
	require.NoError(t, err)

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesStore(t *testing.T) {
	testDownsampler, err := NewTestDownsampler(TestDownsamplerOptions{})
	require.NoError(t, err)

	rulesStore := testDownsampler.RulesStore

	// Create rules
	nss, err := rulesStore.ReadNamespaces()
	require.NoError(t, err)
	_, err = nss.AddNamespace("default", testUpdateMetadata())
	require.NoError(t, err)

	rule := view.MappingRule{
		ID:              "mappingrule",
		Name:            "mappingrule",
		Filter:          "app:test*",
		AggregationID:   aggregation.MustCompressTypes(testAggregationType),
		StoragePolicies: testAggregationStoragePolicies,
	}

	rs := rules.NewEmptyRuleSet("default", testUpdateMetadata())
	_, err = rs.AddMappingRule(rule, testUpdateMetadata())
	require.NoError(t, err)

	err = rulesStore.WriteAll(nss, rs)
	require.NoError(t, err)

	logger := testDownsampler.InstrumentOpts.Logger().
		With(zap.String("test", t.Name()))

	// Wait for mapping rule to appear
	logger.Info("waiting for mapping rules to propagate")
	matcher := testDownsampler.Matcher
	testMatchID := newTestID(t, map[string]string{
		"__name__": "foo",
		"app":      "test123",
	})
	for {
		now := time.Now().UnixNano()
		res := matcher.ForwardMatch(testMatchID, now, now+1)
		results := res.ForExistingIDAt(now)
		if !results.IsDefault() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithTimedSamples(t *testing.T) {
	testDownsampler, err := NewTestDownsampler(TestDownsamplerOptions{
		TimedSamples: true,
		AutoMappingRules: []MappingRule{
			{
				Aggregations: []aggregation.Type{testAggregationType},
				Policies:     testAggregationStoragePolicies,
			},
		},
	})
	require.NoError(t, err)

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithOverrideRules(t *testing.T) {
	testDownsampler, err := NewTestDownsampler(TestDownsamplerOptions{
		SampleAppenderOpts: &SampleAppenderOptions{
			Override: true,
			OverrideRules: SamplesAppenderOverrideRules{
				MappingRules: []MappingRule{
					{
						Aggregations: []aggregation.Type{aggregation.Mean},
						Policies: []policy.StoragePolicy{
							policy.MustParseStoragePolicy("4s:1d"),
						},
					},
				},
			},
		},
		ExpectedAdjusted: map[string]float64{
			"gauge0":   5.0,
			"counter0": 2.0,
		},
		AutoMappingRules: []MappingRule{
			{
				Aggregations: []aggregation.Type{testAggregationType},
				Policies:     testAggregationStoragePolicies,
			},
		},
	})
	require.NoError(t, err)

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRemoteAggregatorClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock client
	remoteClientMock := client.NewMockClient(ctrl)
	remoteClientMock.EXPECT().Init().Return(nil)

	testDownsampler, err := NewTestDownsampler(TestDownsamplerOptions{
		AutoMappingRules: []MappingRule{
			{
				Aggregations: []aggregation.Type{testAggregationType},
				Policies:     testAggregationStoragePolicies,
			},
		},
		RemoteClientMock: remoteClientMock,
	})
	require.NoError(t, err)

	// Test expected output
	testDownsamplerRemoteAggregation(t, testDownsampler)
}

type testCounterMetric struct {
	tags     map[string]string
	samples  []int64
	expected int64
}

type testGaugeMetric struct {
	tags     map[string]string
	samples  []float64
	expected float64
}

func testCounterMetrics() []testCounterMetric {
	return []testCounterMetric{
		{
			tags:     map[string]string{nameTag: "counter0", "app": "testapp", "foo": "bar"},
			samples:  []int64{1, 2, 3},
			expected: 6,
		},
	}
}

func testGaugeMetrics() []testGaugeMetric {
	return []testGaugeMetric{
		{
			tags:     map[string]string{nameTag: "gauge0", "app": "testapp", "qux": "qaz"},
			samples:  []float64{4, 5, 6},
			expected: 15,
		},
	}
}

func testDownsamplerAggregation(
	t *testing.T,
	testDownsampler TestDownsampler,
) {
	testOpts := testDownsampler.TestOpts

	logger := testDownsampler.InstrumentOpts.Logger().
		With(zap.String("test", t.Name()))

	testCounterMetrics := testCounterMetrics()
	testGaugeMetrics := testGaugeMetrics()

	// Ingest points
	testDownsamplerAggregationIngest(t, testDownsampler,
		testCounterMetrics, testGaugeMetrics)

	// Wait for writes
	logger.Info("wait for test metrics to appear")
	for {
		writes := testDownsampler.Storage.Writes()
		if len(writes) == len(testCounterMetrics)+len(testGaugeMetrics) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify writes
	logger.Info("verify test metrics")
	writes := testDownsampler.Storage.Writes()
	for _, metric := range testCounterMetrics {
		name := metric.tags["__name__"]
		expected := metric.expected
		if v, ok := testOpts.ExpectedAdjusted[name]; ok {
			expected = int64(v)
		}

		write := mustFindWrite(t, writes, name)
		assert.Equal(t, metric.tags, tagsToStringMap(write.Tags))
		require.Equal(t, 1, len(write.Datapoints))
		assert.Equal(t, float64(expected), write.Datapoints[0].Value)
	}
	for _, metric := range testGaugeMetrics {
		name := metric.tags["__name__"]
		expected := metric.expected
		if v, ok := testOpts.ExpectedAdjusted[name]; ok {
			expected = v
		}

		write := mustFindWrite(t, writes, name)
		assert.Equal(t, metric.tags, tagsToStringMap(write.Tags))
		require.Equal(t, 1, len(write.Datapoints))
		assert.Equal(t, float64(expected), write.Datapoints[0].Value)
	}
}

func testDownsamplerRemoteAggregation(
	t *testing.T,
	testDownsampler TestDownsampler,
) {
	testOpts := testDownsampler.TestOpts

	testCounterMetrics, expectTestCounterMetrics := testCounterMetrics(), testCounterMetrics()
	testGaugeMetrics, expectTestGaugeMetrics := testGaugeMetrics(), testGaugeMetrics()

	remoteClientMock := testOpts.RemoteClientMock
	require.NotNil(t, remoteClientMock)

	// Expect ingestion
	checkedCounterSamples := 0
	remoteClientMock.EXPECT().
		WriteUntimedCounter(gomock.Any(), gomock.Any()).
		AnyTimes().
		Do(func(counter unaggregated.Counter,
			metadatas metadata.StagedMetadatas,
		) error {
			for _, c := range expectTestCounterMetrics {
				if !strings.Contains(counter.ID.String(), c.tags[nameTag]) {
					continue
				}

				var remainingSamples []int64
				found := false
				for _, s := range c.samples {
					if !found && s == counter.Value {
						found = true
					} else {
						remainingSamples = append(remainingSamples, s)
					}
				}
				c.samples = remainingSamples
				if found {
					checkedCounterSamples++
				}

				break
			}

			return nil
		})

	checkedGaugeSamples := 0
	remoteClientMock.EXPECT().
		WriteUntimedGauge(gomock.Any(), gomock.Any()).
		AnyTimes().
		Do(func(gauge unaggregated.Gauge,
			metadatas metadata.StagedMetadatas,
		) error {
			for _, g := range expectTestGaugeMetrics {
				if !strings.Contains(gauge.ID.String(), g.tags[nameTag]) {
					continue
				}

				var remainingSamples []float64
				found := false
				for _, s := range g.samples {
					if !found && s == gauge.Value {
						found = true
					} else {
						remainingSamples = append(remainingSamples, s)
					}
				}
				g.samples = remainingSamples
				if found {
					checkedGaugeSamples++
				}

				break
			}

			return nil
		})

	// Ingest points
	testDownsamplerAggregationIngest(t, testDownsampler,
		testCounterMetrics, testGaugeMetrics)

	// Ensure we checked counters and gauges
	samplesCounters := 0
	for _, c := range testCounterMetrics {
		samplesCounters += len(c.samples)
	}
	samplesGauges := 0
	for _, c := range testGaugeMetrics {
		samplesGauges += len(c.samples)
	}
	require.Equal(t, samplesCounters, checkedCounterSamples)
	require.Equal(t, samplesGauges, checkedGaugeSamples)
}

func testDownsamplerAggregationIngest(
	t *testing.T,
	testDownsampler TestDownsampler,
	testCounterMetrics []testCounterMetric,
	testGaugeMetrics []testGaugeMetric,
) {
	downsampler := testDownsampler.Downsampler

	testOpts := testDownsampler.TestOpts

	logger := testDownsampler.InstrumentOpts.Logger().
		With(zap.String("test", t.Name()))

	logger.Info("write test metrics")
	appender, err := downsampler.NewMetricsAppender()
	require.NoError(t, err)
	defer appender.Finalize()

	var opts SampleAppenderOptions
	if testOpts.SampleAppenderOpts != nil {
		opts = *testOpts.SampleAppenderOpts
	}
	for _, metric := range testCounterMetrics {
		appender.Reset()
		for name, value := range metric.tags {
			appender.AddTag([]byte(name), []byte(value))
		}

		samplesAppender, err := appender.SamplesAppender(opts)
		require.NoError(t, err)

		for _, sample := range metric.samples {
			if testOpts.TimedSamples {
				err = samplesAppender.AppendCounterTimedSample(time.Now(), sample)
			} else {
				err = samplesAppender.AppendCounterSample(sample)
			}
			require.NoError(t, err)
		}
	}
	for _, metric := range testGaugeMetrics {
		appender.Reset()
		for name, value := range metric.tags {
			appender.AddTag([]byte(name), []byte(value))
		}

		samplesAppender, err := appender.SamplesAppender(opts)
		require.NoError(t, err)

		for _, sample := range metric.samples {
			if testOpts.TimedSamples {
				err = samplesAppender.AppendGaugeTimedSample(time.Now(), sample)
			} else {
				err = samplesAppender.AppendGaugeSample(sample)
			}
			require.NoError(t, err)
		}
	}
}

func tagsToStringMap(tags models.Tags) map[string]string {
	stringMap := make(map[string]string, tags.Len())
	for _, t := range tags.Tags {
		stringMap[string(t.Name)] = string(t.Value)
	}

	return stringMap
}

func newTestID(t *testing.T, tags map[string]string) id.ID {
	tagEncoderPool := serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(),
		pool.NewObjectPoolOptions().SetSize(1))
	tagEncoderPool.Init()

	tagsIter := newTags()
	for name, value := range tags {
		tagsIter.append([]byte(name), []byte(value))
	}

	tagEncoder := tagEncoderPool.Get()
	err := tagEncoder.Encode(tagsIter)
	require.NoError(t, err)

	data, ok := tagEncoder.Data()
	require.True(t, ok)

	tagDecoderPool := serialize.NewTagDecoderPool(serialize.NewTagDecoderOptions(),
		pool.NewObjectPoolOptions().SetSize(1))
	tagDecoderPool.Init()

	tagDecoder := tagDecoderPool.Get()

	iter := serialize.NewMetricTagsIterator(tagDecoder, nil)
	iter.Reset(data.Bytes())
	return iter
}

func mustFindWrite(t *testing.T, writes []*storage.WriteQuery, name string) *storage.WriteQuery {
	var write *storage.WriteQuery
	for _, w := range writes {
		if t, ok := w.Tags.Get([]byte("__name__")); ok {
			if bytes.Equal(t, []byte(name)) {
				write = w
				break
			}
		}
	}

	require.NotNil(t, write)
	return write
}

func testUpdateMetadata() rules.UpdateMetadata {
	return rules.NewRuleSetUpdateHelper(0).NewUpdateMetadata(time.Now().UnixNano(), "test")
}
