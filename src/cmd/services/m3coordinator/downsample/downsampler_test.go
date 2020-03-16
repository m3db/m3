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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/client"
	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules"
	ruleskv "github.com/m3db/m3/src/metrics/rules/store/kv"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
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
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		autoMappingRules: []AutoMappingRule{
			{
				Aggregations: []aggregation.Type{testAggregationType},
				Policies:     testAggregationStoragePolicies,
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesStore(t *testing.T) {
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{})
	rulesStore := testDownsampler.rulesStore

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

	logger := testDownsampler.instrumentOpts.Logger().
		With(zap.String("test", t.Name()))

	// Wait for mapping rule to appear
	logger.Info("waiting for mapping rules to propagate")
	matcher := testDownsampler.matcher
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

func TestDownsamplerAggregationWithRulesConfigMappingRules(t *testing.T) {
	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			nameTag: "foo_metric",
			"app":   "nginx_edge",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 15}, {value: 10}, {value: 30}, {value: 5}, {value: 0},
		},
	}
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "app:nginx*",
					Aggregations: []aggregation.Type{aggregation.Max},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 5 * time.Second,
							Retention:  30 * 24 * time.Hour,
						},
					},
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags:  gaugeMetric.tags,
					value: 30,
					attributes: &storage.Attributes{
						MetricsType: storage.AggregatedMetricsType,
						Resolution:  5 * time.Second,
						Retention:   30 * 24 * time.Hour,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigMappingRulesPartialReplaceAutoMappingRule(t *testing.T) {
	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			nameTag: "foo_metric",
			"app":   "nginx_edge",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 15}, {value: 10}, {value: 30}, {value: 5}, {value: 0},
		},
	}
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		autoMappingRules: []AutoMappingRule{
			{
				Aggregations: []aggregation.Type{aggregation.Sum},
				Policies: policy.StoragePolicies{
					policy.MustParseStoragePolicy("2s:24h"),
					policy.MustParseStoragePolicy("4s:48h"),
				},
			},
		},
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "app:nginx*",
					Aggregations: []aggregation.Type{aggregation.Max},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 2 * time.Second,
							Retention:  24 * time.Hour,
						},
					},
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				// Expect the max to be used and override the default auto
				// mapping rule for the storage policy 2s:24h.
				{
					tags:  gaugeMetric.tags,
					value: 30,
					attributes: &storage.Attributes{
						MetricsType: storage.AggregatedMetricsType,
						Resolution:  2 * time.Second,
						Retention:   24 * time.Hour,
					},
				},
				// Expect the sum to still be used for the storage
				// policy 4s:48h.
				{
					tags:  gaugeMetric.tags,
					value: 60,
					attributes: &storage.Attributes{
						MetricsType: storage.AggregatedMetricsType,
						Resolution:  4 * time.Second,
						Retention:   48 * time.Hour,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigMappingRulesReplaceAutoMappingRule(t *testing.T) {
	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			nameTag: "foo_metric",
			"app":   "nginx_edge",
		},
		timedSamples: []testGaugeMetricTimedSample{
			{value: 15}, {value: 10}, {value: 30}, {value: 5}, {value: 0},
		},
	}
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		autoMappingRules: []AutoMappingRule{
			{
				Aggregations: []aggregation.Type{aggregation.Sum},
				Policies: policy.StoragePolicies{
					policy.MustParseStoragePolicy("2s:24h"),
				},
			},
		},
		rulesConfig: &RulesConfiguration{
			MappingRules: []MappingRuleConfiguration{
				{
					Filter:       "app:nginx*",
					Aggregations: []aggregation.Type{aggregation.Max},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: 2 * time.Second,
							Retention:  24 * time.Hour,
						},
					},
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				// Expect the max to be used and override the default auto
				// mapping rule for the storage policy 2s:24h.
				{
					tags:  gaugeMetric.tags,
					value: 30,
					attributes: &storage.Attributes{
						MetricsType: storage.AggregatedMetricsType,
						Resolution:  2 * time.Second,
						Retention:   24 * time.Hour,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRulesConfigRollupRules(t *testing.T) {
	gaugeMetric := testGaugeMetric{
		tags: map[string]string{
			nameTag:         "http_requests",
			"app":           "nginx_edge",
			"status_code":   "500",
			"endpoint":      "/foo/bar",
			"not_rolled_up": "not_rolled_up_value",
		},
		samples: []float64{42, 64},
		// TODO: Make rollup rules work with timestamped samples (like below)
		// instead of only with untimed samples (this requires being able to
		// write staged metadatas instead of a single storage policy for a
		// timed metric).
		// timedSamples: []testGaugeMetricTimedSample{
		// 	{value: 42}, {value: 64},
		// },
	}
	res := 5 * time.Second
	ret := 30 * 24 * time.Hour
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		rulesConfig: &RulesConfiguration{
			RollupRules: []RollupRuleConfiguration{
				{
					Filter: fmt.Sprintf(
						"%s:http_requests app:* status_code:* endpoint:*",
						nameTag),
					Transforms: []TransformConfiguration{
						// TODO: make multi-stage rollup rules work, for some reason
						// when multiple transforms applied the HasRollup detection
						// fails and hence metric is not forwarded for second stage
						// aggregation.
						// {
						// 	Transform: &TransformOperationConfiguration{
						// 		Type: transformation.PerSecond,
						// 	},
						// },
						{
							Rollup: &RollupOperationConfiguration{
								MetricName:   "http_requests_by_status_code",
								GroupBy:      []string{"app", "status_code", "endpoint"},
								Aggregations: []aggregation.Type{aggregation.Sum},
							},
						},
					},
					StoragePolicies: []StoragePolicyConfiguration{
						{
							Resolution: res,
							Retention:  ret,
						},
					},
				},
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			gaugeMetrics: []testGaugeMetric{gaugeMetric},
		},
		expect: &testDownsamplerOptionsExpect{
			writes: []testExpectedWrite{
				{
					tags: map[string]string{
						nameTag:               "http_requests_by_status_code",
						string(rollupTagName): string(rollupTagValue),
						"app":                 "nginx_edge",
						"status_code":         "500",
						"endpoint":            "/foo/bar",
					},
					value: 106,
					attributes: &storage.Attributes{
						MetricsType: storage.AggregatedMetricsType,
						Resolution:  res,
						Retention:   ret,
					},
				},
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithTimedSamples(t *testing.T) {
	counterMetrics, counterMetricsExpect := testCounterMetrics(testCounterMetricsOptions{
		timedSamples: true,
	})
	gaugeMetrics, gaugeMetricsExpect := testGaugeMetrics(testGaugeMetricsOptions{
		timedSamples: true,
	})
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		autoMappingRules: []AutoMappingRule{
			{
				Aggregations: []aggregation.Type{testAggregationType},
				Policies:     testAggregationStoragePolicies,
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			counterMetrics: counterMetrics,
			gaugeMetrics:   gaugeMetrics,
		},
		expect: &testDownsamplerOptionsExpect{
			writes: append(counterMetricsExpect, gaugeMetricsExpect...),
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithOverrideRules(t *testing.T) {
	counterMetrics, counterMetricsExpect := testCounterMetrics(testCounterMetricsOptions{})
	counterMetricsExpect[0].value = 2

	gaugeMetrics, gaugeMetricsExpect := testGaugeMetrics(testGaugeMetricsOptions{})
	gaugeMetricsExpect[0].value = 5

	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		sampleAppenderOpts: &SampleAppenderOptions{
			Override: true,
			OverrideRules: SamplesAppenderOverrideRules{
				MappingRules: []AutoMappingRule{
					{
						Aggregations: []aggregation.Type{aggregation.Mean},
						Policies: []policy.StoragePolicy{
							policy.MustParseStoragePolicy("4s:1d"),
						},
					},
				},
			},
		},
		autoMappingRules: []AutoMappingRule{
			{
				Aggregations: []aggregation.Type{testAggregationType},
				Policies:     testAggregationStoragePolicies,
			},
		},
		ingest: &testDownsamplerOptionsIngest{
			counterMetrics: counterMetrics,
			gaugeMetrics:   gaugeMetrics,
		},
		expect: &testDownsamplerOptionsExpect{
			writes: append(counterMetricsExpect, gaugeMetricsExpect...),
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithRemoteAggregatorClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock client
	remoteClientMock := client.NewMockClient(ctrl)
	remoteClientMock.EXPECT().Init().Return(nil)

	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		autoMappingRules: []AutoMappingRule{
			{
				Aggregations: []aggregation.Type{testAggregationType},
				Policies:     testAggregationStoragePolicies,
			},
		},
		remoteClientMock: remoteClientMock,
	})

	// Test expected output
	testDownsamplerRemoteAggregation(t, testDownsampler)
}

type testExpectedWrite struct {
	tags       map[string]string
	value      float64
	attributes *storage.Attributes
}

type testCounterMetric struct {
	tags         map[string]string
	samples      []int64
	timedSamples []testCounterMetricTimedSample
}

type testCounterMetricTimedSample struct {
	time  time.Time
	value int64
}

type testGaugeMetric struct {
	tags         map[string]string
	samples      []float64
	timedSamples []testGaugeMetricTimedSample
}

type testGaugeMetricTimedSample struct {
	time  time.Time
	value float64
}

type testCounterMetricsOptions struct {
	timedSamples bool
}

func testCounterMetrics(opts testCounterMetricsOptions) (
	[]testCounterMetric,
	[]testExpectedWrite,
) {
	metric := testCounterMetric{
		tags:    map[string]string{nameTag: "counter0", "app": "testapp", "foo": "bar"},
		samples: []int64{1, 2, 3},
	}
	if opts.timedSamples {
		metric.samples = nil
		metric.timedSamples = []testCounterMetricTimedSample{
			{value: 1}, {value: 2}, {value: 3},
		}
	}
	write := testExpectedWrite{
		tags:  metric.tags,
		value: 6,
	}
	return []testCounterMetric{metric}, []testExpectedWrite{write}
}

type testGaugeMetricsOptions struct {
	timedSamples bool
}

func testGaugeMetrics(opts testGaugeMetricsOptions) ([]testGaugeMetric, []testExpectedWrite) {
	metric := testGaugeMetric{
		tags:    map[string]string{nameTag: "gauge0", "app": "testapp", "qux": "qaz"},
		samples: []float64{4, 5, 6},
	}
	if opts.timedSamples {
		metric.samples = nil
		metric.timedSamples = []testGaugeMetricTimedSample{
			{value: 4}, {value: 5}, {value: 6},
		}
	}
	write := testExpectedWrite{
		tags:  metric.tags,
		value: 15,
	}
	return []testGaugeMetric{metric}, []testExpectedWrite{write}
}

func testDownsamplerAggregation(
	t *testing.T,
	testDownsampler testDownsampler,
) {
	testOpts := testDownsampler.testOpts

	logger := testDownsampler.instrumentOpts.Logger().
		With(zap.String("test", t.Name()))

	counterMetrics, counterMetricsExpect := testCounterMetrics(testCounterMetricsOptions{})
	gaugeMetrics, gaugeMetricsExpect := testGaugeMetrics(testGaugeMetricsOptions{})
	expectedWrites := append(counterMetricsExpect, gaugeMetricsExpect...)

	// Allow overrides
	if ingest := testOpts.ingest; ingest != nil {
		counterMetrics = ingest.counterMetrics
		gaugeMetrics = ingest.gaugeMetrics
	}
	if expect := testOpts.expect; expect != nil {
		expectedWrites = expect.writes
	}

	// Ingest points
	testDownsamplerAggregationIngest(t, testDownsampler,
		counterMetrics, gaugeMetrics)

	// Wait for writes
	logger.Info("wait for test metrics to appear")
	logWritesAccumulated := os.Getenv("TEST_LOG_WRITES_ACCUMULATED") == "true"
	logWritesAccumulatedTicker := time.NewTicker(time.Second)
CheckAllWritesArrivedLoop:
	for {
		writes := testDownsampler.storage.Writes()
		if logWritesAccumulated {
			select {
			case <-logWritesAccumulatedTicker.C:
				logger.Info("logging accmulated writes",
					zap.Int("numWrites", len(writes)))
				for _, write := range writes {
					logger.Info("accumulated write",
						zap.ByteString("tags", write.Tags.ID()),
						zap.Any("datapoints", write.Datapoints))
				}
			default:
			}
		}

		for _, expectedWrite := range expectedWrites {
			name := expectedWrite.tags[nameTag]
			_, ok := findWrite(t, writes, name, expectedWrite.attributes)
			if !ok {
				time.Sleep(100 * time.Millisecond)
				continue CheckAllWritesArrivedLoop
			}
		}
		break
	}

	// Verify writes
	logger.Info("verify test metrics")
	writes := testDownsampler.storage.Writes()
	if logWritesAccumulated {
		logger.Info("logging accmulated writes to verify",
			zap.Int("numWrites", len(writes)))
		for _, write := range writes {
			logger.Info("accumulated write",
				zap.ByteString("tags", write.Tags.ID()),
				zap.Any("datapoints", write.Datapoints))
		}
	}

	for _, expectedWrite := range expectedWrites {
		name := expectedWrite.tags[nameTag]
		value := expectedWrite.value

		write, found := findWrite(t, writes, name, expectedWrite.attributes)
		require.True(t, found)
		assert.Equal(t, expectedWrite.tags, tagsToStringMap(write.Tags))
		require.Equal(t, 1, len(write.Datapoints))
		assert.Equal(t, float64(value), write.Datapoints[0].Value)

		if attrs := expectedWrite.attributes; attrs != nil {
			assert.Equal(t, *attrs, write.Attributes)
		}
	}
}

func testDownsamplerRemoteAggregation(
	t *testing.T,
	testDownsampler testDownsampler,
) {
	testOpts := testDownsampler.testOpts

	expectTestCounterMetrics, _ := testCounterMetrics(testCounterMetricsOptions{})
	testCounterMetrics, _ := testCounterMetrics(testCounterMetricsOptions{})

	expectTestGaugeMetrics, _ := testGaugeMetrics(testGaugeMetricsOptions{})
	testGaugeMetrics, _ := testGaugeMetrics(testGaugeMetricsOptions{})

	remoteClientMock := testOpts.remoteClientMock
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
	testDownsampler testDownsampler,
	testCounterMetrics []testCounterMetric,
	testGaugeMetrics []testGaugeMetric,
) {
	downsampler := testDownsampler.downsampler

	testOpts := testDownsampler.testOpts

	logger := testDownsampler.instrumentOpts.Logger().
		With(zap.String("test", t.Name()))

	logger.Info("write test metrics")
	appender, err := downsampler.NewMetricsAppender()
	require.NoError(t, err)
	defer appender.Finalize()

	var opts SampleAppenderOptions
	if testOpts.sampleAppenderOpts != nil {
		opts = *testOpts.sampleAppenderOpts
	}
	for _, metric := range testCounterMetrics {
		appender.Reset()
		for name, value := range metric.tags {
			appender.AddTag([]byte(name), []byte(value))
		}

		samplesAppender, err := appender.SamplesAppender(opts)
		require.NoError(t, err)

		for _, sample := range metric.samples {
			err = samplesAppender.AppendCounterSample(sample)
			require.NoError(t, err)
		}
		for _, sample := range metric.timedSamples {
			if sample.time.Equal(time.Time{}) {
				sample.time = time.Now() // Allow empty time to mean "now"
			}
			err = samplesAppender.AppendCounterTimedSample(sample.time, sample.value)
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
			err = samplesAppender.AppendGaugeSample(sample)
			require.NoError(t, err)
		}
		for _, sample := range metric.timedSamples {
			if sample.time.Equal(time.Time{}) {
				sample.time = time.Now() // Allow empty time to mean "now"
			}
			err = samplesAppender.AppendGaugeTimedSample(sample.time, sample.value)
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

type testDownsampler struct {
	opts           DownsamplerOptions
	testOpts       testDownsamplerOptions
	downsampler    Downsampler
	matcher        matcher.Matcher
	storage        mock.Storage
	rulesStore     rules.Store
	instrumentOpts instrument.Options
}

type testDownsamplerOptions struct {
	clockOpts      clock.Options
	instrumentOpts instrument.Options

	// Options for the test
	autoMappingRules   []AutoMappingRule
	sampleAppenderOpts *SampleAppenderOptions
	remoteClientMock   *client.MockClient
	rulesConfig        *RulesConfiguration

	// Test ingest and expectations overrides
	ingest *testDownsamplerOptionsIngest
	expect *testDownsamplerOptionsExpect
}

type testDownsamplerOptionsIngest struct {
	counterMetrics []testCounterMetric
	gaugeMetrics   []testGaugeMetric
}

type testDownsamplerOptionsExpect struct {
	writes []testExpectedWrite
}

func newTestDownsampler(t *testing.T, opts testDownsamplerOptions) testDownsampler {
	storage := mock.NewMockStorage()
	rulesKVStore := mem.NewStore()

	clockOpts := clock.NewOptions()
	if opts.clockOpts != nil {
		clockOpts = opts.clockOpts
	}

	// Use a test instrument options by default to get the debug logs on by default.
	instrumentOpts := instrument.NewTestOptions(t)
	if opts.instrumentOpts != nil {
		instrumentOpts = opts.instrumentOpts
	}

	matcherOpts := matcher.NewOptions()

	// Initialize the namespaces
	_, err := rulesKVStore.Set(matcherOpts.NamespacesKey(), &rulepb.Namespaces{})
	require.NoError(t, err)

	rulesetKeyFmt := matcherOpts.RuleSetKeyFn()([]byte("%s"))
	rulesStoreOpts := ruleskv.NewStoreOptions(matcherOpts.NamespacesKey(),
		rulesetKeyFmt, nil)
	rulesStore := ruleskv.NewStore(rulesKVStore, rulesStoreOpts)

	tagEncoderOptions := serialize.NewTagEncoderOptions()
	tagDecoderOptions := serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{})
	tagEncoderPoolOptions := pool.NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("tag-encoder-pool")))
	tagDecoderPoolOptions := pool.NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("tag-decoder-pool")))

	var cfg Configuration
	if opts.remoteClientMock != nil {
		// Optionally set an override to use remote aggregation
		// with a mock client
		cfg.RemoteAggregator = &RemoteAggregatorConfiguration{
			clientOverride: opts.remoteClientMock,
		}
	}
	if opts.rulesConfig != nil {
		cfg.Rules = opts.rulesConfig
	}

	instance, err := cfg.NewDownsampler(DownsamplerOptions{
		Storage:               storage,
		ClusterClient:         clusterclient.NewMockClient(gomock.NewController(t)),
		RulesKVStore:          rulesKVStore,
		AutoMappingRules:      opts.autoMappingRules,
		ClockOptions:          clockOpts,
		InstrumentOptions:     instrumentOpts,
		TagEncoderOptions:     tagEncoderOptions,
		TagDecoderOptions:     tagDecoderOptions,
		TagEncoderPoolOptions: tagEncoderPoolOptions,
		TagDecoderPoolOptions: tagDecoderPoolOptions,
	})
	require.NoError(t, err)

	downcast, ok := instance.(*downsampler)
	require.True(t, ok)

	return testDownsampler{
		opts:           downcast.opts,
		testOpts:       opts,
		downsampler:    instance,
		matcher:        downcast.agg.matcher,
		storage:        storage,
		rulesStore:     rulesStore,
		instrumentOpts: instrumentOpts,
	}
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

	size := 1
	tagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{
			CheckBytesWrapperPoolSize: &size,
		}),
		pool.NewObjectPoolOptions().SetSize(size))
	tagDecoderPool.Init()

	tagDecoder := tagDecoderPool.Get()

	iter := serialize.NewMetricTagsIterator(tagDecoder, nil)
	iter.Reset(data.Bytes())
	return iter
}

func findWrite(
	t *testing.T,
	writes []*storage.WriteQuery,
	name string,
	optionalMatchAttrs *storage.Attributes,
) (*storage.WriteQuery, bool) {
	for _, w := range writes {
		if t, ok := w.Tags.Get([]byte(nameTag)); ok {
			if !bytes.Equal(t, []byte(name)) {
				// Does not match name.
				continue
			}
			if optionalMatchAttrs != nil && w.Attributes != *optionalMatchAttrs {
				// Tried to match attributes and not matched.
				continue
			}
			// Matches name and all optional lookups.
			return w, true
		}
	}
	return nil, false
}

func testUpdateMetadata() rules.UpdateMetadata {
	return rules.NewRuleSetUpdateHelper(0).NewUpdateMetadata(time.Now().UnixNano(), "test")
}
