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
	"testing"
	"time"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules"
	ruleskv "github.com/m3db/m3/src/metrics/rules/store/kv"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testAggregationType            = aggregation.Sum
	testAggregationStoragePolicies = []policy.StoragePolicy{
		policy.MustParseStoragePolicy("2s:1d"),
	}
)

func TestDownsamplerAggregationWithAutoMappingRules(t *testing.T) {
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		autoMappingRules: []MappingRule{
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
		WithFields(xlog.NewField("test", t.Name()))

	// Wait for mapping rule to appear
	logger.Infof("waiting for mapping rules to propagate")
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

func TestDownsamplerAggregationWithTimedSamples(t *testing.T) {
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		timedSamples: true,
		autoMappingRules: []MappingRule{
			{
				Aggregations: []aggregation.Type{testAggregationType},
				Policies:     testAggregationStoragePolicies,
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func TestDownsamplerAggregationWithOverrideRules(t *testing.T) {
	testDownsampler := newTestDownsampler(t, testDownsamplerOptions{
		sampleAppenderOpts: &SampleAppenderOptions{
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
		expectedAdjusted: map[string]float64{
			"gauge0":   5.0,
			"counter0": 2.0,
		},
		autoMappingRules: []MappingRule{
			{
				Aggregations: []aggregation.Type{testAggregationType},
				Policies:     testAggregationStoragePolicies,
			},
		},
	})

	// Test expected output
	testDownsamplerAggregation(t, testDownsampler)
}

func testDownsamplerAggregation(
	t *testing.T,
	testDownsampler testDownsampler,
) {
	downsampler := testDownsampler.downsampler

	testOpts := testDownsampler.testOpts

	logger := testDownsampler.instrumentOpts.Logger().
		WithFields(xlog.NewField("test", t.Name()))

	testCounterMetrics := []struct {
		tags     map[string]string
		samples  []int64
		expected int64
	}{
		{
			tags:     map[string]string{"__name__": "counter0", "app": "testapp", "foo": "bar"},
			samples:  []int64{1, 2, 3},
			expected: 6,
		},
	}

	testGaugeMetrics := []struct {
		tags     map[string]string
		samples  []float64
		expected float64
	}{
		{
			tags:     map[string]string{"__name__": "gauge0", "app": "testapp", "qux": "qaz"},
			samples:  []float64{4, 5, 6},
			expected: 15,
		},
	}

	logger.Infof("write test metrics")
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
			if testOpts.timedSamples {
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
			if testOpts.timedSamples {
				err = samplesAppender.AppendGaugeTimedSample(time.Now(), sample)
			} else {
				err = samplesAppender.AppendGaugeSample(sample)
			}
			require.NoError(t, err)
		}
	}

	// Wait for writes
	logger.Infof("wait for test metrics to appear")
	for {
		writes := testDownsampler.storage.Writes()
		if len(writes) == len(testCounterMetrics)+len(testGaugeMetrics) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify writes
	logger.Infof("verify test metrics")
	writes := testDownsampler.storage.Writes()
	for _, metric := range testCounterMetrics {
		name := metric.tags["__name__"]
		expected := metric.expected
		if v, ok := testOpts.expectedAdjusted[name]; ok {
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
		if v, ok := testOpts.expectedAdjusted[name]; ok {
			expected = v
		}

		write := mustFindWrite(t, writes, name)
		assert.Equal(t, metric.tags, tagsToStringMap(write.Tags))
		require.Equal(t, 1, len(write.Datapoints))
		assert.Equal(t, float64(expected), write.Datapoints[0].Value)
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
	autoMappingRules   []MappingRule
	timedSamples       bool
	sampleAppenderOpts *SampleAppenderOptions

	// Expected values overrides
	expectedAdjusted map[string]float64
}

func newTestDownsampler(t *testing.T, opts testDownsamplerOptions) testDownsampler {
	storage := mock.NewMockStorage()
	rulesKVStore := mem.NewStore()

	clockOpts := clock.NewOptions()
	if opts.clockOpts != nil {
		clockOpts = opts.clockOpts
	}

	instrumentOpts := instrument.NewOptions()
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
	tagDecoderOptions := serialize.NewTagDecoderOptions()
	tagEncoderPoolOptions := pool.NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("tag-encoder-pool")))
	tagDecoderPoolOptions := pool.NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("tag-decoder-pool")))

	var cfg Configuration
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
