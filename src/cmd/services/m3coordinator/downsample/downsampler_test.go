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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/serialize"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3ctl/service/r2/store"
	r2kv "github.com/m3db/m3ctl/service/r2/store/kv"
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/generated/proto/rulepb"
	"github.com/m3db/m3metrics/matcher"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
	ruleskv "github.com/m3db/m3metrics/rules/store/kv"
	"github.com/m3db/m3metrics/rules/view"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"

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
	_, err := rulesStore.CreateNamespace("default", store.NewUpdateOptions())
	require.NoError(t, err)

	rule := view.MappingRule{
		ID:              "mappingrule",
		Name:            "mappingrule",
		Filter:          "app:test*",
		AggregationID:   aggregation.MustCompressTypes(testAggregationType),
		StoragePolicies: testAggregationStoragePolicies,
	}
	_, err = rulesStore.CreateMappingRule("default", rule,
		store.NewUpdateOptions())
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

func testDownsamplerAggregation(
	t *testing.T,
	testDownsampler testDownsampler,
) {
	downsampler := testDownsampler.downsampler

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
	appender := downsampler.NewMetricsAppender()
	defer appender.Finalize()

	for _, metric := range testCounterMetrics {
		appender.Reset()
		for name, value := range metric.tags {
			appender.AddTag(name, value)
		}

		samplesAppender, err := appender.SamplesAppender()
		require.NoError(t, err)

		for _, sample := range metric.samples {
			err := samplesAppender.AppendCounterSample(sample)
			require.NoError(t, err)
		}
	}
	for _, metric := range testGaugeMetrics {
		appender.Reset()
		for name, value := range metric.tags {
			appender.AddTag(name, value)
		}

		samplesAppender, err := appender.SamplesAppender()
		require.NoError(t, err)

		for _, sample := range metric.samples {
			err := samplesAppender.AppendGaugeSample(sample)
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
		write := mustFindWrite(t, writes, metric.tags["__name__"])
		assert.Equal(t, metric.tags, write.Tags.StringMap())
		require.Equal(t, 1, len(write.Datapoints))
		assert.Equal(t, float64(metric.expected), write.Datapoints[0].Value)
	}
	for _, metric := range testGaugeMetrics {
		write := mustFindWrite(t, writes, metric.tags["__name__"])
		assert.Equal(t, metric.tags, write.Tags.StringMap())
		require.Equal(t, 1, len(write.Datapoints))
		assert.Equal(t, float64(metric.expected), write.Datapoints[0].Value)
	}
}

type testDownsampler struct {
	opts           DownsamplerOptions
	downsampler    Downsampler
	matcher        matcher.Matcher
	storage        mock.Storage
	rulesStore     store.Store
	instrumentOpts instrument.Options
}

type testDownsamplerOptions struct {
	autoMappingRules []MappingRule
	clockOpts        clock.Options
	instrumentOpts   instrument.Options
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
	rulesStorageOpts := ruleskv.NewStoreOptions(matcherOpts.NamespacesKey(),
		rulesetKeyFmt, nil)
	rulesStorage := ruleskv.NewStore(rulesKVStore, rulesStorageOpts)

	storeOpts := r2kv.NewStoreOptions().
		SetRuleUpdatePropagationDelay(0)
	rulesStore := r2kv.NewStore(rulesStorage, storeOpts)

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

	instance, err := NewDownsampler(DownsamplerOptions{
		Storage:               storage,
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
		tagsIter.append(name, value)
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

	iter := newEncodedTagsIterator(tagDecoder, nil)
	iter.Reset(data.Bytes())
	return iter
}

func mustFindWrite(t *testing.T, writes []*storage.WriteQuery, name string) *storage.WriteQuery {
	var write *storage.WriteQuery
	for _, w := range writes {
		if t, ok := w.Tags.Get(models.MetricName); ok {
			if t == name {
				write = w
				break
			}
		}
	}

	require.NotNil(t, write)
	return write
}
