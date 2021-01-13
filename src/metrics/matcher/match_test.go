// Copyright (c) 2017 Uber Technologies, Inc.
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

package matcher

import (
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/matcher/cache"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	"github.com/m3db/m3/src/x/watch"
)

func TestMatcherCreateWatchError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kvStore := kv.NewMockStore(ctrl)
	kvStore.EXPECT().Watch(testNamespacesKey).Return(nil, watch.CreateWatchError{})
	opts := NewOptions().
		SetInitWatchTimeout(10 * time.Millisecond).
		SetNamespacesKey(testNamespacesKey).
		SetKVStore(kvStore)

	_, err := NewMatcher(newMemCache(), opts)
	require.Error(t, err)
	_, ok := err.(watch.CreateWatchError)
	require.True(t, ok)
}

func TestMatcherInitializeValueError(t *testing.T) {
	memStore := mem.NewStore()
	opts := NewOptions().
		SetInitWatchTimeout(10 * time.Millisecond).
		SetNamespacesKey(testNamespacesKey).
		SetKVStore(memStore)

	matcher, err := NewMatcher(newMemCache(), opts)
	require.NoError(t, err)
	require.NotNil(t, matcher)
}

func TestMatcherMatchDoesNotExist(t *testing.T) {
	id := &testMetricID{
		id:         []byte("foo"),
		tagValueFn: func(tagName []byte) ([]byte, bool) { return nil, false },
	}
	now := time.Now()
	matcher, testScope := testMatcher(t, testMatcherOptions{
		cache: newMemCache(),
	})
	require.Equal(t, rules.EmptyMatchResult, matcher.ForwardMatch(id, now.UnixNano(), now.UnixNano()))

	requireLatencyMetrics(t, "cached-matcher", testScope)
}

func TestMatcherMatchExists(t *testing.T) {
	var (
		ns = "ns/foo"
		id = &testMetricID{
			id:         []byte("foo"),
			tagValueFn: func(tagName []byte) ([]byte, bool) { return []byte(ns), true },
		}
		now    = time.Now()
		res    = rules.NewMatchResult(0, math.MaxInt64, nil, nil, true)
		memRes = memResults{results: map[string]rules.MatchResult{"foo": res}}
	)
	cache := newMemCache()
	matcher, _ := testMatcher(t, testMatcherOptions{
		cache: cache,
	})
	c := cache.(*memCache)
	c.namespaces[ns] = memRes
	require.Equal(t, res, matcher.ForwardMatch(id, now.UnixNano(), now.UnixNano()))
}

func TestMatcherMatchExistsNoCache(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		ns     = "fooNs"
		metric = &testMetricID{
			id: []byte("foo"),
			tagValueFn: func(tagName []byte) ([]byte, bool) {
				if string(tagName) == "fooTag" {
					return []byte("fooValue"), true
				}
				return []byte(ns), true
			},
		}
		now = time.Now()
	)
	matcher, testScope := testMatcher(t, testMatcherOptions{
		tagFilterOptions: filters.TagsFilterOptions{
			NameAndTagsFn: func(id []byte) (name []byte, tags []byte, err error) {
				name = metric.id
				return
			},
			SortedTagIteratorFn: func(tagPairs []byte) id.SortedTagIterator {
				iter := id.NewMockSortedTagIterator(ctrl)
				iter.EXPECT().Next().Return(true)
				iter.EXPECT().Current().Return([]byte("fooTag"), []byte("fooValue"))
				iter.EXPECT().Next().Return(false)
				iter.EXPECT().Err().Return(nil)
				iter.EXPECT().Close()
				return iter
			},
		},
		storeSetup: func(t *testing.T, store kv.TxnStore) {
			_, err := store.Set(testNamespacesKey, &rulepb.Namespaces{
				Namespaces: []*rulepb.Namespace{
					{
						Name: ns,
						Snapshots: []*rulepb.NamespaceSnapshot{
							{
								ForRulesetVersion: 1,
								Tombstoned:        false,
							},
						},
					},
				},
			})
			require.NoError(t, err)

			_, err = store.Set("/ruleset/fooNs", &rulepb.RuleSet{
				Namespace: ns,
				MappingRules: []*rulepb.MappingRule{
					{
						Snapshots: []*rulepb.MappingRuleSnapshot{
							{
								Filter: "fooTag:fooValue",
								AggregationTypes: []aggregationpb.AggregationType{
									aggregationpb.AggregationType_LAST,
								},
								StoragePolicies: []*policypb.StoragePolicy{
									{
										Resolution: policypb.Resolution{
											WindowSize: int64(time.Minute),
											Precision:  int64(time.Minute),
										},
										Retention: policypb.Retention{
											Period: 24 * int64(time.Hour),
										},
									},
								},
							},
						},
					},
				},
			})
			require.NoError(t, err)
		},
	})

	forExistingID := metadata.StagedMetadatas{
		metadata.StagedMetadata{
			Metadata: metadata.Metadata{
				Pipelines: metadata.PipelineMetadatas{
					metadata.PipelineMetadata{
						AggregationID: aggregation.MustCompressTypes(aggregation.Last),
						StoragePolicies: policy.StoragePolicies{
							policy.MustParseStoragePolicy("1m:1d"),
						},
						Tags: []models.Tag{},
					},
				},
			},
		},
	}
	forNewRollupIDs := []rules.IDWithMetadatas{}
	keepOriginal := false
	expected := rules.NewMatchResult(1, math.MaxInt64,
		forExistingID, forNewRollupIDs, keepOriginal)

	result := matcher.ForwardMatch(metric, now.UnixNano(), now.UnixNano())

	require.Equal(t, expected, result)

	// Check that latency was measured
	requireLatencyMetrics(t, "matcher", testScope)
}

func TestMatcherClose(t *testing.T) {
	matcher, _ := testMatcher(t, testMatcherOptions{
		cache: newMemCache(),
	})
	require.NoError(t, matcher.Close())
}

type testMatcherOptions struct {
	cache            cache.Cache
	storeSetup       func(*testing.T, kv.TxnStore)
	tagFilterOptions filters.TagsFilterOptions
}

func testMatcher(t *testing.T, opts testMatcherOptions) (Matcher, tally.TestScope) {
	scope := tally.NewTestScope("", nil)
	var (
		store       = mem.NewStore()
		matcherOpts = NewOptions().
				SetClockOptions(clock.NewOptions()).
				SetInstrumentOptions(instrument.NewOptions().SetMetricsScope(scope)).
				SetInitWatchTimeout(100 * time.Millisecond).
				SetKVStore(store).
				SetNamespacesKey(testNamespacesKey).
				SetNamespaceTag([]byte("namespace")).
				SetDefaultNamespace([]byte("default")).
				SetRuleSetKeyFn(defaultRuleSetKeyFn).
				SetRuleSetOptions(rules.NewOptions().
					SetTagsFilterOptions(opts.tagFilterOptions)).
				SetMatchRangePast(0)
		proto = &rulepb.Namespaces{
			Namespaces: []*rulepb.Namespace{
				{
					Name: "fooNs",
					Snapshots: []*rulepb.NamespaceSnapshot{
						{
							ForRulesetVersion: 1,
							Tombstoned:        true,
						},
					},
				},
			},
		}
	)

	_, err := store.SetIfNotExists(testNamespacesKey, proto)
	require.NoError(t, err)

	if fn := opts.storeSetup; fn != nil {
		fn(t, store)
	}

	m, err := NewMatcher(opts.cache, matcherOpts)
	require.NoError(t, err)
	return m, scope
}

func requireLatencyMetrics(t *testing.T, metricScope string, testScope tally.TestScope) {
	// Check that latency was measured
	values, found := testScope.Snapshot().Histograms()[metricScope+".match-latency+"]
	require.True(t, found)
	latencyMeasured := false
	for _, valuesInBucket := range values.Durations() {
		if valuesInBucket > 0 {
			latencyMeasured = true
			break
		}
	}
	require.True(t, latencyMeasured)
}

type tagValueFn func(tagName []byte) ([]byte, bool)

type testMetricID struct {
	id         []byte
	tagValueFn tagValueFn
}

func (id *testMetricID) Bytes() []byte                          { return id.id }
func (id *testMetricID) TagValue(tagName []byte) ([]byte, bool) { return id.tagValueFn(tagName) }
