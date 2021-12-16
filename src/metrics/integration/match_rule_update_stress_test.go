// +build integration

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

package integration

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/matcher/cache"
	"github.com/m3db/m3/src/metrics/matcher/namespace"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/id/m3"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/x/pool"

	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

const (
	stressTestNamespacesKey = "namespaces"
	stressTestRuleSetKeyFmt = "rulesets/%s"
	stressTestNameTagKey    = "name"
	stressTestNamespaceName = "stress"
	stressTestRuleSetKey    = "rulesets/stress"
)

var stressTestNamespaceTag = []byte("namespace")

func TestMatchWithRuleUpdatesStress(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Initialize the kv store with namespace and ruleset.
	store := mem.NewStore()
	namespaces := stressTestNamespaces()
	ruleSet := stressTestRuleSet()
	updateStore(t, store, stressTestNamespacesKey, namespaces)
	updateStore(t, store, stressTestRuleSetKey, ruleSet)

	// Create matcher.
	cache := stressTestCache()
	iterPool := stressTestSortedTagIteratorPool()
	opts := stressTestMatcherOptions(store)
	matcher, err := matcher.NewMatcher(cache, opts)
	require.NoError(t, err)

	inputs := []struct {
		idFn      func(int) id.ID
		fromNanos int64
		toNanos   int64
		expected  rules.MatchResult
	}{
		{
			idFn:      func(i int) id.ID { return m3.NewID([]byte(fmt.Sprintf("m3+nomatch%d+namespace=stress", i)), iterPool) },
			fromNanos: 2000,
			toNanos:   math.MaxInt64,
			expected:  rules.EmptyMatchResult,
		},
		{
			idFn: func(i int) id.ID {
				return m3.NewID([]byte(fmt.Sprintf("m3+matchmapping%d+mtagName1=mtagValue1,namespace=stress", i)), iterPool)
			},
			fromNanos: 2000,
			toNanos:   math.MaxInt64,
			expected: rules.NewMatchResult(
				1,
				math.MaxInt64,
				metadata.StagedMetadatas{
					{
						CutoverNanos: 1000,
						Tombstoned:   false,
						Metadata: metadata.Metadata{
							Pipelines: []metadata.PipelineMetadata{
								{
									AggregationID: aggregation.DefaultID,
									StoragePolicies: policy.StoragePolicies{
										policy.MustParseStoragePolicy("10s:1d"),
									},
								},
							},
						},
					},
				},
				nil,
				false,
			),
		},
		{
			idFn: func(i int) id.ID {
				return m3.NewID([]byte(fmt.Sprintf("m3+matchrollup%d+namespace=stress,rtagName1=rtagValue1", i)), iterPool)
			},
			fromNanos: 2000,
			toNanos:   math.MaxInt64,
			expected: rules.NewMatchResult(
				1,
				math.MaxInt64,
				metadata.StagedMetadatas{
					{
						CutoverNanos: 500,
						Tombstoned:   false,
						Metadata:     metadata.DefaultMetadata,
					},
				},
				[]rules.IDWithMetadatas{
					{
						ID: []byte("m3+newRollupName1+m3_rollup=true,namespace=stress,rtagName1=rtagValue1"),
						Metadatas: metadata.StagedMetadatas{
							{
								CutoverNanos: 500,
								Tombstoned:   false,
								Metadata: metadata.Metadata{
									Pipelines: []metadata.PipelineMetadata{
										{
											AggregationID: aggregation.DefaultID,
											StoragePolicies: policy.StoragePolicies{
												policy.MustParseStoragePolicy("1m:2d"),
											},
										},
									},
								},
							},
						},
					},
				},
				true,
			),
		},
		{
			idFn: func(i int) id.ID {
				return m3.NewID([]byte(fmt.Sprintf("m3+matchmappingrollup%d+mtagName1=mtagValue1,namespace=stress,rtagName1=rtagValue1", i)), iterPool)
			},
			fromNanos: 2000,
			toNanos:   math.MaxInt64,
			expected: rules.NewMatchResult(
				1,
				math.MaxInt64,
				metadata.StagedMetadatas{
					{
						CutoverNanos: 1000,
						Tombstoned:   false,
						Metadata: metadata.Metadata{
							Pipelines: []metadata.PipelineMetadata{
								{
									AggregationID: aggregation.DefaultID,
									StoragePolicies: policy.StoragePolicies{
										policy.MustParseStoragePolicy("10s:1d"),
									},
								},
							},
						},
					},
				},
				[]rules.IDWithMetadatas{
					{
						ID: []byte("m3+newRollupName1+m3_rollup=true,namespace=stress,rtagName1=rtagValue1"),
						Metadatas: metadata.StagedMetadatas{
							{
								CutoverNanos: 500,
								Tombstoned:   false,
								Metadata: metadata.Metadata{
									Pipelines: []metadata.PipelineMetadata{
										{
											AggregationID: aggregation.DefaultID,
											StoragePolicies: policy.StoragePolicies{
												policy.MustParseStoragePolicy("1m:2d"),
											},
										},
									},
								},
							},
						},
					},
				},
				true,
			),
		},
	}

	for _, input := range inputs {
		var (
			matchIter  = 100000
			updateIter = 10000
			results    []rules.MatchResult
			expected   []rules.MatchResult
			wg         sync.WaitGroup
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			it := iterPool.Get()
			matchOpts := rules.MatchOptions{
				NameAndTagsFn: m3.NameAndTags,
				SortedTagIteratorFn: func(tagPairs []byte) id.SortedTagIterator {
					it.Reset(tagPairs)
					return it
				},
			}

			for i := 0; i < matchIter; i++ {
				res, err := matcher.ForwardMatch(input.idFn(i), input.fromNanos, input.toNanos, matchOpts)
				require.NoError(t, err)
				results = append(results, res)
				expected = append(expected, input.expected)
			}
			iterPool.Put(it)
		}()

		go func() {
			defer wg.Done()

			for i := 0; i < updateIter; i++ {
				updateStore(t, store, stressTestRuleSetKey, ruleSet)
			}
		}()

		wg.Wait()
		validateMatchResults(t, expected, results, true)
	}
}

func validateMatchResults(
	t *testing.T,
	expected, actual []rules.MatchResult,
	ignoreVersion bool,
) {
	require.Equal(t, len(expected), len(actual))
	for i := 0; i < len(expected); i++ {
		validateMatchResult(t, expected[i], actual[i], ignoreVersion)
	}
}

func validateMatchResult(
	t *testing.T,
	expected, actual rules.MatchResult,
	ignoreVersion bool,
) {
	if ignoreVersion {
		var (
			forExistingID   metadata.StagedMetadatas
			forNewRollupIDs []rules.IDWithMetadatas
		)
		if m := actual.ForExistingIDAt(0); len(m) > 0 {
			forExistingID = m
		}
		if numNewRollupIDs := actual.NumNewRollupIDs(); numNewRollupIDs > 0 {
			forNewRollupIDs = make([]rules.IDWithMetadatas, numNewRollupIDs)
			for i := range forNewRollupIDs {
				forNewRollupIDs[i] = actual.ForNewRollupIDsAt(i, 0)
			}
		}
		actual = rules.NewMatchResult(
			expected.Version(),
			actual.ExpireAtNanos(),
			forExistingID,
			forNewRollupIDs,
			actual.KeepOriginal(),
		)
	}
	testMatchResultCmpOpts := []cmp.Option{
		cmp.AllowUnexported(rules.MatchResult{}),
		cmpopts.EquateEmpty(),
	}

	require.True(t, cmp.Equal(expected, actual, testMatchResultCmpOpts...))
}

func updateStore(
	t *testing.T,
	store kv.Store,
	key string,
	proto proto.Message,
) {
	_, err := store.Set(key, proto)
	require.NoError(t, err)
}

func stressTestNamespaces() *rulepb.Namespaces {
	return &rulepb.Namespaces{
		Namespaces: []*rulepb.Namespace{
			{
				Name: stressTestNamespaceName,
				Snapshots: []*rulepb.NamespaceSnapshot{
					{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
				},
			},
		},
	}
}

func stressTestMappingRulesConfig() []*rulepb.MappingRule {
	return []*rulepb.MappingRule{
		{
			Uuid: "mappingRule1",
			Snapshots: []*rulepb.MappingRuleSnapshot{
				{
					Name:         "mappingRule1.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 1000,
					Filter:       "mtagName1:mtagValue1",
					StoragePolicies: []*policypb.StoragePolicy{
						{
							Resolution: policypb.Resolution{
								WindowSize: int64(10 * time.Second),
								Precision:  int64(time.Second),
							},
							Retention: policypb.Retention{
								Period: int64(24 * time.Hour),
							},
						},
					},
				},
			},
		},
	}
}

func stressTestRollupRulesConfig() []*rulepb.RollupRule {
	return []*rulepb.RollupRule{
		{
			Uuid: "rollupRule1",
			Snapshots: []*rulepb.RollupRuleSnapshot{
				{
					Name:         "rollupRule1.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 500,
					Filter:       "rtagName1:rtagValue1",
					KeepOriginal: true,
					TargetsV2: []*rulepb.RollupTargetV2{
						{
							Pipeline: &pipelinepb.Pipeline{
								Ops: []pipelinepb.PipelineOp{
									{
										Type: pipelinepb.PipelineOp_ROLLUP,
										Rollup: &pipelinepb.RollupOp{
											NewName: "newRollupName1",
											Tags:    []string{"namespace", "rtagName1"},
										},
									},
								},
							},
							StoragePolicies: []*policypb.StoragePolicy{
								{
									Resolution: policypb.Resolution{
										WindowSize: int64(time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: policypb.Retention{
										Period: int64(48 * time.Hour),
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func stressTestRuleSet() *rulepb.RuleSet {
	return &rulepb.RuleSet{
		Uuid:         "07592642-a105-40a5-a5c5-7c416ccb56c5",
		Namespace:    stressTestNamespaceName,
		Tombstoned:   false,
		CutoverNanos: 1000,
		MappingRules: stressTestMappingRulesConfig(),
		RollupRules:  stressTestRollupRulesConfig(),
	}
}

func stressTestCache() cache.Cache {
	return cache.NewCache(cache.NewOptions())
}

func stressTestSortedTagIteratorPool() id.SortedTagIteratorPool {
	poolOpts := pool.NewObjectPoolOptions()
	sortedTagIteratorPool := id.NewSortedTagIteratorPool(poolOpts)
	sortedTagIteratorPool.Init(func() id.SortedTagIterator {
		return m3.NewPooledSortedTagIterator(nil, sortedTagIteratorPool)
	})
	return sortedTagIteratorPool
}

func stressTestMatcherOptions(store kv.Store) matcher.Options {
	tagsFilterOpts := filters.TagsFilterOptions{
		NameTagKey: []byte(stressTestNameTagKey),
	}
	ruleSetOpts := rules.NewOptions().
		SetTagsFilterOptions(tagsFilterOpts).
		SetNewRollupIDFn(m3.NewRollupID)
	return matcher.NewOptions().
		SetKVStore(store).
		SetNamespacesKey(stressTestNamespacesKey).
		SetRuleSetKeyFn(func(namespace []byte) string {
			return fmt.Sprintf(stressTestRuleSetKeyFmt, namespace)
		}).
		SetNamespaceResolver(namespace.NewResolver(stressTestNamespaceTag, nil)).
		SetRuleSetOptions(ruleSetOpts)
}
