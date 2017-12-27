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

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/matcher"
	"github.com/m3db/m3metrics/matcher/cache"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/id/m3"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/pool"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

const (
	stressTestNamespacesKey = "namespaces"
	stressTestNamespaceTag  = "namespace"
	stressTestRuleSetKeyFmt = "rulesets/%s"
	stressTestNameTagKey    = "name"
	stressTestNamespaceName = "stress"
	stressTestRuleSetKey    = "rulesets/stress"
)

func TestMatchWithRuleUpdatesStress(t *testing.T) {
	// Initialize the kv store with namespace and ruleset.
	store := mem.NewStore()
	namespaces := stressTestNamespaces()
	ruleSet := stressTestRuleSet()
	updateStore(t, store, stressTestNamespacesKey, namespaces)
	updateStore(t, store, stressTestRuleSetKey, ruleSet)

	// Create matcher.
	cache := stressTestCache()
	iterPool := stressTestSortedTagIteratorPool()
	opts := stressTestMatcherOptions(store, iterPool)
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
				policy.PoliciesList([]policy.StagedPolicies{
					policy.NewStagedPolicies(
						1000,
						false,
						[]policy.Policy{
							policy.NewPolicy(policy.MustParseStoragePolicy("10s:1d"), policy.DefaultAggregationID),
						},
					),
				}),
				nil,
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
				policy.PoliciesList([]policy.StagedPolicies{
					policy.NewStagedPolicies(0, false, nil),
				}),
				[]rules.RollupResult{
					{
						ID: []byte("m3+newRollupName1+m3_rollup=true,namespace=stress,rtagName1=rtagValue1"),
						PoliciesList: []policy.StagedPolicies{
							policy.NewStagedPolicies(
								500,
								false,
								[]policy.Policy{
									policy.NewPolicy(policy.MustParseStoragePolicy("1m:2d"), policy.DefaultAggregationID),
								},
							),
						},
					},
				},
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
				policy.PoliciesList([]policy.StagedPolicies{
					policy.NewStagedPolicies(
						1000,
						false,
						[]policy.Policy{
							policy.NewPolicy(policy.MustParseStoragePolicy("10s:1d"), policy.DefaultAggregationID),
						},
					),
				}),
				[]rules.RollupResult{
					{
						ID: []byte("m3+newRollupName1+m3_rollup=true,namespace=stress,rtagName1=rtagValue1"),
						PoliciesList: []policy.StagedPolicies{
							policy.NewStagedPolicies(
								500,
								false,
								[]policy.Policy{
									policy.NewPolicy(policy.MustParseStoragePolicy("1m:2d"), policy.DefaultAggregationID),
								},
							),
						},
					},
				},
			),
		},
	}

	for _, input := range inputs {
		var (
			matchIter  = 1000000
			updateIter = 10000
			results    []rules.MatchResult
			expected   []rules.MatchResult
			wg         sync.WaitGroup
		)
		wg.Add(2)
		go func() {
			defer wg.Done()

			for i := 0; i < matchIter; i++ {
				res := matcher.ForwardMatch(input.idFn(i), input.fromNanos, input.toNanos)
				results = append(results, res)
				expected = append(expected, input.expected)
			}
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
			mappings policy.PoliciesList
			rollups  []rules.RollupResult
		)
		if m := actual.MappingsAt(0); len(m) > 0 {
			mappings = m
		}
		if numRollups := actual.NumRollups(); numRollups > 0 {
			rollups = make([]rules.RollupResult, numRollups)
			for i := range rollups {
				r, _ := actual.RollupsAt(i, 0)
				rollups[i] = r
			}
		}
		actual = rules.NewMatchResult(expected.Version(), actual.ExpireAtNanos(), mappings, rollups)
	}
	require.Equal(t, expected, actual)
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

func stressTestNamespaces() *schema.Namespaces {
	return &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: stressTestNamespaceName,
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
				},
			},
		},
	}
}

func stressTestMappingRulesConfig() []*schema.MappingRule {
	return []*schema.MappingRule{
		&schema.MappingRule{
			Uuid: "mappingRule1",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:         "mappingRule1.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 1000,
					Filter:       "mtagName1:mtagValue1",
					Policies: []*schema.Policy{
						&schema.Policy{
							StoragePolicy: &schema.StoragePolicy{
								Resolution: &schema.Resolution{
									WindowSize: int64(10 * time.Second),
									Precision:  int64(time.Second),
								},
								Retention: &schema.Retention{
									Period: int64(24 * time.Hour),
								},
							},
						},
					},
				},
			},
		},
	}
}

func stressTestRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:         "rollupRule1.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 500,
					Filter:       "rtagName1:rtagValue1",
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name: "newRollupName1",
							Tags: []string{"namespace", "rtagName1"},
							Policies: []*schema.Policy{
								&schema.Policy{
									StoragePolicy: &schema.StoragePolicy{
										Resolution: &schema.Resolution{
											WindowSize: int64(time.Minute),
											Precision:  int64(time.Minute),
										},
										Retention: &schema.Retention{
											Period: int64(48 * time.Hour),
										},
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

func stressTestRuleSet() *schema.RuleSet {
	return &schema.RuleSet{
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

func stressTestMatcherOptions(
	store kv.Store,
	sortedTagIteratorPool id.SortedTagIteratorPool,
) matcher.Options {
	sortedTagIteratorFn := func(tagPairs []byte) id.SortedTagIterator {
		it := sortedTagIteratorPool.Get()
		it.Reset(tagPairs)
		return it
	}
	tagsFilterOpts := filters.TagsFilterOptions{
		NameTagKey:          []byte(stressTestNameTagKey),
		NameAndTagsFn:       m3.NameAndTags,
		SortedTagIteratorFn: sortedTagIteratorFn,
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
		SetNamespaceTag([]byte(stressTestNamespaceTag)).
		SetRuleSetOptions(ruleSetOpts)
}
