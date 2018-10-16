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
	"testing"

	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/id/m3"
	"github.com/m3db/m3/src/metrics/policy"

	"github.com/stretchr/testify/require"
)

// TestReportMatchMappingWithRuleUpdates tests that the collector can perform
// rule matching and report metrics correctly when the metrics match against
// mapping rules, while the rules are updated at a high frequency.
func TestReportMatchMappingWithRuleUpdates(t *testing.T) {
	namespaces := defaultNamespaces()
	ruleSet := defaultRuleSet()
	placement, err := defaultStagedPlacementProto()
	require.NoError(t, err)

	// Initialize the kv store with default namespaces and ruleset.
	store := mem.NewStore()
	updateStore(t, store, defaultNamespacesKey, namespaces)
	updateStore(t, store, defaultRuleSetKey, ruleSet)
	updateStore(t, store, defaultPlacementKey, placement)

	// Initialize test parameters.
	iterPool := defaultSortedTagIteratorPool()
	idGen := func(i int) id.ID {
		return m3.NewID([]byte(fmt.Sprintf("m3+matchmapping%d+mtagName1=mtagValue1,namespace=%s", i, defaultNamespace)), iterPool)
	}
	outputRes := []outputResult{
		{
			idGen: idGen,
			metadatas: metadata.StagedMetadatas{
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
		},
	}

	testReportWithRuleUpdates(t, testReportWithRuleUpdatesOptions{
		Description:   "test reporting metrics with mapping rule match and rule updates",
		Store:         store,
		MatcherOpts:   defaultMatcherOptions(store, iterPool),
		AggClientOpts: defaultAggregatorClientOptions(store),
		InputIDGen:    idGen,
		OutputRes:     outputRes,
		RuleUpdateFn:  func() { updateStore(t, store, defaultRuleSetKey, ruleSet) },
	})
}
