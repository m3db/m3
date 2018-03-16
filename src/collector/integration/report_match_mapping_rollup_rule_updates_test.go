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

	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/id/m3"
	"github.com/m3db/m3metrics/policy"

	"github.com/stretchr/testify/require"
)

// TestReportMatchMappingRollupWithRuleUpdates tests that the collector can perform
// rule matching and report metrics correctly when the metrics match against both
// mapping rules and rollup rules, while the rules are updated at a high frequency.
func TestReportMatchMappingRollupWithRuleUpdates(t *testing.T) {
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
		return m3.NewID([]byte(fmt.Sprintf("m3+matchmappingrollup%d+mtagName1=mtagValue1,namespace=%s,rtagName1=rtagValue1", i, defaultNamespace)), iterPool)
	}
	outputRes := []outputResult{
		{
			idGen: idGen,
			policiesList: policy.PoliciesList{
				policy.NewStagedPolicies(
					1000,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.MustParseStoragePolicy("10s:1d"), aggregation.DefaultID),
					},
				),
			},
		},
		{
			idGen: func(i int) id.ID {
				return m3.NewID([]byte(fmt.Sprintf("m3+newRollupName1+m3_rollup=true,namespace=%s,rtagName1=rtagValue1", defaultNamespace)), iterPool)
			},
			policiesList: policy.PoliciesList{
				policy.NewStagedPolicies(
					500,
					false,
					[]policy.Policy{
						policy.NewPolicy(policy.MustParseStoragePolicy("1m:2d"), aggregation.DefaultID),
					},
				),
			},
		},
	}

	testReportWithRuleUpdates(t, testReportWithRuleUpdatesOptions{
		Description:  "test reporting metrics with mapping and rollup rule matches and rule updates",
		Store:        store,
		MatcherOpts:  defaultMatcherOptions(store, iterPool),
		BackendOpts:  defaultBackendOptions(store),
		InputIDGen:   idGen,
		OutputRes:    outputRes,
		RuleUpdateFn: func() { updateStore(t, store, defaultRuleSetKey, ruleSet) },
	})
}
