// Copyright (c) 2019 Uber Technologies, Inc.
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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestBufferForPastTimedMetric(t *testing.T) {
	limits := defaultBufferPastLimits
	tests := []struct {
		value    time.Duration
		expected time.Duration
	}{
		{value: -1 * time.Second, expected: 15 * time.Second},
		{value: 0, expected: 15 * time.Second},
		{value: 1 * time.Second, expected: 15 * time.Second},
		{value: 29 * time.Second, expected: 15 * time.Second},
		{value: 30 * time.Second, expected: 30 * time.Second},
		{value: 59 * time.Second, expected: 30 * time.Second},
		{value: 60 * time.Second, expected: time.Minute},
		{value: 2 * time.Minute, expected: 2 * time.Minute},
		{value: 59 * time.Minute, expected: 2 * time.Minute},
		{value: 61 * time.Minute, expected: 2 * time.Minute},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("test_value_%v", test.value), func(t *testing.T) {
			result := bufferForPastTimedMetric(limits, test.value)
			require.Equal(t, test.expected, result)
		})
	}
}

func TestAutoMappingRules(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	session := client.NewMockSession(ctrl)

	clusters, err := m3.NewClusters(
		m3.UnaggregatedClusterNamespaceDefinition{
			NamespaceID: ident.StringID("default"),
			Retention:   48 * time.Hour,
			Session:     session,
		}, m3.AggregatedClusterNamespaceDefinition{
			NamespaceID: ident.StringID("2s:1d"),
			Resolution:  2 * time.Second,
			Retention:   24 * time.Hour,
			Session:     session,
		}, m3.AggregatedClusterNamespaceDefinition{
			NamespaceID: ident.StringID("4s:2d"),
			Resolution:  4 * time.Second,
			Retention:   48 * time.Hour,
			Session:     session,
			Downsample:  &m3.ClusterNamespaceDownsampleOptions{All: false},
		}, m3.AggregatedClusterNamespaceDefinition{
			NamespaceID: ident.StringID("10s:4d"),
			Resolution:  10 * time.Second,
			Retention:   96 * time.Hour,
			Session:     session,
			ReadOnly:    true,
		},
	)
	require.NoError(t, err)

	rules, err := NewAutoMappingRules(clusters.ClusterNamespaces())
	require.NoError(t, err)

	require.Len(t, rules, 1)
	require.Equal(t, []AutoMappingRule{
		{
			Aggregations: []aggregation.Type{aggregation.Last},
			Policies: policy.StoragePolicies{
				policy.NewStoragePolicy(2*time.Second, xtime.Second, 24*time.Hour),
			},
		},
	}, rules)
}
