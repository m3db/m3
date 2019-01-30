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

package m3

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/query/storage"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFanoutAggregatedDisabledGivesNoClustersOnAggregation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	s, _ := setup(t, ctrl)
	store, ok := s.(*m3storage)
	assert.True(t, ok)
	var r reusedAggregatedNamespaceSlices
	opts := &storage.FanoutOptions{
		FanoutAggregated: storage.FanoutForceDisable,
	}

	clusters := store.clusters.ClusterNamespaces()
	r = aggregatedNamespaces(clusters, r, nil, opts)
	assert.Equal(t, 0, len(r.completeAggregated))
	assert.Equal(t, 0, len(r.partialAggregated))
}

func TestFanoutAggregatedOptimizationDisabledGivesAllClustersAsPartial(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	s, _ := setup(t, ctrl)
	store, ok := s.(*m3storage)
	assert.True(t, ok)
	var r reusedAggregatedNamespaceSlices
	opts := &storage.FanoutOptions{
		FanoutAggregatedOptimized: storage.FanoutForceDisable,
	}

	clusters := store.clusters.ClusterNamespaces()
	r = aggregatedNamespaces(clusters, r, nil, opts)
	assert.Equal(t, 0, len(r.completeAggregated))
	assert.Equal(t, 4, len(r.partialAggregated))
}

func TestFanoutUnaggregatedDisableReturnsAggregatedNamespaces(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	s, _ := setup(t, ctrl)
	store, ok := s.(*m3storage)
	assert.True(t, ok)
	opts := &storage.FanoutOptions{
		FanoutUnaggregated: storage.FanoutForceDisable,
	}

	start := time.Now()
	end := start.Add(time.Hour * 24 * -90)
	_, clusters, err := resolveClusterNamespacesForQuery(start,
		store.clusters, start, end, opts)
	require.NoError(t, err)
	require.Equal(t, 1, len(clusters))
	assert.Equal(t, "metrics_aggregated_1m:30d", clusters[0].NamespaceID().String())
}

func TestFanoutUnaggregatedEnabledReturnsUnaggregatedNamespaces(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	s, _ := setup(t, ctrl)
	store, ok := s.(*m3storage)
	assert.True(t, ok)
	opts := &storage.FanoutOptions{
		FanoutUnaggregated: storage.FanoutForceEnable,
	}

	start := time.Now()
	end := start.Add(time.Hour * 24 * -90)
	_, clusters, err := resolveClusterNamespacesForQuery(start,
		store.clusters, start, end, opts)
	require.NoError(t, err)
	require.Equal(t, 1, len(clusters))
	assert.Equal(t, "metrics_unaggregated", clusters[0].NamespaceID().String())
}

func TestGraphitePath(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	s, _ := setup(t, ctrl)
	store, ok := s.(*m3storage)
	assert.True(t, ok)
	opts := &storage.FanoutOptions{
		FanoutUnaggregated:        storage.FanoutForceDisable,
		FanoutAggregated:          storage.FanoutForceEnable,
		FanoutAggregatedOptimized: storage.FanoutForceDisable,
	}

	start := time.Now()
	end := start.Add(time.Second * -30)
	_, clusters, err := resolveClusterNamespacesForQuery(start,
		store.clusters, start, end, opts)
	require.NoError(t, err)
	require.Equal(t, 4, len(clusters))
	expected := []string{"metrics_aggregated_1m:30d", "metrics_aggregated_5m:90d",
		"metrics_aggregated_partial_1m:180d", "metrics_aggregated_10m:365d"}

	for i, cluster := range clusters {
		assert.Equal(t, expected[i], cluster.NamespaceID().String())
	}
}
