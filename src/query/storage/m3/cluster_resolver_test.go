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

package m3

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		start, end, store.clusters, opts, nil)
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
		start, end, store.clusters, opts, nil)
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
		start, end, store.clusters, opts, nil)
	require.NoError(t, err)
	require.Equal(t, 4, len(clusters))
	expected := []string{"metrics_aggregated_1m:30d", "metrics_aggregated_5m:90d",
		"metrics_aggregated_partial_1m:180d", "metrics_aggregated_10m:365d"}

	for i, cluster := range clusters {
		assert.Equal(t, expected[i], cluster.NamespaceID().String())
	}
}

var (
	genRetentionFiltered, genRetentionUnfiltered = time.Hour, time.Hour * 10
	genResolution                                = time.Minute
)

func generateClusters(t *testing.T, ctrl *gomock.Controller) Clusters {
	session := client.NewMockSession(ctrl)

	clusters, err := NewClusters(UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("UNAGG"),
		Retention:   genRetentionFiltered + time.Minute,
		Session:     session,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("AGG_FILTERED"),
		Retention:   genRetentionFiltered,
		Resolution:  genResolution,
		Downsample:  &ClusterNamespaceDownsampleOptions{All: false},
		Session:     session,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("AGG_NO_FILTER"),
		Retention:   genRetentionUnfiltered,
		Resolution:  genResolution,
		Downsample:  &ClusterNamespaceDownsampleOptions{All: false},
		Session:     session,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("AGG_FILTERED_COMPLETE"),
		Retention:   genRetentionFiltered,
		Resolution:  genResolution + time.Second,
		Downsample:  &ClusterNamespaceDownsampleOptions{All: true},
		Session:     session,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("AGG_NO_FILTER_COMPLETE"),
		Retention:   genRetentionUnfiltered,
		Resolution:  genResolution + time.Second,
		Downsample:  &ClusterNamespaceDownsampleOptions{All: true},
		Session:     session,
	})

	require.NoError(t, err)
	return clusters
}

var testCases = []struct {
	name                     string
	queryLength              time.Duration
	opts                     *storage.FanoutOptions
	restrict                 *storage.RestrictQueryOptions
	expectedType             consolidators.QueryFanoutType
	expectedClusterNames     []string
	expectedErr              error
	expectedErrContains      string
	expectedErrInvalidParams bool
}{
	{
		name: "all disabled",
		opts: &storage.FanoutOptions{
			FanoutUnaggregated:        storage.FanoutForceDisable,
			FanoutAggregated:          storage.FanoutForceDisable,
			FanoutAggregatedOptimized: storage.FanoutForceDisable,
		},
		expectedType: consolidators.NamespaceInvalid,
		expectedErr:  errUnaggregatedAndAggregatedDisabled,
	},
	{
		name: "optimize enabled",
		opts: &storage.FanoutOptions{
			FanoutUnaggregated:        storage.FanoutForceDisable,
			FanoutAggregated:          storage.FanoutForceDisable,
			FanoutAggregatedOptimized: storage.FanoutForceEnable,
		},
		expectedType: consolidators.NamespaceInvalid,
		expectedErr:  errUnaggregatedAndAggregatedDisabled,
	},
	{
		name: "unagg enabled",
		opts: &storage.FanoutOptions{
			FanoutUnaggregated:        storage.FanoutForceEnable,
			FanoutAggregated:          storage.FanoutForceDisable,
			FanoutAggregatedOptimized: storage.FanoutForceDisable,
		},
		expectedType:         consolidators.NamespaceCoversPartialQueryRange,
		expectedClusterNames: []string{"UNAGG"},
	},
	{
		name:        "unagg enabled short range",
		queryLength: time.Minute,
		opts: &storage.FanoutOptions{
			FanoutUnaggregated:        storage.FanoutForceEnable,
			FanoutAggregated:          storage.FanoutForceDisable,
			FanoutAggregatedOptimized: storage.FanoutForceDisable,
		},
		expectedType:         consolidators.NamespaceCoversAllQueryRange,
		expectedClusterNames: []string{"UNAGG"},
	},
	{
		name: "unagg optimized enabled",
		opts: &storage.FanoutOptions{
			FanoutUnaggregated:        storage.FanoutForceEnable,
			FanoutAggregated:          storage.FanoutForceDisable,
			FanoutAggregatedOptimized: storage.FanoutForceEnable,
		},
		expectedType:         consolidators.NamespaceCoversPartialQueryRange,
		expectedClusterNames: []string{"UNAGG"},
	},
	{
		name:        "unagg optimized enabled short range",
		queryLength: time.Minute,
		opts: &storage.FanoutOptions{
			FanoutUnaggregated:        storage.FanoutForceEnable,
			FanoutAggregated:          storage.FanoutForceDisable,
			FanoutAggregatedOptimized: storage.FanoutForceEnable,
		},
		expectedType:         consolidators.NamespaceCoversAllQueryRange,
		expectedClusterNames: []string{"UNAGG"},
	},
	{
		name: "agg enabled",
		opts: &storage.FanoutOptions{
			FanoutUnaggregated:        storage.FanoutForceDisable,
			FanoutAggregated:          storage.FanoutForceEnable,
			FanoutAggregatedOptimized: storage.FanoutForceDisable,
		},
		expectedType: consolidators.NamespaceCoversPartialQueryRange,
		expectedClusterNames: []string{"AGG_FILTERED", "AGG_NO_FILTER",
			"AGG_FILTERED_COMPLETE", "AGG_NO_FILTER_COMPLETE"},
	},
	{
		name:        "agg enabled short range",
		queryLength: time.Minute,
		opts: &storage.FanoutOptions{
			FanoutUnaggregated:        storage.FanoutForceDisable,
			FanoutAggregated:          storage.FanoutForceEnable,
			FanoutAggregatedOptimized: storage.FanoutForceDisable,
		},
		expectedType: consolidators.NamespaceCoversAllQueryRange,
		expectedClusterNames: []string{"AGG_FILTERED", "AGG_NO_FILTER",
			"AGG_FILTERED_COMPLETE", "AGG_NO_FILTER_COMPLETE"},
	},
	{
		name: "unagg and agg enabled",
		opts: &storage.FanoutOptions{
			FanoutUnaggregated:        storage.FanoutForceEnable,
			FanoutAggregated:          storage.FanoutForceEnable,
			FanoutAggregatedOptimized: storage.FanoutForceDisable,
		},
		expectedType: consolidators.NamespaceCoversPartialQueryRange,
		expectedClusterNames: []string{"UNAGG", "AGG_FILTERED", "AGG_NO_FILTER",
			"AGG_FILTERED_COMPLETE", "AGG_NO_FILTER_COMPLETE"},
	},
	{
		name:        "unagg and agg enabled short range",
		queryLength: time.Minute,
		opts: &storage.FanoutOptions{
			FanoutUnaggregated:        storage.FanoutForceEnable,
			FanoutAggregated:          storage.FanoutForceEnable,
			FanoutAggregatedOptimized: storage.FanoutForceDisable,
		},
		expectedType:         consolidators.NamespaceCoversAllQueryRange,
		expectedClusterNames: []string{"UNAGG"},
	},
	{
		name: "agg and optimization enabled",
		opts: &storage.FanoutOptions{
			FanoutUnaggregated:        storage.FanoutForceDisable,
			FanoutAggregated:          storage.FanoutForceEnable,
			FanoutAggregatedOptimized: storage.FanoutForceEnable,
		},
		expectedType:         consolidators.NamespaceCoversAllQueryRange,
		expectedClusterNames: []string{"AGG_NO_FILTER", "AGG_NO_FILTER_COMPLETE"},
	},
	{
		name:        "all enabled short range",
		queryLength: time.Minute,
		opts: &storage.FanoutOptions{
			FanoutUnaggregated:        storage.FanoutForceEnable,
			FanoutAggregated:          storage.FanoutForceEnable,
			FanoutAggregatedOptimized: storage.FanoutForceEnable,
		},
		expectedType:         consolidators.NamespaceCoversAllQueryRange,
		expectedClusterNames: []string{"UNAGG"},
	},
	{
		name:        "all enabled long range",
		queryLength: time.Hour * 1000,
		opts: &storage.FanoutOptions{
			FanoutUnaggregated:        storage.FanoutForceEnable,
			FanoutAggregated:          storage.FanoutForceEnable,
			FanoutAggregatedOptimized: storage.FanoutForceEnable,
		},
		expectedType:         consolidators.NamespaceCoversPartialQueryRange,
		expectedClusterNames: []string{"AGG_NO_FILTER", "AGG_NO_FILTER_COMPLETE"},
	},
	{
		name:        "restrict to unaggregated",
		queryLength: time.Hour * 1000,
		restrict: &storage.RestrictQueryOptions{
			RestrictByType: &storage.RestrictByType{
				MetricsType: storagemetadata.UnaggregatedMetricsType,
			},
		},
		expectedType:         consolidators.NamespaceCoversPartialQueryRange,
		expectedClusterNames: []string{"UNAGG"},
	},
	{
		name:        "restrict to aggregate filtered",
		queryLength: time.Hour * 1000,
		restrict: &storage.RestrictQueryOptions{
			RestrictByType: &storage.RestrictByType{
				MetricsType: storagemetadata.AggregatedMetricsType,
				StoragePolicy: policy.MustParseStoragePolicy(
					genResolution.String() + ":" + genRetentionFiltered.String()),
			},
		},
		expectedType:         consolidators.NamespaceCoversPartialQueryRange,
		expectedClusterNames: []string{"AGG_FILTERED"},
	},
	{
		name:        "restrict to aggregate unfiltered",
		queryLength: time.Hour * 1000,
		restrict: &storage.RestrictQueryOptions{
			RestrictByType: &storage.RestrictByType{
				MetricsType: storagemetadata.AggregatedMetricsType,
				StoragePolicy: policy.MustParseStoragePolicy(
					genResolution.String() + ":" + genRetentionUnfiltered.String()),
			},
		},
		expectedType:         consolidators.NamespaceCoversPartialQueryRange,
		expectedClusterNames: []string{"AGG_NO_FILTER"},
	},
	{
		name:        "restrict with unknown metrics type",
		queryLength: time.Hour * 1000,
		restrict: &storage.RestrictQueryOptions{
			RestrictByType: &storage.RestrictByType{
				MetricsType: storagemetadata.UnknownMetricsType,
			},
		},
		expectedErrContains:      "unrecognized metrics type:",
		expectedErrInvalidParams: true,
	},
	{
		name:        "restrict with unknown storage policy",
		queryLength: time.Hour * 1000,
		restrict: &storage.RestrictQueryOptions{
			RestrictByType: &storage.RestrictByType{
				MetricsType:   storagemetadata.AggregatedMetricsType,
				StoragePolicy: policy.MustParseStoragePolicy("1s:100d"),
			},
		},
		expectedErrContains:      "could not find namespace for storage policy:",
		expectedErrInvalidParams: true,
	},
}

func TestResolveClusterNamespacesForQueryWithOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	clusters := generateClusters(t, ctrl)

	now := time.Now()
	end := now
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			start := now.Add(tt.queryLength * -1)
			if tt.queryLength == 0 {
				// default case
				start = start.Add(time.Hour * -2)
			}

			fanoutType, clusters, err := resolveClusterNamespacesForQuery(now,
				start, end, clusters, tt.opts, tt.restrict)
			if tt.expectedErr != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedErr, err)
				assert.Nil(t, clusters)
				return
			}

			if substr := tt.expectedErrContains; substr != "" {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), substr))
				assert.Nil(t, clusters)
				invalidParams := xerrors.IsInvalidParams(err)
				assert.Equal(t, tt.expectedErrInvalidParams, invalidParams)
				return
			}

			require.NoError(t, err)
			actualNames := make([]string, len(clusters))
			for i, c := range clusters {
				actualNames[i] = c.NamespaceID().String()
			}

			// NB: order does not matter.
			sort.Sort(sort.StringSlice(actualNames))
			sort.Sort(sort.StringSlice(tt.expectedClusterNames))

			assert.Equal(t, tt.expectedClusterNames, actualNames)

			assert.Equal(t, tt.expectedType, fanoutType)
		})
	}
}

func TestLongUnaggregatedRetention(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	session := client.NewMockSession(ctrl)
	retentionFiltered, retentionUnfiltered := time.Hour, time.Hour*10
	resolution := time.Minute

	clusters, err := NewClusters(UnaggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("UNAGG"),
		Retention:   retentionUnfiltered,
		Session:     session,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("AGG_FILTERED"),
		Retention:   retentionFiltered,
		Resolution:  resolution,
		Downsample:  &ClusterNamespaceDownsampleOptions{All: false},
		Session:     session,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("AGG_NO_FILTER"),
		Retention:   retentionUnfiltered + time.Minute,
		Resolution:  resolution,
		Downsample:  &ClusterNamespaceDownsampleOptions{All: false},
		Session:     session,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("AGG_FILTERED_COMPLETE"),
		Retention:   retentionFiltered,
		Resolution:  resolution + time.Second,
		Downsample:  &ClusterNamespaceDownsampleOptions{All: true},
		Session:     session,
	}, AggregatedClusterNamespaceDefinition{
		NamespaceID: ident.StringID("AGG_NO_FILTER_COMPLETE"),
		Retention:   retentionUnfiltered,
		Resolution:  resolution + time.Second,
		Downsample:  &ClusterNamespaceDownsampleOptions{All: true},
		Session:     session,
	})

	require.NoError(t, err)
	now := time.Now()
	end := now
	start := now.Add(time.Hour * -100)
	opts := &storage.FanoutOptions{
		FanoutUnaggregated:        storage.FanoutForceEnable,
		FanoutAggregated:          storage.FanoutForceEnable,
		FanoutAggregatedOptimized: storage.FanoutForceEnable,
	}

	fanoutType, ns, err := resolveClusterNamespacesForQuery(now,
		start, end, clusters, opts, nil)

	require.NoError(t, err)
	actualNames := make([]string, len(ns))
	for i, c := range ns {
		actualNames[i] = c.NamespaceID().String()
	}

	expected := []string{"UNAGG", "AGG_NO_FILTER"}
	// NB: order does not matter.
	sort.Sort(sort.StringSlice(actualNames))
	sort.Sort(sort.StringSlice(expected))
	assert.Equal(t, expected, actualNames)
	assert.Equal(t, consolidators.NamespaceCoversPartialQueryRange, fanoutType)
}

func TestExampleCase(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	session := client.NewMockSession(ctrl)
	ns, err := NewClusters(
		UnaggregatedClusterNamespaceDefinition{
			NamespaceID: ident.StringID("metrics_10s_24h"),
			Retention:   24 * time.Hour,
			Session:     session,
		}, AggregatedClusterNamespaceDefinition{
			NamespaceID: ident.StringID("metrics_180s_360h"),
			Retention:   360 * time.Hour,
			Resolution:  120 * time.Second,
			Downsample:  &ClusterNamespaceDownsampleOptions{All: false},
			Session:     session,
		}, AggregatedClusterNamespaceDefinition{
			NamespaceID: ident.StringID("metrics_600s_17520h"),
			Retention:   17520 * time.Hour,
			Resolution:  600 * time.Second,
			Downsample:  &ClusterNamespaceDownsampleOptions{All: false},
			Session:     session,
		},
	)
	require.NoError(t, err)

	now := time.Now()
	end := now

	for i := 27; i < 17520; i++ {
		start := now.Add(time.Hour * -1 * time.Duration(i))
		fanoutType, clusters, err := resolveClusterNamespacesForQuery(now,
			start, end, ns, &storage.FanoutOptions{}, nil)

		require.NoError(t, err)
		actualNames := make([]string, len(clusters))
		for i, c := range clusters {
			actualNames[i] = c.NamespaceID().String()
		}

		// NB: order does not matter.
		sort.Sort(sort.StringSlice(actualNames))
		assert.Equal(t, []string{"metrics_10s_24h",
			"metrics_180s_360h", "metrics_600s_17520h"}, actualNames)
		assert.Equal(t, consolidators.NamespaceCoversPartialQueryRange, fanoutType)
	}
}
