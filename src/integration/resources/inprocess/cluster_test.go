// +build integration_v2

// Copyright (c) 2021  Uber Technologies, Inc.
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

package inprocess

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestNewCluster(t *testing.T) {
	configs, err := NewClusterConfigsFromYAML(clusterDBNodeConfig, clusterCoordConfig, "")
	require.NoError(t, err)

	m3, err := NewCluster(configs, ClusterOptions{
		DBNode: DBNodeClusterOptions{
			RF:                 3,
			NumInstances:       1,
			NumShards:          64,
			NumIsolationGroups: 3,
		},
	})
	require.NoError(t, err)
	require.NoError(t, m3.Nodes().WaitForHealthy())
	require.NoError(t, m3.Cleanup())
}

func TestNewSingleNodeCluster(t *testing.T) {
	configs, err := NewClusterConfigsFromYAML(clusterDBNodeConfig, clusterCoordConfig, "")
	require.NoError(t, err)

	m3, err := NewCluster(configs, ClusterOptions{
		DBNode: NewDBNodeClusterOptions(),
	})
	require.NoError(t, err)
	require.NoError(t, m3.Nodes().WaitForHealthy())
	require.NoError(t, m3.Cleanup())
}

func TestNewClusterWithAgg(t *testing.T) {
	aggCfg, err := loadDefaultAggregatorConfig()
	require.NoError(t, err)
	aggCfgBytes, err := yaml.Marshal(aggCfg)
	require.NoError(t, err)

	configs, err := NewClusterConfigsFromYAML(clusterDBNodeConfig, aggregatorCoordConfig, string(aggCfgBytes))
	require.NoError(t, err)

	aggClusterOpts := NewAggregatorClusterOptions()
	aggClusterOpts.NumInstances = 2
	m3, err := NewCluster(configs, ClusterOptions{
		DBNode:     NewDBNodeClusterOptions(),
		Aggregator: aggClusterOpts,
	})
	require.NoError(t, err)
	require.NoError(t, m3.Nodes().WaitForHealthy())
	require.NoError(t, m3.Aggregators().WaitForHealthy())
	require.NoError(t, m3.Cleanup())
}

const clusterDBNodeConfig = `
db:
  writeNewSeriesAsync: false
`

const clusterCoordConfig = `
clusters:
  - namespaces:
      - namespace: default
        type: unaggregated
        retention: 1h
`
