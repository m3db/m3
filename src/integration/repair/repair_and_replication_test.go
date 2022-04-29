// +build cluster_integration
//
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

package repair

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/integration/resources/inprocess"
)

func TestRepairAndReplication(t *testing.T) {
	cluster1, cluster2, closer := testSetup(t)
	defer closer()

	RunTest(t, cluster1, cluster2)
}

func testSetup(t *testing.T) (resources.M3Resources, resources.M3Resources, func()) {
	fullCfgs1 := getClusterFullConfgs(t)
	fullCfgs2 := getClusterFullConfgs(t)

	ep1 := fullCfgs1.Configs.Coordinator.Clusters[0].Client.EnvironmentConfig.Services[0].Service.ETCDClusters[0].Endpoints
	ep2 := fullCfgs2.Configs.Coordinator.Clusters[0].Client.EnvironmentConfig.Services[0].Service.ETCDClusters[0].Endpoints

	setRepairAndReplicationCfg(
		&fullCfgs1,
		"cluster-2",
		ep2,
	)
	setRepairAndReplicationCfg(
		&fullCfgs2,
		"cluster-1",
		ep1,
	)

	cluster1, err := inprocess.NewClusterFromSpecification(fullCfgs1, clusterOptions)
	require.NoError(t, err)

	cluster2, err := inprocess.NewClusterFromSpecification(fullCfgs2, clusterOptions)
	require.NoError(t, err)

	return cluster1, cluster2, func() {
		assert.NoError(t, cluster1.Cleanup())
		assert.NoError(t, cluster2.Cleanup())
	}
}

func getClusterFullConfgs(t *testing.T) inprocess.ClusterSpecification {
	cfgs, err := inprocess.NewClusterConfigsFromYAML(
		TestRepairDBNodeConfig, TestRepairCoordinatorConfig, "",
	)
	require.NoError(t, err)

	fullCfgs, err := inprocess.GenerateClusterSpecification(cfgs, clusterOptions)
	require.NoError(t, err)

	return fullCfgs
}

func setRepairAndReplicationCfg(fullCfg *inprocess.ClusterSpecification, clusterName string, endpoints []string) {
	for _, dbnode := range fullCfg.Configs.DBNodes {
		dbnode.DB.Replication.Clusters[0].Name = clusterName
		dbnode.DB.Replication.Clusters[0].Client.EnvironmentConfig.Services[0].Service.ETCDClusters[0].Endpoints = endpoints
	}
}

var clusterOptions = resources.ClusterOptions{
	DBNode: &resources.DBNodeClusterOptions{
		RF:                 2,
		NumShards:          4,
		NumInstances:       1,
		NumIsolationGroups: 2,
	},
	Coordinator: resources.CoordinatorClusterOptions{
		GeneratePorts: true,
	},
}
