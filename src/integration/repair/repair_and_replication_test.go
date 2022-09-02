//go:build cluster_integration
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
	"context"
	"testing"

	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/integration/resources/docker/dockerexternal"
	"github.com/m3db/m3/src/integration/resources/inprocess"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepairAndReplication(t *testing.T) {
	t.Skip("failing after etcd containerization; fix.")
	cluster1, cluster2, closer := testSetup(t)
	defer closer()

	RunTest(t, cluster1, cluster2)
}

func testSetup(t *testing.T) (resources.M3Resources, resources.M3Resources, func()) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	etcd1 := mustNewStartedEtcd(t, pool)
	etcd2 := mustNewStartedEtcd(t, pool)

	ep1 := []string{etcd1.Address()}
	ep2 := []string{etcd2.Address()}

	cluster1Opts := newTestClusterOptions()
	cluster1Opts.EtcdEndpoints = ep1

	cluster2Opts := newTestClusterOptions()
	cluster2Opts.EtcdEndpoints = ep2

	fullCfgs1 := getClusterFullConfgs(t, cluster1Opts)
	fullCfgs2 := getClusterFullConfgs(t, cluster2Opts)

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

	cluster1, err := inprocess.NewClusterFromSpecification(fullCfgs1, cluster1Opts)
	require.NoError(t, err)

	cluster2, err := inprocess.NewClusterFromSpecification(fullCfgs2, cluster2Opts)
	require.NoError(t, err)

	return cluster1, cluster2, func() {
		etcd1.Close(context.TODO())
		etcd2.Close(context.TODO())
		assert.NoError(t, cluster1.Cleanup())
		assert.NoError(t, cluster2.Cleanup())
	}
}

func mustNewStartedEtcd(t *testing.T, pool *dockertest.Pool) *dockerexternal.EtcdNode {
	etcd, err := dockerexternal.NewEtcd(pool, instrument.NewOptions())
	require.NoError(t, err)
	require.NoError(t, etcd.Setup(context.TODO()))
	return etcd
}

func getClusterFullConfgs(t *testing.T, clusterOptions resources.ClusterOptions) inprocess.ClusterSpecification {
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
		etcdService := &(dbnode.DB.Replication.Clusters[0].Client.EnvironmentConfig.Services[0].Service.ETCDClusters[0])
		etcdService.AutoSyncInterval = -1
		etcdService.Endpoints = endpoints
	}
}

func newTestClusterOptions() resources.ClusterOptions {
	return resources.ClusterOptions{
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
}
