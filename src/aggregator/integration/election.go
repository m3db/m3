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
	"testing"

	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/services/leader"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/integration"
)

var (
	testClusterSize     = 1
	testEnvironment     = "testEnv"
	testServiceName     = "testSvc"
	testZone            = "testZone"
	testElectionTTLSecs = 5
)

type testCluster struct {
	t       *testing.T
	cluster *integration.Cluster
}

func newTestCluster(t *testing.T) *testCluster {
	integration.BeforeTestExternal(t)
	return &testCluster{
		t: t,
		cluster: integration.NewCluster(t, &integration.ClusterConfig{
			Size: testClusterSize,
		}),
	}
}

func (tc *testCluster) LeaderService() services.LeaderService {
	svc, err := leader.NewService(tc.etcdClient(), tc.options())
	require.NoError(tc.t, err)
	return svc
}

func (tc *testCluster) Close() {
	tc.cluster.Terminate(tc.t)
}

func (tc *testCluster) etcdClient() *clientv3.Client {
	return tc.cluster.RandClient()
}

func (tc *testCluster) options() leader.Options {
	sid := services.NewServiceID().
		SetEnvironment(testEnvironment).
		SetName(testServiceName).
		SetZone(testZone)
	eopts := services.NewElectionOptions().
		SetTTLSecs(testElectionTTLSecs)
	return leader.NewOptions().
		SetServiceID(sid).
		SetElectionOpts(eopts)
}
