// Copyright (c) 2016 Uber Technologies, Inc.
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

package etcd

import (
	"testing"

	"github.com/m3db/m3cluster/services"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/integration"
	"github.com/stretchr/testify/require"
)

func TestETCDClientGen(t *testing.T) {
	cs, err := NewConfigServiceClient(testOptions())
	require.NoError(t, err)

	c := cs.(*csclient)
	// a zone that does not exist
	_, err = c.etcdClientGen("not_exist")
	require.Error(t, err)
	require.Equal(t, 0, len(c.clis))

	c1, err := c.etcdClientGen("zone1")
	require.NoError(t, err)
	require.Equal(t, 1, len(c.clis))

	c2, err := c.etcdClientGen("zone2")
	require.NoError(t, err)
	require.Equal(t, 2, len(c.clis))
	require.NotEqual(t, c1, c2)

	c1Again, err := c.etcdClientGen("zone1")
	require.NoError(t, err)
	require.Equal(t, 2, len(c.clis))
	require.Equal(t, c1, c1Again)
}

func TestKVAndHeartbeatServiceSharingETCDClient(t *testing.T) {
	sid := services.NewServiceID().SetName("s1")

	cs, err := NewConfigServiceClient(testOptions().SetZone("zone1"))
	require.NoError(t, err)

	c := cs.(*csclient)

	_, err = c.KV()
	require.NoError(t, err)
	require.Equal(t, 1, len(c.clis))

	_, err = c.heartbeatGen()(sid.SetZone("zone1"))
	require.NoError(t, err)
	require.Equal(t, 1, len(c.clis))

	_, err = c.heartbeatGen()(sid.SetZone("zone2"))
	require.NoError(t, err)
	require.Equal(t, 2, len(c.clis))

	_, err = c.heartbeatGen()(sid.SetZone("not_exist"))
	require.Error(t, err)
	require.Equal(t, 2, len(c.clis))
}

func TestClient(t *testing.T) {
	_, err := NewConfigServiceClient(NewOptions())
	require.Error(t, err)

	cs, err := NewConfigServiceClient(testOptions())
	require.NoError(t, err)
	_, err = cs.KV()
	require.NoError(t, err)

	cs, err = NewConfigServiceClient(testOptions())
	c := cs.(*csclient)

	fn, closer := testNewETCDFn(t)
	defer closer()
	c.newFn = fn

	txn, err := c.Txn()
	require.NoError(t, err)

	kv1, err := c.KV()
	require.NoError(t, err)
	require.Equal(t, kv1, txn)

	kv2, err := c.KV()
	require.NoError(t, err)
	require.Equal(t, kv1, kv2)

	kv3, err := c.Store("ns")
	require.NoError(t, err)
	require.NotEqual(t, kv1, kv3)

	// KV store will create an etcd cli for local zone only
	require.Equal(t, 1, len(c.clis))
	_, ok := c.clis["zone1"]
	require.True(t, ok)

	sd1, err := c.Services()
	require.NoError(t, err)

	sd2, err := c.Services()
	require.NoError(t, err)
	require.Equal(t, sd1, sd2)

	err = sd1.SetMetadata(
		services.NewServiceID().SetName("service").SetZone("zone2"),
		services.NewMetadata(),
	)
	require.NoError(t, err)
	// etcd cli for zone1 will be reused
	require.Equal(t, 2, len(c.clis))
	_, ok = c.clis["zone2"]
	require.True(t, ok)

	err = sd1.SetMetadata(
		services.NewServiceID().SetName("service").SetZone("zone3"),
		services.NewMetadata(),
	)
	require.NoError(t, err)
	// etcd cli for zone2 will be created since the request is going to zone2
	require.Equal(t, 3, len(c.clis))
	_, ok = c.clis["zone3"]
	require.True(t, ok)
}

func TestCacheFileForZone(t *testing.T) {
	c, err := NewConfigServiceClient(testOptions())
	require.NoError(t, err)
	cs := c.(*csclient)

	kvOpts := cs.newkvOptions("z1", "namespace")
	require.Equal(t, "", kvOpts.CacheFileFn()(kvOpts.Prefix()))

	cs.opts = cs.opts.SetCacheDir("/cacheDir")
	kvOpts = cs.newkvOptions("z1")
	require.Equal(t, "/cacheDir/test_app_z1.json", kvOpts.CacheFileFn()(kvOpts.Prefix()))
	kvOpts = cs.newkvOptions("z1", "namespace")
	require.Equal(t, "/cacheDir/namespace_test_app_z1.json", kvOpts.CacheFileFn()(kvOpts.Prefix()))

	kvOpts = cs.newkvOptions("z1", "namespace", "")
	require.Equal(t, "/cacheDir/namespace_test_app_z1.json", kvOpts.CacheFileFn()(kvOpts.Prefix()))

	kvOpts = cs.newkvOptions("z1", "namespace", "env")
	require.Equal(t, "/cacheDir/namespace_env_test_app_z1.json", kvOpts.CacheFileFn()(kvOpts.Prefix()))
}

func TestValidateNamespace(t *testing.T) {
	inputs := []struct {
		ns        string
		result    string
		expectErr bool
	}{
		{
			ns:        "ns",
			result:    "/ns",
			expectErr: false,
		},
		{
			ns:        "/ns",
			result:    "/ns",
			expectErr: false,
		},
		{
			ns:        "/ns/ab",
			result:    "/ns/ab",
			expectErr: false,
		},
		{
			ns:        "ns/ab",
			result:    "/ns/ab",
			expectErr: false,
		},
		{
			ns:        "_ns",
			result:    "",
			expectErr: true,
		},
		{
			ns:        "/_ns",
			result:    "",
			expectErr: true,
		},
		{
			ns:        "",
			result:    "",
			expectErr: true,
		},
		{
			ns:        "/",
			result:    "",
			expectErr: true,
		},
	}

	for _, input := range inputs {
		rs, err := validateTopLevelNamespace(input.ns)
		if input.expectErr {
			require.Error(t, err)
			continue
		}
		require.Equal(t, input.result, rs)
	}
}

func testOptions() Options {
	return NewOptions().SetClusters([]Cluster{
		NewCluster().SetZone("zone1").SetEndpoints([]string{"i1"}),
		NewCluster().SetZone("zone2").SetEndpoints([]string{"i2"}),
		NewCluster().SetZone("zone3").SetEndpoints([]string{"i3"}),
	}).SetService("test_app").SetZone("zone1")
}

func testNewETCDFn(t *testing.T) (newClientFn, func()) {
	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	ec := ecluster.RandClient()

	newFn := func(endpoints []string) (*clientv3.Client, error) {
		return ec, nil
	}

	closer := func() {
		ecluster.Terminate(t)
	}

	return newFn, closer
}
