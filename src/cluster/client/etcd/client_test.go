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
	"os"
	"testing"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/services"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"
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
	require.False(t, c1 == c2)

	_, err = c.etcdClientGen("zone3")
	require.Error(t, err)
	require.Equal(t, 2, len(c.clis))

	// TODO(pwoodman): bit of a cop-out- this'll error no matter what as it's looking for
	// a file that won't be in the test environment. So, expect error.
	_, err = c.etcdClientGen("zone4")
	require.Error(t, err)

	_, err = c.etcdClientGen("zone5")
	require.Error(t, err)

	c1Again, err := c.etcdClientGen("zone1")
	require.NoError(t, err)
	require.Equal(t, 2, len(c.clis))
	require.True(t, c1 == c1Again)

	t.Run("TestNewDirectoryMode", func(t *testing.T) {
		require.Equal(t, defaultDirectoryMode, c.opts.NewDirectoryMode())

		expect := os.FileMode(0744)
		opts := testOptions().SetNewDirectoryMode(expect)
		require.Equal(t, expect, opts.NewDirectoryMode())
		cs, err := NewConfigServiceClient(opts)
		require.NoError(t, err)
		require.Equal(t, expect, cs.(*csclient).opts.NewDirectoryMode())
	})
}

func TestKVAndHeartbeatServiceSharingETCDClient(t *testing.T) {
	sid := services.NewServiceID().SetName("s1")

	cs, err := NewConfigServiceClient(testOptions().SetZone("zone1").SetEnv("env"))
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
	require.NoError(t, err)
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

	kv3, err := c.Store(kv.NewOverrideOptions().SetNamespace("ns").SetEnvironment("test_env1"))
	require.NoError(t, err)
	require.NotEqual(t, kv1, kv3)

	kv4, err := c.Store(kv.NewOverrideOptions().SetNamespace("ns"))
	require.NoError(t, err)
	require.NotEqual(t, kv3, kv4)

	// KV store will create an etcd cli for local zone only
	require.Equal(t, 1, len(c.clis))
	_, ok := c.clis["zone1"]
	require.True(t, ok)

	kv5, err := c.Store(kv.NewOverrideOptions().SetZone("zone2").SetNamespace("ns"))
	require.NoError(t, err)
	require.NotEqual(t, kv4, kv5)

	require.Equal(t, 2, len(c.clis))
	_, ok = c.clis["zone2"]
	require.True(t, ok)

	sd1, err := c.Services(nil)
	require.NoError(t, err)

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

func TestServicesWithNamespace(t *testing.T) {
	cs, err := NewConfigServiceClient(testOptions())
	require.NoError(t, err)
	c := cs.(*csclient)

	fn, closer := testNewETCDFn(t)
	defer closer()
	c.newFn = fn

	sd1, err := c.Services(services.NewOverrideOptions())
	require.NoError(t, err)

	nOpts := services.NewNamespaceOptions().SetPlacementNamespace("p").SetMetadataNamespace("m")
	sd2, err := c.Services(services.NewOverrideOptions().SetNamespaceOptions(nOpts))
	require.NoError(t, err)

	require.NotEqual(t, sd1, sd2)

	sid := services.NewServiceID().SetName("service").SetZone("zone2")
	err = sd1.SetMetadata(sid, services.NewMetadata())
	require.NoError(t, err)

	_, err = sd1.Metadata(sid)
	require.NoError(t, err)

	_, err = sd2.Metadata(sid)
	require.Error(t, err)

	sid2 := services.NewServiceID().SetName("service").SetZone("zone2").SetEnvironment("test")
	err = sd2.SetMetadata(sid2, services.NewMetadata())
	require.NoError(t, err)

	_, err = sd1.Metadata(sid2)
	require.Error(t, err)
}

func newOverrideOpts(zone, namespace, environment string) kv.OverrideOptions {
	return kv.NewOverrideOptions().
		SetZone(zone).
		SetNamespace(namespace).
		SetEnvironment(environment)
}

func TestCacheFileForZone(t *testing.T) {
	c, err := NewConfigServiceClient(testOptions())
	require.NoError(t, err)
	cs := c.(*csclient)

	kvOpts := cs.newkvOptions(newOverrideOpts("z1", "namespace", ""), cs.cacheFileFn())
	require.Equal(t, "", kvOpts.CacheFileFn()(kvOpts.Prefix()))

	cs.opts = cs.opts.SetCacheDir("/cacheDir")
	kvOpts = cs.newkvOptions(newOverrideOpts("z1", "", ""), cs.cacheFileFn())
	require.Equal(t, "/cacheDir/test_app_z1.json", kvOpts.CacheFileFn()(kvOpts.Prefix()))

	kvOpts = cs.newkvOptions(newOverrideOpts("z1", "namespace", ""), cs.cacheFileFn())
	require.Equal(t, "/cacheDir/namespace_test_app_z1.json", kvOpts.CacheFileFn()(kvOpts.Prefix()))

	kvOpts = cs.newkvOptions(newOverrideOpts("z1", "namespace", ""), cs.cacheFileFn())
	require.Equal(t, "/cacheDir/namespace_test_app_z1.json", kvOpts.CacheFileFn()(kvOpts.Prefix()))

	kvOpts = cs.newkvOptions(newOverrideOpts("z1", "namespace", "env"), cs.cacheFileFn())
	require.Equal(t, "/cacheDir/namespace_env_test_app_z1.json", kvOpts.CacheFileFn()(kvOpts.Prefix()))

	kvOpts = cs.newkvOptions(newOverrideOpts("z1", "namespace", ""), cs.cacheFileFn("f1", "", "f2"))
	require.Equal(t, "/cacheDir/namespace_test_app_z1_f1_f2.json", kvOpts.CacheFileFn()(kvOpts.Prefix()))

	kvOpts = cs.newkvOptions(newOverrideOpts("z2", "", ""), cs.cacheFileFn("/r2/m3agg"))
	require.Equal(t, "/cacheDir/test_app_z2__r2_m3agg.json", kvOpts.CacheFileFn()(kvOpts.Prefix()))
}

func TestSanitizeKVOverrideOptions(t *testing.T) {
	opts := testOptions()
	cs, err := NewConfigServiceClient(opts)
	require.NoError(t, err)

	client := cs.(*csclient)
	opts1, err := client.sanitizeOptions(kv.NewOverrideOptions())
	require.NoError(t, err)
	require.Equal(t, opts.Env(), opts1.Environment())
	require.Equal(t, opts.Zone(), opts1.Zone())
	require.Equal(t, kvPrefix, opts1.Namespace())
}

func TestReuseKVStore(t *testing.T) {
	opts := testOptions()
	cs, err := NewConfigServiceClient(opts)
	require.NoError(t, err)

	store1, err := cs.Txn()
	require.NoError(t, err)

	store2, err := cs.KV()
	require.NoError(t, err)
	require.Equal(t, store1, store2)

	store3, err := cs.Store(kv.NewOverrideOptions())
	require.NoError(t, err)
	require.Equal(t, store1, store3)

	store4, err := cs.TxnStore(kv.NewOverrideOptions())
	require.NoError(t, err)
	require.Equal(t, store1, store4)

	store5, err := cs.Store(kv.NewOverrideOptions().SetNamespace("foo"))
	require.NoError(t, err)
	require.NotEqual(t, store1, store5)

	store6, err := cs.TxnStore(kv.NewOverrideOptions().SetNamespace("foo"))
	require.NoError(t, err)
	require.Equal(t, store5, store6)

	client := cs.(*csclient)

	client.storeLock.Lock()
	require.Equal(t, 2, len(client.stores))
	client.storeLock.Unlock()
}

func TestGetEtcdClients(t *testing.T) {
	opts := testOptions()
	c, err := NewEtcdConfigServiceClient(opts)
	require.NoError(t, err)

	c1, err := c.etcdClientGen("zone2")
	require.NoError(t, err)
	require.Equal(t, 1, len(c.clis))

	c2, err := c.etcdClientGen("zone1")
	require.NoError(t, err)
	require.Equal(t, 2, len(c.clis))
	require.False(t, c1 == c2)

	clients := c.Clients()
	require.Len(t, clients, 2)

	assert.Equal(t, clients[0].Zone, "zone1")
	assert.Equal(t, clients[0].Client, c2)
	assert.Equal(t, clients[1].Zone, "zone2")
	assert.Equal(t, clients[1].Client, c1)
}

func TestValidateNamespace(t *testing.T) {
	inputs := []struct {
		ns        string
		expectErr bool
	}{
		{
			ns:        "ns",
			expectErr: false,
		},
		{
			ns:        "/ns",
			expectErr: false,
		},
		{
			ns:        "/ns/ab",
			expectErr: false,
		},
		{
			ns:        "ns/ab",
			expectErr: false,
		},
		{
			ns:        "_ns",
			expectErr: true,
		},
		{
			ns:        "/_ns",
			expectErr: true,
		},
		{
			ns:        "",
			expectErr: true,
		},
		{
			ns:        "/",
			expectErr: true,
		},
	}

	for _, input := range inputs {
		err := validateTopLevelNamespace(input.ns)
		if input.expectErr {
			require.Error(t, err)
		}
	}
}

func testOptions() Options {
	clusters := []Cluster{
		NewCluster().SetZone("zone1").SetEndpoints([]string{"i1"}),
		NewCluster().SetZone("zone2").SetEndpoints([]string{"i2"}),
		NewCluster().SetZone("zone3").SetEndpoints([]string{"i3"}).
			SetTLSOptions(NewTLSOptions().SetCrtPath("foo.crt.pem")),
		NewCluster().SetZone("zone4").SetEndpoints([]string{"i4"}).
			SetTLSOptions(NewTLSOptions().SetCrtPath("foo.crt.pem").SetKeyPath("foo.key.pem")),
		NewCluster().SetZone("zone5").SetEndpoints([]string{"i5"}).
			SetTLSOptions(NewTLSOptions().SetCrtPath("foo.crt.pem").SetKeyPath("foo.key.pem").SetCACrtPath("foo_ca.pem")),
	}
	return NewOptions().
		SetClusters(clusters).
		SetService("test_app").
		SetZone("zone1").
		SetEnv("env")
}

func testNewETCDFn(t *testing.T) (newClientFn, func()) {
	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	ec := ecluster.RandClient()

	newFn := func(Cluster) (*clientv3.Client, error) {
		return ec, nil
	}

	closer := func() {
		ecluster.Terminate(t)
	}

	return newFn, closer
}
