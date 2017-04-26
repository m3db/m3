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

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/integration"
	"github.com/m3db/m3cluster/services"
	"github.com/stretchr/testify/assert"
)

func TestETCDClientGen(t *testing.T) {
	cs, err := NewConfigServiceClient(testOptions())
	assert.NoError(t, err)

	c := cs.(*csclient)
	// zone3 does not exist
	_, err = c.etcdClientGen("zone3")
	assert.Error(t, err)
	assert.Equal(t, 0, len(c.clis))

	c1, err := c.etcdClientGen("zone1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(c.clis))

	c2, err := c.etcdClientGen("zone2")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(c.clis))
	assert.NotEqual(t, c1, c2)

	c1Again, err := c.etcdClientGen("zone1")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(c.clis))
	assert.Equal(t, c1, c1Again)
}

func TestKVAndHeartbeatServiceSharingETCDClient(t *testing.T) {
	sid := services.NewServiceID().SetName("s1")

	cs, err := NewConfigServiceClient(testOptions().SetZone("zone1"))
	assert.NoError(t, err)

	c := cs.(*csclient)

	_, err = c.KV()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(c.clis))

	_, err = c.heartbeatGen()(sid.SetZone("zone1"))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(c.clis))

	_, err = c.heartbeatGen()(sid.SetZone("zone2"))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(c.clis))

	_, err = c.heartbeatGen()(sid.SetZone("zone3"))
	assert.Error(t, err)
	assert.Equal(t, 2, len(c.clis))
}

func TestClient(t *testing.T) {
	_, err := NewConfigServiceClient(NewOptions())
	assert.Error(t, err)

	cs, err := NewConfigServiceClient(testOptions())
	assert.NoError(t, err)
	_, err = cs.KV()
	assert.Error(t, err)

	cs, err = NewConfigServiceClient(testOptions().SetZone("zone1"))
	c := cs.(*csclient)

	fn, closer := testNewETCDFn(t)
	defer closer()
	c.newFn = fn

	txn, err := c.Txn()
	assert.NoError(t, err)

	kv1, err := c.KV()
	assert.NoError(t, err)
	assert.Equal(t, kv1, txn)

	kv2, err := c.KV()
	assert.NoError(t, err)
	assert.Equal(t, kv1, kv2)
	// KV store will create an etcd cli for local zone only
	assert.Equal(t, 1, len(c.clis))
	_, ok := c.clis["zone1"]
	assert.True(t, ok)

	sd1, err := c.Services()
	assert.NoError(t, err)

	sd2, err := c.Services()
	assert.NoError(t, err)
	assert.Equal(t, sd1, sd2)

	err = sd1.SetMetadata(
		services.NewServiceID().SetName("service").SetZone("zone1"),
		services.NewMetadata(),
	)
	assert.NoError(t, err)
	// etcd cli for zone1 will be reused
	assert.Equal(t, 1, len(c.clis))

	err = sd1.SetMetadata(
		services.NewServiceID().SetName("service").SetZone("zone2"),
		services.NewMetadata(),
	)
	assert.NoError(t, err)
	// etcd cli for zone2 will be created since the request is going to zone2
	assert.Equal(t, 2, len(c.clis))
	_, ok = c.clis["zone2"]
	assert.True(t, ok)

	err = sd1.SetMetadata(
		services.NewServiceID().SetName("service").SetZone("zone3"),
		services.NewMetadata(),
	)
	// does not have etcd cluster for zone3
	assert.Error(t, err)
	assert.Equal(t, 2, len(c.clis))
}

func TestCacheFileForZone(t *testing.T) {
	assert.Equal(t, "", cacheFileForZone("", "app", "zone"))
	assert.Equal(t, "", cacheFileForZone("/dir", "", "zone"))
	assert.Equal(t, "", cacheFileForZone("/dir", "app", ""))
	assert.Equal(t, "/dir/app-zone.json", cacheFileForZone("/dir", "app", "zone"))
	assert.Equal(t, "/dir/a_b-c_d.json", cacheFileForZone("/dir", "a/b", "c/d"))
}

func TestPrefix(t *testing.T) {
	assert.Equal(t, "_kv/test/", prefix("test"))
	assert.Equal(t, "_kv/", prefix(""))
}

func testOptions() Options {
	return NewOptions().SetClusters([]Cluster{
		NewCluster().SetZone("zone1").SetEndpoints([]string{"i1"}),
		NewCluster().SetZone("zone2").SetEndpoints([]string{"i2"}),
	}).SetService("test_app")
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
