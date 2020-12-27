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
	"time"

	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeepAliveOptions(t *testing.T) {
	opts := NewKeepAliveOptions()
	require.Equal(t, defaultKeepAliveEnabled, opts.KeepAliveEnabled())
	require.Equal(t, defaultKeepAlivePeriod, opts.KeepAlivePeriod())
	require.Equal(t, defaultKeepAlivePeriodMaxJitter, opts.KeepAlivePeriodMaxJitter())
	require.Equal(t, defaultKeepAliveTimeout, opts.KeepAliveTimeout())

	opts = NewKeepAliveOptions().
		SetKeepAliveEnabled(true).
		SetKeepAlivePeriod(1234 * time.Second).
		SetKeepAlivePeriodMaxJitter(5000 * time.Second).
		SetKeepAliveTimeout(time.Hour)

	require.Equal(t, true, opts.KeepAliveEnabled())
	require.Equal(t, 1234*time.Second, opts.KeepAlivePeriod())
	require.Equal(t, 5000*time.Second, opts.KeepAlivePeriodMaxJitter())
	require.Equal(t, time.Hour, opts.KeepAliveTimeout())
}

func TestCluster(t *testing.T) {
	c := NewCluster()
	assert.Equal(t, "", c.Zone())
	assert.Equal(t, 0, len(c.Endpoints()))
	assert.Equal(t, NewTLSOptions(), c.TLSOptions())

	c = c.SetZone("z")
	assert.Equal(t, "z", c.Zone())
	assert.Equal(t, 0, len(c.Endpoints()))

	c = c.SetEndpoints([]string{"e1"})
	assert.Equal(t, "z", c.Zone())
	assert.Equal(t, []string{"e1"}, c.Endpoints())

	aOpts := NewTLSOptions().SetCrtPath("cert").SetKeyPath("key").SetCACrtPath("ca")
	c = c.SetTLSOptions(aOpts)
	assert.Equal(t, "z", c.Zone())
	assert.Equal(t, []string{"e1"}, c.Endpoints())
	assert.Equal(t, aOpts, c.TLSOptions())
	assert.Equal(t, defaultAutoSyncInterval, c.AutoSyncInterval())
	assert.Equal(t, defaultDialTimeout, c.DialTimeout())

	c = c.SetAutoSyncInterval(123 * time.Minute)
	assert.Equal(t, "z", c.Zone())
	assert.Equal(t, []string{"e1"}, c.Endpoints())
	assert.Equal(t, aOpts, c.TLSOptions())
	assert.Equal(t, 123*time.Minute, c.AutoSyncInterval())
	assert.Equal(t, defaultDialTimeout, c.DialTimeout())

	c = c.SetDialTimeout(42 * time.Hour)
	assert.Equal(t, "z", c.Zone())
	assert.Equal(t, []string{"e1"}, c.Endpoints())
	assert.Equal(t, aOpts, c.TLSOptions())
	assert.Equal(t, 123*time.Minute, c.AutoSyncInterval())
	assert.Equal(t, 42*time.Hour, c.DialTimeout())
}

func TestTLSOptions(t *testing.T) {
	aOpts := NewTLSOptions()
	assert.Equal(t, "", aOpts.CrtPath())
	assert.Equal(t, "", aOpts.KeyPath())
	assert.Equal(t, "", aOpts.CACrtPath())

	aOpts = aOpts.SetCrtPath("cert").SetKeyPath("key").SetCACrtPath("ca")
	assert.Equal(t, "cert", aOpts.CrtPath())
	assert.Equal(t, "key", aOpts.KeyPath())
	assert.Equal(t, "ca", aOpts.CACrtPath())
}

func TestOptions(t *testing.T) {
	opts := NewOptions()
	assert.Equal(t, "", opts.Zone())
	assert.Equal(t, "", opts.Env())
	assert.Equal(t,
		services.NewOptions().SetInstrumentsOptions(opts.ServicesOptions().InstrumentsOptions()),
		opts.ServicesOptions())
	assert.Equal(t, "", opts.CacheDir())
	assert.Equal(t, "", opts.Service())
	assert.Equal(t, []Cluster{}, opts.Clusters())
	_, ok := opts.ClusterForZone("z")
	assert.False(t, ok)
	assert.NotNil(t, opts.InstrumentOptions())
	assert.Equal(t, defaultRequestTimeout, opts.RequestTimeout())
	assert.Equal(t, defaultWatchChanCheckInterval, opts.WatchChanCheckInterval())
	assert.Equal(t, defaultWatchChanResetInterval, opts.WatchChanCheckInterval())
	assert.Equal(t, defaultWatchChanInitTimeout, opts.WatchChanInitTimeout())
	assert.False(t, opts.EnableFastGets())
	ropts := opts.RetryOptions()
	assert.Equal(t, defaultRetryJitter, ropts.Jitter())
	assert.Equal(t, defaultRetryInitialBackoff, ropts.InitialBackoff())
	assert.Equal(t, defaultRetryBackoffFactor, ropts.BackoffFactor())
	assert.Equal(t, defaultRetryMaxRetries, ropts.MaxRetries())
	assert.Equal(t, defaultRetryMaxBackoff, ropts.MaxBackoff())

	c1 := NewCluster().SetZone("z1")
	c2 := NewCluster().SetZone("z2")
	iopts := instrument.NewOptions().SetReportInterval(time.Minute)

	sdOpts := services.NewOptions().SetInitTimeout(time.Millisecond)
	opts = opts.SetEnv("env").
		SetZone("zone").
		SetServicesOptions(sdOpts).
		SetCacheDir("/dir").
		SetService("app").
		SetClusters([]Cluster{c1, c2}).
		SetInstrumentOptions(iopts).
		SetWatchWithRevision(1)
	assert.Equal(t, "env", opts.Env())
	assert.Equal(t, "zone", opts.Zone())
	assert.Equal(t, sdOpts, opts.ServicesOptions())
	assert.Equal(t, "/dir", opts.CacheDir())
	assert.Equal(t, "app", opts.Service())
	assert.Equal(t, 2, len(opts.Clusters()))
	assert.Equal(t, int64(1), opts.WatchWithRevision())
	c, ok := opts.ClusterForZone("z1")
	assert.True(t, ok)
	assert.Equal(t, c, c1)
	c, ok = opts.ClusterForZone("z2")
	assert.True(t, ok)
	assert.Equal(t, c, c2)
	assert.Equal(t, iopts, opts.InstrumentOptions())
}

func TestValidate(t *testing.T) {
	opts := NewOptions()
	assert.Error(t, opts.Validate())

	opts = opts.SetService("app")
	assert.Error(t, opts.Validate())

	c1 := NewCluster().SetZone("z1")
	c2 := NewCluster().SetZone("z2")
	opts = opts.SetClusters([]Cluster{c1, c2})
	assert.NoError(t, opts.Validate())

	opts = opts.SetInstrumentOptions(nil)
	assert.Error(t, opts.Validate())
}
