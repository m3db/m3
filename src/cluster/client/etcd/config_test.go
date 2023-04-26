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

package etcd

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestKeepAliveConfig(t *testing.T) {
	const cfgStr = `
enabled: true
period: 10s
jitter: 5s
timeout: 1s
`

	var cfg KeepAliveConfig
	require.NoError(t, yaml.Unmarshal([]byte(cfgStr), &cfg))

	opts := cfg.NewOptions()
	require.Equal(t, true, opts.KeepAliveEnabled())
	require.Equal(t, 10*time.Second, opts.KeepAlivePeriod())
	require.Equal(t, 5*time.Second, opts.KeepAlivePeriodMaxJitter())
	require.Equal(t, time.Second, opts.KeepAliveTimeout())
}

func TestAuthConfig(t *testing.T) {
	const cfgStr = `
enabled: true
username: "test"
password: "test"
`

	var cfg AuthConfig
	require.NoError(t, yaml.Unmarshal([]byte(cfgStr), &cfg))

	opts := cfg.NewOptions()
	require.Equal(t, true, opts.AuthenticationEnabled())
	require.Equal(t, "test", opts.UserName())
	require.Equal(t, "test", opts.Password())
}

func TestConfig(t *testing.T) {
	const testUserName = "test"
	const testPassword = "test"
	const testConfig = `
env: env1
zone: z1
service: service1
cacheDir: /tmp/cache.json
watchWithRevision: 1
etcdClusters:
  - zone: z1
    endpoints:
      - etcd1:2379
      - etcd2:2379
    keepAlive:
      enabled: true
      period: 10s
      jitter: 5s
      timeout: 1s
	auth:
	  enabled: true
      username: test
      password: test
    autoSyncInterval: 160s
    dialTimeout: 42s
  - zone: z2
    endpoints:
      - etcd3:2379
      - etcd4:2379
    tls:
      crtPath: foo.crt.pem
      keyPath: foo.key.pem
  - zone: z3
    endpoints:
      - etcd5:2379
      - etcd6:2379
    tls:
      crtPath: foo.crt.pem
      keyPath: foo.key.pem
      caCrtPath: foo_ca.pem
m3sd:
  initTimeout: 10s
`

	var cfg Configuration
	require.NoError(t, yaml.Unmarshal([]byte(testConfig), &cfg))

	require.Equal(t, "env1", cfg.Env)
	require.Equal(t, "z1", cfg.Zone)
	require.Equal(t, "service1", cfg.Service)
	require.Equal(t, "/tmp/cache.json", cfg.CacheDir)
	require.Equal(t, int64(1), cfg.WatchWithRevision)
	require.Equal(t, []ClusterConfig{
		{
			Zone:      "z1",
			Endpoints: []string{"etcd1:2379", "etcd2:2379"},
			KeepAlive: &KeepAliveConfig{
				Enabled: true,
				Period:  10 * time.Second,
				Jitter:  5 * time.Second,
				Timeout: time.Second,
			},
			AutoSyncInterval: 160 * time.Second,
			DialTimeout:      42 * time.Second,
			Auth: &AuthConfig{
				Enabled: true,
				UserName: testUserName,
				Password: testPassword,
			},
		},
		{
			Zone:      "z2",
			Endpoints: []string{"etcd3:2379", "etcd4:2379"},
			TLS: &TLSConfig{
				CrtPath: "foo.crt.pem",
				KeyPath: "foo.key.pem",
			},
			Auth: &AuthConfig{
				Enabled: false,
				UserName: defaultUsername,
				Password: defaultPassword,
			},
		},
		{
			Zone:      "z3",
			Endpoints: []string{"etcd5:2379", "etcd6:2379"},
			TLS: &TLSConfig{
				CrtPath:   "foo.crt.pem",
				KeyPath:   "foo.key.pem",
				CACrtPath: "foo_ca.pem",
			},
		},
	}, cfg.ETCDClusters)
	require.Equal(t, 10*time.Second, *cfg.SDConfig.InitTimeout)

	opts := cfg.NewOptions()
	cluster1, exists := opts.ClusterForZone("z1")
	require.True(t, exists)
	keepAliveOpts := cluster1.KeepAliveOptions()
	require.Equal(t, true, keepAliveOpts.KeepAliveEnabled())
	require.Equal(t, 10*time.Second, keepAliveOpts.KeepAlivePeriod())
	require.Equal(t, 5*time.Second, keepAliveOpts.KeepAlivePeriodMaxJitter())
	require.Equal(t, time.Second, keepAliveOpts.KeepAliveTimeout())
	require.Equal(t, 160*time.Second, cluster1.AutoSyncInterval())
	require.Equal(t, 42*time.Second, cluster1.DialTimeout())

	authOpts := cluster1.AuthOptions()
	require.Equal(t, testUserName, authOpts.UserName())
	require.Equal(t, testPassword, authOpts.Password())
	require.Equal(t, true, authOpts.AuthenticationEnabled())

	cluster2, exists := opts.ClusterForZone("z2")
	require.True(t, exists)
	keepAliveOpts = cluster2.KeepAliveOptions()
	require.Equal(t, true, keepAliveOpts.KeepAliveEnabled())
	require.Equal(t, 20*time.Second, keepAliveOpts.KeepAlivePeriod())
	require.Equal(t, 10*time.Second, keepAliveOpts.KeepAlivePeriodMaxJitter())
	require.Equal(t, 10*time.Second, keepAliveOpts.KeepAliveTimeout())

	authOpts = cluster2.AuthOptions()
	require.Equal(t, defaultUsername, authOpts.UserName())
	require.Equal(t, defaultPassword, authOpts.Password())
	require.Equal(t, false, authOpts.AuthenticationEnabled())

	t.Run("TestOptionsNewDirectoryMode", func(t *testing.T) {
		opts := cfg.NewOptions()
		require.Equal(t, defaultDirectoryMode, opts.NewDirectoryMode())

		const testConfigWithDir = `
env: env1
zone: z1
service: service1
cacheDir: /tmp/cache.json
watchWithRevision: 1
newDirectoryMode: 0744
etcdClusters:
  - zone: z1
    endpoints:
      - etcd1:2379
      - etcd2:2379
    keepAlive:
      enabled: true
      period: 10s
      jitter: 5s
      timeout: 1s
    autoSyncInterval: 60s
m3sd:
  initTimeout: 10s
`
		var cfg2 Configuration
		require.NoError(t, yaml.Unmarshal([]byte(testConfigWithDir), &cfg2))
		require.Equal(t, os.FileMode(0744), *cfg2.NewDirectoryMode)
	})
}

func TestDefaultConfig(t *testing.T) {
	cluster := ClusterConfig{}.NewCluster()
	require.Equal(t, defaultDialTimeout, cluster.DialTimeout())
	require.Equal(t, defaultAutoSyncInterval, cluster.AutoSyncInterval())
	require.Equal(t, defaultAuthEnabled , cluster.AuthOptions().AuthenticationEnabled())
	require.Equal(t, defaultPassword , cluster.AuthOptions().Password())
	require.Equal(t, defaultUsername , cluster.AuthOptions().UserName())
}

func TestConfig_negativeAutosync(t *testing.T) {
	cluster := ClusterConfig{
		AutoSyncInterval: -5,
	}.NewCluster()
	require.Equal(t, time.Duration(-5), cluster.AutoSyncInterval())
}
