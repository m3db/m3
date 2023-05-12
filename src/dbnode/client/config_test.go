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

package client

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/environment"
	"github.com/m3db/m3/src/dbnode/topology"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/retry"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfiguration(t *testing.T) {
	in := `
writeConsistencyLevel: majority
readConsistencyLevel: unstrict_majority
iterateEqualTimestampStrategy: iterate_lowest_value
connectConsistencyLevel: any
writeTimeout: 10s
fetchTimeout: 15s
connectTimeout: 20s
writeRetry:
    initialBackoff: 500ms
    backoffFactor: 3
    maxRetries: 2
    jitter: true
fetchRetry:
    initialBackoff: 500ms
    backoffFactor: 2
    maxRetries: 3
    jitter: true
backgroundHealthCheckFailLimit: 4
backgroundHealthCheckFailThrottleFactor: 0.5
hashing:
  seed: 42
proto:
  enabled: false
  schema_registry:
    "ns1:2d":
      schemaFilePath: "/path/to/schema"
      messageName: "ns1_msg_name"
    ns2:
      schemaDeployID: "deployID-345"
      messageName: "ns2_msg_name"
`

	fd, err := ioutil.TempFile("", "config.yaml")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fd.Name()))
	}()

	_, err = fd.Write([]byte(in))
	require.NoError(t, err)

	var cfg Configuration
	err = xconfig.LoadFile(&cfg, fd.Name(), xconfig.Options{})
	require.NoError(t, err)

	var (
		levelMajority        = topology.ConsistencyLevelMajority
		readUnstrictMajority = topology.ReadConsistencyLevelUnstrictMajority
		iterateLowestValue   = encoding.IterateLowestValue
		connectAny           = topology.ConnectConsistencyLevelAny
		second10             = 10 * time.Second
		second15             = 15 * time.Second
		second20             = 20 * time.Second
		num4                 = 4
		numHalf              = 0.5
		boolTrue             = true
	)

	expected := Configuration{
		WriteConsistencyLevel:         &levelMajority,
		ReadConsistencyLevel:          &readUnstrictMajority,
		IterateEqualTimestampStrategy: &iterateLowestValue,
		ConnectConsistencyLevel:       &connectAny,
		WriteTimeout:                  &second10,
		FetchTimeout:                  &second15,
		ConnectTimeout:                &second20,
		WriteRetry: &retry.Configuration{
			InitialBackoff: 500 * time.Millisecond,
			BackoffFactor:  3,
			MaxRetries:     2,
			Jitter:         &boolTrue,
		},
		FetchRetry: &retry.Configuration{
			InitialBackoff: 500 * time.Millisecond,
			BackoffFactor:  2,
			MaxRetries:     3,
			Jitter:         &boolTrue,
		},
		BackgroundHealthCheckFailLimit:          &num4,
		BackgroundHealthCheckFailThrottleFactor: &numHalf,
		HashingConfiguration: &HashingConfiguration{
			Seed: 42,
		},
		Proto: &ProtoConfiguration{
			Enabled: false,
			SchemaRegistry: map[string]NamespaceProtoSchema{
				"ns1:2d": {SchemaFilePath: "/path/to/schema", MessageName: "ns1_msg_name"},
				"ns2":    {SchemaDeployID: "deployID-345", MessageName: "ns2_msg_name"},
			},
		},
	}

	assert.Equal(t, expected, cfg)
}

func TestConfigurationWithAuth(t *testing.T) {
	clientCfg := `
  config:
    service:
      env: default_env
      zone: embedded
      service: m3db
      auth:
          enabled: true
          username: user_db
          password: pass_db
      etcdClusters:
        - zone: embedded
          endpoints:
            - etcd:2379
            - etcd:456
          auth:
            enabled: true
            username: user_etcd
            password: pass_etcd
  writeConsistencyLevel: majority
  readConsistencyLevel: unstrict_majority
`
	fd, err := os.CreateTemp("", "config.yaml")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fd.Name()))
	}()

	_, err = fd.Write([]byte(clientCfg))
	require.NoError(t, err)

	var cfg Configuration
	err = xconfig.LoadFile(&cfg, fd.Name(), xconfig.Options{})
	require.NoError(t, err)

	var (
		levelMajority        = topology.ConsistencyLevelMajority
		readUnstrictMajority = topology.ReadConsistencyLevelUnstrictMajority
	)

	expected := Configuration{
		EnvironmentConfig: &environment.Configuration{
			Services: []*environment.DynamicCluster{
				{
					Service: &etcdclient.Configuration{
						Env:     "default_env",
						Zone:    "embedded",
						Service: "m3db",
						Auth: &etcdclient.AuthConfig{
							Enabled:  true,
							UserName: "user_db",
							Password: "pass_db",
						},
						ETCDClusters: []etcdclient.ClusterConfig{
							{
								Zone:      "embedded",
								Endpoints: []string{"etcd:2379", "etcd:456"},
								Auth: &etcdclient.AuthConfig{
									Enabled:  true,
									UserName: "user_etcd",
									Password: "pass_etcd",
								},
							},
						},
					},
				},
			},
		},
		WriteConsistencyLevel: &levelMajority,
		ReadConsistencyLevel:  &readUnstrictMajority,
	}

	assert.Equal(t, expected, cfg)
}

func TestConfigurationWithAuthMultiple(t *testing.T) {
	clientCfg := `
  config:
    services:
       - service:
          env: default_env1
          zone: embedded1
          service: m3db
          auth:
              enabled: true
              username: user_db1
              password: pass_db1
          etcdClusters:
            - zone: embedded1
              endpoints:
                - etcd:2379
                - etcd:7756
              auth:
                enabled: true
                username: user_etcd1
                password: pass_etcd1
       - service:
          env: default_env2
          zone: embedded2
          service: m3db
          auth:
              enabled: true
              username: user_db2
              password: pass_db2
          etcdClusters:
            - zone: embedded2
              endpoints:
                - etcd:2379
                - etcd:7756
              auth:
                enabled: true
                username: user_etcd2
                password: pass_etcd2      
  writeConsistencyLevel: majority
  readConsistencyLevel: unstrict_majority
`
	fd, err := os.CreateTemp("", "config.yaml")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fd.Name()))
	}()

	_, err = fd.Write([]byte(clientCfg))
	require.NoError(t, err)

	var cfg Configuration
	err = xconfig.LoadFile(&cfg, fd.Name(), xconfig.Options{})
	require.NoError(t, err)

	var (
		levelMajority        = topology.ConsistencyLevelMajority
		readUnstrictMajority = topology.ReadConsistencyLevelUnstrictMajority
	)

	expected := Configuration{
		EnvironmentConfig: &environment.Configuration{
			Services: []*environment.DynamicCluster{
				{
					Service: &etcdclient.Configuration{
						Env:     "default_env1",
						Zone:    "embedded1",
						Service: "m3db",
						Auth: &etcdclient.AuthConfig{
							Enabled:  true,
							UserName: "user_db1",
							Password: "pass_db1",
						},
						ETCDClusters: []etcdclient.ClusterConfig{
							{
								Zone:      "embedded1",
								Endpoints: []string{"etcd:2379", "etcd:7756"},
								Auth: &etcdclient.AuthConfig{
									Enabled:  true,
									UserName: "user_etcd1",
									Password: "pass_etcd1",
								},
							},
						},
					},
				}, {
					Service: &etcdclient.Configuration{
						Env:     "default_env2",
						Zone:    "embedded2",
						Service: "m3db",
						Auth: &etcdclient.AuthConfig{
							Enabled:  true,
							UserName: "user_db2",
							Password: "pass_db2",
						},
						ETCDClusters: []etcdclient.ClusterConfig{
							{
								Zone:      "embedded2",
								Endpoints: []string{"etcd:2379", "etcd:7756"},
								Auth: &etcdclient.AuthConfig{
									Enabled:  true,
									UserName: "user_etcd2",
									Password: "pass_etcd2",
								},
							},
						},
					},
				},
			},
		},
		WriteConsistencyLevel: &levelMajority,
		ReadConsistencyLevel:  &readUnstrictMajority,
	}

	assert.Equal(t, expected, cfg)
}
