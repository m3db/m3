// Copyright (c) 2018 Uber Technologies, Inc.
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

package environment

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"

	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/instrument"
)

var initTimeout = time.Minute

func TestConfigureStatic(t *testing.T) {
	tests := []struct {
		name       string
		staticTopo *topology.StaticConfiguration
		expectErr  bool
	}{
		{
			name: "0 replicas get defaulted to 1",
			staticTopo: &topology.StaticConfiguration{
				Shards:   32,
				Replicas: 0,
				Hosts: []topology.HostShardConfig{
					{
						HostID:        "localhost",
						ListenAddress: "0.0.0.0:1234",
					},
				},
			},
			expectErr: false,
		},
		{
			name: "1 replica, 1 host",
			staticTopo: &topology.StaticConfiguration{
				Shards:   32,
				Replicas: 1,
				Hosts: []topology.HostShardConfig{
					{
						HostID:        "localhost",
						ListenAddress: "0.0.0.0:1234",
					},
				},
			},
			expectErr: false,
		},
		{
			name: "1 replica, 3 hosts",
			staticTopo: &topology.StaticConfiguration{
				Shards:   32,
				Replicas: 1,
				Hosts: []topology.HostShardConfig{
					{
						HostID:        "host0",
						ListenAddress: "0.0.0.0:1000",
					},
					{
						HostID:        "host1",
						ListenAddress: "0.0.0.0:1001",
					},
					{
						HostID:        "host2",
						ListenAddress: "0.0.0.0:1002",
					},
				},
			},
			expectErr: false,
		},
		{
			name: "3 replicas, 3 hosts",
			staticTopo: &topology.StaticConfiguration{
				Shards:   32,
				Replicas: 3,
				Hosts: []topology.HostShardConfig{
					{
						HostID:        "host0",
						ListenAddress: "0.0.0.0:1000",
					},
					{
						HostID:        "host1",
						ListenAddress: "0.0.0.0:1001",
					},
					{
						HostID:        "host2",
						ListenAddress: "0.0.0.0:1002",
					},
				},
			},
			expectErr: false,
		},
		{
			name: "3 replicas, 5 hosts",
			staticTopo: &topology.StaticConfiguration{
				Shards:   32,
				Replicas: 3,
				Hosts: []topology.HostShardConfig{
					{
						HostID:        "host0",
						ListenAddress: "0.0.0.0:1000",
					},
					{
						HostID:        "host1",
						ListenAddress: "0.0.0.0:1001",
					},
					{
						HostID:        "host2",
						ListenAddress: "0.0.0.0:1002",
					},
					{
						HostID:        "host3",
						ListenAddress: "0.0.0.0:1003",
					},
					{
						HostID:        "host4",
						ListenAddress: "0.0.0.0:1004",
					},
				},
			},
			expectErr: false,
		},
		{
			name: "invalid: replicas > hosts",
			staticTopo: &topology.StaticConfiguration{
				Shards:   32,
				Replicas: 3,
				Hosts: []topology.HostShardConfig{
					{
						HostID:        "host0",
						ListenAddress: "0.0.0.0:1000",
					},
				},
			},
			expectErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := Configuration{
				Statics: StaticConfiguration{
					&StaticCluster{
						Namespaces: []namespace.MetadataConfiguration{
							{
								ID: "metrics",
								Retention: retention.Configuration{
									RetentionPeriod: 24 * time.Hour,
									BlockSize:       time.Hour,
								},
							},
							{
								ID: "other-metrics",
								Retention: retention.Configuration{
									RetentionPeriod: 24 * time.Hour,
									BlockSize:       time.Hour,
								},
							},
						},
						ListenAddress:  "0.0.0.0:9000",
						TopologyConfig: test.staticTopo,
					},
				},
			}

			configRes, err := config.Configure(ConfigurationParameters{})
			if test.expectErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, configRes)
		})
	}
}

func TestGeneratePlacement(t *testing.T) {
	tests := []struct {
		name      string
		numHosts  int
		numShards int
		rf        int
		expectErr bool
	}{
		{
			name:      "1 host, 1 rf",
			numHosts:  1,
			numShards: 16,
			rf:        1,
			expectErr: false,
		},
		{
			name:      "3 hosts, 1 rf",
			numHosts:  3,
			numShards: 16,
			rf:        1,
			expectErr: false,
		},
		{
			name:      "3 hosts, 1 rf with more shards",
			numHosts:  3,
			numShards: 32,
			rf:        1,
			expectErr: false,
		},
		{
			name:      "3 hosts, 3 rf",
			numHosts:  3,
			numShards: 16,
			rf:        3,
			expectErr: false,
		},
		{
			name:      "5 hosts, 3 rf",
			numHosts:  5,
			numShards: 16,
			rf:        3,
			expectErr: false,
		},
		{
			name:      "prod-like cluster",
			numHosts:  100,
			numShards: 4096,
			rf:        3,
			expectErr: false,
		},
		{
			name:      "invalid: hosts < rf",
			numHosts:  2,
			numShards: 16,
			rf:        3,
			expectErr: true,
		},
		{
			name:      "invalid: hosts < rf 2",
			numHosts:  10,
			numShards: 16,
			rf:        11,
			expectErr: true,
		},
		{
			name:      "invalid: no hosts",
			numHosts:  0,
			numShards: 16,
			rf:        3,
			expectErr: true,
		},
		{
			name:      "invalid: 0 rf",
			numHosts:  10,
			numShards: 16,
			rf:        0,
			expectErr: true,
		},
		{
			name:      "invalid: no shards",
			numHosts:  10,
			numShards: 0,
			rf:        3,
			expectErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var hosts []topology.HostShardConfig
			for i := 0; i < test.numHosts; i++ {
				hosts = append(hosts, topology.HostShardConfig{
					HostID:        fmt.Sprintf("id%d", i),
					ListenAddress: fmt.Sprintf("id%d", i),
				})
			}

			hostShardSets, err := generatePlacement(hosts, test.numShards, test.rf)
			if test.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			var (
				minShardCount = test.numShards + 1
				maxShardCount = 0
				shardCounts   = make([]int, test.numShards)
			)
			for _, hostShardSet := range hostShardSets {
				ids := hostShardSet.ShardSet().AllIDs()
				if len(ids) < minShardCount {
					minShardCount = len(ids)
				}
				if len(ids) > maxShardCount {
					maxShardCount = len(ids)
				}

				for _, id := range ids {
					shardCounts[id]++
				}
			}

			// Assert balanced shard distribution
			assert.True(t, maxShardCount-minShardCount < 2)
			// Assert each shard has `rf` replicas
			for _, shardCount := range shardCounts {
				assert.Equal(t, test.rf, shardCount)
			}
		})
	}
}

func TestGeneratePlacementConsistency(t *testing.T) {
	// Asserts that the placement generated by `generatePlacement` is
	// deterministic even when the ordering of hosts passed in is in a
	// different order.
	var (
		numHosts  = 123
		numShards = 4096
		rf        = 3
		iters     = 10
		hosts     = make([]topology.HostShardConfig, 0, numHosts)
	)

	for i := 0; i < numHosts; i++ {
		hosts = append(hosts, topology.HostShardConfig{
			HostID:        fmt.Sprintf("id%d", i),
			ListenAddress: fmt.Sprintf("id%d", i),
		})
	}

	var pl []topology.HostShardSet
	for i := 0; i < iters; i++ {
		rand.Shuffle(len(hosts), func(i, j int) {
			hosts[i], hosts[j] = hosts[j], hosts[i]
		})

		hostShardSets, err := generatePlacement(hosts, numShards, rf)
		require.NoError(t, err)

		if i == 0 {
			pl = hostShardSets
		} else {
			assertHostShardSetsEqual(t, pl, hostShardSets)
		}
	}
}

// assertHostShardSetsEqual asserts that two HostShardSets are semantically
// equal. Mandates that the two HostShardSets are in the same order too.
func assertHostShardSetsEqual(t *testing.T, one, two []topology.HostShardSet) {
	require.Equal(t, len(one), len(two))

	for i := range one {
		oneHost := one[i].Host()
		twoHost := two[i].Host()
		assert.Equal(t, oneHost.ID(), twoHost.ID())
		assert.Equal(t, oneHost.Address(), twoHost.Address())

		oneIDs := one[i].ShardSet().All()
		twoIDs := two[i].ShardSet().All()
		require.Equal(t, len(oneIDs), len(twoIDs))
		for j := range oneIDs {
			assert.True(t, oneIDs[j].Equals(twoIDs[j]))
		}
	}
}

func TestConfigureDynamic(t *testing.T) {
	config := Configuration{
		Services: DynamicConfiguration{
			&DynamicCluster{
				Service: &etcdclient.Configuration{
					Zone:     "local",
					Env:      "test",
					Service:  "m3dbnode_test",
					CacheDir: "/",
					ETCDClusters: []etcdclient.ClusterConfig{
						{
							Zone:      "local",
							Endpoints: []string{"localhost:1111"},
						},
					},
					SDConfig: services.Configuration{
						InitTimeout: &initTimeout,
					},
				},
			},
		},
	}

	cfgParams := ConfigurationParameters{
		InstrumentOpts: instrument.NewOptions(),
	}

	configRes, err := config.Configure(cfgParams)
	assert.NotNil(t, configRes)
	assert.NoError(t, err)
}

func TestUnmarshalDynamicSingle(t *testing.T) {
	in := `
service:
  zone: dca8
  env: test
`

	var cfg Configuration
	err := yaml.Unmarshal([]byte(in), &cfg)
	assert.NoError(t, err)
	assert.NoError(t, cfg.Validate())
	assert.Len(t, cfg.Services, 1)
}

func TestUnmarshalDynamicList(t *testing.T) {
	in := `
services:
  - service:
      zone: dca8
      env: test
  - service:
      zone: phx3
      env: test
    async: true
`

	var cfg Configuration
	err := yaml.Unmarshal([]byte(in), &cfg)
	assert.NoError(t, err)
	assert.NoError(t, cfg.Validate())
	assert.Len(t, cfg.Services, 2)
}

var configValidationTests = []struct {
	name      string
	in        string
	expectErr error
}{
	{
		name:      "empty config",
		in:        ``,
		expectErr: errInvalidConfig,
	},
	{
		name: "static and dynamic",
		in: `
services:
  - service:
      zone: dca8
      env: test
statics:
  - listenAddress: 0.0.0.0:9000`,
		expectErr: errInvalidConfig,
	},
	{
		name: "invalid dynamic config",
		in: `
services:
  - async: true`,
		expectErr: errInvalidSyncCount,
	},
	{
		name: "invalid static config",
		in: `
statics:
  - async: true`,
		expectErr: errInvalidSyncCount,
	},
	{
		name: "valid config",
		in: `
services:
  - service:
      zone: dca8
      env: test
  - service:
      zone: phx3
      env: test
    async: true`,
		expectErr: nil,
	},
}

func TestConfigValidation(t *testing.T) {
	for _, tt := range configValidationTests {
		var cfg Configuration
		err := yaml.Unmarshal([]byte(tt.in), &cfg)
		assert.NoError(t, err)
		assert.Equal(t, tt.expectErr, cfg.Validate())
	}
}
