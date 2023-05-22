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
	"testing"
	"time"

	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

var initTimeout = time.Minute

func TestConfigureStatic(t *testing.T) {
	tests := []struct {
		name       string
		staticTopo *topology.StaticConfiguration
		expectErr  bool
	}{
		{
			name: "0 replicas; 1 host",
			staticTopo: &topology.StaticConfiguration{
				Shards:   2,
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
			name: "1 replica; 1 host",
			staticTopo: &topology.StaticConfiguration{
				Shards:   2,
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
			name: "1 replica; 3 hosts",
			staticTopo: &topology.StaticConfiguration{
				Shards:   2,
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
			name: "3 replicas; 3 hosts",
			staticTopo: &topology.StaticConfiguration{
				Shards:   2,
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
			name: "3 replicas; 1 host",
			staticTopo: &topology.StaticConfiguration{
				Shards:   2,
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
			expectErr: true,
		},
		{
			name: "3 replicas; 5 hosts",
			staticTopo: &topology.StaticConfiguration{
				Shards:   2,
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
						etcdclient.ClusterConfig{
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
