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

	"gopkg.in/yaml.v2"

	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/assert"
)

var initTimeout = time.Minute

func TestConfigureStatic(t *testing.T) {
	config := Configuration{
		Static: &StaticConfiguration{
			Namespaces: []namespace.MetadataConfiguration{
				namespace.MetadataConfiguration{
					ID: "metrics",
					Retention: retention.Configuration{
						RetentionPeriod: 24 * time.Hour,
						BlockSize:       time.Hour,
					},
				},
				namespace.MetadataConfiguration{
					ID: "other-metrics",
					Retention: retention.Configuration{
						RetentionPeriod: 24 * time.Hour,
						BlockSize:       time.Hour,
					},
				},
			},
			TopologyConfig: &topology.StaticConfiguration{
				Shards: 2,
				Hosts: []topology.HostShardConfig{
					topology.HostShardConfig{
						HostID:        "localhost",
						ListenAddress: "0.0.0.0:1234",
					},
				},
			},
			ListenAddress: "0.0.0.0:9000",
		},
	}

	configRes, err := config.Configure(ConfigurationParameters{})
	assert.NotNil(t, configRes)
	assert.NoError(t, err)
}

func TestConfigureDynamic(t *testing.T) {
	config := Configuration{
		Services: []*etcdclient.Configuration{
			&etcdclient.Configuration{
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
	assert.Len(t, cfg.Services, 1)
}

func TestUnmarshalDynamicList(t *testing.T) {
	in := `
services:
  - zone: dca8
    env: test
  - zone: phx3
    env: test
    async: true
`

	var cfg Configuration
	err := yaml.Unmarshal([]byte(in), &cfg)
	assert.NoError(t, err)
	assert.Len(t, cfg.Services, 2)
}

func TestUnmarshalDynamicValidation(t *testing.T) {
	in := `
services:
  - zone: dca8
    env: test
  - zone: phx3
    env: test
`

	var cfg Configuration
	err := yaml.Unmarshal([]byte(in), &cfg)
	assert.Error(t, err)
}
