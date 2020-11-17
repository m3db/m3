// Copyright (c) 2020 Uber Technologies, Inc.
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

package discovery

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/dbnode/environment"
	"github.com/m3db/m3/src/x/config"
)

func TestM3DBSingleNodeType(t *testing.T) {
	in := `
type: m3db_single_node
`

	hostID := "test_id"
	envConfig := getEnvConfig(t, in, hostID)

	assert.Equal(t, 1, len(envConfig.Services))
	assert.Equal(t, 1, len(envConfig.SeedNodes.InitialCluster))

	s := envConfig.Services[0].Service
	assert.Equal(t, defaultM3DBService, s.Service)
	assert.Equal(t, defaultEnvironment, s.Env)
	assert.Equal(t, defaultZone, s.Zone)
	assert.Equal(t, defaultCacheDirectory, s.CacheDir)
	assert.Equal(t, 1, len(s.ETCDClusters))
	assert.Equal(t, defaultZone, s.ETCDClusters[0].Zone)
	assert.Equal(t, 1, len(s.ETCDClusters[0].Endpoints))
	assert.Equal(t, defaultSingleNodeClusterEndpoint, s.ETCDClusters[0].Endpoints[0])

	c := envConfig.SeedNodes.InitialCluster[0]
	assert.Equal(t, defaultSingleNodeClusterSeedEndpoint, c.Endpoint)
	assert.Equal(t, hostID, c.HostID)
}

func TestM3DBClusterType(t *testing.T) {
	in := `
type: m3db_cluster
m3dbCluster:
  env: a
  zone: b
  endpoints:
    - end_1
    - end_2
`

	envConfig := getEnvConfig(t, in, "")
	validateClusterConfig(t, envConfig, defaultM3DBService)
}

func TestM3AggregatorClusterType(t *testing.T) {
	in := `
type: m3aggregator_cluster
m3AggregatorCluster:
  env: a
  zone: b
  endpoints:
    - end_1
    - end_2
`

	envConfig := getEnvConfig(t, in, "")
	validateClusterConfig(t, envConfig, defaultM3AggregatorService)
}

func TestConfigType(t *testing.T) {
	in := `
config:
    service:
        env: test_env
        zone: test_zone
        service: test_service
        cacheDir: test/cache
        etcdClusters:
            - zone: test_zone_2
              endpoints:
                  - 127.0.0.1:2379
    seedNodes:
        initialCluster:
            - hostID: host_id
              endpoint: http://127.0.0.1:2380
`

	hostID := "test_id"
	envConfig := getEnvConfig(t, in, hostID)

	assert.Equal(t, 1, len(envConfig.Services))
	assert.Equal(t, 1, len(envConfig.SeedNodes.InitialCluster))

	s := envConfig.Services[0].Service
	assert.Equal(t, "test_service", s.Service)
	assert.Equal(t, "test_env", s.Env)
	assert.Equal(t, "test_zone", s.Zone)
	assert.Equal(t, "test/cache", s.CacheDir)
	assert.Equal(t, 1, len(s.ETCDClusters))
	assert.Equal(t, "test_zone_2", s.ETCDClusters[0].Zone)
	assert.Equal(t, 1, len(s.ETCDClusters[0].Endpoints))
	assert.Equal(t, "127.0.0.1:2379", s.ETCDClusters[0].Endpoints[0])

	c := envConfig.SeedNodes.InitialCluster[0]
	assert.Equal(t, "http://127.0.0.1:2380", c.Endpoint)
	assert.Equal(t, "host_id", c.HostID)
}

func getEnvConfig(t *testing.T, in string, hostID string) environment.Configuration {
	fd, err := ioutil.TempFile("", "config.yaml")
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fd.Name()))
	}()

	_, err = fd.Write([]byte(in))
	assert.NoError(t, err)

	var cfg Configuration
	err = config.LoadFile(&cfg, fd.Name(), config.Options{})
	assert.NoError(t, err)

	envConfig, err := cfg.EnvironmentConfig(hostID)
	assert.NoError(t, err)

	return envConfig
}

func validateClusterConfig(t *testing.T,
	envConfig environment.Configuration,
	expectedService string,
) {
	assert.Equal(t, 1, len(envConfig.Services))
	assert.Nil(t, envConfig.SeedNodes)
	s := envConfig.Services[0].Service
	assert.Equal(t, expectedService, s.Service)
	assert.Equal(t, "a", s.Env)
	assert.Equal(t, "b", s.Zone)
	assert.Equal(t, defaultCacheDirectory, s.CacheDir)
	assert.Equal(t, 1, len(s.ETCDClusters))
	assert.Equal(t, "b", s.ETCDClusters[0].Zone)
	assert.Equal(t, 2, len(s.ETCDClusters[0].Endpoints))
	assert.Equal(t, "end_1", s.ETCDClusters[0].Endpoints[0])
	assert.Equal(t, "end_2", s.ETCDClusters[0].Endpoints[1])
}
