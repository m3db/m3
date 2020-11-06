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

package environment

import (
	"errors"
	"fmt"

	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/dbnode/environment"
	"github.com/m3db/m3/src/x/config/hostid"
)

const (
	defaultEnvironment                   = "default_env"
	defaultZone                          = "embedded"
	defaultM3DBService                   = "m3db"
	defaultM3AggregatorService           = "m3aggregator"
	defaultCacheDirectory                = "/var/lib/m3kv"
	defaultSingleNodeClusterEndpoint     = "127.0.0.1:2379"
	defaultSingleNodeClusterSeedEndpoint = "127.0.0.1:2380"

	defaultDiscoveryConfigType = ConfigType
)

var validDiscoveryConfigTypes = []DiscoveryConfigurationType{
	ConfigType,
	M3DBSingleNodeType,
	M3DBClusterType,
	M3AggregatorClusterType,
}

// DiscoveryConfigurationType defines the type of discovery configuration.
type DiscoveryConfigurationType uint

const (
	// ConfigType defines a generic definition for service discovery via etcd.
	ConfigType DiscoveryConfigurationType = iota
	// M3DBSingleNodeType defines configuration for a single M3DB node via etcd.
	M3DBSingleNodeType
	// M3DBClusterType defines M3DB discovery via etcd.
	M3DBClusterType
	// M3AggregatorClusterType defines M3DB discovery via etcd.
	M3AggregatorClusterType
)

// UnmarshalYAML unmarshals an DiscoveryConfigurationType into a valid type from string.
func (t *DiscoveryConfigurationType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	// If unspecified, use default mode.
	if str == "" {
		*t = defaultDiscoveryConfigType
		return nil
	}

	for _, valid := range validDiscoveryConfigTypes {
		if str == valid.String() {
			*t = valid
			return nil
		}
	}
	return fmt.Errorf("invalid DiscoveryConfigurationType '%s' valid types are: %s",
		str, validDiscoveryConfigTypes)
}

// String returns the discovery configuration type as a string.
func (t DiscoveryConfigurationType) String() string {
	switch t {
	case ConfigType:
		return "config"
	case M3DBSingleNodeType:
		return "m3db_single_node"
	case M3DBClusterType:
		return "m3db_cluster"
	case M3AggregatorClusterType:
		return "m3aggregator_cluster"
	}
	return "unknown"
}

// Configuration defines how services are to be discovered.
type Configuration struct {
	// Type defines the type of discovery configuration being used.
	Type *DiscoveryConfigurationType `yaml:"type"`

	// M3DBCluster defines M3DB discovery via etcd.
	M3DBCluster *M3DBClusterDiscoveryConfiguration `yaml:"m3dbCluster"`

	// M3AggregatorCluster defines M3Aggregator discovery via etcd.
	M3AggregatorCluster *M3AggregatorClusterDiscoveryConfiguration `yaml:"m3AggregatorCluster"`

	// Config defines a generic definition for service discovery via etcd.
	Config *environment.Configuration `yaml:"config"`
}

// M3DBClusterDiscoveryConfiguration defines discovery configuration for M3DB.
type M3DBClusterDiscoveryConfiguration struct {
	Env       string   `yaml:"env" validate:"nonzero"`
	Zone      *string  `yaml:"zone"`
	Endpoints []string `yaml:"endpoints"`
}

// M3AggregatorClusterDiscoveryConfiguration defines discovery configuration for M3Aggregator.
type M3AggregatorClusterDiscoveryConfiguration struct {
	Env       string   `yaml:"env"`
	Zone      *string  `yaml:"zone"`
	Endpoints []string `yaml:"endpoints"`
}

// EnvironmentConfiguration provides the environment configuration
// based on the type of discovery configuration set.
func (c *Configuration) EnvironmentConfiguration(
	hostID hostid.Configuration,
) (environment.Configuration, error) {
	discoveryConfigType := defaultDiscoveryConfigType
	if c.Type != nil {
		discoveryConfigType = *c.Type
	}

	switch discoveryConfigType {
	case ConfigType:
		return *c.Config, nil
	case M3DBSingleNodeType:
		return c.m3dbSingleNodeEnvConfig(hostID)
	case M3DBClusterType:
		return c.M3DBCluster.envConfig()
	case M3AggregatorClusterType:
		return c.M3AggregatorCluster.envConfig()
	}

	return environment.Configuration{}, fmt.Errorf("unrecognized discovery type: %d", c.Type)
}

func (c *Configuration) m3dbSingleNodeEnvConfig(
	hostID hostid.Configuration,
) (environment.Configuration, error) {
	resolvedHostID, err := hostID.Resolve()
	if err != nil {
		return environment.Configuration{}, err
	}
	return environment.Configuration{
		Services: []*environment.DynamicCluster{
			{
				Service: &etcdclient.Configuration{
					Service:  defaultM3DBService,
					CacheDir: defaultCacheDirectory,
					Zone:     defaultZone,
					Env:      defaultEnvironment,
					ETCDClusters: []etcdclient.ClusterConfig{
						etcdclient.ClusterConfig{
							Zone:      defaultZone,
							Endpoints: []string{defaultSingleNodeClusterEndpoint},
						},
					},
				},
			},
		},
		SeedNodes: &environment.SeedNodesConfig{
			InitialCluster: []environment.SeedNode{
				{
					HostID:   resolvedHostID,
					Endpoint: defaultSingleNodeClusterSeedEndpoint,
				},
			},
		},
	}, nil
}

func (c *M3DBClusterDiscoveryConfiguration) envConfig() (environment.Configuration, error) {
	if c == nil {
		return environment.Configuration{},
			errors.New("discovery type specified required m3dbCluster section")
	}

	zone := defaultZone
	if c.Zone != nil {
		zone = *c.Zone
	}

	return environment.Configuration{
		Services: []*environment.DynamicCluster{
			{
				Service: &etcdclient.Configuration{
					Service:  defaultM3DBService,
					CacheDir: defaultCacheDirectory,
					Zone:     zone,
					Env:      c.Env,
					ETCDClusters: []etcdclient.ClusterConfig{
						etcdclient.ClusterConfig{
							Zone:      zone,
							Endpoints: c.Endpoints,
						},
					},
				},
			},
		},
	}, nil
}

func (c *M3AggregatorClusterDiscoveryConfiguration) envConfig() (
	environment.Configuration, error) {
	if c == nil {
		return environment.Configuration{},
			errors.New("discovery type specified required m3AggregatorCluster section")
	}

	zone := defaultZone
	if c.Zone != nil {
		zone = *c.Zone
	}

	return environment.Configuration{
		Services: []*environment.DynamicCluster{
			{
				Service: &etcdclient.Configuration{
					Service:  defaultM3AggregatorService,
					CacheDir: defaultCacheDirectory,
					Zone:     zone,
					Env:      c.Env,
					ETCDClusters: []etcdclient.ClusterConfig{
						etcdclient.ClusterConfig{
							Zone:      zone,
							Endpoints: c.Endpoints,
						},
					},
				},
			},
		},
	}, nil
}
