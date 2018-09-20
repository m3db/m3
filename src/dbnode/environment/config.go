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
	"time"

	"github.com/m3db/m3/src/dbnode/kvconfig"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/topology"
	clusterclient "github.com/m3db/m3cluster/client"
	etcdclient "github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3cluster/kv"
	m3clusterkvmem "github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultSDTimeout = time.Duration(0) // Wait indefinitely by default
)

var (
	errInvalidConfig = errors.New("must supply either service or static config")
)

// Configuration is a configuration that can be used to create namespaces, a topology, and kv store
type Configuration struct {
	// Service is used when a topology initializer is not supplied.
	Service *etcdclient.Configuration `yaml:"service"`

	// StaticConfiguration is used for running M3DB with a static config
	Static *StaticConfiguration `yaml:"static"`

	// Presence of a (etcd) server in this config denotes an embedded cluster
	SeedNodes *SeedNodesConfig `yaml:"seedNodes"`

	// NamespaceResolutionTimeout is the maximum time to wait to discover namespaces from KV
	NamespaceResolutionTimeout time.Duration `yaml:"namespaceResolutionTimeout"`

	// TopologyResolutionTimeout is the maximum time to wait for a topology from KV
	TopologyResolutionTimeout time.Duration `yaml:"topologyResolutionTimeout"`
}

// SeedNodesConfig defines fields for seed node
type SeedNodesConfig struct {
	RootDir                  string                 `yaml:"rootDir"`
	InitialAdvertisePeerUrls []string               `yaml:"initialAdvertisePeerUrls"`
	AdvertiseClientUrls      []string               `yaml:"advertiseClientUrls"`
	ListenPeerUrls           []string               `yaml:"listenPeerUrls"`
	ListenClientUrls         []string               `yaml:"listenClientUrls"`
	InitialCluster           []SeedNode             `yaml:"initialCluster"`
	ClientTransportSecurity  SeedNodeSecurityConfig `yaml:"clientTransportSecurity"`
	PeerTransportSecurity    SeedNodeSecurityConfig `yaml:"peerTransportSecurity"`
}

// SeedNode represents a seed node for the cluster
type SeedNode struct {
	HostID   string `yaml:"hostID"`
	Endpoint string `yaml:"endpoint"`
}

// SeedNodeSecurityConfig contains the data used for security in seed nodes
type SeedNodeSecurityConfig struct {
	CAFile        string `yaml:"caFile"`
	CertFile      string `yaml:"certFile"`
	KeyFile       string `yaml:"keyFile"`
	TrustedCAFile string `yaml:"trustedCaFile"`
	CertAuth      bool   `yaml:"clientCertAuth"`
	AutoTLS       bool   `yaml:"autoTls"`
}

// StaticConfiguration is used for running M3DB with a static config
type StaticConfiguration struct {
	Namespaces     []namespace.MetadataConfiguration `yaml:"namespaces"`
	TopologyConfig *topology.StaticConfiguration     `yaml:"topology"`
	ListenAddress  string                            `yaml:"listenAddress"`
}

// ConfigureResults stores initializers and kv store for dynamic and static configs
type ConfigureResults struct {
	NamespaceInitializer namespace.Initializer
	TopologyInitializer  topology.Initializer
	ClusterClient        clusterclient.Client
	KVStore              kv.Store
}

// ConfigurationParameters are options used to create new ConfigureResults
type ConfigurationParameters struct {
	InstrumentOpts    instrument.Options
	HashingSeed       uint32
	HostID            string
	ResolutionTimeout time.Duration
}

// Configure creates a new ConfigureResults
func (c Configuration) Configure(cfgParams ConfigurationParameters) (ConfigureResults, error) {
	var emptyConfig ConfigureResults

	if c.Service != nil && c.Static != nil {
		return emptyConfig, errInvalidConfig
	}

	if c.Service != nil {
		return c.configureDynamic(cfgParams)
	}

	if c.Static != nil {
		return c.configureStatic(cfgParams)
	}

	return emptyConfig, errInvalidConfig
}

func (c Configuration) configureDynamic(cfgParams ConfigurationParameters) (ConfigureResults, error) {
	configSvcClientOpts := c.Service.NewOptions().
		SetInstrumentOptions(cfgParams.InstrumentOpts).
		SetServicesOptions(services.NewOptions().SetInitTimeout(cfgParams.ResolutionTimeout))
	configSvcClient, err := etcdclient.NewConfigServiceClient(configSvcClientOpts)
	if err != nil {
		err = fmt.Errorf("could not create m3cluster client: %v", err)
		return ConfigureResults{}, err
	}

	dynamicOpts := namespace.NewDynamicOptions().
		SetInstrumentOptions(cfgParams.InstrumentOpts).
		SetConfigServiceClient(configSvcClient).
		SetNamespaceRegistryKey(kvconfig.NamespacesKey).
		SetInitTimeout(cfgParams.ResolutionTimeout)
	nsInit := namespace.NewDynamicInitializer(dynamicOpts)

	serviceID := services.NewServiceID().
		SetName(c.Service.Service).
		SetEnvironment(c.Service.Env).
		SetZone(c.Service.Zone)

	topoOpts := topology.NewDynamicOptions().
		SetConfigServiceClient(configSvcClient).
		SetServiceID(serviceID).
		SetQueryOptions(services.NewQueryOptions().SetIncludeUnhealthy(true)).
		SetInstrumentOptions(cfgParams.InstrumentOpts).
		SetHashGen(sharding.NewHashGenWithSeed(cfgParams.HashingSeed)).
		SetInitTimeout(cfgParams.ResolutionTimeout)
	topoInit := topology.NewDynamicInitializer(topoOpts)

	kv, err := configSvcClient.KV()
	if err != nil {
		err = fmt.Errorf("could not create KV client, %v", err)
		return ConfigureResults{}, err
	}

	return ConfigureResults{
		NamespaceInitializer: nsInit,
		TopologyInitializer:  topoInit,
		ClusterClient:        configSvcClient,
		KVStore:              kv,
	}, nil
}

func (c Configuration) configureStatic(cfgParams ConfigurationParameters) (ConfigureResults, error) {
	var emptyConfig ConfigureResults

	nsList := []namespace.Metadata{}
	for _, ns := range c.Static.Namespaces {
		md, err := ns.Metadata()
		if err != nil {
			err = fmt.Errorf("unable to create metadata for static config: %v", err)
			return emptyConfig, err
		}
		nsList = append(nsList, md)
	}

	nsInitStatic := namespace.NewStaticInitializer(nsList)

	shardSet, hostShardSets, err := newStaticShardSet(c.Static.TopologyConfig.Shards, c.Static.TopologyConfig.Hosts)
	if err != nil {
		err = fmt.Errorf("unable to create shard set for static config: %v", err)
		return emptyConfig, err
	}
	staticOptions := topology.NewStaticOptions().
		SetHostShardSets(hostShardSets).
		SetShardSet(shardSet)

	numHosts := len(c.Static.TopologyConfig.Hosts)
	numReplicas := c.Static.TopologyConfig.Replicas

	switch numReplicas {
	case 0:
		if numHosts != 1 {
			err := fmt.Errorf("number of hosts (%d) must be 1 if replicas is not set", numHosts)
			return emptyConfig, err
		}
		staticOptions = staticOptions.SetReplicas(1)
	default:
		if numHosts != numReplicas {
			err := fmt.Errorf("number of hosts (%d) not equal to number of replicas (%d)", numHosts, numReplicas)
			return emptyConfig, err
		}
		staticOptions = staticOptions.SetReplicas(c.Static.TopologyConfig.Replicas)
	}

	topoInit := topology.NewStaticInitializer(staticOptions)

	return ConfigureResults{
		NamespaceInitializer: nsInitStatic,
		TopologyInitializer:  topoInit,
		KVStore:              m3clusterkvmem.NewStore(),
	}, nil
}

func newStaticShardSet(numShards int, hosts []topology.HostShardConfig) (sharding.ShardSet, []topology.HostShardSet, error) {
	var (
		shardSet      sharding.ShardSet
		hostShardSets []topology.HostShardSet
		shardIDs      []uint32
		err           error
	)

	for i := uint32(0); i < uint32(numShards); i++ {
		shardIDs = append(shardIDs, i)
	}

	shards := sharding.NewShards(shardIDs, shard.Available)
	shardSet, err = sharding.NewShardSet(shards, sharding.DefaultHashFn(len(shards)))
	if err != nil {
		return nil, nil, err
	}

	for _, i := range hosts {
		host := topology.NewHost(i.HostID, i.ListenAddress)
		hostShardSet := topology.NewHostShardSet(host, shardSet)
		hostShardSets = append(hostShardSets, hostShardSet)
	}

	return shardSet, hostShardSets, nil
}
