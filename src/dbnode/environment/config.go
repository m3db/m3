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
	"os"
	"sort"
	"time"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/kv"
	m3clusterkvmem "github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/kvconfig"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/instrument"
)

var (
	errInvalidConfig    = errors.New("must supply either service or static config")
	errInvalidSyncCount = errors.New("must supply exactly one synchronous cluster")
)

// Configuration is a configuration that can be used to create namespaces, a topology, and kv store
type Configuration struct {
	// Services is used when a topology initializer is not supplied.
	Services DynamicConfiguration `yaml:"services"`

	// StaticConfiguration is used for running M3DB with static configs
	Statics StaticConfiguration `yaml:"statics"`

	// Presence of a (etcd) server in this config denotes an embedded cluster
	SeedNodes *SeedNodesConfig `yaml:"seedNodes"`
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
	HostID       string `yaml:"hostID"`
	Endpoint     string `yaml:"endpoint"`
	ClusterState string `yaml:"clusterState"`
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

// DynamicConfiguration is used for running M3DB with a dynamic config
type DynamicConfiguration []*DynamicCluster

// DynamicCluster is a single cluster in a dynamic configuration
type DynamicCluster struct {
	Async           bool                      `yaml:"async"`
	ClientOverrides ClientOverrides           `yaml:"clientOverrides"`
	Service         *etcdclient.Configuration `yaml:"service"`
}

// ClientOverrides represents M3DB client overrides for a given cluster.
type ClientOverrides struct {
	HostQueueFlushInterval   *time.Duration `yaml:"hostQueueFlushInterval"`
	TargetHostQueueFlushSize *int           `yaml:"targetHostQueueFlushSize"`
}

// Validate validates the DynamicConfiguration.
func (c DynamicConfiguration) Validate() error {
	syncCount := 0
	for _, cfg := range c {
		if !cfg.Async {
			syncCount++
		}
		if cfg.ClientOverrides.TargetHostQueueFlushSize != nil && *cfg.ClientOverrides.TargetHostQueueFlushSize <= 1 {
			return fmt.Errorf("target host queue flush size must be larger than zero but was: %d", cfg.ClientOverrides.TargetHostQueueFlushSize)
		}

		if cfg.ClientOverrides.HostQueueFlushInterval != nil && *cfg.ClientOverrides.HostQueueFlushInterval <= 0 {
			return fmt.Errorf("host queue flush interval must be larger than zero but was: %s", cfg.ClientOverrides.HostQueueFlushInterval.String())
		}
	}
	if syncCount != 1 {
		return errInvalidSyncCount
	}
	return nil
}

// SyncCluster returns the synchronous cluster in the DynamicConfiguration
func (c DynamicConfiguration) SyncCluster() (*DynamicCluster, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	for _, cluster := range c {
		if !cluster.Async {
			return cluster, nil
		}
	}
	return nil, errInvalidSyncCount
}

// StaticConfiguration is used for running M3DB with a static config
type StaticConfiguration []*StaticCluster

// StaticCluster is a single cluster in a static configuration
type StaticCluster struct {
	Async           bool                              `yaml:"async"`
	ClientOverrides ClientOverrides                   `yaml:"clientOverrides"`
	Namespaces      []namespace.MetadataConfiguration `yaml:"namespaces"`
	TopologyConfig  *topology.StaticConfiguration     `yaml:"topology"`
	ListenAddress   string                            `yaml:"listenAddress"`
}

// Validate validates the StaticConfiguration
func (c StaticConfiguration) Validate() error {
	syncCount := 0
	for _, cfg := range c {
		if !cfg.Async {
			syncCount++
		}
		if cfg.ClientOverrides.TargetHostQueueFlushSize != nil && *cfg.ClientOverrides.TargetHostQueueFlushSize <= 1 {
			return fmt.Errorf("target host queue flush size must be larger than zero but was: %d", cfg.ClientOverrides.TargetHostQueueFlushSize)
		}

		if cfg.ClientOverrides.HostQueueFlushInterval != nil && *cfg.ClientOverrides.HostQueueFlushInterval <= 0 {
			return fmt.Errorf("host queue flush interval must be larger than zero but was: %s", cfg.ClientOverrides.HostQueueFlushInterval.String())
		}
	}
	if syncCount != 1 {
		return errInvalidSyncCount
	}
	return nil
}

// ConfigureResult stores initializers and kv store for dynamic and static configs
type ConfigureResult struct {
	NamespaceInitializer namespace.Initializer
	TopologyInitializer  topology.Initializer
	ClusterClient        clusterclient.Client
	KVStore              kv.Store
	Async                bool
	ClientOverrides      ClientOverrides
}

// ConfigureResults stores initializers and kv store for dynamic and static configs
type ConfigureResults []ConfigureResult

// SyncCluster returns the synchronous cluster in the ConfigureResults
func (c ConfigureResults) SyncCluster() (ConfigureResult, error) {
	for _, result := range c {
		if !result.Async {
			return result, nil
		}
	}
	return ConfigureResult{}, errInvalidSyncCount
}

// ConfigurationParameters are options used to create new ConfigureResults
type ConfigurationParameters struct {
	InterruptedCh          <-chan struct{}
	InstrumentOpts         instrument.Options
	HashingSeed            uint32
	HostID                 string
	NewDirectoryMode       os.FileMode
	ForceColdWritesEnabled bool
	// AllowEmptyInitialNamespaceRegistry determines whether to allow the initial
	// namespace update to be empty or to wait indefinitely until namespaces are received.
	// This is used when configuring the namespaceInitializer.
	AllowEmptyInitialNamespaceRegistry bool
}

// UnmarshalYAML normalizes the config into a list of services.
func (c *Configuration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var cfg struct {
		Services  DynamicConfiguration      `yaml:"services"`
		Service   *etcdclient.Configuration `yaml:"service"`
		Static    *StaticCluster            `yaml:"static"`
		Statics   StaticConfiguration       `yaml:"statics"`
		SeedNodes *SeedNodesConfig          `yaml:"seedNodes"`
	}

	if err := unmarshal(&cfg); err != nil {
		return err
	}

	c.SeedNodes = cfg.SeedNodes
	c.Statics = cfg.Statics
	if cfg.Static != nil {
		c.Statics = StaticConfiguration{cfg.Static}
	}
	c.Services = cfg.Services
	if cfg.Service != nil {
		c.Services = DynamicConfiguration{
			&DynamicCluster{Service: cfg.Service},
		}
	}

	return nil
}

// Validate validates the configuration.
func (c *Configuration) Validate() error {
	if (c.Services == nil && c.Statics == nil) ||
		(len(c.Services) > 0 && len(c.Statics) > 0) {
		return errInvalidConfig
	}

	if len(c.Services) > 0 {
		if err := c.Services.Validate(); err != nil {
			return err
		}
	}

	if len(c.Statics) > 0 {
		if err := c.Statics.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Configure creates a new ConfigureResults
func (c Configuration) Configure(cfgParams ConfigurationParameters) (ConfigureResults, error) {
	var emptyConfig ConfigureResults

	// Validate here rather than UnmarshalYAML since we need to ensure one of
	// dynamic or static configuration are provided. A blank YAML does not
	// call UnmarshalYAML and therefore validation would be skipped.
	if err := c.Validate(); err != nil {
		return emptyConfig, err
	}

	if len(c.Services) > 0 {
		return c.configureDynamic(cfgParams)
	}

	if len(c.Statics) > 0 {
		return c.configureStatic(cfgParams)
	}

	return emptyConfig, errInvalidConfig
}

func (c Configuration) configureDynamic(cfgParams ConfigurationParameters) (ConfigureResults, error) {
	var emptyConfig ConfigureResults
	if err := c.Services.Validate(); err != nil {
		return emptyConfig, err
	}

	cfgResults := make(ConfigureResults, 0, len(c.Services))
	for _, cluster := range c.Services {
		configSvcClientOpts := cluster.Service.NewOptions().
			SetInstrumentOptions(cfgParams.InstrumentOpts).
			// Set timeout to zero so it will wait indefinitely for the
			// initial value.
			SetServicesOptions(services.NewOptions().SetInitTimeout(0)).
			SetNewDirectoryMode(cfgParams.NewDirectoryMode)
		configSvcClient, err := etcdclient.NewConfigServiceClient(configSvcClientOpts)
		if err != nil {
			err = fmt.Errorf("could not create m3cluster client: %v", err)
			return emptyConfig, err
		}

		dynamicOpts := namespace.NewDynamicOptions().
			SetInstrumentOptions(cfgParams.InstrumentOpts).
			SetConfigServiceClient(configSvcClient).
			SetNamespaceRegistryKey(kvconfig.NamespacesKey).
			SetForceColdWritesEnabled(cfgParams.ForceColdWritesEnabled).
			SetAllowEmptyInitialNamespaceRegistry(cfgParams.AllowEmptyInitialNamespaceRegistry)
		nsInit := namespace.NewDynamicInitializer(dynamicOpts)

		serviceID := services.NewServiceID().
			SetName(cluster.Service.Service).
			SetEnvironment(cluster.Service.Env).
			SetZone(cluster.Service.Zone)

		topoOpts := topology.NewDynamicOptions().
			SetConfigServiceClient(configSvcClient).
			SetServiceID(serviceID).
			SetQueryOptions(services.NewQueryOptions().
				SetIncludeUnhealthy(true).
				SetInterruptedCh(cfgParams.InterruptedCh)).
			SetInstrumentOptions(cfgParams.InstrumentOpts).
			SetHashGen(sharding.NewHashGenWithSeed(cfgParams.HashingSeed))
		topoInit := topology.NewDynamicInitializer(topoOpts)

		kv, err := configSvcClient.KV()
		if err != nil {
			err = fmt.Errorf("could not create KV client, %v", err)
			return emptyConfig, err
		}

		result := ConfigureResult{
			NamespaceInitializer: nsInit,
			TopologyInitializer:  topoInit,
			ClusterClient:        configSvcClient,
			KVStore:              kv,
			Async:                cluster.Async,
			ClientOverrides:      cluster.ClientOverrides,
		}
		cfgResults = append(cfgResults, result)
	}

	return cfgResults, nil
}

func (c Configuration) configureStatic(cfgParams ConfigurationParameters) (ConfigureResults, error) {
	var emptyConfig ConfigureResults

	if err := c.Statics.Validate(); err != nil {
		return emptyConfig, err
	}

	cfgResults := make(ConfigureResults, 0, len(c.Services))
	for _, cluster := range c.Statics {
		nsList := []namespace.Metadata{}
		for _, ns := range cluster.Namespaces {
			md, err := ns.Metadata()
			if err != nil {
				err = fmt.Errorf("unable to create metadata for static config: %v", err)
				return emptyConfig, err
			}
			nsList = append(nsList, md)
		}
		// NB(bodu): Force cold writes to be enabled for all ns if specified.
		if cfgParams.ForceColdWritesEnabled {
			nsList = namespace.ForceColdWritesEnabledForMetadatas(nsList)
		}

		nsInitStatic := namespace.NewStaticInitializer(nsList)

		numReplicas := cluster.TopologyConfig.Replicas
		if numReplicas == 0 {
			numReplicas = 1
		}

		shardSet, hostShardSets, err := newStaticShardSet(
			cluster.TopologyConfig.Shards,
			numReplicas,
			cluster.TopologyConfig.Hosts,
		)
		if err != nil {
			err = fmt.Errorf("unable to create shard set for static config: %v", err)
			return emptyConfig, err
		}
		staticOptions := topology.NewStaticOptions().
			SetHostShardSets(hostShardSets).
			SetShardSet(shardSet).
			SetReplicas(numReplicas)

		topoInit := topology.NewStaticInitializer(staticOptions)
		result := ConfigureResult{
			NamespaceInitializer: nsInitStatic,
			TopologyInitializer:  topoInit,
			KVStore:              m3clusterkvmem.NewStore(),
			Async:                cluster.Async,
			ClientOverrides:      cluster.ClientOverrides,
		}
		cfgResults = append(cfgResults, result)
	}

	return cfgResults, nil
}

func newStaticShardSet(
	numShards int,
	rf int,
	hosts []topology.HostShardConfig,
) (sharding.ShardSet, []topology.HostShardSet, error) {
	var (
		shardSet sharding.ShardSet
		shardIDs []uint32
		err      error
	)

	for i := uint32(0); i < uint32(numShards); i++ {
		shardIDs = append(shardIDs, i)
	}

	shards := sharding.NewShards(shardIDs, shard.Available)
	shardSet, err = sharding.NewShardSet(shards, sharding.DefaultHashFn(numShards))
	if err != nil {
		return nil, nil, err
	}

	hostShardSets, err := generatePlacement(hosts, numShards, rf)
	if err != nil {
		return nil, nil, err
	}

	return shardSet, hostShardSets, nil
}

func generatePlacement(hosts []topology.HostShardConfig, numShards int, rf int) ([]topology.HostShardSet, error) {
	numHosts := len(hosts)
	if numHosts == 0 || numShards < 1 || rf < 1 {
		return nil, errors.New("number of hosts, shards, and RF must be positive")
	}
	if rf > numHosts {
		return nil, errors.New("number of hosts must be >=RF")
	}

	hostShards := make([][]shard.Shard, numHosts)
	hostIdx := 0
	// Round robin assign shard replicas to hosts.
	for shardInt := uint32(0); shardInt < uint32(numShards); shardInt++ {
		for replica := 0; replica < rf; replica++ {
			newShard := shard.NewShard(shardInt).SetState(shard.Available)
			hostShards[hostIdx] = append(hostShards[hostIdx], newShard)
			hostIdx = (hostIdx + 1) % numHosts
		}
	}

	hostShardSets := make([]topology.HostShardSet, 0, numHosts)
	sortedHosts := make([]topology.HostShardConfig, numHosts)
	// Plain copy is okay because struct just contains strings.
	copy(sortedHosts, hosts)
	sort.Slice(sortedHosts, func(i, j int) bool { return sortedHosts[i].HostID < sortedHosts[j].HostID })
	for i, host := range sortedHosts {
		host := topology.NewHost(host.HostID, host.ListenAddress)
		shardSet, err := sharding.NewShardSet(hostShards[i], sharding.DefaultHashFn(numShards))
		if err != nil {
			return nil, fmt.Errorf("error constructing new ShardSet: %w", err)
		}
		hostShardSet := topology.NewHostShardSet(host, shardSet)
		hostShardSets = append(hostShardSets, hostShardSet)
	}

	return hostShardSets, nil
}
