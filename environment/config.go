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

	"github.com/m3db/m3cluster/client"
	etcdclient "github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/kvconfig"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultSDTimeout = 30 * time.Second
)

var (
	errNilRetention  = errors.New("namespace retention options cannot be empty")
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
	Namespaces     []StaticNamespaceConfiguration `yaml:"namespaces"`
	TopologyConfig *topology.StaticConfiguration  `yaml:"topology"`
	ListenAddress  string                         `yaml:"listenAddress"`
}

// StaticNamespaceConfiguration sets the static namespace
type StaticNamespaceConfiguration struct {
	Name      string                    `yaml:"name"`
	Options   *StaticNamespaceOptions   `yaml:"options"`
	Retention *StaticNamespaceRetention `yaml:"retention"`
}

// StaticNamespaceOptions sets namespace options- if nil, default is used
type StaticNamespaceOptions struct {
	BootstrapEnabled  bool `yaml:"bootstrapEnabled"`
	FlushEnabled      bool `yaml:"flushEnabled"`
	SnapshotEnabled   bool `yaml:"snapshotEnabled"`
	WritesToCommitLog bool `yaml:"writesToCommitLog"`
	CleanupEnabled    bool `yaml:"cleanupEnabled"`
	RepairEnabled     bool `yaml:"repairEnabled"`
}

// StaticNamespaceRetention sets the retention per namespace (required)
type StaticNamespaceRetention struct {
	RetentionPeriod                     time.Duration `yaml:"retentionPeriod"`
	BlockSize                           time.Duration `yaml:"blockSize"`
	BufferFuture                        time.Duration `yaml:"bufferFuture"`
	BufferPast                          time.Duration `yaml:"bufferPast"`
	BlockDataExpiry                     bool          `yaml:"blockDataExpiry"`
	BlockDataExpiryAfterNotAccessPeriod time.Duration `yaml:"blockDataExpiryAfterNotAccessPeriod"`
}

// ConfigureResults stores initializers and kv store for dynamic and static configs
type ConfigureResults struct {
	NamespaceInitializer namespace.Initializer
	TopologyInitializer  topology.Initializer
	KVStore              kv.Store
}

// ConfigurationParameters are options used to create new ConfigureResults
type ConfigurationParameters struct {
	InstrumentOpts             instrument.Options
	HashingSeed                uint32
	HostID                     string
	NamespaceResolutionTimeout time.Duration
	TopologyResolutionTimeout  time.Duration
}

// Configure creates a new ConfigureResults
func (c Configuration) Configure(cfgParams ConfigurationParameters) (ConfigureResults, error) {
	var emptyConfig ConfigureResults

	sdTimeout := defaultSDTimeout
	if initTimeout := c.Service.SDConfig.InitTimeout; initTimeout != nil && *initTimeout != 0 {
		sdTimeout = *initTimeout
	}

	configSvcClientOpts := c.Service.NewOptions().
		SetInstrumentOptions(cfgParams.InstrumentOpts).
		SetServicesOptions(c.Service.SDConfig.NewOptions().SetInitTimeout(sdTimeout))
	configSvcClient, err := etcdclient.NewConfigServiceClient(configSvcClientOpts)
	if err != nil {
		err = fmt.Errorf("could not create m3cluster client: %v", err)
		return emptyConfig, err
	}

	if c.Service != nil && c.Static != nil {
		return emptyConfig, errInvalidConfig
	}

	if c.Service != nil {
		return c.configureDynamic(configSvcClient, cfgParams)
	}

	if c.Static != nil {
		return c.configureStatic(configSvcClient, cfgParams)
	}

	return emptyConfig, errInvalidConfig
}

func (c Configuration) configureDynamic(configSvcClient client.Client, cfgParams ConfigurationParameters) (ConfigureResults, error) {
	dynamicOpts := namespace.NewDynamicOptions().
		SetInstrumentOptions(cfgParams.InstrumentOpts).
		SetConfigServiceClient(configSvcClient).
		SetNamespaceRegistryKey(kvconfig.NamespacesKey).
		SetInitTimeout(cfgParams.NamespaceResolutionTimeout)
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
		SetInitTimeout(cfgParams.TopologyResolutionTimeout)
	topoInit := topology.NewDynamicInitializer(topoOpts)

	kv, err := configSvcClient.KV()
	if err != nil {
		err = fmt.Errorf("could not create KV client, %v", err)
		return ConfigureResults{}, err
	}

	configureResults := ConfigureResults{
		NamespaceInitializer: nsInit,
		TopologyInitializer:  topoInit,
		KVStore:              kv,
	}
	return configureResults, nil
}

func (c Configuration) configureStatic(configSvcClient client.Client, cfgParams ConfigurationParameters) (ConfigureResults, error) {
	var emptyConfig ConfigureResults

	nsList := []namespace.Metadata{}
	for _, ns := range c.Static.Namespaces {
		md, err := newNamespaceMetadata(ns)
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

	kv, err := configSvcClient.KV()
	if err != nil {
		err = fmt.Errorf("could not create KV client, %v", err)
		return emptyConfig, err
	}

	configureResults := ConfigureResults{
		NamespaceInitializer: nsInitStatic,
		TopologyInitializer:  topoInit,
		KVStore:              kv,
	}
	return configureResults, nil
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

func newNamespaceMetadata(cfg StaticNamespaceConfiguration) (namespace.Metadata, error) {
	if cfg.Retention == nil {
		return nil, errNilRetention
	}
	if cfg.Options == nil {
		cfg.Options = &StaticNamespaceOptions{
			BootstrapEnabled:  true,
			CleanupEnabled:    true,
			FlushEnabled:      true,
			SnapshotEnabled:   true,
			RepairEnabled:     true,
			WritesToCommitLog: true,
		}
	}
	md, err := namespace.NewMetadata(
		ident.StringID(cfg.Name),
		namespace.NewOptions().
			SetBootstrapEnabled(cfg.Options.BootstrapEnabled).
			SetCleanupEnabled(cfg.Options.CleanupEnabled).
			SetFlushEnabled(cfg.Options.FlushEnabled).
			SetSnapshotEnabled(cfg.Options.SnapshotEnabled).
			SetRepairEnabled(cfg.Options.RepairEnabled).
			SetWritesToCommitLog(cfg.Options.WritesToCommitLog).
			SetRetentionOptions(
				retention.NewOptions().
					SetBlockSize(cfg.Retention.BlockSize).
					SetRetentionPeriod(cfg.Retention.RetentionPeriod).
					SetBufferFuture(cfg.Retention.BufferFuture).
					SetBufferPast(cfg.Retention.BufferPast).
					SetBlockDataExpiry(cfg.Retention.BlockDataExpiry).
					SetBlockDataExpiryAfterNotAccessedPeriod(cfg.Retention.BlockDataExpiryAfterNotAccessPeriod)))
	if err != nil {
		return nil, err
	}

	return md, nil
}
