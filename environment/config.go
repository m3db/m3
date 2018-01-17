package environment

import (
	"errors"
	"fmt"
	"time"

	etcdclient "github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3cluster/kv"
	m3clusterkvmem "github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/kvconfig"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/instrument"
)

var (
	errNilRetention  = errors.New("namespace retention options cannot be empty")
	errMissingConfig = errors.New("must supply service or static config")
)

// Configuration is a configuration that can be used to create namespaces, a topology, and kv store
type Configuration struct {
	// Service is used when a topology initializer is not supplied.
	Service *etcdclient.Configuration `yaml:"service"`

	// StaticConfiguration is used for running M3DB with a static config
	Static *StaticConfiguration `yaml:"static"`
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
	NeedsBootstrap      bool `yaml:"needsBootstrap"`
	NeedsFlush          bool `yaml:"needsFlush"`
	WritesToCommitLog   bool `yaml:"writesToCommitLog"`
	NeedsFilesetCleanup bool `yaml:"needsFilesetCleanup"`
	NeedsRepair         bool `yaml:"needsRepair"`
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

// Configure creates a new ConfigureResults
func (c Configuration) Configure(
	iopts instrument.Options,
	hashingSeed uint32,
) (ConfigureResults, error) {

	var emptyConfig ConfigureResults

	switch {
	case c.Service != nil:
		configSvcClientOpts := c.Service.NewOptions().
			SetInstrumentOptions(iopts)
		configSvcClient, err := etcdclient.NewConfigServiceClient(configSvcClientOpts)
		if err != nil {
			err = fmt.Errorf("could not create m3cluster client: %v", err)
			return emptyConfig, err
		}

		dynamicOpts := namespace.NewDynamicOptions().
			SetInstrumentOptions(iopts).
			SetConfigServiceClient(configSvcClient).
			SetNamespaceRegistryKey(kvconfig.NamespacesKey)
		nsInit := namespace.NewDynamicInitializer(dynamicOpts)

		serviceID := services.NewServiceID().
			SetName(c.Service.Service).
			SetEnvironment(c.Service.Env).
			SetZone(c.Service.Zone)

		topoOpts := topology.NewDynamicOptions().
			SetConfigServiceClient(configSvcClient).
			SetServiceID(serviceID).
			SetQueryOptions(services.NewQueryOptions().SetIncludeUnhealthy(true)).
			SetInstrumentOptions(iopts).
			SetHashGen(sharding.NewHashGenWithSeed(hashingSeed))
		topoInit := topology.NewDynamicInitializer(topoOpts)

		kv, err := configSvcClient.KV()
		if err != nil {
			err = fmt.Errorf("could not create KV client, %v", err)
			return emptyConfig, err
		}

		configureResults := ConfigureResults{
			NamespaceInitializer: nsInit,
			TopologyInitializer:  topoInit,
			KVStore:              kv,
		}
		return configureResults, nil

	case c.Static != nil:
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

		shardSet, hostShardSets, err := newStaticShardSet(c.Static.TopologyConfig.Shards, c.Static.ListenAddress)
		if err != nil {
			err = fmt.Errorf("unable to create shard set for static config: %v", err)
			return emptyConfig, err
		}
		staticOptions := topology.NewStaticOptions().
			SetReplicas(1).
			SetHostShardSets(hostShardSets).
			SetShardSet(shardSet)

		topoInit := topology.NewStaticInitializer(staticOptions)

		kv := m3clusterkvmem.NewStore()

		configureResults := ConfigureResults{
			NamespaceInitializer: nsInitStatic,
			TopologyInitializer:  topoInit,
			KVStore:              kv,
		}
		return configureResults, nil

	default:
		return emptyConfig, errMissingConfig
	}
}

func newStaticShardSet(numShards int, listenAddress string) (sharding.ShardSet, []topology.HostShardSet, error) {
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
	shardSet, err = sharding.NewShardSet(shards, sharding.DefaultHashFn(1))
	if err != nil {
		return nil, nil, err
	}

	host := topology.NewHost("localhost", listenAddress)
	hostShardSet := topology.NewHostShardSet(host, shardSet)
	hostShardSets = append(hostShardSets, hostShardSet)

	return shardSet, hostShardSets, nil
}

func newNamespaceMetadata(cfg StaticNamespaceConfiguration) (namespace.Metadata, error) {
	if cfg.Retention == nil {
		return nil, errNilRetention
	}
	if cfg.Options == nil {
		cfg.Options = &StaticNamespaceOptions{
			NeedsBootstrap:      true,
			NeedsFilesetCleanup: true,
			NeedsFlush:          true,
			NeedsRepair:         true,
			WritesToCommitLog:   true,
		}
	}
	md, err := namespace.NewMetadata(
		ts.StringID(cfg.Name),
		namespace.NewOptions().
			SetNeedsBootstrap(cfg.Options.NeedsBootstrap).
			SetNeedsFilesetCleanup(cfg.Options.NeedsFilesetCleanup).
			SetNeedsFlush(cfg.Options.NeedsFlush).
			SetNeedsRepair(cfg.Options.NeedsRepair).
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
