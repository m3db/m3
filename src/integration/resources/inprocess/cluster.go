// Copyright (c) 2021  Uber Technologies, Inc.
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

package inprocess

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	aggcfg "github.com/m3db/m3/src/cmd/services/m3aggregator/config"
	dbcfg "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	coordinatorcfg "github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/discovery"
	"github.com/m3db/m3/src/dbnode/environment"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/integration/resources"
	nettest "github.com/m3db/m3/src/integration/resources/net"
	"github.com/m3db/m3/src/query/storage/m3"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/config/hostid"
	xerrors "github.com/m3db/m3/src/x/errors"
)

// ClusterConfigs contain the input config to use for components within
// the cluster. There is one default configuration for each type of component.
// Given a set of ClusterConfigs, the function NewCluster can spin up an m3 cluster.
// Or one can use GenerateClusterSpecification to get the per-instance configuration
// and options based on the given ClusterConfigs.
type ClusterConfigs struct {
	// DBNode is the configuration for db nodes.
	DBNode dbcfg.Configuration
	// Coordinator is the configuration for the coordinator.
	Coordinator coordinatorcfg.Configuration
	// Aggregator is the configuration for aggregators.
	// If Aggregator is nil, the cluster contains only m3coordinator and dbnodes.
	Aggregator *aggcfg.Configuration
}

// ClusterSpecification contain the per-instance configuration and options to use
// for starting each components within the cluster.
// The function NewClusterFromSpecification will spin up an m3 cluster
// with the given ClusterSpecification.
type ClusterSpecification struct {
	// Configs contains the per-instance configuration for all components in the cluster.
	Configs PerInstanceConfigs
	// Options contains the per-insatance options for setting up the cluster.
	Options PerInstanceOptions
}

// PerInstanceConfigs contain the per-instance configuration for all components.
type PerInstanceConfigs struct {
	// DBNodes contains the per-instance configuration for db nodes.
	DBNodes []dbcfg.Configuration
	// Coordinator is the configuration for the coordinator.
	Coordinator coordinatorcfg.Configuration
	// Aggregators is the configuration for aggregators.
	// If Aggregators is nil, the cluster contains only m3coordinator and dbnodes.
	Aggregators []aggcfg.Configuration
}

// PerInstanceOptions contain the per-instance options for setting up the cluster.
type PerInstanceOptions struct {
	// DBNodes contains the per-instance options for db nodes in the cluster.
	DBNode []DBNodeOptions
}

// NewClusterConfigsFromConfigFile creates a new ClusterConfigs object from the
// provided filepaths for dbnode and coordinator configuration.
func NewClusterConfigsFromConfigFile(
	pathToDBNodeCfg string,
	pathToCoordCfg string,
	pathToAggCfg string,
) (ClusterConfigs, error) {
	var dCfg dbcfg.Configuration
	if err := xconfig.LoadFile(&dCfg, pathToDBNodeCfg, xconfig.Options{}); err != nil {
		return ClusterConfigs{}, err
	}

	var cCfg coordinatorcfg.Configuration
	if err := xconfig.LoadFile(&cCfg, pathToCoordCfg, xconfig.Options{}); err != nil {
		return ClusterConfigs{}, err
	}

	var aCfg aggcfg.Configuration
	if pathToAggCfg != "" {
		if err := xconfig.LoadFile(&aCfg, pathToAggCfg, xconfig.Options{}); err != nil {
			return ClusterConfigs{}, err
		}
	}

	return ClusterConfigs{
		DBNode:      dCfg,
		Coordinator: cCfg,
		Aggregator:  &aCfg,
	}, nil
}

// NewClusterConfigsFromYAML creates a new ClusterConfigs object from YAML strings
// representing component configs.
func NewClusterConfigsFromYAML(dbnodeYaml string, coordYaml string, aggYaml string) (ClusterConfigs, error) {
	var dbCfg dbcfg.Configuration
	if err := yaml.Unmarshal([]byte(dbnodeYaml), &dbCfg); err != nil {
		return ClusterConfigs{}, err
	}

	var coordCfg coordinatorcfg.Configuration
	if err := yaml.Unmarshal([]byte(coordYaml), &coordCfg); err != nil {
		return ClusterConfigs{}, err
	}

	var aggCfg aggcfg.Configuration
	if aggYaml != "" {
		if err := yaml.Unmarshal([]byte(aggYaml), &aggCfg); err != nil {
			return ClusterConfigs{}, err
		}
	}

	return ClusterConfigs{
		Coordinator: coordCfg,
		DBNode:      dbCfg,
		Aggregator:  &aggCfg,
	}, nil
}

// NewCluster creates a new M3 cluster based on the ClusterOptions provided.
// Expects at least a coordinator, a dbnode and an aggregator config.
func NewCluster(
	configs ClusterConfigs,
	opts resources.ClusterOptions,
) (resources.M3Resources, error) {
	fullConfigs, err := GenerateClusterSpecification(configs, opts)
	if err != nil {
		return nil, err
	}

	return NewClusterFromSpecification(fullConfigs, opts)
}

// NewClusterFromSpecification creates a new M3 cluster with the given ClusterSpecification.
func NewClusterFromSpecification(
	specs ClusterSpecification,
	opts resources.ClusterOptions,
) (resources.M3Resources, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	logger, err := resources.NewLogger()
	if err != nil {
		return nil, err
	}

	var (
		coord resources.Coordinator
		nodes = make(resources.Nodes, 0, len(specs.Configs.DBNodes))
		aggs  = make(resources.Aggregators, 0, len(specs.Configs.Aggregators))
	)

	fs.DisableIndexClaimsManagersCheckUnsafe()

	// Ensure that once we start creating resources, they all get cleaned up even if the function
	// fails half way.
	defer func() {
		if err != nil {
			cleanup(logger, nodes, coord, aggs)
		}
	}()

	for i := 0; i < len(specs.Configs.DBNodes); i++ {
		var node resources.Node
		node, err = NewDBNode(specs.Configs.DBNodes[i], specs.Options.DBNode[i])
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}

	coord, err = NewCoordinator(
		specs.Configs.Coordinator,
		CoordinatorOptions{GeneratePorts: opts.Coordinator.GeneratePorts},
	)
	if err != nil {
		return nil, err
	}

	for _, aggCfg := range specs.Configs.Aggregators {
		var agg resources.Aggregator
		agg, err = NewAggregator(aggCfg, AggregatorOptions{
			GeneratePorts:  true,
			GenerateHostID: false,
			Start:          false,
		})
		if err != nil {
			return nil, err
		}
		aggs = append(aggs, agg)
	}

	m3 := NewM3Resources(ResourceOptions{
		Coordinator: coord,
		DBNodes:     nodes,
		Aggregators: aggs,
	})

	if err = resources.SetupCluster(m3, opts); err != nil {
		return nil, err
	}

	return m3, nil
}

// GenerateClusterSpecification generates the per-instance configuration and options
// for the cluster set up based on the given input configuation and options.
func GenerateClusterSpecification(
	configs ClusterConfigs,
	opts resources.ClusterOptions,
) (ClusterSpecification, error) {
	if err := opts.Validate(); err != nil {
		return ClusterSpecification{}, err
	}

	nodeCfgs, nodeOpts, envConfig, err := GenerateDBNodeConfigsForCluster(configs, opts.DBNode)
	if err != nil {
		return ClusterSpecification{}, err
	}

	coordConfig := configs.Coordinator
	// TODO(nate): refactor to support having envconfig if no DB.
	if len(coordConfig.Clusters) > 0 {
		coordConfig.Clusters[0].Client.EnvironmentConfig = &envConfig
	} else {
		coordConfig.Clusters = m3.ClustersStaticConfiguration{
			{
				Client: client.Configuration{
					EnvironmentConfig: &envConfig,
				},
			},
		}
	}

	var aggCfgs []aggcfg.Configuration
	if opts.Aggregator != nil {
		aggCfgs, err = GenerateAggregatorConfigsForCluster(configs, opts.Aggregator)
		if err != nil {
			return ClusterSpecification{}, err
		}
	}

	return ClusterSpecification{
		Configs: PerInstanceConfigs{
			DBNodes:     nodeCfgs,
			Coordinator: coordConfig,
			Aggregators: aggCfgs,
		},
		Options: PerInstanceOptions{
			DBNode: nodeOpts,
		},
	}, nil
}

// GenerateDBNodeConfigsForCluster generates the unique configs and options
// for each DB node that will be instantiated. Additionally, provides
// default environment config that can be used to connect to embedded KV
// within the DB nodes.
func GenerateDBNodeConfigsForCluster(
	configs ClusterConfigs,
	opts *resources.DBNodeClusterOptions,
) ([]dbcfg.Configuration, []DBNodeOptions, environment.Configuration, error) {
	if opts == nil {
		return nil, nil, environment.Configuration{}, errors.New("dbnode cluster options is nil")
	}

	var (
		numNodes            = opts.RF * opts.NumInstances
		generatePortsAndIDs = numNodes > 1
	)

	// TODO(nate): eventually support clients specifying their own discovery stanza.
	// Practically, this should cover 99% of cases.
	//
	// Generate a discovery config with the dbnode using the generated hostID marked as
	// the etcd server (i.e. seed node).
	hostID := uuid.NewString()
	defaultDBNodesCfg := configs.DBNode
	discoveryCfg, envConfig, err := generateDefaultDiscoveryConfig(
		defaultDBNodesCfg,
		hostID,
		generatePortsAndIDs)
	if err != nil {
		return nil, nil, environment.Configuration{}, err
	}

	var (
		defaultDBNodeOpts = DBNodeOptions{
			GenerateHostID: generatePortsAndIDs,
			GeneratePorts:  generatePortsAndIDs,
		}
		cfgs     = make([]dbcfg.Configuration, 0, numNodes)
		nodeOpts = make([]DBNodeOptions, 0, numNodes)
	)
	for i := 0; i < int(numNodes); i++ {
		var cfg dbcfg.Configuration
		cfg, err = defaultDBNodesCfg.DeepCopy()
		if err != nil {
			return nil, nil, environment.Configuration{}, err
		}
		dbnodeOpts := defaultDBNodeOpts

		if i == 0 {
			// Mark the initial node as the etcd seed node.
			dbnodeOpts.GenerateHostID = false
			cfg.DB.HostID = &hostid.Configuration{
				Resolver: hostid.ConfigResolver,
				Value:    &hostID,
			}
		}
		cfg.DB.Discovery = &discoveryCfg

		cfgs = append(cfgs, cfg)
		nodeOpts = append(nodeOpts, dbnodeOpts)
	}

	return cfgs, nodeOpts, envConfig, nil
}

// generateDefaultDiscoveryConfig handles creating the correct config
// for having an embedded ETCD server with the correct server and
// client configuration.
func generateDefaultDiscoveryConfig(
	cfg dbcfg.Configuration,
	hostID string,
	generateETCDPorts bool,
) (discovery.Configuration, environment.Configuration, error) {
	discoveryConfig := cfg.DB.DiscoveryOrDefault()
	envConfig, err := discoveryConfig.EnvironmentConfig(hostID)
	if err != nil {
		return discovery.Configuration{}, environment.Configuration{}, err
	}

	var (
		etcdClientPort = dbcfg.DefaultEtcdClientPort
		etcdServerPort = dbcfg.DefaultEtcdServerPort
	)
	if generateETCDPorts {
		etcdClientPort, err = nettest.GetAvailablePort()
		if err != nil {
			return discovery.Configuration{}, environment.Configuration{}, err
		}

		etcdServerPort, err = nettest.GetAvailablePort()
		if err != nil {
			return discovery.Configuration{}, environment.Configuration{}, err
		}
	}

	etcdServerURL := fmt.Sprintf("http://0.0.0.0:%d", etcdServerPort)
	etcdClientAddr := net.JoinHostPort("0.0.0.0", strconv.Itoa(etcdClientPort))
	etcdClientURL := fmt.Sprintf("http://0.0.0.0:%d", etcdClientPort)

	envConfig.SeedNodes.InitialCluster[0].Endpoint = etcdServerURL
	envConfig.SeedNodes.InitialCluster[0].HostID = hostID
	envConfig.Services[0].Service.ETCDClusters[0].Endpoints = []string{etcdClientAddr}
	if generateETCDPorts {
		envConfig.SeedNodes.ListenPeerUrls = []string{etcdServerURL}
		envConfig.SeedNodes.ListenClientUrls = []string{etcdClientURL}
		envConfig.SeedNodes.InitialAdvertisePeerUrls = []string{etcdServerURL}
		envConfig.SeedNodes.AdvertiseClientUrls = []string{etcdClientURL}
	}

	configType := discovery.ConfigType
	return discovery.Configuration{
		Type:   &configType,
		Config: &envConfig,
	}, envConfig, nil
}

func cleanup(logger *zap.Logger, nodes resources.Nodes, coord resources.Coordinator, aggs resources.Aggregators) {
	var multiErr xerrors.MultiError
	for _, n := range nodes {
		multiErr = multiErr.Add(n.Close())
	}

	if coord != nil {
		multiErr = multiErr.Add(coord.Close())
	}

	for _, a := range aggs {
		multiErr = multiErr.Add(a.Close())
	}

	if !multiErr.Empty() {
		logger.Warn("failed closing resources", zap.Error(multiErr.FinalError()))
	}
}

// GenerateAggregatorConfigsForCluster generates the unique configs for each aggregator instance.
func GenerateAggregatorConfigsForCluster(
	configs ClusterConfigs,
	opts *resources.AggregatorClusterOptions,
) ([]aggcfg.Configuration, error) {
	if configs.Aggregator == nil {
		return nil, nil
	}

	cfgs := make([]aggcfg.Configuration, 0, int(opts.NumInstances))
	for i := 0; i < int(opts.NumInstances); i++ {
		cfg, err := configs.Aggregator.DeepCopy()
		if err != nil {
			return nil, err
		}

		hostID := fmt.Sprintf("m3aggregator%02d", i)
		aggCfg := cfg.AggregatorOrDefault()
		aggCfg.HostID = &hostid.Configuration{
			Resolver: hostid.ConfigResolver,
			Value:    &hostID,
		}
		cfg.Aggregator = &aggCfg

		cfgs = append(cfgs, cfg)
	}

	return cfgs, nil
}
