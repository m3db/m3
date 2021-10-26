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

	dbcfg "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	coordinatorcfg "github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/discovery"
	"github.com/m3db/m3/src/dbnode/environment"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/integration/resources"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/config/hostid"
	xerrors "github.com/m3db/m3/src/x/errors"
)

// ClusterOptions contains options for spinning up a new M3 cluster
// composed of in-process components.
type ClusterOptions struct {
	// DBNode contains cluster options for spinning up dbnodes.
	DBNode DBNodeClusterOptions

	// TODO: support for Aggregator cluster options
}

// ClusterConfigs contain the config to use for components within
// the cluster.
type ClusterConfigs struct {
	// DBNode is the configuration for db nodes.
	DBNode dbcfg.Configuration
	// Coordinator is the configuration for the coordinator.
	Coordinator coordinatorcfg.Configuration
}

// NewClusterConfigsFromConfigFile creates a new ClusterConfigs object from the
// provided filepaths for dbnode and coordinator configuration.
func NewClusterConfigsFromConfigFile(
	pathToDBNodeCfg string,
	pathToCoordCfg string,
) (ClusterConfigs, error) {
	var dCfg dbcfg.Configuration
	if err := xconfig.LoadFile(&dCfg, pathToDBNodeCfg, xconfig.Options{}); err != nil {
		return ClusterConfigs{}, err
	}

	var cCfg coordinatorcfg.Configuration
	if err := xconfig.LoadFile(&cCfg, pathToCoordCfg, xconfig.Options{}); err != nil {
		return ClusterConfigs{}, err
	}

	return ClusterConfigs{
		DBNode:      dCfg,
		Coordinator: cCfg,
	}, nil
}

// NewClusterConfigsFromYAML creates a new ClusterConfigs object from YAML strings
// representing component configs.
func NewClusterConfigsFromYAML(dbnodeYaml string, coordYaml string) (ClusterConfigs, error) {
	var dbCfg dbcfg.Configuration
	if err := yaml.Unmarshal([]byte(dbnodeYaml), &dbCfg); err != nil {
		return ClusterConfigs{}, err
	}

	var coordCfg coordinatorcfg.Configuration
	if err := yaml.Unmarshal([]byte(coordYaml), &coordCfg); err != nil {
		return ClusterConfigs{}, err
	}

	return ClusterConfigs{
		Coordinator: coordCfg,
		DBNode:      dbCfg,
	}, nil
}

// DBNodeClusterOptions contains the cluster options for spinning up
// dbnodes.
type DBNodeClusterOptions struct {
	// RF is the replication factor to use for the cluster.
	RF int32
	// NumShards is the number of shards to use for each RF.
	NumShards int32
	// NumInstances is the number of dbnode instances per RF.
	NumInstances int32
	// NumIsolationGroups is the number of isolation groups to split
	// nodes into.
	NumIsolationGroups int32
}

// NewDBNodeClusterOptions creates DBNodeClusteOptions with sane defaults.
// DBNode config must still be provided.
func NewDBNodeClusterOptions() DBNodeClusterOptions {
	return DBNodeClusterOptions{
		RF:                 1,
		NumShards:          4,
		NumInstances:       1,
		NumIsolationGroups: 1,
	}
}

// Validate validates the DBNodeClusterOptions.
func (d *DBNodeClusterOptions) Validate() error {
	if d.RF < 1 {
		return errors.New("rf must be at least 1")
	}

	if d.NumShards < 1 {
		return errors.New("numShards must be at least 1")
	}

	if d.NumInstances < 1 {
		return errors.New("numInstances must be at least 1")
	}

	if d.NumIsolationGroups < 1 {
		return errors.New("numIsolationGroups must be at least 1")
	}

	if d.RF > d.NumIsolationGroups {
		return errors.New("rf must be less than or equal to numIsolationGroups")
	}

	return nil
}

// NewCluster creates a new M3 cluster based on the ClusterOptions provided.
// Expects at least a coordinator and a dbnode config.
func NewCluster(configs ClusterConfigs, opts ClusterOptions) (resources.M3Resources, error) {
	if err := opts.DBNode.Validate(); err != nil {
		return nil, err
	}

	logger, err := resources.NewLogger()
	if err != nil {
		return nil, err
	}

	nodeCfgs, nodeOpts, envConfig, err := GenerateDBNodeConfigsForCluster(configs, opts.DBNode)
	if err != nil {
		return nil, err
	}

	var (
		coord resources.Coordinator
		nodes = make(resources.Nodes, 0, len(nodeCfgs))
	)

	fs.DisableIndexClaimsManagersCheckUnsafe()

	// Ensure that once we start creating resources, they all get cleaned up even if the function
	// fails half way.
	defer func() {
		if err != nil {
			cleanup(logger, nodes, coord)
		}
	}()

	for i := 0; i < len(nodeCfgs); i++ {
		var node resources.Node
		node, err = NewDBNode(nodeCfgs[i], nodeOpts[i])
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}

	coordConfig := configs.Coordinator
	// TODO(nate): refactor to support having envconfig if no DB.
	coordConfig.Clusters[0].Client.EnvironmentConfig = &envConfig
	coord, err = NewCoordinator(coordConfig, CoordinatorOptions{})
	if err != nil {
		return nil, err
	}

	m3 := NewM3Resources(ResourceOptions{
		Coordinator: coord,
		DBNodes:     nodes,
	})
	if err = resources.SetupCluster(m3, &resources.ClusterOptions{
		ReplicationFactor:  opts.DBNode.RF,
		NumShards:          opts.DBNode.NumShards,
		NumIsolationGroups: opts.DBNode.NumIsolationGroups,
	}); err != nil {
		return nil, err
	}

	return m3, nil
}

// GenerateDBNodeConfigsForCluster generates the unique configs and options
// for each DB node that will be instantiated. Additionally, provides
// default environment config that can be used to connect to embedded KV
// within the DB nodes.
func GenerateDBNodeConfigsForCluster(
	configs ClusterConfigs,
	opts DBNodeClusterOptions,
) ([]dbcfg.Configuration, []DBNodeOptions, environment.Configuration, error) {
	// TODO(nate): eventually support clients specifying their own discovery stanza.
	// Practically, this should cover 99% of cases.
	//
	// Generate a discovery config with the dbnode using the generated hostID marked as
	// the etcd server (i.e. seed node).
	hostID := uuid.NewString()
	defaultDBNodesCfg := configs.DBNode
	discoveryCfg, envConfig, err := generateDefaultDiscoveryConfig(defaultDBNodesCfg, hostID)
	if err != nil {
		return nil, nil, environment.Configuration{}, err
	}

	var (
		numNodes            = opts.RF * opts.NumInstances
		generatePortsAndIDs = numNodes > 1
		defaultDBNodeOpts   = DBNodeOptions{
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
) (discovery.Configuration, environment.Configuration, error) {
	discoveryConfig := cfg.DB.DiscoveryOrDefault()
	envConfig, err := discoveryConfig.EnvironmentConfig(hostID)
	if err != nil {
		return discovery.Configuration{}, environment.Configuration{}, err
	}

	// TODO(nate): Fix expectations in envconfig for:
	//   - InitialAdvertisePeerUrls
	//	 - AdvertiseClientUrls
	//	 - ListenPeerUrls
	//	 - ListenClientUrls
	// when not using the default ports of 2379 and 2380
	envConfig.SeedNodes.InitialCluster[0].Endpoint =
		fmt.Sprintf("http://0.0.0.0:%d", 2380)
	envConfig.SeedNodes.InitialCluster[0].HostID = hostID
	envConfig.Services[0].Service.ETCDClusters[0].Endpoints = []string{
		net.JoinHostPort("0.0.0.0", strconv.Itoa(2379)),
	}
	configType := discovery.ConfigType
	return discovery.Configuration{
		Type:   &configType,
		Config: &envConfig,
	}, envConfig, nil
}

func cleanup(logger *zap.Logger, nodes resources.Nodes, coord resources.Coordinator) {
	var multiErr xerrors.MultiError
	for _, n := range nodes {
		multiErr = multiErr.Add(n.Close())
	}

	if coord != nil {
		multiErr = multiErr.Add(coord.Close())
	}

	if !multiErr.Empty() {
		logger.Warn("failed closing resources", zap.Error(multiErr.FinalError()))
	}
}
