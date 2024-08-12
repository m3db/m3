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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	aggcfg "github.com/m3db/m3/src/cmd/services/m3aggregator/config"
	dbcfg "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	coordinatorcfg "github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/discovery"
	"github.com/m3db/m3/src/dbnode/environment"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/integration/resources/docker/dockerexternal"
	"github.com/m3db/m3/src/query/storage/m3"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/config/hostid"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
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
	// "db":
	//  discovery:
	//    "config":
	//      "service":
	//        "etcdClusters":
	//          - "endpoints": ["http://127.0.0.1:2379"]
	//            "zone": "embedded"
	//        "service": "m3db"
	//        "zone": "embedded"
	//        "env": "default_env"
	etcdClientCfg := &etcdclient.Configuration{
		Zone:    "embedded",
		Env:     "default_env",
		Service: "m3db",
		ETCDClusters: []etcdclient.ClusterConfig{{
			Zone:      "embedded",
			Endpoints: []string{"http://127.0.0.1:2379"},
		}},
	}
	var dbCfg = dbcfg.Configuration{
		DB: &dbcfg.DBConfiguration{
			Discovery: &discovery.Configuration{
				Config: &environment.Configuration{
					Services: environment.DynamicConfiguration{{
						Service: etcdClientCfg,
					}},
				},
			},
		},
	}
	if err := yaml.Unmarshal([]byte(dbnodeYaml), &dbCfg); err != nil {
		return ClusterConfigs{}, err
	}

	var coordCfg = coordinatorcfg.Configuration{
		ClusterManagement: coordinatorcfg.ClusterManagementConfiguration{
			Etcd: etcdClientCfg,
		},
	}
	if err := yaml.Unmarshal([]byte(coordYaml), &coordCfg); err != nil {
		return ClusterConfigs{}, err
	}

	var aggCfg = aggcfg.Configuration{}

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
) (_ resources.M3Resources, finalErr error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	logger, err := resources.NewLogger()
	if err != nil {
		return nil, err
	}

	var (
		etcd  *dockerexternal.EtcdNode
		coord resources.Coordinator
		nodes = make(resources.Nodes, 0, len(specs.Configs.DBNodes))
		aggs  = make(resources.Aggregators, 0, len(specs.Configs.Aggregators))
	)

	fs.DisableIndexClaimsManagersCheckUnsafe()

	// Ensure that once we start creating resources, they all get cleaned up even if the function
	// fails half way.
	defer func() {
		if finalErr != nil {
			cleanup(logger, etcd, nodes, coord, aggs)
		}
	}()

	etcdEndpoints := opts.EtcdEndpoints
	if len(opts.EtcdEndpoints) == 0 {
		// TODO: amainsd: maybe not the cleanest place to do this.
		pool, err := dockertest.NewPool("")
		if err != nil {
			return nil, err
		}
		etcd, err = dockerexternal.NewEtcd(pool, instrument.NewOptions())
		if err != nil {
			return nil, err
		}

		// TODO(amains): etcd *needs* to be setup before the coordinator, because ConfigurePlacementsForAggregation spins
		// up a dedicated coordinator for some reason. Either clean this up or just accept it.
		if err := etcd.Setup(context.TODO()); err != nil {
			return nil, err
		}
		etcdEndpoints = []string{fmt.Sprint(etcd.Address())}
	}

	updateEtcdEndpoints := func(etcdCfg *etcdclient.Configuration) {
		etcdCfg.ETCDClusters[0].Endpoints = etcdEndpoints
		etcdCfg.ETCDClusters[0].AutoSyncInterval = -1
	}
	for i := 0; i < len(specs.Configs.DBNodes); i++ {
		var node resources.Node
		updateEtcdEndpoints(specs.Configs.DBNodes[i].DB.Discovery.Config.Services[0].Service)
		node, err = NewDBNode(specs.Configs.DBNodes[i], specs.Options.DBNode[i])
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}

	for _, aggCfg := range specs.Configs.Aggregators {
		var agg resources.Aggregator
		agg, err = NewAggregator(aggCfg, AggregatorOptions{
			GeneratePorts:  true,
			GenerateHostID: false,
			EtcdEndpoints:  etcdEndpoints,
		})
		if err != nil {
			return nil, err
		}
		aggs = append(aggs, agg)
	}

	updateEtcdEndpoints(specs.Configs.Coordinator.ClusterManagement.Etcd)
	coord, err = NewCoordinator(
		specs.Configs.Coordinator,
		CoordinatorOptions{GeneratePorts: opts.Coordinator.GeneratePorts},
	)
	if err != nil {
		return nil, err
	}

	if err = ConfigurePlacementsForAggregation(nodes, coord, aggs, specs, opts); err != nil {
		return nil, fmt.Errorf("failed to setup placements for aggregation: %w", err)
	}

	// Start all the configured resources.
	m3 := NewM3Resources(ResourceOptions{
		Coordinator: coord,
		DBNodes:     nodes,
		Aggregators: aggs,
		Etcd:        etcd,
	})
	m3.Start()

	if err = resources.SetupCluster(m3, opts); err != nil {
		return nil, err
	}

	return m3, nil
}

// ConfigurePlacementsForAggregation sets up the correct placement information for
// coordinators and aggregators when aggregation is enabled.
func ConfigurePlacementsForAggregation(
	nodes resources.Nodes,
	coord resources.Coordinator,
	aggs resources.Aggregators,
	specs ClusterSpecification,
	opts resources.ClusterOptions,
) error {
	if len(aggs) == 0 {
		return nil
	}

	coordAPI := coord
	hostDetails, err := coord.HostDetails()
	if err != nil {
		return err
	}

	// With remote aggregation enabled, aggregation is not handled within the coordinator.
	// When this is true, the coordinator will fail to start until placement is updated with
	// aggregation related information. As such, use the coordinator embedded within the dbnode
	// to configure the placement and topics.
	if specs.Configs.Coordinator.Downsample.RemoteAggregator != nil {
		if len(specs.Configs.DBNodes) == 0 ||
			specs.Configs.DBNodes[0].Coordinator == nil {
			return errors.New("remote aggregation requires at least one DB node" +
				" running an embedded coordinator for placement and topic configuration")
		}

		embedded, err := NewEmbeddedCoordinator(nodes[0].(*DBNode))
		if err != nil {
			return nil
		}

		coordAPI = embedded
	} else {
		// TODO(nate): Remove this in a follow up. If we're not doing remote aggregation
		// we should not be starting aggs which is what requires the coordinator to get started.
		// Once we've refactored existing tests that have aggs w/o remote aggregation enabled,
		// this should be killable.
		coord.Start()
	}

	if err = coordAPI.WaitForNamespace(""); err != nil {
		return err
	}

	if err = resources.SetupPlacement(coordAPI, *hostDetails, aggs, *opts.Aggregator); err != nil {
		return err
	}

	aggInstanceInfo, err := aggs[0].HostDetails()
	if err != nil {
		return err
	}

	return resources.SetupM3MsgTopics(coordAPI, *aggInstanceInfo, opts)
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

	if configs.DBNode.DB.Discovery == nil {
		return nil, nil, environment.Configuration{}, errors.New(
			"configuration must specify at least `discovery`" +
				" in order to construct an etcd client")
	}
	discoveryCfg, envConfig := configs.DBNode.DB.Discovery, configs.DBNode.DB.Discovery.Config

	var (
		defaultDBNodeOpts = DBNodeOptions{
			GenerateHostID: generatePortsAndIDs,
			GeneratePorts:  generatePortsAndIDs,
			Start:          true,
		}
		cfgs     = make([]dbcfg.Configuration, 0, numNodes)
		nodeOpts = make([]DBNodeOptions, 0, numNodes)
	)
	for i := 0; i < int(numNodes); i++ {
		cfg, err := defaultDBNodesCfg.DeepCopy()
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
		cfg.DB.Discovery = discoveryCfg

		cfgs = append(cfgs, cfg)
		nodeOpts = append(nodeOpts, dbnodeOpts)
	}

	return cfgs, nodeOpts, *envConfig, nil
}

func cleanup(
	logger *zap.Logger,
	etcd *dockerexternal.EtcdNode,
	nodes resources.Nodes,
	coord resources.Coordinator,
	aggs resources.Aggregators,
) {
	var multiErr xerrors.MultiError

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if etcd != nil {
		multiErr = multiErr.Add(etcd.Close(ctx))
	}

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
