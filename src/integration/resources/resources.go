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

package resources

import (
	"fmt"
	"net"
	"strconv"
	"time"

	protobuftypes "github.com/gogo/protobuf/types"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	aggcfg "github.com/m3db/m3/src/cmd/services/m3aggregator/config"
	"github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/msg/generated/proto/topicpb"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/x/headers"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	retention = "6h"

	// AggName is the name of the aggregated namespace.
	AggName = "aggregated"
	// UnaggName is the name of the unaggregated namespace.
	UnaggName = "default"
	// ColdWriteNsName is the name for cold write namespace.
	ColdWriteNsName = "coldWritesRepairAndNoIndex"
)

// SetupCluster setups m3 cluster on provided docker containers.
func SetupCluster(cluster M3Resources, opts *ClusterOptions) error { // nolint: gocyclo
	coordinator := cluster.Coordinator()
	iOpts := instrument.NewOptions()
	logger := iOpts.Logger().With(zap.String("source", "harness"))
	hosts := make([]*admin.Host, 0, len(cluster.Nodes()))
	ids := make([]string, 0, len(cluster.Nodes()))
	for i, n := range cluster.Nodes() {
		h, err := n.HostDetails(9000)
		if err != nil {
			logger.Error("could not get host details", zap.Error(err))
			return err
		}
		if opts != nil && opts.NumIsolationGroups > 0 {
			h.IsolationGroup = fmt.Sprintf("isogroup-%d", int32(i)%opts.NumIsolationGroups)
		}

		hosts = append(hosts, h)
		ids = append(ids, h.GetId())
	}

	replicationFactor := int32(1)
	numShards := int32(4)
	if opts != nil {
		if opts.ReplicationFactor > 0 {
			replicationFactor = opts.ReplicationFactor
		}
		if opts.NumShards > 0 {
			numShards = opts.NumShards
		}
	}

	var (
		aggDatabase = admin.DatabaseCreateRequest{
			Type:              "cluster",
			NamespaceName:     AggName,
			RetentionTime:     retention,
			NumShards:         numShards,
			ReplicationFactor: replicationFactor,
			Hosts:             hosts,
		}

		unaggDatabase = admin.DatabaseCreateRequest{
			NamespaceName: UnaggName,
			RetentionTime: retention,
		}

		coldWriteNamespace = admin.NamespaceAddRequest{
			Name: ColdWriteNsName,
			Options: &namespace.NamespaceOptions{
				BootstrapEnabled:      true,
				FlushEnabled:          true,
				WritesToCommitLog:     true,
				CleanupEnabled:        true,
				SnapshotEnabled:       true,
				RepairEnabled:         true,
				ColdWritesEnabled:     true,
				CacheBlocksOnRetrieve: &protobuftypes.BoolValue{Value: true},
				RetentionOptions: &namespace.RetentionOptions{
					RetentionPeriodNanos:                     int64(4 * time.Hour),
					BlockSizeNanos:                           int64(time.Hour),
					BufferFutureNanos:                        int64(time.Minute * 10),
					BufferPastNanos:                          int64(time.Minute * 10),
					BlockDataExpiry:                          true,
					BlockDataExpiryAfterNotAccessPeriodNanos: int64(time.Minute * 5),
				},
			},
		}
	)

	logger.Info("waiting for coordinator")
	if err := coordinator.WaitForNamespace(""); err != nil {
		return err
	}

	logger.Info("creating database", zap.Any("request", aggDatabase))
	if _, err := coordinator.CreateDatabase(aggDatabase); err != nil {
		return err
	}

	logger.Info("waiting for placements", zap.Strings("placement ids", ids))
	if err := coordinator.WaitForInstances(ids); err != nil {
		return err
	}

	logger.Info("waiting for namespace", zap.String("name", AggName))
	if err := coordinator.WaitForNamespace(AggName); err != nil {
		return err
	}

	logger.Info("creating namespace", zap.Any("request", unaggDatabase))
	if _, err := coordinator.CreateDatabase(unaggDatabase); err != nil {
		return err
	}

	logger.Info("waiting for namespace", zap.String("name", UnaggName))
	if err := coordinator.WaitForNamespace(UnaggName); err != nil {
		return err
	}

	logger.Info("creating namespace", zap.Any("request", coldWriteNamespace))
	if _, err := coordinator.AddNamespace(coldWriteNamespace); err != nil {
		return err
	}

	logger.Info("waiting for namespace", zap.String("name", ColdWriteNsName))
	if err := coordinator.WaitForNamespace(UnaggName); err != nil {
		return err
	}

	logger.Info("waiting for healthy")
	if err := cluster.Nodes().WaitForHealthy(); err != nil {
		return err
	}

	logger.Info("waiting for shards ready")
	if err := coordinator.WaitForShardsReady(); err != nil {
		return err
	}

	logger.Info("waiting for cluster to be ready")
	if err := coordinator.WaitForClusterReady(); err != nil {
		return err
	}

	logger.Info("all healthy")
	return nil
}

// SetupPlacement setups placements for m3 cluster.
func SetupPlacement(
	coordinator Coordinator,
	opts AggregatorClusterOptions,
	aggCfgs []aggcfg.Configuration,
) error {
	// Setup aggregator placement.
	aggPlacementRequestOptions := PlacementRequestOptions{
		Service: ServiceTypeM3Aggregator,
		// TODO: get zone and env from aggCfgs
		Zone: headers.DefaultServiceZone,
		Env:  headers.DefaultServiceEnvironment,
	}
	instances := make([]*placementpb.Instance, 0, len(aggCfgs))
	for i, cfg := range aggCfgs {
		hostID, err := cfg.AggregatorOrDefault().HostID.Resolve()
		if err != nil {
			return err
		}

		_, p, err := net.SplitHostPort(cfg.M3MsgOrDefault().Server.ListenAddress)
		if err != nil {
			return fmt.Errorf("failed to handle m3msg server address: %w", err)
		}
		port, err := strconv.Atoi(p)
		if err != nil {
			return fmt.Errorf("faild to convert m3msg server port: %w", err)
		}

		instance := &placementpb.Instance{
			Id:             hostID,
			IsolationGroup: fmt.Sprintf("isogroup-%02d", i%int(opts.NumIsolationGroups)),
			// TODO: get zone from cfg
			Zone:     headers.DefaultServiceZone,
			Weight:   1,
			Endpoint: cfg.M3MsgOrDefault().Server.ListenAddress,
			Hostname: hostID,
			Port:     uint32(port),
		}
		instances = append(instances, instance)
	}

	_, err := coordinator.InitPlacement(
		aggPlacementRequestOptions,
		admin.PlacementInitRequest{
			NumShards:         opts.NumShards,
			ReplicationFactor: opts.ReplicationFactor,
			Instances:         instances,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to init aggregator placement: %w", err)
	}

	// Setup coordinator placement.
	coordPlacementRequestOptions := PlacementRequestOptions{
		Service: ServiceTypeM3Coordinator,
		// TODO: get zone and env from aggCfgs
		Zone: headers.DefaultServiceZone,
		Env:  headers.DefaultServiceEnvironment,
	}
	coordHost, err := coordinator.HostDetails()
	if err != nil {
		return fmt.Errorf("failed to get coordinator host details: %w", err)
	}
	_, err = coordinator.InitPlacement(
		coordPlacementRequestOptions,
		admin.PlacementInitRequest{
			Instances: []*placementpb.Instance{
				{
					Id:       coordHost.Id,
					Zone:     coordHost.Zone,
					Endpoint: fmt.Sprintf("%s:%d", coordHost.Address, coordHost.Port),
					Hostname: coordHost.Id,
					Port:     coordHost.Port,
				},
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to init coordinator placement: %w", err)
	}

	return nil
}

// SetupM3msgTopics setups m3msg topics for m3 cluster.
func SetupM3msgTopics(coord Coordinator, aggCfgs []aggcfg.Configuration) error {
	aggInputTopicOpts := M3msgTopicOptions{
		Zone:      headers.DefaultServiceZone,
		Env:       headers.DefaultServiceEnvironment,
		TopicName: "aggregator_ingest",
	}

	aggOutputTopicOpts := M3msgTopicOptions{
		Zone:      headers.DefaultServiceZone,
		Env:       headers.DefaultServiceEnvironment,
		TopicName: "aggregated_metrics",
	}

	if len(aggCfgs) > 0 {
		// TODO: improve this!
		aggInputTopicOpts.TopicName = aggCfgs[0].Aggregator.Client.M3Msg.Producer.Writer.TopicName
		aggOutputTopicOpts.TopicName = aggCfgs[0].Aggregator.Flush.Handlers[0].DynamicBackend.Producer.Writer.TopicName
	}

	_, err := coord.InitM3msgTopic(aggInputTopicOpts, admin.TopicInitRequest{NumberOfShards: 4})
	if err != nil {
		return fmt.Errorf("failed to init aggregator input m3msg topic: %w", err)
	}

	_, err = coord.AddM3msgTopicConsumer(aggInputTopicOpts, admin.TopicAddRequest{
		ConsumerService: &topicpb.ConsumerService{
			ServiceId: &topicpb.ServiceID{
				Name:        "m3aggregator",
				Environment: aggInputTopicOpts.Env,
				Zone:        aggInputTopicOpts.Zone,
			},
			ConsumptionType: topicpb.ConsumptionType_REPLICATED,
			MessageTtlNanos: 600000000000, // 10 mins
		},
	})
	if err != nil {
		return fmt.Errorf("failed to add aggregator input m3msg topic consumer: %w", err)
	}

	_, err = coord.InitM3msgTopic(aggOutputTopicOpts, admin.TopicInitRequest{NumberOfShards: 4})
	if err != nil {
		return fmt.Errorf("failed to init aggregator output m3msg topic: %w", err)
	}

	_, err = coord.AddM3msgTopicConsumer(aggOutputTopicOpts, admin.TopicAddRequest{
		ConsumerService: &topicpb.ConsumerService{
			ServiceId: &topicpb.ServiceID{
				Name:        "m3coordinator",
				Environment: aggInputTopicOpts.Env,
				Zone:        aggInputTopicOpts.Zone,
			},
			ConsumptionType: topicpb.ConsumptionType_SHARED,
			MessageTtlNanos: 600000000000, // 10 mins
		},
	})
	if err != nil {
		return fmt.Errorf("failed to add aggregator output m3msg topic consumer: %w", err)
	}

	return nil
}
