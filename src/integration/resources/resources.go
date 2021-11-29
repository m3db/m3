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
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	protobuftypes "github.com/gogo/protobuf/types"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/msg/generated/proto/topicpb"
	"github.com/m3db/m3/src/query/generated/proto/admin"
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
	// AggregatorInputTopic is the m3msg topic name for coordinator->aggregator traffic.
	AggregatorInputTopic = "aggregator_ingest"
	// AggregatorOutputTopic is the m3msg topic name for aggregator->coordinator traffic.
	AggregatorOutputTopic = "aggregated_metrics"
)

// SetupCluster setups m3 cluster on provided docker containers.
func SetupCluster(
	cluster M3Resources,
	opts ClusterOptions,
) error { // nolint: gocyclo
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
		if opts.DBNode != nil && opts.DBNode.NumIsolationGroups > 0 {
			h.IsolationGroup = fmt.Sprintf("isogroup-%d", int32(i)%opts.DBNode.NumIsolationGroups)
		}

		hosts = append(hosts, h)
		ids = append(ids, h.GetId())
	}

	replicationFactor := int32(1)
	numShards := int32(4)
	if opts.DBNode != nil {
		if opts.DBNode.RF > 0 {
			replicationFactor = opts.DBNode.RF
		}
		if opts.DBNode.NumShards > 0 {
			numShards = opts.DBNode.NumShards
		}
	}

	var (
		unaggDatabase = admin.DatabaseCreateRequest{
			Type:              "cluster",
			NamespaceName:     UnaggName,
			RetentionTime:     retention,
			NumShards:         numShards,
			ReplicationFactor: replicationFactor,
			Hosts:             hosts,
		}

		aggNamespace = admin.NamespaceAddRequest{
			Name: AggName,
			Options: &namespace.NamespaceOptions{
				BootstrapEnabled:  true,
				FlushEnabled:      true,
				WritesToCommitLog: true,
				CleanupEnabled:    true,
				SnapshotEnabled:   true,
				IndexOptions: &namespace.IndexOptions{
					Enabled:        true,
					BlockSizeNanos: int64(30 * time.Minute),
				},
				RetentionOptions: &namespace.RetentionOptions{
					RetentionPeriodNanos:                     int64(6 * time.Hour),
					BlockSizeNanos:                           int64(30 * time.Minute),
					BufferFutureNanos:                        int64(2 * time.Minute),
					BufferPastNanos:                          int64(10 * time.Minute),
					BlockDataExpiry:                          true,
					BlockDataExpiryAfterNotAccessPeriodNanos: int64(time.Minute * 5),
				},
				AggregationOptions: &namespace.AggregationOptions{
					Aggregations: []*namespace.Aggregation{
						{
							Aggregated: true,
							Attributes: &namespace.AggregatedAttributes{
								ResolutionNanos: int64(5 * time.Second),
							},
						},
					},
				},
			},
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

	logger.Info("creating database", zap.Any("request", unaggDatabase))
	if _, err := coordinator.CreateDatabase(unaggDatabase); err != nil {
		return err
	}

	logger.Info("waiting for placements", zap.Strings("placement ids", ids))
	if err := coordinator.WaitForInstances(ids); err != nil {
		return err
	}

	logger.Info("waiting for namespace", zap.String("name", UnaggName))
	if err := coordinator.WaitForNamespace(UnaggName); err != nil {
		return err
	}

	logger.Info("creating namespace", zap.Any("request", aggNamespace))
	if _, err := coordinator.AddNamespace(aggNamespace); err != nil {
		return err
	}

	logger.Info("waiting for namespace", zap.String("name", AggName))
	if err := coordinator.WaitForNamespace(AggName); err != nil {
		return err
	}

	logger.Info("creating namespace", zap.Any("request", coldWriteNamespace))
	if _, err := coordinator.AddNamespace(coldWriteNamespace); err != nil {
		return err
	}

	logger.Info("waiting for namespace", zap.String("name", ColdWriteNsName))
	if err := coordinator.WaitForNamespace(ColdWriteNsName); err != nil {
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

	if opts.Aggregator != nil {
		aggregators := cluster.Aggregators()
		if len(aggregators) == 0 {
			return errors.New("no aggregators have been initiazted")
		}

		if err := setupPlacement(coordinator, aggregators, *opts.Aggregator); err != nil {
			return err
		}

		aggInstanceInfo, err := aggregators[0].HostDetails()
		if err != nil {
			return err
		}

		if err := setupM3msgTopics(coordinator, *aggInstanceInfo, opts); err != nil {
			return err
		}

		for _, agg := range aggregators {
			agg.Start()
		}

		if err := aggregators.WaitForHealthy(); err != nil {
			return err
		}
	}

	logger.Info("all healthy")

	return nil
}

func setupPlacement(
	coordinator Coordinator,
	aggs Aggregators,
	opts AggregatorClusterOptions,
) error {
	// Setup aggregator placement.
	var env, zone string
	instances := make([]*placementpb.Instance, 0, len(aggs))
	for i, agg := range aggs {
		info, err := agg.HostDetails()
		if err != nil {
			return err
		}

		if i == 0 {
			env = info.Env
			zone = info.Zone
		}

		instance := &placementpb.Instance{
			Id:             info.ID,
			IsolationGroup: fmt.Sprintf("isogroup-%02d", i%int(opts.NumIsolationGroups)),
			Zone:           info.Zone,
			Weight:         1,
			Endpoint:       net.JoinHostPort(info.M3msgAddress, strconv.Itoa(int(info.M3msgPort))),
			Hostname:       info.ID,
			Port:           info.M3msgPort,
		}

		instances = append(instances, instance)
	}

	aggPlacementRequestOptions := PlacementRequestOptions{
		Service: ServiceTypeM3Aggregator,
		Zone:    zone,
		Env:     env,
	}

	_, err := coordinator.InitPlacement(
		aggPlacementRequestOptions,
		admin.PlacementInitRequest{
			NumShards:         opts.NumShards,
			ReplicationFactor: opts.RF,
			Instances:         instances,
			OptionOverride: &placementpb.Options{
				SkipPortMirroring: &protobuftypes.BoolValue{Value: true},
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to init aggregator placement: %w", err)
	}

	// Setup coordinator placement.
	coordHost, err := coordinator.HostDetails()
	if err != nil {
		return fmt.Errorf("failed to get coordinator host details: %w", err)
	}

	coordPlacementRequestOptions := PlacementRequestOptions{
		Service: ServiceTypeM3Coordinator,
		Zone:    coordHost.Zone,
		Env:     coordHost.Env,
	}
	_, err = coordinator.InitPlacement(
		coordPlacementRequestOptions,
		admin.PlacementInitRequest{
			Instances: []*placementpb.Instance{
				{
					Id:       coordHost.ID,
					Zone:     coordHost.Zone,
					Endpoint: net.JoinHostPort(coordHost.M3msgAddress, strconv.Itoa(int(coordHost.M3msgPort))),
					Hostname: coordHost.ID,
					Port:     coordHost.M3msgPort,
				},
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to init coordinator placement: %w", err)
	}

	return nil
}

func setupM3msgTopics(
	coord Coordinator,
	aggInstanceInfo InstanceInfo,
	opts ClusterOptions,
) error {
	aggInputTopicOpts := M3msgTopicOptions{
		Zone:      aggInstanceInfo.Zone,
		Env:       aggInstanceInfo.Env,
		TopicName: AggregatorInputTopic,
	}

	aggOutputTopicOpts := M3msgTopicOptions{
		Zone:      aggInstanceInfo.Zone,
		Env:       aggInstanceInfo.Env,
		TopicName: AggregatorOutputTopic,
	}

	_, err := coord.InitM3msgTopic(
		aggInputTopicOpts,
		admin.TopicInitRequest{NumberOfShards: uint32(opts.Aggregator.NumShards)},
	)
	if err != nil {
		return fmt.Errorf("failed to init aggregator input m3msg topic: %w", err)
	}

	_, err = coord.AddM3msgTopicConsumer(aggInputTopicOpts, admin.TopicAddRequest{
		ConsumerService: &topicpb.ConsumerService{
			ServiceId: &topicpb.ServiceID{
				Name:        handleroptions.M3AggregatorServiceName,
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

	_, err = coord.InitM3msgTopic(
		aggOutputTopicOpts,
		admin.TopicInitRequest{NumberOfShards: uint32(opts.DBNode.NumShards)},
	)
	if err != nil {
		return fmt.Errorf("failed to init aggregator output m3msg topic: %w", err)
	}

	_, err = coord.AddM3msgTopicConsumer(aggOutputTopicOpts, admin.TopicAddRequest{
		ConsumerService: &topicpb.ConsumerService{
			ServiceId: &topicpb.ServiceID{
				Name:        handleroptions.M3CoordinatorServiceName,
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
