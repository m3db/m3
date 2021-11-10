package resources

import (
	"errors"
	"fmt"
)

// ClusterOptions contains options for spinning up a new M3 cluster
// composed of in-process components.
type ClusterOptions struct {
	// DBNode contains cluster options for spinning up dbnodes.
	DBNode *DBNodeClusterOptions
	// Aggregator is the optional cluster options for spinning up aggregators.
	// If Aggregator is nil, the cluster contains only m3coordinator and dbnodes.
	Aggregator *AggregatorClusterOptions
	// Coordinator is the options for spinning up the coordinator
	Coordinator CoordinatorClusterOptions
}

// Validate validates the ClusterOptions.
func (opts *ClusterOptions) Validate() error {
	if err := opts.DBNode.Validate(); err != nil {
		return fmt.Errorf("invalid dbnode cluster options: %w", err)
	}

	if opts.Aggregator != nil {
		if err := opts.Aggregator.Validate(); err != nil {
			return fmt.Errorf("invalid aggregator cluster options: %w", err)
		}
	}

	return nil
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
func NewDBNodeClusterOptions() *DBNodeClusterOptions {
	return &DBNodeClusterOptions{
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

// AggregatorClusterOptions contains the cluster options for spinning up
// aggregators.
type AggregatorClusterOptions struct {
	// RF is the replication factor to use for aggregators.
	// It should be 1 for non-replicated mode and 2 for leader-follower mode.
	RF int32
	// NumShards is the number of shards to use for each RF.
	NumShards int32
	// NumInstances is the number of aggregator instances in total.
	NumInstances int32
	// NumIsolationGroups is the number of isolation groups to split
	// aggregators into.
	NumIsolationGroups int32
}

// NewAggregatorClusterOptions creates AggregatorClusterOptions with sane defaults.
// Aggregator config must still be provided.
func NewAggregatorClusterOptions() *AggregatorClusterOptions {
	return &AggregatorClusterOptions{
		RF:                 1,
		NumShards:          4,
		NumInstances:       1,
		NumIsolationGroups: 1,
	}
}

// Validate validates the AggregatorClusterOptions.
func (a *AggregatorClusterOptions) Validate() error {
	if a.RF < 1 {
		return errors.New("rf must be at least 1")
	}

	if a.RF > 2 {
		return errors.New("rf must be at most 2")
	}

	if a.NumShards < 1 {
		return errors.New("numShards must be at least 1")
	}

	if a.NumInstances < 1 {
		return errors.New("numInstances must be at least 1")
	}

	if a.NumIsolationGroups < 1 {
		return errors.New("numIsolationGroups must be at least 1")
	}

	if a.RF > a.NumIsolationGroups {
		return errors.New("rf must be less than or equal to numIsolationGroups")
	}

	return nil
}

// CoordinatorClusterOptions contains the cluster options for spinning up
// the coordinator.
type CoordinatorClusterOptions struct {
	// GeneratePortsi ndicates whether to update the coordinator config to use open ports.
	GeneratePorts bool
}

// M3msgTopicOptions represents a set of options for an m3msg topic.
type M3msgTopicOptions struct {
	// Zone is the zone of the m3msg topic.
	Zone string
	// Env is the environment of the m3msg topic.
	Env string
	// TopicName is the topic name of the m3msg topic name.
	TopicName string
}

// PlacementRequestOptions represents a set of options for placement-related requests.
type PlacementRequestOptions struct {
	// Service is the type of service for the placement request.
	Service ServiceType
	// Env is the environment of the placement.
	Env string
	// Zone is the zone of the placement.
	Zone string
}

// ServiceType represents the type of an m3 service.
type ServiceType int

const (
	// ServiceTypeUnknown is an unknown service type.
	ServiceTypeUnknown ServiceType = iota
	// ServiceTypeM3DB represents M3DB service.
	ServiceTypeM3DB
	// ServiceTypeM3Aggregator represents M3aggregator service.
	ServiceTypeM3Aggregator
	// ServiceTypeM3Coordinator represents M3coordinator service.
	ServiceTypeM3Coordinator
)
