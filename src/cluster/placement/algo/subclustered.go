package algo

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/cluster/placement"
)

var (
	errIncompatibleWithSubclusteredAlgo = errors.New("could not apply subclustered algo on the placement")
)

type subclusteredPlacementAlgorithm struct {
	opts placement.Options
}

func newSubclusteredAlgorithm(opts placement.Options) placement.Algorithm {
	return subclusteredPlacementAlgorithm{opts: opts}
}

func (a subclusteredPlacementAlgorithm) IsCompatibleWith(p placement.Placement) error {
	if p == nil {
		return fmt.Errorf("placement is nil")
	}

	if !p.IsSharded() {
		return errIncompatibleWithSubclusteredAlgo
	}

	if !p.HasSubClusters() {
		return errIncompatibleWithSubclusteredAlgo
	}
	return nil
}

func (a subclusteredPlacementAlgorithm) InitialPlacement(
	instances []placement.Instance,
	shards []uint32,
	rf int,
) (placement.Placement, error) {
	if a.opts.InstancesPerSubCluster()%rf != 0 {
		return nil, fmt.Errorf("instances per subcluster is not a multiple of replica factor")
	}

	ph, err := newSubclusteredInitHelper(instances, shards, a.opts, rf)
	if err != nil {
		return nil, err
	}

	for i := 0; i < rf; i++ {
		err := ph.placeShards(newShards(shards), nil, ph.Instances())
		if err != nil {
			return nil, err
		}
	}

	return ph.generatePlacement(), nil
}

func (a subclusteredPlacementAlgorithm) AddReplica(p placement.Placement) (placement.Placement, error) {
	return nil, fmt.Errorf("AddReplica is not supported for subclustered placement")
}

func (a subclusteredPlacementAlgorithm) RemoveInstances(
	p placement.Placement,
	instanceIDs []string,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p = p.Clone()
	for _, instanceID := range instanceIDs {
		ph, leavingInstance, err := newubclusteredRemoveInstanceHelper(p, instanceID, a.opts)
		if err != nil {
			return nil, err
		}
		if err := ph.placeShards(leavingInstance.Shards().All(), leavingInstance, ph.Instances()); err != nil {
			return nil, err
		}

		// if err := ph.optimize(safe); err != nil {
		// 	return nil, err
		// }

		if p, _, err = addInstanceToPlacement(ph.generatePlacement(), leavingInstance, withShards); err != nil {
			return nil, err
		}
	}
	return tryCleanupShardState(p, a.opts)
}

// nolint:dupl
func (a subclusteredPlacementAlgorithm) AddInstances(
	p placement.Placement,
	instances []placement.Instance,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p = p.Clone()
	for _, instance := range instances {
		ph, addingInstance, err := newubclusteredAddInstanceHelper(p, instance, a.opts, withLeavingShardsOnly)
		if err != nil {
			return nil, err
		}

		if err := ph.addInstance(addingInstance); err != nil {
			return nil, err
		}

		p = ph.generatePlacement()
	}

	return tryCleanupShardState(p, a.opts)
}

func (a subclusteredPlacementAlgorithm) ReplaceInstances(
	p placement.Placement,
	leavingInstanceIDs []string,
	addingInstances []placement.Instance,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p = p.Clone()
	ph, leavingInstances, addingInstances, err := newubclusteredReplaceInstanceHelper(p,
		leavingInstanceIDs, addingInstances, a.opts)
	if err != nil {
		return nil, err
	}

	for i, leavingInstance := range leavingInstances {
		err = ph.placeShards(leavingInstance.Shards().All(), leavingInstance, []placement.Instance{addingInstances[i]})
		if err != nil {
			return nil, err
		}
		load := loadOnInstance(leavingInstance)
		if load != 0 {
			return nil, fmt.Errorf("could not fully replace all shards from %s, %d shards left unassigned",
				leavingInstance.ID(), load)
		}
	}

	p = ph.generatePlacement()
	for _, leavingInstance := range leavingInstances {
		if p, _, err = addInstanceToPlacement(p, leavingInstance, withShards); err != nil {
			return nil, err
		}
	}
	return tryCleanupShardState(p, a.opts)
}

func (a subclusteredPlacementAlgorithm) MarkShardsAvailable(
	p placement.Placement,
	instanceID string,
	shardIDs ...uint32,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	return markShardsAvailable(p.Clone(), instanceID, shardIDs, a.opts)
}

func (a subclusteredPlacementAlgorithm) MarkAllShardsAvailable(
	p placement.Placement,
) (placement.Placement, bool, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, false, err
	}

	return markAllShardsAvailable(p, a.opts)
}

func (a subclusteredPlacementAlgorithm) BalanceShards(
	p placement.Placement,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	// TODO: Implement subclustered balance shards logic
	return nil, fmt.Errorf("subclustered balance shards not yet implemented")
}
