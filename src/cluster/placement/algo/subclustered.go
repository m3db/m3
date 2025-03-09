package algo

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/cluster/placement"
)

type subClusterByWeightDesc []*subCluster

func (s subClusterByWeightDesc) Len() int      { return len(s) }
func (s subClusterByWeightDesc) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s subClusterByWeightDesc) Less(i, j int) bool {
	if s[i].weight == s[j].weight {
		return s[i].ID < s[j].ID
	}
	return s[i].weight < s[j].weight
}

type subClusteredPlacementAlgorithm struct {
	opts placement.Options
}

func (a subClusteredPlacementAlgorithm) InitialPlacement(instances []placement.Instance, shards []uint32, rf int) (placement.Placement, error) {
	fmt.Printf("building initial placement")
	fmt.Printf("Printing instances: %+v\n", instances)
	fmt.Printf("Printing options: %+v\n", a.opts)

	sph := newInitSubClusterHelper(instances, shards, rf, a.opts)
	if err := sph.placeShardForInitialPlacement(newShards(shards)); err != nil {
		return nil, err
	}

	var (
		p = sph.generatePlacement()
	)

	fmt.Printf("Printing final placement state1: %+v\n", p)
	return tryCleanupShardState(p, a.opts)

}

func (a subClusteredPlacementAlgorithm) AddReplica(p placement.Placement) (placement.Placement, error) {
	return nil, fmt.Errorf("not supported for subclustered algorithm")
}

func (a subClusteredPlacementAlgorithm) AddInstances(p placement.Placement, instances []placement.Instance) (placement.Placement, error) {
	fmt.Printf("adding instances")
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}
	ph, addingInstance, err := newSubClusteredAddInstanceHelper(p.Clone(), instances, a.opts, withLeavingShardsOnly)
	if err != nil {
		return nil, err
	}

	for _, instance := range addingInstance {
		if err := ph.addInstance(instance); err != nil {
			return nil, err
		}

	}
	p = ph.generatePlacement()
	fmt.Printf("Printing final placement state2: %+v\n", p)
	return tryCleanupShardState(p, a.opts)
}
func (a subClusteredPlacementAlgorithm) RemoveInstances(p placement.Placement, leavingInstanceIDs []string) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}
	ph, leavingInstances, err := newSubClusterRemoveInstancesHelper(p.Clone(), leavingInstanceIDs, a.opts)
	if err != nil {
		return nil, err
	}

	for _, instance := range leavingInstances {
		if err := ph.placeShards(instance.Shards().All(), instance, ph.Instances()); err != nil {
			return nil, err
		}

	}
	p = ph.generatePlacement()
	return tryCleanupShardState(p, a.opts)
}

// nolint:dupl
func (a subClusteredPlacementAlgorithm) ReplaceInstances(p placement.Placement, leavingInstanecIDs []string, addingInstances []placement.Instance) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p = p.Clone()
	ph, leavingInstances, addingInstances, err := newSubClusterReplaceInstanceHelper(p, leavingInstanecIDs, addingInstances, a.opts)
	if err != nil {
		return nil, err
	}

	for _, leavingInstance := range leavingInstances {
		err = ph.placeShards(leavingInstance.Shards().All(), leavingInstance, addingInstances)
		if err != nil && !errors.Is(err, errNotEnoughIsolationGroups) {
			// errNotEnoughIsolationGroups means the adding instances do not
			// have enough isolation groups to take all the shards, but the rest
			// instances might have more isolation groups to take all the shards.
			return nil, err
		}
		load := loadOnInstance(leavingInstance)
		if load != 0 && !a.opts.AllowPartialReplace() {
			return nil, fmt.Errorf("could not fully replace all shards "+
				"from %s, %d shards left unassigned",
				leavingInstance.ID(), load)
		}
	}

	if a.opts.AllowPartialReplace() {
		// Place the shards left on the leaving instance to the rest of the cluster.
		for _, leavingInstance := range leavingInstances {
			if err = ph.placeShards(leavingInstance.Shards().All(), leavingInstance, ph.Instances()); err != nil {
				return nil, err
			}
		}

		if err := ph.optimize(unsafe); err != nil {
			return nil, err
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

func (a subClusteredPlacementAlgorithm) MarkShardsAvailable(
	p placement.Placement,
	instanceID string,
	shardIDs ...uint32,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	return markShardsAvailable(p.Clone(), instanceID, shardIDs, a.opts)
}

func (a subClusteredPlacementAlgorithm) MarkAllShardsAvailable(
	p placement.Placement,
) (placement.Placement, bool, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, false, err
	}

	return markAllShardsAvailable(p, a.opts)
}

func (a subClusteredPlacementAlgorithm) BalanceShards(p placement.Placement) (placement.Placement, error) {
	// TODO implement me
	panic("implement me")
}

func newSubClusteredAlgorithm(opts placement.Options) placement.Algorithm {
	return subClusteredPlacementAlgorithm{opts: opts}
}

func (a subClusteredPlacementAlgorithm) IsCompatibleWith(p placement.Placement) error {
	if !p.IsSharded() {
		return errIncompatibleWithShardedAlgo
	}
	if !p.HasSubClusters() {
		return errors.New("could not apply subclustered algo on the placement")
	}

	return nil
}
