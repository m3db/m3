package algo

import (
	"errors"
	"fmt"
	"github.com/m3db/m3/src/cluster/placement"
	//"github.com/m3db/m3/src/cluster/shard"
	"math/rand"
	"sort"
)

type BySubClusterIDInstanceID []placement.Instance

func (a BySubClusterIDInstanceID) Len() int { return len(a) }

func (a BySubClusterIDInstanceID) Less(i, j int) bool {
	if a[i].SubClusterID() == a[j].SubClusterID() {
		return a[i].ID() < a[j].ID()
	}
	return a[i].SubClusterID() < a[j].SubClusterID()
}

func (a BySubClusterIDInstanceID) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

type UInts []uint32

func (u UInts) Len() int { return len(u) }

func (u UInts) Less(i, j int) bool { return u[i] < u[j] }

func (u UInts) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

type UIntsDesc []uint32

func (u UIntsDesc) Len() int { return len(u) }

func (u UIntsDesc) Less(i, j int) bool { return u[i] > u[j] }

func (u UIntsDesc) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

type ByIsolationGroup []placement.Instance

func (b ByIsolationGroup) Len() int { return len(b) }

func (b ByIsolationGroup) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

func (b ByIsolationGroup) Less(i, j int) bool {
	return b[i].IsolationGroup() < b[j].IsolationGroup()
}

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
	sph := newInitSubClusterHelper(instances, shards, rf, a.opts)
	if err := sph.placeShardForInitialPlacement(newShards(shards)); err != nil {
		return nil, err
	}

	var (
		p = sph.generatePlacement()
	)

	return tryCleanupShardState(p, a.opts)

}

func (a subClusteredPlacementAlgorithm) AddReplica(p placement.Placement) (placement.Placement, error) {
	return nil, fmt.Errorf("not supported for subclustered algorithm")
}

func (a subClusteredPlacementAlgorithm) AddInstances(p placement.Placement, instances []placement.Instance) (placement.Placement, error) {

	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}
	ph, addingInstance, err := newSubClusteredAddInstanceHelper(p, instances, a.opts, withLeavingShardsOnly)
	if err != nil {
		return nil, err
	}

	p = p.Clone()
	for _, instance := range addingInstance {
		if err := ph.addInstance(instance); err != nil {
			return nil, err
		}

	}
	p = ph.generatePlacement()

	return tryCleanupShardState(p, a.opts)
}

func (a subClusteredPlacementAlgorithm) optimizeCurrentInstances(instances []placement.Instance) ([]placement.Instance, error) {
	shards := make([]uint32, 0)
	uniqueShards := make(map[uint32]struct{})
	for _, instance := range instances {
		assignedShards := instance.Shards().All()
		for _, s := range assignedShards {
			if _, ok := uniqueShards[s.ID()]; !ok {
				shards = append(shards, s.ID())
				uniqueShards[s.ID()] = struct{}{}
			}
		}
	}
	emptyPlacement := placement.NewPlacement().SetReplicaFactor(3).SetInstances(instances).SetShards(shards)
	shardedAlgo := newShardedAlgorithm(a.opts)
	p, err := shardedAlgo.BalanceShards(emptyPlacement)
	if err != nil {
		return nil, err
	}
	return p.Instances(), nil
}

func (a subClusteredPlacementAlgorithm) MoveShards(shards []uint32, rf int, from, to []placement.Instance) (placement.Placement, error) {
	// optimize current placement
	//shardToInstanceMappingFrom, iGToiInstanceMapFrom := getShardAndIGMap(from)
	//shardToInstanceMappingTo, iGToiInstanceMapTo := getShardAndIGMap(to)
	//a.deterministicShuffle(shards, int64(len(iGToiInstanceMapTo)))
	//
	//for _, shardID := range shards {
	//	instancesFrom := shardToInstanceMappingFrom[shardID]
	//	for _, instance := range instancesFrom {
	//		helper := newHelper(placement.NewPlacement().SetInstances(append(to, instance)), rf, a.opts)
	//		shard
	//		helper.placeShards([]shard.Shard{instance.Shards().Shard(shardID)}, instance, to)
	//	}
	//}
	return nil, nil
}

func (a subClusteredPlacementAlgorithm) assignShard(s uint32, from, to placement.Instance) {

}

func (a subClusteredPlacementAlgorithm) deterministicShuffle(arr []uint32, seed int64) {
	r := rand.New(rand.NewSource(seed)) // Create a new PRNG with the fixed seed

	for i := len(arr) - 1; i > 0; i-- {
		j := r.Intn(i + 1) // Generate a random index
		arr[i], arr[j] = arr[j], arr[i]
	}
}

func getShardAndIGMap(instances []placement.Instance) (map[uint32][]placement.Instance, map[string][]placement.Instance) {
	shardToInstanceMapping := make(map[uint32][]placement.Instance)
	iGToiInstanceMap := make(map[string][]placement.Instance)
	for _, instance := range instances {
		isolationGroup := instance.IsolationGroup()
		for _, shard := range instance.Shards().AllIDs() {
			if _, ok := shardToInstanceMapping[shard]; !ok {
				shardToInstanceMapping[shard] = make([]placement.Instance, 0)
			}
			shardToInstanceMapping[shard] = append(shardToInstanceMapping[shard], instance)
		}
		if _, ok := iGToiInstanceMap[isolationGroup]; !ok {
			iGToiInstanceMap[isolationGroup] = make([]placement.Instance, 0)
		}
		iGToiInstanceMap[isolationGroup] = append(iGToiInstanceMap[isolationGroup], instance)
	}
	for _, instances := range shardToInstanceMapping {
		sort.Sort(placement.ByIDAscending(instances))
	}
	for _, instances := range iGToiInstanceMap {
		sort.Sort(placement.ByIDAscending(instances))
	}

	return shardToInstanceMapping, iGToiInstanceMap
}

func (a subClusteredPlacementAlgorithm) RemoveInstances(p placement.Placement, leavingInstanceIDs []string) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}
	ph, leavingInstances, err := newSubClusterRemoveInstancesHelper(p, leavingInstanceIDs, a.opts)
	if err != nil {
		return nil, err
	}

	p = p.Clone()
	for _, instance := range leavingInstances {
		if err := ph.placeShards(instance.Shards().All(), instance, ph.Instances()); err != nil {
			return nil, err
		}

	}
	p = ph.generatePlacement()
	return tryCleanupShardState(p, a.opts)
}

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
		if err != nil && err != errNotEnoughIsolationGroups {
			// errNotEnoughIsolationGroups means the adding instances do not
			// have enough isolation groups to take all the shards, but the rest
			// instances might have more isolation groups to take all the shards.
			return nil, err
		}
		load := loadOnInstance(leavingInstance)
		if load != 0 && !a.opts.AllowPartialReplace() {
			return nil, fmt.Errorf("could not fully replace all shards from %s, %d shards left unassigned",
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

func (a subClusteredPlacementAlgorithm) MarkShardsAvailable(p placement.Placement, instanceID string, shardIDs ...uint32) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	return markShardsAvailable(p.Clone(), instanceID, shardIDs, a.opts)
}

func (a subClusteredPlacementAlgorithm) MarkAllShardsAvailable(p placement.Placement) (placement.Placement, bool, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, false, err
	}

	return markAllShardsAvailable(p, a.opts)
}

func (a subClusteredPlacementAlgorithm) BalanceShards(p placement.Placement) (placement.Placement, error) {
	//TODO implement me
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

func (a subClusteredPlacementAlgorithm) validateInstancesPerSubCluster(subClusterToInstanceMapping map[uint32][]placement.Instance, rf int) error {
	for subClusterID, instances := range subClusterToInstanceMapping {
		if len(instances) > a.opts.InstancesPerSubCluster() {
			return errors.New("number of instances per subCluster is greater than the number of allowed instances per subCluster")
		}
		if len(instances)%rf != 0 {
			return errors.New(fmt.Sprintf("sub %d cluster too small, atleast %d instances are required per subcluster", subClusterID, rf))
		}
	}
	return nil
}

func (a subClusteredPlacementAlgorithm) subClusterToShardMapping(subClusterToInstanceMapping map[uint32][]placement.Instance, shards []uint32) {
	subClusterToWeightMap := make(map[uint32]int)
	currSubClusterToShardMapping := make(map[uint32]map[uint32]struct{})
	totalWeight := 0
	for subClusterID, instances := range subClusterToInstanceMapping {
		instanceWeight := 0
		subClusterShards := make(map[uint32]struct{})
		for _, instance := range instances {
			instanceWeight += int(instance.Weight())
			for _, shard := range instance.Shards().All() {
				subClusterShards[shard.ID()] = struct{}{}
			}
		}
		currSubClusterToShardMapping[subClusterID] = subClusterShards
		subClusterToWeightMap[subClusterID] = instanceWeight
		totalWeight += instanceWeight
	}
}

func (a subClusteredPlacementAlgorithm) placeShards(subClusterToShardMapping map[uint32][]uint32, subClusterToInstanceMapping map[uint32][]placement.Instance) (placement.Placement, error) {
	panic("implement me")
}

func (a subClusteredPlacementAlgorithm) instancesInSubCluster(instances []placement.Instance, rf int, validate bool) (map[uint32][]placement.Instance, error) {
	subClusterToInstanceMapping := make(map[uint32][]placement.Instance)
	for _, instance := range instances {
		subClusterID := instance.SubClusterID()
		if subClusterID == 0 {
			fmt.Println(instance.ID(), instance.SubClusterID())
			return nil, errors.New("sub cluster ID cannot be 0")
		}
		if subClusterInstances, ok := subClusterToInstanceMapping[subClusterID]; ok {
			subClusterInstances = append(subClusterInstances, instance)
			subClusterToInstanceMapping[subClusterID] = subClusterInstances
			continue
		}
		subClusterToInstanceMapping[subClusterID] = []placement.Instance{instance}
	}
	if validate {
		err := a.validateInstancesPerSubCluster(subClusterToInstanceMapping, rf)
		if err != nil {
			return nil, err
		}
	}
	return subClusterToInstanceMapping, nil
}
