// Copyright (c) 2017 Uber Technologies, Inc.
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

package algo

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
)

var (
	errIncompatibleWithMirrorAlgo = errors.New("could not apply mirrored algo on the placement")
)

type mirroredAlgorithm struct {
	opts        services.PlacementOptions
	shardedAlgo placement.Algorithm
}

func newMirroredAlgorithm(opts services.PlacementOptions) placement.Algorithm {
	return mirroredAlgorithm{
		opts: opts,
		// Mirrored algorithm requires full replacement.
		shardedAlgo: newShardedAlgorithm(opts.SetAllowPartialReplace(false)),
	}
}

func (a mirroredAlgorithm) IsCompatibleWith(p services.Placement) error {
	if !p.IsMirrored() {
		return errIncompatibleWithMirrorAlgo
	}

	if !p.IsSharded() {
		return errIncompatibleWithMirrorAlgo
	}

	return nil
}

func (a mirroredAlgorithm) InitialPlacement(
	instances []services.PlacementInstance,
	shards []uint32,
	rf int,
) (services.Placement, error) {
	mirrorInstances, err := groupInstancesByShardSetID(instances, rf)
	if err != nil {
		return nil, err
	}

	// We use the sharded algorithm to generate a mirror placement with rf equals 1.
	mirrorPlacement, err := a.shardedAlgo.InitialPlacement(mirrorInstances, shards, 1)
	if err != nil {
		return nil, err
	}

	return placementFromMirror(mirrorPlacement, instances, rf)
}

func (a mirroredAlgorithm) AddReplica(p services.Placement) (services.Placement, error) {
	// TODO(cw): We could support AddReplica(p services.Placement, instances []services.PlacementInstance)
	// and apply the shards from the new replica to the adding instances in the future.
	return nil, errors.New("not supported")
}

func (a mirroredAlgorithm) RemoveInstances(
	p services.Placement,
	instanceIDs []string,
) (services.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	removingInstances := make([]services.PlacementInstance, len(instanceIDs))
	for i, id := range instanceIDs {
		instance, ok := p.Instance(id)
		if !ok {
			return nil, fmt.Errorf("instance %s not found in placement", id)
		}
		removingInstances[i] = instance
	}

	p, err := placement.MarkAllShardsAsAvailable(p)
	if err != nil {
		return nil, err
	}

	mirrorPlacement, err := mirrorFromPlacement(p)
	if err != nil {
		return nil, err
	}

	mirrorInstances, err := groupInstancesByShardSetID(removingInstances, p.ReplicaFactor())
	if err != nil {
		return nil, err
	}

	for _, instance := range mirrorInstances {
		ph, leavingInstance, err := newRemoveInstanceHelper(mirrorPlacement, instance.ID(), a.opts)
		if err != nil {
			return nil, err
		}
		// Place the shards from the leaving instance to the rest of the cluster
		if err := ph.PlaceShards(leavingInstance.Shards().All(), leavingInstance, ph.Instances()); err != nil {
			return nil, err
		}

		if mirrorPlacement, _, err = addInstanceToPlacement(ph.GeneratePlacement(), leavingInstance, nonEmptyOnly); err != nil {
			return nil, err
		}
	}
	return placementFromMirror(mirrorPlacement, p.Instances(), p.ReplicaFactor())
}

func (a mirroredAlgorithm) AddInstances(
	p services.Placement,
	addingInstances []services.PlacementInstance,
) (services.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p, err := placement.MarkAllShardsAsAvailable(p)
	if err != nil {
		return nil, err
	}

	// At this point, all leaving instances in the placement are cleaned up.
	for _, instance := range addingInstances {
		if _, exist := p.Instance(instance.ID()); exist {
			return nil, fmt.Errorf("instance %s already exist in the placement", instance.ID())
		}
	}

	mirrorPlacement, err := mirrorFromPlacement(p)
	if err != nil {
		return nil, err
	}

	mirrorInstances, err := groupInstancesByShardSetID(addingInstances, p.ReplicaFactor())
	if err != nil {
		return nil, err
	}

	for _, instance := range mirrorInstances {
		if _, ok := mirrorPlacement.Instance(instance.ID()); ok {
			return nil, fmt.Errorf("shard set id %d already exist in current placement", instance.ShardSetID())
		}
		ph := newAddInstanceHelper(mirrorPlacement, instance, a.opts)
		ph.AddInstance(instance)
		mirrorPlacement = ph.GeneratePlacement()
	}

	return placementFromMirror(mirrorPlacement, append(p.Instances(), addingInstances...), p.ReplicaFactor())
}

func (a mirroredAlgorithm) ReplaceInstances(
	p services.Placement,
	leavingInstanceIDs []string,
	addingInstances []services.PlacementInstance,
) (services.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	if len(addingInstances) != len(leavingInstanceIDs) {
		return nil, fmt.Errorf("could not replace %d instances with %d instances for mirrored replace", len(leavingInstanceIDs), len(addingInstances))
	}

	p, err := placement.MarkAllShardsAsAvailable(p)
	if err != nil {
		return nil, err
	}

	for i := range leavingInstanceIDs {
		// We want full replacement for each instance.
		p, err = a.shardedAlgo.ReplaceInstances(p, leavingInstanceIDs[i:i+1], addingInstances[i:i+1])
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

func groupInstancesByShardSetID(
	instances []services.PlacementInstance,
	rf int,
) ([]services.PlacementInstance, error) {
	var (
		shardSetMap = make(map[uint32]*shardSetMetadata, len(instances))
		res         = make([]services.PlacementInstance, 0, len(instances))
	)
	for _, instance := range instances {
		ssID := instance.ShardSetID()
		weight := instance.Weight()
		rack := instance.Rack()
		meta, ok := shardSetMap[ssID]
		if !ok {
			meta = &shardSetMetadata{
				weight: weight,
				racks:  make(map[string]struct{}, rf),
				shards: instance.Shards(),
			}
			shardSetMap[ssID] = meta
		}
		if _, ok := meta.racks[rack]; ok {
			return nil, fmt.Errorf("found duplicated rack %s for shardset id %d", rack, ssID)
		}

		if meta.weight != weight {
			return nil, fmt.Errorf("found different weights: %d and %d, for shardset id %d", meta.weight, weight, ssID)
		}

		meta.racks[rack] = struct{}{}
		meta.count++
	}

	for ssID, meta := range shardSetMap {
		if meta.count != rf {
			return nil, fmt.Errorf("found %d count of shard set id %d, expecting %d", meta.count, ssID, rf)
		}

		// NB(cw) The shard set ID should to be assigned in placement service,
		// the algorithm does not change the shard set id assigned to each instance.
		ssIDStr := strconv.Itoa(int(ssID))
		res = append(
			res,
			placement.NewInstance().
				SetID(ssIDStr).
				SetRack(ssIDStr).
				SetWeight(meta.weight).
				SetShardSetID(ssID).
				SetShards(placement.CloneShards(meta.shards)),
		)
	}

	return res, nil
}

// mirrorFromPlacement zips all instances with the same shardSetID into a virtual instance
// and create a placement with those virtual instance and rf=1.
func mirrorFromPlacement(p services.Placement) (services.Placement, error) {
	mirrorInstances, err := groupInstancesByShardSetID(p.Instances(), p.ReplicaFactor())
	if err != nil {
		return nil, err
	}

	return placement.NewPlacement().
		SetInstances(mirrorInstances).
		SetReplicaFactor(1).
		SetShards(p.Shards()).
		SetIsSharded(true).
		SetIsMirrored(true), nil
}

// placementFromMirror duplicates the shards for each shard set id and assign
// them to the instance with the shard set id.
func placementFromMirror(
	mirror services.Placement,
	instances []services.PlacementInstance,
	rf int,
) (services.Placement, error) {
	var (
		mirrorInstances     = mirror.Instances()
		shardSetMap         = make(map[uint32][]services.PlacementInstance, len(mirrorInstances))
		instancesWithShards = make([]services.PlacementInstance, 0, len(instances))
	)
	for _, instance := range instances {
		instances, ok := shardSetMap[instance.ShardSetID()]
		if !ok {
			instances = make([]services.PlacementInstance, 0, rf)
		}
		instances = append(instances, instance)
		shardSetMap[instance.ShardSetID()] = instances
	}

	for _, mirrorInstance := range mirrorInstances {
		instances, err := instancesFromMirror(mirrorInstance, shardSetMap)
		if err != nil {
			return nil, err
		}
		instancesWithShards = append(instancesWithShards, instances...)
	}

	return placement.NewPlacement().
		SetInstances(instancesWithShards).
		SetReplicaFactor(rf).
		SetShards(mirror.Shards()).
		SetIsMirrored(true).
		SetIsSharded(true), nil
}

func instancesFromMirror(
	mirrorInstance services.PlacementInstance,
	instancesMap map[uint32][]services.PlacementInstance,
) ([]services.PlacementInstance, error) {
	ssID := mirrorInstance.ShardSetID()
	instances, ok := instancesMap[ssID]
	if !ok {
		return nil, fmt.Errorf("could not find shard set id %d in placement", ssID)
	}

	shards := mirrorInstance.Shards()
	for i, instance := range instances {
		newShards := make([]shard.Shard, shards.NumShards())
		for j, s := range shards.All() {
			newShard := shard.NewShard(s.ID()).SetState(s.State())
			sourceID := s.SourceID()
			if sourceID != "" {
				// The sourceID in the mirror placement is shardSetID, need to be converted
				// to instanceID.
				shardSetID, err := strconv.Atoi(sourceID)
				if err != nil {
					return nil, fmt.Errorf("could not convert source id %s to shard set id", sourceID)
				}
				sourceInstances, ok := instancesMap[uint32(shardSetID)]
				if !ok {
					return nil, fmt.Errorf("could not find source id %s in placement", sourceID)
				}

				sourceID = sourceInstances[i].ID()
			}
			newShards[j] = newShard.SetSourceID(sourceID)
		}
		instances[i] = instance.SetShards(shard.NewShards(newShards))
	}
	return instances, nil
}

type shardSetMetadata struct {
	weight uint32
	count  int
	racks  map[string]struct{}
	shards shard.Shards
}
