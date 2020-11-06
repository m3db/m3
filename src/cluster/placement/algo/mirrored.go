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
	"io"
	"os"
	"strconv"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
)

var (
	errIncompatibleWithMirrorAlgo = errors.New("could not apply mirrored algo on the placement")
)

type mirroredAlgorithm struct {
	opts        placement.Options
	shardedAlgo placement.Algorithm
}

func newMirroredAlgorithm(opts placement.Options) placement.Algorithm {
	return mirroredAlgorithm{
		opts: opts,
		// Mirrored algorithm requires full replacement.
		shardedAlgo: newShardedAlgorithm(opts.SetAllowPartialReplace(false)),
	}
}

func (a mirroredAlgorithm) IsCompatibleWith(p placement.Placement) error {
	if !p.IsMirrored() {
		return errIncompatibleWithMirrorAlgo
	}

	if !p.IsSharded() {
		return errIncompatibleWithMirrorAlgo
	}

	return nil
}

func (a mirroredAlgorithm) InitialPlacement(
	instances []placement.Instance,
	shards []uint32,
	rf int,
) (placement.Placement, error) {
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

func (a mirroredAlgorithm) AddReplica(p placement.Placement) (placement.Placement, error) {
	// TODO(cw): We could support AddReplica(p placement.Placement, instances []placement.Instance)
	// and apply the shards from the new replica to the adding instances in the future.
	return nil, errors.New("not supported")
}

func (a mirroredAlgorithm) RemoveInstances(
	p placement.Placement,
	instanceIDs []string,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	nowNanos := a.opts.NowFn()().UnixNano()
	// If the instances being removed are all the initializing instances in the placement.
	// We just need to return these shards back to their sources.
	if exactlyInstancesInitializing(p, instanceIDs, nowNanos) {
		return a.returnInitializingShards(p, instanceIDs)
	}

	p, _, err := a.MarkAllShardsAvailable(p)
	if err != nil {
		return nil, err
	}

	removingInstances := make([]placement.Instance, 0, len(instanceIDs))
	for _, id := range instanceIDs {
		instance, ok := p.Instance(id)
		if !ok {
			return nil, fmt.Errorf("instance %s does not exist in the placement", id)
		}
		removingInstances = append(removingInstances, instance)
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
		if mirrorPlacement, err = a.shardedAlgo.RemoveInstances(
			mirrorPlacement,
			[]string{instance.ID()},
		); err != nil {
			return nil, err
		}
	}
	return placementFromMirror(mirrorPlacement, p.Instances(), p.ReplicaFactor())
}

func (a mirroredAlgorithm) AddInstances(
	p placement.Placement,
	addingInstances []placement.Instance,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	nowNanos := a.opts.NowFn()().UnixNano()
	// If the instances being added are all the leaving instances in the placement.
	// We just need to get their shards back.
	if exactlyInstancesLeaving(p, addingInstances, nowNanos) {
		return a.reclaimLeavingShards(p, addingInstances)
	}

	p, _, err := a.MarkAllShardsAvailable(p)
	if err != nil {
		return nil, err
	}

	// At this point, all leaving instances in the placement are cleaned up.
	if addingInstances, err = validAddingInstances(p, addingInstances); err != nil {
		return nil, err
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
		if mirrorPlacement, err = a.shardedAlgo.AddInstances(
			mirrorPlacement,
			[]placement.Instance{instance},
		); err != nil {
			return nil, err
		}
	}

	return placementFromMirror(mirrorPlacement, append(p.Instances(), addingInstances...), p.ReplicaFactor())
}

func (a mirroredAlgorithm) ReplaceInstances(
	p placement.Placement,
	leavingInstanceIDs []string,
	addingInstances []placement.Instance,
) (placement.Placement, error) {
	err := a.IsCompatibleWith(p)
	if err != nil {
		return nil, err
	}

	if len(addingInstances) != len(leavingInstanceIDs) {
		return nil, fmt.Errorf("could not replace %d instances with %d instances for mirrored replace", len(leavingInstanceIDs), len(addingInstances))
	}

	nowNanos := a.opts.NowFn()().UnixNano()
	leaving := atLeastInstancesLeaving(p, addingInstances, nowNanos)

	initializing := atLeastInstancesInitializing(p, leavingInstanceIDs, nowNanos)
	if leaving && initializing && instancesAreReplacement(p, leavingInstanceIDs, addingInstances) {
		if p, err = a.reclaimLeavingShards(p, addingInstances); err != nil {
			return nil, err
		}

		// NB(cw) I don't think we will ever get here, but just being defensive.
		return a.returnInitializingShards(p, leavingInstanceIDs)
	}

	p, err = a.markReplacingInstancesAvailable(p, leavingInstanceIDs)
	if err != nil {
	    return nil, err
	}

	// At this point, all leaving instances in the placement are cleaned up.
	if addingInstances, err = validAddingInstances(p, addingInstances); err != nil {
		return nil, err
	}
	for i := range leavingInstanceIDs {
		// We want full replacement for each instance.
		if p, err = a.shardedAlgo.ReplaceInstances(
			p,
			leavingInstanceIDs[i:i+1],
			addingInstances[i:i+1],
		); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// The leaving instances were a replacement for the adding instances iff:
//  - all shards on the leaving node are initializing and came from the adding node
//  - all shards on the adding node are leaving (and implicitly, they should be going to the adding node--I could add a defensive check there too, come to think of it)
//  - every leaving node has a unique adding node and every adding node has a unique leaving node (i.e. there is a bijection between them).
// amainsd: TODO BEFORE LAND: cover the bijection case
func instancesAreReplacement(p placement.Placement, leavingInstanceIDs []string, sourceAddingInstances []placement.Instance) bool {
	// lookup all the instances. If one of them isn't there, we can't proceed with this case--return false
	var leavingInstances []placement.Instance 
	for _, instID := range leavingInstanceIDs {
		inst, ok := p.Instance(instID)
		if !ok {
			return false
		}
		leavingInstances = append(leavingInstances, inst)
	}

	var addingInstances []placement.Instance
	for _, addingInst := range sourceAddingInstances {
		// N.B.: lookup here because the adding instance doesn't have shards yet.
		inst, ok := p.Instance(addingInst.ID())
		if !ok {
			return false
		}
		addingInstances = append(addingInstances, inst)
	}
	
	// every shard in leaving must have come from the same node in adding
	validNodeSources := make(map[string]struct{})
	for _, addingInst := range addingInstances {
		validNodeSources[addingInst.ID()] = struct{}{}
	}


	for _, leavingInst := range leavingInstances {
		var leavingNode string
		for _, s := range leavingInst.Shards().All() {
			switch s.State() {
			case shard.Initializing:
				// pass
			default:
				return false
			}

			// didn't come from one of the adding nodes
			if s.SourceID() == "" {
				return false
			}

			_, cameFromAddingNode := validNodeSources[s.SourceID()]
			if !cameFromAddingNode {
				return false
			}

			if leavingNode == "" {
				// first iteration
				leavingNode = s.SourceID()
				continue
			}

			if leavingNode != s.SourceID() {
				// came from a different node
				return false
			}
		}
	}

	return true
}

type leavingShardKey struct{
	LeavingShardSetID uint32
	ShardID       uint32
}

func (a mirroredAlgorithm) markReplacingInstancesAvailable(p placement.Placement, leavingInstanceIDs []string) (placement.Placement, error) {
	helper, err := newReplaceMarkAvailableHelper(a, p)
	if err != nil {
	    return nil, err
	}

	return helper.MarkLeavingInstancesAvailable(p, leavingInstanceIDs)
}

type replaceMarkAvailableHelper struct{
	algo                              mirroredAlgorithm
	leavingShardSetIDToOwningShardSet map[leavingShardKey]uint32
	shardSetIDToOwnerIDs              map[uint32][]string
}

func newReplaceMarkAvailableHelper(
	algo mirroredAlgorithm,
	p placement.Placement,
) (replaceMarkAvailableHelper, error) {
	leavingShardIDToOwningInst := make(map[leavingShardKey]uint32)
	for _, inst := range p.Instances() {
		for _, s := range inst.Shards().All() {
			if s.State() != shard.Initializing {
				continue
			}
			sourceID := s.SourceID()
			if sourceID == "" {
				// this is true for initial placements
				continue
			}

			// determine the source *shard set*
			sourceInst, ok := p.Instance(sourceID)
			if !ok {
				return replaceMarkAvailableHelper{}, fmt.Errorf(
					"source instance %q not in placement for shard %d on instance %q",
					sourceID,
					s.ID(),
					inst.ID(),
					)
			}

			leavingShardIDToOwningInst[leavingShardKey{
				ShardID:           s.ID(),
				LeavingShardSetID: sourceInst.ShardSetID(),
			}] = inst.ShardSetID()
		}
	}

	shardSetIDToOwners := make(map[uint32][]string)
	for _, inst := range p.Instances() {
		shardSetIDToOwners[inst.ShardSetID()] = append(shardSetIDToOwners[inst.ShardSetID()], inst.ID())
	}
	return replaceMarkAvailableHelper{
		algo:                              algo,
		leavingShardSetIDToOwningShardSet: leavingShardIDToOwningInst,
		shardSetIDToOwnerIDs:              shardSetIDToOwners,
	}, nil
}

func (h replaceMarkAvailableHelper) MarkLeavingInstancesAvailable(
	p placement.Placement,
	leavingInstanceIDs []string,
) (placement.Placement, error) {
	// lookup the entire shardset
	// amainsd: what if we get the same shard set multiple times (e.g. multi node replace in same shardset)? Could dedupe here.
	var err error
	for _, leavingInstID := range leavingInstanceIDs {
		leavingInst, ok := p.Instance(leavingInstID)
		if !ok {
			return nil, fmt.Errorf("")
		}

		shardSetOwners, ok := h.shardSetIDToOwnerIDs[leavingInst.ShardSetID()]
		if !ok {
			return nil, fmt.Errorf(
				"shard set %d isn't owned by anything in the placement; placement may be invalid",
				leavingInst.ShardSetID(),
			)
		}
		p, err = h.markReplacePairAvailable(p, shardSetOwners)
		if err != nil {
		    return nil, err
		}
	}
	return p, nil
}

func (h replaceMarkAvailableHelper) markReplacePairAvailable(
	p placement.Placement,
	shardSetOwners []string,
) (placement.Placement, error) {
	for _, leavingInstID := range shardSetOwners {
		leavingInst, err := getInstanceOrErr(p, leavingInstID)
		if err != nil {
		    return nil, err
		}
		// find initializing shards
		for _, s := range leavingInst.Shards().All() {
			switch s.State() {
			case shard.Available:
				continue
			case shard.Initializing:
				p, err = h.algo.MarkShardsAvailable(p, leavingInstID, s.ID())
				if err != nil {
					return nil, fmt.Errorf("failed marking initializing shard available: %w", err)
				}
				// mark available
			case shard.Leaving:
				// find the pair, mark that available
				newOwnerShardSet, ok := h.leavingShardSetIDToOwningShardSet[leavingShardKey{
					LeavingShardSetID: leavingInst.ShardSetID(),
					ShardID:       s.ID(),
				}]
				if !ok {
					return nil, fmt.Errorf(
						"couldn't find new owning instance for leaving shard %d on instance %s",
						s.ID(),
						leavingInstID,
					)
				}

				newOwners, ok := h.shardSetIDToOwnerIDs[newOwnerShardSet]
				if !ok {
					return nil, fmt.Errorf(
						"couldn't find new owning shardset %d for shard %d on instance %s",
						newOwnerShardSet,
						s.ID(),
						leavingInstID,
					)
				}

				for _, newOwnerID := range newOwners {
					newOwner, err := getInstanceOrErr(p, newOwnerID)
					if err != nil {
					    return nil, err
					}
					newOwnerShard, ok := newOwner.Shards().Shard(s.ID())
					if !ok {
						return nil, fmt.Errorf("shards don't match")
					}

					if newOwnerShard.State() != shard.Initializing {
						continue
					}

					p, err = h.algo.MarkShardsAvailable(p, newOwner.ID(), s.ID())
					if err != nil {
						return nil, fmt.Errorf(
							"marking shards available on new owning instance %s (shardSetID: %d): %w",
							newOwner.ID(),
							newOwnerShardSet,
							err,
						)
					}
				}
			}
		}
	}

	return p, nil

	// initializingShards := leavingInst.Shards().ShardsForState(shard.Initializing)
	// initializingShardIDs := make([]uint32, 0, len(initializingShards))
	// for _, s := range initializingShards {
	// 	initializingShardIDs = append(initializingShardIDs, s.ID())
	// }
	//
	// p, err = a.MarkShardsAvailable(p, leavingInstID, initializingShardIDs...)
	// if err != nil {
	// 	return nil, fmt.Errorf(
	// 		"failed to mark shards available for leaving instance %s: %w",
	// 		leavingInstID,
	// 		err,
	// 	)
	// }
}

func getInstanceOrErr(p placement.Placement, id string) (placement.Instance, error) {
	inst, ok := p.Instance(id)
	if !ok {
		return nil, fmt.Errorf("instance %s not in placement", id)
	}
	return inst, nil
}


// amainsd: cleanup
func printInstances(instances []placement.Instance) {
	if err := printInstancesToStream(os.Stdout, instances); err != nil {
		// N.B.: printing to stdout basically shouldn't error; panic is appropriate here.
		panic(err.Error())
	}
}

func printInstancesToStream(out io.Writer, instances []placement.Instance) error {
	for _, instance := range instances {
		s := instance.Shards()
		_, err := fmt.Fprintf(out, "instance %s shardset: %d, weight %d: %d Init, %d Available, %d Leaving, %d total\n",
			instance.ID(), instance.ShardSetID(), instance.Weight(), s.NumShardsForState(shard.Initializing),
			s.NumShardsForState(shard.Available), s.NumShardsForState(shard.Leaving), s.NumShards())
		if err != nil {
			return err
		}
	}
	return nil
}

func printPlacement(p placement.Placement) {
	if err := printPlacementToStream(os.Stdout, p); err != nil {
		// printing to stdout shouldn't error; panic is appropriate if it does.
		panic(err.Error())
	}
}

func printPlacementToStream(out io.Writer, p placement.Placement) error {
	if _, err := fmt.Fprintln(out, "shards distribution:"); err != nil {
		return err
	}

	return printInstancesToStream(out, p.Instances())
}


func (a mirroredAlgorithm) MarkShardsAvailable(
	p placement.Placement,
	instanceID string,
	shardIDs ...uint32,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	return a.shardedAlgo.MarkShardsAvailable(p, instanceID, shardIDs...)
}

func (a mirroredAlgorithm) MarkAllShardsAvailable(
	p placement.Placement,
) (placement.Placement, bool, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, false, err
	}

	return a.shardedAlgo.MarkAllShardsAvailable(p)
}

// exactlyInstancesInitializing returns true when
// 1: the given list of instances matches all the initializing instances in the placement.
// 2: the shards are not cutover yet.
func exactlyInstancesInitializing(p placement.Placement, instances []string, nowNanos int64) bool {
	return instancesInitializing(p, instances, nowNanos, true)
}

// atLeastInstancesInitializing returns true when
// 1: the given list of instances matches all the initializing instances in the placement.
// 2: the shards are not cutover yet.
func atLeastInstancesInitializing(p placement.Placement, instances []string, nowNanos int64) bool {
	return instancesInitializing(p, instances, nowNanos, false)
}

func instancesInitializing(p placement.Placement, instances []string, nowNanos int64, exactly bool) bool {
	ids := make(map[string]struct{}, len(instances))
	for _, i := range instances {
		ids[i] = struct{}{}
	}

	return allInstancesInState(ids, p, func(s shard.Shard) bool {
		return s.State() == shard.Initializing && s.CutoverNanos() > nowNanos
	}, exactly)
}

// exactlyInstancesLeaving returns true when
// 1: the given list of instances matches all the leaving instances in the placement.
// 2: the shards are not cutoff yet.
func exactlyInstancesLeaving(p placement.Placement, instances []placement.Instance, nowNanos int64) bool {
	return instancesLeaving(p, instances, nowNanos, true)
}

func atLeastInstancesLeaving(p placement.Placement, instances []placement.Instance, nowNanos int64) bool {
	return instancesLeaving(p, instances, nowNanos, false)
}


func instancesLeaving(p placement.Placement, instances []placement.Instance, nowNanos int64, exactly bool) bool {
	ids := make(map[string]struct{}, len(instances))
	for _, i := range instances {
		ids[i.ID()] = struct{}{}
	}

	return allInstancesInState(ids, p, func(s shard.Shard) bool {
		return s.State() == shard.Leaving && s.CutoffNanos() > nowNanos
	}, exactly)
}

func instanceCheck(instance placement.Instance, shardCheckFn func(s shard.Shard) bool) bool {
	for _, s := range instance.Shards().All() {
		if !shardCheckFn(s) {
			return false
		}
	}
	return true
}

func allInstancesInState(
	instanceIDs map[string]struct{},
	p placement.Placement,
	forEachShardFn func(s shard.Shard) bool,
	exactly bool,
) bool {
	// Each instance ID in instanceIDs must pass instanceCheck
	// Each instance ID *not* in instanceIDs must fail instance check.
	for _, instance := range p.Instances() {
		_, isExpectedToBeInState := instanceIDs[instance.ID()]

		instanceIsInState := instanceCheck(instance, forEachShardFn)

		if !isExpectedToBeInState {
			// if exactly, we want only the expected instances in the state.
			if exactly && instanceIsInState {
				return false
			}
			// the instance is either not in the state (ok), or we don't care if it is.
			continue
		}
		// instance should be in state
		if !instanceIsInState {
			return false
		}

		// mark this instance as found
		delete(instanceIDs, instance.ID())
		continue
	}

	// check that we hit all of the expected instances
	return len(instanceIDs) == 0
}

// returnInitializingShards tries to return initializing shards on the given instances
// and retries until no more initializing shards could be returned.
func (a mirroredAlgorithm) returnInitializingShards(
	p placement.Placement,
	instanceIDs []string,
) (placement.Placement, error) {
	for {
		madeProgess := false
		for _, id := range instanceIDs {
			_, exist := p.Instance(id)
			if !exist {
				continue
			}
			ph, instance, err := newRemoveInstanceHelper(p, id, a.opts)
			if err != nil {
				return nil, err
			}
			numInitShards := instance.Shards().NumShardsForState(shard.Initializing)
			ph.returnInitializingShards(instance)
			if instance.Shards().NumShardsForState(shard.Initializing) < numInitShards {
				// Made some progress on returning shards.
				madeProgess = true
			}
			p = ph.generatePlacement()
			if instance.Shards().NumShards() > 0 {
				p = p.SetInstances(append(p.Instances(), instance))
			}
		}
		if !madeProgess {
			break
		}
	}

	for _, id := range instanceIDs {
		instance, ok := p.Instance(id)
		if !ok {
			continue
		}
		numInitializingShards := instance.Shards().NumShardsForState(shard.Initializing)
		if numInitializingShards != 0 {
			return nil, fmt.Errorf("there are %d initializing shards could not be returned for instance %s", numInitializingShards, id)
		}
	}

	return p, nil
}

// reclaimLeavingShards tries to reclaim leaving shards on the given instances
// and retries until no more leaving shards could be reclaimed.
func (a mirroredAlgorithm) reclaimLeavingShards(
	p placement.Placement,
	addingInstances []placement.Instance,
) (placement.Placement, error) {
	for {
		madeProgess := false
		for _, instance := range addingInstances {
			ph, instance, err := newAddInstanceHelper(p, instance, a.opts, withAvailableOrLeavingShardsOnly)
			if err != nil {
				return nil, err
			}
			numLeavingShards := instance.Shards().NumShardsForState(shard.Leaving)
			ph.reclaimLeavingShards(instance)
			if instance.Shards().NumShardsForState(shard.Leaving) < numLeavingShards {
				// Made some progress on reclaiming shards.
				madeProgess = true
			}
			p = ph.generatePlacement()
		}
		if !madeProgess {
			break
		}
	}

	for _, instance := range addingInstances {
		id := instance.ID()
		instance, ok := p.Instance(id)
		if !ok {
			return nil, fmt.Errorf("could not find instance %s in placement after reclaiming leaving shards", id)
		}
		numLeavingShards := instance.Shards().NumShardsForState(shard.Leaving)
		if numLeavingShards != 0 {
			return nil, fmt.Errorf("there are %d leaving shards could not be reclaimed for instance %s", numLeavingShards, id)
		}
	}

	return p, nil
}

func validAddingInstances(p placement.Placement, addingInstances []placement.Instance) ([]placement.Instance, error) {
	for i, instance := range addingInstances {
		if _, exist := p.Instance(instance.ID()); exist {
			return nil, fmt.Errorf("instance %s already exist in the placement", instance.ID())
		}
		if instance.IsLeaving() {
			// The instance was leaving in placement, after markAllShardsAsAvailable it is now removed
			// from the placement, so we should treat them as fresh new instances.
			addingInstances[i] = instance.SetShards(shard.NewShards(nil))
		}
	}
	return addingInstances, nil
}

func groupInstancesByShardSetID(
	instances []placement.Instance,
	rf int,
) ([]placement.Instance, error) {
	var (
		shardSetMap = make(map[uint32]*shardSetMetadata, len(instances))
		res         = make([]placement.Instance, 0, len(instances))
	)
	for _, instance := range instances {
		var (
			ssID   = instance.ShardSetID()
			weight = instance.Weight()
			group  = instance.IsolationGroup()
			shards = instance.Shards()
		)
		meta, ok := shardSetMap[ssID]
		if !ok {
			meta = &shardSetMetadata{
				weight: weight,
				groups: make(map[string]struct{}, rf),
				shards: shards,
			}
			shardSetMap[ssID] = meta
		}
		if _, ok := meta.groups[group]; ok {
			return nil, fmt.Errorf("found duplicated isolation group %s for shardset id %d", group, ssID)
		}

		if meta.weight != weight {
			return nil, fmt.Errorf("found different weights: %d and %d, for shardset id %d", meta.weight, weight, ssID)
		}

		if !meta.shards.Equals(shards) {
			return nil, fmt.Errorf("found different shards: %v and %v, for shardset id %d", meta.shards, shards, ssID)
		}

		meta.groups[group] = struct{}{}
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
				SetIsolationGroup(ssIDStr).
				SetWeight(meta.weight).
				SetShardSetID(ssID).
				SetShards(meta.shards.Clone()),
		)
	}

	return res, nil
}

// mirrorFromPlacement zips all instances with the same shardSetID into a virtual instance
// and create a placement with those virtual instance and rf=1.
func mirrorFromPlacement(p placement.Placement) (placement.Placement, error) {
	mirrorInstances, err := groupInstancesByShardSetID(p.Instances(), p.ReplicaFactor())
	if err != nil {
		return nil, err
	}

	return placement.NewPlacement().
		SetInstances(mirrorInstances).
		SetReplicaFactor(1).
		SetShards(p.Shards()).
		SetCutoverNanos(p.CutoverNanos()).
		SetIsSharded(true).
		SetIsMirrored(true).
		SetMaxShardSetID(p.MaxShardSetID()), nil
}

// placementFromMirror duplicates the shards for each shard set id and assign
// them to the instance with the shard set id.
func placementFromMirror(
	mirror placement.Placement,
	instances []placement.Instance,
	rf int,
) (placement.Placement, error) {
	var (
		mirrorInstances     = mirror.Instances()
		shardSetMap         = make(map[uint32][]placement.Instance, len(mirrorInstances))
		instancesWithShards = make([]placement.Instance, 0, len(instances))
	)
	for _, instance := range instances {
		instances, ok := shardSetMap[instance.ShardSetID()]
		if !ok {
			instances = make([]placement.Instance, 0, rf)
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
		SetCutoverNanos(mirror.CutoverNanos()).
		SetIsMirrored(true).
		SetIsSharded(true).
		SetMaxShardSetID(mirror.MaxShardSetID()), nil
}

func instancesFromMirror(
	mirrorInstance placement.Instance,
	instancesMap map[uint32][]placement.Instance,
) ([]placement.Instance, error) {
	ssID := mirrorInstance.ShardSetID()
	instances, ok := instancesMap[ssID]
	if !ok {
		return nil, fmt.Errorf("could not find shard set id %d in placement", ssID)
	}

	shards := mirrorInstance.Shards()
	for i, instance := range instances {
		newShards := make([]shard.Shard, shards.NumShards())
		for j, s := range shards.All() {
			// TODO move clone() to shard interface
			newShard := shard.NewShard(s.ID()).SetState(s.State()).SetCutoffNanos(s.CutoffNanos()).SetCutoverNanos(s.CutoverNanos())
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
	groups map[string]struct{}
	shards shard.Shards
}
