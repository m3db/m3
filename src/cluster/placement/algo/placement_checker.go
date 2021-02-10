// Copyright (c) 2021 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
)

type (
	placementChecker struct {
		instanceEvaluator instanceEvaluatorFunc
	}
	shardCheckFunc        func(shard.Shard) bool
	instanceEvaluatorFunc func(map[string]struct{}, placement.Placement, shardCheckFunc) bool
)

var (
	// globalChecker verifies that the specified list of instances are the only instances
	// in the given state in the placement.
	globalChecker = &placementChecker{
		instanceEvaluator: onlyAmongAllInstancesInState,
	}

	// localChecker verifies that the specified list of instances are in the given state,
	// and ignores the state of the rest of instances in the placement.
	localChecker = &placementChecker{
		instanceEvaluator: instancesInState,
	}
)

func (pc *placementChecker) allInitializing(p placement.Placement, instances []string, nowNanos int64) bool {
	ids := make(map[string]struct{}, len(instances))
	for _, i := range instances {
		ids[i] = struct{}{}
	}

	shardCheckFn := func(s shard.Shard) bool {
		return s.State() == shard.Initializing && s.CutoverNanos() > nowNanos
	}

	return pc.instanceEvaluator(ids, p, shardCheckFn)
}

func (pc *placementChecker) allLeaving(p placement.Placement, instances []placement.Instance, nowNanos int64) bool {
	ids := make(map[string]struct{}, len(instances))
	for _, i := range instances {
		ids[i.ID()] = struct{}{}
	}

	shardCheckFn := func(s shard.Shard) bool {
		return s.State() == shard.Leaving && s.CutoffNanos() > nowNanos
	}

	return pc.instanceEvaluator(ids, p, shardCheckFn)
}

// allLeavingByIDs is a wrapper of allLeaving that uses string IDs
func (pc *placementChecker) allLeavingByIDs(p placement.Placement, instanceIDs []string, nowNanos int64) bool {
	instances := make([]placement.Instance, len(instanceIDs))
	for i, id := range instanceIDs {
		curr, exists := p.Instance(id)
		if !exists {
			return false
		}

		instances[i] = curr
	}

	return pc.allLeaving(p, instances, nowNanos)
}

func (pc *placementChecker) allAvailable(p placement.Placement, instances []string, nowNanos int64) bool {
	ids := make(map[string]struct{}, len(instances))
	for _, i := range instances {
		ids[i] = struct{}{}
	}

	shardCheckFn := func(s shard.Shard) bool {
		return s.State() == shard.Available && s.CutoffNanos() > nowNanos
	}

	return pc.instanceEvaluator(ids, p, shardCheckFn)
}

// onlyAmongAllInstancesInState returns true when only the specified instances
// have all their shards in the expected state and the rest of instances
// in the placement do not.
func onlyAmongAllInstancesInState(
	instanceIDs map[string]struct{},
	p placement.Placement,
	forEachShardFn shardCheckFunc,
) bool {
	for _, instance := range p.Instances() {
		if !instanceCheck(instance, forEachShardFn) {
			continue
		}
		if _, ok := instanceIDs[instance.ID()]; !ok {
			return false
		}
		delete(instanceIDs, instance.ID())
	}

	return len(instanceIDs) == 0
}

// instancesInState returns true when the specified instances
// have all their shards in the expected state, and ignores the
// rest of instances in the placement.
func instancesInState(
	instanceIDs map[string]struct{},
	p placement.Placement,
	forEachShardFn shardCheckFunc,
) bool {
	if len(instanceIDs) == 0 {
		return false
	}

	for id := range instanceIDs {
		instance, exist := p.Instance(id)
		if !exist {
			return false
		}

		if !instanceCheck(instance, forEachShardFn) {
			return false
		}
	}

	return true
}

func instanceCheck(instance placement.Instance, shardCheckFn func(s shard.Shard) bool) bool {
	for _, s := range instance.Shards().All() {
		if !shardCheckFn(s) {
			return false
		}
	}
	return true
}
