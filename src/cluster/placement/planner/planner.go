// Copyright (c) 2016 Uber Technologies, Inc.
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

package planner

import (
	"sort"

	"github.com/m3db/m3/src/cluster/placement"
)

// shardAwareDeploymentPlanner plans the deployment so that as many instances can be deployed
// at the same time without making more than 1 replica of any shard unavailable
type shardAwareDeploymentPlanner struct {
	options placement.DeploymentOptions
}

// NewShardAwareDeploymentPlanner returns a deployment planner
func NewShardAwareDeploymentPlanner(options placement.DeploymentOptions) placement.DeploymentPlanner {
	return shardAwareDeploymentPlanner{options: options}
}

func (dp shardAwareDeploymentPlanner) DeploymentSteps(p placement.Placement) [][]placement.Instance {
	instances := p.Instances()
	sort.Sort(placement.ByIDAscending(instances))
	var steps sortableSteps
	for len(instances) > 0 {
		step := getDeployStep(instances, dp.options.MaxStepSize())
		steps = append(steps, step)
		instances = getLeftInstances(instances, step)
	}
	sort.Sort(steps)
	return steps
}

func getDeployStep(instances []placement.Instance, maxStepSize int) []placement.Instance {
	var parallel []placement.Instance
	shards := make(map[uint32]struct{})
	for _, instance := range instances {
		if len(parallel) >= maxStepSize {
			break
		}
		if isSharingShard(shards, instance) {
			continue
		}
		parallel = append(parallel, instance)
		for _, s := range instance.Shards().All() {
			shards[s.ID()] = struct{}{}
		}
	}
	return parallel
}

func isSharingShard(shards map[uint32]struct{}, instance placement.Instance) bool {
	for _, s := range instance.Shards().All() {
		if _, exist := shards[s.ID()]; exist {
			return true
		}
	}
	return false
}

func getLeftInstances(all, toBeRemoved []placement.Instance) []placement.Instance {
	for _, instance := range toBeRemoved {
		all = removeInstance(all, instance)
	}
	return all
}

func removeInstance(all []placement.Instance, remove placement.Instance) []placement.Instance {
	for i, instance := range all {
		if instance.ID() == remove.ID() {
			return append(all[:i], all[i+1:]...)
		}
	}
	return all
}

type sortableSteps [][]placement.Instance

func (s sortableSteps) Len() int {
	return len(s)
}

func (s sortableSteps) Less(i, j int) bool {
	return len(s[i]) > len(s[j])
}

func (s sortableSteps) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
