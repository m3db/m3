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

	"github.com/m3db/m3cluster/placement"
)

// shardAwareDeploymentPlanner plans the deployment so that as many hosts can be deployed
// at the same time without making more than 1 replica of any shard unavailable
type shardAwareDeploymentPlanner struct {
}

type sortableSteps [][]placement.HostShards

func (s sortableSteps) Len() int {
	return len(s)
}

func (s sortableSteps) Less(i, j int) bool {
	return len(s[i]) > len(s[j])
}

func (s sortableSteps) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// NewShardAwareDeploymentPlanner returns a deployment planner
func NewShardAwareDeploymentPlanner() placement.DeploymentPlanner {
	return shardAwareDeploymentPlanner{}
}

func (dp shardAwareDeploymentPlanner) DeploymentSteps(ps placement.Snapshot) [][]placement.HostShards {
	//ph := newReplicaPlacementHelper(ps, ps.Replicas())
	hss := ps.HostShards()
	var steps sortableSteps
	for len(hss) > 0 {
		step := getDeployStep(hss)
		steps = append(steps, step)
		hss = getLeftHostShards(hss, step)
	}
	sort.Sort(steps)
	return steps
}

func getDeployStep(hss []placement.HostShards) []placement.HostShards {
	var parallel []placement.HostShards
	shards := make(map[uint32]struct{})
	for _, hs := range hss {
		if isSharingShard(shards, hs) {
			continue
		}
		parallel = append(parallel, hs)
		for _, shard := range hs.Shards() {
			shards[shard] = struct{}{}
		}
	}
	return parallel
}

func isSharingShard(shards map[uint32]struct{}, hs placement.HostShards) bool {
	for _, shard := range hs.Shards() {
		if _, exist := shards[shard]; exist {
			return true
		}
	}
	return false
}

func getLeftHostShards(all, toBeRemoved []placement.HostShards) []placement.HostShards {
	for _, hs := range toBeRemoved {
		all = removeHostShards(all, hs)
	}
	return all
}

func removeHostShards(all []placement.HostShards, remove placement.HostShards) []placement.HostShards {
	for i, hs := range all {
		if hs.Host().ID() == remove.Host().ID() {
			return append(all[:i], all[i+1:]...)
		}
	}
	return all
}
