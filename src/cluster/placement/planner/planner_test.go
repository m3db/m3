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
	"testing"

	"github.com/m3db/m3cluster/placement"
	"github.com/stretchr/testify/assert"
)

func TestDeployment(t *testing.T) {
	h1 := placement.NewHostShards(placement.NewHost("r1h1", "r1", "z1", 1))
	h1.AddShard(1)
	h1.AddShard(2)
	h1.AddShard(3)

	h2 := placement.NewHostShards(placement.NewHost("r2h2", "r2", "z1", 1))
	h2.AddShard(4)
	h2.AddShard(5)
	h2.AddShard(6)

	h3 := placement.NewHostShards(placement.NewHost("r3h3", "r3", "z1", 1))
	h3.AddShard(1)
	h3.AddShard(3)
	h3.AddShard(5)

	h4 := placement.NewHostShards(placement.NewHost("r4h4", "r4", "z1", 1))
	h4.AddShard(2)
	h4.AddShard(4)
	h4.AddShard(6)

	h5 := placement.NewHostShards(placement.NewHost("r5h5", "r5", "z1", 1))
	h5.AddShard(5)
	h5.AddShard(6)
	h5.AddShard(1)

	h6 := placement.NewHostShards(placement.NewHost("r6h6", "r6", "z1", 1))
	h6.AddShard(2)
	h6.AddShard(3)
	h6.AddShard(4)

	hss := []placement.HostShards{h1, h2, h3, h4, h5, h6}

	mp := placement.NewPlacementSnapshot(hss, []uint32{1, 2, 3, 4, 5, 6}, 3)

	dp := NewShardAwareDeploymentPlanner(placement.NewDeploymentOptions())
	steps := dp.DeploymentSteps(mp)
	total := 0
	for _, step := range steps {
		total += len(step)
	}
	assert.Equal(t, total, 6)
	assert.True(t, len(steps) == 3)
}

func TestDeploymentWithThreeReplica(t *testing.T) {
	h1 := placement.NewHostShards(placement.NewHost("r1h1", "r1", "z1", 1))
	h1.AddShard(1)
	h1.AddShard(2)

	h2 := placement.NewHostShards(placement.NewHost("r2h2", "r2", "z1", 1))
	h2.AddShard(3)
	h2.AddShard(4)

	h3 := placement.NewHostShards(placement.NewHost("r3h3", "r3", "z1", 1))
	h3.AddShard(5)
	h3.AddShard(6)

	h4 := placement.NewHostShards(placement.NewHost("r4h4", "r4", "z1", 1))
	h4.AddShard(1)
	h4.AddShard(3)

	h5 := placement.NewHostShards(placement.NewHost("r5h5", "r5", "z1", 1))
	h5.AddShard(4)
	h5.AddShard(6)

	h6 := placement.NewHostShards(placement.NewHost("r6h6", "r6", "z1", 1))
	h6.AddShard(2)
	h6.AddShard(5)

	h7 := placement.NewHostShards(placement.NewHost("r7h7", "r7", "z1", 1))
	h7.AddShard(2)
	h7.AddShard(3)

	h8 := placement.NewHostShards(placement.NewHost("r8h8", "r8", "z1", 1))
	h8.AddShard(4)
	h8.AddShard(5)

	h9 := placement.NewHostShards(placement.NewHost("r9h9", "r9", "z1", 1))
	h9.AddShard(6)
	h9.AddShard(1)

	hss := []placement.HostShards{h1, h2, h3, h4, h5, h6, h7, h8, h9}

	mp := placement.NewPlacementSnapshot(hss, []uint32{1, 2, 3, 4, 5, 6}, 3)

	dp := NewShardAwareDeploymentPlanner(placement.NewDeploymentOptions())
	steps := dp.DeploymentSteps(mp)
	total := 0
	for _, step := range steps {
		total += len(step)
	}
	assert.Equal(t, total, 9)
	assert.True(t, len(steps) == 3)

	dp = NewShardAwareDeploymentPlanner(placement.NewDeploymentOptions().SetMaxStepSize(2))
	steps = dp.DeploymentSteps(mp)
	total = 0
	for _, step := range steps {
		total += len(step)
	}
	assert.Equal(t, total, 9)
	assert.True(t, len(steps) == 5)

	dp = NewShardAwareDeploymentPlanner(placement.NewDeploymentOptions().SetMaxStepSize(1))
	steps = dp.DeploymentSteps(mp)
	total = 0
	for _, step := range steps {
		total += len(step)
	}
	assert.Equal(t, total, 9)
	assert.True(t, len(steps) == 9)
}

func TestRemoveHostShards(t *testing.T) {
	h1 := placement.NewHostShards(placement.NewHost("r1h1", "r1", "z1", 1))
	h2 := placement.NewHostShards(placement.NewHost("r2h2", "r2", "z1", 1))
	h3 := placement.NewHostShards(placement.NewHost("r3h3", "r3", "z1", 1))
	h4 := placement.NewHostShards(placement.NewHost("r4h4", "r4", "z1", 1))

	hss := []placement.HostShards{h1, h2, h3, h4}

	left := removeHostShards(hss, h4)
	assert.Equal(t, 3, len(left))

	left = removeHostShards(hss, placement.NewHostShards(placement.NewHost("r5h5", "r5", "z1", 1)))
	assert.Equal(t, 4, len(left))
}

func TestSort(t *testing.T) {
	var steps sortableSteps
	steps = append(steps, []placement.HostShards{
		placement.NewHostShards(placement.NewHost("", "", "", 1)),
		placement.NewHostShards(placement.NewHost("", "", "", 1))})
	steps = append(steps, []placement.HostShards{
		placement.NewHostShards(placement.NewHost("", "", "", 1)),
		placement.NewHostShards(placement.NewHost("", "", "", 1)),
		placement.NewHostShards(placement.NewHost("", "", "", 1))})
	steps = append(steps, []placement.HostShards{
		placement.NewHostShards(placement.NewHost("", "", "", 1))})
	sort.Sort(steps)

	assert.Equal(t, 3, len(steps))
	assert.Equal(t, 3, len(steps[0]))
	assert.Equal(t, 2, len(steps[1]))
	assert.Equal(t, 1, len(steps[2]))
}
