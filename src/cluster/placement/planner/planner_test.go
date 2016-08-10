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
	"testing"

	"github.com/m3db/m3cluster/placement"
	"github.com/stretchr/testify/assert"
)

func TestDeployment(t *testing.T) {
	h1 := placement.NewEmptyHostShards("r1h1", "r1")
	h1.AddShard(1)
	h1.AddShard(2)
	h1.AddShard(3)

	h2 := placement.NewEmptyHostShards("r2h2", "r2")
	h2.AddShard(4)
	h2.AddShard(5)
	h2.AddShard(6)

	h3 := placement.NewEmptyHostShards("r3h3", "r3")
	h3.AddShard(1)
	h3.AddShard(3)
	h3.AddShard(5)

	h4 := placement.NewEmptyHostShards("r4h4", "r4")
	h4.AddShard(2)
	h4.AddShard(4)
	h4.AddShard(6)

	h5 := placement.NewEmptyHostShards("r5h5", "r5")
	h5.AddShard(5)
	h5.AddShard(6)
	h5.AddShard(1)

	h6 := placement.NewEmptyHostShards("r6h6", "r6")
	h6.AddShard(2)
	h6.AddShard(3)
	h6.AddShard(4)

	hss := []placement.HostShards{h1, h2, h3, h4, h5, h6}

	mp := placement.NewPlacementSnapshot(hss, []uint32{1, 2, 3, 4, 5, 6}, 3)

	dp := NewShardAwareDeploymentPlanner()
	steps := dp.DeploymentSteps(mp)
	total := 0
	for _, step := range steps {
		total += len(step)
	}
	assert.Equal(t, total, 6)
	assert.True(t, len(steps) == 3)
}

func TestDeploymentWithThreeReplica(t *testing.T) {
	h1 := placement.NewEmptyHostShards("r1h1", "r1")
	h1.AddShard(1)
	h1.AddShard(2)

	h2 := placement.NewEmptyHostShards("r2h2", "r2")
	h2.AddShard(3)
	h2.AddShard(4)

	h3 := placement.NewEmptyHostShards("r3h3", "r3")
	h3.AddShard(5)
	h3.AddShard(6)

	h4 := placement.NewEmptyHostShards("r4h4", "r4")
	h4.AddShard(1)
	h4.AddShard(3)

	h5 := placement.NewEmptyHostShards("r5h5", "r5")
	h5.AddShard(4)
	h5.AddShard(6)

	h6 := placement.NewEmptyHostShards("r6h6", "r6")
	h6.AddShard(2)
	h6.AddShard(5)

	h7 := placement.NewEmptyHostShards("r7h7", "r7")
	h7.AddShard(2)
	h7.AddShard(3)

	h8 := placement.NewEmptyHostShards("r8h8", "r8")
	h8.AddShard(4)
	h8.AddShard(5)

	h9 := placement.NewEmptyHostShards("r9h9", "r9")
	h9.AddShard(6)
	h9.AddShard(1)

	hss := []placement.HostShards{h1, h2, h3, h4, h5, h6, h7, h8, h9}

	mp := placement.NewPlacementSnapshot(hss, []uint32{1, 2, 3, 4, 5, 6}, 3)

	dp := NewShardAwareDeploymentPlanner()
	steps := dp.DeploymentSteps(mp)
	total := 0
	for _, step := range steps {
		total += len(step)
	}
	assert.Equal(t, total, 9)
	assert.True(t, len(steps) == 3)
}

func TestRemoveHostShards(t *testing.T) {
	h1 := placement.NewEmptyHostShards("r1h1", "r1")
	h2 := placement.NewEmptyHostShards("r2h2", "r2")
	h3 := placement.NewEmptyHostShards("r3h3", "r3")
	h4 := placement.NewEmptyHostShards("r4h4", "r4")

	hss := []placement.HostShards{h1, h2, h3, h4}

	left := removeHostShards(hss, h4)
	assert.Equal(t, 3, len(left))

	left = removeHostShards(hss, placement.NewEmptyHostShards("r5h5", "r5"))
	assert.Equal(t, 4, len(left))
}
