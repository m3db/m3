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

package algo

import (
	"fmt"
	"testing"

	"github.com/m3db/m3cluster/placement"
	"github.com/stretchr/testify/assert"
)

func TestGoodCase(t *testing.T) {
	h1 := placement.NewHost("r1h1", "r1", "z1")
	h2 := placement.NewHost("r1h2", "r1", "z1")
	h3 := placement.NewHost("r2h3", "r2", "z1")
	h4 := placement.NewHost("r2h4", "r2", "z1")
	h5 := placement.NewHost("r3h5", "r3", "z1")
	h6 := placement.NewHost("r4h6", "r4", "z1")
	h7 := placement.NewHost("r5h7", "r5", "z1")
	h8 := placement.NewHost("r6h8", "r6", "z1")
	h9 := placement.NewHost("r7h9", "r7", "z1")

	hosts := []placement.Host{h1, h2, h3, h4, h5, h6, h7, h8, h9}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(false))
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 replica 1")

	p, err = a.AddHost(p, placement.NewHost("r6h21", "r6", "z1"))
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 add 1")

	p, err = a.RemoveHost(p, h1)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 remove 1")

	h12 := placement.NewHost("r3h12", "r3", "z1")
	p, err = a.ReplaceHost(p, h5, h12)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 add 1")

	p, err = a.RemoveHost(p, h2)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 remove 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 replica 2")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 replica 3")

	h10 := placement.NewHost("r4h10", "r4", "z1")
	p, err = a.AddHost(p, h10)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 add 1")

	h11 := placement.NewHost("r7h11", "r7", "z1")
	p, err = a.AddHost(p, h11)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 add 2")

	h13 := placement.NewHost("r5h13", "r5", "z1")
	p, err = a.ReplaceHost(p, h3, h13)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 replace 1")

	p, err = a.RemoveHost(p, h4)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.02, "good case1 remove 2")
}

func TestOverSizedRack(t *testing.T) {
	r1h1 := placement.NewHost("r1h1", "r1", "z1")
	r1h6 := placement.NewHost("r1h6", "r1", "z1")
	r1h7 := placement.NewHost("r1h7", "r1", "z1")

	r2h2 := placement.NewHost("r2h2", "r2", "z1")
	r2h3 := placement.NewHost("r2h3", "r2", "z1")

	r3h4 := placement.NewHost("r3h4", "r3", "z1")
	r3h8 := placement.NewHost("r3h8", "r3", "z1")

	r4h5 := placement.NewHost("r4h5", "r4", "z1")

	r5h9 := placement.NewHost("r5h9", "r5", "z1")

	hosts := []placement.Host{r1h1, r2h2, r2h3, r3h4, r4h5, r1h6, r1h7, r3h8, r5h9}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(false))
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOverSizedRack replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOverSizedRack replica 2")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOverSizedRack replica 3")

	r4h10 := placement.NewHost("r4h10", "r4", "z1")
	p, err = a.ReplaceHost(p, r3h8, r4h10)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOverSizedRack replace 1")

	// At this point, r1 has 4 hosts to share a copy of 1024 partitions
	r1h11 := placement.NewHost("r1h11", "r1", "z1")
	p, err = a.ReplaceHost(p, r2h2, r1h11)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.22, "TestOverSizedRack replace 2")

	// adding a new host to relieve the load on the hot hosts
	r4h12 := placement.NewHost("r4h12", "r4", "z1")
	p, err = a.AddHost(p, r4h12)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.15, "TestOverSizedRack add 1")
}

func TestInitPlacementOn0Host(t *testing.T) {
	hosts := []placement.Host{}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(false))
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.Error(t, err)
	assert.Nil(t, p)
}

func TestOneRack(t *testing.T) {
	r1h1 := placement.NewHost("r1h1", "r1", "z1")
	r1h2 := placement.NewHost("r1h2", "r1", "z1")

	hosts := []placement.Host{r1h1, r1h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(false))
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOneRack replica 1")

	r1h6 := placement.NewHost("r1h6", "r1", "z1")

	p, err = a.AddHost(p, r1h6)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOneRack addhost 1")
}

func TestRFGreaterThanRackLen(t *testing.T) {
	r1h1 := placement.NewHost("r1h1", "r1", "z1")
	r1h6 := placement.NewHost("r1h6", "r1", "z1")

	r2h2 := placement.NewHost("r2h2", "r2", "z1")
	r2h3 := placement.NewHost("r2h3", "r2", "z1")

	hosts := []placement.Host{r1h1, r2h2, r2h3, r1h6}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(false))
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLen replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLen replica 2")

	p1, err := a.AddReplica(p)
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, p.Validate())
}

func TestRFGreaterThanRackLenAfterHostRemoval(t *testing.T) {
	r1h1 := placement.NewHost("r1h1", "r1", "z1")

	r2h2 := placement.NewHost("r2h2", "r2", "z1")

	hosts := []placement.Host{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(false))
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLenAfterHostRemoval replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLenAfterHostRemoval replica 2")

	p1, err := a.RemoveHost(p, r2h2)
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, p.Validate())
}

func TestRFGreaterThanRackLenAfterHostReplace(t *testing.T) {
	r1h1 := placement.NewHost("r1h1", "r1", "z1")

	r2h2 := placement.NewHost("r2h2", "r2", "z1")

	hosts := []placement.Host{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(false))
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLenAfterHostRemoval replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLenAfterHostRemoval replica 2")

	r1h3 := placement.NewHost("r1h3", "r1", "z1")
	p1, err := a.ReplaceHost(p, r2h2, r1h3)
	assert.Equal(t, errNotEnoughRacks, err)
	assert.Nil(t, p1)
	assert.NoError(t, p.Validate())
}

func TestLooseRackCheckAlgorithm(t *testing.T) {
	r1h1 := placement.NewHost("r1h1", "r1", "z1")

	r2h2 := placement.NewHost("r2h2", "r2", "z1")

	hosts := []placement.Host{r1h1, r2h2}

	ids := make([]uint32, 10)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(false))
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	assert.NoError(t, p.Validate())

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	assert.NoError(t, p.Validate())

	p1, err := a.AddReplica(p)
	assert.Equal(t, errNotEnoughRacks, err)
	assert.Nil(t, p1)
	assert.NoError(t, p.Validate())

	r2h4 := placement.NewHost("r2h4", "r2", "z1")
	p, err = a.AddHost(p, r2h4)
	assert.NoError(t, err)
	assert.NoError(t, p.Validate())

	p1, err = a.AddReplica(p)
	assert.Equal(t, errNotEnoughRacks, err)
	assert.Nil(t, p1)
	assert.NoError(t, p.Validate())

	b := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(true))
	// different with normal algo, which would return error here
	r1h3 := placement.NewHost("r1h3", "r1", "z1")
	p, err = b.ReplaceHost(p, r2h2, r1h3)
	assert.NoError(t, err)
	assert.NoError(t, p.Validate())

	p1, err = b.ReplaceHost(p, r2h4, r1h3)
	assert.Equal(t, errAddingHostAlreadyExist, err)
	assert.Nil(t, p1)
	assert.NoError(t, p.Validate())

	p, err = b.RemoveHost(p, r1h3)
	assert.NoError(t, err)
	assert.NoError(t, p.Validate())

	r3h5 := placement.NewHost("r3h5", "r3", "z1")
	p, err = b.AddHost(p, r3h5)
	assert.NoError(t, err)
	assert.NoError(t, p.Validate())

	p, err = b.AddReplica(p)
	assert.NoError(t, err)
	assert.NoError(t, p.Validate())
}

func TestAddHostCouldNotReachTargetLoad(t *testing.T) {
	r1h1 := placement.NewHost("r1h1", "r1", "z1")

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	p := placement.NewPlacementSnapshot([]placement.HostShards{}, ids, 1)

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(false))

	p1, err := a.AddHost(p, r1h1)
	// errCouldNotReachTargetLoad should only happen when trying to add a host to
	// an invalid snapshot that does not have enough shards for the rf it thought it has
	assert.Equal(t, errCouldNotReachTargetLoad, err)
	assert.Nil(t, p1)
}

func TestAddExistHost(t *testing.T) {
	r1h1 := placement.NewHost("r1h1", "r1", "z1")

	r2h2 := placement.NewHost("r2h2", "r2", "z1")

	hosts := []placement.Host{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(false))
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestAddExistHost replica 1")

	p1, err := a.AddHost(p, r2h2)
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, p.Validate())
}

func TestRemoveAbsentHost(t *testing.T) {
	r1h1 := placement.NewHost("r1h1", "r1", "z1")

	r2h2 := placement.NewHost("r2h2", "r2", "z1")

	hosts := []placement.Host{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(false))
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRemoveAbsentHost replica 1")

	r3h3 := placement.NewHost("r3h3", "r3", "z1")

	p1, err := a.RemoveHost(p, r3h3)
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, p.Validate())
}

func TestReplaceAbsentHost(t *testing.T) {
	r1h1 := placement.NewHost("r1h1", "r1", "z1")

	r2h2 := placement.NewHost("r2h2", "r2", "z1")

	hosts := []placement.Host{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(false))
	p, err := a.BuildInitialPlacement(hosts, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestReplaceAbsentHost replica 1")

	r3h3 := placement.NewHost("r3h3", "r3", "z1")
	r4h4 := placement.NewHost("r4h4", "r4", "z1")

	p1, err := a.ReplaceHost(p, r3h3, r4h4)
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, p.Validate())
}

func TestCanAssignHost(t *testing.T) {
	h1 := placement.NewEmptyHostShards("r1h1", "r1", "z1")
	h1.AddShard(1)
	h1.AddShard(2)
	h1.AddShard(3)

	h2 := placement.NewEmptyHostShards("r1h2", "r1", "z1")
	h2.AddShard(4)
	h2.AddShard(5)
	h2.AddShard(6)

	h3 := placement.NewEmptyHostShards("r2h3", "r2", "z1")
	h3.AddShard(1)
	h3.AddShard(3)
	h3.AddShard(5)

	h4 := placement.NewEmptyHostShards("r2h4", "r2", "z1")
	h4.AddShard(2)
	h4.AddShard(4)
	h4.AddShard(6)

	h5 := placement.NewEmptyHostShards("r3h5", "r3", "z1")
	h5.AddShard(5)
	h5.AddShard(6)
	h5.AddShard(1)

	h6 := placement.NewEmptyHostShards("r4h6", "r4", "z1")
	h6.AddShard(2)
	h6.AddShard(3)
	h6.AddShard(4)

	hss := []placement.HostShards{h1, h2, h3, h4, h5, h6}

	mp := placement.NewPlacementSnapshot(hss, []uint32{1, 2, 3, 4, 5, 6}, 3)

	ph := newPlacementHelperWithTargetRF(placement.NewOptions(), mp, 3).(*placementHelper)
	assert.True(t, ph.canAssignHost(2, h6, h5))
	assert.True(t, ph.canAssignHost(1, h1, h6))
	assert.False(t, ph.canAssignHost(2, h6, h1))
	// rack check
	assert.False(t, ph.canAssignHost(2, h6, h3))
	ph = newPlacementHelperWithTargetRF(placement.NewOptions().SetLooseRackCheck(true), mp, 3).(*placementHelper)
	assert.True(t, ph.canAssignHost(2, h6, h3))
}

func validateDistribution(t *testing.T, mp placement.Snapshot, expectPeakOverAvg float64, testCase string) {
	sh := NewPlacementHelper(placement.NewOptions(), mp)
	total := 0
	for _, hostShard := range mp.HostShards() {
		hostLoad := hostShard.ShardsLen()
		total += hostLoad
		hostOverAvg := float64(hostLoad) / float64(getAvgLoad(mp))
		assert.True(t, hostOverAvg <= expectPeakOverAvg, fmt.Sprintf("Bad distribution in %s, peak/Avg on %s is too high: %v, expecting %v, load on host: %v, avg load: %v",
			testCase, hostShard.Host().ID(), hostOverAvg, expectPeakOverAvg, hostLoad, getAvgLoad(mp)))

		target := sh.GetTargetLoadForHost(hostShard.Host().ID())
		hostOverTarget := float64(hostLoad) / float64(target)
		assert.True(t, hostOverTarget <= 1.03, fmt.Sprintf("Bad distribution in %s, peak/Target is too high. %s: %v, load on host: %v, target load: %v",
			testCase, hostShard.Host().ID(), hostOverTarget, hostLoad, target))
	}
	assert.Equal(t, total, mp.Replicas()*mp.ShardsLen(), fmt.Sprintf("Wrong total partition: expecting %v, but got %v", mp.Replicas()*mp.ShardsLen(), total))
	assert.NoError(t, mp.Validate(), "snapshot validation failed")
}

func getAvgLoad(ps placement.Snapshot) int {
	return ps.Replicas() * ps.ShardsLen() / ps.HostsLen()
}
