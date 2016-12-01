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
	"math"
	"testing"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/stretchr/testify/assert"
)

func TestGoodCase(t *testing.T) {
	h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)
	h2 := placement.NewEmptyInstance("r1h2", "r1", "z1", 1)
	h3 := placement.NewEmptyInstance("r2h3", "r2", "z1", 1)
	h4 := placement.NewEmptyInstance("r2h4", "r2", "z1", 1)
	h5 := placement.NewEmptyInstance("r3h5", "r3", "z1", 1)
	h6 := placement.NewEmptyInstance("r4h6", "r4", "z1", 1)
	h7 := placement.NewEmptyInstance("r5h7", "r5", "z1", 1)
	h8 := placement.NewEmptyInstance("r6h8", "r6", "z1", 1)
	h9 := placement.NewEmptyInstance("r7h9", "r7", "z1", 1)

	instances := []services.PlacementInstance{h1, h2, h3, h4, h5, h6, h7, h8, h9}

	numShards := 1024
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetAllowPartialReplace(true))
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "good case1 replica 1")

	p, err = a.AddInstance(p, placement.NewEmptyInstance("r6h21", "r6", "z1", 1))
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "good case1 add 1")

	p, err = a.RemoveInstance(p, h1.ID())
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	assert.Nil(t, p.Instance(h1.ID()))
	validateDistribution(t, p, 1.01, "good case1 remove 1")

	h12 := placement.NewEmptyInstance("r3h12", "r3", "z1", 1)
	p, err = a.ReplaceInstance(p, h5.ID(), []services.PlacementInstance{h12})
	assert.NoError(t, err)
	assert.NotNil(t, p.Instance(h5.ID()))
	p = markAllShardsAsAvailable(t, p)
	assert.Nil(t, p.Instance(h5.ID()))
	validateDistribution(t, p, 1.01, "good case1 add 1")

	p, err = a.RemoveInstance(p, h2.ID())
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "good case1 remove 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "good case1 replica 2")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "good case1 replica 3")

	h10 := placement.NewEmptyInstance("r4h10", "r4", "z1", 1)
	p, err = a.AddInstance(p, h10)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "good case1 add 1")

	h11 := placement.NewEmptyInstance("r7h11", "r7", "z1", 1)
	p, err = a.AddInstance(p, h11)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "good case1 add 2")

	h13 := placement.NewEmptyInstance("r5h13", "r5", "z1", 1)
	p, err = a.ReplaceInstance(p, h3.ID(), []services.PlacementInstance{h13})
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "good case1 replace 1")

	p, err = a.RemoveInstance(p, h4.ID())
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.02, "good case1 remove 2")
}

func TestGoodCaseWithWeight(t *testing.T) {
	h1 := placement.NewEmptyInstance("h1", "r1", "z1", 10)
	h2 := placement.NewEmptyInstance("h2", "r1", "z1", 10)
	h3 := placement.NewEmptyInstance("h3", "r2", "z1", 20)
	h4 := placement.NewEmptyInstance("h4", "r3", "z1", 10)
	h5 := placement.NewEmptyInstance("h5", "r4", "z1", 30)
	h6 := placement.NewEmptyInstance("h6", "r6", "z1", 40)
	h7 := placement.NewEmptyInstance("h7", "r7", "z1", 10)
	h8 := placement.NewEmptyInstance("h8", "r8", "z1", 10)
	h9 := placement.NewEmptyInstance("h9", "r9", "z1", 10)

	instances := []services.PlacementInstance{h1, h2, h3, h4, h5, h6, h7, h8, h9}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight replica 1")

	p, err = a.AddInstance(p, placement.NewEmptyInstance("h21", "r2", "z1", 10))
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight add 1")

	p, err = a.RemoveInstance(p, h1.ID())
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight remove 1")

	p, err = a.ReplaceInstance(p, h3.ID(),
		[]services.PlacementInstance{
			placement.NewEmptyInstance("h31", "r1", "z1", 10),
			placement.NewEmptyInstance("h32", "r1", "z1", 10),
		})
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight replace 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight replica 2")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight replica 3")

	h10 := placement.NewEmptyInstance("h10", "r10", "z1", 10)
	p, err = a.AddInstance(p, h10)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight add 1")

	h11 := placement.NewEmptyInstance("h11", "r7", "z1", 10)
	p, err = a.AddInstance(p, h11)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight add 2")

	h13 := placement.NewEmptyInstance("h13", "r5", "z1", 10)

	p, err = a.ReplaceInstance(p, h11.ID(), []services.PlacementInstance{h13})
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight replace 1")
}

func TestPlacementChangeWithoutStateUpdate(t *testing.T) {
	// similar to test case 1 but not marking any shard as Available after placement change
	h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)
	h2 := placement.NewEmptyInstance("r1h2", "r1", "z1", 1)
	h3 := placement.NewEmptyInstance("r2h3", "r2", "z1", 1)
	h4 := placement.NewEmptyInstance("r2h4", "r2", "z1", 1)
	h5 := placement.NewEmptyInstance("r3h5", "r3", "z1", 1)
	h6 := placement.NewEmptyInstance("r4h6", "r4", "z1", 1)
	h7 := placement.NewEmptyInstance("r5h7", "r5", "z1", 1)
	h8 := placement.NewEmptyInstance("r6h8", "r6", "z1", 1)
	h9 := placement.NewEmptyInstance("r7h9", "r7", "z1", 1)

	instances := []services.PlacementInstance{h1, h2, h3, h4, h5, h6, h7, h8, h9}

	numShards := 100
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetAllowPartialReplace(true))
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	assert.Equal(t, 1, p.ReplicaFactor())
	assert.Equal(t, numShards, p.NumShards())
	for _, instance := range p.Instances() {
		for _, s := range instance.Shards().All() {
			assert.Equal(t, shard.Initializing, s.State())
			assert.Equal(t, "", s.SourceID())
		}
	}
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate replica 1")

	p, err = a.AddInstance(p, placement.NewEmptyInstance("r6h21", "r6", "z1", 1))
	assert.NoError(t, err)
	instance := p.Instance("r6h21")
	for _, s := range instance.Shards().All() {
		assert.Equal(t, shard.Initializing, s.State())
		// there should be no source shard because non of the Initializing shards has been marked as available yet
		assert.Equal(t, "", s.SourceID())
	}
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate add 1")

	p, err = a.RemoveInstance(p, h1.ID())
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate remove 1")

	h12 := placement.NewEmptyInstance("r3h12", "r3", "z1", 1)
	p, err = a.ReplaceInstance(p, h5.ID(), []services.PlacementInstance{h12})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate add 1")

	p, err = a.RemoveInstance(p, h2.ID())
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate remove 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate replica 2")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate replica 3")

	h10 := placement.NewEmptyInstance("r4h10", "r4", "z1", 1)
	p, err = a.AddInstance(p, h10)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate add 1")

	h11 := placement.NewEmptyInstance("r7h11", "r7", "z1", 1)
	p, err = a.AddInstance(p, h11)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate add 2")

	h13 := placement.NewEmptyInstance("r5h13", "r5", "z1", 1)
	p, err = a.ReplaceInstance(p, h3.ID(), []services.PlacementInstance{h13})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate replace 1")

	p, err = a.RemoveInstance(p, h4.ID())
	assert.NoError(t, err)
	validateDistribution(t, p, 1.02, "TestPlacementChangeWithoutStateUpdate remove 2")
}

func TestOverSizedRack(t *testing.T) {
	r1h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)
	r1h6 := placement.NewEmptyInstance("r1h6", "r1", "z1", 1)
	r1h7 := placement.NewEmptyInstance("r1h7", "r1", "z1", 1)

	r2h2 := placement.NewEmptyInstance("r2h2", "r2", "z1", 1)
	r2h3 := placement.NewEmptyInstance("r2h3", "r2", "z1", 1)

	r3h4 := placement.NewEmptyInstance("r3h4", "r3", "z1", 1)
	r3h8 := placement.NewEmptyInstance("r3h8", "r3", "z1", 1)

	r4h5 := placement.NewEmptyInstance("r4h5", "r4", "z1", 1)

	r5h9 := placement.NewEmptyInstance("r5h9", "r5", "z1", 1)

	instances := []services.PlacementInstance{r1h1, r2h2, r2h3, r3h4, r4h5, r1h6, r1h7, r3h8, r5h9}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOverSizedRack replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOverSizedRack replica 2")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOverSizedRack replica 3")

	r4h10 := placement.NewEmptyInstance("r4h10", "r4", "z1", 1)
	_, err = a.ReplaceInstance(p, r3h8.ID(), []services.PlacementInstance{r4h10})
	assert.Error(t, err)

	a = NewRackAwarePlacementAlgorithm(placement.NewOptions().SetAllowPartialReplace(true))
	p, err = a.ReplaceInstance(p, r3h8.ID(), []services.PlacementInstance{r4h10})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOverSizedRack replace 1")

	// At this point, r1 has 4 instances to share a copy of 1024 partitions
	r1h11 := placement.NewEmptyInstance("r1h11", "r1", "z1", 1)

	p, err = a.ReplaceInstance(p, r2h2.ID(), []services.PlacementInstance{r1h11})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.22, "TestOverSizedRack replace 2")

	// adding a new instance to relieve the load on the hot instances
	r4h12 := placement.NewEmptyInstance("r4h12", "r4", "z1", 1)
	p, err = a.AddInstance(p, r4h12)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.15, "TestOverSizedRack add 1")
}

func TestInitPlacementOnNoInstances(t *testing.T) {
	instances := []services.PlacementInstance{}

	ids := make([]uint32, 128)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.Error(t, err)
	assert.Nil(t, p)
}

func TestOneRack(t *testing.T) {
	r1h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)
	r1h2 := placement.NewEmptyInstance("r1h2", "r1", "z1", 1)

	instances := []services.PlacementInstance{r1h1, r1h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOneRack replica 1")

	r1h6 := placement.NewEmptyInstance("r1h6", "r1", "z1", 1)

	p, err = a.AddInstance(p, r1h6)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOneRack addinstance 1")
}

func TestRFGreaterThanRackLen(t *testing.T) {
	r1h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)
	r1h6 := placement.NewEmptyInstance("r1h6", "r1", "z1", 1)

	r2h2 := placement.NewEmptyInstance("r2h2", "r2", "z1", 1)
	r2h3 := placement.NewEmptyInstance("r2h3", "r2", "z1", 1)

	instances := []services.PlacementInstance{r1h1, r2h2, r2h3, r1h6}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLen replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLen replica 2")

	p1, err := a.AddReplica(p)
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))
}

func TestRFGreaterThanRackLenAfterInstanceRemoval(t *testing.T) {
	r1h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)

	r2h2 := placement.NewEmptyInstance("r2h2", "r2", "z1", 1)

	instances := []services.PlacementInstance{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLenAfterInstanceRemoval replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLenAfterInstanceRemoval replica 2")

	p1, err := a.RemoveInstance(p, r2h2.ID())
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))
}

func TestRFGreaterThanRackLenAfterInstanceReplace(t *testing.T) {
	r1h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)

	r2h2 := placement.NewEmptyInstance("r2h2", "r2", "z1", 1)

	instances := []services.PlacementInstance{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLenAfterInstanceReplace replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLenAfterInstanceReplace replica 2")

	r1h3 := placement.NewEmptyInstance("r1h3", "r1", "z1", 1)
	p1, err := a.ReplaceInstance(p, r2h2.ID(), []services.PlacementInstance{r1h3})
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))
}

func TestLooseRackCheckAlgorithm(t *testing.T) {
	r1h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)

	r2h2 := placement.NewEmptyInstance("r2h2", "r2", "z1", 1)

	instances := []services.PlacementInstance{r1h1, r2h2}

	ids := make([]uint32, 10)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	markAllShardsAsAvailable(t, p)
	assert.NoError(t, placement.Validate(p))

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	markAllShardsAsAvailable(t, p)
	assert.NoError(t, placement.Validate(p))

	p1, err := a.AddReplica(p)
	assert.Equal(t, errNotEnoughRacks, err)
	assert.Nil(t, p1)
	markAllShardsAsAvailable(t, p)
	assert.NoError(t, placement.Validate(p))

	r2h4 := placement.NewEmptyInstance("r2h4", "r2", "z1", 1)
	p, err = a.AddInstance(p, r2h4)
	assert.NoError(t, err)
	markAllShardsAsAvailable(t, p)
	assert.NoError(t, placement.Validate(p))

	p1, err = a.AddReplica(p)
	assert.Equal(t, errNotEnoughRacks, err)
	assert.Nil(t, p1)
	markAllShardsAsAvailable(t, p)
	assert.NoError(t, placement.Validate(p))

	b := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(true))
	// different with normal algo, which would return error here
	r1h3 := placement.NewEmptyInstance("r1h3", "r1", "z1", 1)
	p, err = b.ReplaceInstance(p, r2h2.ID(), []services.PlacementInstance{r1h3})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))

	p1, err = b.ReplaceInstance(p, r2h4.ID(), []services.PlacementInstance{r1h3})
	assert.Equal(t, errAddingInstanceAlreadyExist, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))

	markAllShardsAsAvailable(t, p)
	p, err = b.RemoveInstance(p, r1h3.ID())
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))

	markAllShardsAsAvailable(t, p)
	r3h5 := placement.NewEmptyInstance("r3h5", "r3", "z1", 1)
	p, err = b.AddInstance(p, r3h5)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))

	p, err = b.AddReplica(p)
	assert.NoError(t, err)
	markAllShardsAsAvailable(t, p)
	assert.NoError(t, placement.Validate(p))
}

func TestAddInstancesCouldNotReachTargetLoad(t *testing.T) {
	r1h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	p := placement.NewPlacement([]services.PlacementInstance{}, ids, 1)

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())

	p1, err := a.AddInstance(p, r1h1)
	// errCouldNotReachTargetLoad should only happen when trying to add a instance to
	// an invalid placement that does not have enough shards for the rf it thought it has
	assert.Equal(t, errCouldNotReachTargetLoad, err)
	assert.Nil(t, p1)
}

func TestAddExistInstance(t *testing.T) {
	r1h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)

	r2h2 := placement.NewEmptyInstance("r2h2", "r2", "z1", 1)

	instances := []services.PlacementInstance{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestAddExistInstance replica 1")

	p1, err := a.AddInstance(p, r2h2)
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))
}

func TestRemoveAbsentInstance(t *testing.T) {
	r1h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)

	r2h2 := placement.NewEmptyInstance("r2h2", "r2", "z1", 1)

	instances := []services.PlacementInstance{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRemoveAbsentInstance replica 1")

	r3h3 := placement.NewEmptyInstance("r3h3", "r3", "z1", 1)

	p1, err := a.RemoveInstance(p, r3h3.ID())
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))
}

func TestReplaceAbsentInstance(t *testing.T) {
	r1h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)

	r2h2 := placement.NewEmptyInstance("r2h2", "r2", "z1", 1)

	instances := []services.PlacementInstance{r1h1, r2h2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestReplaceAbsentInstance replica 1")

	r3h3 := placement.NewEmptyInstance("r3h3", "r3", "z1", 1)
	r4h4 := placement.NewEmptyInstance("r4h4", "r4", "z1", 1)

	p1, err := a.ReplaceInstance(p, r3h3.ID(), []services.PlacementInstance{r4h4})
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))
}

func TestInit(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "", 1)
	i2 := placement.NewEmptyInstance("i2", "r2", "", 1)
	instances := []services.PlacementInstance{i1, i2}

	numShards := 4
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	assert.Equal(t, 1, p.ReplicaFactor())
	assert.Equal(t, numShards, p.NumShards())
	i1 = p.Instance("i1")
	i2 = p.Instance("i2")
	assert.Equal(t, 2, loadOnInstance(i1))
	assert.Equal(t, 2, i1.Shards().NumShards())
	assert.Equal(t, 2, loadOnInstance(i2))
	assert.Equal(t, 2, i2.Shards().NumShards())
	for _, instance := range p.Instances() {
		for _, s := range instance.Shards().All() {
			assert.Equal(t, shard.Initializing, s.State())
			assert.Equal(t, "", s.SourceID())
		}
	}
}

func TestAddReplica(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "", 1)
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r2", "", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	instances := []services.PlacementInstance{i1, i2}

	numShards := 4
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}
	p := placement.NewPlacement(instances, ids, 1)

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	p, err := a.AddReplica(p)
	assert.NoError(t, err)
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.Equal(t, numShards, p.NumShards())
	i1 = p.Instance("i1")
	i2 = p.Instance("i2")
	assert.Equal(t, 4, loadOnInstance(i1))
	assert.Equal(t, 4, i1.Shards().NumShards())
	assert.Equal(t, 4, loadOnInstance(i2))
	assert.Equal(t, 4, i2.Shards().NumShards())
	for _, instance := range p.Instances() {
		availableTotal := 0
		initTotal := 0
		for _, s := range instance.Shards().All() {
			assert.NotEqual(t, shard.Leaving, s.State())
			assert.Equal(t, "", s.SourceID())
			if s.State() == shard.Available {
				availableTotal++
			}
			if s.State() == shard.Initializing {
				initTotal++
			}
		}
		assert.Equal(t, 2, availableTotal)
		assert.Equal(t, 2, initTotal)
	}
}

func TestAddInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "", 1)
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	instances := []services.PlacementInstance{i1}

	numShards := 2
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}
	p := placement.NewPlacement(instances, ids, 1)

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	i2 := placement.NewEmptyInstance("i2", "r2", "", 1)
	p, err := a.AddInstance(p, i2)
	assert.NoError(t, err)
	assert.Equal(t, 1, p.ReplicaFactor())
	i1 = p.Instance("i1")
	i2 = p.Instance("i2")
	assert.Equal(t, 1, loadOnInstance(i1))
	assert.Equal(t, 2, i1.Shards().NumShards())
	assert.Equal(t, 1, loadOnInstance(i2))
	assert.Equal(t, 1, i2.Shards().NumShards())
	for _, s := range i2.Shards().All() {
		assert.Equal(t, shard.Initializing, s.State())
		assert.Equal(t, "i1", s.SourceID())
	}
}

func TestRemoveInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "", 1)
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r2", "", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	instances := []services.PlacementInstance{i1, i2}

	numShards := 4
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}
	p := placement.NewPlacement(instances, ids, 1)

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	p, err := a.RemoveInstance(p, i2.ID())
	assert.NoError(t, err)
	assert.Equal(t, 1, p.ReplicaFactor())
	assert.Equal(t, numShards, p.NumShards())
	i1 = p.Instance("i1")
	i2 = p.Instance("i2")
	assert.Equal(t, 4, loadOnInstance(i1))
	assert.Equal(t, 4, i1.Shards().NumShards())
	assert.Equal(t, 0, loadOnInstance(i2))
	assert.Equal(t, 2, i2.Shards().NumShards())
	for _, s := range i2.Shards().All() {
		assert.Equal(t, shard.Leaving, s.State())
		assert.Equal(t, "", s.SourceID())
	}
	availableTotal := 0
	initTotal := 0
	for _, s := range i1.Shards().All() {
		if s.State() == shard.Available {
			availableTotal++
		}
		if s.State() == shard.Initializing {
			initTotal++
		}
	}
	assert.Equal(t, 2, availableTotal)
	assert.Equal(t, 2, initTotal)
}

func TestReplaceInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "", 1)
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r2", "", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	instances := []services.PlacementInstance{i1, i2}

	numShards := 4
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}
	p := placement.NewPlacement(instances, ids, 1)

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions())
	i3 := placement.NewEmptyInstance("i3", "r3", "", 1)
	p, err := a.ReplaceInstance(p, i2.ID(), []services.PlacementInstance{i3})
	assert.NoError(t, err)
	i1 = p.Instance("i1")
	i2 = p.Instance("i2")
	i3 = p.Instance("i3")
	assert.Equal(t, 2, loadOnInstance(i1))
	assert.Equal(t, 2, i1.Shards().NumShards())
	assert.Equal(t, 0, loadOnInstance(i2))
	assert.Equal(t, 2, i2.Shards().NumShards())
	assert.Equal(t, 2, loadOnInstance(i3))
	assert.Equal(t, 2, i3.Shards().NumShards())
	for _, s := range i1.Shards().All() {
		assert.Equal(t, shard.Available, s.State())
		assert.Equal(t, "", s.SourceID())
	}
	for _, s := range i2.Shards().All() {
		assert.Equal(t, shard.Leaving, s.State())
		assert.Equal(t, "", s.SourceID())
	}
	for _, s := range i3.Shards().All() {
		assert.Equal(t, shard.Initializing, s.State())
		assert.Equal(t, "i2", s.SourceID())
	}
}

func markAllShardsAsAvailable(t *testing.T, p services.ServicePlacement) services.ServicePlacement {
	var err error
	for _, instance := range p.Instances() {
		for _, s := range instance.Shards().All() {
			if s.State() == shard.Initializing {
				p, err = MarkShardAvailable(p, instance.ID(), s.ID())
				assert.NoError(t, err)
			}
		}
	}
	return p
}

func validateDistribution(t *testing.T, p services.ServicePlacement, expectPeakOverAvg float64, testCase string) {
	assert.NoError(t, placement.Validate(p), "placement validation failed")
	ph := NewPlacementHelper(p, placement.NewOptions()).(*placementHelper)
	total := 0
	for _, i := range p.Instances() {
		load := loadOnInstance(i)
		total += load
		avgLoad := getWeightedLoad(ph, i.Weight())
		instanceOverAvg := float64(load) / float64(avgLoad)
		if math.Abs(float64(load-avgLoad)) > 1 {
			assert.True(t, instanceOverAvg <= expectPeakOverAvg, fmt.Sprintf("Bad distribution in %s, peak/Avg on %s is too high: %v, expecting %v, load on instance: %v, avg load: %v",
				testCase, i.ID(), instanceOverAvg, expectPeakOverAvg, load, avgLoad))
		}

		targetLoad := ph.TargetLoadForInstance(i.ID())
		if targetLoad == 0 {
			continue
		}
		instanceOverTarget := float64(load) / float64(targetLoad)
		if math.Abs(float64(load-targetLoad)) > 1 {
			assert.True(t, instanceOverTarget <= 1.03, fmt.Sprintf("Bad distribution in %s, peak/Target on %s is too high: %v, load on instance: %v, target load: %v",
				testCase, i.ID(), instanceOverTarget, load, targetLoad))
		}
	}
	assert.Equal(t, total, p.ReplicaFactor()*p.NumShards(), fmt.Sprintf("Wrong total shards: expecting %v, but got %v", p.ReplicaFactor()*p.NumShards(), total))
}

func getWeightedLoad(ph *placementHelper, weight uint32) int {
	return ph.rf * len(ph.shardToInstanceMap) * int(weight) / int(ph.totalWeight)
}
