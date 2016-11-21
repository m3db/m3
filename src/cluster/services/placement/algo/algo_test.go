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

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetAllowPartialReplace(true))
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 replica 1")

	p, err = a.AddInstance(p, placement.NewEmptyInstance("r6h21", "r6", "z1", 1))
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 add 1")

	p, err = a.RemoveInstance(p, h1)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 remove 1")

	h12 := placement.NewEmptyInstance("r3h12", "r3", "z1", 1)
	p, err = a.ReplaceInstance(p, h5, []services.PlacementInstance{h12})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 add 1")

	p, err = a.RemoveInstance(p, h2)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 remove 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 replica 2")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 replica 3")

	h10 := placement.NewEmptyInstance("r4h10", "r4", "z1", 1)
	p, err = a.AddInstance(p, h10)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 add 1")

	h11 := placement.NewEmptyInstance("r7h11", "r7", "z1", 1)
	p, err = a.AddInstance(p, h11)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 add 2")

	h13 := placement.NewEmptyInstance("r5h13", "r5", "z1", 1)
	p, err = a.ReplaceInstance(p, h3, []services.PlacementInstance{h13})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 replace 1")

	p, err = a.RemoveInstance(p, h4)
	assert.NoError(t, err)
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
	validateDistribution(t, p, 1.01, "good case1 replica 1")

	p, err = a.AddInstance(p, placement.NewEmptyInstance("h21", "r2", "z1", 10))
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 add 1")

	p, err = a.RemoveInstance(p, h1)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 remove 1")

	p, err = a.ReplaceInstance(p, h3,
		[]services.PlacementInstance{
			placement.NewEmptyInstance("h31", "r1", "z1", 10),
			placement.NewEmptyInstance("h32", "r1", "z1", 10),
		})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 replace 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 replica 2")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 replica 3")

	h10 := placement.NewEmptyInstance("h10", "r10", "z1", 10)
	p, err = a.AddInstance(p, h10)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 add 1")

	h11 := placement.NewEmptyInstance("h11", "r7", "z1", 10)
	p, err = a.AddInstance(p, h11)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 add 2")

	h13 := placement.NewEmptyInstance("h13", "r5", "z1", 10)

	p, err = a.ReplaceInstance(p, h11, []services.PlacementInstance{h13})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "good case1 replace 1")
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
	_, err = a.ReplaceInstance(p, r3h8, []services.PlacementInstance{r4h10})
	assert.Error(t, err)

	a = NewRackAwarePlacementAlgorithm(placement.NewOptions().SetAllowPartialReplace(true))
	p, err = a.ReplaceInstance(p, r3h8, []services.PlacementInstance{r4h10})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOverSizedRack replace 1")

	// At this point, r1 has 4 instances to share a copy of 1024 partitions
	r1h11 := placement.NewEmptyInstance("r1h11", "r1", "z1", 1)

	p, err = a.ReplaceInstance(p, r2h2, []services.PlacementInstance{r1h11})
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

	p1, err := a.RemoveInstance(p, r2h2)
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
	p1, err := a.ReplaceInstance(p, r2h2, []services.PlacementInstance{r1h3})
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
	assert.NoError(t, placement.Validate(p))

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))

	p1, err := a.AddReplica(p)
	assert.Equal(t, errNotEnoughRacks, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))

	r2h4 := placement.NewEmptyInstance("r2h4", "r2", "z1", 1)
	p, err = a.AddInstance(p, r2h4)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))

	p1, err = a.AddReplica(p)
	assert.Equal(t, errNotEnoughRacks, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))

	b := NewRackAwarePlacementAlgorithm(placement.NewOptions().SetLooseRackCheck(true))
	// different with normal algo, which would return error here
	r1h3 := placement.NewEmptyInstance("r1h3", "r1", "z1", 1)
	p, err = b.ReplaceInstance(p, r2h2, []services.PlacementInstance{r1h3})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))

	p1, err = b.ReplaceInstance(p, r2h4, []services.PlacementInstance{r1h3})
	assert.Equal(t, errAddingInstanceAlreadyExist, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))

	p, err = b.RemoveInstance(p, r1h3)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))

	r3h5 := placement.NewEmptyInstance("r3h5", "r3", "z1", 1)
	p, err = b.AddInstance(p, r3h5)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))

	p, err = b.AddReplica(p)
	assert.NoError(t, err)
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

	p1, err := a.RemoveInstance(p, r3h3)
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

	p1, err := a.ReplaceInstance(p, r3h3, []services.PlacementInstance{r4h4})
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))
}

func TestCanAssignInstance(t *testing.T) {
	h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)
	h1.Shards().AddShard(1)
	h1.Shards().AddShard(2)
	h1.Shards().AddShard(3)

	h2 := placement.NewEmptyInstance("r1h2", "r1", "z1", 1)
	h2.Shards().AddShard(4)
	h2.Shards().AddShard(5)
	h2.Shards().AddShard(6)

	h3 := placement.NewEmptyInstance("r2h3", "r2", "z1", 1)
	h3.Shards().AddShard(1)
	h3.Shards().AddShard(3)
	h3.Shards().AddShard(5)

	h4 := placement.NewEmptyInstance("r2h4", "r2", "z1", 1)
	h4.Shards().AddShard(2)
	h4.Shards().AddShard(4)
	h4.Shards().AddShard(6)

	h5 := placement.NewEmptyInstance("r3h5", "r3", "z1", 1)
	h5.Shards().AddShard(5)
	h5.Shards().AddShard(6)
	h5.Shards().AddShard(1)

	h6 := placement.NewEmptyInstance("r4h6", "r4", "z1", 1)
	h6.Shards().AddShard(2)
	h6.Shards().AddShard(3)
	h6.Shards().AddShard(4)

	instances := []services.PlacementInstance{h1, h2, h3, h4, h5, h6}

	mp := placement.NewPlacement(instances, []uint32{1, 2, 3, 4, 5, 6}, 3)

	ph := newHelper(mp, 3, placement.NewOptions()).(*placementHelper)
	assert.True(t, ph.canAssignInstance(2, h6, h5))
	assert.True(t, ph.canAssignInstance(1, h1, h6))
	assert.False(t, ph.canAssignInstance(2, h6, h1))
	// rack check
	assert.False(t, ph.canAssignInstance(2, h6, h3))
	ph = newHelper(mp, 3, placement.NewOptions().SetLooseRackCheck(true)).(*placementHelper)
	assert.True(t, ph.canAssignInstance(2, h6, h3))
}

func TestCopy(t *testing.T) {
	h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)
	h1.Shards().AddShard(1)
	h1.Shards().AddShard(2)
	h1.Shards().AddShard(3)

	h2 := placement.NewEmptyInstance("r2h2", "r2", "z1", 1)
	h2.Shards().AddShard(4)
	h2.Shards().AddShard(5)
	h2.Shards().AddShard(6)

	instances := []services.PlacementInstance{h1, h2}

	ids := []uint32{1, 2, 3, 4, 5, 6}
	s := placement.NewPlacement(instances, ids, 1)
	copy := copyPlacement(s)
	assert.Equal(t, s.NumInstances(), copy.NumInstances())
	assert.Equal(t, s.Shards(), copy.Shards())
	assert.Equal(t, s.ReplicaFactor(), copy.ReplicaFactor())
	for _, i := range s.Instances() {
		assert.Equal(t, copy.Instance(i.ID()), i)
		// make sure they are different objects, updating one won't update the other
		i.Shards().AddShard(100)
		assert.NotEqual(t, copy.Instance(i.ID()), i)
	}
}

func validateDistribution(t *testing.T, p services.ServicePlacement, expectPeakOverAvg float64, testCase string) {
	ph := NewPlacementHelper(p, placement.NewOptions()).(*placementHelper)
	total := 0
	for _, i := range p.Instances() {
		load := i.Shards().NumShards()
		total += load
		avgLoad := getWeightedLoad(ph, i.Weight())
		instanceOverAvg := float64(load) / float64(avgLoad)
		if math.Abs(float64(load-avgLoad)) > 1 {
			assert.True(t, instanceOverAvg <= expectPeakOverAvg, fmt.Sprintf("Bad distribution in %s, peak/Avg on %s is too high: %v, expecting %v, load on instance: %v, avg load: %v",
				testCase, i.ID(), instanceOverAvg, expectPeakOverAvg, load, avgLoad))
		}

		targetLoad := ph.TargetLoadForInstance(i.ID())
		instanceOverTarget := float64(load) / float64(targetLoad)
		if math.Abs(float64(load-targetLoad)) > 1 {
			assert.True(t, instanceOverTarget <= 1.03, fmt.Sprintf("Bad distribution in %s, peak/Target on %s is too high: %v, load on instance: %v, target load: %v",
				testCase, i.ID(), instanceOverTarget, load, targetLoad))
		}
	}
	assert.Equal(t, total, p.ReplicaFactor()*p.NumShards(), fmt.Sprintf("Wrong total partition: expecting %v, but got %v", p.ReplicaFactor()*p.NumShards(), total))
	assert.NoError(t, placement.Validate(p), "placement validation failed")
}

func getWeightedLoad(ph *placementHelper, weight uint32) int {
	return ph.rf * len(ph.shardToInstanceMap) * int(weight) / int(ph.totalWeight)
}
