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

package placement

import (
	"fmt"
	"sort"
	"testing"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/instrument"
	"github.com/stretchr/testify/assert"
)

func TestPlacement(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	i3 := NewEmptyInstance("i3", "r3", "z1", "endpoint", 1)
	i3.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(5).SetState(shard.Available))

	i4 := NewEmptyInstance("i4", "r4", "z1", "endpoint", 1)
	i4.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	i5 := NewEmptyInstance("i5", "r5", "z1", "endpoint", 1)
	i5.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i5.Shards().Add(shard.NewShard(6).SetState(shard.Available))
	i5.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i6 := NewEmptyInstance("i6", "r6", "z1", "endpoint", 1)
	i6.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i6.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	i6.Shards().Add(shard.NewShard(4).SetState(shard.Available))

	instances := []services.PlacementInstance{i1, i2, i3, i4, i5, i6}

	ids := []uint32{1, 2, 3, 4, 5, 6}
	p := NewPlacement().SetInstances(instances).SetShards(ids).SetReplicaFactor(3)
	assert.False(t, p.IsSharded())
	p = p.SetIsSharded(true)
	assert.True(t, p.IsSharded())
	assert.NoError(t, Validate(p))

	i, exist := p.Instance("i6")
	assert.True(t, exist)
	assert.Equal(t, i6, i)
	_, exist = p.Instance("not_exist")
	assert.False(t, exist)

	assert.Equal(t, 6, p.NumInstances())
	assert.Equal(t, 3, p.ReplicaFactor())
	assert.Equal(t, ids, p.Shards())
	assert.Equal(t, 6, p.NumShards())
	is := p.Instances()
	sort.Sort(ByIDAscending(is))
	assert.Equal(t, instances, is)
}

func TestValidateGood(t *testing.T) {
	ids := []uint32{1, 2, 3, 4, 5, 6}

	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Initializing))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(6).SetState(shard.Initializing))

	instances := []services.PlacementInstance{i1, i2}
	p := NewPlacement().SetInstances(instances).SetShards(ids).SetReplicaFactor(1).SetIsSharded(true)
	assert.NoError(t, Validate(p))
}

func TestUnknownShardState(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(0))

	// unknown shard state
	p := NewPlacement().SetInstances([]services.PlacementInstance{i1, i2}).SetShards([]uint32{0, 1}).SetReplicaFactor(1).SetIsSharded(true)
	assert.Error(t, Validate(p))
}

func TestMismatchShards(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	// mismatch shards
	p := NewPlacement().SetInstances([]services.PlacementInstance{i1, i2}).SetShards([]uint32{1, 2, 3}).SetReplicaFactor(1)
	assert.Error(t, Validate(p))
}

func TestNonSharded(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	p := NewPlacement().SetInstances([]services.PlacementInstance{i1}).SetShards([]uint32{1}).SetReplicaFactor(1)
	assert.NoError(t, Validate(p))

	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	p = NewPlacement().SetInstances([]services.PlacementInstance{i1}).SetShards([]uint32{1}).SetReplicaFactor(1)
	assert.Error(t, Validate(p))
}

func TestValidateMissingShard(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	ids := []uint32{1, 2}
	p := NewPlacement().SetInstances([]services.PlacementInstance{i1, i2}).SetShards(ids).SetReplicaFactor(2).SetIsSharded(true)
	err := Validate(p)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "invalid placement, the total available shards in the placement is 3, expecting 4")
}

func TestValidateUnexpectedShard(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	p := NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(2).
		SetIsSharded(true)

	assert.Error(t, Validate(p))
	assert.Equal(t, errUnexpectedShards, Validate(p))
}

func TestValidateDuplicatedShards(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(4).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	p := NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2}).
		SetShards([]uint32{2, 3, 4, 4, 5, 6}).
		SetReplicaFactor(1)
	assert.Error(t, Validate(p))
	assert.Equal(t, errDuplicatedShards, Validate(p))
}

func TestValidateWrongReplicaForSomeShards(t *testing.T) {
	// three shard 2 and only one shard 4
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(4).SetState(shard.Available))

	i3 := NewEmptyInstance("i3", "r3", "z1", "endpoint", 1)
	i3.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	p := NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2, i3}).
		SetShards([]uint32{1, 2, 3, 4}).
		SetReplicaFactor(2)
	assert.Error(t, Validate(p))
}

func TestValidateLeavingMoreThanInitializing(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Initializing))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Leaving))
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Leaving))
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Leaving))

	p := NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2}).
		SetShards([]uint32{1, 2, 3}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	err := Validate(p)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "invalid placement, 3 shards in Leaving state, more than 2 in Initializing state")
}

func TestValidateLeavingNotMatchInitializingWithSourceID(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Leaving))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Leaving))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i1"))

	p := NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2}).
		SetShards([]uint32{1, 2, 3}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	err := Validate(p)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "invalid placement, 2 shards in Leaving state, not equal 1 in Initializing state with source id")
}

func TestValidateNoEndpoint(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	p := NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	err := Validate(p)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not contain valid endpoint")
}

func TestValidateInstanceWithNoShard(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	p := NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2}).
		SetShards([]uint32{1}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	err := Validate(p)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "contains no shard")
}

func TestInstance(t *testing.T) {
	i1 := NewInstance().
		SetID("id").
		SetEndpoint("endpoint").
		SetRack("rack").
		SetWeight(1).
		SetZone("zone")
	assert.NotNil(t, i1.Shards())
	s := shard.NewShards([]shard.Shard{
		shard.NewShard(1).SetState(shard.Available),
		shard.NewShard(2).SetState(shard.Available),
		shard.NewShard(3).SetState(shard.Available),
	})
	i1.SetShards(s)
	description := fmt.Sprintf(
		"Instance[ID=id, Rack=rack, Zone=zone, Weight=1, Endpoint=endpoint, Shards=%s]",
		s.String())
	assert.Equal(t, description, i1.String())

	assert.True(t, i1.Shards().Contains(1))
	assert.False(t, i1.Shards().Contains(100))
	assert.Equal(t, 3, i1.Shards().NumShards())
	assert.Equal(t, "id", i1.ID())
	assert.Equal(t, "endpoint", i1.Endpoint())
	assert.Equal(t, uint32(1), i1.Weight())
	assert.Equal(t, "zone", i1.Zone())
	assert.Equal(t, "rack", i1.Rack())

	i1.Shards().Remove(1)
	assert.False(t, i1.Shards().Contains(1))
	assert.False(t, i1.Shards().Contains(100))
	assert.Equal(t, 2, i1.Shards().NumShards())
	assert.Equal(t, "id", i1.ID())
	assert.Equal(t, "rack", i1.Rack())
}

func TestIsInstanceLeaving(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	assert.False(t, IsInstanceLeaving(i1))

	i1.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	assert.False(t, IsInstanceLeaving(i1))

	i1.Shards().Add(shard.NewShard(1).SetState(shard.Leaving))
	assert.True(t, IsInstanceLeaving(i1))

	i1.SetShards(shard.NewShards(nil))
	assert.False(t, IsInstanceLeaving(i1))
}

func TestSortInstanceByID(t *testing.T) {
	i1 := NewEmptyInstance("i1", "", "", "endpoint", 1)
	i2 := NewEmptyInstance("i2", "", "", "endpoint", 1)
	i3 := NewEmptyInstance("i3", "", "", "endpoint", 1)
	i4 := NewEmptyInstance("i4", "", "", "endpoint", 1)
	i5 := NewEmptyInstance("i5", "", "", "endpoint", 1)
	i6 := NewEmptyInstance("i6", "", "", "endpoint", 1)

	i := []services.PlacementInstance{i1, i6, i4, i2, i3, i5}
	sort.Sort(ByIDAscending(i))

	assert.Equal(t, []services.PlacementInstance{i1, i2, i3, i4, i5, i6}, i)
}

func TestVersion(t *testing.T) {
	p1 := NewPlacement()
	assert.Equal(t, 0, p1.GetVersion())

	p1 = p1.SetVersion(100)
	assert.Equal(t, 100, p1.GetVersion())
}

func TestOptions(t *testing.T) {
	o := NewOptions()
	assert.False(t, o.LooseRackCheck())
	assert.True(t, o.AllowPartialReplace())
	assert.True(t, o.IsSharded())
	assert.False(t, o.Dryrun())
	assert.Equal(t, instrument.NewOptions(), o.InstrumentOptions())

	o = o.SetLooseRackCheck(true)
	assert.True(t, o.LooseRackCheck())

	o = o.SetAllowPartialReplace(false)
	assert.False(t, o.AllowPartialReplace())

	o = o.SetIsSharded(false)
	assert.False(t, o.IsSharded())

	o = o.SetDryrun(true)
	assert.True(t, o.Dryrun())

	iopts := instrument.NewOptions().SetMetricsSamplingRate(0.5)
	o = o.SetInstrumentOptions(iopts)
	assert.Equal(t, iopts, o.InstrumentOptions())

	dopts := NewDeploymentOptions()
	assert.Equal(t, defaultMaxStepSize, dopts.MaxStepSize())
	dopts = dopts.SetMaxStepSize(5)
	assert.Equal(t, 5, dopts.MaxStepSize())
}

func TestMarkShardSuccess(t *testing.T) {
	i1 := NewEmptyInstance("i1", "", "", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Initializing))

	i2 := NewEmptyInstance("i2", "", "", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))

	p := NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(2)

	_, err := MarkShardAvailable(p, "i1", 1)
	assert.NoError(t, err)
}

func TestMarkShardFailure(t *testing.T) {
	i1 := NewEmptyInstance("i1", "", "", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "", "", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Initializing).SetSourceID("i3"))
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i1"))
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Initializing).SetSourceID("i1"))

	p := NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(2)

	_, err := MarkShardAvailable(p, "i3", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist in placement")

	_, err = MarkShardAvailable(p, "i1", 3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist in instance")

	_, err = MarkShardAvailable(p, "i1", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not in Initializing state")

	_, err = MarkShardAvailable(p, "i2", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist in placement")

	_, err = MarkShardAvailable(p, "i2", 3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist in source instance")

	_, err = MarkShardAvailable(p, "i2", 2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not leaving instance")
}

func TestRemoveInstanceFromArray(t *testing.T) {
	instances := []services.PlacementInstance{
		NewEmptyInstance("i1", "", "", "endpoint", 1),
		NewEmptyInstance("i2", "", "", "endpoint", 1),
	}

	assert.Equal(t, instances, RemoveInstance(instances, "not_exist"))
	assert.Equal(t, []services.PlacementInstance{NewEmptyInstance("i2", "", "", "endpoint", 1)}, RemoveInstance(instances, "i1"))
}

func TestMarkAllAsAvailable(t *testing.T) {
	i1 := NewEmptyInstance("i1", "", "", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Initializing))

	i2 := NewEmptyInstance("i2", "", "", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Initializing))

	p := NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(2)

	p, err := MarkAllShardsAsAvailable(p)
	assert.NoError(t, err)

	i2.Shards().Add(shard.NewShard(3).SetState(shard.Initializing).SetSourceID("i3"))
	p = NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(2)
	_, err = MarkAllShardsAsAvailable(p)
	assert.Contains(t, err.Error(), "does not exist in placement")
}
