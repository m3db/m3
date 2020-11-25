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

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/shard"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	instances := []Instance{i1, i2, i3, i4, i5, i6}

	ids := []uint32{1, 2, 3, 4, 5, 6}
	p := NewPlacement().SetInstances(instances).SetShards(ids).SetReplicaFactor(3)
	assert.False(t, p.IsSharded())
	p = p.SetIsSharded(true)
	assert.True(t, p.IsSharded())
	p = p.SetCutoverNanos(1234)
	assert.Equal(t, int64(1234), p.CutoverNanos())
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

	expectedInstancesByShard := map[uint32][]Instance{
		1: {i1, i3, i5},
		2: {i1, i4, i6},
		3: {i1, i3, i6},
		4: {i2, i4, i6},
		5: {i2, i3, i5},
		6: {i2, i4, i5},
	}
	for _, shard := range ids {
		assert.Equal(t, expectedInstancesByShard[shard], p.InstancesForShard(shard))
	}
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

	instances := []Instance{i1, i2}
	p := NewPlacement().SetInstances(instances).SetShards(ids).SetReplicaFactor(1).SetIsSharded(true)
	assert.NoError(t, Validate(p))
}

func TestUnknownShardState(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(0))

	// unknown shard state
	p := NewPlacement().SetInstances([]Instance{i1, i2}).SetShards([]uint32{0, 1}).SetReplicaFactor(1).SetIsSharded(true)
	assert.Error(t, Validate(p))
}

func TestMismatchShards(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	// mismatch shards
	p := NewPlacement().SetInstances([]Instance{i1, i2}).SetShards([]uint32{1, 2, 3}).SetReplicaFactor(1)
	require.Error(t, Validate(p))
}

func TestNonSharded(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	p := NewPlacement().SetInstances([]Instance{i1}).SetShards([]uint32{1}).SetReplicaFactor(1)
	assert.NoError(t, Validate(p))

	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	p = NewPlacement().SetInstances([]Instance{i1}).SetShards([]uint32{1}).SetReplicaFactor(1)
	assert.Error(t, Validate(p))
}

func TestValidateMirrorButNotSharded(t *testing.T) {
	p := NewPlacement().SetIsMirrored(true)
	err := Validate(p)
	require.Error(t, err)
	assert.Equal(t, errMirrorNotSharded.Error(), err.Error())
}

func TestValidateMissingShard(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	ids := []uint32{1, 2}
	p := NewPlacement().SetInstances([]Instance{i1, i2}).SetShards(ids).SetReplicaFactor(2).SetIsSharded(true)
	err := Validate(p)
	require.Error(t, err)
	assert.Equal(t, "invalid placement, the total available shards in the placement is 3, expecting 4", err.Error())
}

func TestValidateUnexpectedShard(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	p := NewPlacement().
		SetInstances([]Instance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(2).
		SetIsSharded(true)

	err := Validate(p)
	require.Error(t, err)
	assert.Equal(t, errUnexpectedShards.Error(), err.Error())
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
		SetInstances([]Instance{i1, i2}).
		SetShards([]uint32{2, 3, 4, 4, 5, 6}).
		SetReplicaFactor(1)
	err := Validate(p)
	require.Error(t, err)
	assert.Equal(t, errDuplicatedShards.Error(), err.Error())
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
		SetInstances([]Instance{i1, i2, i3}).
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
		SetInstances([]Instance{i1, i2}).
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
		SetInstances([]Instance{i1, i2}).
		SetShards([]uint32{1, 2, 3}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	err := Validate(p)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "invalid placement, 2 shards in Leaving state, not equal 1 in Initializing state with source id")
}

func TestValidateLeavingAndInitializingWithSourceIDMissing(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Leaving))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Leaving))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Initializing).SetSourceID("unknown"))
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i1"))

	p := NewPlacement().
		SetInstances([]Instance{i1, i2}).
		SetShards([]uint32{1, 2, 3}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	err := Validate(p)
	require.Error(t, err)
	assert.Equal(t, err.Error(), "instance i2 has initializing shard 1 with source ID unknown but no such instance in placement")
}

func TestValidateLeavingAndInitializingWithSourceIDNoSuchShard(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Leaving))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i1"))
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Initializing).SetSourceID("i1"))

	p := NewPlacement().
		SetInstances([]Instance{i1, i2}).
		SetShards([]uint32{1, 2, 3}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	err := Validate(p)
	require.Error(t, err)
	assert.Equal(t, err.Error(), "instance i2 has initializing shard 3 with source ID i1 but leaving instance has no such shard")
}

func TestValidateLeavingAndInitializingWithSourceIDShardNotLeaving(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Leaving))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Initializing).SetSourceID("i1"))
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i1"))

	p := NewPlacement().
		SetInstances([]Instance{i1, i2}).
		SetShards([]uint32{1, 2, 3}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	err := Validate(p)
	require.Error(t, err)
	assert.Equal(t, err.Error(), "instance i2 has initializing shard 1 with source ID i1 but leaving instance has shard with state Available")
}

func TestValidateLeavingAndInitializingWithSourceIDDoubleMatched(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Leaving))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i1"))

	i3 := NewEmptyInstance("i3", "r2", "z1", "endpoint", 1)
	i3.Shards().Add(shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i1"))

	p := NewPlacement().
		SetInstances([]Instance{i1, i2, i3}).
		SetShards([]uint32{1, 2, 3}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	err := Validate(p)
	require.Error(t, err)
	assert.Equal(t, err.Error(), "instance i3 has initializing shard 2 with source ID i1 but leaving instance has shard already matched by i2")
}

func TestValidateNoEndpoint(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	p := NewPlacement().
		SetInstances([]Instance{i1, i2}).
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
		SetInstances([]Instance{i1, i2}).
		SetShards([]uint32{1}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	err := Validate(p)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "contains no shard")
}

func TestValidateInstanceWithSameShardSetIDSameShards(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1).SetShardSetID(1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z2", "endpoint2", 1).SetShardSetID(1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	p := NewPlacement().
		SetInstances([]Instance{i1, i2}).
		SetShards([]uint32{1}).
		SetReplicaFactor(2).
		SetIsSharded(true).
		SetMaxShardSetID(1)
	assert.NoError(t, Validate(p))
}

func TestValidateInstanceWithSameShardSetIDDifferentShards(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1).SetShardSetID(1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z2", "endpoint2", 1).SetShardSetID(1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	p := NewPlacement().
		SetInstances([]Instance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(1).
		SetIsSharded(true).
		SetMaxShardSetID(1)
	err := Validate(p)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "have the same shard set id")
}

func TestValidateInstanceWithValidMaxShardSetID(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1).SetShardSetID(1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1).SetShardSetID(3)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	p := NewPlacement().
		SetInstances([]Instance{i1, i2}).
		SetShards([]uint32{1}).
		SetReplicaFactor(2).
		SetIsSharded(true).
		SetMaxShardSetID(3)
	assert.NoError(t, Validate(p))
}

func TestValidateInstanceWithShardSetIDLargerThanMax(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1).SetShardSetID(1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1).SetShardSetID(3)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	p := NewPlacement().
		SetInstances([]Instance{i1, i2}).
		SetShards([]uint32{1}).
		SetReplicaFactor(1).
		SetIsSharded(true).
		SetMaxShardSetID(2)
	err := Validate(p)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "larger than max shard set id")
}

func TestInstance(t *testing.T) {
	i1 := NewInstance().
		SetID("id").
		SetEndpoint("endpoint").
		SetIsolationGroup("isolationGroup").
		SetWeight(1).
		SetShardSetID(0).
		SetZone("zone").
		SetHostname("host1").
		SetPort(123).
		SetMetadata(InstanceMetadata{DebugPort: 456})
	assert.NotNil(t, i1.Shards())
	s := shard.NewShards([]shard.Shard{
		shard.NewShard(1).SetState(shard.Available),
		shard.NewShard(2).SetState(shard.Available),
		shard.NewShard(3).SetState(shard.Available),
	})
	i1.SetShards(s)
	description := fmt.Sprintf(
		"Instance[ID=id, IsolationGroup=isolationGroup, Zone=zone, Weight=1, Endpoint=endpoint, Hostname=host1, Port=123, ShardSetID=0, Shards=%s, Metadata={DebugPort:456}]",
		s.String())
	assert.Equal(t, description, i1.String())

	assert.True(t, i1.Shards().Contains(1))
	assert.False(t, i1.Shards().Contains(100))
	assert.Equal(t, 3, i1.Shards().NumShards())
	assert.Equal(t, "id", i1.ID())
	assert.Equal(t, "endpoint", i1.Endpoint())
	assert.Equal(t, uint32(1), i1.Weight())
	assert.Equal(t, "zone", i1.Zone())
	assert.Equal(t, "isolationGroup", i1.IsolationGroup())

	i1.Shards().Remove(1)
	assert.False(t, i1.Shards().Contains(1))
	assert.False(t, i1.Shards().Contains(100))
	assert.Equal(t, 2, i1.Shards().NumShards())
	assert.Equal(t, "id", i1.ID())
	assert.Equal(t, "isolationGroup", i1.IsolationGroup())
}

func TestInstanceIsLeaving(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	assert.False(t, i1.IsLeaving())

	i1.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	assert.False(t, i1.IsLeaving())

	i1.Shards().Add(shard.NewShard(1).SetState(shard.Leaving))
	assert.True(t, i1.IsLeaving())

	i1.SetShards(shard.NewShards(nil))
	assert.False(t, i1.IsLeaving())
}

func TestInstanceIsInitializing(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	assert.False(t, i1.IsInitializing())

	i1.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	assert.True(t, i1.IsInitializing())

	i1.Shards().Add(shard.NewShard(1).SetState(shard.Leaving))
	assert.False(t, i1.IsInitializing())

	i1.SetShards(shard.NewShards(nil))
	assert.False(t, i1.IsInitializing())
}

func TestInstanceIsAvailable(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Leaving))
	assert.False(t, i1.IsAvailable())

	i1.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	assert.False(t, i1.IsAvailable())

	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	assert.True(t, i1.IsAvailable())

	i1.SetShards(shard.NewShards(nil))
	assert.False(t, i1.IsAvailable())
}

func TestSortInstanceByID(t *testing.T) {
	i1 := NewEmptyInstance("i1", "", "", "endpoint", 1)
	i2 := NewEmptyInstance("i2", "", "", "endpoint", 1)
	i3 := NewEmptyInstance("i3", "", "", "endpoint", 1)
	i4 := NewEmptyInstance("i4", "", "", "endpoint", 1)
	i5 := NewEmptyInstance("i5", "", "", "endpoint", 1)
	i6 := NewEmptyInstance("i6", "", "", "endpoint", 1)

	i := []Instance{i1, i6, i4, i2, i3, i5}
	sort.Sort(ByIDAscending(i))

	assert.Equal(t, []Instance{i1, i2, i3, i4, i5, i6}, i)
}

func TestClonePlacement(t *testing.T) {
	i1 := NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	instances := []Instance{i1, i2}

	ids := []uint32{1, 2, 3, 4, 5, 6}
	p := NewPlacement().
		SetInstances(instances).
		SetShards(ids).
		SetReplicaFactor(1).
		SetIsMirrored(false).
		SetIsSharded(true).
		SetCutoverNanos(1234).
		SetMaxShardSetID(2)
	copy := p.Clone()
	assert.Equal(t, p.NumInstances(), copy.NumInstances())
	assert.Equal(t, p.Shards(), copy.Shards())
	assert.Equal(t, p.ReplicaFactor(), copy.ReplicaFactor())
	assert.Equal(t, p.MaxShardSetID(), copy.MaxShardSetID())
	for _, instance := range p.Instances() {
		copiedInstance, exist := copy.Instance(instance.ID())
		assert.True(t, exist)
		for _, s := range copiedInstance.Shards().All() {
			otherS, _ := instance.Shards().Shard(s.ID())
			assert.Equal(t, s, otherS)
		}
		assert.Equal(t, copiedInstance, instance)
		// make sure they are different objects, updating one won't update the other
		instance.Shards().Add(shard.NewShard(100).SetState(shard.Available))
		assert.NotEqual(t, copiedInstance, instance)
	}
}

func TestVersion(t *testing.T) {
	p1 := NewPlacement()
	assert.Equal(t, 0, p1.Version())

	p1 = p1.SetVersion(100)
	assert.Equal(t, 100, p1.Version())
}

func TestConvertBetweenProtoAndPlacement(t *testing.T) {
	protoShards := getProtoShards([]uint32{0, 1, 2})
	placementProto := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"i1": &placementpb.Instance{
				Id:             "i1",
				IsolationGroup: "r1",
				Zone:           "z1",
				Endpoint:       "e1",
				Weight:         1,
				Shards:         protoShards,
				ShardSetId:     0,
				Metadata:       &placementpb.InstanceMetadata{DebugPort: 123},
			},
			"i2": &placementpb.Instance{
				Id:             "i2",
				IsolationGroup: "r2",
				Zone:           "z1",
				Endpoint:       "e2",
				Weight:         1,
				Shards:         protoShards,
				ShardSetId:     1,
				Metadata:       &placementpb.InstanceMetadata{DebugPort: 456},
			},
		},
		ReplicaFactor: 2,
		NumShards:     3,
		IsSharded:     true,
		CutoverTime:   1234,
		MaxShardSetId: 1,
	}

	p, err := NewPlacementFromProto(placementProto)
	assert.NoError(t, err)
	assert.Equal(t, 2, p.NumInstances())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.True(t, p.IsSharded())
	assert.Equal(t, []uint32{0, 1, 2}, p.Shards())
	assert.Equal(t, int64(1234), p.CutoverNanos())
	assert.Equal(t, uint32(1), p.MaxShardSetID())
	instances := p.Instances()
	assert.Equal(t, uint32(0), instances[0].ShardSetID())
	assert.Equal(t, uint32(123), instances[0].Metadata().DebugPort)
	assert.Equal(t, uint32(1), instances[1].ShardSetID())
	assert.Equal(t, uint32(456), instances[1].Metadata().DebugPort)

	placementProtoNew, err := p.Proto()
	assert.NoError(t, err)
	assert.Equal(t, placementProto.ReplicaFactor, placementProtoNew.ReplicaFactor)
	assert.Equal(t, placementProto.NumShards, placementProtoNew.NumShards)
	assert.Equal(t, placementProto.CutoverTime, placementProtoNew.CutoverTime)
	assert.Equal(t, placementProto.MaxShardSetId, placementProtoNew.MaxShardSetId)
	for id, h := range placementProto.Instances {
		instance := placementProtoNew.Instances[id]
		assert.Equal(t, h.Id, instance.Id)
		assert.Equal(t, h.IsolationGroup, instance.IsolationGroup)
		assert.Equal(t, h.Zone, instance.Zone)
		assert.Equal(t, h.Weight, instance.Weight)
		assert.Equal(t, h.Shards, instance.Shards)
		assert.Equal(t, h.ShardSetId, instance.ShardSetId)
	}
}

func TestPlacementInstanceFromProto(t *testing.T) {
	protoShardsUnsorted := getProtoShards([]uint32{2, 1, 0})

	instanceProto := &placementpb.Instance{
		Id:             "i1",
		IsolationGroup: "r1",
		Zone:           "z1",
		Endpoint:       "e1",
		Weight:         1,
		Shards:         protoShardsUnsorted,
	}

	instance, err := NewInstanceFromProto(instanceProto)
	assert.NoError(t, err)

	instanceShards := instance.Shards()

	// assert.Equal can't compare shards due to pointer types, we check them
	// manually
	instance.SetShards(shard.NewShards(nil))

	expShards := shard.NewShards([]shard.Shard{
		shard.NewShard(0).SetSourceID("i1").SetState(shard.Available),
		shard.NewShard(1).SetSourceID("i1").SetState(shard.Available),
		shard.NewShard(2).SetSourceID("i1").SetState(shard.Available),
	})

	expInstance := NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("e1").
		SetWeight(1)

	assert.Equal(t, expInstance, instance)
	assert.Equal(t, expShards.AllIDs(), instanceShards.AllIDs())

	instanceProto.Shards[0].State = placementpb.ShardState(1000)
	instance, err = NewInstanceFromProto(instanceProto)
	assert.Error(t, err)
	assert.Nil(t, instance)
}

func TestPlacementInstanceToProto(t *testing.T) {
	shards := shard.NewShards([]shard.Shard{
		shard.NewShard(2).SetSourceID("i1").SetState(shard.Available),
		shard.NewShard(1).SetSourceID("i1").SetState(shard.Available),
		shard.NewShard(0).SetSourceID("i1").SetState(shard.Available),
	})

	instance := NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("e1").
		SetWeight(1).
		SetShards(shards).
		SetMetadata(InstanceMetadata{
			DebugPort: 123,
		})

	instanceProto, err := instance.Proto()
	assert.NoError(t, err)

	protoShards := getProtoShards([]uint32{0, 1, 2})
	for _, s := range protoShards {
		s.SourceId = "i1"
	}

	expInstance := &placementpb.Instance{
		Id:             "i1",
		IsolationGroup: "r1",
		Zone:           "z1",
		Endpoint:       "e1",
		Weight:         1,
		Shards:         protoShards,
		Metadata: &placementpb.InstanceMetadata{
			DebugPort: 123,
		},
	}

	assert.Equal(t, expInstance, instanceProto)
}

func getProtoShards(ids []uint32) []*placementpb.Shard {
	r := make([]*placementpb.Shard, len(ids))
	for i, id := range ids {
		r[i] = &placementpb.Shard{
			Id:    id,
			State: placementpb.ShardState_AVAILABLE,
		}
	}
	return r
}
