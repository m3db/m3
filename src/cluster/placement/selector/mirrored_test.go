// Copyright (c) 2017 Uber Technologies, Inc.
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

package selector

import (
	"testing"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectInitialInstancesForMirror(t *testing.T) {
	h1p1 := placement.NewInstance().
		SetID("h1p1").
		SetHostname("h1").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p1e").
		SetWeight(1)
	h1p2 := placement.NewInstance().
		SetID("h1p2").
		SetHostname("h1").
		SetPort(2).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p2e").
		SetWeight(1)
	h2p1 := placement.NewInstance().
		SetID("h2p1").
		SetHostname("h2").
		SetPort(1).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p1e").
		SetWeight(1)
	h2p2 := placement.NewInstance().
		SetID("h2p2").
		SetHostname("h2").
		SetPort(2).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p2e").
		SetWeight(1)
	h3p1 := placement.NewInstance().
		SetID("h3p1").
		SetHostname("h3").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h3p1e").
		SetWeight(1)
	h3p2 := placement.NewInstance().
		SetID("h3p2").
		SetHostname("h3").
		SetPort(2).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h3p2e").
		SetWeight(1)
	h4p1 := placement.NewInstance().
		SetID("h4p1").
		SetHostname("h4").
		SetPort(1).
		SetIsolationGroup("r3").
		SetZone("z1").
		SetEndpoint("h4p1e").
		SetWeight(1)
	h4p2 := placement.NewInstance().
		SetID("h4p2").
		SetHostname("h4").
		SetPort(2).
		SetIsolationGroup("r3").
		SetZone("z1").
		SetEndpoint("h4p2e").
		SetWeight(1)

	selector := NewPortMirroredSelector(placement.NewOptions().SetValidZone("z1"))
	res, err := selector.SelectInitialInstances(
		[]placement.Instance{
			h1p1.SetShardSetID(0),
			h1p2.SetShardSetID(0),
			h2p1.SetShardSetID(0),
			h2p2.SetShardSetID(0),
			h3p1.SetShardSetID(0),
			h3p2.SetShardSetID(0),
			h4p1.SetShardSetID(0),
			h4p2.SetShardSetID(0),
		},
		2,
	)
	require.NoError(t, err)
	require.Equal(t, 8, len(res))

	ssIDs := make(map[uint32]int)
	for i := 1; i <= 4; i++ {
		ssIDs[uint32(i)] = 2
	}

	for _, instance := range res {
		ssIDs[instance.ShardSetID()] = ssIDs[instance.ShardSetID()] - 1
	}

	for _, count := range ssIDs {
		require.Equal(t, 0, count)
	}
}

func TestSelectInitialInstancesForMirrorRF2(t *testing.T) {
	h1p1 := placement.NewInstance().
		SetID("h1p1").
		SetHostname("h1").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p1e").
		SetWeight(1)
	h1p2 := placement.NewInstance().
		SetID("h1p2").
		SetHostname("h1").
		SetPort(2).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p2e").
		SetWeight(1)
	h1p3 := placement.NewInstance().
		SetID("h1p3").
		SetHostname("h1").
		SetPort(3).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p3e").
		SetWeight(1)
	h2p1 := placement.NewInstance().
		SetID("h2p1").
		SetHostname("h2").
		SetPort(1).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p1e").
		SetWeight(1)
	h2p2 := placement.NewInstance().
		SetID("h2p2").
		SetHostname("h2").
		SetPort(2).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p2e").
		SetWeight(1)
	h2p3 := placement.NewInstance().
		SetID("h2p3").
		SetHostname("h2").
		SetPort(3).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p3e").
		SetWeight(1)
	h3p1 := placement.NewInstance().
		SetID("h3p1").
		SetHostname("h3").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h3p1e").
		SetWeight(2)
	h3p2 := placement.NewInstance().
		SetID("h3p2").
		SetHostname("h3").
		SetPort(2).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h3p2e").
		SetWeight(2)
	h3p3 := placement.NewInstance().
		SetID("h3p3").
		SetHostname("h3").
		SetPort(3).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h3p3e").
		SetWeight(2)

	selector := NewPortMirroredSelector(placement.NewOptions().SetValidZone("z1"))
	res, err := selector.SelectInitialInstances(
		[]placement.Instance{h1p1, h1p2, h1p3, h2p1, h2p2, h2p3, h3p1, h3p2, h3p3},
		2,
	)
	require.NoError(t, err)
	require.Equal(t, 6, len(res))
	require.Equal(t, h1p1.ShardSetID(), h2p1.ShardSetID())
	require.Equal(t, h1p2.ShardSetID(), h2p2.ShardSetID())
	require.Equal(t, h1p3.ShardSetID(), h2p3.ShardSetID())
	ssIDs := make(map[uint32]int)
	for i := 1; i <= 3; i++ {
		ssIDs[uint32(i)] = 2
	}

	for _, instance := range res {
		ssIDs[instance.ShardSetID()] = ssIDs[instance.ShardSetID()] - 1
	}

	for _, count := range ssIDs {
		require.Equal(t, 0, count)
	}

	require.Equal(t, h3p1.ShardSetID(), h3p2.ShardSetID())
	require.Empty(t, h3p1.ShardSetID())

	h4p1 := placement.NewInstance().
		SetID("h4p1").
		SetHostname("h4").
		SetPort(1).
		SetIsolationGroup("r3").
		SetZone("z1").
		SetEndpoint("h4p1e").
		SetWeight(2)
	h4p2 := placement.NewInstance().
		SetID("h4p2").
		SetHostname("h4").
		SetPort(2).
		SetIsolationGroup("r3").
		SetZone("z1").
		SetEndpoint("h4p2e").
		SetWeight(2)
	h4p3 := placement.NewInstance().
		SetID("h4p3").
		SetHostname("h4").
		SetPort(3).
		SetIsolationGroup("r3").
		SetZone("z1").
		SetEndpoint("h4p3e").
		SetWeight(2)

	res, err = selector.SelectInitialInstances(
		[]placement.Instance{
			h1p1.SetShardSetID(0),
			h1p2.SetShardSetID(0),
			h1p3.SetShardSetID(0),
			h2p1.SetShardSetID(0),
			h2p2.SetShardSetID(0),
			h2p3.SetShardSetID(0),
			h3p1.SetShardSetID(0),
			h3p2.SetShardSetID(0),
			h3p3.SetShardSetID(0),
			h4p1.SetShardSetID(0),
			h4p2.SetShardSetID(0),
			h4p3.SetShardSetID(0),
		},
		2,
	)
	require.NoError(t, err)
	require.Equal(t, 12, len(res))
	require.Equal(t, h1p1.ShardSetID(), h2p1.ShardSetID())
	require.Equal(t, h1p2.ShardSetID(), h2p2.ShardSetID())
	require.Equal(t, h1p3.ShardSetID(), h2p3.ShardSetID())
	require.Equal(t, h3p1.ShardSetID(), h4p1.ShardSetID())
	require.Equal(t, h3p2.ShardSetID(), h4p2.ShardSetID())
	require.Equal(t, h3p3.ShardSetID(), h4p3.ShardSetID())

	ssIDs = make(map[uint32]int)
	for i := 1; i <= 6; i++ {
		ssIDs[uint32(i)] = 2
	}

	for _, instance := range res {
		ssIDs[instance.ShardSetID()] = ssIDs[instance.ShardSetID()] - 1
	}

	for _, count := range ssIDs {
		require.Equal(t, 0, count)
	}
}

func TestSelectInitialInstancesForMirrorRF3(t *testing.T) {
	h1p1 := placement.NewInstance().
		SetID("h1p1").
		SetHostname("h1").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p1e").
		SetWeight(1)
	h1p2 := placement.NewInstance().
		SetID("h1p2").
		SetHostname("h1").
		SetPort(2).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p2e").
		SetWeight(1)
	h1p3 := placement.NewInstance().
		SetID("h1p3").
		SetHostname("h1").
		SetPort(3).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p3e").
		SetWeight(1)
	h2p1 := placement.NewInstance().
		SetID("h2p1").
		SetHostname("h2").
		SetPort(1).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p1e").
		SetWeight(1)
	h2p2 := placement.NewInstance().
		SetID("h2p2").
		SetHostname("h2").
		SetPort(2).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p2e").
		SetWeight(1)
	h2p3 := placement.NewInstance().
		SetID("h2p3").
		SetHostname("h2").
		SetPort(3).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p3e").
		SetWeight(1)
	h3p1 := placement.NewInstance().
		SetID("h3p1").
		SetHostname("h3").
		SetPort(1).
		SetIsolationGroup("r3").
		SetZone("z1").
		SetEndpoint("h3p1e").
		SetWeight(1)
	h3p2 := placement.NewInstance().
		SetID("h3p2").
		SetHostname("h3").
		SetPort(2).
		SetIsolationGroup("r3").
		SetZone("z1").
		SetEndpoint("h3p2e").
		SetWeight(1)
	h3p3 := placement.NewInstance().
		SetID("h3p3").
		SetHostname("h3").
		SetPort(3).
		SetIsolationGroup("r3").
		SetZone("z1").
		SetEndpoint("h3p3e").
		SetWeight(1)

	selector := NewPortMirroredSelector(placement.NewOptions().SetValidZone("z1"))
	res, err := selector.SelectInitialInstances(
		[]placement.Instance{h1p1, h1p2, h1p3, h2p1, h2p2, h2p3, h3p1, h3p2, h3p3},
		3,
	)
	require.NoError(t, err)
	require.Equal(t, 9, len(res))
	require.Equal(t, h1p1.ShardSetID(), h2p1.ShardSetID())
	require.Equal(t, h1p1.ShardSetID(), h3p1.ShardSetID())
	require.Equal(t, h1p2.ShardSetID(), h2p2.ShardSetID())
	require.Equal(t, h1p2.ShardSetID(), h3p2.ShardSetID())
	require.Equal(t, h1p3.ShardSetID(), h2p3.ShardSetID())
	require.Equal(t, h1p3.ShardSetID(), h3p3.ShardSetID())

	ssIDs := make(map[uint32]int)
	for i := 1; i <= 3; i++ {
		ssIDs[uint32(i)] = 3
	}

	for _, instance := range res {
		ssIDs[instance.ShardSetID()] = ssIDs[instance.ShardSetID()] - 1
	}

	for _, count := range ssIDs {
		require.Equal(t, 0, count)
	}
}

func TestSelectReplaceInstanceForMirror(t *testing.T) {
	h1p1 := placement.NewInstance().
		SetID("h1p1").
		SetHostname("h1").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p1e").
		SetWeight(1).
		SetShardSetID(1)
	h1p2 := placement.NewInstance().
		SetID("h1p2").
		SetHostname("h1").
		SetPort(2).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p2e").
		SetWeight(1).
		SetShardSetID(2)
	h2p1 := placement.NewInstance().
		SetID("h2p1").
		SetHostname("h2").
		SetPort(1).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p1e").
		SetWeight(1).
		SetShardSetID(1)
	h2p2 := placement.NewInstance().
		SetID("h2p2").
		SetHostname("h2").
		SetPort(2).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p2e").
		SetWeight(1).
		SetShardSetID(2)

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{h1p1, h1p2, h2p1, h2p2}).
		SetIsMirrored(true).
		SetIsSharded(true).
		SetReplicaFactor(2)

	h3p1 := placement.NewInstance().
		SetID("h3p1").
		SetHostname("h3").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h3p1e").
		SetWeight(1)
	h3p2 := placement.NewInstance().
		SetID("h3p2").
		SetHostname("h3").
		SetPort(2).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h3p2e").
		SetWeight(1)

	selector := NewPortMirroredSelector(placement.NewOptions().SetValidZone("z1"))
	res, err := selector.SelectReplaceInstances(
		[]placement.Instance{h3p1, h3p2},
		[]string{h1p1.ID(), h1p2.ID()},
		p,
	)
	require.NoError(t, err)
	require.Equal(t, 2, len(res))
	require.Equal(t, h3p1.ShardSetID(), res[0].ShardSetID())
	require.Equal(t, h3p2.ShardSetID(), res[1].ShardSetID())

	// Isolation group conflict.
	_, err = selector.SelectReplaceInstances(
		[]placement.Instance{h3p1, h3p2},
		[]string{h2p1.ID(), h2p2.ID()},
		p,
	)
	require.Error(t, err)

	// More than 1 host.
	_, err = selector.SelectReplaceInstances(
		[]placement.Instance{h3p1, h3p2},
		[]string{h1p1.ID(), h2p1.ID()},
		p,
	)
	require.Error(t, err)

	// No matching weight.
	h3p1.SetWeight(2)
	h3p2.SetWeight(2)

	_, err = selector.SelectReplaceInstances(
		[]placement.Instance{h3p1, h3p2},
		[]string{h1p1.ID(), h1p2.ID()},
		p,
	)
	require.Error(t, err)
}

func TestSelectReplaceInstancesWithLeaving(t *testing.T) {
	s1 := shard.NewShard(1).SetState(shard.Leaving)
	h1p1 := placement.NewInstance().
		SetID("h1p1").
		SetHostname("h1").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p1e").
		SetWeight(1).
		SetShardSetID(1).
		SetShards(shard.NewShards([]shard.Shard{s1}))
	s2 := shard.NewShard(1).SetState(shard.Initializing)
	h2p1 := placement.NewInstance().
		SetID("h2p1").
		SetHostname("h2").
		SetPort(1).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p1e").
		SetWeight(1).
		SetShardSetID(1).
		SetShards(shard.NewShards([]shard.Shard{s2}))

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{h1p1, h2p1}).
		SetIsMirrored(true).
		SetIsSharded(true).
		SetReplicaFactor(1)

	selector := NewPortMirroredSelector(placement.NewOptions().SetValidZone("z1"))
	res, err := selector.SelectReplaceInstances(
		[]placement.Instance{h1p1, h2p1},
		[]string{h2p1.ID()},
		p,
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	require.Equal(t, h1p1, res[0])
}

func TestSelectAddingInstanceForMirror(t *testing.T) {
	h1p1 := placement.NewInstance().
		SetID("h1p1").
		SetHostname("h1").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p1e").
		SetWeight(1).
		SetShardSetID(1)
	h1p2 := placement.NewInstance().
		SetID("h1p2").
		SetHostname("h1").
		SetPort(2).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p2e").
		SetWeight(1).
		SetShardSetID(2)
	h2p1 := placement.NewInstance().
		SetID("h2p1").
		SetHostname("h2").
		SetPort(1).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p1e").
		SetWeight(1).
		SetShardSetID(1)
	h2p2 := placement.NewInstance().
		SetID("h2p2").
		SetHostname("h2").
		SetPort(2).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p2e").
		SetWeight(1).
		SetShardSetID(2)

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{h1p1, h1p2, h2p1, h2p2}).
		SetIsMirrored(true).
		SetIsSharded(true).
		SetReplicaFactor(2).
		SetMaxShardSetID(2)

	h3p1 := placement.NewInstance().
		SetID("h3p1").
		SetHostname("h3").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h3p1e").
		SetWeight(1)
	h3p2 := placement.NewInstance().
		SetID("h3p2").
		SetHostname("h3").
		SetPort(2).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h3p2e").
		SetWeight(1)
	h4p1 := placement.NewInstance().
		SetID("h4p1").
		SetHostname("h4").
		SetPort(1).
		SetIsolationGroup("r3").
		SetZone("z1").
		SetEndpoint("h4p1e").
		SetWeight(1)
	h4p2 := placement.NewInstance().
		SetID("h4p2").
		SetHostname("h4").
		SetPort(2).
		SetIsolationGroup("r3").
		SetZone("z1").
		SetEndpoint("h4p2e").
		SetWeight(1)

	selector := NewPortMirroredSelector(placement.NewOptions().SetValidZone("z1"))
	res, err := selector.SelectAddingInstances(
		[]placement.Instance{h3p1, h3p2, h4p1, h4p2},
		p,
	)
	require.NoError(t, err)
	require.Equal(t, 4, len(res))

	// Make sure leaving nodes are eligible for adding.
	h1p1.SetShards(shard.NewShards([]shard.Shard{shard.NewShard(0).SetState(shard.Leaving)}))
	h2p1.SetShards(shard.NewShards([]shard.Shard{shard.NewShard(0).SetState(shard.Leaving)}))

	require.Equal(t, h1p1.ShardSetID(), h2p1.ShardSetID())
	require.Equal(t, uint32(1), h1p1.ShardSetID())
	res, err = selector.SelectAddingInstances(
		[]placement.Instance{h1p1, h2p1},
		p,
	)
	require.NoError(t, err)
	require.Equal(t, 2, len(res))
	for _, instance := range res {
		require.Equal(t, uint32(1), instance.ShardSetID())
	}

	res, err = selector.SelectAddingInstances(
		[]placement.Instance{h1p1, h4p1},
		p,
	)
	require.NoError(t, err)
	require.Equal(t, 2, len(res))
	for _, instance := range res {
		require.Equal(t, uint32(3), instance.ShardSetID())
	}

	_, err = selector.SelectAddingInstances(
		[]placement.Instance{h3p2, h4p1},
		p,
	)
	require.Error(t, err)

	_, err = selector.SelectAddingInstances(
		[]placement.Instance{h3p1, h3p2},
		p,
	)
	require.Error(t, err)

	// No matching weight.
	h3p1.SetWeight(2)
	h3p2.SetWeight(2)

	_, err = selector.SelectAddingInstances(
		[]placement.Instance{h3p1, h3p2, h4p1, h4p2},
		p,
	)
	require.Error(t, err)
}

func TestSelectAddingInstanceForMirrorWithAddAllCandidates(t *testing.T) {
	h1p1 := placement.NewInstance().
		SetID("h1p1").
		SetHostname("h1").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p1e").
		SetWeight(1).
		SetShardSetID(1)
	h1p2 := placement.NewInstance().
		SetID("h1p2").
		SetHostname("h1").
		SetPort(2).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p2e").
		SetWeight(1).
		SetShardSetID(2)
	h2p1 := placement.NewInstance().
		SetID("h2p1").
		SetHostname("h2").
		SetPort(1).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p1e").
		SetWeight(1).
		SetShardSetID(1)
	h2p2 := placement.NewInstance().
		SetID("h2p2").
		SetHostname("h2").
		SetPort(2).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p2e").
		SetWeight(1).
		SetShardSetID(2)

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{h1p1, h1p2, h2p1, h2p2}).
		SetIsMirrored(true).
		SetIsSharded(true).
		SetReplicaFactor(2).
		SetMaxShardSetID(2)

	h3p1 := placement.NewInstance().
		SetID("h3p1").
		SetHostname("h3").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h3p1e").
		SetWeight(1)
	h3p2 := placement.NewInstance().
		SetID("h3p2").
		SetHostname("h3").
		SetPort(2).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h3p2e").
		SetWeight(1)
	h4p1 := placement.NewInstance().
		SetID("h4p1").
		SetHostname("h4").
		SetPort(1).
		SetIsolationGroup("r3").
		SetZone("z1").
		SetEndpoint("h4p1e").
		SetWeight(1)
	h4p2 := placement.NewInstance().
		SetID("h4p2").
		SetHostname("h4").
		SetPort(2).
		SetIsolationGroup("r3").
		SetZone("z1").
		SetEndpoint("h4p2e").
		SetWeight(1)
	h5p1 := placement.NewInstance().
		SetID("h5p1").
		SetHostname("h5").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h5p1e").
		SetWeight(1)
	h5p2 := placement.NewInstance().
		SetID("h5p2").
		SetHostname("h5").
		SetPort(2).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h5p2e").
		SetWeight(1)
	h6p1 := placement.NewInstance().
		SetID("h6p1").
		SetHostname("h6").
		SetPort(1).
		SetIsolationGroup("r3").
		SetZone("z1").
		SetEndpoint("h6p1e").
		SetWeight(1)
	h6p2 := placement.NewInstance().
		SetID("h6p2").
		SetHostname("h6").
		SetPort(2).
		SetIsolationGroup("r3").
		SetZone("z1").
		SetEndpoint("h6p2e").
		SetWeight(1)

	selector := NewPortMirroredSelector(placement.NewOptions().SetValidZone("z1").SetAddAllCandidates(true))
	res, err := selector.SelectAddingInstances(
		[]placement.Instance{h3p1, h3p2, h4p1, h4p2, h5p1, h5p2, h6p1, h6p2},
		p,
	)
	require.NoError(t, err)
	require.Equal(t, 8, len(res))
	require.Equal(t, h3p1.ShardSetID(), h4p1.ShardSetID())
	require.Equal(t, h3p2.ShardSetID(), h4p2.ShardSetID())
	require.Equal(t, h5p1.ShardSetID(), h6p1.ShardSetID())
	require.Equal(t, h5p1.ShardSetID(), h6p1.ShardSetID())
}

func TestGroupInstancesByHostPort(t *testing.T) {
	t.Run("maintains host order with 2 instances", func(t *testing.T) {
		port := uint32(5)
		i1 := newInstanceWithID("i1").SetPort(port).SetHostname("h1")
		i2 := newInstanceWithID("i2").SetPort(port).SetHostname("h2")

		hosts := [][]host{{{
			name:           "h1",
			isolationGroup: "g1",
			portToInstance: map[uint32]placement.Instance{
				port: i1,
			},
		}, {
			name:           "h2",
			isolationGroup: "g2",
			portToInstance: map[uint32]placement.Instance{
				port: i2,
			},
		}}}

		groups, err := groupInstancesByHostPort(hosts, false)
		require.NoError(t, err)

		assert.Equal(t, [][]placement.Instance{{i1, i2}}, groups)
	})

	t.Run("maintains host order with 2 instances with different ports", func(t *testing.T) {
		port1 := uint32(5)
		i1 := newInstanceWithID("i1").SetPort(port1).SetHostname("h1")
		port2 := uint32(15)
		i2 := newInstanceWithID("i2").SetPort(port2).SetHostname("h2")

		hosts := [][]host{{{
			name:           "h1",
			isolationGroup: "g1",
			portToInstance: map[uint32]placement.Instance{
				port1: i1,
			},
		}, {
			name:           "h2",
			isolationGroup: "g2",
			portToInstance: map[uint32]placement.Instance{
				port2: i2,
			},
		}}}

		groups, err := groupInstancesByHostPort(hosts, true)
		require.NoError(t, err)
		assert.Equal(t, [][]placement.Instance{{i1, i2}}, groups)

		_, err = groupInstancesByHostPort(hosts, false)
		require.NotNil(t, err)
	})
}
