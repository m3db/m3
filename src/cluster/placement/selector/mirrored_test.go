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

	"github.com/m3db/m3cluster/placement"

	"github.com/stretchr/testify/require"
)

func TestFilterInitialInstancesForMirror(t *testing.T) {
	h1p1 := placement.NewInstance().
		SetID("h1p1").
		SetHostname("h1").
		SetPort(1).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h1p1e").
		SetWeight(1)
	h1p2 := placement.NewInstance().
		SetID("h1p2").
		SetHostname("h1").
		SetPort(2).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h1p2e").
		SetWeight(1)
	h2p1 := placement.NewInstance().
		SetID("h2p1").
		SetHostname("h2").
		SetPort(1).
		SetRack("r2").
		SetZone("z1").
		SetEndpoint("h2p1e").
		SetWeight(1)
	h2p2 := placement.NewInstance().
		SetID("h2p2").
		SetHostname("h2").
		SetPort(2).
		SetRack("r2").
		SetZone("z1").
		SetEndpoint("h2p2e").
		SetWeight(1)
	h3p1 := placement.NewInstance().
		SetID("h3p1").
		SetHostname("h3").
		SetPort(1).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h3p1e").
		SetWeight(1)
	h3p2 := placement.NewInstance().
		SetID("h3p2").
		SetHostname("h3").
		SetPort(2).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h3p2e").
		SetWeight(1)
	h4p1 := placement.NewInstance().
		SetID("h4p1").
		SetHostname("h4").
		SetPort(1).
		SetRack("r3").
		SetZone("z1").
		SetEndpoint("h4p1e").
		SetWeight(1)
	h4p2 := placement.NewInstance().
		SetID("h4p2").
		SetHostname("h4").
		SetPort(2).
		SetRack("r3").
		SetZone("z1").
		SetEndpoint("h4p2e").
		SetWeight(1)

	filter := newMirroredSelector(placement.NewOptions().SetValidZone("z1"))
	res, err := filter.SelectInitialInstances(
		[]placement.Instance{h1p1, h1p2, h2p1, h2p2, h3p1, h3p2, h4p1, h4p2},
		2,
	)
	require.NoError(t, err)
	require.Equal(t, 8, len(res))

	ssIDs := make(map[uint32]int)
	for i := 0; i < 4; i++ {
		ssIDs[uint32(i)] = 2
	}

	for _, instance := range res {
		ssIDs[instance.ShardSetID()] = ssIDs[instance.ShardSetID()] - 1
	}

	for _, count := range ssIDs {
		require.Equal(t, 0, count)
	}
}

func TestFilterInitialInstancesForMirrorRF2(t *testing.T) {
	h1p1 := placement.NewInstance().
		SetID("h1p1").
		SetHostname("h1").
		SetPort(1).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h1p1e").
		SetWeight(1)
	h1p2 := placement.NewInstance().
		SetID("h1p2").
		SetHostname("h1").
		SetPort(2).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h1p2e").
		SetWeight(1)
	h1p3 := placement.NewInstance().
		SetID("h1p3").
		SetHostname("h1").
		SetPort(3).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h1p3e").
		SetWeight(1)
	h2p1 := placement.NewInstance().
		SetID("h2p1").
		SetHostname("h2").
		SetPort(1).
		SetRack("r2").
		SetZone("z1").
		SetEndpoint("h2p1e").
		SetWeight(1)
	h2p2 := placement.NewInstance().
		SetID("h2p2").
		SetHostname("h2").
		SetPort(2).
		SetRack("r2").
		SetZone("z1").
		SetEndpoint("h2p2e").
		SetWeight(1)
	h2p3 := placement.NewInstance().
		SetID("h2p3").
		SetHostname("h2").
		SetPort(3).
		SetRack("r2").
		SetZone("z1").
		SetEndpoint("h2p3e").
		SetWeight(1)
	h3p1 := placement.NewInstance().
		SetID("h3p1").
		SetHostname("h3").
		SetPort(1).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h3p1e").
		SetWeight(2)
	h3p2 := placement.NewInstance().
		SetID("h3p2").
		SetHostname("h3").
		SetPort(2).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h3p2e").
		SetWeight(2)
	h3p3 := placement.NewInstance().
		SetID("h3p3").
		SetHostname("h3").
		SetPort(3).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h3p3e").
		SetWeight(2)

	filter := newMirroredSelector(placement.NewOptions().SetValidZone("z1"))
	res, err := filter.SelectInitialInstances(
		[]placement.Instance{h1p1, h1p2, h1p3, h2p1, h2p2, h2p3, h3p1, h3p2, h3p3},
		2,
	)
	require.NoError(t, err)
	require.Equal(t, 6, len(res))
	require.Equal(t, h1p1.ShardSetID(), h2p1.ShardSetID())
	require.Equal(t, h1p2.ShardSetID(), h2p2.ShardSetID())
	require.Equal(t, h1p3.ShardSetID(), h2p3.ShardSetID())
	ssIDs := make(map[uint32]int)
	for i := 0; i < 3; i++ {
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
		SetRack("r3").
		SetZone("z1").
		SetEndpoint("h4p1e").
		SetWeight(2)
	h4p2 := placement.NewInstance().
		SetID("h4p2").
		SetHostname("h4").
		SetPort(2).
		SetRack("r3").
		SetZone("z1").
		SetEndpoint("h4p2e").
		SetWeight(2)
	h4p3 := placement.NewInstance().
		SetID("h4p3").
		SetHostname("h4").
		SetPort(3).
		SetRack("r3").
		SetZone("z1").
		SetEndpoint("h4p3e").
		SetWeight(2)

	res, err = filter.SelectInitialInstances(
		[]placement.Instance{h1p1, h1p2, h1p3, h2p1, h2p2, h2p3, h3p1, h3p2, h3p3, h4p1, h4p2, h4p3},
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
	for i := 0; i < 6; i++ {
		ssIDs[uint32(i)] = 2
	}

	for _, instance := range res {
		ssIDs[instance.ShardSetID()] = ssIDs[instance.ShardSetID()] - 1
	}

	for _, count := range ssIDs {
		require.Equal(t, 0, count)
	}
}

func TestFilterInitialInstancesForMirrorRF3(t *testing.T) {
	h1p1 := placement.NewInstance().
		SetID("h1p1").
		SetHostname("h1").
		SetPort(1).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h1p1e").
		SetWeight(1)
	h1p2 := placement.NewInstance().
		SetID("h1p2").
		SetHostname("h1").
		SetPort(2).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h1p2e").
		SetWeight(1)
	h1p3 := placement.NewInstance().
		SetID("h1p3").
		SetHostname("h1").
		SetPort(3).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h1p3e").
		SetWeight(1)
	h2p1 := placement.NewInstance().
		SetID("h2p1").
		SetHostname("h2").
		SetPort(1).
		SetRack("r2").
		SetZone("z1").
		SetEndpoint("h2p1e").
		SetWeight(1)
	h2p2 := placement.NewInstance().
		SetID("h2p2").
		SetHostname("h2").
		SetPort(2).
		SetRack("r2").
		SetZone("z1").
		SetEndpoint("h2p2e").
		SetWeight(1)
	h2p3 := placement.NewInstance().
		SetID("h2p3").
		SetHostname("h2").
		SetPort(3).
		SetRack("r2").
		SetZone("z1").
		SetEndpoint("h2p3e").
		SetWeight(1)
	h3p1 := placement.NewInstance().
		SetID("h3p1").
		SetHostname("h3").
		SetPort(1).
		SetRack("r3").
		SetZone("z1").
		SetEndpoint("h3p1e").
		SetWeight(1)
	h3p2 := placement.NewInstance().
		SetID("h3p2").
		SetHostname("h3").
		SetPort(2).
		SetRack("r3").
		SetZone("z1").
		SetEndpoint("h3p2e").
		SetWeight(1)
	h3p3 := placement.NewInstance().
		SetID("h3p3").
		SetHostname("h3").
		SetPort(3).
		SetRack("r3").
		SetZone("z1").
		SetEndpoint("h3p3e").
		SetWeight(1)

	filter := newMirroredSelector(placement.NewOptions().SetValidZone("z1"))
	res, err := filter.SelectInitialInstances(
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
	for i := 0; i < 3; i++ {
		ssIDs[uint32(i)] = 3
	}

	for _, instance := range res {
		ssIDs[instance.ShardSetID()] = ssIDs[instance.ShardSetID()] - 1
	}

	for _, count := range ssIDs {
		require.Equal(t, 0, count)
	}
}
func TestFilterReplaceInstanceForMirror(t *testing.T) {
	h1p1 := placement.NewInstance().
		SetID("h1p1").
		SetHostname("h1").
		SetPort(1).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h1p1e").
		SetWeight(1).
		SetShardSetID(0)
	h1p2 := placement.NewInstance().
		SetID("h1p2").
		SetHostname("h1").
		SetPort(2).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h1p2e").
		SetWeight(1).
		SetShardSetID(1)
	h2p1 := placement.NewInstance().
		SetID("h2p1").
		SetHostname("h2").
		SetPort(1).
		SetRack("r2").
		SetZone("z1").
		SetEndpoint("h2p1e").
		SetWeight(1).
		SetShardSetID(0)
	h2p2 := placement.NewInstance().
		SetID("h2p2").
		SetHostname("h2").
		SetPort(2).
		SetRack("r2").
		SetZone("z1").
		SetEndpoint("h2p2e").
		SetWeight(1).
		SetShardSetID(1)

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{h1p1, h1p2, h2p1, h2p2}).
		SetIsMirrored(true).
		SetIsSharded(true).
		SetReplicaFactor(2)

	h3p1 := placement.NewInstance().
		SetID("h3p1").
		SetHostname("h3").
		SetPort(1).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h3p1e").
		SetWeight(1)
	h3p2 := placement.NewInstance().
		SetID("h3p2").
		SetHostname("h3").
		SetPort(2).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h3p2e").
		SetWeight(1)

	filter := newMirroredSelector(placement.NewOptions().SetValidZone("z1"))
	res, err := filter.SelectReplaceInstances(
		[]placement.Instance{h3p1, h3p2},
		[]string{h1p1.ID(), h1p2.ID()},
		p,
	)
	require.NoError(t, err)
	require.Equal(t, 2, len(res))
	require.Equal(t, h3p1.ShardSetID(), res[0].ShardSetID())
	require.Equal(t, h3p2.ShardSetID(), res[1].ShardSetID())

	// Rack conflict.
	_, err = filter.SelectReplaceInstances(
		[]placement.Instance{h3p1, h3p2},
		[]string{h2p1.ID(), h2p2.ID()},
		p,
	)
	require.Error(t, err)

	// More than 1 host.
	_, err = filter.SelectReplaceInstances(
		[]placement.Instance{h3p1, h3p2},
		[]string{h1p1.ID(), h2p1.ID()},
		p,
	)
	require.Error(t, err)

	// No matching weight.
	h3p1.SetWeight(2)
	h3p2.SetWeight(2)

	_, err = filter.SelectReplaceInstances(
		[]placement.Instance{h3p1, h3p2},
		[]string{h1p1.ID(), h1p2.ID()},
		p,
	)
	require.Error(t, err)
}

func TestFilterAddingInstanceForMirror(t *testing.T) {
	h1p1 := placement.NewInstance().
		SetID("h1p1").
		SetHostname("h1").
		SetPort(1).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h1p1e").
		SetWeight(1).
		SetShardSetID(0)
	h1p2 := placement.NewInstance().
		SetID("h1p2").
		SetHostname("h1").
		SetPort(2).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h1p2e").
		SetWeight(1).
		SetShardSetID(1)
	h2p1 := placement.NewInstance().
		SetID("h2p1").
		SetHostname("h2").
		SetPort(1).
		SetRack("r2").
		SetZone("z1").
		SetEndpoint("h2p1e").
		SetWeight(1).
		SetShardSetID(0)
	h2p2 := placement.NewInstance().
		SetID("h2p2").
		SetHostname("h2").
		SetPort(2).
		SetRack("r2").
		SetZone("z1").
		SetEndpoint("h2p2e").
		SetWeight(1).
		SetShardSetID(1)

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{h1p1, h1p2, h2p1, h2p2}).
		SetIsMirrored(true).
		SetIsSharded(true).
		SetReplicaFactor(2)

	h3p1 := placement.NewInstance().
		SetID("h3p1").
		SetHostname("h3").
		SetPort(1).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h3p1e").
		SetWeight(1)
	h3p2 := placement.NewInstance().
		SetID("h3p2").
		SetHostname("h3").
		SetPort(2).
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("h3p2e").
		SetWeight(1)
	h4p1 := placement.NewInstance().
		SetID("h4p1").
		SetHostname("h4").
		SetPort(1).
		SetRack("r3").
		SetZone("z1").
		SetEndpoint("h4p1e").
		SetWeight(1)
	h4p2 := placement.NewInstance().
		SetID("h4p2").
		SetHostname("h4").
		SetPort(2).
		SetRack("r3").
		SetZone("z1").
		SetEndpoint("h4p2e").
		SetWeight(1)

	filter := newMirroredSelector(placement.NewOptions().SetValidZone("z1"))
	res, err := filter.SelectAddingInstances(
		[]placement.Instance{h3p1, h3p2, h4p1, h4p2},
		p,
	)
	require.NoError(t, err)
	require.Equal(t, 4, len(res))

	_, err = filter.SelectAddingInstances(
		[]placement.Instance{h3p2, h4p1},
		p,
	)
	require.Error(t, err)

	_, err = filter.SelectAddingInstances(
		[]placement.Instance{h3p1, h3p2},
		p,
	)
	require.Error(t, err)

	// No matching weight.
	h3p1.SetWeight(2)
	h3p2.SetWeight(2)

	_, err = filter.SelectAddingInstances(
		[]placement.Instance{h3p1, h3p2, h4p1, h4p2},
		p,
	)
	require.Error(t, err)
}

func TestNextNShardSetIDs(t *testing.T) {
	res := nextNShardSetIDs(placement.NewPlacement(), 3)
	require.Equal(t, []uint32{0, 1, 2}, res)

	p := placement.NewPlacement()
	p.SetInstances([]placement.Instance{
		placement.NewInstance().SetID("i1").SetShardSetID(0),
		placement.NewInstance().SetID("i2").SetShardSetID(2),
	})

	res = nextNShardSetIDs(p, 3)
	require.Equal(t, []uint32{1, 3, 4}, res)
}
