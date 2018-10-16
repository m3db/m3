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

	"github.com/stretchr/testify/require"
)

func TestGetValidCandidates(t *testing.T) {
	i1 := placement.NewInstance().SetID("i1").SetZone("z1")
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Available))

	i2 := placement.NewInstance().SetID("i2").SetZone("z1")
	i2.Shards().Add(shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2"))

	i3 := placement.NewInstance().SetID("i3").SetZone("z1")
	i3.Shards().Add(shard.NewShard(0).SetState(shard.Leaving))

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{i1, i2, i3}).
		SetIsSharded(true).
		SetReplicaFactor(2).
		SetShards([]uint32{0})

	emptyI3 := placement.NewInstance().SetID("i3").SetZone("z3")
	i4 := placement.NewInstance().SetID("i4").SetZone("z1")
	candidates := []placement.Instance{i3, emptyI3, i1, i4}
	res, err := getValidCandidates(p, candidates, placement.NewOptions().SetValidZone("z1"))
	require.NoError(t, err)
	require.Equal(t, []placement.Instance{i3, i3, i4}, res)
}
