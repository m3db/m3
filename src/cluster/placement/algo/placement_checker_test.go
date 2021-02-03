// Copyright (c) 2021 Uber Technologies, Inc.
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
	"testing"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"

	"github.com/stretchr/testify/require"
)

func TestPlacementChecker(t *testing.T) {
	var nowNanos int64 = 10
	pastNanos := nowNanos - 1
	futureNanos := nowNanos + 1

	i1 := newTestInstance("i1").SetShards(newTestShards(shard.Available, 1, 4, 0, pastNanos))
	i2 := newTestInstance("i2").SetShards(newTestShards(shard.Available, 1, 4, 0, pastNanos))
	i3 := newTestInstance("i3").SetShards(newTestShards(shard.Leaving, 5, 8, futureNanos, 0))
	i4 := newTestInstance("i4").SetShards(newTestShards(shard.Leaving, 5, 8, futureNanos, 0))
	i5 := newTestInstance("i5").SetShards(newTestShards(shard.Initializing, 9, 12, 0, futureNanos))
	i6 := newTestInstance("i6").SetShards(newTestShards(shard.Initializing, 9, 12, 0, futureNanos))
	p := placement.NewPlacement().SetInstances([]placement.Instance{i1, i2, i3, i4, i5, i6})

	require.True(t, globalChecker.allAvailable(p, []string{"i1", "i2"}, nowNanos))
	require.False(t, globalChecker.allAvailable(p, []string{"i1"}, nowNanos))
	require.False(t, globalChecker.allAvailable(p, []string{"i2"}, nowNanos))
	require.False(t, globalChecker.allAvailable(p, []string{"i3"}, nowNanos))
	require.False(t, globalChecker.allAvailable(p, []string{"i1,i3"}, nowNanos))
	require.False(t, globalChecker.allAvailable(p, []string{"non-existent"}, nowNanos))
	require.False(t, globalChecker.allAvailable(p, []string{}, nowNanos))

	require.True(t, localChecker.allAvailable(p, []string{"i1", "i2"}, nowNanos))
	require.True(t, localChecker.allAvailable(p, []string{"i1"}, nowNanos))
	require.True(t, localChecker.allAvailable(p, []string{"i2"}, nowNanos))
	require.False(t, localChecker.allAvailable(p, []string{"i1,i3"}, nowNanos))
	require.False(t, globalChecker.allAvailable(p, []string{"non-existent"}, nowNanos))
	require.False(t, globalChecker.allAvailable(p, []string{}, nowNanos))

	require.True(t, globalChecker.allLeaving(p, []placement.Instance{i3, i4}, nowNanos))
	require.False(t, globalChecker.allLeaving(p, []placement.Instance{i3}, nowNanos))
	require.False(t, globalChecker.allLeaving(p, []placement.Instance{i4}, nowNanos))
	require.False(t, globalChecker.allLeaving(p, []placement.Instance{i1}, nowNanos))
	require.False(t, globalChecker.allLeaving(p, []placement.Instance{i3, i6}, nowNanos))
	require.False(t, globalChecker.allLeaving(p, []placement.Instance{}, nowNanos))

	require.True(t, localChecker.allLeaving(p, []placement.Instance{i3, i4}, nowNanos))
	require.True(t, localChecker.allLeaving(p, []placement.Instance{i3}, nowNanos))
	require.True(t, localChecker.allLeaving(p, []placement.Instance{i4}, nowNanos))
	require.False(t, localChecker.allLeaving(p, []placement.Instance{i1}, nowNanos))
	require.False(t, localChecker.allLeaving(p, []placement.Instance{i3, i6}, nowNanos))
	require.False(t, localChecker.allLeaving(p, []placement.Instance{}, nowNanos))

	require.True(t, globalChecker.allInitializing(p, []string{"i5", "i6"}, nowNanos))
	require.False(t, globalChecker.allInitializing(p, []string{"i5"}, nowNanos))
	require.False(t, globalChecker.allInitializing(p, []string{"i6"}, nowNanos))
	require.False(t, globalChecker.allInitializing(p, []string{"i5,i1"}, nowNanos))
	require.False(t, globalChecker.allInitializing(p, []string{"non-existent"}, nowNanos))

	require.True(t, localChecker.allInitializing(p, []string{"i5", "i6"}, nowNanos))
	require.True(t, localChecker.allInitializing(p, []string{"i5"}, nowNanos))
	require.True(t, localChecker.allInitializing(p, []string{"i6"}, nowNanos))
	require.False(t, localChecker.allInitializing(p, []string{"i5,i1"}, nowNanos))
	require.False(t, localChecker.allInitializing(p, []string{"non-existent"}, nowNanos))
}

func newTestShards(s shard.State, minID, maxID uint32, cutoffNanos int64, cutoverNanos int64) shard.Shards {
	var shards []shard.Shard
	for id := minID; id <= maxID; id++ {
		s := shard.NewShard(id).SetState(s).SetCutoffNanos(cutoffNanos).SetCutoverNanos(cutoverNanos)
		shards = append(shards, s)
	}

	return shard.NewShards(shards)
}
