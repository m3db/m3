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

package shard

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShard(t *testing.T) {
	s := NewShard(1).
		SetState(Initializing).
		SetSourceID("id").
		SetCutoffNanos(1000).
		SetCutoverNanos(100).
		SetRedirectToShardID(uint32Ptr(5))

	assert.Equal(t, uint32(1), s.ID())
	assert.Equal(t, Initializing, s.State())
	assert.Equal(t, "id", s.SourceID())
	assert.Equal(t, int64(1000), s.CutoffNanos())
	assert.Equal(t, int64(100), s.CutoverNanos())
	assert.Equal(t, uint32(5), *s.RedirectToShardID())
}

func TestShardEquals(t *testing.T) {
	s := NewShard(1).SetState(Initializing).SetSourceID("id").SetCutoffNanos(1000).SetCutoverNanos(100)
	assert.True(t, s.Equals(s)) //nolint
	assert.False(t, s.Equals(NewShard(1).SetState(Initializing).SetSourceID("id").SetCutoffNanos(1000)))
	assert.False(t, s.Equals(NewShard(1).SetState(Initializing).SetSourceID("id").SetCutoverNanos(100)))
	assert.False(t, s.Equals(NewShard(1).SetState(Initializing).SetCutoffNanos(1000).SetCutoverNanos(100)))
	assert.False(t, s.Equals(NewShard(1).SetSourceID("id").SetCutoffNanos(1000).SetCutoverNanos(100)))
	assert.False(t, s.Equals(NewShard(1).SetSourceID("id").SetCutoffNanos(1000).SetCutoverNanos(100)))
	assert.False(t, s.Equals(NewShard(2).SetState(Initializing).SetSourceID("id").SetCutoffNanos(1000).SetCutoverNanos(100)))
}

func TestShardEqualsWithRedirectShardID(t *testing.T) {
	s := NewShard(1).SetRedirectToShardID(uint32Ptr(1))
	assert.True(t, s.Equals(s)) //nolint
	assert.False(t, s.Equals(NewShard(1).SetRedirectToShardID(nil)))
	assert.False(t, NewShard(1).SetRedirectToShardID(nil).Equals(s))
	assert.False(t, s.Equals(NewShard(1).SetRedirectToShardID(uint32Ptr(0))))
	assert.True(t, s.Equals(NewShard(1).SetRedirectToShardID(uint32Ptr(1))))
}

func TestShards(t *testing.T) {
	redirectToShardID := new(uint32)
	*redirectToShardID = 9

	shards := NewShards(nil)
	assert.Equal(t, 0, shards.NumShards())

	s1 := NewShard(1).SetState(Initializing).SetSourceID("id").SetRedirectToShardID(redirectToShardID)
	shards.Add(s1)
	assert.Equal(t, 1, shards.NumShards())
	assert.Equal(t, 1, shards.NumShardsForState(Initializing))
	assert.Equal(t, 0, shards.NumShardsForState(Available))
	assert.Equal(t, 0, shards.NumShardsForState(Leaving))
	assert.True(t, shards.Contains(1))
	s, ok := shards.Shard(1)
	assert.True(t, ok)
	assert.Equal(t, s1, s)
	shardsInState := shards.ShardsForState(Initializing)
	assert.NotNil(t, shardsInState)
	assert.Equal(t, []Shard{s1}, shards.All())
	assert.Equal(t, shardsInState, shards.All())

	s2 := NewShard(2).SetState(Available)
	shards.Add(s2)
	assert.Equal(t, 2, shards.NumShards())
	assert.Equal(t, 1, shards.NumShardsForState(Initializing))
	assert.Equal(t, 1, shards.NumShardsForState(Available))
	assert.Equal(t, 0, shards.NumShardsForState(Leaving))
	s, ok = shards.Shard(2)
	assert.True(t, ok)
	assert.Equal(t, s2, s)

	shards.Remove(1)
	assert.False(t, shards.Contains(1))
	_, ok = shards.Shard(1)
	assert.False(t, ok)

	shards = NewShards([]Shard{s1, s2})
	assert.Equal(t, 2, shards.NumShards())

	shards.Add(NewShard(3).SetState(Leaving))
	assert.Equal(t, "[Initializing=[1 -> 9], Available=[2], Leaving=[3]]", shards.String())

	shards.Add(NewShard(4))
	shards.Add(NewShard(4))
	shards.Add(NewShard(4))
	assert.Equal(t, 4, shards.NumShards())
}

func TestShardsEquals(t *testing.T) {
	ss1 := NewShards(nil)
	ss2 := NewShards(nil)

	s1 := NewShard(1).SetState(Initializing).SetSourceID("id")
	s2 := NewShard(2).SetState(Available)
	ss1.Add(s1)
	ss2.Add(s2)
	assert.False(t, ss1.Equals(ss2))
	ss2.Add(s1)
	assert.False(t, ss1.Equals(ss2))

	ss2.Remove(s2.ID())
	assert.True(t, ss1.Equals(ss2))
}

func TestSort(t *testing.T) {
	var shards []Shard
	shards = append(shards, NewShard(1))
	shards = append(shards, NewShard(2))
	shards = append(shards, NewShard(0))
	shards = append(shards, NewShard(3))

	shardsAreSorted(t, NewShards(shards))
}

func TestShardCutoverTimes(t *testing.T) {
	s := NewShard(1).SetState(Initializing).SetSourceID("id")
	inputs := []struct {
		actualNanos   int64
		expectedNanos int64
	}{
		{actualNanos: 0, expectedNanos: 0},
		{actualNanos: 12345, expectedNanos: 12345},
		{actualNanos: math.MaxInt64, expectedNanos: math.MaxInt64},
	}

	for _, input := range inputs {
		s.SetCutoverNanos(input.actualNanos)
		require.Equal(t, input.expectedNanos, s.CutoverNanos())
	}
}

func TestShardCutoffTimes(t *testing.T) {
	s := NewShard(1).SetState(Initializing).SetSourceID("id").(*shard)
	inputs := []struct {
		actualNanos   int64
		expectedNanos int64
		storedNanos   int64
	}{
		{actualNanos: 0, expectedNanos: math.MaxInt64, storedNanos: 0},
		{actualNanos: 12345, expectedNanos: 12345, storedNanos: 12345},
		{actualNanos: math.MaxInt64, expectedNanos: math.MaxInt64, storedNanos: 0},
	}

	for _, input := range inputs {
		s.SetCutoffNanos(input.actualNanos)
		require.Equal(t, input.expectedNanos, s.CutoffNanos())
		require.Equal(t, input.storedNanos, s.cutoffNanos)
	}
}

func TestShardStateToProtoError(t *testing.T) {
	_, err := Unknown.Proto()
	assert.Error(t, err)
}

func TestShardsToProto(t *testing.T) {
	shardSet := []Shard{
		NewShard(0).SetState(Initializing).SetCutoverNanos(1234).SetCutoffNanos(5678),
		NewShard(1).SetState(Initializing).SetCutoverNanos(0).SetCutoffNanos(5678),
		NewShard(2).SetState(Initializing).SetCutoverNanos(math.MaxInt64).SetCutoffNanos(5678),
		NewShard(3).SetState(Initializing).SetCutoverNanos(1234).SetCutoffNanos(0),
		NewShard(4).SetState(Initializing).SetCutoverNanos(1234).SetCutoffNanos(math.MaxInt64),
	}
	shards := NewShards(shardSet)
	proto, err := shards.Proto()
	require.NoError(t, err)

	expected := []struct {
		cutoverNanos int64
		cutoffNanos  int64
	}{
		{cutoverNanos: 1234, cutoffNanos: 5678},
		{cutoverNanos: 0, cutoffNanos: 5678},
		{cutoverNanos: math.MaxInt64, cutoffNanos: 5678},
		{cutoverNanos: 1234, cutoffNanos: 0},
		{cutoverNanos: 1234, cutoffNanos: 0},
	}
	for i, shardProto := range proto {
		require.Equal(t, expected[i].cutoverNanos, shardProto.CutoverNanos)
		require.Equal(t, expected[i].cutoffNanos, shardProto.CutoffNanos)
	}

	reconstructed, err := NewShardsFromProto(proto)
	require.NoError(t, err)
	require.Equal(t, shards.NumShards(), reconstructed.NumShards())
	for i := 0; i < shards.NumShards(); i++ {
		shardID := uint32(i)
		expected, found := shards.Shard(shardID)
		require.True(t, found)
		actual, found := shards.Shard(shardID)
		require.True(t, found)
		require.Equal(t, expected, actual)
	}
}

func TestClone(t *testing.T) {
	s1 := NewShard(1).SetState(Initializing).SetSourceID("s").SetCutoffNanos(123).SetCutoverNanos(100)
	s2 := NewShard(2).SetState(Available)

	ss1 := NewShards([]Shard{s1, s2})
	ss2 := NewShards([]Shard{s1, s2})
	require.True(t, ss1.Clone().Equals(ss2))

	ss1.Add(NewShard(2).SetState(Leaving))
	require.False(t, ss1.Equals(ss2))
}

func TestCloneCopiesToShardMap(t *testing.T) {
	s := NewShard(1).SetState(Available)

	ss := NewShards([]Shard{s})
	clonedSS := ss.Clone()
	require.True(t, ss.Equals(clonedSS))

	s.SetState(Leaving)

	clonedS, ok := clonedSS.Shard(1)
	require.True(t, ok)
	assert.Equal(t, Available, clonedS.State())
}

func TestShardAdd(t *testing.T) {
	for i := 1; i < 500; i++ {
		rndShards := makeTestShards(i)
		shards := NewShards(nil)
		for j := 0; j < len(rndShards); j++ {
			id := rndShards[j].ID()
			require.False(t, shards.Contains(id))

			shards.Add(rndShards[j])
			require.True(t, shards.Contains(id))

			shrd, ok := shards.Shard(id)
			require.True(t, ok)
			require.Equal(t, id, shrd.ID())
		}
		shardsAreSorted(t, shards)
	}
}

func TestShardRemove(t *testing.T) {
	for i := 1; i < 500; i++ {
		rndShards := makeTestShards(i)
		shards := NewShards(rndShards)
		for j := 0; j < len(rndShards); j++ {
			id := rndShards[j].ID()
			require.True(t, shards.Contains(id))
			shards.Remove(id)
			require.Equal(t, len(rndShards)-j-1, shards.NumShards())
			require.False(t, shards.Contains(id))
		}
		shardsAreSorted(t, shards)
	}
}

func randomIDs(seed int64, num int) []uint32 {
	rnd := rand.New(rand.NewSource(seed)) // #nosec
	ids := make([]uint32, num)

	for i := uint32(0); i < uint32(num); i++ {
		ids[i] = i
	}

	rnd.Shuffle(len(ids), func(i, j int) {
		ids[i], ids[j] = ids[j], ids[i]
	})
	return ids
}

func makeTestShards(num int) []Shard {
	shardIDs := randomIDs(0, num)
	s := make([]Shard, num)
	for i, shardID := range shardIDs {
		s[i] = NewShard(shardID)
	}
	return s
}

func shardsAreSorted(t *testing.T, shards Shards) {
	prev := -1
	for _, shard := range shards.All() {
		id := int(shard.ID())
		if id <= prev {
			t.Fatalf("expected id to be greater than %d, got %d", prev, id)
		}
		prev = id
	}
}

func uint32Ptr(value uint32) *uint32 {
	ptr := new(uint32)
	*ptr = value
	return ptr
}
