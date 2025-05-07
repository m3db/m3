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

package sharding

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/x/ident"
)

func TestShardSet(t *testing.T) {
	ss, err := NewShardSet(
		NewShards([]uint32{1, 1}, shard.Available),
		func(id ident.ID) uint32 {
			return 0
		})
	require.Equal(t, ErrDuplicateShards, err)
	require.Nil(t, ss)

	staticShard := uint32(1)
	ss, err = NewShardSet(
		NewShards([]uint32{1, 5, 3}, shard.Available),
		func(id ident.ID) uint32 {
			return staticShard
		})
	require.NoError(t, err)
	require.NotNil(t, ss)
	require.Equal(t, []uint32{1, 5, 3}, ss.AllIDs())
	require.Equal(t, uint32(1), ss.Min())
	require.Equal(t, uint32(5), ss.Max())

	id := ident.StringID("bla")
	s := ss.Lookup(id)
	require.Equal(t, staticShard, s)
	fn := ss.HashFn()
	require.Equal(t, staticShard, fn(id))
}

func TestLookupShardState(t *testing.T) {
	staticShard := uint32(1)
	ss, err := NewShardSet(
		NewShards([]uint32{1, 5, 3}, shard.Available),
		func(id ident.ID) uint32 {
			return staticShard
		})
	require.NoError(t, err)

	shardOneState, err := ss.LookupStateByID(1)
	require.NoError(t, err)
	require.Equal(t, shard.Available, shardOneState)

	var noState shard.State
	shardTwoState, err := ss.LookupStateByID(2)
	require.Equal(t, ErrInvalidShardID, err)
	require.Equal(t, noState, shardTwoState)
}

func TestEmptyShardSet(t *testing.T) {
	ss := NewEmptyShardSet(func(id ident.ID) uint32 { return 0 })
	require.NotNil(t, ss)
	require.Empty(t, ss.All())
	require.Empty(t, ss.AllIDs())
	require.Equal(t, uint32(math.MaxUint32), ss.Min())
	require.Equal(t, uint32(0), ss.Max())

	id := ident.StringID("test")
	shardID := ss.Lookup(id)
	require.Equal(t, uint32(0), shardID)

	_, err := ss.LookupShard(1)
	require.Equal(t, ErrInvalidShardID, err)

	_, err = ss.LookupStateByID(1)
	require.Equal(t, ErrInvalidShardID, err)
}

func TestHashFunctions(t *testing.T) {
	defaultFn := DefaultHashFn(10)
	id := ident.StringID("test")
	hash := defaultFn(id)
	require.True(t, hash < 10)

	seed := uint32(42)
	hashFn := NewHashFn(10, seed)
	hash1 := hashFn(id)
	hash2 := hashFn(id)
	require.Equal(t, hash1, hash2)
	require.True(t, hash1 < 10)

	hashGen := NewHashGenWithSeed(seed)
	hashFn = hashGen(10)
	hash1 = hashFn(id)
	hash2 = hashFn(id)
	require.Equal(t, hash1, hash2)
	require.True(t, hash1 < 10)
}

func TestShardOperations(t *testing.T) {
	states := []shard.State{
		shard.Available,
		shard.Initializing,
		shard.Leaving,
	}
	for _, state := range states {
		shards := NewShards([]uint32{1, 2, 3}, state)
		require.Len(t, shards, 3)
		for _, s := range shards {
			require.Equal(t, state, s.State())
		}
	}

	shards := NewShards([]uint32{1, 2, 3}, shard.Available)
	ids := IDs(shards)
	require.Equal(t, []uint32{1, 2, 3}, ids)
}

func TestShardSetEdgeCases(t *testing.T) {
	ss, err := NewShardSet(
		NewShards([]uint32{1}, shard.Available),
		func(id ident.ID) uint32 { return 1 },
	)
	require.NoError(t, err)
	require.Equal(t, uint32(1), ss.Min())
	require.Equal(t, uint32(1), ss.Max())

	ss, err = NewShardSet(
		NewShards([]uint32{0, math.MaxUint32}, shard.Available),
		func(id ident.ID) uint32 { return 0 },
	)
	require.NoError(t, err)
	require.Equal(t, uint32(0), ss.Min())
	require.Equal(t, uint32(math.MaxUint32), ss.Max())

	ss, err = NewShardSet(
		NewShards([]uint32{0}, shard.Available),
		func(id ident.ID) uint32 { return 0 },
	)
	require.NoError(t, err)
	require.Equal(t, uint32(0), ss.Min())
	require.Equal(t, uint32(0), ss.Max())
}

func TestShardSetValidation(t *testing.T) {
	ss, err := NewShardSet(nil, func(id ident.ID) uint32 { return 0 })
	require.NoError(t, err)
	require.NotNil(t, ss)

	ss, err = NewShardSet([]shard.Shard{}, func(id ident.ID) uint32 { return 0 })
	require.NoError(t, err)
	require.NotNil(t, ss)

	ss, err = NewShardSet(
		NewShards([]uint32{1, 2, 3}, shard.Available),
		func(id ident.ID) uint32 { return 0 },
	)
	require.NoError(t, err)

	_, err = ss.LookupShard(999)
	require.Equal(t, ErrInvalidShardID, err)

	_, err = ss.LookupStateByID(999)
	require.Equal(t, ErrInvalidShardID, err)
}
