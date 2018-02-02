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
	"testing"

	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/require"
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
