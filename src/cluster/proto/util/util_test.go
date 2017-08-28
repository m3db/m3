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

package util

import (
	"math"
	"testing"

	placementproto "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardStateToProtoError(t *testing.T) {
	s, err := shardStateToProto(shard.Unknown)
	assert.Error(t, err)
	assert.Equal(t, placementproto.ShardState_INITIALIZING, s)
}

func TestShardsToProto(t *testing.T) {
	shardSet := []shard.Shard{
		shard.NewShard(0).SetState(shard.Initializing).SetCutoverNanos(1234).SetCutoffNanos(5678),
		shard.NewShard(1).SetState(shard.Initializing).SetCutoverNanos(0).SetCutoffNanos(5678),
		shard.NewShard(2).SetState(shard.Initializing).SetCutoverNanos(math.MaxInt64).SetCutoffNanos(5678),
		shard.NewShard(3).SetState(shard.Initializing).SetCutoverNanos(1234).SetCutoffNanos(0),
		shard.NewShard(4).SetState(shard.Initializing).SetCutoverNanos(1234).SetCutoffNanos(math.MaxInt64),
	}
	shards := shard.NewShards(shardSet)
	proto, err := shardsToProto(shards)
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

	reconstructed, err := shard.NewShardsFromProto(proto)
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
