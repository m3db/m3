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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShard(t *testing.T) {
	s := NewShard(1).SetState(Initializing).SetSourceID("id")
	assert.Equal(t, uint32(1), s.ID())
	assert.Equal(t, Initializing, s.State())
	assert.Equal(t, "id", s.SourceID())
}

func TestShards(t *testing.T) {
	shards := NewShards(nil)
	assert.Equal(t, 0, shards.NumShards())

	s1 := NewShard(1).SetState(Initializing).SetSourceID("id")
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
}
