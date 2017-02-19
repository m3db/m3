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

package fs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSeekerManagerCacheShardIndices(t *testing.T) {
	shards := []uint32{2, 5, 9, 478, 1023}
	m := NewSeekerManager(nil, NewOptions()).(*seekerManager)
	var byTimes []*seekersByTime
	m.openAnyUnopenSeekersFn = func(byTime *seekersByTime) error {
		byTimes = append(byTimes, byTime)
		return nil
	}

	require.NoError(t, m.CacheShardIndices(shards))

	// Assert captured byTime objects match expectations
	require.Equal(t, len(shards), len(byTimes))
	for idx, shard := range shards {
		byTimes[idx].shard = shard
	}

	// Assert seeksByShardIdx match expectations
	shardSet := make(map[uint32]struct{}, len(shards))
	for _, shard := range shards {
		shardSet[shard] = struct{}{}
	}
	for shard, byTime := range m.seekersByShardIdx {
		_, exists := shardSet[uint32(shard)]
		if !exists {
			require.False(t, byTime.accessed)
		} else {
			require.True(t, byTime.accessed)
			require.Equal(t, uint32(shard), byTime.shard)
		}
	}
}
