// Copyright (c) 2020 Uber Technologies, Inc.
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

package result

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShardTimeRangesAdd(t *testing.T) {
	start := time.Now().Truncate(testBlockSize)
	times := []time.Time{start, start.Add(testBlockSize), start.Add(2 * testBlockSize), start.Add(3 * testBlockSize)}

	sr := []ShardTimeRanges{
		NewShardTimeRangesFromRange(times[0], times[1], 1, 2, 3),
		NewShardTimeRangesFromRange(times[1], times[2], 1, 2, 3),
		NewShardTimeRangesFromRange(times[2], times[3], 1, 2, 3),
	}
	ranges := NewShardTimeRanges()
	for _, r := range sr {
		ranges.AddRanges(r)
	}
	for i, r := range sr {
		min, max, r := r.MinMaxRange()
		require.Equal(t, r, testBlockSize)
		require.Equal(t, min, times[i])
		require.Equal(t, max, times[i+1])
	}
}

func TestShardTimeRangesIsSuperset(t *testing.T) {
	start := time.Now().Truncate(testBlockSize)

	ranges1 := createShardTimeRanges(start, testBlockSize, 3, []uint32{1, 2, 3})
	ranges2 := createShardTimeRanges(start, testBlockSize, 3, []uint32{1, 2})
	ranges3 := createShardTimeRanges(start, testBlockSize, 1, []uint32{1, 2, 3})

	require.True(t, ranges1.IsSuperset(ranges2))
	require.True(t, ranges1.IsSuperset(ranges1))
	require.True(t, ranges1.IsSuperset(ranges3))

	// Subset of shards fails superset condition.
	require.False(t, ranges2.IsSuperset(ranges1))
	// Subset of time ranges fails superset condition.
	require.False(t, ranges3.IsSuperset(ranges1))
}

func TestShardTimeRangesIsSupersetNoOverlapShards(t *testing.T) {
	start := time.Now().Truncate(testBlockSize)

	ranges1 := createShardTimeRanges(start, testBlockSize, 3, []uint32{1, 3, 5})
	ranges2 := createShardTimeRanges(start, testBlockSize, 3, []uint32{2, 4, 6})

	// Neither are supersets of each other as they have non overlapping shards.
	require.False(t, ranges1.IsSuperset(ranges2))
	require.False(t, ranges2.IsSuperset(ranges1))
}

func TestShardTimeRangesIsSupersetPartialOverlapShards(t *testing.T) {
	start := time.Now().Truncate(testBlockSize)

	ranges1 := createShardTimeRanges(start, testBlockSize, 3, []uint32{1, 3, 5})
	ranges2 := createShardTimeRanges(start, testBlockSize, 3, []uint32{3, 4, 6})

	// Neither are supersets of each other as they only have partially overlapping shards.
	require.False(t, ranges1.IsSuperset(ranges2))
	require.False(t, ranges2.IsSuperset(ranges1))
}

func TestShardTimeRangesIsSupersetNoOverlapTimeRanges(t *testing.T) {
	start := time.Now().Truncate(testBlockSize)

	ranges1 := createShardTimeRanges(start, testBlockSize, 3, []uint32{1, 3, 5})
	ranges2 := createShardTimeRanges(start.Add(testBlockSize*3), testBlockSize, 3,
		[]uint32{1, 3, 5})

	// Neither are supersets of each other as they have non overlapping time ranges.
	require.False(t, ranges1.IsSuperset(ranges2))
	require.False(t, ranges2.IsSuperset(ranges1))
}

func TestShardTimeRangesIsSupersetPartialOverlapTimeRanges(t *testing.T) {
	start := time.Now().Truncate(testBlockSize)

	ranges1 := createShardTimeRanges(start, testBlockSize, 3, []uint32{1, 3, 5})
	ranges2 := createShardTimeRanges(start.Add(testBlockSize*2), testBlockSize, 3,
		[]uint32{1, 3, 5})

	// Neither are supersets of each other as they have non overlapping time ranges.
	require.False(t, ranges1.IsSuperset(ranges2))
	require.False(t, ranges2.IsSuperset(ranges1))
}

func TestShardTimeRangesIsSupersetEmpty(t *testing.T) {
	start := time.Now().Truncate(testBlockSize)

	ranges1 := createShardTimeRanges(start, testBlockSize, 3, []uint32{1, 3, 5})
	ranges2 := NewShardTimeRanges()

	require.True(t, ranges1.IsSuperset(ranges2))
	require.False(t, ranges2.IsSuperset(ranges1))
	require.True(t, ranges2.IsSuperset(ranges2))
}

func createShardTimeRanges(
	start time.Time,
	blockSize time.Duration,
	numBlocks int,
	shards []uint32,
) ShardTimeRanges {
	ranges := NewShardTimeRanges()
	for i := 0; i < numBlocks; i++ {
		blockStart := start.Add(time.Duration(i) * blockSize)
		ranges.AddRanges(NewShardTimeRangesFromRange(
			blockStart,
			blockStart.Add(blockSize),
			shards...,
		))
	}
	return ranges
}
