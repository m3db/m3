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
	times := []time.Time{start, start.Add(testBlockSize), start.Add(2 * testBlockSize), start.Add(3 * testBlockSize)}

	sr1 := []ShardTimeRanges{
		NewShardTimeRangesFromRange(times[0], times[1], 1, 2, 3),
		NewShardTimeRangesFromRange(times[1], times[2], 1, 2, 3),
		NewShardTimeRangesFromRange(times[2], times[3], 1, 2, 3),
	}
	ranges1 := NewShardTimeRanges()
	for _, r := range sr1 {
		ranges1.AddRanges(r)
	}
	sr2 := []ShardTimeRanges{
		NewShardTimeRangesFromRange(times[0], times[1], 2),
		NewShardTimeRangesFromRange(times[1], times[2], 1, 2),
		NewShardTimeRangesFromRange(times[2], times[3], 1),
	}
	ranges2 := NewShardTimeRanges()
	for _, r := range sr2 {
		ranges2.AddRanges(r)
	}
	sr3 := []ShardTimeRanges{
		NewShardTimeRangesFromRange(times[1], times[2], 1, 2, 3),
	}
	ranges3 := NewShardTimeRanges()
	for _, r := range sr3 {
		ranges3.AddRanges(r)
	}

	require.True(t, ranges1.IsSuperset(ranges2))
	require.True(t, ranges1.IsSuperset(ranges1))
	require.True(t, ranges1.IsSuperset(ranges3))

	// Reverse sanity checks.
	require.False(t, ranges2.IsSuperset(ranges1))
	require.False(t, ranges3.IsSuperset(ranges1))

	// Added some more false checks for non overlapping time ranges and no time ranges.
	sr1 = []ShardTimeRanges{
		NewShardTimeRangesFromRange(times[0], times[1], 1, 2, 3),
	}
	ranges1 = NewShardTimeRanges()
	for _, r := range sr1 {
		ranges1.AddRanges(r)
	}
	sr2 = []ShardTimeRanges{
		NewShardTimeRangesFromRange(times[1], times[2], 1, 2, 3),
	}
	ranges2 = NewShardTimeRanges()
	for _, r := range sr2 {
		ranges2.AddRanges(r)
	}
	ranges3 = NewShardTimeRanges()

	require.False(t, ranges2.IsSuperset(ranges1))
	require.False(t, ranges1.IsSuperset(ranges2))
	require.True(t, ranges2.IsSuperset(ranges3))
	require.False(t, ranges3.IsSuperset(ranges1))
	require.False(t, ranges3.IsSuperset(ranges2))
}
