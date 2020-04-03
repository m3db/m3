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
