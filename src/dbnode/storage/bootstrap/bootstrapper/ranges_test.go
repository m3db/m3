package bootstrapper

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/stretchr/testify/require"
)

func TestIntersectingShardTimeRanges(t *testing.T) {
	shards := []uint32{0, 1, 2, 3}
	blockSize := time.Hour
	t0 := time.Now().Truncate(blockSize)
	t1 := t0.Add(blockSize)
	t2 := t1.Add(blockSize)
	fullRange := result.NewShardTimeRangesFromRange(t0, t2, shards...)

	expectedIntersect := result.NewShardTimeRangesFromRange(t1, t2, shards...)
	intersect := IntersectingShardTimeRanges(fullRange, shards, t1, blockSize)
	require.True(t, intersect.Equal(expectedIntersect))

	// Try with non-overlapping shards.
	intersectShards := []uint32{0}
	expectedIntersect = result.NewShardTimeRangesFromRange(t1, t2, intersectShards...)
	intersect = IntersectingShardTimeRanges(fullRange, intersectShards, t1, blockSize)
	require.True(t, intersect.Equal(expectedIntersect))
}
