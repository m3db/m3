package bootstrapper

import (
	"math"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	xtime "github.com/m3db/m3/src/x/time"
)

// ShardTimeRangesTimeWindowGroup represents all time ranges for every shard w/in a block of time.
type ShardTimeRangesTimeWindowGroup struct {
	Ranges result.ShardTimeRanges
	window xtime.Range
}

// NewShardTimeRangesTimeWindowGroups divides shard time ranges into grouped blocks.
func NewShardTimeRangesTimeWindowGroups(
	shardTimeRanges result.ShardTimeRanges,
	windowSize time.Duration,
) []ShardTimeRangesTimeWindowGroup {
	min, max := shardTimeRanges.MinMax()
	estimate := int(math.Ceil(float64(max.Sub(min)) / float64(windowSize)))
	grouped := make([]ShardTimeRangesTimeWindowGroup, 0, estimate)
	for t := min.Truncate(windowSize); t.Before(max); t = t.Add(windowSize) {
		currRange := xtime.Range{
			Start: t,
			End:   minTime(t.Add(windowSize), max),
		}

		group := make(result.ShardTimeRanges)
		for shard, tr := range shardTimeRanges {
			iter := tr.Iter()
			for iter.Next() {
				evaluateRange := iter.Value()
				intersection, intersects := evaluateRange.Intersect(currRange)
				if !intersects {
					continue
				}
				// Add to this range.
				group[shard] = group[shard].AddRange(intersection)
			}
		}

		entry := ShardTimeRangesTimeWindowGroup{
			Ranges: group,
			window: currRange,
		}

		grouped = append(grouped, entry)
	}
	return grouped
}

func minTime(x, y time.Time) time.Time {
	if x.Before(y) {
		return x
	}
	return y
}
