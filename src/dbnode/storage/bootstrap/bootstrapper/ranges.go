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

		group := result.NewShardTimeRanges()
		for shard, tr := range shardTimeRanges.Iter() {
			iter := tr.Iter()
			for iter.Next() {
				evaluateRange := iter.Value()
				intersection, intersects := evaluateRange.Intersect(currRange)
				if !intersects {
					continue
				}
				// Add to this range.
				group.GetOrAdd(shard).AddRange(intersection)
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

// IntersectingShardTimeRanges gets intersecting shard time ranges across shards
// for requested shard time ranges and a given block start x block size.
func IntersectingShardTimeRanges(
	shardTimeRanges result.ShardTimeRanges,
	shards []uint32,
	blockStart time.Time,
	blockSize time.Duration,
) result.ShardTimeRanges {
	blockRange := xtime.Range{
		Start: blockStart,
		End:   blockStart.Add(blockSize),
	}
	willFulfill := result.NewShardTimeRanges()
	for _, shard := range shards {
		tr, ok := shardTimeRanges.Get(shard)
		if !ok {
			// No ranges match for this shard.
			continue
		}
		if _, ok := willFulfill.Get(shard); !ok {
			willFulfill.Set(shard, xtime.NewRanges())
		}

		iter := tr.Iter()
		for iter.Next() {
			curr := iter.Value()
			intersection, intersects := curr.Intersect(blockRange)
			if !intersects {
				continue
			}
			willFulfill.GetOrAdd(shard).AddRange(intersection)
		}
	}
	return willFulfill
}
