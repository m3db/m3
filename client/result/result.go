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

package result

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"
)

type shardResult struct {
	opts   Options
	blocks map[ts.Hash]DatabaseSeriesBlocks
}

// NewShardResult creates a new shard result.
func NewShardResult(capacity int, opts Options) ShardResult {
	return &shardResult{
		opts:   opts,
		blocks: make(map[ts.Hash]DatabaseSeriesBlocks, capacity),
	}
}

func (sr *shardResult) newBlocksLen() int {
	ropts := sr.opts.RetentionOptions()
	return int(ropts.RetentionPeriod() / ropts.BlockSize())
}

// IsEmpty returns whether the result is empty.
func (sr *shardResult) IsEmpty() bool {
	return len(sr.blocks) == 0
}

// AddBlock adds a data block.
func (sr *shardResult) AddBlock(id ts.ID, b block.DatabaseBlock) {
	curSeries, exists := sr.blocks[id.Hash()]
	if !exists {
		curSeries = DatabaseSeriesBlocks{
			ID:     id,
			Blocks: block.NewDatabaseSeriesBlocks(sr.newBlocksLen(), sr.opts.DatabaseBlockOptions()),
		}
		sr.blocks[id.Hash()] = curSeries
	}
	curSeries.Blocks.AddBlock(b)
}

// AddSeries adds a single series.
func (sr *shardResult) AddSeries(id ts.ID, rawSeries block.DatabaseSeriesBlocks) {
	curSeries, exists := sr.blocks[id.Hash()]
	if !exists {
		curSeries = DatabaseSeriesBlocks{
			ID:     id,
			Blocks: block.NewDatabaseSeriesBlocks(sr.newBlocksLen(), sr.opts.DatabaseBlockOptions()),
		}
		sr.blocks[id.Hash()] = curSeries
	}
	curSeries.Blocks.AddSeries(rawSeries)
}

// AddResult adds a shard result.
func (sr *shardResult) AddResult(other ShardResult) {
	if other == nil {
		return
	}
	otherSeries := other.AllSeries()
	for _, series := range otherSeries {
		sr.AddSeries(series.ID, series.Blocks)
	}
}

// RemoveBlockAt removes a data block at a given timestamp
func (sr *shardResult) RemoveBlockAt(id ts.ID, t time.Time) {
	curSeries, exists := sr.blocks[id.Hash()]
	if !exists {
		return
	}
	curSeries.Blocks.RemoveBlockAt(t)
}

// RemoveSeries removes a single series of blocks.
func (sr *shardResult) RemoveSeries(id ts.ID) {
	delete(sr.blocks, id.Hash())
}

// AllSeries returns all series in the map.
func (sr *shardResult) AllSeries() map[ts.Hash]DatabaseSeriesBlocks {
	return sr.blocks
}

func (sr *shardResult) BlockAt(id ts.ID, t time.Time) (block.DatabaseBlock, bool) {
	series, exists := sr.blocks[id.Hash()]
	if !exists {
		return nil, false
	}
	return series.Blocks.BlockAt(t)
}

// Close closes a shard result.
func (sr *shardResult) Close() {
	for _, series := range sr.blocks {
		series.Blocks.Close()
	}
}

// AddResults adds other shard results to the current shard results.
func (r ShardResults) AddResults(other ShardResults) {
	for shard, result := range other {
		if result == nil || len(result.AllSeries()) == 0 {
			continue
		}
		if existing, ok := r[shard]; ok {
			existing.AddResult(result)
		} else {
			r[shard] = result
		}
	}
}

// Equal returns whether another shard results is equal to the current shard results,
// will not perform a deep equal only a shallow equal of series and their block addresses.
func (r ShardResults) Equal(other ShardResults) bool {
	for shard, result := range r {
		otherResult, ok := r[shard]
		if !ok {
			return false
		}
		allSeries := result.AllSeries()
		otherAllSeries := otherResult.AllSeries()
		if len(allSeries) != len(otherAllSeries) {
			return false
		}
		for id, series := range allSeries {
			otherSeries, ok := otherAllSeries[id]
			if !ok {
				return false
			}
			allBlocks := series.Blocks.AllBlocks()
			otherAllBlocks := otherSeries.Blocks.AllBlocks()
			if len(allBlocks) != len(otherAllBlocks) {
				return false
			}
			for start, block := range allBlocks {
				otherBlock, ok := otherAllBlocks[start]
				if !ok {
					return false
				}
				// Just performing shallow equals so simply compare block addresses
				if block != otherBlock {
					return false
				}
			}
		}
	}
	return true
}

// IsEmpty returns whether the shard time ranges is empty or not.
func (r ShardTimeRanges) IsEmpty() bool {
	for _, ranges := range r {
		if !xtime.IsEmpty(ranges) {
			return false
		}
	}
	return true
}

// Equal returns whether two shard time ranges are equal.
func (r ShardTimeRanges) Equal(other ShardTimeRanges) bool {
	if len(r) != len(other) {
		return false
	}
	for shard, ranges := range r {
		otherRanges, ok := other[shard]
		if !ok {
			return false
		}
		if ranges.Len() != otherRanges.Len() {
			return false
		}
		it := ranges.Iter()
		otherIt := otherRanges.Iter()
		if it.Next() && otherIt.Next() {
			value := it.Value()
			otherValue := otherIt.Value()
			if !value.Start.Equal(otherValue.Start) ||
				!value.End.Equal(otherValue.End) {
				return false
			}
		}
	}
	return true
}

// Copy will return a copy of the current shard time ranges.
func (r ShardTimeRanges) Copy() ShardTimeRanges {
	result := make(map[uint32]xtime.Ranges, len(r))
	for shard, ranges := range r {
		result[shard] = xtime.NewRanges().AddRanges(ranges)
	}
	return result
}

// AddRanges adds other shard time ranges to the current shard time ranges.
func (r ShardTimeRanges) AddRanges(other ShardTimeRanges) {
	for shard, ranges := range other {
		if xtime.IsEmpty(ranges) {
			continue
		}
		if existing, ok := r[shard]; ok {
			r[shard] = existing.AddRanges(ranges)
		} else {
			r[shard] = ranges
		}
	}
}

// Subtract will subtract another range from the current range.
func (r ShardTimeRanges) Subtract(other ShardTimeRanges) {
	for shard, ranges := range r {
		otherRanges, ok := other[shard]
		if !ok {
			continue
		}

		subtractedRanges := ranges.RemoveRanges(otherRanges)
		if subtractedRanges.IsEmpty() {
			delete(r, shard)
		} else {
			r[shard] = subtractedRanges
		}
	}
}

// MinMax will return the very minimum time as a start and the
// maximum time as an end in the ranges.
func (r ShardTimeRanges) MinMax() (time.Time, time.Time) {
	min, max := time.Time{}, time.Time{}
	for _, ranges := range r {
		if xtime.IsEmpty(ranges) {
			continue
		}
		it := ranges.Iter()
		for it.Next() {
			curr := it.Value()
			if min.IsZero() || curr.Start.Before(min) {
				min = curr.Start
			}
			if max.IsZero() || curr.End.After(max) {
				max = curr.End
			}
		}
	}
	return min, max
}

// String returns a description of the time ranges
func (r ShardTimeRanges) String() string {
	var (
		buf    bytes.Buffer
		values []shardTimeRanges
	)
	for shard, ranges := range r {
		values = append(values, shardTimeRanges{shard: shard, value: ranges})
	}

	sort.Sort(shardTimeRangesByShard(values))
	hasPrev := false
	buf.WriteString("{")
	for _, v := range values {
		shard, ranges := v.shard, v.value
		if hasPrev {
			buf.WriteString(", ")
		}
		hasPrev = true

		buf.WriteString(fmt.Sprintf("%d: %s", shard, ranges.String()))
	}
	buf.WriteString("}")

	return buf.String()
}

// SummaryString returns a summary description of the time ranges
func (r ShardTimeRanges) SummaryString() string {
	var (
		buf    bytes.Buffer
		values []shardTimeRanges
	)
	for shard, ranges := range r {
		values = append(values, shardTimeRanges{shard: shard, value: ranges})
	}

	sort.Sort(shardTimeRangesByShard(values))
	hasPrev := false
	buf.WriteString("{")
	for _, v := range values {
		shard, ranges := v.shard, v.value
		if hasPrev {
			buf.WriteString(", ")
		}
		hasPrev = true

		var (
			duration time.Duration
			it       = ranges.Iter()
		)
		for it.Next() {
			curr := it.Value()
			duration += curr.End.Sub(curr.Start)
		}
		buf.WriteString(fmt.Sprintf("%d: %s", shard, duration.String()))
	}
	buf.WriteString("}")

	return buf.String()
}

type shardTimeRanges struct {
	shard uint32
	value xtime.Ranges
}

type shardTimeRangesByShard []shardTimeRanges

func (str shardTimeRangesByShard) Len() int      { return len(str) }
func (str shardTimeRangesByShard) Swap(i, j int) { str[i], str[j] = str[j], str[i] }
func (str shardTimeRangesByShard) Less(i, j int) bool {
	return str[i].shard < str[j].shard
}
