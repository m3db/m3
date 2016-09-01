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

package bootstrap

import (
	"bytes"
	"fmt"
	"time"

	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3x/time"
)

type result struct {
	results     ShardResults
	unfulfilled ShardTimeRanges
}

// NewResult creates a new result.
func NewResult() Result {
	return &result{
		results:     make(ShardResults),
		unfulfilled: make(ShardTimeRanges),
	}
}

func (r *result) ShardResults() ShardResults {
	return r.results
}

func (r *result) Unfulfilled() ShardTimeRanges {
	return r.unfulfilled
}

func (r *result) AddShardResult(shard uint32, result ShardResult, unfulfilled xtime.Ranges) {
	if result != nil && len(result.AllSeries()) > 0 {
		if existing, ok := r.results[shard]; ok {
			existing.AddResult(result)
		} else {
			r.results[shard] = result
		}
	}
	if unfulfilled != nil && !unfulfilled.IsEmpty() {
		r.unfulfilled[shard] = unfulfilled
	}
}

func (r *result) SetUnfulfilled(unfulfilled ShardTimeRanges) {
	r.unfulfilled = unfulfilled
}

func (r *result) AddResult(other Result) {
	if other == nil {
		return
	}
	r.results.AddResults(other.ShardResults())
	r.unfulfilled.AddRanges(other.Unfulfilled())
}

type shardResult struct {
	opts   Options
	blocks map[string]block.DatabaseSeriesBlocks
}

// NewShardResult creates a new shard result.
func NewShardResult(opts Options) ShardResult {
	return &shardResult{
		opts:   opts,
		blocks: make(map[string]block.DatabaseSeriesBlocks),
	}
}

// IsEmpty returns whether the result is empty.
func (sr *shardResult) IsEmpty() bool {
	return len(sr.blocks) == 0
}

// AddBlock adds a data block.
func (sr *shardResult) AddBlock(id string, b block.DatabaseBlock) {
	curSeries, exists := sr.blocks[id]
	if !exists {
		curSeries = block.NewDatabaseSeriesBlocks(sr.opts.GetDatabaseBlockOptions())
		sr.blocks[id] = curSeries
	}
	curSeries.AddBlock(b)
}

// AddSeries adds a single series.
func (sr *shardResult) AddSeries(id string, rawSeries block.DatabaseSeriesBlocks) {
	curSeries, exists := sr.blocks[id]
	if !exists {
		curSeries = block.NewDatabaseSeriesBlocks(sr.opts.GetDatabaseBlockOptions())
		sr.blocks[id] = curSeries
	}
	curSeries.AddSeries(rawSeries)
}

// AddResult adds a shard result.
func (sr *shardResult) AddResult(other ShardResult) {
	if other == nil {
		return
	}
	otherSeries := other.AllSeries()
	for id, rawSeries := range otherSeries {
		sr.AddSeries(id, rawSeries)
	}
}

// RemoveSeries removes a single series of blocks.
func (sr *shardResult) RemoveSeries(id string) {
	delete(sr.blocks, id)
}

// AllSeries returns all series in the map.
func (sr *shardResult) AllSeries() map[string]block.DatabaseSeriesBlocks {
	return sr.blocks
}

// Close closes a shard result.
func (sr *shardResult) Close() {
	for _, series := range sr.blocks {
		series.Close()
	}
}

// AddResults adds other shard results to the current shard results.
func (r ShardResults) AddResults(other ShardResults) {
	for shard, result := range other {
		if existing, ok := r[shard]; ok {
			existing.AddResult(result)
		} else {
			r[shard] = result
		}
	}
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

// ToUnfulfilledResult will return a result that is comprised of wholly
// unfufilled time ranges from the set of shard time ranges.
func (r ShardTimeRanges) ToUnfulfilledResult() Result {
	result := NewResult()
	for shard, ranges := range r {
		result.AddShardResult(shard, nil, ranges)
	}
	return result
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

// String returns a description of the time ranges
func (r ShardTimeRanges) String() string {
	var buf bytes.Buffer
	buf.WriteString("{")
	hasPrev := false
	for shard, ranges := range r {
		buf.WriteString(fmt.Sprintf("%d: %s", shard, ranges.String()))
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
	var buf bytes.Buffer
	buf.WriteString("{")
	hasPrev := false
	for shard, ranges := range r {
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
