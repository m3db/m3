// Copyright (c) 2018 Uber Technologies, Inc.
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

	xtime "github.com/m3db/m3x/time"
)

// NewShardTimeRanges returns a new ShardTimeRanges with provided shards and time range.
func NewShardTimeRanges(start, end time.Time, shards ...uint32) ShardTimeRanges {
	timeRange := xtime.NewRanges(xtime.Range{start, end})
	ranges := make(map[uint32]xtime.Ranges)
	for _, s := range shards {
		ranges[s] = timeRange
	}
	return ranges
}

// IsEmpty returns whether the shard time ranges is empty or not.
func (r ShardTimeRanges) IsEmpty() bool {
	for _, ranges := range r {
		if !ranges.IsEmpty() {
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
		result[shard] = xtime.Ranges{}.AddRanges(ranges)
	}
	return result
}

// AddRanges adds other shard time ranges to the current shard time ranges.
func (r ShardTimeRanges) AddRanges(other ShardTimeRanges) {
	for shard, ranges := range other {
		if ranges.IsEmpty() {
			continue
		}
		if existing, ok := r[shard]; ok {
			r[shard] = existing.AddRanges(ranges)
		} else {
			r[shard] = ranges
		}
	}
}

// ToUnfulfilledDataResult will return a result that is comprised of wholly
// unfufilled time ranges from the set of shard time ranges.
func (r ShardTimeRanges) ToUnfulfilledDataResult() DataBootstrapResult {
	result := NewDataBootstrapResult()
	for shard, ranges := range r {
		result.Add(shard, nil, ranges)
	}
	return result
}

// ToUnfulfilledIndexResult will return a result that is comprised of wholly
// unfufilled time ranges from the set of shard time ranges.
func (r ShardTimeRanges) ToUnfulfilledIndexResult() IndexBootstrapResult {
	result := NewIndexBootstrapResult()
	result.SetUnfulfilled(r)
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

// MinMax will return the very minimum time as a start and the
// maximum time as an end in the ranges.
func (r ShardTimeRanges) MinMax() (time.Time, time.Time) {
	min, max := time.Time{}, time.Time{}
	for _, ranges := range r {
		if ranges.IsEmpty() {
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

type summaryFn func(xtime.Ranges) string

func (r ShardTimeRanges) summarize(sfn summaryFn) string {
	values := make([]shardTimeRanges, 0, len(r))
	for shard, ranges := range r {
		values = append(values, shardTimeRanges{shard: shard, value: ranges})
	}
	sort.Sort(shardTimeRangesByShard(values))

	var (
		buf     bytes.Buffer
		hasPrev = false
	)

	buf.WriteString("{")
	for _, v := range values {
		shard, ranges := v.shard, v.value
		if hasPrev {
			buf.WriteString(", ")
		}
		hasPrev = true

		buf.WriteString(fmt.Sprintf("%d: %s", shard, sfn(ranges)))
	}
	buf.WriteString("}")

	return buf.String()
}

// String returns a description of the time ranges
func (r ShardTimeRanges) String() string {
	return r.summarize(xtime.Ranges.String)
}

func rangesDuration(ranges xtime.Ranges) string {
	var (
		duration time.Duration
		it       = ranges.Iter()
	)
	for it.Next() {
		curr := it.Value()
		duration += curr.End.Sub(curr.Start)
	}
	return duration.String()
}

// SummaryString returns a summary description of the time ranges
func (r ShardTimeRanges) SummaryString() string {
	return r.summarize(rangesDuration)
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
