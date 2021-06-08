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

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexResultMergeMergesExistingSegments(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := xtime.Now().Truncate(testBlockSize)

	segments := []Segment{
		NewSegment(segment.NewMockSegment(ctrl), false),
		NewSegment(segment.NewMockSegment(ctrl), false),
		NewSegment(segment.NewMockSegment(ctrl), false),
		NewSegment(segment.NewMockSegment(ctrl), false),
		NewSegment(segment.NewMockSegment(ctrl), false),
		NewSegment(segment.NewMockSegment(ctrl), false),
	}

	times := []xtime.UnixNano{start, start.Add(testBlockSize), start.Add(2 * testBlockSize)}
	tr0 := NewShardTimeRangesFromRange(times[0], times[1], 1, 2, 3)
	tr1 := NewShardTimeRangesFromRange(times[1], times[2], 1, 2, 3)

	first := NewIndexBootstrapResult()
	blk1 := NewIndexBlockByVolumeType(times[0])
	blk1.SetBlock(idxpersist.DefaultIndexVolumeType, NewIndexBlock([]Segment{segments[0]}, tr0))
	first.Add(blk1, nil)
	blk2 := NewIndexBlockByVolumeType(times[0])
	blk2.SetBlock(idxpersist.DefaultIndexVolumeType, NewIndexBlock([]Segment{segments[1]}, tr0))
	first.Add(blk2, nil)
	blk3 := NewIndexBlockByVolumeType(times[1])
	blk3.SetBlock(idxpersist.DefaultIndexVolumeType, NewIndexBlock([]Segment{segments[2], segments[3]}, tr1))
	first.Add(blk3, nil)

	second := NewIndexBootstrapResult()
	blk4 := NewIndexBlockByVolumeType(times[0])
	blk4.SetBlock(idxpersist.DefaultIndexVolumeType, NewIndexBlock([]Segment{segments[4]}, tr0))
	second.Add(blk4, nil)
	blk5 := NewIndexBlockByVolumeType(times[1])
	blk5.SetBlock(idxpersist.DefaultIndexVolumeType, NewIndexBlock([]Segment{segments[5]}, tr1))
	second.Add(blk5, nil)

	merged := MergedIndexBootstrapResult(first, second)

	expected := NewIndexBootstrapResult()
	blk6 := NewIndexBlockByVolumeType(times[0])
	blk6.SetBlock(idxpersist.DefaultIndexVolumeType, NewIndexBlock([]Segment{segments[0], segments[1], segments[4]}, tr0))
	expected.Add(blk6, nil)
	blk7 := NewIndexBlockByVolumeType(times[1])
	blk7.SetBlock(idxpersist.DefaultIndexVolumeType, NewIndexBlock([]Segment{segments[2], segments[3], segments[5]}, tr1))
	expected.Add(blk7, nil)

	assert.True(t, segmentsInResultsSame(expected.IndexResults(), merged.IndexResults()))
}

func TestIndexResultSetUnfulfilled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t0 := xtime.Now().Truncate(time.Hour)
	tn := func(i int) xtime.UnixNano {
		return t0.Add(time.Duration(i) * time.Hour)
	}
	results := NewIndexBootstrapResult()
	testRanges := NewShardTimeRangesFromRange(tn(0), tn(1), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	results.SetUnfulfilled(testRanges)
	require.Equal(t, testRanges, results.Unfulfilled())
}

func TestIndexResultAdd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t0 := xtime.Now().Truncate(time.Hour)
	tn := func(i int) xtime.UnixNano {
		return t0.Add(time.Duration(i) * time.Hour)
	}
	results := NewIndexBootstrapResult()
	testRanges := NewShardTimeRangesFromRange(tn(0), tn(1), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	results.Add(NewIndexBlockByVolumeType(0), testRanges)
	require.Equal(t, testRanges, results.Unfulfilled())
}

func TestShardTimeRangesToUnfulfilledIndexResult(t *testing.T) {
	now := xtime.Now()
	str := shardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: now,
			End:   now.Add(time.Minute),
		}),
		1: xtime.NewRanges(xtime.Range{
			Start: now.Add(3 * time.Minute),
			End:   now.Add(4 * time.Minute),
		}),
	}
	r := str.ToUnfulfilledIndexResult()
	assert.Equal(t, 0, len(r.IndexResults()))
	assert.True(t, r.Unfulfilled().Equal(str))
}

func TestIndexResultsMarkFulfilled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	iopts := namespace.NewIndexOptions().SetBlockSize(time.Hour * 2)
	t0 := xtime.Now().Truncate(2 * time.Hour)
	tn := func(i int) xtime.UnixNano {
		return t0.Add(time.Duration(i) * time.Hour)
	}
	results := make(IndexResults)

	// range checks
	require.Error(t, results.MarkFulfilled(tn(0),
		NewShardTimeRangesFromRange(tn(4), tn(6), 1), idxpersist.DefaultIndexVolumeType, iopts))
	require.Error(t, results.MarkFulfilled(tn(0),
		NewShardTimeRangesFromRange(tn(-1), tn(1), 1), idxpersist.DefaultIndexVolumeType, iopts))

	// valid add
	fulfilledRange := NewShardTimeRangesFromRange(tn(0), tn(1), 1)
	require.NoError(t, results.MarkFulfilled(tn(0), fulfilledRange, idxpersist.DefaultIndexVolumeType, iopts))
	require.Equal(t, 1, len(results))
	blkByVolumeType, ok := results[tn(0)]
	require.True(t, ok)
	require.True(t, tn(0).Equal(blkByVolumeType.blockStart))
	blk, ok := blkByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	require.True(t, ok)
	require.Equal(t, fulfilledRange, blk.fulfilled)

	// additional add for same block
	nextFulfilledRange := NewShardTimeRangesFromRange(tn(1), tn(2), 2)
	require.NoError(t, results.MarkFulfilled(tn(1), nextFulfilledRange, idxpersist.DefaultIndexVolumeType, iopts))
	require.Equal(t, 1, len(results))
	blkByVolumeType, ok = results[tn(0)]
	require.True(t, ok)
	require.True(t, tn(0).Equal(blkByVolumeType.blockStart))
	fulfilledRange.AddRanges(nextFulfilledRange)
	blk, ok = blkByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	require.True(t, ok)
	require.Equal(t, fulfilledRange, blk.fulfilled)

	// additional add for next block
	nextFulfilledRange = NewShardTimeRangesFromRange(tn(2), tn(4), 1, 2, 3)
	require.NoError(t, results.MarkFulfilled(tn(2), nextFulfilledRange, idxpersist.DefaultIndexVolumeType, iopts))
	require.Equal(t, 2, len(results))
	blkByVolumeType, ok = results[tn(2)]
	require.True(t, ok)
	require.True(t, tn(2).Equal(blkByVolumeType.blockStart))
	blk, ok = blkByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	require.True(t, ok)
	require.Equal(t, nextFulfilledRange, blk.fulfilled)
}

func segmentsInResultsSame(a, b IndexResults) bool {
	if len(a) != len(b) {
		return false
	}
	for t, blockByVolumeType := range a {
		otherBlockByVolumeType, ok := b[t]
		if !ok {
			return false
		}
		block, ok := blockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
		if !ok {
			return false
		}
		otherBlock, ok := otherBlockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
		if !ok {
			return false
		}
		if len(block.Segments()) != len(otherBlock.Segments()) {
			return false
		}
		for i, s := range block.Segments() {
			if s != otherBlock.Segments()[i] {
				return false
			}
		}
	}
	return true
}
