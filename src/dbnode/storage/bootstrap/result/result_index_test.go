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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexResultGetOrAddSegment(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	created := segment.NewMockMutableSegment(ctrl)
	allocated := 0
	opts := NewOptions().
		SetIndexMutableSegmentAllocator(func() (segment.MutableSegment, error) {
			allocated++
			return created, nil
		})

	now := time.Now()
	blockSize := time.Hour
	idxOpts := namespace.NewIndexOptions().SetBlockSize(blockSize)
	aligned := now.Truncate(blockSize)

	results := IndexResults{}
	seg, err := results.GetOrAddSegment(aligned.Add(time.Minute), idxOpts, opts)
	require.NoError(t, err)
	require.True(t, seg == created)
	require.Equal(t, 1, len(results))

	seg, err = results.GetOrAddSegment(aligned.Add(2*time.Minute), idxOpts, opts)
	require.NoError(t, err)
	require.True(t, seg == created)
	require.Equal(t, 1, len(results))

	seg, err = results.GetOrAddSegment(aligned.Add(blockSize), idxOpts, opts)
	require.NoError(t, err)
	require.True(t, seg == created)
	require.Equal(t, 2, len(results))

	// Total allocs should've only been two
	require.Equal(t, 2, allocated)
}

func TestIndexResultMergeMergesExistingSegments(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(testBlockSize)

	segments := []segment.Segment{
		segment.NewMockSegment(ctrl),
		segment.NewMockSegment(ctrl),
		segment.NewMockSegment(ctrl),
		segment.NewMockSegment(ctrl),
		segment.NewMockSegment(ctrl),
		segment.NewMockSegment(ctrl),
	}

	times := []time.Time{start, start.Add(testBlockSize), start.Add(2 * testBlockSize)}
	tr0 := NewShardTimeRanges(times[0], times[1], 1, 2, 3)
	tr1 := NewShardTimeRanges(times[1], times[2], 1, 2, 3)

	first := NewIndexBootstrapResult()
	first.Add(NewIndexBlock(times[0], []segment.Segment{segments[0]}, tr0), nil)
	first.Add(NewIndexBlock(times[0], []segment.Segment{segments[1]}, tr0), nil)
	first.Add(NewIndexBlock(times[1], []segment.Segment{segments[2], segments[3]}, tr1), nil)

	second := NewIndexBootstrapResult()
	second.Add(NewIndexBlock(times[0], []segment.Segment{segments[4]}, tr0), nil)
	second.Add(NewIndexBlock(times[1], []segment.Segment{segments[5]}, tr1), nil)

	merged := MergedIndexBootstrapResult(first, second)

	expected := NewIndexBootstrapResult()
	expected.Add(NewIndexBlock(times[0], []segment.Segment{segments[0], segments[1], segments[4]}, tr0), nil)
	expected.Add(NewIndexBlock(times[1], []segment.Segment{segments[2], segments[3], segments[5]}, tr1), nil)

	assert.True(t, segmentsInResultsSame(expected.IndexResults(), merged.IndexResults()))
}

func TestIndexResultSetUnfulfilled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t0 := time.Now().Truncate(time.Hour)
	tn := func(i int) time.Time {
		return t0.Add(time.Duration(i) * time.Hour)
	}
	results := NewIndexBootstrapResult()
	testRanges := NewShardTimeRanges(tn(0), tn(1), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	results.SetUnfulfilled(testRanges)
	require.Equal(t, testRanges, results.Unfulfilled())
}

func TestIndexResultAdd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t0 := time.Now().Truncate(time.Hour)
	tn := func(i int) time.Time {
		return t0.Add(time.Duration(i) * time.Hour)
	}
	results := NewIndexBootstrapResult()
	testRanges := NewShardTimeRanges(tn(0), tn(1), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	results.Add(IndexBlock{}, testRanges)
	require.Equal(t, testRanges, results.Unfulfilled())
}

func TestShardTimeRangesToUnfulfilledIndexResult(t *testing.T) {
	str := ShardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: time.Now(),
			End:   time.Now().Add(time.Minute),
		}),
		1: xtime.NewRanges(xtime.Range{
			Start: time.Now().Add(3 * time.Minute),
			End:   time.Now().Add(4 * time.Minute),
		}),
	}
	r := str.ToUnfulfilledIndexResult()
	assert.Equal(t, 0, len(r.IndexResults()))
	assert.True(t, r.Unfulfilled().Equal(str))
}

func TestIndexResulsMarkFulfilled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	iopts := namespace.NewIndexOptions().SetBlockSize(time.Hour * 2)
	t0 := time.Now().Truncate(2 * time.Hour)
	tn := func(i int) time.Time {
		return t0.Add(time.Duration(i) * time.Hour)
	}
	results := make(IndexResults)

	// range checks
	require.Error(t, results.MarkFulfilled(tn(0),
		NewShardTimeRanges(tn(4), tn(6), 1), iopts))
	require.Error(t, results.MarkFulfilled(tn(0),
		NewShardTimeRanges(tn(-1), tn(1), 1), iopts))

	// valid add
	fulfilledRange := NewShardTimeRanges(tn(0), tn(1), 1)
	require.NoError(t, results.MarkFulfilled(tn(0), fulfilledRange, iopts))
	require.Equal(t, 1, len(results))
	blk, ok := results[xtime.ToUnixNano(tn(0))]
	require.True(t, ok)
	require.True(t, tn(0).Equal(blk.blockStart))
	require.Equal(t, fulfilledRange, blk.fulfilled)

	// additional add for same block
	nextFulfilledRange := NewShardTimeRanges(tn(1), tn(2), 2)
	require.NoError(t, results.MarkFulfilled(tn(1), nextFulfilledRange, iopts))
	require.Equal(t, 1, len(results))
	blk, ok = results[xtime.ToUnixNano(tn(0))]
	require.True(t, ok)
	require.True(t, tn(0).Equal(blk.blockStart))
	fulfilledRange.AddRanges(nextFulfilledRange)
	require.Equal(t, fulfilledRange, blk.fulfilled)

	// additional add for next block
	nextFulfilledRange = NewShardTimeRanges(tn(2), tn(4), 1, 2, 3)
	require.NoError(t, results.MarkFulfilled(tn(2), nextFulfilledRange, iopts))
	require.Equal(t, 2, len(results))
	blk, ok = results[xtime.ToUnixNano(tn(2))]
	require.True(t, ok)
	require.True(t, tn(2).Equal(blk.blockStart))
	require.Equal(t, nextFulfilledRange, blk.fulfilled)
}

func segmentsInResultsSame(a, b IndexResults) bool {
	if len(a) != len(b) {
		return false
	}
	for t, block := range a {
		otherBlock, ok := b[t]
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
