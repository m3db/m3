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

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3ninx/index/segment"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
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

	times := []time.Time{start, start.Add(testBlockSize)}

	first := NewIndexBootstrapResult()
	first.Add(NewIndexBlock(times[0], []segment.Segment{segments[0]}), nil)
	first.Add(NewIndexBlock(times[0], []segment.Segment{segments[1]}), nil)
	first.Add(NewIndexBlock(times[1], []segment.Segment{segments[2], segments[3]}), nil)

	second := NewIndexBootstrapResult()
	second.Add(NewIndexBlock(times[0], []segment.Segment{segments[4]}), nil)
	second.Add(NewIndexBlock(times[1], []segment.Segment{segments[5]}), nil)

	merged := MergedIndexBootstrapResult(first, second)

	expected := NewIndexBootstrapResult()
	expected.Add(NewIndexBlock(times[0], []segment.Segment{segments[0], segments[1], segments[4]}), nil)
	expected.Add(NewIndexBlock(times[1], []segment.Segment{segments[2], segments[3], segments[5]}), nil)

	assert.True(t, segmentsInResultsSame(expected.IndexResults(), merged.IndexResults()))
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
