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

package builder

import (
	"fmt"
	"sort"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
)

// TestMultiSegmentsBuilderSortedBySizeDescending ensures that segments in the
// multi segments builder are in descending order by size for optimal compaction.
func TestMultiSegmentsBuilderSortedBySizeDescending(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	b := NewBuilderFromSegments(NewOptions()).(*builderFromSegments)

	numSegments := 5
	segs := make([]segment.Segment, 0, numSegments)
	for i := 1; i <= numSegments; i++ {
		pi := postings.NewMockIterator(ctrl)
		gomock.InOrder(
			pi.EXPECT().Next().Return(true),
			pi.EXPECT().Current().Return(postings.ID(0)),
			pi.EXPECT().Next().Return(false),
			pi.EXPECT().Close().Return(nil),
		)
		r := segment.NewMockReader(ctrl)
		it := index.NewIDDocIterator(r, pi)
		gomock.InOrder(
			r.EXPECT().AllDocs().Return(it, nil),
			r.EXPECT().Metadata(postings.ID(0)).Return(doc.Metadata{
				ID: []byte("foo" + strconv.Itoa(i)),
				Fields: []doc.Field{
					{
						Name:  []byte("bar"),
						Value: []byte("baz"),
					},
				},
			}, nil),
			r.EXPECT().Close().Return(nil),
		)

		seg := segment.NewMockSegment(ctrl)
		seg.EXPECT().Reader().Return(r, nil)
		seg.EXPECT().Size().Return(int64(i * 10)).AnyTimes()
		segs = append(segs, seg)
	}
	require.NoError(t, b.AddSegments(segs))

	// Sort segments in descending order by size.
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].Size() > segs[j].Size()
	})

	actualSegs := make([]segment.Segment, 0, numSegments)
	for _, segMD := range b.segments {
		actualSegs = append(actualSegs, segMD.segment)
	}

	for i, segMD := range b.segments {
		actualSize := segMD.segment.Size()
		require.Equal(t, segs[i].Size(), actualSize)

		if i > 0 {
			// Check that offset is going up.
			actualPrevOffset := b.segments[i-1].offset
			actualCurrOffset := b.segments[i].offset
			require.True(t, actualCurrOffset > actualPrevOffset,
				fmt.Sprintf("expected=(actualCurrOffset > actualPrevOffset)\n"+
					"actual=(actualCurrOffset=%d, actualPrevOffset=%d)\n",
					actualCurrOffset, actualPrevOffset))
		}
	}
}
