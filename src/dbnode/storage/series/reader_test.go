// Copyright (c) 2017 Uber Technologies, Inc.
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

package series

import (
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/storage/block"
	"github.com/m3db/m3db/src/dbnode/x/xio"
	"github.com/m3db/m3x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReaderUsingRetrieverReadEncoded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	ropts := opts.RetentionOptions()

	end := opts.ClockOptions().NowFn()().Truncate(ropts.BlockSize())
	start := end.Add(-2 * ropts.BlockSize())

	onRetrieveBlock := block.NewMockOnRetrieveBlock(ctrl)

	retriever := NewMockQueryableBlockRetriever(ctrl)
	retriever.EXPECT().IsBlockRetrievable(start).Return(true)
	retriever.EXPECT().IsBlockRetrievable(start.Add(ropts.BlockSize())).Return(true)

	var blockReaders []xio.BlockReader
	for i := 0; i < 2; i++ {
		reader := xio.NewMockSegmentReader(ctrl)
		blockReaders = append(blockReaders, xio.BlockReader{
			SegmentReader: reader,
		})
	}

	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	retriever.EXPECT().
		Stream(ctx, ident.NewIDMatcher("foo"),
			start, onRetrieveBlock).
		Return(blockReaders[0], nil)
	retriever.EXPECT().
		Stream(ctx, ident.NewIDMatcher("foo"),
			start.Add(ropts.BlockSize()), onRetrieveBlock).
		Return(blockReaders[1], nil)

	reader := NewReaderUsingRetriever(
		ident.StringID("foo"), retriever, onRetrieveBlock, nil, opts)

	// Check reads as expected
	r, err := reader.ReadEncoded(ctx, start, end)
	require.NoError(t, err)
	require.Equal(t, 2, len(r))
	for i, readers := range r {
		require.Equal(t, 1, len(readers))
		assert.Equal(t, blockReaders[i], readers[0])
	}
}

func TestReaderUsingRetrieverFetchBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	ropts := opts.RetentionOptions()

	end := opts.ClockOptions().NowFn()().Truncate(ropts.BlockSize())
	start := end.Add(-2 * ropts.BlockSize())

	onRetrieveBlock := block.NewMockOnRetrieveBlock(ctrl)

	retriever := NewMockQueryableBlockRetriever(ctrl)
	retriever.EXPECT().IsBlockRetrievable(start).Return(true)
	retriever.EXPECT().IsBlockRetrievable(start.Add(ropts.BlockSize())).Return(true)

	var blockReaders []xio.BlockReader
	for i := 0; i < 2; i++ {
		reader := xio.NewMockSegmentReader(ctrl)

		block := xio.BlockReader{
			SegmentReader: reader,
			Start:         start,
			BlockSize:     ropts.BlockSize() * time.Duration(i+1),
		}

		blockReaders = append(blockReaders, block)
	}

	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	retriever.EXPECT().
		Stream(ctx, ident.NewIDMatcher("foo"),
			start, nil).
		Return(blockReaders[0], nil)
	retriever.EXPECT().
		Stream(ctx, ident.NewIDMatcher("foo"),
			start.Add(ropts.BlockSize()), nil).
		Return(blockReaders[1], nil)

	reader := NewReaderUsingRetriever(
		ident.StringID("foo"), retriever, onRetrieveBlock, nil, opts)

	// Check reads as expected
	times := []time.Time{
		start,
		start.Add(ropts.BlockSize()),
	}

	r, err := reader.FetchBlocks(ctx, times)
	require.NoError(t, err)
	require.Equal(t, 2, len(r))

	for i, result := range r {
		assert.Equal(t, times[i], result.Start)
		require.Equal(t, 1, len(result.Blocks))
		br := result.Blocks[0]
		assert.Equal(t, ropts.BlockSize()*time.Duration(i+1), br.BlockSize)
		assert.Equal(t, blockReaders[i], br)
	}
}
