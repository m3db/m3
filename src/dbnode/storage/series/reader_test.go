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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/namespace"
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
			start, onRetrieveBlock, gomock.Any()).
		Return(blockReaders[0], nil)
	retriever.EXPECT().
		Stream(ctx, ident.NewIDMatcher("foo"),
			start.Add(ropts.BlockSize()), onRetrieveBlock, gomock.Any()).
		Return(blockReaders[1], nil)

	reader := NewReaderUsingRetriever(
		ident.StringID("foo"), retriever, onRetrieveBlock, nil, opts)

	// Check reads as expected
	r, err := reader.ReadEncoded(ctx, start, end, namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 2, len(r))
	for i, readers := range r {
		require.Equal(t, 1, len(readers))
		assert.Equal(t, blockReaders[i], readers[0])
	}
}

type readerFetchBlocksTestCase struct {
	title           string
	times           []time.Time
	cachedBlocks    map[xtime.UnixNano]streamResponse
	diskBlocks      map[xtime.UnixNano]streamResponse
	bufferBlocks    map[xtime.UnixNano]block.FetchBlockResult
	expectedResults []block.FetchBlockResult
}

type streamResponse struct {
	blockReader xio.BlockReader
	err         error
}

func TestReaderUsingRetrieverFetchBlocks(t *testing.T) {
	var (
		opts      = newSeriesTestOptions()
		ropts     = opts.RetentionOptions()
		blockSize = ropts.BlockSize()
		end       = opts.ClockOptions().NowFn()().Truncate(blockSize)
		start     = end.Add(-2 * blockSize)
	)

	testCases := []readerFetchBlocksTestCase{
		{
			// Should return an empty slice if there is no data.
			title: "Handle no data",
			times: []time.Time{start},
		},
		{
			// Read one block from disk which should return an error.
			title: "Handles disk read errors",
			times: []time.Time{start},
			diskBlocks: map[xtime.UnixNano]streamResponse{
				xtime.ToUnixNano(start): streamResponse{
					err: errors.New("some-error"),
				},
			},
			expectedResults: []block.FetchBlockResult{
				{
					Start: start,
					Err:   errors.New("some-error"),
				},
			},
		},
		{
			// Read one block from the disk cache which should return an error.
			title: "Handles disk cache read errors",
			times: []time.Time{start},
			cachedBlocks: map[xtime.UnixNano]streamResponse{
				xtime.ToUnixNano(start): streamResponse{
					err: errors.New("some-error"),
				},
			},
			expectedResults: []block.FetchBlockResult{
				{
					Start: start,
					Err:   errors.New("some-error"),
				},
			},
		},
		{
			// Read one block from the buffer which should return an error.
			title: "Handles buffer read errors",
			times: []time.Time{start},
			bufferBlocks: map[xtime.UnixNano]block.FetchBlockResult{
				xtime.ToUnixNano(start): {
					Start: start,
					Err:   errors.New("some-error"),
				},
			},
			expectedResults: []block.FetchBlockResult{
				{
					Start: start,
					Err:   errors.New("some-error"),
				},
			},
		},
		{
			// Read one block from the disk cache.
			title: "Handles disk cache reads (should not query disk)",
			times: []time.Time{start},
			cachedBlocks: map[xtime.UnixNano]streamResponse{
				xtime.ToUnixNano(start): streamResponse{
					blockReader: xio.BlockReader{
						SegmentReader: xio.NewSegmentReader(ts.Segment{}),
						Start:         start,
						BlockSize:     blockSize,
					},
				},
			},
			expectedResults: []block.FetchBlockResult{
				{
					Start:  start,
					Blocks: []xio.BlockReader{xio.BlockReader{Start: start, BlockSize: blockSize}},
				},
			},
		},
		{
			// Read two blocks, each of which should be returned from disk.
			title: "Handles multiple disk reads",
			times: []time.Time{start, start.Add(blockSize)},
			diskBlocks: map[xtime.UnixNano]streamResponse{
				xtime.ToUnixNano(start): streamResponse{
					blockReader: xio.BlockReader{
						SegmentReader: xio.NewSegmentReader(ts.Segment{}),
						Start:         start,
						BlockSize:     blockSize,
					},
				},
				xtime.ToUnixNano(start.Add(blockSize)): streamResponse{
					blockReader: xio.BlockReader{
						SegmentReader: xio.NewSegmentReader(ts.Segment{}),
						Start:         start.Add(blockSize),
						BlockSize:     blockSize,
					},
				},
			},
			expectedResults: []block.FetchBlockResult{
				{
					Start:  start,
					Blocks: []xio.BlockReader{xio.BlockReader{Start: start, BlockSize: blockSize}},
				},
				{
					Start:  start.Add(blockSize),
					Blocks: []xio.BlockReader{xio.BlockReader{Start: start.Add(blockSize), BlockSize: blockSize}},
				},
			},
		},
		{
			// Read one block from the buffer.
			title: "Handles buffer reads",
			times: []time.Time{start},
			bufferBlocks: map[xtime.UnixNano]block.FetchBlockResult{
				xtime.ToUnixNano(start): {
					Start:  start,
					Blocks: []xio.BlockReader{xio.BlockReader{Start: start, BlockSize: blockSize}},
				},
			},
			expectedResults: []block.FetchBlockResult{
				{
					Start:  start,
					Blocks: []xio.BlockReader{xio.BlockReader{Start: start, BlockSize: blockSize}},
				},
			},
		},
		{
			title: "Combines data from disk cache and buffer for same blockstart",
			times: []time.Time{start},
			cachedBlocks: map[xtime.UnixNano]streamResponse{
				xtime.ToUnixNano(start): streamResponse{
					blockReader: xio.BlockReader{
						SegmentReader: xio.NewSegmentReader(ts.Segment{}),
						Start:         start,
						BlockSize:     blockSize,
					},
				},
			},
			bufferBlocks: map[xtime.UnixNano]block.FetchBlockResult{
				xtime.ToUnixNano(start): {
					Start:  start,
					Blocks: []xio.BlockReader{xio.BlockReader{Start: start, BlockSize: blockSize}},
				},
			},
			expectedResults: []block.FetchBlockResult{
				{
					Start: start,
					Blocks: []xio.BlockReader{
						// One from disk cache.
						xio.BlockReader{Start: start, BlockSize: blockSize},
						// One from buffer.
						xio.BlockReader{Start: start, BlockSize: blockSize},
					},
				},
			},
		},
		{
			title: "Combines data from disk and buffer for same blockstart",
			times: []time.Time{start},
			diskBlocks: map[xtime.UnixNano]streamResponse{
				xtime.ToUnixNano(start): streamResponse{
					blockReader: xio.BlockReader{
						SegmentReader: xio.NewSegmentReader(ts.Segment{}),
						Start:         start,
						BlockSize:     blockSize,
					},
				},
			},
			bufferBlocks: map[xtime.UnixNano]block.FetchBlockResult{
				xtime.ToUnixNano(start): {
					Start:  start,
					Blocks: []xio.BlockReader{xio.BlockReader{Start: start, BlockSize: blockSize}},
				},
			},
			expectedResults: []block.FetchBlockResult{
				{
					Start: start,
					Blocks: []xio.BlockReader{
						// One from disk.
						xio.BlockReader{Start: start, BlockSize: blockSize},
						// One from buffer.
						xio.BlockReader{Start: start, BlockSize: blockSize},
					},
				},
			},
		},
		// Both disk cache and buffer have data for same blockstart but buffer has an
		// error. The error should be propagated to the caller (not masked by the
		// valid data from the disk cache).
		{
			title: "Handles buffer and disk cache merge with buffer error for same blockstart",
			times: []time.Time{start},
			cachedBlocks: map[xtime.UnixNano]streamResponse{
				xtime.ToUnixNano(start): streamResponse{
					blockReader: xio.BlockReader{
						SegmentReader: xio.NewSegmentReader(ts.Segment{}),
						Start:         start,
						BlockSize:     blockSize,
					},
				},
			},
			bufferBlocks: map[xtime.UnixNano]block.FetchBlockResult{
				xtime.ToUnixNano(start): {
					Start: start,
					Err:   errors.New("some-error"),
				},
			},
			expectedResults: []block.FetchBlockResult{
				{
					Start: start,
					Err:   errors.New("some-error"),
				},
			},
		},
		// Both disk and buffer have data for same blockstart but buffer has an
		// error. The error should be propagated to the caller (not masked by the
		// valid data from the disk cache).
		{
			title: "Handles buffer and disk merge with buffer error for same blockstart",
			times: []time.Time{start},
			cachedBlocks: map[xtime.UnixNano]streamResponse{
				xtime.ToUnixNano(start): streamResponse{
					blockReader: xio.BlockReader{
						SegmentReader: xio.NewSegmentReader(ts.Segment{}),
						Start:         start,
						BlockSize:     blockSize,
					},
				},
			},
			bufferBlocks: map[xtime.UnixNano]block.FetchBlockResult{
				xtime.ToUnixNano(start): {
					Start: start,
					Err:   errors.New("some-error"),
				},
			},
			expectedResults: []block.FetchBlockResult{
				{
					Start: start,
					Err:   errors.New("some-error"),
				},
			},
		},
		{
			title: "Combines data from all sources for different block starts",
			times: []time.Time{start, start.Add(blockSize), start.Add(2 * blockSize)},
			// First block from cache.
			cachedBlocks: map[xtime.UnixNano]streamResponse{
				xtime.ToUnixNano(start): streamResponse{
					blockReader: xio.BlockReader{
						SegmentReader: xio.NewSegmentReader(ts.Segment{}),
						Start:         start,
						BlockSize:     blockSize,
					},
				},
			},
			// Second block from disk.
			diskBlocks: map[xtime.UnixNano]streamResponse{
				xtime.ToUnixNano(start.Add(blockSize)): streamResponse{
					blockReader: xio.BlockReader{
						SegmentReader: xio.NewSegmentReader(ts.Segment{}),
						Start:         start.Add(blockSize),
						BlockSize:     blockSize,
					},
				},
			},
			// Third block from buffer.
			bufferBlocks: map[xtime.UnixNano]block.FetchBlockResult{
				xtime.ToUnixNano(start.Add(2 * blockSize)): {
					Start:  start.Add(2 * blockSize),
					Blocks: []xio.BlockReader{xio.BlockReader{Start: start.Add(2 * blockSize), BlockSize: blockSize}},
				},
			},
			expectedResults: []block.FetchBlockResult{
				{
					Start: start,
					Blocks: []xio.BlockReader{
						xio.BlockReader{Start: start, BlockSize: blockSize},
					},
				},
				{
					Start: start.Add(blockSize),
					Blocks: []xio.BlockReader{
						xio.BlockReader{Start: start.Add(blockSize), BlockSize: blockSize},
					},
				},
				{
					Start: start.Add(2 * blockSize),
					Blocks: []xio.BlockReader{
						xio.BlockReader{Start: start.Add(2 * blockSize), BlockSize: blockSize},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var (
				onRetrieveBlock = block.NewMockOnRetrieveBlock(ctrl)
				retriever       = NewMockQueryableBlockRetriever(ctrl)
				diskCache       = block.NewMockDatabaseSeriesBlocks(ctrl)
				buffer          = NewMockdatabaseBuffer(ctrl)
				bufferReturn    []block.FetchBlockResult
			)

			ctx := opts.ContextPool().Get()
			defer ctx.Close()

			// Setup mocks.
			for _, currTime := range tc.times {
				cachedBlocks, wasInDiskCache := tc.cachedBlocks[xtime.ToUnixNano(currTime)]
				if wasInDiskCache {
					// If the data was in the disk cache then expect a read from it but don't expect
					// disk reads.
					b := block.NewMockDatabaseBlock(ctrl)
					if cachedBlocks.err != nil {
						b.EXPECT().Stream(ctx).Return(xio.BlockReader{}, cachedBlocks.err)
					} else {
						b.EXPECT().Stream(ctx).Return(cachedBlocks.blockReader, nil)
					}
					diskCache.EXPECT().BlockAt(currTime).Return(b, true)
				} else {
					// If the data was not in the disk cache then expect that and setup a query
					// for disk.
					diskCache.EXPECT().BlockAt(currTime).Return(nil, false)
					diskBlocks, ok := tc.diskBlocks[xtime.ToUnixNano(currTime)]
					if !ok {
						retriever.EXPECT().IsBlockRetrievable(currTime).Return(false)
					} else {
						retriever.EXPECT().IsBlockRetrievable(currTime).Return(true)
						if diskBlocks.err != nil {
							retriever.EXPECT().
								Stream(ctx, ident.NewIDMatcher("foo"), currTime, nil, gomock.Any()).
								Return(xio.BlockReader{}, diskBlocks.err)
						} else {
							retriever.EXPECT().
								Stream(ctx, ident.NewIDMatcher("foo"), currTime, nil, gomock.Any()).
								Return(diskBlocks.blockReader, nil)
						}
					}
				}

				// Prepare buffer response one block at a time.
				bufferBlocks, wasInBuffer := tc.bufferBlocks[xtime.ToUnixNano(currTime)]
				if wasInBuffer {
					bufferReturn = append(bufferReturn, bufferBlocks)
				}
			}

			// Expect final buffer result (batched function call).
			if len(tc.bufferBlocks) == 0 {
				buffer.EXPECT().IsEmpty().Return(true)
			} else {
				buffer.EXPECT().IsEmpty().Return(false)
				buffer.EXPECT().
					FetchBlocks(ctx, tc.times, namespace.Context{}).
					Return(bufferReturn)
			}

			reader := NewReaderUsingRetriever(
				ident.StringID("foo"), retriever, onRetrieveBlock, nil, opts)

			r, err := reader.fetchBlocksWithBlocksMapAndBuffer(ctx, tc.times, diskCache, buffer, namespace.Context{})
			require.NoError(t, err)
			require.Equal(t, len(tc.expectedResults), len(r))

			for i, result := range r {
				expectedResult := tc.expectedResults[i]
				assert.Equal(t, expectedResult.Start, result.Start)

				if expectedResult.Err != nil {
					require.True(t, strings.Contains(result.Err.Error(), expectedResult.Err.Error()))
				} else {
					require.Equal(t, len(expectedResult.Blocks), len(result.Blocks))
					for _, block := range result.Blocks {
						require.Equal(t, expectedResult.Start, block.Start)
					}
				}
			}
		})
	}
}
