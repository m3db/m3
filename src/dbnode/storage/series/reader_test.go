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
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReaderUsingRetrieverReadEncoded(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	ropts := opts.RetentionOptions()

	end := opts.ClockOptions().NowFn()().Truncate(ropts.BlockSize())
	start := end.Add(-2 * ropts.BlockSize())

	onRetrieveBlock := block.NewMockOnRetrieveBlock(ctrl)

	retriever := NewMockQueryableBlockRetriever(ctrl)
	retriever.EXPECT().IsBlockRetrievable(start).Return(true, nil)
	retriever.EXPECT().IsBlockRetrievable(start.Add(ropts.BlockSize())).Return(true, nil)

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

func TestReaderUsingRetrieverWideEntrysBlockInvalid(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	retriever := NewMockQueryableBlockRetriever(ctrl)
	reader := NewReaderUsingRetriever(
		ident.StringID("foo"), retriever, nil, nil, opts)

	retriever.EXPECT().IsBlockRetrievable(gomock.Any()).
		Return(false, errors.New("err"))
	_, err := reader.FetchWideEntry(ctx, time.Now(), nil, namespace.Context{})
	assert.EqualError(t, err, "err")

	retriever.EXPECT().IsBlockRetrievable(gomock.Any()).Return(false, nil)
	e, err := reader.FetchWideEntry(ctx, time.Now(), nil, namespace.Context{})
	assert.NoError(t, err)

	entry, err := e.RetrieveWideEntry()
	require.NoError(t, err)
	assert.Equal(t, int64(0), entry.MetadataChecksum)
	assert.Nil(t, entry.ID)
}

func TestReaderUsingRetrieverWideEntrys(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	ropts := opts.RetentionOptions()

	end := opts.ClockOptions().NowFn()().Truncate(ropts.BlockSize())
	alignedStart := end.Add(-2 * ropts.BlockSize())

	retriever := NewMockQueryableBlockRetriever(ctrl)
	retriever.EXPECT().IsBlockRetrievable(alignedStart).Return(true, nil).Times(2)

	streamedEntry := block.NewMockStreamedWideEntry(ctrl)
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	retriever.EXPECT().
		StreamWideEntry(ctx, ident.NewIDMatcher("foo"),
			alignedStart, nil, gomock.Any()).
		Return(streamedEntry, nil).Times(2)

	reader := NewReaderUsingRetriever(
		ident.StringID("foo"), retriever, nil, nil, opts)

	streamedEntry.EXPECT().RetrieveWideEntry().Return(xio.WideEntry{}, errors.New("err"))
	streamed, err := reader.FetchWideEntry(ctx, alignedStart, nil, namespace.Context{})
	require.NoError(t, err)
	_, err = streamed.RetrieveWideEntry()
	assert.EqualError(t, err, "err")

	// Check reads as expected
	entry := xio.WideEntry{
		MetadataChecksum: 5,
		ID:               ident.StringID("foo"),
	}

	streamedEntry.EXPECT().RetrieveWideEntry().Return(entry, nil)
	streamed, err = reader.FetchWideEntry(ctx, alignedStart, nil, namespace.Context{})
	require.NoError(t, err)
	actual, err := streamed.RetrieveWideEntry()
	require.NoError(t, err)
	assert.Equal(t, entry, actual)
}

type readTestCase struct {
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

var (
	opts      = newSeriesTestOptions()
	ropts     = opts.RetentionOptions()
	blockSize = ropts.BlockSize()
	// Subtract a few blocksizes to make sure the test cases don't try and query into
	// the future.
	start = opts.ClockOptions().NowFn()().Truncate(blockSize).Add(-5 * blockSize)
)

var robustReaderTestCases = []readTestCase{
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
		title: "Combines data from all sources for different block starts and same block starts",
		times: []time.Time{start, start.Add(blockSize), start.Add(2 * blockSize), start.Add(3 * blockSize)},
		// Block 1 and 3 from disk cache.
		cachedBlocks: map[xtime.UnixNano]streamResponse{
			xtime.ToUnixNano(start): streamResponse{
				blockReader: xio.BlockReader{
					SegmentReader: xio.NewSegmentReader(ts.Segment{}),
					Start:         start,
					BlockSize:     blockSize,
				},
			},
			xtime.ToUnixNano(start.Add(2 * blockSize)): streamResponse{
				blockReader: xio.BlockReader{
					SegmentReader: xio.NewSegmentReader(ts.Segment{}),
					Start:         start.Add(2 * blockSize),
					BlockSize:     blockSize,
				},
			},
		},
		// blocks 2 and 4 from disk.
		diskBlocks: map[xtime.UnixNano]streamResponse{
			xtime.ToUnixNano(start.Add(blockSize)): streamResponse{
				blockReader: xio.BlockReader{
					SegmentReader: xio.NewSegmentReader(ts.Segment{}),
					Start:         start.Add(blockSize),
					BlockSize:     blockSize,
				},
			},
			xtime.ToUnixNano(start.Add(3 * blockSize)): streamResponse{
				blockReader: xio.BlockReader{
					SegmentReader: xio.NewSegmentReader(ts.Segment{}),
					Start:         start.Add(3 * blockSize),
					BlockSize:     blockSize,
				},
			},
		},
		// Blocks 1, 2, and 3 from buffer.
		bufferBlocks: map[xtime.UnixNano]block.FetchBlockResult{
			xtime.ToUnixNano(start): {
				Start:  start,
				Blocks: []xio.BlockReader{xio.BlockReader{Start: start, BlockSize: blockSize}},
			},
			xtime.ToUnixNano(start.Add(blockSize)): {
				Start:  start.Add(blockSize),
				Blocks: []xio.BlockReader{xio.BlockReader{Start: start.Add(blockSize), BlockSize: blockSize}},
			},
			xtime.ToUnixNano(start.Add(2 * blockSize)): {
				Start:  start.Add(2 * blockSize),
				Blocks: []xio.BlockReader{xio.BlockReader{Start: start.Add(2 * blockSize), BlockSize: blockSize}},
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
			{
				Start: start.Add(blockSize),
				Blocks: []xio.BlockReader{
					// One from disk.
					xio.BlockReader{Start: start.Add(blockSize), BlockSize: blockSize},
					// One from buffer.
					xio.BlockReader{Start: start.Add(blockSize), BlockSize: blockSize},
				},
			},
			{
				Start: start.Add(2 * blockSize),
				Blocks: []xio.BlockReader{
					// One from disk cache.
					xio.BlockReader{Start: start.Add(2 * blockSize), BlockSize: blockSize},
					// One from buffer.
					xio.BlockReader{Start: start.Add(2 * blockSize), BlockSize: blockSize},
				},
			},
			{
				Start: start.Add(3 * blockSize),
				Blocks: []xio.BlockReader{
					// One from disk.
					xio.BlockReader{Start: start.Add(3 * blockSize), BlockSize: blockSize},
				},
			},
		},
	},
}

func TestReaderFetchBlocksRobust(t *testing.T) {
	for _, tc := range robustReaderTestCases {
		t.Run(tc.title, func(t *testing.T) {
			ctrl := xtest.NewController(t)
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
						retriever.EXPECT().IsBlockRetrievable(currTime).Return(false, nil)
					} else {
						retriever.EXPECT().IsBlockRetrievable(currTime).Return(true, nil)
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

func TestReaderReadEncodedRobust(t *testing.T) {
	for _, tc := range robustReaderTestCases {
		t.Run(tc.title, func(t *testing.T) {
			ctrl := xtest.NewController(t)
			defer ctrl.Finish()

			var (
				onRetrieveBlock = block.NewMockOnRetrieveBlock(ctrl)
				retriever       = NewMockQueryableBlockRetriever(ctrl)
				diskCache       = block.NewMockDatabaseSeriesBlocks(ctrl)
				buffer          = NewMockdatabaseBuffer(ctrl)
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
					b.EXPECT().SetLastReadTime(gomock.Any()).AnyTimes()
					if cachedBlocks.err != nil {
						b.EXPECT().Stream(ctx).Return(xio.BlockReader{}, cachedBlocks.err)
					} else {
						b.EXPECT().Stream(ctx).Return(cachedBlocks.blockReader, nil)
					}
					diskCache.EXPECT().BlockAt(currTime).Return(b, true)
					if cachedBlocks.err != nil {
						// Stop setting up mocks since the function will early return.
						break
					}
				} else {
					// If the data was not in the disk cache then expect that and setup a query
					// for disk.
					diskCache.EXPECT().BlockAt(currTime).Return(nil, false)
					diskBlocks, ok := tc.diskBlocks[xtime.ToUnixNano(currTime)]
					if !ok {
						retriever.EXPECT().IsBlockRetrievable(currTime).Return(false, nil)
					} else {
						retriever.EXPECT().IsBlockRetrievable(currTime).Return(true, nil)
						if diskBlocks.err != nil {
							retriever.EXPECT().
								Stream(ctx, ident.NewIDMatcher("foo"), currTime, onRetrieveBlock, gomock.Any()).
								Return(xio.BlockReader{}, diskBlocks.err)
							break
						} else {
							retriever.EXPECT().
								Stream(ctx, ident.NewIDMatcher("foo"), currTime, onRetrieveBlock, gomock.Any()).
								Return(diskBlocks.blockReader, nil)
						}
					}
				}

				// Setup buffer mocks.
				bufferBlocks, wasInBuffer := tc.bufferBlocks[xtime.ToUnixNano(currTime)]
				if wasInBuffer {
					if bufferBlocks.Err != nil {
						buffer.EXPECT().
							ReadEncoded(ctx, currTime, currTime.Add(blockSize), namespace.Context{}).
							Return(nil, bufferBlocks.Err)
					} else {
						buffer.EXPECT().
							ReadEncoded(ctx, currTime, currTime.Add(blockSize), namespace.Context{}).
							Return([][]xio.BlockReader{bufferBlocks.Blocks}, nil)
					}
				} else {
					buffer.EXPECT().
						ReadEncoded(ctx, currTime, currTime.Add(blockSize), namespace.Context{}).
						Return(nil, nil)
				}
			}

			var (
				reader = NewReaderUsingRetriever(
					ident.StringID("foo"), retriever, onRetrieveBlock, nil, opts)
				start = tc.times[0]
				// End is not inclusive so add blocksize to the last time.
				end = tc.times[len(tc.times)-1].Add(blockSize)
			)
			r, err := reader.readersWithBlocksMapAndBuffer(ctx, start, end, diskCache, buffer, namespace.Context{})

			anyContainErr := false
			for _, sr := range tc.cachedBlocks {
				if sr.err != nil {
					anyContainErr = true
					break
				}
			}
			for _, sr := range tc.diskBlocks {
				if sr.err != nil {
					anyContainErr = true
					break
				}
			}
			for _, br := range tc.bufferBlocks {
				if br.Err != nil {
					anyContainErr = true
					break
				}
			}

			if anyContainErr {
				require.Error(t, err)
				return
			}

			require.Equal(t, len(tc.expectedResults), len(r))

			for i, result := range r {
				expectedResult := tc.expectedResults[i]
				for _, br := range result {
					assert.Equal(t, expectedResult.Start, br.Start)
				}
				require.Equal(t, len(expectedResult.Blocks), len(result))
			}
		})
	}
}
